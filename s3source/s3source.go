package s3source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MetadataKey = "qfs"

var metadataRe = regexp.MustCompile(`^(\d+) (?:([0-7]{4})|->(.+))$`)
var ctx = context.Background()
var recvMutex sync.Mutex // for local file system changes

type Options func(*S3Source)

type S3Source struct {
	s3Client   *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	bucket     string
	prefix     string
	// Everything below requires mutex protection.
	m         sync.Mutex
	modTimes  map[string]time.Time
	db        database.Database
	dbChanged bool
	seen      map[string]struct{}
}

func New(bucket, prefix string, options ...Options) (*S3Source, error) {
	s := &S3Source{
		bucket:   bucket,
		prefix:   prefix,
		modTimes: map[string]time.Time{},
	}
	for _, fn := range options {
		fn(s)
	}
	if s.s3Client == nil {
		return nil, fmt.Errorf("an s3 client must be given when creating an S3Source")
	}
	s.uploader = manager.NewUploader(s.s3Client)
	s.downloader = manager.NewDownloader(s.s3Client)
	return s, nil
}

func (s *S3Source) withLock(fn func()) {
	s.m.Lock()
	defer s.m.Unlock()
	fn()
}

func WithS3Client(client *s3.Client) func(*S3Source) {
	return func(s *S3Source) {
		s.s3Client = client
	}
}

func WithDatabase(db database.Database) func(*S3Source) {
	return func(s *S3Source) {
		s.db = db
		s.seen = map[string]struct{}{}
	}
}

func (s *S3Source) FullPath(path string) string {
	return fmt.Sprintf("s3://%s/%s", s.bucket, filepath.Join(s.prefix, path))
}

func (s *S3Source) FileInfo(path string) (*fileinfo.FileInfo, error) {
	key := filepath.Join(s.prefix, path)
	// If we have a reference database and the s3 timestamp matches what is in the
	// database, we can use the cached result instead of calling out to S3. Under any
	// other conditions, we will call out to S3 and then update the database.
	var s3Time *time.Time
	var dbEntry *fileinfo.FileInfo
	s.withLock(func() {
		t, haveTime := s.modTimes[key]
		if !haveTime {
			t, haveTime = s.modTimes[key+"/"]
			if haveTime {
				key += "/"
			}
		}
		if haveTime {
			s3Time = &t
		}
		if s3Time == nil || s.db == nil {
			return
		}
		s.seen[path] = struct{}{}
		e, haveEntry := s.db[path]
		if haveEntry {
			if s3Time.Equal(e.S3Time) {
				dbEntry = e
			} else {
				s.dbChanged = true
				delete(s.db, path)
			}
		}
	})
	if dbEntry != nil {
		return dbEntry, nil
	}
	input := &s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	output, err := s.s3Client.HeadObject(ctx, input)
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		input = &s3.HeadObjectInput{
			Bucket: &s.bucket,
			Key:    aws.String(key + "/"),
		}
		output, err = s.s3Client.HeadObject(ctx, input)
		if err == nil {
			key += "/"
		}
	}
	if err != nil {
		return nil, fmt.Errorf("get information for %s: %w", s.FullPath(path), err)
	}
	var qfsData string
	if output.Metadata != nil {
		qfsData = output.Metadata[MetadataKey]
	}
	// HeadObject returns time with a lower granularity than ListObjectsV2.
	if s3Time == nil {
		listInput := &s3.ListObjectsV2Input{
			Bucket:  &s.bucket,
			Prefix:  &key,
			MaxKeys: aws.Int32(1),
		}
		listOutput, err := s.s3Client.ListObjectsV2(ctx, listInput)
		if err != nil {
			// TEST: NOT COVERED
			return nil, fmt.Errorf("get listing for %s: %w", s.FullPath(path), err)
		}
		if len(listOutput.Contents) == 0 || *(listOutput.Contents[0].Key) != key {
			// TEST: NOT COVERED
			return nil, fmt.Errorf("no listing available for %s", s.FullPath(path))
		}
		s3Time = listOutput.Contents[0].LastModified
	}
	fi := &fileinfo.FileInfo{
		Path:        path,
		FileType:    fileinfo.TypeUnknown,
		ModTime:     *output.LastModified,
		Size:        *output.ContentLength,
		Permissions: 0777,
		Uid:         database.CurUid,
		Gid:         database.CurGid,
		S3Time:      *s3Time,
	}
	if qfsData != "" {
		if m := metadataRe.FindStringSubmatch(qfsData); m != nil {
			milliseconds, _ := strconv.Atoi(m[1])
			fi.ModTime = time.UnixMilli(int64(milliseconds))
			if m[2] != "" {
				permissions, _ := strconv.ParseInt(m[2], 8, 32)
				fi.Permissions = uint16(permissions)
			}
			if m[3] != "" {
				fi.FileType = fileinfo.TypeLink
				fi.Special = m[3] // link target
			}
		}
	}
	if fi.FileType == fileinfo.TypeUnknown {
		if strings.HasSuffix(key, "/") {
			fi.FileType = fileinfo.TypeDirectory
		} else {
			fi.FileType = fileinfo.TypeFile
		}
	}
	if s.db != nil {
		s.withLock(func() {
			s.dbChanged = true
			s.db[path] = fi
		})
	}
	return fi, nil
}

func (s *S3Source) DirEntries(path string) ([]fileinfo.DirEntry, error) {
	key := filepath.Join(s.prefix, path)
	if key != "" && !strings.HasSuffix(key, "/") {
		key += "/"
	}
	input := &s3.ListObjectsV2Input{
		Bucket:    &s.bucket,
		Delimiter: aws.String("/"),
		Prefix:    &key,
	}
	p := s3.NewListObjectsV2Paginator(s.s3Client, input)
	var entries []fileinfo.DirEntry
	for p.HasMorePages() {
		output, err := p.NextPage(ctx)
		if err != nil {
			// TEST: NOT COVERED
			return nil, fmt.Errorf("list %s: %w", s.FullPath(path), err)
		}
		// qfs stores metadata on directories by creating empty keys whose paths end with
		// /. If you have a key x/y/, when you list x/ with delimiter /, you will see
		// x/y/ as a prefix, and when list x/y/ with delimiter /, you will see x/y/ as
		// content. We can only get the modify time when we see it as content.
		for _, x := range output.CommonPrefixes {
			// This is a directory. An explicit key ending with / may exist, in which case it
			// will be seen when we read the children.
			entries = append(entries, fileinfo.DirEntry{
				Name:  filepath.Base(*x.Prefix),
				S3Dir: true,
			})
		}
		for _, x := range output.Contents {
			// If the key ends with /, this is the directory marker. We don't want to return
			// it, but we still want to record its modification time so we can get a cache
			// hit.
			if !strings.HasSuffix(*x.Key, "/") {
				entries = append(entries, fileinfo.DirEntry{Name: filepath.Base(*x.Key)})
			}
			s.withLock(func() {
				s.modTimes[*x.Key] = x.LastModified.Truncate(time.Millisecond)
			})
		}
	}
	return entries, nil
}

func (*S3Source) HasStDev() bool {
	return false
}

func (*S3Source) IsS3() bool {
	return true
}

func (s *S3Source) Open(path string) (io.ReadCloser, error) {
	key := filepath.Join(s.prefix, path)
	input := &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	output, err := s.s3Client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", s.FullPath(path), err)
	}
	return output.Body, nil
}

func (s *S3Source) Remove(path string) error {
	isDir := strings.HasSuffix(path, "/")
	key := filepath.Join(s.prefix, path)
	if isDir {
		key += "/"
	}
	input := &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err := s.s3Client.DeleteObject(ctx, input)
	if err != nil {
		// TEST: NOT COVERED. DeleteObject is idempotent.
		return fmt.Errorf("delete object %s: %w", s.FullPath(path), err)
	}
	return nil
}

// Store copies the local file at `path` into the repository with the appropriate
// metadata. `path` is relative to top of the file collection in both the local
// and repository contexts.
func (s *S3Source) Store(localPath *fileinfo.Path, repoPath string) error {
	info, err := localPath.FileInfo()
	if err != nil {
		return err
	}
	if repoPath == "." {
		repoPath = ""
	}
	key := filepath.Join(s.prefix, repoPath)
	var qfsData string
	var body io.Reader
	switch info.FileType {
	case fileinfo.TypeDirectory:
		key += "/"
		qfsData = fmt.Sprintf("%d %04o", info.ModTime.UnixMilli(), info.Permissions)
	case fileinfo.TypeFile:
		qfsData = fmt.Sprintf("%d %04o", info.ModTime.UnixMilli(), info.Permissions)
		fileBody, err := localPath.Open()
		if err != nil {
			// TEST: NOT COVERED
			return err
		}
		defer func() { _ = fileBody.Close() }()
		body = fileBody
	case fileinfo.TypeLink:
		qfsData = fmt.Sprintf("%d ->%s", info.ModTime.UnixMilli(), info.Special)
	default:
		return fmt.Errorf("storing %s: can only store files, links, and directories", localPath)
	}
	metadata := map[string]string{
		"qfs": qfsData,
	}
	if body == nil {
		body = &bytes.Buffer{}
	}
	input := &s3.PutObjectInput{
		Bucket:   &s.bucket,
		Key:      &key,
		Body:     body,
		Metadata: metadata,
	}
	_, err = s.uploader.Upload(ctx, input)
	if err != nil {
		// TEST: NOT COVERED
		return fmt.Errorf("upload %s: %w", s.FullPath(repoPath), err)
	}
	return nil
}

// Retrieve retrieves a file from the repository. No action is performed
// If localPath has the same size and modification time as indicated in the repo.
// The return value indicates whether the file changed.
func (s *S3Source) Retrieve(repoPath string, localPath string) (bool, error) {
	// Lock a mutex for local file system operations. Unlock the mutex while interacting with S3.
	recvMutex.Lock()
	defer recvMutex.Unlock()
	withUnlocked := func(fn func()) {
		recvMutex.Unlock()
		defer recvMutex.Lock()
		fn()
	}

	srcPath := fileinfo.NewPath(s, repoPath)
	destPath := fileinfo.NewPath(fileinfo.NewLocal(""), localPath)
	srcInfo, err := srcPath.FileInfo()
	if err != nil {
		return false, err
	}
	if srcInfo.FileType == fileinfo.TypeLink {
		target, err := os.Readlink(localPath)
		if err == nil && target == srcInfo.Special {
			return false, nil
		}
		err = os.MkdirAll(filepath.Dir(localPath), 0777)
		if err != nil {
			return false, err
		}
		err = os.RemoveAll(localPath)
		if err != nil {
			return false, err
		}
		err = os.Symlink(srcInfo.Special, localPath)
		if err != nil {
			return false, err
		}
		return true, nil
	} else if srcInfo.FileType == fileinfo.TypeDirectory {
		p := fileinfo.NewPath(fileinfo.NewLocal(""), localPath)
		info, err := p.FileInfo()
		if err != nil || info.FileType != fileinfo.TypeDirectory {
			err = os.RemoveAll(localPath)
			if err != nil {
				return false, err
			}
		}
		// Ignore directory times.
		if info != nil && info.FileType == fileinfo.TypeDirectory && info.Permissions == srcInfo.Permissions {
			// No action required
			return false, nil
		}
		err = os.MkdirAll(localPath, 0777)
		if err != nil {
			return false, err
		}
		if err := os.Chmod(localPath, fs.FileMode(srcInfo.Permissions)); err != nil {
			return false, fmt.Errorf("set mode for %s: %w", localPath, err)
		}
		return true, nil
	} else if srcInfo.FileType != fileinfo.TypeFile {
		// TEST: NOT COVERED. There is no way to represent this, and Store doesn't store
		// specials.
		return false, fmt.Errorf("downloading special files is not supported")
	}
	var requiresCopy bool
	withUnlocked(func() {
		requiresCopy, err = fileinfo.RequiresCopy(srcPath, destPath)
	})
	if err != nil {
		return false, err
	}
	if !requiresCopy {
		return false, nil
	}
	err = os.MkdirAll(filepath.Dir(localPath), 0777)
	if err != nil {
		return false, err
	}
	err = os.Chmod(localPath, fs.FileMode(srcInfo.Permissions|0o600))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	f, err := os.Create(localPath)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	key := filepath.Join(s.prefix, repoPath)
	input := &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	withUnlocked(func() {
		_, err = s.downloader.Download(ctx, f, input)
	})
	if err != nil {
		return false, err
	}
	if err := f.Close(); err != nil {
		return false, err
	}
	if err := os.Chtimes(localPath, time.Time{}, srcInfo.ModTime); err != nil {
		return false, fmt.Errorf("set times for %s: %w", localPath, err)
	}
	if err := os.Chmod(localPath, fs.FileMode(srcInfo.Permissions)); err != nil {
		return false, fmt.Errorf("set mode for %s: %w", localPath, err)
	}
	return true, nil
}

func (s *S3Source) DbChanged() bool {
	return s.dbChanged
}

func (s *S3Source) Finish() {
	// This is single-threaded, so we don't need the lock. Remove anything from the
	// database that we didn't see during traversal.
	if s.db != nil {
		for k := range s.db {
			if _, ok := s.seen[k]; !ok {
				delete(s.db, k)
			}
		}
	}
}
