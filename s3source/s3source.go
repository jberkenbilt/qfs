package s3source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"io"
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

type Options func(*S3Source)

type S3Source struct {
	s3Client *s3.Client
	bucket   string
	prefix   string
	// Everything below requires mutex protection.
	m         sync.Mutex
	modTimes  map[string]time.Time
	db        database.Memory
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
		// TEST: NOT COVERED. We don't have any automated tests that use a real S3
		// bucket.
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, err
		}
		s.s3Client = s3.NewFromConfig(cfg)
	}
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

func WithDatabase(db database.Memory) func(*S3Source) {
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
	isDir := false
	if errors.As(err, &notFound) {
		isDir = true
		input = &s3.HeadObjectInput{
			Bucket: &s.bucket,
			Key:    aws.String(key + "/"),
		}
		output, err = s.s3Client.HeadObject(ctx, input)
	}
	if err != nil {
		// TEST: NOT COVERED
		return nil, fmt.Errorf("get information for %s: %w", s.FullPath(path), err)
	}
	var qfsData string
	if output.Metadata != nil {
		qfsData = output.Metadata[MetadataKey]
	}
	fi := &fileinfo.FileInfo{
		Path:        path,
		FileType:    fileinfo.TypeUnknown,
		ModTime:     *output.LastModified,
		Size:        *output.ContentLength,
		Permissions: 0777,
		Uid:         database.CurUid,
		Gid:         database.CurGid,
		S3Time:      output.LastModified.Truncate(time.Millisecond),
	}
	// HeadObject returns time with a lower granularity, so use the one we got from
	// ListObjectsV2 if possible.
	if s3Time != nil {
		fi.S3Time = *s3Time
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
		if isDir {
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
	key := filepath.Join(s.prefix, path)
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
func (s *S3Source) Store(localPath string, repoPath string) error {
	p := fileinfo.NewPath(fileinfo.NewLocal(""), localPath)
	info, err := p.FileInfo()
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
		fileBody, err := p.Open()
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
	uploader := manager.NewUploader(s.s3Client)
	input := &s3.PutObjectInput{
		Bucket:   &s.bucket,
		Key:      &key,
		Body:     body,
		Metadata: metadata,
	}
	_, err = uploader.Upload(ctx, input)
	if err != nil {
		// TEST: NOT COVERED
		return fmt.Errorf("upload %s: %w", s.FullPath(repoPath), err)
	}
	return nil
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
