package s3source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/localsource"
	"github.com/jberkenbilt/qfs/s3lister"
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

var pathRe = regexp.MustCompile(`^((?:[^@]|@@)+)@([fdl]),(\d+),((?:[^@]|@@)+)$`)
var permRe = regexp.MustCompile(`^[0-7]{4}$`)
var ctx = context.Background()

type Options func(*S3Source)

type S3Source struct {
	s3Client   *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	bucket     string
	prefix     string
	repoRules  bool
	// Everything below requires mutex protection.
	dbMutex   sync.Mutex
	db        database.Database
	extraKeys []string
	recvMutex sync.Mutex // for local file system changes
}

func New(bucket, prefix string, options ...Options) (*S3Source, error) {
	if strings.Contains(prefix, "@") {
		return nil, fmt.Errorf("prefix may not contain '@'")
	}
	s := &S3Source{
		bucket: bucket,
		prefix: prefix,
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

func (s *S3Source) withDbLock(fn func()) {
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()
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
	}
}

func WithRepoRules(repoRules bool) func(*S3Source) {
	return func(s *S3Source) {
		s.repoRules = repoRules
	}
}

func (s *S3Source) FullPath(path string) string {
	return fmt.Sprintf("s3://%s/%s@...", s.bucket, filepath.Join(s.prefix, path))
}

func (s *S3Source) keyToFileInfo(key string, size int64) *fileinfo.FileInfo {
	prefix := s.prefix
	if prefix != "" {
		prefix += "/"
	}
	if !strings.HasPrefix(key, prefix) {
		// TEST: NOT COVERED. ListObjectsV2 won't return a key that doesn't start with
		// the requested prefix.
		panic("key doesn't start with prefix")
	}
	key = key[len(prefix):]
	m := pathRe.FindStringSubmatch(key)
	if m == nil {
		return nil
	}
	base := strings.Replace(m[1], "@@", "@", -1)
	modTimeMs, err := strconv.ParseInt(m[3], 10, 64)
	if err != nil {
		// modTime is invalid
		return nil
	}
	modTime := time.UnixMilli(modTimeMs)
	// Setting fType this way is known to be safe because of the regular expression.
	fType := fileinfo.FileType(m[2][0])
	rest := m[4]
	var special string
	var permissions int64
	if fType == fileinfo.TypeDirectory || fType == fileinfo.TypeFile {
		if !permRe.MatchString(rest) {
			// Invalid permissions
			return nil
		}
		permissions, _ = strconv.ParseInt(rest, 8, 16)
	} else {
		special = strings.Replace(rest, "@@", "@", -1)
		permissions = 0o777
	}
	return &fileinfo.FileInfo{
		Path:        base,
		FileType:    fType,
		ModTime:     modTime,
		Size:        size,
		Permissions: uint16(permissions),
		Uid:         database.CurUid,
		Gid:         database.CurGid,
		Special:     special,
	}
}

func (s *S3Source) FileInfo(path string) (*fileinfo.FileInfo, error) {
	// If we have a reference database, try to use it instead of calling out to S3.
	// Under any other conditions, we will call out to S3 and then update the
	// database.
	var dbEntry *fileinfo.FileInfo
	s.withDbLock(func() {
		e, haveEntry := s.db[path]
		if haveEntry {
			dbEntry = e
		}
	})
	if dbEntry != nil {
		return dbEntry, nil
	}
	prefix := s.key(path, nil)
	listInput := &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: &prefix,
	}
	paginator := s3.NewListObjectsV2Paginator(s.s3Client, listInput)
	var fi *fileinfo.FileInfo
	for paginator.HasMorePages() {
		listOutput, err := paginator.NextPage(ctx)
		if err != nil {
			// TEST: NOT COVERED
			return nil, fmt.Errorf("get listing for %s: %w", s.FullPath(path), err)
		}
		for _, output := range listOutput.Contents {
			newFi := s.keyToFileInfo(*output.Key, *output.Size)
			if newFi.Path != path {
				// This is for the wrong path -- that most likely means there were extra @ signs
				// in the name.
				continue
			}
			if fi != nil && newFi.ModTime.Before(fi.ModTime) {
				// Keep only the latest match
				continue
			}
			fi = newFi
		}
	}
	if fi == nil {
		return nil, fmt.Errorf("%s: %w", s.FullPath(path), fs.ErrNotExist)
	}
	if s.db != nil {
		s.withDbLock(func() {
			s.db[path] = fi
		})
	}
	return fi, nil
}

func (s *S3Source) key(path string, fi *fileinfo.FileInfo) string {
	key := s.prefix
	if key != "" {
		key += "/"
	}
	key += strings.Replace(path, "@", "@@", -1) + "@"
	if fi != nil {
		var rest string
		if fi.FileType == fileinfo.TypeLink {
			rest = strings.Replace(fi.Special, "@", "@@", -1)
		} else {
			rest = fmt.Sprintf("%04o", fi.Permissions)
		}
		key += fmt.Sprintf("%c,%d,%s", fi.FileType, fi.ModTime.UnixMilli(), rest)
	}
	return key
}

func (s *S3Source) Open(path string) (io.ReadCloser, error) {
	info, err := s.FileInfo(path)
	if err != nil {
		return nil, err
	}
	key := s.key(path, info)
	input := &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	output, err := s.s3Client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get object s3://%s/%s: %w", s.bucket, key, err)
	}
	return output.Body, nil
}

func (s *S3Source) Remove(path string) error {
	info, err := s.FileInfo(path)
	if errors.Is(err, fs.ErrNotExist) {
		// Make Remove idempotent
		return nil
	}
	key := s.key(path, info)
	input := &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err = s.s3Client.DeleteObject(ctx, input)
	if err != nil {
		// TEST: NOT COVERED. DeleteObject is idempotent.
		return fmt.Errorf("delete object s3://%s/%s: %w", s.bucket, key, err)
	}
	if s.db != nil {
		s.withDbLock(func() {
			delete(s.db, path)
		})
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
	err = s.Remove(repoPath)
	if err != nil {
		return err
	}
	key := s.key(repoPath, info)
	var body io.Reader
	switch info.FileType {
	case fileinfo.TypeFile:
		fileBody, err := localPath.Open()
		if err != nil {
			// TEST: NOT COVERED
			return err
		}
		defer func() { _ = fileBody.Close() }()
		body = fileBody
	case fileinfo.TypeDirectory:
	case fileinfo.TypeLink:
	default:
		return fmt.Errorf("can only store files, directories, and links")
	}
	if body == nil {
		body = &bytes.Buffer{}
	}
	input := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
		Body:   body,
	}
	_, err = s.uploader.Upload(ctx, input)
	if err != nil {
		// TEST: NOT COVERED
		return fmt.Errorf("upload s3://%s/%s: %w", s.bucket, key, err)
	}
	if s.db != nil {
		s.withDbLock(func() {
			newFi := *info
			newFi.Path = repoPath
			s.db[repoPath] = &newFi
		})
	}
	return nil
}

// Retrieve retrieves a file from the repository. No action is performed
// If localPath has the same size and modification time as indicated in the repo.
// The return value indicates whether the file changed.
func (s *S3Source) Retrieve(repoPath string, localPath string) (bool, error) {
	// Lock a mutex for local file system operations. Unlock the mutex while interacting with S3.
	s.recvMutex.Lock()
	defer s.recvMutex.Unlock()
	withUnlocked := func(fn func()) {
		s.recvMutex.Unlock()
		defer s.recvMutex.Lock()
		fn()
	}

	srcPath := fileinfo.NewPath(s, repoPath)
	destPath := fileinfo.NewPath(localsource.New(""), localPath)
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
		p := fileinfo.NewPath(localsource.New(""), localPath)
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
	key := s.key(repoPath, srcInfo)
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

func (s *S3Source) Database() (database.Database, error) {
	if s.db != nil {
		return s.db, nil
	}
	s.db = database.Database{}
	s.extraKeys = nil
	lister, err := s3lister.New(s3lister.WithS3Client(s.s3Client))
	if err != nil {
		return nil, err
	}
	prefix := s.prefix
	if prefix != "" {
		prefix += "/"
	}
	input := &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: &prefix,
	}
	err = lister.List(
		context.Background(),
		input,
		func(objects []types.Object) {
			for _, object := range objects {
				s.dbHandleObject(object)
			}
		},
	)
	if err != nil {
		return nil, err
	}
	return s.db, nil
}

func (s *S3Source) dbHandleObject(object types.Object) {
	fi := s.keyToFileInfo(*object.Key, *object.Size)
	if fi == nil {
		s.withDbLock(func() {
			s.extraKeys = append(s.extraKeys, *object.Key)
		})
		return
	}
	s.withDbLock(func() {
		existing := s.db[fi.Path]
		if existing != nil {
			if fi.ModTime.After(existing.ModTime) {
				// This is a newer match for the same path, so keep it in favor of the one. This
				// should never actually happen, but it could happen if we stored a new key
				// without deleting an old one.
				s.extraKeys = append(s.extraKeys, s.key(fi.Path, existing))
				s.db[fi.Path] = fi
			} else {
				// This is an older version than the one we already saw.
				s.extraKeys = append(s.extraKeys, s.key(fi.Path, fi))
			}
		} else {
			s.db[fi.Path] = fi
		}
	})
}

func (s *S3Source) ExtraKeys() []string {
	return s.extraKeys
}
