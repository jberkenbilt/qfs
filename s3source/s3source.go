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
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/misc"
	"github.com/jberkenbilt/qfs/repofiles"
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

// DeleteBatchSize is the number of items we delete from S3 at once. It is set to
// the maximum value supported by S3 and is a variable that is overridden from
// the test suite to exercise the batching logic.
var DeleteBatchSize = 1000

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
	// Everything below requires mutex protection.
	dbMutex   sync.Mutex
	db        database.Database
	extraKeys map[string]time.Time
}

func New(bucket, prefix string, options ...Options) (*S3Source, error) {
	if strings.Contains(prefix, "@") {
		return nil, fmt.Errorf("prefix may not contain '@'")
	}
	if strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("prefix may not end with '/'")
	}
	s := &S3Source{
		bucket:    bucket,
		prefix:    prefix,
		extraKeys: map[string]time.Time{},
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

func (s *S3Source) FullPath(path string) string {
	return fmt.Sprintf("s3://%s/%s@...", s.bucket, filepath.Join(s.prefix, path))
}

func (s *S3Source) KeyToFileInfo(key string, size int64) *fileinfo.FileInfo {
	key = misc.RemovePrefix(key, s.prefix)
	m := pathRe.FindStringSubmatch(key)
	if m == nil {
		return nil
	}
	base := strings.ReplaceAll(m[1], "@@", "@")
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
		special = strings.ReplaceAll(rest, "@@", "@")
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
	prefix := s.KeyFromPath(path, nil)
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
			newFi := s.KeyToFileInfo(*output.Key, *output.Size)
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

func (s *S3Source) KeyFromPath(path string, fi *fileinfo.FileInfo) string {
	key := s.prefix
	if key != "" {
		key += "/"
	}
	key += strings.ReplaceAll(path, "@", "@@") + "@"
	if fi != nil {
		var rest string
		if fi.FileType == fileinfo.TypeLink {
			rest = strings.ReplaceAll(fi.Special, "@", "@@")
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
	key := s.KeyFromPath(path, info)
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
	key := s.KeyFromPath(path, info)
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

func (s *S3Source) RemoveKeys(toDelete []string) error {
	for len(toDelete) > 0 {
		last := min(len(toDelete), DeleteBatchSize)
		batch := toDelete[:last]
		if len(toDelete) == last {
			toDelete = nil
		} else {
			toDelete = toDelete[last:]
		}
		var objects []types.ObjectIdentifier
		for _, key := range batch {
			objects = append(objects, types.ObjectIdentifier{
				Key: &key,
			})
		}
		deleteBatch := types.Delete{
			Objects: objects,
		}
		deleteInput := &s3.DeleteObjectsInput{
			Bucket: &s.bucket,
			Delete: &deleteBatch,
		}
		_, err := s.s3Client.DeleteObjects(ctx, deleteInput)
		if err != nil {
			// TEST: NOT COVERED
			return fmt.Errorf("delete keys: %w", err)
		}
	}
	return nil
}

func (s *S3Source) RemoveBatch(toDelete []*fileinfo.FileInfo) error {
	var keys []string
	for _, fi := range toDelete {
		misc.Message("removing %s", fi.Path)
		keys = append(keys, s.KeyFromPath(fi.Path, fi))
	}
	err := s.RemoveKeys(keys)
	if err != nil {
		return err
	}
	if s.db != nil {
		s.withDbLock(func() {
			for _, fi := range toDelete {
				delete(s.db, fi.Path)
			}
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
	key := s.KeyFromPath(repoPath, info)
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

func (s *S3Source) DownloadVersion(
	key string,
	versionId *string,
	f *os.File,
) error {
	input := &s3.GetObjectInput{
		Bucket:    &s.bucket,
		Key:       &key,
		VersionId: versionId,
	}
	_, err := s.downloader.Download(ctx, f, input)
	return err
}

func (s *S3Source) Download(repoPath string, srcInfo *fileinfo.FileInfo, f *os.File) error {
	key := s.KeyFromPath(repoPath, srcInfo)
	input := &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err := s.downloader.Download(ctx, f, input)
	if err != nil {
		return fmt.Errorf("downloading %s: %w", key, err)
	}
	return nil
}

func (s *S3Source) Database(
	regenerate bool,
	repoRules bool,
	filters []*filter.Filter,
) (database.Database, error) {
	if !regenerate && s.db != nil {
		return s.db, nil
	}
	s.db = database.Database{}
	s.extraKeys = map[string]time.Time{}
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
				s.dbHandleObject(object, repoRules, filters)
			}
		},
	)
	if err != nil {
		return nil, err
	}
	return s.db, nil
}

func (s *S3Source) dbHandleObject(
	object types.Object,
	repoRules bool,
	filters []*filter.Filter,
) {
	if *object.Key == filepath.Join(s.prefix, repofiles.Busy) {
		return
	}
	fi := s.KeyToFileInfo(*object.Key, *object.Size)
	if fi == nil {
		s.withDbLock(func() {
			s.extraKeys[*object.Key] = *object.LastModified
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
				s.extraKeys[s.KeyFromPath(fi.Path, existing)] = existing.ModTime
				s.db[fi.Path] = fi
			} else {
				// This is an older version than the one we already saw.
				s.extraKeys[s.KeyFromPath(fi.Path, fi)] = fi.ModTime
			}
		} else {
			included, _ := filter.IsIncluded(fi.Path, repoRules, filters...)
			if included {
				s.db[fi.Path] = fi
			} else if !strings.HasPrefix(fi.Path, repofiles.Top+"/") {
				s.extraKeys[s.KeyFromPath(fi.Path, fi)] = fi.ModTime
			}
		}
	})
}

func (s *S3Source) ExtraKeys() map[string]time.Time {
	return s.extraKeys
}
