package repo

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

type Options func(*Repo)

type Repo struct {
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

func New(bucket, prefix string, options ...Options) (*Repo, error) {
	r := &Repo{
		bucket:   bucket,
		prefix:   prefix,
		modTimes: map[string]time.Time{},
	}
	for _, fn := range options {
		fn(r)
	}
	if r.s3Client == nil {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, err
		}
		r.s3Client = s3.NewFromConfig(cfg)
	}
	return r, nil
}

func (r *Repo) withLock(fn func()) {
	r.m.Lock()
	defer r.m.Unlock()
	fn()
}

func WithS3Client(client *s3.Client) func(*Repo) {
	return func(r *Repo) {
		r.s3Client = client
	}
}

func WithDatabase(db database.Memory) func(*Repo) {
	return func(r *Repo) {
		r.db = db
		r.seen = map[string]struct{}{}
	}
}

func (r *Repo) FullPath(path string) string {
	return fmt.Sprintf("s3://%s/%s", r.bucket, filepath.Join(r.prefix, path))
}

func (r *Repo) FileInfo(path string) (*fileinfo.FileInfo, error) {
	key := filepath.Join(r.prefix, path)
	// If we have a reference database and the s3 timestamp matches what is in the
	// database, we can use the cached result instead of calling out to S3. Under any
	// other conditions, we will call out to S3 and then update the database.
	var s3Time *time.Time
	var dbEntry *fileinfo.FileInfo
	r.withLock(func() {
		t, haveTime := r.modTimes[key]
		if !haveTime {
			t, haveTime = r.modTimes[key+"/"]
		}
		if haveTime {
			s3Time = &t
		}
		if s3Time == nil || r.db == nil {
			return
		}
		r.seen[path] = struct{}{}
		e, haveEntry := r.db[path]
		if haveEntry {
			if s3Time.Equal(e.S3Time) {
				dbEntry = e
			} else {
				r.dbChanged = true
				delete(r.db, path)
			}
		}
	})
	if dbEntry != nil {
		return dbEntry, nil
	}
	input := &s3.HeadObjectInput{
		Bucket: &r.bucket,
		Key:    &key,
	}
	output, err := r.s3Client.HeadObject(ctx, input)
	var notFound *types.NotFound
	isDir := false
	if errors.As(err, &notFound) {
		isDir = true
		input = &s3.HeadObjectInput{
			Bucket: &r.bucket,
			Key:    aws.String(key + "/"),
		}
		output, err = r.s3Client.HeadObject(ctx, input)
	}
	if err != nil {
		return nil, fmt.Errorf("get information for %s: %w", r.FullPath(path), err)
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
	if r.db != nil {
		r.withLock(func() {
			r.dbChanged = true
			r.db[path] = fi
		})
	}
	return fi, nil
}

func (r *Repo) DirEntries(path string) ([]fileinfo.DirEntry, error) {
	key := filepath.Join(r.prefix, path)
	if key != "" && !strings.HasSuffix(key, "/") {
		key += "/"
	}
	input := &s3.ListObjectsV2Input{
		Bucket:    &r.bucket,
		Delimiter: aws.String("/"),
		Prefix:    &key,
	}
	p := s3.NewListObjectsV2Paginator(r.s3Client, input)
	var entries []fileinfo.DirEntry
	for p.HasMorePages() {
		output, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", r.bucket, key, err)
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
			r.withLock(func() {
				r.modTimes[*x.Key] = x.LastModified.Truncate(time.Millisecond)
			})
		}
	}
	return entries, nil
}

func (*Repo) HasStDev() bool {
	return false
}

func (*Repo) IsS3() bool {
	return true
}

func (r *Repo) Open(path string) (io.ReadCloser, error) {
	key := filepath.Join(r.prefix, path)
	input := &s3.GetObjectInput{
		Bucket: &r.bucket,
		Key:    &key,
	}
	output, err := r.s3Client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", r.FullPath(path), err)
	}
	return output.Body, nil
}

func (r *Repo) Remove(path string) error {
	key := filepath.Join(r.prefix, path)
	input := &s3.DeleteObjectInput{
		Bucket: &r.bucket,
		Key:    &key,
	}
	_, err := r.s3Client.DeleteObject(ctx, input)
	if err != nil {
		return fmt.Errorf("delete object %s: %w", r.FullPath(path), err)
	}
	return nil
}

// Store copies the local file at `path` into the repository with the appropriate
// metadata. `path` is relative to top of the file collection in both the local
// and repository contexts.
func (r *Repo) Store(localPath string, repoPath string) error {
	p := fileinfo.NewPath(fileinfo.NewLocal(""), localPath)
	info, err := p.FileInfo()
	if err != nil {
		return err
	}
	if repoPath == "." {
		repoPath = ""
	}
	key := filepath.Join(r.prefix, repoPath)
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
	uploader := manager.NewUploader(r.s3Client)
	input := &s3.PutObjectInput{
		Bucket:   &r.bucket,
		Key:      &key,
		Body:     body,
		Metadata: metadata,
	}
	_, err = uploader.Upload(ctx, input)
	if err != nil {
		return fmt.Errorf("upload %s: %w", r.FullPath(repoPath), err)
	}
	return nil
}

func (r *Repo) DbChanged() bool {
	return r.dbChanged
}

func (r *Repo) Finish() {
	// This is single-threaded, so we don't need the lock. Remove anything from the
	// database that we didn't see during traversal.
	if r.db != nil {
		for k := range r.db {
			if _, ok := r.seen[k]; !ok {
				delete(r.db, k)
			}
		}
	}
}
