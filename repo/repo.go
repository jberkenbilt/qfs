package repo

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jberkenbilt/qfs/fileinfo"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var repo = &Repo{}

type Options func(*Repo)

type Repo struct {
	s3Client      *s3.Client
	localEndpoint string
	bucket        string
	prefix        string
}

type repoFileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
}

type repoDirEntry struct {
	name     string
	isDir    bool
	s3Time   *time.Time
	fileInfo *repoFileInfo
}

func (di *repoDirEntry) Name() string {
	return di.name
}

func (di *repoDirEntry) IsDir() bool {
	return di.isDir
}

func (di *repoDirEntry) Type() fs.FileMode {
	if di.fileInfo == nil {
		//TODO implement me
		panic("implement me")
	}
	return di.fileInfo.Mode().Type()
}

func (di *repoDirEntry) Info() (fs.FileInfo, error) {
	if di.fileInfo == nil {
		//TODO implement me
		panic("implement me")
	}
	return di.fileInfo, nil
}

func (fi *repoFileInfo) Name() string {
	return fi.name
}

func (fi *repoFileInfo) Size() int64 {
	return fi.size
}

func (fi *repoFileInfo) Mode() fs.FileMode {
	return fi.mode
}

func (fi *repoFileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi *repoFileInfo) IsDir() bool {
	return fi.mode.IsDir()
}

func (fi *repoFileInfo) Sys() any {
	return nil
}

func New(bucket, prefix string, options ...Options) (*Repo, error) {
	r := &Repo{
		bucket: bucket,
		prefix: prefix,
	}
	for _, fn := range options {
		fn(r)
	}
	if r.s3Client == nil {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, err
		}
		r.s3Client = s3.NewFromConfig(cfg)
	}
	return r, nil
}

func WithS3Client(client *s3.Client) func(*Repo) {
	return func(r *Repo) {
		r.s3Client = client
	}
}

func (s *Repo) Lstat(path string) (fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Repo) Readlink(path string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Repo) ReadDir(path string) ([]fs.DirEntry, error) {
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
	var entries []*repoDirEntry
	for p.HasMorePages() {
		output, err := p.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", s.bucket, key, err)
		}
		for _, x := range output.CommonPrefixes {
			// We don't know the s3 time for a directory by its prefix. We have to wait until
			// we read that and see the empty key.
			entries = append(entries, &repoDirEntry{
				name:  filepath.Base(*x.Prefix),
				isDir: true,
			})
			// XXX Deal with nil time -- we have to work this out in traverse -- see special case comment.
		}
		for _, x := range output.Contents {
			entries = append(entries, &repoDirEntry{
				name:   filepath.Base(*x.Key),
				isDir:  false,
				s3Time: x.LastModified,
			})
		}
	}
	return nil, nil
}

func (s *Repo) HasStDev() bool {
	return false
}

func (s *Repo) Open(path string) (io.ReadCloser, error) {
	key := filepath.Join(s.prefix, path)
	s3path := fmt.Sprintf("s3://%s/%s", s.bucket, key)
	input := &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	output, err := s.s3Client.GetObject(context.Background(), input)
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", s3path, err)
	}
	return output.Body, nil
}

func (s *Repo) Remove(path string) error {
	key := filepath.Join(s.prefix, path)
	s3path := fmt.Sprintf("s3://%s/%s", s.bucket, key)
	input := &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	}
	_, err := s.s3Client.DeleteObject(context.Background(), input)
	if err != nil {
		return fmt.Errorf("delete object %s: %w", s3path, err)
	}
	return nil
}

func (s *Repo) FileInfo(path string) (*fileinfo.FileInfo, error) {
	// XXX
	return nil, nil
}

// Store copies the local file at `path` into the repository with the appropriate
// metadata. `path` is relative to top of the file collection in both the local
// and repository contexts.
func (s *Repo) Store(path string) error {
	r, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()
	st, err := os.Lstat(path)
	if err != nil {
		// TEST: NOT COVERED. No way to fail to stat a file we just successfully opened.
		return err
	}
	// XXX HERE
	metadata := map[string]string{
		"qfs": fmt.Sprintf(""),
	}

	key := filepath.Join(s.prefix, path)
	s3path := fmt.Sprintf("s3://%s/%s", s.bucket, key)
	uploader := manager.NewUploader(s.s3Client)
	input := s3.PutObjectInput{
		Bucket:   &s.bucket,
		Key:      &key,
		Body:     r,
		Metadata: metadata,
	}
}
