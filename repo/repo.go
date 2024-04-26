package repo

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io/fs"
	"path/filepath"
	"strings"
	"time"
)

var repo = &Repo{}

type Repo struct {
	s3Client *s3.Client
	bucket   string
	prefix   string
}

type repoDirEntry struct {
	name     string
	isDir    bool
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

type repoFileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
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
	return struct{}{}
}

func New(bucket, prefix, region, endpoint string) (*Repo, error) {
	var loadOptions []func(*config.LoadOptions) error
	if region != "" {
		loadOptions = append(loadOptions, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	var s3Options []func(options *s3.Options)
	// XXX This should something like WithLocalEndpoint
	if endpoint != "" {
		s3Options = append(
			s3Options,
			func(options *s3.Options) {
				options.BaseEndpoint = &endpoint
				options.UsePathStyle = true
			},
		)
	}
	s3Client := s3.NewFromConfig(cfg, s3Options...)
	return &Repo{
		s3Client: s3Client,
		bucket:   bucket,
		prefix:   prefix,
	}, nil
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
	for p.HasMorePages() {
		output, err := p.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("list s3://%s/%s: %w", s.bucket, key, err)
		}
		for _, x := range output.CommonPrefixes {
			fmt.Println("XXX prefix", *x.Prefix)
		}
		for _, x := range output.Contents {
			fmt.Println("XXX content", *x.Key, *x.LastModified)
		}
	}
	return nil, nil
}

func (s *Repo) HasStDev() bool {
	return false
}
