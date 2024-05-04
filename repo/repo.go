package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/s3source"
	"github.com/jberkenbilt/qfs/traverse"
	"os"
	"path/filepath"
	"regexp"
)

type Options func(*Repo)

const (
	ConfigFile = ".qfs/repo/config"
	DbFile     = ".qfs/repo/db"
	PendingDb  = ".qfs/pending/repo/db"
	Busy       = ".qfs/busy"
)

type Repo struct {
	localTop string
	bucket   string
	prefix   string
	s3Client *s3.Client
}

var s3Re = regexp.MustCompile(`^s3://([^/]+)/(.*)\n?$`)
var ctx = context.Background()

func New(options ...Options) (*Repo, error) {
	r := &Repo{}
	for _, fn := range options {
		fn(r)
	}
	data, err := os.ReadFile(r.localPath(ConfigFile).Path())
	if err != nil {
		return nil, err
	}
	m := s3Re.FindSubmatch(data)
	if m == nil {
		return nil, fmt.Errorf("%s must contain s3://bucket/prefix", ConfigFile)
	}
	r.bucket = string(m[1])
	r.prefix = string(m[2])
	if r.s3Client == nil {
		// TEST: NOT COVERED. We don't have any automated tests that use a real S3
		// bucket.
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, err
		}
		r.s3Client = s3.NewFromConfig(cfg)
	}
	return r, nil
}

func WithLocalTop(path string) func(r *Repo) {
	return func(r *Repo) {
		r.localTop = path
	}
}

// WithS3Client sets the s3 client to use. If nil, the default client will be used.
func WithS3Client(s3Client *s3.Client) func(r *Repo) {
	return func(r *Repo) {
		r.s3Client = s3Client
	}
}

func (r *Repo) createBusy() error {
	input := &s3.PutObjectInput{
		Bucket: &r.bucket,
		Key:    aws.String(filepath.Join(r.prefix, Busy)),
		Body:   bytes.NewBuffer(make([]byte, 0)),
	}
	_, err := r.s3Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("create \"busy\" object: %w", err)
	}
	return nil
}

func (r *Repo) removeBusy() error {
	input := &s3.DeleteObjectInput{
		Bucket: &r.bucket,
		Key:    aws.String(filepath.Join(r.prefix, Busy)),
	}
	_, err := r.s3Client.DeleteObject(ctx, input)
	if err != nil {
		return fmt.Errorf("remove \"busy\" object: %w", err)
	}
	return nil
}

func (r *Repo) IsInitialized() (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: &r.bucket,
		Key:    aws.String(filepath.Join(r.prefix, DbFile)),
	}
	_, err := r.s3Client.HeadObject(ctx, input)
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *Repo) localPath(relPath string) *fileinfo.Path {
	return fileinfo.NewPath(fileinfo.NewLocal(r.localTop), relPath)
}

func (r *Repo) Init() error {
	isInitialized, err := r.IsInitialized()
	if err != nil {
		return err
	}
	if isInitialized {
		return fmt.Errorf(
			"repository is already initialized; delete s3://%s/%s/%s to re-initialize",
			r.bucket,
			r.prefix,
			DbFile,
		)
	}
	err = r.createBusy()
	if err != nil {
		return err
	}
	src, err := s3source.New(r.bucket, r.prefix, s3source.WithS3Client(r.s3Client))
	if err != nil {
		return err
	}
	tr, err := traverse.New("", traverse.WithSource(src))
	if err != nil {
		return err
	}
	files, err := tr.Traverse(nil, nil)
	if err != nil {
		return err
	}
	tmpDb := r.localPath(PendingDb).Path()
	err = database.WriteDb(tmpDb, files, database.DbRepo)
	if err != nil {
		return err
	}
	err = src.Store(tmpDb, DbFile)
	if err != nil {
		return err
	}
	err = r.removeBusy()
	if err != nil {
		return err
	}
	err = os.Rename(tmpDb, r.localPath(DbFile).Path())
	if err != nil {
		return err
	}
	return nil
}

func (r *Repo) LoadDb(localCopy string) (database.Memory, error) {
	// XXX
	src, err := s3source.New(r.bucket, r.prefix, s3source.WithS3Client(r.s3Client))
	if err != nil {
		return nil, err
	}
	srcPath := fileinfo.NewPath(src, DbFile)
	var toLoad *fileinfo.Path
	if localCopy != "" {
		localPath := r.localPath(localCopy)
		requiresCopy, err := fileinfo.RequiresCopy(srcPath, localPath)
		if err != nil {
			return nil, err
		}
		if !requiresCopy {
			toLoad = localPath
		}
	}
	if toLoad == nil {
		toLoad = srcPath
	}
	files, err := database.Open(toLoad)
	if err != nil {
		return nil, err
	}
	return database.Load(files)
}
