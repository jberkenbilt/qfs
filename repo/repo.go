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
	"github.com/jberkenbilt/qfs/diff"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/repofiles"
	"github.com/jberkenbilt/qfs/s3source"
	"github.com/jberkenbilt/qfs/traverse"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type Options func(*Repo)

type Repo struct {
	localTop string
	bucket   string
	prefix   string
	s3Client *s3.Client
}

type PushConfig struct {
	Cleanup     bool
	NoOp        bool
	LocalTar    string
	SaveSite    string
	SaveSiteTar string
}

var s3Re = regexp.MustCompile(`^s3://([^/]+)/(.*)\n?$`)
var ctx = context.Background()
var progName = filepath.Base(os.Args[0])

func New(options ...Options) (*Repo, error) {
	r := &Repo{}
	for _, fn := range options {
		fn(r)
	}
	data, err := os.ReadFile(r.localPath(repofiles.RepoConfig).Path())
	if err != nil {
		return nil, err
	}
	m := s3Re.FindSubmatch(data)
	if m == nil {
		return nil, fmt.Errorf("%s must contain s3://bucket/prefix", repofiles.RepoConfig)
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
		Key:    aws.String(filepath.Join(r.prefix, repofiles.Busy)),
		Body:   bytes.NewBuffer(make([]byte, 0)),
	}
	_, err := r.s3Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("create \"busy\" object: %w", err)
	}
	return nil
}

func (r *Repo) checkBusy() error {
	exists, err := r.existsInRepo(repofiles.Busy)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf(
			"s3://%s/%s/%s exists; if necessary, rerun qfs init-repo",
			r.bucket,
			r.prefix,
			repofiles.Busy,
		)
	}
	return nil
}

func (r *Repo) removeBusy() error {
	input := &s3.DeleteObjectInput{
		Bucket: &r.bucket,
		Key:    aws.String(filepath.Join(r.prefix, repofiles.Busy)),
	}
	_, err := r.s3Client.DeleteObject(ctx, input)
	if err != nil {
		return fmt.Errorf("remove \"busy\" object: %w", err)
	}
	return nil
}

func (r *Repo) existsInRepo(path string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: &r.bucket,
		Key:    aws.String(filepath.Join(r.prefix, path)),
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

func (r *Repo) IsInitialized() (bool, error) {
	return r.existsInRepo(repofiles.RepoDb())
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
			repofiles.RepoDb(),
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
	tr, err := traverse.New(
		"",
		traverse.WithSource(src),
		traverse.WithQfsOverride("repo"),
	)
	if err != nil {
		return err
	}
	files, err := tr.Traverse(nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = files.Close() }()
	tmpDb := r.localPath(repofiles.PendingDb(repofiles.RepoSite)).Path()
	err = database.WriteDb(tmpDb, files, database.DbRepo)
	if err != nil {
		return err
	}
	err = src.Store(tmpDb, repofiles.RepoDb())
	if err != nil {
		return err
	}
	err = r.removeBusy()
	if err != nil {
		return err
	}
	err = os.Rename(tmpDb, r.localPath(repofiles.RepoDb()).Path())
	if err != nil {
		return err
	}
	return nil
}

func (r *Repo) currentSite() (string, error) {
	data, err := os.ReadFile(r.localPath(repofiles.Site).Path())
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func (r *Repo) Push(config *PushConfig) error {
	err := r.checkBusy()
	if err != nil {
		return err
	}
	site, err := r.currentSite()
	if err != nil {
		return err
	}
	// Open the local copy of the repo database early
	localRepoDb, err := database.Open(r.localPath(repofiles.RepoDb()))
	if err != nil {
		return err
	}
	defer func() { _ = localRepoDb.Close() }()
	// Generate the local site database using prunes only from the repo and site filters.
	filterFiles := []string{
		repofiles.SiteFilter(repofiles.RepoSite),
		repofiles.SiteFilter(site),
	}
	var filters []*filter.Filter
	for _, file := range filterFiles {
		f := filter.New()
		err = f.ReadFile(r.localPath(file), true)
		if err != nil {
			return err
		}
		filters = append(filters, f)
	}
	tr, err := traverse.New(
		r.localTop,
		traverse.WithNoSpecial(true),
		traverse.WithFilters(filters),
		traverse.WithQfsOverride(site),
		traverse.WithCleanup(config.Cleanup),
	)
	if err != nil {
		return err
	}
	fmt.Printf("%s: generating local database\n", progName)
	localFiles, err := tr.Traverse(nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = localFiles.Close() }()
	localDb, err := database.Load(localFiles)
	if err != nil {
		return err
	}
	err = database.WriteDb(r.localPath(repofiles.SiteDb(site)).Path(), localDb, database.DbQfs)
	if err != nil {
		return err
	}
	// Diff against the local copy of the repo database using the same filters but
	// honoring everything, not just prunes.
	filters = nil
	for _, file := range filterFiles {
		f := filter.New()
		err = f.ReadFile(r.localPath(file), false)
		if err != nil {
			return err
		}
		filters = append(filters, f)
	}
	d := diff.New(
		diff.WithFilters(filters),
		diff.WithNoOwnerships(true),
		diff.WithNoSpecial(true),
	)
	diffResult, err := d.Run(localRepoDb, localDb)
	if err != nil {
		return err
	}
	// XXX HERE -- do conflict checking
	// XXX
	err = diffResult.WriteDiff(os.Stdout, true)
	if err != nil {
		return err
	}

	// XXX remember rest of config

	return nil
}

func (r *Repo) LoadDb(localCopy string) (database.Memory, error) {
	// XXX
	src, err := s3source.New(r.bucket, r.prefix, s3source.WithS3Client(r.s3Client))
	if err != nil {
		return nil, err
	}
	srcPath := fileinfo.NewPath(src, repofiles.RepoDb())
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
