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
	"github.com/jberkenbilt/qfs/misc"
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
	err = r.traverseAndStore()
	if err != nil {
		return err
	}
	err = r.removeBusy()
	if err != nil {
		return err
	}
	return nil
}

func (r *Repo) traverseAndStore() error {
	src, err := s3source.New(r.bucket, r.prefix, s3source.WithS3Client(r.s3Client))
	if err != nil {
		return err
	}
	tr, err := traverse.New(
		"",
		traverse.WithSource(src),
		traverse.WithRepoRules(true),
	)
	if err != nil {
		return err
	}
	files, err := tr.Traverse(nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = files.Close() }()
	tmpDb := r.localPath(repofiles.PendingDb(repofiles.RepoSite))
	err = database.WriteDb(tmpDb.Path(), files, database.DbRepo)
	if err != nil {
		return err
	}
	fmt.Println("uploading repository database")
	err = src.Store(tmpDb, repofiles.RepoDb())
	if err != nil {
		return err
	}
	err = os.Rename(tmpDb.Path(), r.localPath(repofiles.RepoDb()).Path())
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
	localRepoFiles, err := database.Open(r.localPath(repofiles.RepoDb()), database.WithRepoRules(true))
	if err != nil {
		return err
	}
	defer func() { _ = localRepoFiles.Close() }()
	localRepoDb, err := database.Load(localRepoFiles)
	if err != nil {
		return err
	}
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
		traverse.WithRepoRules(true),
		traverse.WithCleanup(config.Cleanup),
	)
	if err != nil {
		return err
	}
	fmt.Println("generating local database")
	localFiles, err := tr.Traverse(nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = localFiles.Close() }()
	localDb, err := database.Load(localFiles)
	if err != nil {
		return err
	}
	localSiteDbPath := r.localPath(repofiles.SiteDb(site))
	err = database.WriteDb(localSiteDbPath.Path(), localDb, database.DbQfs)
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

	if !config.NoOp {
		if err := os.MkdirAll(r.localPath(repofiles.PendingDir(repofiles.RepoSite)).Path(), 0777); err != nil {
			return err
		}
		f, err := os.Create(r.localPath(repofiles.PendingDiff(repofiles.RepoSite)).Path())
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		err = diffResult.WriteDiff(f, true)
		if err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
	}

	downloaded, remoteRepoDb, err := r.loadRepoDb(r.localPath(repofiles.RepoDb()))
	if err != nil {
		return err
	}

	conflicts := false
	for _, ch := range diffResult.Check {
		info, ok := remoteRepoDb[ch.Path]
		if !ok {
			// It's fine if it doesn't exist.
		} else {
			conflict := true
			for _, m := range ch.ModTime {
				if m == info.ModTime.UnixMilli() {
					conflict = false
					break
				}
			}
			if conflict {
				conflicts = true
				_, _ = fmt.Fprintf(os.Stderr, "%s: conflict: modTime=%v\n", ch.Path, info.ModTime)
			}
		}
	}
	if conflicts {
		if !config.NoOp {
			fmt.Printf("Conflicts detected. Override? [y/n] ")
			var answer string
			_, err = fmt.Scanln(&answer)
			if err != nil {
				return err
			}
			if answer == "y" {
				conflicts = false
			}
		}
		if conflicts {
			return fmt.Errorf("conflicts detected")
		}
	}
	if config.NoOp {
		fmt.Println("no conflicts found")
		return nil
	}

	// XXX Remember LocalTar, LocalSite, SaveSiteTar

	// Apply changes to the repository.
	err = r.createBusy()
	if err != nil {
		return err
	}

	// Delete what needs to be deleted.
	toDelete := diffResult.Rm
	for len(toDelete) > 0 {
		last := min(len(toDelete), 1000)
		batch := toDelete[:last]
		if len(batch) == last {
			toDelete = nil
		} else {
			toDelete = toDelete[last:]
		}
		var objects []types.ObjectIdentifier
		for _, p := range batch {
			objects = append(objects, types.ObjectIdentifier{
				Key: aws.String(p),
			})
		}
		deleteBatch := types.Delete{
			Objects: objects,
		}
		deleteInput := &s3.DeleteObjectsInput{
			Bucket: &r.bucket,
			Delete: &deleteBatch,
		}
		_, err = r.s3Client.DeleteObjects(ctx, deleteInput)
		if err != nil {
			return fmt.Errorf("delete keys: %w", err)
		}
	}

	// Upload added and changed files.
	src, err := s3source.New(
		r.bucket,
		r.prefix,
		s3source.WithS3Client(r.s3Client),
		s3source.WithDatabase(remoteRepoDb),
	)
	if err != nil {
		return err
	}

	const numWorkers = 10
	c := make(chan *fileinfo.FileInfo, numWorkers)
	go func() {
		for _, f := range diffResult.Add {
			c <- f
		}
		for _, f := range diffResult.Change {
			c <- f
		}
		close(c)
	}()
	var allErrors []error
	misc.DoConcurrently(
		func(c chan *fileinfo.FileInfo, errorChan chan error) {
			for f := range c {
				fmt.Printf("storing %s\n", f.Path)
				err = src.Store(r.localPath(f.Path), f.Path)
				if err != nil {
					errorChan <- err
				}
			}
		},
		func(e error) {
			allErrors = append(allErrors, e)
		},
		c,
		numWorkers,
	)
	if len(allErrors) > 0 {
		return errors.Join(allErrors...)
	}

	// If we made any changes, update the repo database. Otherwise, update our local copy if needed.
	if len(diffResult.Change) == 0 && len(diffResult.Add) == 0 && len(diffResult.Rm) == 0 {
		// No changes
		if downloaded {
			// Our local copy was outdated, so update it.
			fmt.Println("updating local copy of repository database")
			err = os.Rename(
				r.localPath(repofiles.PendingDb(repofiles.RepoSite)).Path(),
				r.localPath(repofiles.RepoDb()).Path(),
			)
			if err != nil {
				return err
			}
		} else {
			fmt.Println("no changes required to repository database")
		}
	} else {
		err = r.traverseAndStore()
		if err != nil {
			return err
		}
	}

	// Store the site's database in the repository
	fmt.Println("uploading site database")
	err = src.Store(localSiteDbPath, repofiles.SiteDb(site))
	if err != nil {
		return err
	}

	err = r.removeBusy()
	if err != nil {
		return err
	}
	return nil
}

func (r *Repo) loadRepoDb(localPath *fileinfo.Path) (bool, database.Memory, error) {
	src, err := s3source.New(r.bucket, r.prefix, s3source.WithS3Client(r.s3Client))
	if err != nil {
		return false, nil, err
	}
	srcPath := fileinfo.NewPath(src, repofiles.RepoDb())
	var toLoad *fileinfo.Path
	requiresCopy, err := fileinfo.RequiresCopy(srcPath, localPath)
	if err != nil {
		return false, nil, err
	}
	if !requiresCopy {
		fmt.Println("local copy of repository database is current")
		toLoad = localPath
	}
	downloaded := false
	if toLoad == nil {
		fmt.Println("downloading latest repository database")
		downloaded = true
		pending := r.localPath(repofiles.PendingDb(repofiles.RepoSite))
		_, err = src.Retrieve(repofiles.RepoDb(), pending.Path())
		if err != nil {
			return false, nil, err
		}
		toLoad = pending
	}
	files, err := database.Open(toLoad, database.WithRepoRules(true))
	if err != nil {
		return false, nil, err
	}
	db, err := database.Load(files)
	if err != nil {
		return false, nil, err
	}
	return downloaded, db, nil
}
