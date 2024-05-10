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
	"io/fs"
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

type PullConfig struct {
	NoOp        bool
	LocalFilter bool
	SiteTar     string
}

const numWorkers = 10

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
		if !misc.Prompt("Repository is already initialized. Rebuild database?") {
			return fmt.Errorf(
				"repository is already initialized; delete s3://%s/%s/%s to re-initialize",
				r.bucket,
				r.prefix,
				repofiles.RepoDb(),
			)
		}
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
	tmpDb := r.localPath(repofiles.TempRepoDb())
	err = database.WriteDb(tmpDb.Path(), files, database.DbRepo)
	if err != nil {
		return err
	}
	misc.Message("uploading repository database")
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

func checkConflicts(
	checks []*diff.Check,
	allowOverride bool,
	getInfo func(path string) (*fileinfo.FileInfo, error),
) error {
	conflicts := false
	for _, ch := range checks {
		info, err := getInfo(ch.Path)
		if err != nil {
			return err
		}
		if info == nil {
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
	if conflicts && allowOverride && !misc.Prompt("Conflicts detected. Exit?") {
		misc.Message("overriding conflicts")
		conflicts = false
	}
	if conflicts {
		return fmt.Errorf("conflicts detected")
	}
	return nil
}

func makeDiff(filters []*filter.Filter) *diff.Diff {
	return diff.New(
		diff.WithFilters(filters),
		diff.WithNoOwnerships(true),
		diff.WithNoSpecial(true),
		diff.WithNoDirTimes(true),
		diff.WithRepoRules(true),
	)
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
	localRepoFiles, err := database.Open(
		r.localPath(repofiles.RepoDb()),
		database.WithRepoRules(true),
	)
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
	misc.Message("generating local database")
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
	d := makeDiff(filters)
	diffResult, err := d.Run(localRepoDb, localDb)
	if err != nil {
		return err
	}

	if !config.NoOp {
		// Write diff to a local file as a marker that a push has been run.
		err = r.SaveDiff(repofiles.Push, diffResult)
		if err != nil {
			return err
		}
	}

	downloadedRepoDb, remoteRepoDb, err := r.loadRepoDb()
	if err != nil {
		return err
	}

	err = checkConflicts(diffResult.Check, !config.NoOp, func(path string) (*fileinfo.FileInfo, error) {
		info, ok := remoteRepoDb[path]
		if !ok {
			return nil, nil
		}
		return info, nil
	})
	if err != nil {
		return err
	}
	misc.Message("no conflicts found")

	// XXX Remember LocalTar, LocalSite, SaveSiteTar

	changes := len(diffResult.Change)+len(diffResult.Add)+len(diffResult.Rm)+len(diffResult.MetaChange) > 0
	if changes {
		misc.Message("----- changes to push -----")
		_ = diffResult.WriteDiff(os.Stdout, false)
		misc.Message("-----")
		if !config.NoOp && !misc.Prompt("Continue?") {
			return fmt.Errorf("exiting")
		}
	} else {
		misc.Message("no changes to push")
	}

	if config.NoOp {
		return nil
	}

	// Apply changes to the repository.
	err = r.createBusy()
	if err != nil {
		return err
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

	if changes {
		err = r.pushChangesToRepo(src, diffResult)
		if err != nil {
			return err
		}
		// Update the repository database. It would be nice if we could update our
		// working copy and push it up to avoid the extra traversal, but as of 2024-05,
		// this is not possible since ListObjectsV2 is the only way to get the
		// millisecond granularity S3 timestamps that we need for the repository
		// database.
		err = r.traverseAndStore()
		if err != nil {
			return err
		}
	} else {
		if downloadedRepoDb {
			// Our local copy was outdated, so update it.
			misc.Message("updating local copy of repository database")
			err = os.Rename(
				r.localPath(repofiles.TempRepoDb()).Path(),
				r.localPath(repofiles.RepoDb()).Path(),
			)
			if err != nil {
				return err
			}
		}
	}

	// Store the site's database in the repository
	misc.Message("uploading site database")
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

func (r *Repo) pushChangesToRepo(src *s3source.S3Source, diffResult *diff.Result) error {
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
			misc.Message("removing %s", p)
			objects = append(objects, types.ObjectIdentifier{
				Key: aws.String(filepath.Join(r.prefix, p)),
			})
		}
		deleteBatch := types.Delete{
			Objects: objects,
		}
		deleteInput := &s3.DeleteObjectsInput{
			Bucket: &r.bucket,
			Delete: &deleteBatch,
		}
		_, err := r.s3Client.DeleteObjects(ctx, deleteInput)
		if err != nil {
			return fmt.Errorf("delete keys: %w", err)
		}
	}

	c := make(chan *fileinfo.FileInfo, numWorkers)
	go func() {
		for _, f := range diffResult.Add {
			c <- f
		}
		for _, f := range diffResult.Change {
			c <- f
		}
		for _, f := range diffResult.MetaChange {
			if f.Permissions != nil {
				c <- f.Info
			}
		}
		close(c)
	}()
	var allErrors []error
	misc.DoConcurrently(
		func(c chan *fileinfo.FileInfo, errorChan chan error) {
			for f := range c {
				misc.Message("storing %s", f.Path)
				err := src.Store(r.localPath(f.Path), f.Path)
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

	return nil
}

func (r *Repo) SaveDiff(path string, diffResult *diff.Result) error {
	f, err := os.Create(r.localPath(path).Path())
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
	return nil
}

func (r *Repo) Pull(config *PullConfig) error {
	err := r.checkBusy()
	if err != nil {
		return err
	}
	site, err := r.currentSite()
	if err != nil {
		return err
	}
	downloadedRepoDb, repoDb, err := r.loadRepoDb()
	if err != nil {
		return err
	}

	// Load the repository copy of the site database. If not found, use an empty database.
	src, err := s3source.New(r.bucket, r.prefix, s3source.WithS3Client(r.s3Client))
	if err != nil {
		return err
	}
	repoSiteDbPath := fileinfo.NewPath(src, repofiles.SiteDb(site))
	files, err := database.Open(repoSiteDbPath, database.WithRepoRules(true))
	var siteDb database.Memory
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		misc.Message("repository doesn't contain a database for this site")
		siteDb = database.Memory{}
	} else if err != nil {
		return err
	} else {
		misc.Message("loading site database from repository")
		defer func() { _ = files.Close() }()
		siteDb, err = database.Load(files)
		if err != nil {
			return err
		}
	}

	// Load filters from the repository. If the site filter doesn't exist on the
	// repository, fall back to a local copy for bootstrapping. This makes it
	// possible to bootstrap a new site from the new site rather than pre-creating
	// the filter.
	repoFilter := filter.New()
	repoFilterPath := fileinfo.NewPath(src, repofiles.SiteFilter(repofiles.RepoSite))
	err = repoFilter.ReadFile(repoFilterPath, false)
	if err != nil {
		return fmt.Errorf("reading repository copy of repository filter: %w", err)
	}
	var siteFilterPath *fileinfo.Path
	localFilter := config.LocalFilter
	siteFilter := filter.New()
	for {
		if localFilter {
			siteFilterPath = r.localPath(repofiles.SiteFilter(site))
		} else {
			siteFilterPath = fileinfo.NewPath(src, repofiles.SiteFilter(site))
		}
		err = siteFilter.ReadFile(siteFilterPath, false)
		if errors.As(err, &nsk) || errors.Is(err, fs.ErrNotExist) {
			if localFilter {
				misc.Message("no filter is configured for this site; bootstrapping with exclude all")
				siteFilter.SetDefaultInclude(false)
				break
			} else {
				misc.Message("site filter does not exist on the repository; trying local copy")
				localFilter = true
			}
		} else if err != nil {
			return fmt.Errorf("reading site filter: %w", err)
		} else {
			break
		}
	}
	filters := []*filter.Filter{
		repoFilter,
		siteFilter,
	}

	// Look at differences between the repository's state and the repository's last
	// record of the site's state.
	d := makeDiff(filters)
	diffResult, err := d.Run(siteDb, repoDb)
	if err != nil {
		return err
	}

	if !config.NoOp {
		// Write diff to a local file for reference.
		err = r.SaveDiff(repofiles.Pull, diffResult)
		if err != nil {
			return err
		}
	}

	// Check conflicts
	localSrc := fileinfo.NewLocal(r.localTop)
	err = checkConflicts(diffResult.Check, !config.NoOp, func(path string) (*fileinfo.FileInfo, error) {
		info, err := localSrc.FileInfo(path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return info, nil
	})
	if err != nil {
		return err
	}

	// XXX site tar

	changes := len(diffResult.Change)+len(diffResult.Add)+len(diffResult.Rm)+len(diffResult.MetaChange) > 0
	if changes {
		misc.Message("----- changes to pull -----")
		_ = diffResult.WriteDiff(os.Stdout, false)
		misc.Message("-----")
		if !config.NoOp && !misc.Prompt("Continue?") {
			return fmt.Errorf("exiting")
		}
	} else {
		misc.Message("no changes to pull")
	}

	if config.NoOp {
		return nil
	}

	if changes {
		err = r.applyChangesFromRepo(src, diffResult, siteDb)
		if err != nil {
			return err
		}
		// Push a modified copy of the site database
		localSiteFile := r.localPath(repofiles.TempSiteDb(site))
		err = database.WriteDb(localSiteFile.Path(), siteDb, database.DbQfs)
		if err != nil {
			return err
		}
		err = src.Store(localSiteFile, repofiles.SiteDb(site))
		if err != nil {
			return fmt.Errorf("update site database in repository: %w", err)
		}
		misc.Message("updated repository copy of site database to reflect changes")
	}

	if downloadedRepoDb {
		err = os.Rename(
			r.localPath(repofiles.TempRepoDb()).Path(),
			r.localPath(repofiles.RepoDb()).Path(),
		)
		if err != nil {
			return err
		}
	}

	err = r.localPath(repofiles.Push).Remove()
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return nil
}

func (r *Repo) applyChangesFromRepo(
	src *s3source.S3Source,
	diffResult *diff.Result,
	localDb database.Memory,
) error {
	// Apply changes. Possible enhancement: make sure every directory we have to
	// modify (by adding or removing files) is writable first, and if we change it,
	// change it back. For now, if we try to modify a read-only directory, it will be
	// an error. The user can change the permissions and run again. The pull
	// operation will restore the permissions.

	// Remove what needs to be removed, then add/modify, then apply permission
	// changes. We ignore ownerships, directory modification times, and special
	// files.
	for _, rm := range diffResult.Rm {
		path := r.localPath(rm).Path()
		misc.Message("removing %s", path)
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("remove %s: %w", path, err)
		}
		delete(localDb, rm)
	}

	// Make sure files we are changing will be writable. We will set the correct
	// permissions when we replace them.
	for _, ch := range diffResult.Change {
		path := r.localPath(ch.Path)
		err := os.Chmod(path.Path(), fs.FileMode(ch.Permissions|0o600))
		if err != nil {
			return fmt.Errorf("%s: make writable: %w", path.Path(), err)
		}
	}

	// Concurrently pull changed files from the repository. This sets permissions and modification time.
	c := make(chan *fileinfo.FileInfo, numWorkers)
	var allErrors []error
	go func() {
		for _, info := range diffResult.Add {
			localDb[info.Path] = info
			c <- info
		}
		for _, info := range diffResult.Change {
			localDb[info.Path] = info
			c <- info
		}
		close(c)
	}()
	misc.DoConcurrently(
		func(c chan *fileinfo.FileInfo, errorChan chan error) {
			for info := range c {
				path := r.localPath(info.Path)
				downloaded, err := src.Retrieve(info.Path, path.Path())
				if err != nil {
					errorChan <- fmt.Errorf("retrieve %s: %w", info.Path, err)
				}
				if downloaded && info.FileType != fileinfo.TypeDirectory {
					misc.Message("downloaded %s", info.Path)
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
	for _, m := range diffResult.MetaChange {
		if m.Permissions == nil {
			continue
		}
		path := r.localPath(m.Info.Path).Path()
		misc.Message("chmod %04o %s", *m.Permissions, m.Info.Path)
		err := os.Chmod(path, os.FileMode(*m.Permissions))
		if err != nil {
			return fmt.Errorf("chmod %04o %s: %w", *m.Permissions, path, err)
		}
		localDb[m.Info.Path] = m.Info
	}
	return nil
}

func (r *Repo) loadRepoDb() (bool, database.Memory, error) {
	localPath := r.localPath(repofiles.RepoDb())
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
		misc.Message("local copy of repository database is current")
		toLoad = localPath
	}
	downloaded := false
	if toLoad == nil {
		misc.Message("downloading latest repository database")
		downloaded = true
		pending := r.localPath(repofiles.TempRepoDb())
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
	defer func() { _ = files.Close() }()
	db, err := database.Load(files)
	if err != nil {
		return false, nil, err
	}
	return downloaded, db, nil
}
