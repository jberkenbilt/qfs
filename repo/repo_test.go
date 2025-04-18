package repo_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/gztar"
	"github.com/jberkenbilt/qfs/localsource"
	"github.com/jberkenbilt/qfs/misc"
	"github.com/jberkenbilt/qfs/qfs"
	"github.com/jberkenbilt/qfs/repo"
	"github.com/jberkenbilt/qfs/repofiles"
	"github.com/jberkenbilt/qfs/s3source"
	"github.com/jberkenbilt/qfs/s3test"
	"github.com/jberkenbilt/qfs/testutil"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	TestContainer = "qfs-test-minio"
	TestBucket    = "qfs-test-repo"
)

var testS3 struct {
	s3      *s3test.S3Test
	started bool
}
var s3Client *s3.Client
var ctx = context.Background()

func startMinio() {
	s, err := s3test.New(TestContainer)
	if err != nil {
		panic(err.Error())
	}
	started, err := s.Start()
	if err != nil {
		panic(err.Error())
	}
	if started {
		fmt.Println("Run ./bin/start-minio to speed testing and persist state after test.")
		err = s.Init()
		if err != nil {
			_ = testS3.s3.Stop()
			panic(err.Error())
		}
	}
	testS3.started = started
	testS3.s3 = s
	s3Client = s.Client()
}

func TestMain(m *testing.M) {
	startMinio()
	status := m.Run()
	if testS3.started {
		err := testS3.s3.Stop()
		if err != nil {
			fmt.Printf("WARNING: errors stopping containers: %v", err)
		}
	}
	os.Exit(status)
}

func TestStartMinio(t *testing.T) {
	// This is mainly for coverage. The test container is started by setup/tear-down.
	// This exercises that it is already started.
	s, err := s3test.New(TestContainer)
	testutil.Check(t, err)
	started, err := s.Start()
	testutil.Check(t, err)
	if started {
		t.Errorf("test container should already be running")
	}
}

func deleteTestBucket() {
	i1 := &s3.ListObjectVersionsInput{
		Bucket: aws.String(TestBucket),
	}
	p := s3.NewListObjectVersionsPaginator(s3Client, i1)
	for p.HasMorePages() {
		o1, err := p.NextPage(ctx)
		if err != nil {
			return
		}
		var objects []types.ObjectIdentifier
		for _, ov := range o1.Versions {
			objects = append(objects, types.ObjectIdentifier{
				Key:       ov.Key,
				VersionId: ov.VersionId,
			})
		}
		for _, ov := range o1.DeleteMarkers {
			objects = append(objects, types.ObjectIdentifier{
				Key:       ov.Key,
				VersionId: ov.VersionId,
			})
		}
		if len(objects) > 0 {
			i2 := &s3.DeleteObjectsInput{
				Bucket: aws.String(TestBucket),
				Delete: &types.Delete{
					Objects: objects,
				},
			}
			_, err = s3Client.DeleteObjects(ctx, i2)
			if err != nil {
				panic(err.Error())
			}
		}
	}
	i3 := &s3.DeleteBucketInput{
		Bucket: aws.String(TestBucket),
	}
	_, err := s3Client.DeleteBucket(ctx, i3)
	if err != nil {
		panic("delete: " + err.Error())
	}
}

func setUpTestBucket() {
	deleteTestBucket()
	i1 := &s3.CreateBucketInput{
		Bucket: aws.String(TestBucket),
	}
	_, err := s3Client.CreateBucket(ctx, i1)
	if err != nil {
		panic(err.Error())
	}
	i2 := &s3.PutBucketVersioningInput{
		Bucket: aws.String(TestBucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	}
	_, err = s3Client.PutBucketVersioning(ctx, i2)
	if err != nil {
		panic(err.Error())
	}
}

func writeFile(
	t *testing.T,
	filename string,
	modTime int64,
	permissions uint16,
	contents string,
) {
	if contents == "" {
		contents = filename
	}
	_ = os.Chmod(filename, 0o600)
	testutil.Check(t, os.MkdirAll(filepath.Dir(filename), 0o777))
	testutil.Check(t, os.WriteFile(filename, []byte(contents), 0o666))
	testutil.Check(t, os.Chtimes(filename, time.Time{}, time.UnixMilli(modTime)))
	testutil.Check(t, os.Chmod(filename, fs.FileMode(permissions)))
}

func TestS3Source(t *testing.T) {
	setUpTestBucket()
	tmp := t.TempDir()
	j := func(path string) *fileinfo.Path {
		return fileinfo.NewPath(localsource.New(tmp), path)
	}
	err := gztar.Extract("testdata/files.tar.gz", tmp)
	testutil.Check(t, err)
	makeSrc := func(db database.Database) *s3source.S3Source {
		t.Helper()
		src, err := s3source.New(
			TestBucket,
			"home",
			s3source.WithS3Client(s3Client),
			s3source.WithDatabase(db),
		)
		testutil.Check(t, err)
		return src
	}
	mem1 := database.Database{}
	src := makeSrc(mem1)
	for _, f := range []string{
		".",
		"dir1",
		"dir1/empty-directory",
		"dir1/potato",
		"dir1/salad",
		"file1",
		"file2",
		"file3",
	} {
		err = src.Store(j("files/"+f), f)
		if err != nil {
			t.Fatalf("store: %v", err)
		}
	}

	// Store errors
	err = src.Store(fileinfo.NewPath(localsource.New(""), "/nope"), "nope")
	if err == nil || !strings.Contains(err.Error(), "/nope") {
		t.Errorf("wrong error: %v", err)
	}
	err = src.Store(fileinfo.NewPath(localsource.New(""), "/dev/null"), "nope")
	if err == nil || !strings.Contains(err.Error(), "can only store files") {
		t.Errorf("wrong error: %v", err)
	}

	// Spot check one entry. For the rest, we will rely on repo tests.
	fi := mem1["file1"]
	if fi == nil {
		for k := range mem1 {
			t.Error(k)
		}
		t.Fatalf("file1 not found")
	}
	if fi.ModTime.UnixMilli() != 1714235352000 {
		t.Errorf("wrong modtime: %d", fi.ModTime.UnixMilli())
	}
	if fi.Permissions != 0o644 {
		t.Errorf("wrong permissions %04o", fi.Permissions)
	}
	if fi.Size != 16 {
		t.Errorf("wrong size: %v", fi.Size)
	}

	testutil.Check(t, database.WriteDb(j("qfs-from-s3").Path(), mem1, database.DbQfs))
	testutil.Check(t, database.WriteDb(j("repo-from-s3").Path(), mem1, database.DbRepo))
	stdout, stderr := testutil.WithStdout(func() {
		err = qfs.RunWithArgs([]string{"qfs", "diff", j("qfs-from-s3").Path(), j("repo-from-s3").Path()})
		if err != nil {
			t.Errorf("error from diff: %v", err)
		}
	})
	if len(stderr) > 0 || len(stdout) > 0 {
		t.Errorf("output: %s\n%s", stdout, stderr)
	}
	stdout, stderr = testutil.WithStdout(func() {
		err = qfs.RunWithArgs([]string{"qfs", "diff", j("qfs-from-s3").Path(), j("files").Path()})
		if err != nil {
			t.Errorf("error from diff: %v", err)
		}
	})
	if len(stderr) > 0 || len(stdout) > 0 {
		t.Errorf("output: %s\n%s", stdout, stderr)
	}

	// Traverse again. We should get the same database.
	mem2, _ := src.Database(true, false, nil)
	o1, _ := testutil.WithStdout(func() {
		_ = mem1.Print(true)
	})
	o2, _ := testutil.WithStdout(func() {
		_ = mem2.Print(true)
	})
	if !slices.Equal(o1, o2) {
		t.Errorf("new result doesn't match old result")
	}

	// Change S3 to exercise remaining S3 functions and recheck database. Calling
	// remove mutates mem1.
	src = makeSrc(mem1)
	if _, ok := mem1["file1"]; !ok {
		t.Errorf("wrong precondition")
	}
	testutil.Check(t, src.Remove("file1"))
	// Remove is idempotent, so no error to do it again.
	testutil.Check(t, src.Remove("file1"))
	if _, ok := mem1["file1"]; ok {
		t.Errorf("file1 is still there")
	}
	mem2, _ = src.Database(true, false, nil)
	o1, _ = testutil.WithStdout(func() {
		_ = mem1.Print(true)
	})
	o2, _ = testutil.WithStdout(func() {
		_ = mem2.Print(true)
	})
	if !slices.Equal(o1, o2) {
		t.Errorf("new result doesn't match old result")
	}

	_, err = src.Open("nope")
	if err == nil || !strings.Contains(err.Error(), "s3://qfs-test-repo/home/nope@...:") {
		t.Errorf("wrong error: %v", err)
	}
	rd, err := src.Open("dir1/potato")
	testutil.Check(t, err)
	defer func() { _ = rd.Close() }()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, rd)
	testutil.Check(t, err)
	_ = rd.Close()
	if s := buf.String(); s != "salad\n" {
		t.Errorf("got wrong body: %s", s)
	}

	// Test FileInfo prior to traversal. This is needed to check the repo database before downloading.
	file1, err := fileinfo.NewPath(src, "dir1/potato").FileInfo()
	testutil.Check(t, err)
	dir1, err := fileinfo.NewPath(src, "dir1").FileInfo()
	testutil.Check(t, err)
	src = makeSrc(nil)
	file2, err := fileinfo.NewPath(src, "dir1/potato").FileInfo()
	testutil.Check(t, err)
	dir2, err := fileinfo.NewPath(src, "dir1").FileInfo()
	testutil.Check(t, err)
	if !file1.ModTime.Equal(file2.ModTime) {
		t.Errorf("file metadata is inconsistent")
	}
	if !dir1.ModTime.Equal(dir2.ModTime) {
		t.Errorf("dir metadata is inconsistent")
	}

	// Remove a directory node. Traverse should handle this.
	src = makeSrc(mem1)
	if _, ok := mem1["dir1"]; !ok {
		t.Errorf("wrong precondition")
	}
	testutil.Check(t, src.Remove("dir1"))
	if len(src.ExtraKeys()) > 0 {
		t.Errorf("there are extra keys")
	}
	if _, ok := mem1["dir1"]; ok {
		t.Errorf("still there")
	}
	if _, ok := mem1["dir1/potato"]; !ok {
		t.Errorf("descendents are missing")
	}

	// Exercise retrieval
	srcPath := fileinfo.NewPath(src, "dir1/potato")
	srcInfo, err := srcPath.FileInfo()
	testutil.Check(t, err)
	destPath := fileinfo.NewPath(localsource.New(tmp), "files/dir1/potato")
	if x, err := fileinfo.RequiresCopy(srcInfo, destPath); err != nil {
		t.Fatal(err.Error())
	} else if x {
		t.Errorf("initially requires copy")
	}
	retrieved, err := fileinfo.Retrieve(fileinfo.NewPath(src, "dir1/potato"), destPath)
	if err != nil {
		t.Error(err.Error())
	}
	if retrieved {
		t.Error("shouldn't have retrieved file")
	}
	testutil.Check(t, os.WriteFile(destPath.Path(), []byte("something new"), 0o666))
	retrieved, err = fileinfo.Retrieve(fileinfo.NewPath(src, "dir1/potato"), destPath)
	if err != nil {
		t.Error(err.Error())
	}
	if !retrieved {
		t.Error("didn't retrieve file")
	}
	data, err := os.ReadFile(destPath.Path())
	testutil.Check(t, err)
	if string(data) != "salad\n" {
		t.Errorf("wrong body: %s", data)
	}
	if x, err := fileinfo.RequiresCopy(srcInfo, destPath); err != nil {
		t.Fatal(err.Error())
	} else if x {
		t.Errorf("initially requires copy")
	}

	// Test reading the database from S3
	testutil.Check(t, src.Store(j("repo-from-s3"), "repo-db"))
	s3Path := fileinfo.NewPath(src, "repo-db")
	mem1, err = database.Load(s3Path)
	testutil.Check(t, err)
	mem2, err = database.LoadFile(j("repo-from-s3").Path())
	testutil.Check(t, err)
	if !reflect.DeepEqual(mem1, mem2) {
		t.Errorf("inconsistent results")
	}

	// Exercise pure s3 scan
	qfs.S3Client = s3Client
	defer func() { qfs.S3Client = nil }()
	fileRe := regexp.MustCompile(`^(.+)@(.*?)$`)
	testutil.CheckLinesSorted(
		t,
		func(s string) string {
			m := fileRe.FindStringSubmatch(s)
			if m == nil {
				return s
			}
			return m[1]
		},
		[]string{"qfs", "scan", "s3://" + TestBucket},
		[]string{
			"home/.",
			"home/dir1/empty-directory",
			"home/dir1/potato",
			"home/dir1/salad",
			"home/file2",
			"home/file3",
			"home/repo-db",
		},
	)
}

func TestKeyLogic(t *testing.T) {
	setUpTestBucket()
	input := &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
	}
	for _, k := range []string{
		".@d,1715443064999,0755",
		".@d,1715443064888,0555",
		".@.@f,1715443064888,0775",
		"potato-salad",
		"potato/.@f,1715443000777,0644",
		"potato/a@@b@l,1715443000777,target@@here",
	} {
		input.Key = &k
		_, err := s3Client.PutObject(ctx, input)
		testutil.Check(t, err)
	}
	src, err := s3source.New(
		TestBucket,
		"",
		s3source.WithS3Client(s3Client),
	)
	testutil.Check(t, err)
	db, err := src.Database(false, false, nil)
	testutil.Check(t, err)
	expExtra := []string{
		".@d,1715443064888,0555",   // older
		".@.@f,1715443064888,0775", // invalid name
		"potato-salad",             // invalid name
	}
	expDb := database.Database{
		".": {
			FileType:    fileinfo.TypeDirectory,
			ModTime:     time.UnixMilli(1715443064999),
			Permissions: 0o755,
		},
		"potato/.": {
			FileType:    fileinfo.TypeFile,
			ModTime:     time.UnixMilli(1715443000777),
			Permissions: 0o644,
		},
		"potato/a@b": {
			FileType:    fileinfo.TypeLink,
			ModTime:     time.UnixMilli(1715443000777),
			Permissions: 0o777,
			Special:     "target@here",
		},
	}
	sort.Strings(expExtra)
	extra := misc.SortedKeys(src.ExtraKeys())
	if !slices.Equal(extra, expExtra) {
		t.Errorf("wrong extra: %#v", extra)
	}
	for k, exp := range expDb {
		actual := db[k]
		if actual == nil {
			t.Errorf("missing %s", k)
		} else if !(actual.Path == k &&
			actual.ModTime.Equal(exp.ModTime) &&
			actual.Permissions == exp.Permissions &&
			actual.Special == exp.Special) {
			t.Errorf("wrong db: %v", actual)
		}
	}
	for k := range db {
		if _, ok := expDb[k]; !ok {
			t.Errorf("missing: %s", k)
		}
	}
}

func TestNoClient(t *testing.T) {
	_, err := s3source.New("x", "y")
	if err == nil || !strings.Contains(err.Error(), "an s3 client") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestRepo_IsInitialized(t *testing.T) {
	setUpTestBucket()
	_, err := repo.New(
		repo.WithS3Client(s3Client),
	)
	if err == nil || !strings.Contains(err.Error(), ".qfs/repo") {
		t.Errorf("wrong error: %v", err)
	}
	r, err := repo.New(
		repo.WithLocalTop("testdata/files1"),
		repo.WithS3Client(s3Client),
	)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = r.Init(repo.InitCleanRepo)
	var nsb *types.NoSuchBucket
	if err == nil || !errors.As(err, &nsb) {
		t.Errorf("wrong error: %v", err)
	}
}

func TestMigrate(t *testing.T) {
	defer func() {
		misc.TestPromptChannel = nil
		misc.TestMessageChannel = nil
	}()
	misc.TestPromptChannel = make(chan string, 5)
	cleanupMessages, checkMessages := testutil.CaptureMessages()
	defer cleanupMessages()
	qfs.S3Client = s3Client
	defer func() { qfs.S3Client = nil }()
	setUpTestBucket()
	tmp := t.TempDir()
	j := func(path string) string { return filepath.Join(tmp, path) }
	// Create files locally and copy some of them to the repository area such that
	// the S3 time of some but not all is more recent than the local time.
	now := time.Now().UnixMilli()
	before := int64(1715856724523) // some time in the past
	after := now + 3600000
	writeFile(t, j(".qfs/repo"), now, 0o644, "s3://"+TestBucket+"/repo")
	writeFile(t, j("one/in-sync"), before, 0o644, "")
	writeFile(t, j("two/also-in-sync"), before, 0o444, "")
	writeFile(t, j("one/out-of-date"), after, 0o664, "")
	testutil.Check(t, os.Symlink("one", j("link")))
	input := &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
	}
	for _, path := range []string{"one/in-sync", "one/out-of-date", "two/also-in-sync"} {
		input.Key = aws.String("repo/" + path)
		f, err := os.Open(j(path))
		testutil.Check(t, err)
		input.Body = f
		_, err = s3Client.PutObject(ctx, input)
		_ = f.Close()
		testutil.Check(t, err)
	}

	// Migrate keys we can migrate.
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // Continue?
			_ = qfs.RunWithArgs([]string{"qfs", "init-repo", "--migrate", "--top", tmp})
		},
		`repo/one/in-sync -> repo/one/in-sync@f,1715856724523,0644
repo/two/also-in-sync -> repo/two/also-in-sync@f,1715856724523,0444
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"----- keys to migrate -----",
		"-----",
		"moving repo/one/in-sync -> repo/one/in-sync@f,1715856724523,0644",
		"moving repo/two/also-in-sync -> repo/two/also-in-sync@f,1715856724523,0444",
		"uploading repository database",
	},
	)

	// Do an initial push. This will push everything that was not migrated. This has
	// to be done before init-repo -cleanup so the repo filter will be there.
	writeFile(t, j(".qfs/site"), now, 0o644, "site\n")
	writeFile(t, j(".qfs/filters/repo"), now, 0o644, ":include:\n.\n")
	writeFile(t, j(".qfs/filters/site"), now, 0o644, ":read:repo\n")
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // Continue?
			_ = qfs.RunWithArgs([]string{"qfs", "push", "--top", tmp})
		},
		`mkdir .
mkdir .qfs
add .qfs/filters/repo
add .qfs/filters/site
add link
mkdir one
add one/out-of-date
mkdir two
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"generating local database",
		"no conflicts found",
		"----- changes to push -----",
		"-----",
		"storing .",
		"storing .qfs",
		"storing .qfs/filters/repo",
		"storing .qfs/filters/site",
		"storing link",
		"storing one",
		"storing one/out-of-date",
		"storing two",
		"uploading site database",
		"uploading repository database",
	},
	)

	// Delete keys we couldn't migrate.
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y"
			_ = qfs.RunWithArgs([]string{"qfs", "init-repo", "--clean-repo", "--top", tmp})
		},
		`repo/one/out-of-date
prompt: Remove above keys?
`,
		"",
	)
	misc.TestPromptChannel <- "y" // Continue?
	checkMessages(t, []string{
		"local copy of repository database is current",
		"----- keys to remove -----",
		"-----",
		"uploading repository database",
	},
	)
}

func checkSync(t *testing.T, srcDir, destDir, filter string) {
	t.Helper()
	tmp := t.TempDir()
	j := func(path string) string {
		return filepath.Join(tmp, path)
	}
	// Create a file so diff output won't be empty
	marker := filepath.Join(destDir, "z")
	writeFile(t, marker, time.Now().UnixMilli(), 0644, "")
	defer func() { _ = os.Remove(marker) }()
	testutil.Check(t, qfs.RunWithArgs([]string{"qfs", "scan", srcDir, "--filter", filter, "--db", j("src-db")}))
	testutil.Check(t, qfs.RunWithArgs([]string{"qfs", "scan", destDir, "--db", j("dest-db")}))
	testutil.ExpStdout(
		t,
		func() {
			err := qfs.RunWithArgs([]string{"qfs", "diff", j("src-db"), j("dest-db")})
			if err != nil {
				t.Error(err.Error())
			}
		},
		"add z\n",
		"",
	)
}

func TestLifecycle(t *testing.T) {
	defer func() {
		misc.TestPromptChannel = nil
		misc.TestMessageChannel = nil
	}()
	setUpTestBucket()
	tmp := t.TempDir()
	j := func(path string) string { return filepath.Join(tmp, path) }

	// This very long test exercises a series of sites and a repository through a
	// lifecycle of supported operations. As such, later parts of the test will
	// depend on earlier parts in a fairly tight way, but this is an acceptable price
	// to pay since it is essential to exercise that qfs works properly over a long
	// series of transactions.

	cleanupMessages, checkMessages := testutil.CaptureMessages()
	defer cleanupMessages()

	// Create a directory for a site.
	testutil.Check(t, os.MkdirAll(j("site1/"+repofiles.Top), 0o777))

	// Attempt to initialize without a repository configuration.
	err := qfs.RunWithArgs([]string{"qfs", "init-repo", "--top", j("site1")})
	if err == nil || !strings.Contains(err.Error(), "/site1/.qfs/repo:") {
		t.Errorf("expected no repo config: %v", err)
	}

	writeFile(t, j("site1/"+repofiles.RepoConfig), time.Now().UnixMilli(), 0o644, "invalid contents")
	err = qfs.RunWithArgs([]string{"qfs", "init-repo", "--top", j("site1")})
	if err == nil || !strings.Contains(err.Error(), "must contain s3://bucket/prefix") {
		t.Errorf("expected no repo config: %v", err)
	}

	// Initialize a repository normally

	// No newline on repo file
	writeFile(t, j("site1/"+repofiles.RepoConfig), time.Now().UnixMilli(), 0o644, "s3://"+TestBucket+"/home")
	qfs.S3Client = s3Client
	defer func() { qfs.S3Client = nil }()
	err = qfs.RunWithArgs([]string{"qfs", "init-repo", "--top", j("site1")})
	if err != nil {
		t.Errorf("init: %v", err)
	}
	checkMessages(t, []string{"uploading repository database"})

	// Re-initialize and abort
	misc.TestPromptChannel = make(chan string, 5)
	misc.TestPromptChannel <- "n"
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "init-repo", "--top", j("site1")})
			if err == nil || !strings.Contains(err.Error(), "already initialized") {
				t.Errorf("wrong error: %v", err)
			}
		},
		"prompt: Repository is already initialized. Rebuild database?\n",
		"",
	)
	checkMessages(t, []string{"local copy of repository database is current"})

	// Re-initialize
	misc.TestPromptChannel <- "y"
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "init-repo", "--top", j("site1")})
			if err != nil {
				t.Errorf("error: %v", err)
			}
		},
		"prompt: Repository is already initialized. Rebuild database?\n",
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"uploading repository database",
	})

	// Do the initial push without initializing site
	err = qfs.RunWithArgs([]string{"qfs", "push", "--top", j("site1")})
	if err == nil || !strings.Contains(err.Error(), "site1/.qfs/site:") {
		t.Errorf("wrong error: %v", err)
	}

	// Set up the site, and do the initial push. We'll explicitly assign timestamps
	// relative to the current time so we can force them to be updated at various
	// points.
	start := time.Now().Add(-24 * time.Hour).UnixMilli()
	// No newline on site file
	writeFile(t, j("site1/.qfs/site"), start, 0o644, "site1")
	writeFile(t, j("site1/.qfs/filters/prune"), start, 0o644, `
:prune:
junk
:junk:(~|\.junk)$
`)
	writeFile(t, j("site1/.qfs/filters/repo"), start, 0o644, `
:read:prune
:exclude:
*/no-sync
:include:
dir1
dir2
dir3
dir4
excluded/included
*/always
`)
	writeFile(t, j("site1/.qfs/filters/site1"), start, 0o644, `
:read:prune
:include:
dir1
dir2
# dir3 is only on site1
dir3
excluded/included
*/always
`)
	// Create various files and directories for upcoming tests. Passing empty string
	// as last argument writes the file's path as its contents so every file is
	// different.
	writeFile(t, j("site1/dir1/change-in-site1"), start, 0o644, "")
	writeFile(t, j("site1/dir1/file-to-change-and-chmod"), start+1111, 0o664, "")
	writeFile(t, j("site1/dir1/ro-file-to-change"), start+2222, 0o444, "")
	writeFile(t, j("site1/dir1/file-then-dir"), start+3000, 0o644, "")
	writeFile(t, j("site1/dir1/file-then-link"), start+4000, 0o644, "")
	writeFile(t, j("site1/dir1/file-to-chmod"), start+5000, 0o644, "")
	writeFile(t, j("site1/dir1/file-to-remove"), start+6000, 0o444, "")
	writeFile(t, j("site1/dir3/only-in-site1"), start+7000, 0o644, "")
	writeFile(t, j("site1/junk/ignored"), start, 0o644, "")
	writeFile(t, j("site1/dir/a.junk"), start, 0o644, "")
	// excluded/included is included by base, so excluded gets included without descendants
	writeFile(t, j("site1/excluded/included/here"), start, 0o644, "")
	writeFile(t, j("site1/excluded/skipped"), start, 0o644, "")
	// other/always/* is included by base. It would be nice if `other` got included
	// without descendants, but it doesn't. See comments in filter.go around
	// fullPath.
	writeFile(t, j("site1/other/always/here"), start, 0o644, "")
	writeFile(t, j("site1/other/skipped"), start, 0o644, "")
	testutil.Check(t, os.Mkdir(j("site1/dir2"), 0o755))
	testutil.Check(t, os.Symlink("../dir1/change-in-site1", j("site1/dir2/link-to-change")))
	testutil.Check(t, os.Symlink("replace-with-file", j("site1/dir2/link-then-file")))
	testutil.Check(t, os.Symlink("replace-with-dir", j("site1/dir2/link-then-directory")))
	testutil.Check(t, os.Symlink("/dev/null", j("site1/dir2/link-to-remove")))
	testutil.Check(t, os.Mkdir(j("site1/dir2/dir-then-file"), 0o755))
	testutil.Check(t, os.Mkdir(j("site1/dir2/dir-then-link"), 0o755))
	testutil.Check(t, os.Mkdir(j("site1/dir2/dir-to-chmod"), 0o775))
	testutil.Check(t, os.Mkdir(j("site1/dir2/dir-to-remove"), 0o555))
	// Make sure weird files that look like how the repository encodes things don't confuse things.
	testutil.Check(t, os.Symlink(
		"looks-like-repo@f,1715443064543,0555",
		j("site1/dir1/looks-like-repo@l,1715443064543,0777"),
	))

	// Make sure some time passes for versioning.
	time.Sleep(10 * time.Millisecond)
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // Continue?
			err = qfs.RunWithArgs([]string{"qfs", "push", "--cleanup", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		// This is lexically sorted, since diff output is sorted, based on the things
		// created above.
		`mkdir .
mkdir .qfs
add .qfs/filters/prune
add .qfs/filters/repo
add .qfs/filters/site1
mkdir dir1
add dir1/change-in-site1
add dir1/file-then-dir
add dir1/file-then-link
add dir1/file-to-change-and-chmod
add dir1/file-to-chmod
add dir1/file-to-remove
add dir1/looks-like-repo@l,1715443064543,0777
add dir1/ro-file-to-change
mkdir dir2
mkdir dir2/dir-then-file
mkdir dir2/dir-then-link
mkdir dir2/dir-to-chmod
mkdir dir2/dir-to-remove
add dir2/link-then-directory
add dir2/link-then-file
add dir2/link-to-change
add dir2/link-to-remove
mkdir dir3
add dir3/only-in-site1
mkdir excluded
mkdir excluded/included
add excluded/included/here
mkdir other/always
add other/always/here
prompt: Continue?
`,
		filepath.Base(os.Args[0])+": removing dir/a.junk\n",
	)
	// Lines in checkMessages are in a non-deterministic order, but checkMessages
	// sorts expected and actual.
	checkMessages(t, []string{
		"generating local database",
		"local copy of repository database is current",
		"no conflicts found",
		"----- changes to push -----",
		// diff is written to stdout
		"-----",
		"storing .",
		"storing .qfs",
		"storing .qfs/filters/prune",
		"storing .qfs/filters/repo",
		"storing .qfs/filters/site1",
		"storing dir1",
		"storing dir1/change-in-site1",
		"storing dir1/file-then-dir",
		"storing dir1/file-then-link",
		"storing dir1/file-to-change-and-chmod",
		"storing dir1/file-to-chmod",
		"storing dir1/file-to-remove",
		"storing dir1/ro-file-to-change",
		"storing dir1/looks-like-repo@l,1715443064543,0777",
		"storing dir2/link-then-directory",
		"storing dir2/link-then-file",
		"storing dir2/link-to-change",
		"storing dir2/link-to-remove",
		"storing dir2",
		"storing dir2/dir-then-file",
		"storing dir2/dir-then-link",
		"storing dir2/dir-to-chmod",
		"storing dir2/dir-to-remove",
		"storing dir3",
		"storing dir3/only-in-site1",
		"storing excluded",
		"storing excluded/included",
		"storing excluded/included/here",
		"storing other/always",
		"storing other/always/here",
		"uploading repository database",
		"uploading site database",
	})

	// Do another push right away. There should be no changes. Do this with the local
	// database changed so it gets back in sync.
	testutil.Check(t, os.Chtimes(j("site1/.qfs/db/repo"), time.Time{}, time.Now()))
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "push", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"downloading latest repository database",
		"no conflicts found",
		"no changes to push",
		"updating local copy of repository database",
		"uploading site database",
	})

	testutil.Check(t, os.MkdirAll(j("sync"), 0777))
	err = qfs.RunWithArgs([]string{
		"qfs",
		"sync",
		"-n",
		"--filter",
		j("site1/.qfs/filters/site1"),
		j("site1"),
		j("sync"),
	})
	if err == nil || !strings.Contains(err.Error(), "sync doesn't work with filters that have") {
		t.Errorf("wrong error: %v", err)
	}

	// Change file on site1 without pushing -- will be pushed later.
	writeFile(t, j("site1/dir1/change-in-site1"), start, 0o644, "")

	// Pull after push -- no changes expected since the above file is unknown to the repository.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"loading site database from repository",
		"no conflicts found",
		"no changes to pull",
	})

	// Site up a second site, and do a series of pull. For the first pull, there is no filter in the
	// repository, so we just get filters. This time, write newlines in the repo and site files.
	writeFile(t, j("site2/.qfs/repo"), start, 0o644, "s3://"+TestBucket+"/home\n")
	writeFile(t, j("site2/.qfs/site"), start, 0o644, "site2\n")
	// Run a pull but answer no to the continue prompt.
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "n" // Continue?
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site2")})
			if err == nil || err.Error() != "exiting" {
				t.Errorf("%v", err)
			}
		},
		`mkdir .qfs
add .qfs/filters/prune
add .qfs/filters/repo
add .qfs/filters/site1
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"downloading latest repository database",
		"repository doesn't contain a database for this site",
		"site filter does not exist on the repository; trying local copy",
		"no filter is configured for this site; bootstrapping with exclude all",
		"no conflicts found",
		"----- changes to pull -----",
		"-----",
	})
	// Now do the pull.
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // Continue?
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`mkdir .qfs
add .qfs/filters/prune
add .qfs/filters/repo
add .qfs/filters/site1
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"downloading latest repository database",
		"repository doesn't contain a database for this site",
		"site filter does not exist on the repository; trying local copy",
		"no filter is configured for this site; bootstrapping with exclude all",
		"no conflicts found",
		"----- changes to pull -----",
		"-----",
		"copied .qfs/filters/repo",
		"copied .qfs/filters/site1",
		"copied .qfs/filters/prune",
		"updated repository copy of site database to reflect changes",
	})

	// Pull with no repo filter, so local filter is used
	writeFile(t, j("site2/.qfs/filters/site2"), start, 0o644, `
:read:prune
:include:
dir1
dir2
# dir4 is only site2
dir4
`)
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // continue
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site2")})
			if err != nil {
				t.Errorf("pull: %v", err)
			}
		},
		`mkdir .
mkdir dir1
add dir1/change-in-site1
add dir1/file-then-dir
add dir1/file-then-link
add dir1/file-to-change-and-chmod
add dir1/file-to-chmod
add dir1/file-to-remove
add dir1/looks-like-repo@l,1715443064543,0777
add dir1/ro-file-to-change
mkdir dir2
mkdir dir2/dir-then-file
mkdir dir2/dir-then-link
mkdir dir2/dir-to-chmod
mkdir dir2/dir-to-remove
add dir2/link-then-directory
add dir2/link-then-file
add dir2/link-to-change
add dir2/link-to-remove
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"loading site database from repository",
		"site filter does not exist on the repository; trying local copy",
		"no conflicts found",
		"----- changes to pull -----",
		"-----",
		"copied dir1/change-in-site1",
		"copied dir1/file-then-dir",
		"copied dir1/file-then-link",
		"copied dir1/file-to-change-and-chmod",
		"copied dir1/file-to-chmod",
		"copied dir1/file-to-remove",
		"copied dir1/ro-file-to-change",
		"copied dir1/looks-like-repo@l,1715443064543,0777",
		"copied dir2/link-then-directory",
		"copied dir2/link-then-file",
		"copied dir2/link-to-change",
		"copied dir2/link-to-remove",
		"updated repository copy of site database to reflect changes",
	})

	// Pull again: no changes.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"loading site database from repository",
		"site filter does not exist on the repository; trying local copy",
		"no conflicts found",
		"no changes to pull",
	})

	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.RunWithArgs([]string{
				"qfs",
				"sync",
				"--no-op",
				"--filter",
				j("site2/.qfs/filters/site2"),
				j("site2"),
				j("sync"),
			})
		},
		`mkdir dir1
add dir1/change-in-site1
add dir1/file-then-dir
add dir1/file-then-link
add dir1/file-to-change-and-chmod
add dir1/file-to-chmod
add dir1/file-to-remove
add dir1/looks-like-repo@l,1715443064543,0777
add dir1/ro-file-to-change
mkdir dir2
mkdir dir2/dir-then-file
mkdir dir2/dir-then-link
mkdir dir2/dir-to-chmod
mkdir dir2/dir-to-remove
add dir2/link-then-directory
add dir2/link-then-file
add dir2/link-to-change
add dir2/link-to-remove
`,
		"",
	)
	checkMessages(t, nil)
	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.RunWithArgs([]string{
				"qfs",
				"sync",
				"--filter",
				j("site2/.qfs/filters/site2"),
				j("site2"),
				j("sync"),
			})
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"copied dir1/change-in-site1",
		"copied dir1/file-then-dir",
		"copied dir1/file-then-link",
		"copied dir1/file-to-change-and-chmod",
		"copied dir1/file-to-chmod",
		"copied dir1/file-to-remove",
		"copied dir1/looks-like-repo@l,1715443064543,0777",
		"copied dir1/ro-file-to-change",
		"copied dir2/link-then-directory",
		"copied dir2/link-then-file",
		"copied dir2/link-to-change",
		"copied dir2/link-to-remove",
	})
	checkSync(t, j("site2"), j("sync"), j("site2/.qfs/filters/site2"))

	// Make a second copy. We will use this later to test get.
	testutil.Check(t, os.MkdirAll(j("sync2"), 0777))
	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.RunWithArgs([]string{
				"qfs",
				"sync",
				"--filter",
				j("site2/.qfs/filters/site2"),
				j("site2"),
				j("sync2"),
			})
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"copied dir1/change-in-site1",
		"copied dir1/file-then-dir",
		"copied dir1/file-then-link",
		"copied dir1/file-to-change-and-chmod",
		"copied dir1/file-to-chmod",
		"copied dir1/file-to-remove",
		"copied dir1/looks-like-repo@l,1715443064543,0777",
		"copied dir1/ro-file-to-change",
		"copied dir2/link-then-directory",
		"copied dir2/link-then-file",
		"copied dir2/link-to-change",
		"copied dir2/link-to-remove",
	})
	checkSync(t, j("site2"), j("sync2"), j("site2/.qfs/filters/site2"))
	lvOut1, _ := testutil.WithStdout(
		func() {
			testutil.Check(t, qfs.RunWithArgs([]string{
				"qfs",
				"list-versions",
				"--top",
				j("site2"),
				".qfs/db/repo",
			}))
		},
	)
	timeRe := regexp.MustCompile(`[.\n]*(\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}\.\d{3}) f`)
	m := timeRe.FindSubmatch(lvOut1)
	if m == nil {
		t.Fatalf("can't find time in %s", lvOut1)
	}
	pushTime1 := string(m[1])

	// Check also with push-times.
	ptOut, _ := testutil.WithStdout(func() {
		testutil.Check(t, qfs.RunWithArgs([]string{
			"qfs",
			"push-times",
			"--top",
			j("site1"),
		}))
	})
	ptLines := strings.Split(string(ptOut), "\n")
	if len(ptLines) == 0 {
		t.Fatalf("didn't get output from push-times")
	}
	if ptLines[0] != pushTime1 {
		t.Errorf("got wrong first push time: %s", ptLines[0])
	}

	// Save the output of a listing at this time.
	lvOut1, _ = testutil.WithStdout(
		func() {
			testutil.Check(t, qfs.RunWithArgs([]string{
				"qfs",
				"list-versions",
				"--top",
				j("site2"),
				"dir1",
			}))
		},
	)
	lvOutLong1, _ := testutil.WithStdout(
		func() {
			testutil.Check(t, qfs.RunWithArgs([]string{
				"qfs",
				"list-versions",
				"--top",
				j("site2"),
				"dir1",
				"--long",
			}))
		},
	)
	if !strings.Contains(string(lvOut1), "dir1/file-to-change") {
		t.Errorf("didn't find expected contents: %s", lvOut1)
	}
	if !strings.Contains(string(lvOutLong1), "dir1/file-to-change") {
		t.Errorf("didn't find expected contents: %s", lvOut1)
	}
	if string(lvOut1) == string(lvOutLong1) {
		t.Errorf("regular and long outputs are the same")
	}

	// In the next stage of testing, exercise that changes are carried across.
	// Advance the timestamps so changes are detected in case the test suite runs
	// faster than qfs or s3 timestamp granularity. Then make changes relative to the
	// way things were originally set up. We are making changes in site2 to things
	// that were originally created in site1.
	start += 3600000
	writeFile(t, j("site2/dir1/file-to-change-and-chmod"), start, 0o444, "new contents")
	writeFile(t, j("site2/dir1/ro-file-to-change"), start, 0o444, "new ro contents")
	writeFile(t, j("site2/dir4/only-site-2"), start, 0o444, "")
	testutil.Check(t, os.Remove(j("site2/dir1/file-then-dir")))
	testutil.Check(t, os.Mkdir(j("site2/dir1/file-then-dir"), 0o755))
	testutil.Check(t, os.Remove(j("site2/dir1/file-then-link")))
	testutil.Check(t, os.Symlink("now a symlink", j("site2/dir1/file-then-link")))
	testutil.Check(t, os.Chmod(j("site2/dir1/file-to-chmod"), 0o600))
	testutil.Check(t, os.Remove(j("site2/dir1/file-to-remove")))
	testutil.Check(t, os.Remove(j("site2/dir2/link-to-change")))
	testutil.Check(t, os.Symlink("new-target", j("site2/dir2/link-to-change")))
	testutil.Check(t, os.Remove(j("site2/dir2/link-then-file")))
	writeFile(t, j("site2/dir2/link-then-file"), start, 0o644, "this is a file now")
	testutil.Check(t, os.Remove(j("site2/dir2/link-then-directory")))
	testutil.Check(t, os.Mkdir(j("site2/dir2/link-then-directory"), 0o755))
	testutil.Check(t, os.Remove(j("site2/dir2/link-to-remove")))
	testutil.Check(t, os.Remove(j("site2/dir2/dir-then-file")))
	writeFile(t, j("site2/dir2/dir-then-file"), start, 0o644, "this is a file now")
	testutil.Check(t, os.Remove(j("site2/dir2/dir-then-link")))
	testutil.Check(t, os.Symlink("link-now", j("site2/dir2/dir-then-link")))
	testutil.Check(t, os.Chmod(j("site2/dir2/dir-to-chmod"), 0o750))
	testutil.Check(t, os.Remove(j("site2/dir2/dir-to-remove")))
	writeFile(t, j("site2/dir2/new-file"), start, 0o644, "")
	testutil.Check(t, os.Symlink("new-target", j("site2/dir2/new-link")))
	testutil.Check(t, os.Mkdir(j("site2/dir2/new-directory"), 0777))

	// Push -n
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "push", "-n", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`typechange dir1/file-then-dir
typechange dir1/file-then-link
typechange dir2/dir-then-file
typechange dir2/dir-then-link
typechange dir2/link-then-directory
typechange dir2/link-then-file
rm dir1/file-then-dir
rm dir1/file-then-link
rm dir1/file-to-remove
rm dir2/dir-then-file
rm dir2/dir-then-link
rm dir2/dir-to-remove
rm dir2/link-then-directory
rm dir2/link-then-file
rm dir2/link-to-remove
add .qfs/filters/site2
mkdir dir1/file-then-dir
add dir1/file-then-link
add dir2/dir-then-file
add dir2/dir-then-link
mkdir dir2/link-then-directory
add dir2/link-then-file
mkdir dir2/new-directory
add dir2/new-file
add dir2/new-link
mkdir dir4
add dir4/only-site-2
change dir1/file-to-change-and-chmod
change dir1/ro-file-to-change
change dir2/link-to-change
chmod 0600 dir1/file-to-chmod
chmod 0750 dir2/dir-to-chmod
`,
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"local copy of repository database is current",
		"no conflicts found",
		"----- changes to push -----",
		// diff is written to stdout
		"-----",
	})

	// Push
	oldBatchSize := s3source.DeleteBatchSize
	s3source.DeleteBatchSize = 3 // Exercise deleting in multiple batches
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // continue
			err = qfs.RunWithArgs([]string{"qfs", "push", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`typechange dir1/file-then-dir
typechange dir1/file-then-link
typechange dir2/dir-then-file
typechange dir2/dir-then-link
typechange dir2/link-then-directory
typechange dir2/link-then-file
rm dir1/file-then-dir
rm dir1/file-then-link
rm dir1/file-to-remove
rm dir2/dir-then-file
rm dir2/dir-then-link
rm dir2/dir-to-remove
rm dir2/link-then-directory
rm dir2/link-then-file
rm dir2/link-to-remove
add .qfs/filters/site2
mkdir dir1/file-then-dir
add dir1/file-then-link
add dir2/dir-then-file
add dir2/dir-then-link
mkdir dir2/link-then-directory
add dir2/link-then-file
mkdir dir2/new-directory
add dir2/new-file
add dir2/new-link
mkdir dir4
add dir4/only-site-2
change dir1/file-to-change-and-chmod
change dir1/ro-file-to-change
change dir2/link-to-change
chmod 0600 dir1/file-to-chmod
chmod 0750 dir2/dir-to-chmod
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"local copy of repository database is current",
		"no conflicts found",
		"----- changes to push -----",
		// diff is written to stdout
		"-----",
		"removing dir1/file-then-dir",
		"removing dir1/file-then-link",
		"removing dir1/file-to-remove",
		"removing dir2/dir-then-file",
		"removing dir2/dir-then-link",
		"removing dir2/dir-to-remove",
		"removing dir2/link-then-directory",
		"removing dir2/link-then-file",
		"removing dir2/link-to-remove",
		"storing .qfs/filters/site2",
		"storing dir1/file-then-dir",
		"storing dir1/file-then-link",
		"storing dir2/dir-then-file",
		"storing dir2/dir-then-link",
		"storing dir2/link-then-directory",
		"storing dir2/link-then-file",
		"storing dir2/new-directory",
		"storing dir2/new-file",
		"storing dir2/new-link",
		"storing dir1/file-to-change-and-chmod",
		"storing dir1/ro-file-to-change",
		"storing dir2/link-to-change",
		"storing dir1/file-to-chmod",
		"storing dir2/dir-to-chmod",
		"storing dir4",
		"storing dir4/only-site-2",
		"uploading repository database",
		"uploading site database",
	})
	s3source.DeleteBatchSize = oldBatchSize

	// Change file on site1 but don't push yet
	start += 3600000
	writeFile(t, j("site1/dir1/change-in-site1"), start, 0o644, "change in site 1")

	// Exercise repo scan
	splitFields := func(s string) string {
		fields := strings.Split(s, " ")
		if len(fields) >= 6 {
			return fields[1] + " " + fields[5]
		}
		return s
	}
	testutil.CheckLinesSorted(
		t,
		splitFields,
		[]string{"qfs", "scan", "repo:", "--top", j("site1")},
		[]string{
			"d .",
			"d .qfs",
			"d dir1",
			"d dir1/file-then-dir",
			"d dir2",
			"d dir2/dir-to-chmod",
			"d dir2/link-then-directory",
			"d dir2/new-directory",
			"d dir3",
			"d dir4",
			"d excluded",
			"d excluded/included",
			"d other/always",
			"f .qfs/db/repo",
			"f .qfs/db/site1",
			"f .qfs/db/site2",
			"f .qfs/filters/prune",
			"f .qfs/filters/repo",
			"f .qfs/filters/site1",
			"f .qfs/filters/site2",
			"f dir1/change-in-site1",
			"f dir1/file-to-change-and-chmod",
			"f dir1/file-to-chmod",
			"f dir1/ro-file-to-change",
			"f dir2/dir-then-file",
			"f dir2/link-then-file",
			"f dir2/new-file",
			"f dir3/only-in-site1",
			"f dir4/only-site-2",
			"f excluded/included/here",
			"f other/always/here",
			"l dir1/file-then-link",
			"l dir1/looks-like-repo@l,1715443064543,0777",
			"l dir2/dir-then-link",
			"l dir2/link-to-change",
			"l dir2/new-link",
		},
	)
	// Exercise repo site database scan
	testutil.CheckLinesSorted(
		t,
		splitFields,
		[]string{"qfs", "scan", "repo:site2", "--top", j("site2")},
		[]string{
			"d .",
			"d .qfs",
			"d dir1",
			"d dir1/file-then-dir",
			"d dir2",
			"d dir2/dir-to-chmod",
			"d dir2/link-then-directory",
			"d dir2/new-directory",
			"d dir4",
			"f .qfs/filters/prune",
			"f .qfs/filters/repo",
			"f .qfs/filters/site1",
			"f .qfs/filters/site2",
			"f dir1/change-in-site1",
			"f dir1/file-to-change-and-chmod",
			"f dir1/file-to-chmod",
			"f dir1/ro-file-to-change",
			"f dir2/dir-then-file",
			"f dir2/link-then-file",
			"f dir2/new-file",
			"f dir4/only-site-2",
			"l dir1/file-then-link",
			"l dir1/looks-like-repo@l,1715443064543,0777",
			"l dir2/dir-then-link",
			"l dir2/link-to-change",
			"l dir2/new-link",
		},
	)

	// Pull changes to site1. The file that was changed in site1 is ignored because
	// we haven't pushed it yet.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "pull", "-n", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`typechange dir1/file-then-dir
typechange dir1/file-then-link
typechange dir2/dir-then-file
typechange dir2/dir-then-link
typechange dir2/link-then-directory
typechange dir2/link-then-file
rm dir1/file-then-dir
rm dir1/file-then-link
rm dir1/file-to-remove
rm dir2/dir-then-file
rm dir2/dir-then-link
rm dir2/dir-to-remove
rm dir2/link-then-directory
rm dir2/link-then-file
rm dir2/link-to-remove
add .qfs/filters/site2
mkdir dir1/file-then-dir
add dir1/file-then-link
add dir2/dir-then-file
add dir2/dir-then-link
mkdir dir2/link-then-directory
add dir2/link-then-file
mkdir dir2/new-directory
add dir2/new-file
add dir2/new-link
change dir1/file-to-change-and-chmod
change dir1/ro-file-to-change
change dir2/link-to-change
chmod 0600 dir1/file-to-chmod
chmod 0750 dir2/dir-to-chmod
`,
		"",
	)
	checkMessages(t, []string{
		"downloading latest repository database",
		"loading site database from repository",
		"no conflicts found",
		"----- changes to pull -----",
		// diff is written to stdout
		"-----",
	})

	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // continue
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`typechange dir1/file-then-dir
typechange dir1/file-then-link
typechange dir2/dir-then-file
typechange dir2/dir-then-link
typechange dir2/link-then-directory
typechange dir2/link-then-file
rm dir1/file-then-dir
rm dir1/file-then-link
rm dir1/file-to-remove
rm dir2/dir-then-file
rm dir2/dir-then-link
rm dir2/dir-to-remove
rm dir2/link-then-directory
rm dir2/link-then-file
rm dir2/link-to-remove
add .qfs/filters/site2
mkdir dir1/file-then-dir
add dir1/file-then-link
add dir2/dir-then-file
add dir2/dir-then-link
mkdir dir2/link-then-directory
add dir2/link-then-file
mkdir dir2/new-directory
add dir2/new-file
add dir2/new-link
change dir1/file-to-change-and-chmod
change dir1/ro-file-to-change
change dir2/link-to-change
chmod 0600 dir1/file-to-chmod
chmod 0750 dir2/dir-to-chmod
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"downloading latest repository database",
		"loading site database from repository",
		"no conflicts found",
		"----- changes to pull -----",
		// diff is written to stdout
		"-----",
		"removing dir1/file-then-dir",
		"removing dir1/file-then-link",
		"removing dir1/file-to-remove",
		"removing dir2/dir-then-file",
		"removing dir2/dir-then-link",
		"removing dir2/dir-to-remove",
		"removing dir2/link-then-directory",
		"removing dir2/link-then-file",
		"removing dir2/link-to-remove",
		"copied .qfs/filters/site2",
		"copied dir1/file-then-link",
		"copied dir2/dir-then-file",
		"copied dir2/dir-then-link",
		"copied dir2/link-then-file",
		"copied dir2/new-file",
		"copied dir2/new-link",
		"copied dir1/file-to-change-and-chmod",
		"copied dir1/ro-file-to-change",
		"copied dir2/link-to-change",
		"chmod 0600 dir1/file-to-chmod",
		"chmod 0750 dir2/dir-to-chmod",
		"updated repository copy of site database to reflect changes",
	})

	// The sites should be in sync except the file we changed in site1 and haven't pushed.
	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.RunWithArgs([]string{
				"qfs",
				"diff",
				j("site1"),
				j("site2"),
				"--filter",
				j("site1/.qfs/filters/site1"),
				"--filter",
				j("site1/.qfs/filters/site2"),
			})
		},
		"change dir1/change-in-site1\n",
		"",
	)

	// Change the site filter for site2 to include previously excluded dir3. A
	// regular pull doesn't pull it, but a pull with -local-filter does.
	testutil.Check(t, os.Rename(j("site2/.qfs/filters/site2"), j("site2/.qfs/filters/site2.off")))
	writeFile(t, j("site2/.qfs/filters/site2"), start, 0o644, `
:read:prune
:include:
dir1
dir2
dir3
`)

	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"loading site database from repository",
		"no conflicts found",
		"no changes to pull",
	})
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "pull", "-n", "--top", j("site2"), "--local-filter"})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`mkdir dir3
add dir3/only-in-site1
`,
		"",
	)
	checkMessages(t, []string{
		"loading site database from repository",
		"local copy of repository database is current",
		"no conflicts found",
		"----- changes to pull -----",
		// diff is written to stdout
		"-----",
	})
	// Set things back as they were
	testutil.Check(t, os.Rename(j("site2/.qfs/filters/site2.off"), j("site2/.qfs/filters/site2")))
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "pull", "-n", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"loading site database from repository",
		"no conflicts found",
		"no changes to pull",
	})

	// Test conflict detection.

	start += 3600000
	// This file was previously changed on site1. Also change in site2 to create a conflict
	writeFile(t, j("site2/dir1/change-in-site1"), start, 0o644, "")
	// Change a file in site1 and then replace the file with a directory on site2.
	writeFile(t, j("site1/dir2/dir-then-file"), start, 0o644, "modified on site1")
	// Modify a file on site1; change it (back) to a directory on site2
	testutil.Check(t, os.Remove(j("site2/dir2/dir-then-file")))
	testutil.Check(t, os.Mkdir(j("site2/dir2/dir-then-file"), 0755))

	// Both sites can push to the repo and not see conflicts since the repo hasn't
	// been updated with either site's changes. Start by doing a push -n from site1
	// to show that there are no conflicts. Then answer no, and finally yes to the
	// continue prompt for a full push from site2.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "push", "-n", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`change dir1/change-in-site1
change dir2/dir-then-file
`,
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"local copy of repository database is current",
		"no conflicts found",
		"----- changes to push -----",
		// diff is written to stdout
		"-----",
	})
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "n" // continue
			err = qfs.RunWithArgs([]string{"qfs", "push", "--top", j("site1")})
			if err == nil || err.Error() != "exiting" {
				t.Errorf("%v", err)
			}
		},
		`change dir1/change-in-site1
change dir2/dir-then-file
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"local copy of repository database is current",
		"no conflicts found",
		"----- changes to push -----",
		// diff is written to stdout
		"-----",
	})
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // continue
			err = qfs.RunWithArgs([]string{"qfs", "push", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`typechange dir2/dir-then-file
rm dir2/dir-then-file
mkdir dir2/dir-then-file
change dir1/change-in-site1
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"local copy of repository database is current",
		"no conflicts found",
		"----- changes to push -----",
		// diff is written to stdout
		"-----",
		"removing dir2/dir-then-file",
		"storing dir2/dir-then-file",
		"storing dir1/change-in-site1",
		"uploading repository database",
		"uploading site database",
	})

	// Now either a push from site1 or a pull to site1 will show conflicts.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "push", "-n", "--top", j("site1")})
			if err == nil || err.Error() != "conflicts detected" {
				t.Errorf("%v", err)
			}
		},
		`conflict: dir1/change-in-site1
conflict: dir2/dir-then-file
`,
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"downloading latest repository database",
	})
	// Pull but exit on conflicts prompt
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // conflicts detected
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site1")})
			if err == nil || err.Error() != "conflicts detected" {
				t.Errorf("%v", err)
			}
		},
		`conflict: dir1/change-in-site1
conflict: dir2/dir-then-file
prompt: Conflicts detected. Exit?
`,
		"",
	)
	checkMessages(t, []string{
		"loading site database from repository",
		"downloading latest repository database",
	})
	// Resolve letting repository win
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "n" // conflicts detected
			misc.TestPromptChannel <- "y" // continue
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`conflict: dir1/change-in-site1
conflict: dir2/dir-then-file
prompt: Conflicts detected. Exit?
typechange dir2/dir-then-file
rm dir2/dir-then-file
mkdir dir2/dir-then-file
change dir1/change-in-site1
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"loading site database from repository",
		"downloading latest repository database",
		"overriding conflicts",
		"----- changes to pull -----",
		"-----",
		"removing dir2/dir-then-file",
		"copied dir1/change-in-site1",
		"updated repository copy of site database to reflect changes",
	})

	// Change a file and do another push from site2 without an intervening pull.
	writeFile(t, j("site2/dir1/change-in-site1"), start+1000, 0o644, "")
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // continue
			err = qfs.RunWithArgs([]string{"qfs", "push", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`change dir1/change-in-site1
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"local copy of repository database is current",
		"no conflicts found",
		"----- changes to push -----",
		"-----",
		"storing dir1/change-in-site1",
		"uploading repository database",
		"uploading site database",
	})

	// Change the same file on site1. A push will show conflicts.
	writeFile(t, j("site1/dir1/change-in-site1"), start+2000, 0o644, "")
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // conflicts detected
			err = qfs.RunWithArgs([]string{"qfs", "push", "--top", j("site1")})
			if err == nil || err.Error() != "conflicts detected" {
				t.Errorf("%v", err)
			}
		},
		`conflict: dir1/change-in-site1
prompt: Conflicts detected. Exit?
`,
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"downloading latest repository database",
	})

	// Push with busy file
	putInput := &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String("home/.qfs/busy"),
		Body:   bytes.NewReader([]byte{}),
	}
	_, err = s3Client.PutObject(ctx, putInput)
	testutil.Check(t, err)
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "push", "--top", j("site1")})
			if err == nil || !strings.Contains(err.Error(), ".qfs/busy exists") {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{"downloading latest repository database"})
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String("home/.qfs/busy"),
	}
	_, err = s3Client.DeleteObject(ctx, deleteInput)
	testutil.Check(t, err)

	// Push allowing local to override
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "n" // conflicts detected
			misc.TestPromptChannel <- "y" // continue
			err = qfs.RunWithArgs([]string{"qfs", "push", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`conflict: dir1/change-in-site1
prompt: Conflicts detected. Exit?
change dir1/change-in-site1
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"downloading latest repository database",
		"overriding conflicts",
		"----- changes to push -----",
		"-----",
		"storing dir1/change-in-site1",
		"uploading repository database",
		"uploading site database",
	})

	// Check versions again
	lvOut2, _ := testutil.WithStdout(
		func() {
			testutil.Check(t, qfs.RunWithArgs([]string{
				"qfs",
				"list-versions",
				"--top",
				j("site2"),
				".qfs/db/repo",
			}))
		},
	)
	m = timeRe.FindSubmatch(lvOut2)
	if m == nil {
		t.Fatalf("can't find time in %s", lvOut1)
	}
	pushTime2 := string(m[1])
	if pushTime1 == pushTime2 {
		t.Errorf("push times match: %s", pushTime1)
	}
	if slices.Equal(lvOut1, lvOut2) {
		t.Errorf("listings are equal at different times: %s\n%s", lvOut1, lvOut2)
	}

	// We know the straight listings were different. Do another listing as of the
	// previous time. This should match the earlier listing.
	lvOut2, _ = testutil.WithStdout(
		func() {
			testutil.Check(t, qfs.RunWithArgs([]string{
				"qfs",
				"list-versions",
				"--top",
				j("site2"),
				"dir1",
				"--as-of",
				pushTime1,
			}))
		},
	)
	lvOutLong2, _ := testutil.WithStdout(
		func() {
			testutil.Check(t, qfs.RunWithArgs([]string{
				"qfs",
				"list-versions",
				"--top",
				j("site2"),
				"dir1",
				"--long",
				"--as-of",
				pushTime1,
			}))
		},
	)
	if !slices.Equal(lvOut1, lvOut2) {
		t.Errorf("not equal: %s\n%s", lvOut1, lvOut2)
	}
	if !slices.Equal(lvOutLong1, lvOutLong2) {
		t.Errorf("not equal: %s\n%s", lvOut1, lvOut2)
	}

	// Site1 is current
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"loading site database from repository",
		"no conflicts found",
		"no changes to pull",
	})

	// Pull with busy file
	_, err = s3Client.PutObject(ctx, putInput)
	testutil.Check(t, err)
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site1")})
			if err == nil || !strings.Contains(err.Error(), ".qfs/busy exists") {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{"local copy of repository database is current"})
	_, err = s3Client.DeleteObject(ctx, deleteInput)
	testutil.Check(t, err)

	// Bring site2 back in sync
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // continue
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`change dir1/change-in-site1
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"loading site database from repository",
		"downloading latest repository database",
		"no conflicts found",
		"----- changes to pull -----",
		"-----",
		"copied dir1/change-in-site1",
		"updated repository copy of site database to reflect changes",
	})
	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.RunWithArgs([]string{
				"qfs",
				"sync",
				"--filter",
				j("site2/.qfs/filters/site2"),
				j("site2"),
				j("sync"),
			})
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"removing dir1/file-then-dir",
		"removing dir1/file-then-link",
		"removing dir1/file-to-remove",
		"removing dir2/dir-then-link",
		"removing dir2/dir-to-remove",
		"removing dir2/link-then-directory",
		"removing dir2/link-then-file",
		"removing dir2/link-to-remove",
		"copied dir1/file-then-link",
		"copied dir2/dir-then-link",
		"copied dir2/link-to-change",
		"copied dir2/new-link",
		"copied dir2/new-file",
		"copied dir1/file-to-change-and-chmod",
		"copied dir2/link-then-file",
		"copied dir4/only-site-2",
		"copied dir1/change-in-site1",
		"copied dir1/ro-file-to-change",
		"chmod 0600 dir1/file-to-chmod",
		"chmod 0750 dir2/dir-to-chmod",
	})
	checkSync(t, j("site2"), j("sync"), j("site2/.qfs/filters/site2"))

	// Get files from the repository. It should match what we synced.
	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.RunWithArgs([]string{
				"qfs",
				"get",
				"--top",
				j("site2"),
				"dir1",
				j("get1"),
			})
		},
		`dir1
dir1/change-in-site1
dir1/file-then-dir
dir1/file-then-link
dir1/file-to-change-and-chmod
dir1/file-to-chmod
dir1/looks-like-repo@l,1715443064543,0777
dir1/ro-file-to-change
`,
		"",
	)
	// This should match sync.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{
				"qfs",
				"diff",
				j("sync/dir1"),
				j("get1/dir1"),
			})
			if err != nil {
				t.Error(err.Error())
			}
		},
		"",
		"",
	)
	// Get doesn't allow writing to a path that exists.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{
				"qfs",
				"get",
				"--top",
				j("site2"),
				"dir1",
				j("get1"),
			})
			if err == nil || !strings.Contains(err.Error(), "/get1/dir1 must not exist") {
				t.Error(err.Error())
			}
		},
		"",
		"",
	)
	// Get from an earlier time
	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.RunWithArgs([]string{
				"qfs",
				"get",
				"--top",
				j("site2"),
				"dir1",
				j("get2"),
				"--as-of",
				pushTime1,
			})
		},
		`dir1
dir1/change-in-site1
dir1/file-then-dir
dir1/file-then-link
dir1/file-to-change-and-chmod
dir1/file-to-chmod
dir1/file-to-remove
dir1/looks-like-repo@l,1715443064543,0777
dir1/ro-file-to-change
`,
		"",
	)
	// This should match sync2.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{
				"qfs",
				"diff",
				j("sync2/dir1"),
				j("get2/dir1"),
			})
			if err != nil {
				t.Error(err.Error())
			}
		},
		"",
		"",
	)
	// Get with a filter. It should match what we synced except the filtered files.
	// Use site1 to read the repo data. It's the same, so it doesn't matter.
	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.RunWithArgs([]string{
				"qfs",
				"get",
				"--top",
				j("site1"),
				"dir1",
				j("get3"),
				"--exclude",
				"*/change-in-site1",
			})
		},
		`dir1
dir1/file-then-dir
dir1/file-then-link
dir1/file-to-change-and-chmod
dir1/file-to-chmod
dir1/looks-like-repo@l,1715443064543,0777
dir1/ro-file-to-change
`,
		"",
	)
	// This should match sync except the excluded file.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{
				"qfs",
				"diff",
				j("sync/dir1"),
				j("get3/dir1"),
			})
			if err != nil {
				t.Error(err.Error())
			}
		},
		"rm change-in-site1\n",
		"",
	)

	// Back in sync
	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.RunWithArgs([]string{
				"qfs",
				"diff",
				j("site1"),
				j("site2"),
				"--filter",
				j("site1/.qfs/filters/site1"),
				"--filter",
				j("site1/.qfs/filters/site2"),
			})
		},
		"",
		"",
	)
	checkMessages(t, nil)

	// Clean repo -- should be clean
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "init-repo", "--clean-repo", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"no objects to clean from repository",
		"uploading repository database",
	})

	// Remove dir3 and dir4 from the repo. We can do this from either site.
	writeFile(t, j("site1/.qfs/filters/repo"), start, 0o644, `
:read:prune
:exclude:
*/no-sync
:include:
dir1
dir2
excluded/included
*/always
`)
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // continue
			err = qfs.RunWithArgs([]string{"qfs", "push", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`change .qfs/filters/repo
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"downloading latest repository database",
		"no conflicts found",
		"----- changes to push -----",
		"-----",
		"storing .qfs/filters/repo",
		"uploading repository database",
		"uploading site database",
	})

	// Now clean-repo will remove dir3 and dir4 plus some junk we will add but not something outside the prefix
	putInput.Key = aws.String("home/potato")
	_, err = s3Client.PutObject(ctx, putInput)
	testutil.Check(t, err)
	putInput.Key = aws.String("this/is/safe")
	_, err = s3Client.PutObject(ctx, putInput)
	testutil.Check(t, err)
	stdout, _ := testutil.WithStdout(func() {
		misc.TestPromptChannel <- "y"
		err = qfs.RunWithArgs([]string{"qfs", "init-repo", "--clean-repo", "--top", j("site1")})
		if err != nil {
			t.Errorf("%v", err)
		}
	})
	re := regexp.MustCompile(`^home/dir3/only-in-site1@\S+
home/dir3@\S+
home/dir4/only-site-2@\S+
home/dir4@\S+
home/potato
prompt: Remove above keys\?
$`)
	if !re.Match(stdout) {
		t.Errorf("wrong stdout: %s", stdout)
	}
	checkMessages(t, []string{
		"local copy of repository database is current",
		"----- keys to remove -----",
		"-----",
		"uploading repository database",
	})

	// The files locally are safe. They will not be removed by pull since they are not in the filter.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"loading site database from repository",
		"no conflicts found",
		"no changes to pull",
	})
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y"
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`change .qfs/filters/repo
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"loading site database from repository",
		"downloading latest repository database",
		"no conflicts found",
		"----- changes to pull -----",
		"-----",
		"copied .qfs/filters/repo",
		"updated repository copy of site database to reflect changes",
	})

	// Make a change locally. Then push-db and pull to revert the change.
	writeFile(t, j("site2/dir2/will-be-reverted"), start, 0644, "")
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.RunWithArgs([]string{"qfs", "push-db", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(t, []string{
		"generating local database",
		"uploading site database",
	})
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y"
			err = qfs.RunWithArgs([]string{"qfs", "pull", "--top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`rm dir2/will-be-reverted
prompt: Continue?
`,
		"",
	)
	checkMessages(t, []string{
		"local copy of repository database is current",
		"loading site database from repository",
		"no conflicts found",
		"----- changes to pull -----",
		"-----",
		"removing dir2/will-be-reverted",
		"updated repository copy of site database to reflect changes",
	})
}
