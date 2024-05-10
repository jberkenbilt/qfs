package repo_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/gztar"
	"github.com/jberkenbilt/qfs/misc"
	"github.com/jberkenbilt/qfs/qfs"
	"github.com/jberkenbilt/qfs/repo"
	"github.com/jberkenbilt/qfs/repofiles"
	"github.com/jberkenbilt/qfs/s3source"
	"github.com/jberkenbilt/qfs/s3test"
	"github.com/jberkenbilt/qfs/testutil"
	"github.com/jberkenbilt/qfs/traverse"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
		return fileinfo.NewPath(fileinfo.NewLocal(tmp), path)
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
	src := makeSrc(nil)
	entries, err := src.DirEntries("")
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(entries) > 0 {
		t.Errorf("unexpected entries: %#v", entries)
	}
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
	err = src.Store(fileinfo.NewPath(fileinfo.NewLocal(""), "/nope"), "nope")
	if err == nil || !strings.Contains(err.Error(), "/nope") {
		t.Errorf("wrong error: %v", err)
	}
	err = src.Store(fileinfo.NewPath(fileinfo.NewLocal(""), "/dev/null"), "nope")
	if err == nil || !strings.Contains(err.Error(), "can only store files") {
		t.Errorf("wrong error: %v", err)
	}

	// Check one key manually to make sure it is in the right place is correct. For
	// the rest, rely on traversal.
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String("home/file1"),
	}
	headOutput, err := s3Client.HeadObject(ctx, headInput)
	testutil.Check(t, err)
	if !reflect.DeepEqual(headOutput.Metadata, map[string]string{s3source.MetadataKey: "1714235352000 0644"}) {
		t.Errorf("wrong metadata %#v", headOutput.Metadata)
	}
	if *headOutput.ContentLength != 16 {
		t.Errorf("wrong size: %v", *headOutput.ContentLength)
	}

	doTraverse := func(src fileinfo.Source) fileinfo.Provider {
		t.Helper()
		tr, err := traverse.New("", traverse.WithSource(src))
		testutil.Check(t, err)
		files, err := tr.Traverse(
			func(s string) {
				t.Errorf("notify: %v", s)
			},
			func(err error) {
				t.Errorf("error: %v", err)
			},
		)
		testutil.Check(t, err)
		return files
	}

	files := doTraverse(src)
	mem1, err := database.Load(files)
	testutil.Check(t, err)
	testutil.Check(t, database.WriteDb(j("qfs-from-s3").Path(), mem1, database.DbQfs))
	testutil.Check(t, database.WriteDb(j("repo-from-s3").Path(), mem1, database.DbRepo))
	stdout, stderr := testutil.WithStdout(func() {
		err = qfs.Run([]string{"qfs", "diff", j("qfs-from-s3").Path(), j("repo-from-s3").Path()})
		if err != nil {
			t.Errorf("error from diff: %v", err)
		}
	})
	if len(stderr) > 0 || len(stdout) > 0 {
		t.Errorf("output: %s\n%s", stdout, stderr)
	}
	stdout, stderr = testutil.WithStdout(func() {
		err = qfs.Run([]string{"qfs", "diff", j("qfs-from-s3").Path(), j("files").Path()})
		if err != nil {
			t.Errorf("error from diff: %v", err)
		}
	})
	if len(stderr) > 0 || len(stdout) > 0 {
		t.Errorf("output: %s\n%s", stdout, stderr)
	}

	// Traverse again with the reference database. We should get 100% cache hits.
	src = makeSrc(mem1)
	files = doTraverse(src)
	mem2, _ := database.Load(files)
	if !reflect.DeepEqual(mem1, mem2) {
		t.Errorf("databases are inconsistent")
		_ = fileinfo.PrintDb(mem1, true)
		fmt.Println("---")
		_ = fileinfo.PrintDb(mem2, true)
	}
	// Modify the database and make sure gets back in sync
	mem2["extra"] = &fileinfo.FileInfo{Path: "extra", FileType: fileinfo.TypeUnknown, ModTime: time.Now()}
	delete(mem2, "file1")
	mem2["dir1/potato"].S3Time = mem2["dir1/potato"].S3Time.Add(-1 * time.Second)
	src = makeSrc(mem2)
	files = doTraverse(src)
	if !src.DbChanged() {
		t.Errorf("db didn't change")
	}
	mem3, _ := database.Load(files)
	o1, _ := testutil.WithStdout(func() {
		_ = fileinfo.PrintDb(mem1, true)
	})
	o2, _ := testutil.WithStdout(func() {
		_ = fileinfo.PrintDb(mem2, true)
	})
	o3, _ := testutil.WithStdout(func() {
		_ = fileinfo.PrintDb(mem3, true)
	})
	if !slices.Equal(o1, o3) {
		t.Errorf("new result doesn't match old result")
	}
	if !slices.Equal(o1, o2) {
		t.Errorf("reference didn't get back in sync")
		fmt.Printf("%s---%s", o1, o2)
	}

	// Change S3 to exercise remaining S3 functions and recheck database
	delete(mem2, "file1")
	testutil.Check(t, src.Remove("file1"))
	// Remove is idempotent, so no error to do it again.
	testutil.Check(t, src.Remove("file1"))
	src = makeSrc(mem2)
	_ = doTraverse(src)
	if src.DbChanged() {
		t.Errorf("db changed")
	}

	_, err = src.Open("nope")
	if err == nil || !strings.Contains(err.Error(), "get object s3://qfs-test-repo/home/nope:") {
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
	if !file1.S3Time.Equal(file2.S3Time) || !file1.ModTime.Equal(file2.ModTime) {
		t.Errorf("file metadata is inconsistent")
	}
	if !dir1.S3Time.Equal(dir2.S3Time) || !dir1.ModTime.Equal(dir2.ModTime) {
		t.Errorf("dir metadata is inconsistent")
	}

	// Remove a directory node. Traverse should handle this.
	src = makeSrc(mem1)
	if _, ok := mem1["dir1"]; !ok {
		t.Errorf("wrong precondition")
	}
	testutil.Check(t, src.Remove("dir1/"))
	files = doTraverse(src)
	if _, ok := mem1["dir1"]; ok {
		t.Errorf("stil there")
	}
	if _, ok := mem1["dir1/potato"]; !ok {
		t.Errorf("descendents are missing")
	}
	mem2, _ = database.Load(files)
	if !reflect.DeepEqual(mem1, mem2) {
		t.Errorf("unexpected results")
		_ = fileinfo.PrintDb(files, true)
		fmt.Println("---")
		_ = fileinfo.PrintDb(mem1, true)
	}

	// Exercise retrieval
	srcPath := fileinfo.NewPath(src, "dir1/potato")
	destPath := fileinfo.NewPath(fileinfo.NewLocal(tmp), "files/dir1/potato")
	if x, err := fileinfo.RequiresCopy(srcPath, destPath); err != nil {
		t.Fatalf(err.Error())
	} else if x {
		t.Errorf("initially requires copy")
	}
	retrieved, err := src.Retrieve("dir1/potato", destPath.Path())
	if err != nil {
		t.Errorf(err.Error())
	}
	if retrieved {
		t.Error("shouldn't have retrieved file")
	}
	testutil.Check(t, os.WriteFile(destPath.Path(), []byte("something new"), 0o666))
	retrieved, err = src.Retrieve("dir1/potato", destPath.Path())
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
	if x, err := fileinfo.RequiresCopy(srcPath, destPath); err != nil {
		t.Fatalf(err.Error())
	} else if x {
		t.Errorf("initially requires copy")
	}

	// Test reading the database from S3
	testutil.Check(t, src.Store(j("repo-from-s3"), "repo-db"))
	s3Path := fileinfo.NewPath(src, "repo-db")
	dbFromS3, err := database.Open(s3Path)
	testutil.Check(t, err)
	defer func() { _ = dbFromS3.Close() }()
	dbFromDisk, err := database.OpenFile(j("repo-from-s3").Path())
	testutil.Check(t, err)
	defer func() { _ = dbFromDisk.Close() }()
	mem1, err = database.Load(dbFromS3)
	testutil.Check(t, err)
	mem2, err = database.Load(dbFromDisk)
	testutil.Check(t, err)
	if !reflect.DeepEqual(mem1, mem2) {
		t.Errorf("inconsistent results")
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
	testutil.Check(t, err)
	v, err := r.IsInitialized()
	testutil.Check(t, err)
	if v {
		t.Errorf("repo is initialized when brand new")
	}
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

	// Monitor messages. Send a magic string to catch up send messages accumulated so
	// far.
	msgChan := make(chan []string, 1)
	misc.TestMessageChannel = make(chan string, 5)
	defer close(misc.TestMessageChannel)
	const MsgCatchup = "!CHECK!"
	go func() {
		var accumulated []string
		for m := range misc.TestMessageChannel {
			if m == MsgCatchup {
				msgChan <- accumulated
				accumulated = nil
			} else {
				accumulated = append(accumulated, m)
			}
		}
	}()
	getMessages := func() []string {
		misc.Message(MsgCatchup)
		return <-msgChan
	}
	checkMessages := func(exp []string) {
		t.Helper()
		messages := getMessages()
		mActual := map[string]struct{}{}
		for _, m := range messages {
			mActual[m] = struct{}{}
		}
		mExp := map[string]struct{}{}
		for _, m := range exp {
			if _, ok := mActual[m]; !ok {
				t.Errorf("missing message: %s", m)
			}
			mExp[m] = struct{}{}
		}
		for _, m := range messages {
			if _, ok := mExp[m]; !ok {
				t.Errorf("extra message: %s", m)
			}
		}
	}

	// Create a directory for a site.
	testutil.Check(t, os.MkdirAll(j("site1/"+repofiles.Top), 0o777))

	// Attempt to initialize without a repository configuration.
	err := qfs.Run([]string{"qfs", "init-repo", "-top", j("site1")})
	if err == nil || !strings.Contains(err.Error(), "/site1/.qfs/repo:") {
		t.Errorf("expected no repo config: %v", err)
	}

	writeFile(t, j("site1/"+repofiles.RepoConfig), time.Now().UnixMilli(), 0o644, "invalid contents")
	err = qfs.Run([]string{"qfs", "init-repo", "-top", j("site1")})
	if err == nil || !strings.Contains(err.Error(), "must contain s3://bucket/prefix") {
		t.Errorf("expected no repo config: %v", err)
	}

	// Initialize a repository normally

	// No newline on repo file
	writeFile(t, j("site1/"+repofiles.RepoConfig), time.Now().UnixMilli(), 0o644, "s3://"+TestBucket+"/home")
	qfs.S3Client = s3Client
	defer func() { qfs.S3Client = nil }()
	err = qfs.Run([]string{"qfs", "init-repo", "-top", j("site1")})
	if err != nil {
		t.Errorf("init: %v", err)
	}
	checkMessages([]string{"uploading repository database"})

	// Re-initialize and abort
	misc.TestPromptChannel = make(chan string, 5)
	misc.TestPromptChannel <- "n"
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "init-repo", "-top", j("site1")})
			if err == nil || !strings.Contains(err.Error(), "already initialized") {
				t.Errorf("wrong error: %v", err)
			}
		},
		"prompt: Repository is already initialized. Rebuild database?\n",
		"",
	)
	checkMessages(nil)

	// Re-initialize
	misc.TestPromptChannel <- "y"
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "init-repo", "-top", j("site1")})
			if err != nil {
				t.Errorf("error: %v", err)
			}
		},
		"prompt: Repository is already initialized. Rebuild database?\n",
		"",
	)
	checkMessages([]string{"uploading repository database"})

	// Do the initial push without initializing site
	err = qfs.Run([]string{"qfs", "push", "-top", j("site1")})
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
:junk:(~|.junk)$
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
dir5
`)
	writeFile(t, j("site1/.qfs/filters/site1"), start, 0o644, `
:read:prune
:include:
dir1
dir2
# dir3 is only on site1
dir3
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
	testutil.Check(t, os.Mkdir(j("site1/dir2"), 0o755))
	testutil.Check(t, os.Symlink("../dir1/change-in-site1", j("site1/dir2/link-to-change")))
	testutil.Check(t, os.Symlink("replace-with-file", j("site1/dir2/link-then-file")))
	testutil.Check(t, os.Symlink("replace-with-dir", j("site1/dir2/link-then-directory")))
	testutil.Check(t, os.Symlink("/dev/null", j("site1/dir2/link-to-remove")))
	testutil.Check(t, os.Mkdir(j("site1/dir2/dir-then-file"), 0o755))
	testutil.Check(t, os.Mkdir(j("site1/dir2/dir-then-link"), 0o755))
	testutil.Check(t, os.Mkdir(j("site1/dir2/dir-to-chmod"), 0o775))
	testutil.Check(t, os.Mkdir(j("site1/dir2/dir-to-remove"), 0o555))

	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // Continue?
			err = qfs.Run([]string{"qfs", "push", "-top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		// This is lexically sorted, since diff output is sorted, based on the things
		// created above.
		`add .qfs/filters/prune
add .qfs/filters/repo
add .qfs/filters/site1
mkdir dir1
add dir1/change-in-site1
add dir1/file-then-dir
add dir1/file-then-link
add dir1/file-to-change-and-chmod
add dir1/file-to-chmod
add dir1/file-to-remove
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
prompt: Continue?
`,
		"",
	)
	// Lines in checkMessages are in a non-deterministic order, but checkMessages
	// sorts expected and actual.
	checkMessages([]string{
		"generating local database",
		"local copy of repository database is current",
		"no conflicts found",
		"----- changes to push -----",
		// diff is written to stdout
		"-----",
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
		"uploading repository database",
		"uploading site database",
	})

	// Do another push right away. There should be no changes. Do this with the local
	// database changed so it gets back in sync.
	testutil.Check(t, os.Chtimes(j("site1/.qfs/db/repo"), time.Time{}, time.Now()))
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "push", "-top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages([]string{
		"generating local database",
		"downloading latest repository database",
		"no conflicts found",
		"no changes to push",
		"updating local copy of repository database",
		"uploading site database",
	})

	// Change file on site1 without pushing -- will be pushed later.
	writeFile(t, j("site1/dir1/change-in-site1"), start, 0o644, "")

	// Pull after push -- no changes expected since the above file is unknown to the repository.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages([]string{
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
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site2")})
			if err == nil || err.Error() != "exiting" {
				t.Errorf("%v", err)
			}
		},
		`add .qfs/filters/prune
add .qfs/filters/repo
add .qfs/filters/site1
prompt: Continue?
`,
		"",
	)
	checkMessages([]string{
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
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`add .qfs/filters/prune
add .qfs/filters/repo
add .qfs/filters/site1
prompt: Continue?
`,
		"",
	)
	checkMessages([]string{
		"downloading latest repository database",
		"repository doesn't contain a database for this site",
		"site filter does not exist on the repository; trying local copy",
		"no filter is configured for this site; bootstrapping with exclude all",
		"no conflicts found",
		"----- changes to pull -----",
		"-----",
		"downloaded .qfs/filters/repo",
		"downloaded .qfs/filters/site1",
		"downloaded .qfs/filters/prune",
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
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site2")})
			if err != nil {
				t.Errorf("pull: %v", err)
			}
		},
		`mkdir dir1
add dir1/change-in-site1
add dir1/file-then-dir
add dir1/file-then-link
add dir1/file-to-change-and-chmod
add dir1/file-to-chmod
add dir1/file-to-remove
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
	checkMessages([]string{
		"local copy of repository database is current",
		"loading site database from repository",
		"site filter does not exist on the repository; trying local copy",
		"no conflicts found",
		"----- changes to pull -----",
		"-----",
		"downloaded dir1/change-in-site1",
		"downloaded dir1/file-then-dir",
		"downloaded dir1/file-then-link",
		"downloaded dir1/file-to-change-and-chmod",
		"downloaded dir1/file-to-chmod",
		"downloaded dir1/file-to-remove",
		"downloaded dir1/ro-file-to-change",
		"downloaded dir2/link-then-directory",
		"downloaded dir2/link-then-file",
		"downloaded dir2/link-to-change",
		"downloaded dir2/link-to-remove",
		"updated repository copy of site database to reflect changes",
	})

	// Pull again: no changes.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages([]string{
		"local copy of repository database is current",
		"loading site database from repository",
		"site filter does not exist on the repository; trying local copy",
		"no conflicts found",
		"no changes to pull",
	})

	// In the next stage of testing, exercise that changes are carried across.
	// Advance the timestamps so changes are detected in case the test suite runs
	// faster than qfs or s3 timestamp granularity. Then make changes relative to the
	// way things were originally set up. We are making changes in site2 to things
	// that were originally created in site1.
	start += 3600000
	writeFile(t, j("site2/dir1/file-to-change-and-chmod"), start, 0o444, "new contents")
	writeFile(t, j("site2/dir1/ro-file-to-change"), start, 0o444, "new ro contents")
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
			err = qfs.Run([]string{"qfs", "push", "-n", "-top", j("site2")})
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
	checkMessages([]string{
		"generating local database",
		"local copy of repository database is current",
		"no conflicts found",
		"----- changes to push -----",
		// diff is written to stdout
		"-----",
	})

	// Push
	oldBatchSize := repo.DeleteBatchSize
	repo.DeleteBatchSize = 3 // Exercise deleting in multiple batches
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // continue
			err = qfs.Run([]string{"qfs", "push", "-top", j("site2")})
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
	checkMessages([]string{
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
		"uploading repository database",
		"uploading site database",
	})
	repo.DeleteBatchSize = oldBatchSize

	// Change file on site1 but don't push yet
	start += 3600000
	writeFile(t, j("site1/dir1/change-in-site1"), start, 0o644, "change in site 1")

	// Pull changes from site1. The file that was changed in site1 is ignored because
	// we haven't pushed it yet.
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "pull", "-n", "-top", j("site1")})
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
	checkMessages([]string{
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
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site1")})
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
	checkMessages([]string{
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
		"downloaded .qfs/filters/site2",
		"downloaded dir1/file-then-link",
		"downloaded dir2/dir-then-file",
		"downloaded dir2/dir-then-link",
		"downloaded dir2/link-then-file",
		"downloaded dir2/new-file",
		"downloaded dir2/new-link",
		"downloaded dir1/file-to-change-and-chmod",
		"downloaded dir1/ro-file-to-change",
		"downloaded dir2/link-to-change",
		"chmod 0600 dir1/file-to-chmod",
		"chmod 0750 dir2/dir-to-chmod",
		"updated repository copy of site database to reflect changes",
	})

	// The sites should be in sync except the file we changed in site1 and haven't pushed.
	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.Run([]string{
				"qfs",
				"diff",
				j("site1"),
				j("site2"),
				"-filter",
				j("site1/.qfs/filters/site1"),
				"-filter",
				j("site1/.qfs/filters/site2"),
				"-no-dir-times",
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
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages([]string{
		"local copy of repository database is current",
		"loading site database from repository",
		"no conflicts found",
		"no changes to pull",
	})
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "pull", "-n", "-top", j("site2"), "-local-filter"})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`mkdir dir3
add dir3/only-in-site1
`,
		"",
	)
	checkMessages([]string{
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
			err = qfs.Run([]string{"qfs", "pull", "-n", "-top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages([]string{
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
			err = qfs.Run([]string{"qfs", "push", "-n", "-top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`change dir1/change-in-site1
change dir2/dir-then-file
`,
		"",
	)
	checkMessages([]string{
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
			err = qfs.Run([]string{"qfs", "push", "-top", j("site1")})
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
	checkMessages([]string{
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
			err = qfs.Run([]string{"qfs", "push", "-top", j("site2")})
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
	checkMessages([]string{
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
			err = qfs.Run([]string{"qfs", "push", "-n", "-top", j("site1")})
			if err == nil || err.Error() != "conflicts detected" {
				t.Errorf("%v", err)
			}
		},
		`conflict: dir1/change-in-site1
conflict: dir2/dir-then-file
`,
		"",
	)
	checkMessages([]string{
		"generating local database",
		"downloading latest repository database",
	})
	// Pull but exit on conflicts prompt
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // conflicts detected
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site1")})
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
	checkMessages([]string{
		"loading site database from repository",
		"downloading latest repository database",
	})
	// Resolve letting repository win
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "n" // conflicts detected
			misc.TestPromptChannel <- "y" // continue
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site1")})
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
	checkMessages([]string{
		"loading site database from repository",
		"downloading latest repository database",
		"overriding conflicts",
		"----- changes to pull -----",
		"-----",
		"removing dir2/dir-then-file",
		"downloaded dir1/change-in-site1",
		"updated repository copy of site database to reflect changes",
	})

	// Change a file and do another push from site2 without an intervening pull.
	writeFile(t, j("site2/dir1/change-in-site1"), start+1000, 0o644, "")
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // continue
			err = qfs.Run([]string{"qfs", "push", "-top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`change dir1/change-in-site1
prompt: Continue?
`,
		"",
	)
	checkMessages([]string{
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
			err = qfs.Run([]string{"qfs", "push", "-top", j("site1")})
			if err == nil || err.Error() != "conflicts detected" {
				t.Errorf("%v", err)
			}
		},
		`conflict: dir1/change-in-site1
prompt: Conflicts detected. Exit?
`,
		"",
	)
	checkMessages([]string{
		"generating local database",
		"downloading latest repository database",
	})

	// Push with busy file
	src, err := s3source.New(
		TestBucket,
		"home",
		s3source.WithS3Client(s3Client),
	)
	testutil.Check(t, err)
	testutil.Check(t, src.Store(
		fileinfo.NewPath(fileinfo.NewLocal(tmp), "site1/dir1/change-in-site1"),
		".qfs/busy",
	))
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "push", "-top", j("site1")})
			if err == nil || !strings.Contains(err.Error(), ".qfs/busy exists") {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(nil)
	testutil.Check(t, src.Remove(".qfs/busy"))

	// Push allowing local to override
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "n" // conflicts detected
			misc.TestPromptChannel <- "y" // continue
			err = qfs.Run([]string{"qfs", "push", "-top", j("site1")})
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
	checkMessages([]string{
		"generating local database",
		"downloading latest repository database",
		"overriding conflicts",
		"----- changes to push -----",
		"-----",
		"storing dir1/change-in-site1",
		"uploading repository database",
		"uploading site database",
	})

	// Site1 is current
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site1")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages([]string{
		"local copy of repository database is current",
		"loading site database from repository",
		"no conflicts found",
		"no changes to pull",
	})

	// Pull with busy file
	testutil.Check(t, src.Store(
		fileinfo.NewPath(fileinfo.NewLocal(tmp), "site1/dir1/change-in-site1"),
		".qfs/busy",
	))
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site1")})
			if err == nil || !strings.Contains(err.Error(), ".qfs/busy exists") {
				t.Errorf("%v", err)
			}
		},
		"",
		"",
	)
	checkMessages(nil)
	testutil.Check(t, src.Remove(".qfs/busy"))

	// Bring site2 back in sync
	testutil.ExpStdout(
		t,
		func() {
			misc.TestPromptChannel <- "y" // continue
			err = qfs.Run([]string{"qfs", "pull", "-top", j("site2")})
			if err != nil {
				t.Errorf("%v", err)
			}
		},
		`change dir1/change-in-site1
prompt: Continue?
`,
		"",
	)
	checkMessages([]string{
		"loading site database from repository",
		"downloading latest repository database",
		"no conflicts found",
		"----- changes to pull -----",
		"-----",
		"downloaded dir1/change-in-site1",
		"updated repository copy of site database to reflect changes",
	})

	// Back in sync
	testutil.ExpStdout(
		t,
		func() {
			_ = qfs.Run([]string{
				"qfs",
				"diff",
				j("site1"),
				j("site2"),
				"-filter",
				j("site1/.qfs/filters/site1"),
				"-filter",
				j("site1/.qfs/filters/site2"),
				"-no-dir-times",
			})
		},
		"",
		"",
	)
}
