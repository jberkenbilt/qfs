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
	_, _ = s3Client.DeleteBucket(ctx, i3)
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

func TestS3Source(t *testing.T) {
	setUpTestBucket()
	tmp := t.TempDir()
	j := func(path string) *fileinfo.Path {
		return fileinfo.NewPath(fileinfo.NewLocal(tmp), path)
	}
	err := gztar.Extract("testdata/files.tar.gz", tmp)
	testutil.Check(t, err)
	makeSrc := func(db database.Memory) *s3source.S3Source {
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
	testutil.Check(t, os.WriteFile(destPath.Path(), []byte("something new"), 0666))
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
			mExp[m] = struct{}{}
		}
		if !reflect.DeepEqual(mActual, mExp) {
			t.Errorf("messages: got %v, wanted %v", messages, exp)
		}
	}

	// Extract files to create a site.
	err := gztar.Extract("testdata/files.tar.gz", j("site1"))
	testutil.Check(t, os.MkdirAll(j("site1/"+repofiles.Top), 0777))

	// Attempt to initialize without a repository configuration.
	err = qfs.Run([]string{"qfs", "init-repo", "-top", j("site1")})
	if err == nil || !strings.Contains(err.Error(), "/site1/.qfs/repo:") {
		t.Errorf("expected no repo config: %v", err)
	}

	// Initialize a repository normally
	testutil.Check(t, os.WriteFile(j("site1/"+repofiles.RepoConfig), []byte("s3://"+TestBucket+"/home"), 0666))
	qfs.S3Client = s3Client
	defer func() { qfs.S3Client = nil }()
	err = qfs.Run([]string{"qfs", "init-repo", "-top", j("site1")})
	if err != nil {
		t.Errorf("init: %v", err)
	}
	checkMessages([]string{"uploading repository database"})

	// Re-initialize and abort
	misc.TestPromptChannel = make(chan string, 1)
	misc.TestPromptChannel <- "n"
	testutil.ExpStdout(
		t,
		func() {
			err = qfs.Run([]string{"qfs", "init-repo", "-top", j("site1")})
			if err == nil || !strings.Contains(err.Error(), "already initialized") {
				t.Errorf("wrong error: %v", err)
			}
		},
		"Repository is already initialized. Rebuild database?",
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
		"Repository is already initialized. Rebuild database?",
		"",
	)
	checkMessages([]string{"uploading repository database"})
}
