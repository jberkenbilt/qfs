package repo_test

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/gztar"
	"github.com/jberkenbilt/qfs/qfs"
	"github.com/jberkenbilt/qfs/repo"
	"github.com/jberkenbilt/qfs/s3test"
	"github.com/jberkenbilt/qfs/testutil"
	"github.com/jberkenbilt/qfs/traverse"
	"os"
	"path/filepath"
	"reflect"
	"testing"
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
		err = s.Init()
		if err != nil {
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
	if err != nil {
		t.Fatalf(err.Error())
	}
	started, err := s.Start()
	if err != nil {
		t.Fatalf(err.Error())
	}
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

func TestRepo(t *testing.T) {
	setUpTestBucket()
	tmp := t.TempDir()
	j := func(path string) string { return filepath.Join(tmp, path) }
	err := gztar.Extract("testdata/files.tar.gz", tmp)
	if err != nil {
		t.Fatalf(err.Error())
	}
	r, err := repo.New(
		TestBucket,
		"home",
		repo.WithS3Client(s3Client),
	)
	if err != nil {
		t.Fatalf(err.Error())
	}
	entries, err := r.DirEntries("")
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
		err = r.Store(j("files/"+f), f)
		if err != nil {
			t.Fatalf("store: %v", err)
		}
	}

	// Check one key manually to make sure it is in the right place is correct. For
	// the rest, rely on traversal.
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String("home/file1"),
	}
	headOutput, err := s3Client.HeadObject(ctx, headInput)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if !reflect.DeepEqual(headOutput.Metadata, map[string]string{repo.MetadataKey: "1714235352000 0644"}) {
		t.Errorf("wrong metadata %#v", headOutput.Metadata)
	}
	if *headOutput.ContentLength != 16 {
		t.Errorf("wrong size: %v", *headOutput.ContentLength)
	}

	tr, err := traverse.New("", traverse.WithSource(r))
	if err != nil {
		t.Fatalf(err.Error())
	}
	files, err := tr.Traverse(
		func(s string) {
			t.Errorf("notify: %v", s)
		},
		func(err error) {
			t.Errorf("error: %v", err)
		},
	)
	if err != nil {
		t.Fatalf(err.Error())
	}
	mem := database.Memory{}
	testutil.Check(t, mem.Load(files))
	testutil.Check(t, database.WriteDb(j("qfs-from-s3"), mem, database.DbQfs))
	testutil.Check(t, database.WriteDb(j("repo-from-s3"), mem, database.DbRepo))
	stdout, stderr := testutil.WithStdout(func() {
		err = qfs.Run([]string{"qfs", "diff", j("qfs-from-s3"), j("repo-from-s3")})
		if err != nil {
			t.Errorf("error from diff: %v", err)
		}
	})
	if len(stderr) > 0 || len(stdout) > 0 {
		t.Errorf("output: %s\n%s", stdout, stderr)
	}
	stdout, stderr = testutil.WithStdout(func() {
		err = qfs.Run([]string{"qfs", "diff", j("qfs-from-s3"), j("files")})
		if err != nil {
			t.Errorf("error from diff: %v", err)
		}
	})
	if len(stderr) > 0 || len(stdout) > 0 {
		t.Errorf("output: %s\n%s", stdout, stderr)
	}
}
