package repo_test

import (
	"fmt"
	"github.com/jberkenbilt/qfs/repo"
	"github.com/jberkenbilt/qfs/s3test"
	"os"
	"testing"
)

const TestContainer = "qfs-test-minio"

var testS3 struct {
	s3      *s3test.S3Test
	started bool
}

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

func TestXXX(t *testing.T) {
	r, err := repo.New(
		"quack",
		"src",
		repo.WithS3Client(testS3.s3.Client()),
	)
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = r.ReadDir("")
	if err != nil {
		t.Logf(err.Error())
	}
}
