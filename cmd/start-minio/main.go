package main

import (
	"fmt"
	"github.com/jberkenbilt/qfs/s3test"
	"os"
	"path/filepath"
)

func run(containerName string) error {
	s, err := s3test.New(containerName)
	if err != nil {
		return err
	}
	started, err := s.Start()
	if err != nil {
		return err
	}
	if started {
		err = s.Init()
		if err != nil {
			return fmt.Errorf("init: %w", err)
		}
	}
	fmt.Print(s.Env())
	return nil
}

func main() {
	if err := run("qfs-test-minio"); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s: %v", filepath.Base(os.Args[0]), err)
	}
}
