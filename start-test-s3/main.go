package main

import (
	"fmt"
	"github.com/jberkenbilt/qfs/s3test"
	"os"
	"path/filepath"
)

func run() error {
	s, err := s3test.New()
	if err != nil {
		return err
	}
	_, err = s.Start()
	if err != nil {
		return err
	}
	fmt.Print(s.Env())
	return nil
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", filepath.Base(os.Args[0]), err)
	}
}
