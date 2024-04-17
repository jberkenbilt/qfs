package main

import (
	"fmt"
	"github.com/jberkenbilt/qfs/qfs"
	"os"
	"path/filepath"
)

func run() error {
	q, err := qfs.New(qfs.WithCliArgs(os.Args))
	if err != nil {
		return err
	}
	return q.Run()
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", filepath.Base(os.Args[0]), err)
		os.Exit(2)
	}
}
