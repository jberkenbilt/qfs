package main

import (
	"fmt"
	"github.com/jberkenbilt/qfs/qfs"
	"os"
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
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}
}
