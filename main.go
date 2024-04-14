package main

import (
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/jberkenbilt/qfs/traverse"
	"os"
)

type Args struct {
	Dir string `arg:"required"`
}

func run() error {
	var args Args
	arg.MustParse(&args)
	files, err := traverse.Traverse(args.Dir)
	if err != nil {
		return err
	}
	files.Traverse(func(path string, f *traverse.FileInfo) {
		fmt.Printf("%v %v %v %o4o\n", path, f.ModTime.UnixMicro(), f.Size, f.Mode&0o7777)

	})
	return err
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}
}
