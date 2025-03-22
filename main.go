package main

import (
	"fmt"
	"github.com/jberkenbilt/qfs/qfs"
	"os"
	"path/filepath"
)

func main() {
	if err := qfs.Run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", filepath.Base(os.Args[0]), err)
		os.Exit(2)
	}
}
