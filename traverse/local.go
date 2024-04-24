package traverse

import (
	"io/fs"
	"os"
)

var local = &Local{}

type Local struct {
}

func (*Local) Lstat(path string) (fs.FileInfo, error) {
	return os.Lstat(path)
}

func (*Local) Readlink(path string) (string, error) {
	return os.Readlink(path)
}

func (*Local) ReadDir(path string) ([]os.DirEntry, error) {
	return os.ReadDir(path)
}

func (*Local) HasDev() bool {
	return true
}
