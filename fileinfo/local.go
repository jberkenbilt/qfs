package fileinfo

import (
	"io"
	"io/fs"
	"os"
)

var LocalSource = &localSource{}

type localSource struct {
}

func (*localSource) Lstat(path string) (fs.FileInfo, error) {
	return os.Lstat(path)
}

func (*localSource) Readlink(path string) (string, error) {
	return os.Readlink(path)
}

func (*localSource) ReadDir(path string) ([]os.DirEntry, error) {
	return os.ReadDir(path)
}

func (*localSource) HasStDev() bool {
	return true
}

func (*localSource) Open(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (*localSource) Remove(path string) error {
	return os.Remove(path)
}
