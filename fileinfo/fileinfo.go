package fileinfo

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type FileType rune

const (
	TypeFile      FileType = 'f'
	TypeDirectory FileType = 'd'
	TypeLink      FileType = 'l'
	TypeCharDev   FileType = 'c'
	TypeBlockDev  FileType = 'b'
	TypePipe      FileType = 'p'
	TypeSocket    FileType = 's'
	TypeUnknown   FileType = 'x'
)

type FileInfo struct {
	Path        string
	FileType    FileType
	ModTime     time.Time
	Size        int64
	Permissions uint16
	Uid         int
	Gid         int
	Special     string
	Dev         uint64
}

type DirEntry struct {
	Name   string
	S3Dir  bool
	S3Time time.Time
}

type Source interface {
	FullPath(path string) string
	FileInfo(path string) (*FileInfo, error)
	Open(path string) (io.ReadCloser, error)
	Remove(path string) error
	Retrieve(repoPath string, localPath string) (bool, error)
}

type Path struct {
	source Source
	path   string
}

func NewPath(source Source, path string) *Path {
	return &Path{
		source: source,
		path:   path,
	}
}

func (p *Path) Path() string {
	return p.source.FullPath(p.path)
}

func (p *Path) FileInfo() (*FileInfo, error) {
	return p.source.FileInfo(p.path)
}

func (p *Path) Open() (io.ReadCloser, error) {
	return p.source.Open(p.path)
}

func (p *Path) Remove() error {
	return p.source.Remove(p.path)
}

// Relative returns the path for `other` relative to the current path.
func (p *Path) Relative(other string) *Path {
	return NewPath(p.source, filepath.Join(filepath.Dir(p.path), other))
}

func (p *Path) Join(elem string) *Path {
	return NewPath(p.source, filepath.Join(p.path, elem))
}

// RequiresCopy returns true when src is a plain file and dest is other than a
// plain file with the same size and modification time. These are the only
// conditions under which an actual download/copy is required. In all other
// cases, the operation to bring the files in sync can be done with the file
// information alone and doesn't require actually reading the source. It is an
// error to call this if the destination exists and is not a plain file.
func RequiresCopy(srcInfo *FileInfo, dest *Path) (bool, error) {
	if srcInfo.FileType != TypeFile {
		return false, nil
	}
	destInfo, err := dest.FileInfo()
	// os.IsNotExist returns false for this
	var pathError *os.PathError
	if errors.As(err, &pathError) {
		return true, nil
	} else if err != nil {
		// TEST: NOT COVERED
		return false, err
	} else if destInfo.FileType != TypeFile {
		// It is the caller's responsibility to make sure we can retrieve this safely.
		return false, fmt.Errorf("%s exists and is not a plain file", dest.Path())
	}
	if destInfo.FileType == TypeFile && destInfo.Size == srcInfo.Size && destInfo.ModTime.Equal(srcInfo.ModTime) {
		return false, nil
	}
	return true, nil
}
