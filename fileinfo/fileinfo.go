package fileinfo

import (
	"io"
	"io/fs"
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
	Uid         uint32
	Gid         uint32
	Special     string
}

type Provider interface {
	// ForEach provides each FileInfo in a non-deterministic order.
	ForEach(func(*FileInfo) error) error
	Close() error
}

type Source interface {
	Lstat(path string) (fs.FileInfo, error)
	Readlink(path string) (string, error)
	ReadDir(path string) ([]os.DirEntry, error)
	Open(path string) (io.ReadCloser, error)
	Remove(path string) error
	HasStDev() bool
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
	return p.path
}

func (p *Path) Lstat() (fs.FileInfo, error) {
	return p.source.Lstat(p.path)
}

func (p *Path) ReadLink() (string, error) {
	return p.source.Readlink(p.path)
}

func (p *Path) ReadDir() ([]os.DirEntry, error) {
	return p.source.ReadDir(p.path)
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
