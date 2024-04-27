package fileinfo

import (
	"io"
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
	S3Time      time.Time
}

type Provider interface {
	// ForEach provides each FileInfo in a non-deterministic order.
	ForEach(func(*FileInfo) error) error
	Close() error
}

type Source interface {
	FullPath(path string) string
	FileInfo(path string) (*FileInfo, error)
	DirEntries(path string) ([]string, error)
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
	return p.source.FullPath(p.path)
}

func (p *Path) FileInfo() (*FileInfo, error) {
	return p.source.FileInfo(p.path)
}

func (p *Path) DirEntries() ([]string, error) {
	return p.source.DirEntries(p.path)
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
