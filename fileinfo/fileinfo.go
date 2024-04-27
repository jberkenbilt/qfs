package fileinfo

import (
	"fmt"
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

type DirEntry struct {
	Name   string
	S3Dir  bool
	S3Time time.Time
}

type Source interface {
	FullPath(path string) string
	FileInfo(path string) (*FileInfo, error)
	DirEntries(path string) ([]DirEntry, error)
	Open(path string) (io.ReadCloser, error)
	Remove(path string) error
	HasStDev() bool
	IsS3() bool
	Finish()
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

func (p *Path) DirEntries() ([]DirEntry, error) {
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

func PrintDb(p Provider, long bool) error {
	return p.ForEach(func(f *FileInfo) error {
		fmt.Printf("%013d %c %08d %04o", f.ModTime.UnixMilli(), f.FileType, f.Size, f.Permissions)
		if long {
			fmt.Printf(" %05d %05d", f.Uid, f.Gid)
		}
		fmt.Printf(" %s %s", f.ModTime.Format("2006-01-02 15:04:05.000Z07:00"), f.Path)
		if f.FileType == TypeLink {
			fmt.Printf(" -> %s", f.Special)
		} else if f.FileType == TypeBlockDev || f.FileType == TypeCharDev {
			fmt.Printf(" %s", f.Special)
		}
		fmt.Println("")
		return nil
	})

}
