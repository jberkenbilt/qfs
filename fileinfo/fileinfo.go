package fileinfo

import (
	"io/fs"
	"os"
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
	HasStDev() bool
}
