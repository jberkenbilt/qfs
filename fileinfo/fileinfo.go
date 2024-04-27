package fileinfo

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
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

func (p *Path) FileInfo(relativePath string) (*FileInfo, error) {
	fi := &FileInfo{
		Path:     relativePath,
		FileType: TypeUnknown,
	}
	lst, err := p.Lstat()
	if err != nil {
		// TEST: CAN'T COVER. There is way to intentionally create a file that we can see
		// in its directory but can't lstat, so this is not exercised.
		return nil, fmt.Errorf("lstat %s: %w", p.Path(), err)
	}
	fi.ModTime = lst.ModTime().Truncate(time.Millisecond)
	mode := lst.Mode()
	fi.Permissions = uint16(mode.Perm())
	st, ok := lst.Sys().(*syscall.Stat_t)
	var major, minor uint32
	if ok && st != nil {
		fi.Uid = int(st.Uid)
		fi.Gid = int(st.Gid)
		fi.Dev = st.Dev
		major = uint32(st.Rdev >> 8 & 0xfff)
		minor = uint32(st.Rdev&0xff | (st.Rdev >> 12 & 0xfff00))
	}
	modeType := mode.Type()
	switch {
	case mode.IsRegular():
		fi.FileType = TypeFile
		fi.Size = lst.Size()
	case modeType&os.ModeDevice != 0:
		if modeType&os.ModeCharDevice != 0 {
			fi.FileType = TypeCharDev
		} else {
			fi.FileType = TypeBlockDev
		}
		fi.Special = fmt.Sprintf("%d,%d", major, minor)
	case modeType&os.ModeSocket != 0:
		fi.FileType = TypeSocket
	case modeType&os.ModeNamedPipe != 0:
		fi.FileType = TypePipe
	case modeType&os.ModeSymlink != 0:
		fi.FileType = TypeLink
		target, err := p.ReadLink()
		if err != nil {
			// TEST: CAN'T COVER. We have no way to create a link we can lstat but for which
			// readlink fails.
			return nil, fmt.Errorf("readlink %s: %w", p.Path(), err)
		}
		fi.Special = target
	case mode.IsDir():
		fi.FileType = TypeDirectory
	}
	return fi, nil
}
