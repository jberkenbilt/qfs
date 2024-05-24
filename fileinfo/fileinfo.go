package fileinfo

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TimeFormat = "2006-01-02 15:04:05.000-07:00"

type FileType rune

// fsMutex is for local file system operations.
var fsMutex sync.Mutex

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
	Download(srcPath string, srcInfo *FileInfo, f *os.File) error
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

func (p *Path) Download(srcInfo *FileInfo, f *os.File) error {
	return p.source.Download(p.path, srcInfo, f)
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

// Retrieve retrieves the source path and writes to the local path. No action is
// performed If localPath has the same size and modification time as indicated in
// the source. The return value indicates whether the file changed.
func Retrieve(srcPath, destPath *Path) (bool, error) {
	// Lock a mutex for local file system operations. Unlock the mutex while interacting with the source.
	fsMutex.Lock()
	defer fsMutex.Unlock()
	withUnlocked := func(fn func()) {
		fsMutex.Unlock()
		defer fsMutex.Lock()
		fn()
	}

	localPath := destPath.Path()
	srcInfo, err := srcPath.FileInfo()
	if err != nil {
		return false, err
	}
	if srcInfo.FileType == TypeLink {
		target, err := os.Readlink(localPath)
		if err == nil && target == srcInfo.Special {
			return false, nil
		}
		err = os.MkdirAll(filepath.Dir(localPath), 0777)
		if err != nil {
			return false, err
		}
		err = os.RemoveAll(localPath)
		if err != nil {
			return false, err
		}
		err = os.Symlink(srcInfo.Special, localPath)
		if err != nil {
			return false, err
		}
		return true, nil
	} else if srcInfo.FileType == TypeDirectory {
		info, err := destPath.FileInfo()
		if err != nil || info.FileType != TypeDirectory {
			err = os.RemoveAll(localPath)
			if err != nil {
				return false, err
			}
		}
		// Ignore directory times.
		if info != nil && info.FileType == TypeDirectory && info.Permissions == srcInfo.Permissions {
			// No action required
			return false, nil
		}
		err = os.MkdirAll(localPath, 0777)
		if err != nil {
			return false, err
		}
		if err := os.Chmod(localPath, fs.FileMode(srcInfo.Permissions)); err != nil {
			return false, fmt.Errorf("set mode for %s: %w", localPath, err)
		}
		return true, nil
	} else if srcInfo.FileType != TypeFile {
		// TEST: NOT COVERED. There is no way to represent this, and Store doesn't store
		// specials.
		return false, fmt.Errorf("downloading special files is not supported")
	}
	var requiresCopy bool
	withUnlocked(func() {
		requiresCopy, err = RequiresCopy(srcInfo, destPath)
	})
	if err != nil {
		return false, err
	}
	if !requiresCopy {
		return false, nil
	}
	err = os.MkdirAll(filepath.Dir(localPath), 0777)
	if err != nil {
		return false, err
	}
	err = os.Chmod(localPath, fs.FileMode(srcInfo.Permissions|0o600))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	f, err := os.Create(localPath)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	withUnlocked(func() {
		err = srcPath.Download(srcInfo, f)
	})
	if err != nil {
		return false, err
	}
	if err := f.Close(); err != nil {
		return false, err
	}
	if err := os.Chtimes(localPath, time.Time{}, srcInfo.ModTime); err != nil {
		return false, fmt.Errorf("set times for %s: %w", localPath, err)
	}
	if err := os.Chmod(localPath, fs.FileMode(srcInfo.Permissions)); err != nil {
		return false, fmt.Errorf("set mode for %s: %w", localPath, err)
	}
	return true, nil
}
