package fileinfo

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type LocalSource struct {
	top string
}

func NewLocal(top string) *LocalSource {
	return &LocalSource{
		top: top,
	}
}

func (ls *LocalSource) FullPath(path string) string {
	return filepath.Join(ls.top, path)
}

func (ls *LocalSource) Readlink(path string) (string, error) {
	return os.Readlink(ls.FullPath(path))
}

func (ls *LocalSource) DirEntries(path string) ([]DirEntry, error) {
	entries, err := os.ReadDir(ls.FullPath(path))
	if err != nil {
		return nil, err
	}
	var result []DirEntry
	for _, e := range entries {
		result = append(result, DirEntry{Name: e.Name()})
	}
	return result, nil
}

func (*LocalSource) HasStDev() bool {
	return true
}

func (*LocalSource) IsS3() bool {
	return false
}

func (ls *LocalSource) Open(path string) (io.ReadCloser, error) {
	return os.Open(ls.FullPath(path))
}

func (ls *LocalSource) Remove(path string) error {
	return os.Remove(ls.FullPath(path))
}

func (ls *LocalSource) FileInfo(path string) (*FileInfo, error) {
	fi := &FileInfo{
		Path:     path,
		FileType: TypeUnknown,
	}
	fullPath := ls.FullPath(path)
	lst, err := os.Lstat(fullPath)
	if err != nil {
		// TEST: CAN'T COVER. There is way to intentionally create a file that we can see
		// in its directory but can't lstat, so this is not exercised.
		return nil, fmt.Errorf("lstat %s: %w", fullPath, err)
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
		target, err := os.Readlink(fullPath)
		if err != nil {
			// TEST: CAN'T COVER. We have no way to create a link we can lstat but for which
			// readlink fails.
			return nil, fmt.Errorf("readlink %s: %w", fullPath, err)
		}
		fi.Special = target
	case mode.IsDir():
		fi.FileType = TypeDirectory
	}
	return fi, nil
}

func (*LocalSource) Finish() {
}
