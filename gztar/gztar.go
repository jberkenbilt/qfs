package gztar

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"golang.org/x/exp/maps"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"
)

func Extract(filename string, dest string) error {
	tarFile, err := os.Open(filename)
	if err != nil {
		return err
	}
	gz, err := gzip.NewReader(tarFile)
	if err != nil {
		return err
	}
	archive := tar.NewReader(gz)
	dirTimes := map[string]time.Time{}
	for {
		h, err := archive.Next()
		if h == nil || errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		fi := h.FileInfo()
		mode := fi.Mode()
		modeType := mode.Type()
		perm := mode.Perm()
		name := filepath.Join(dest, h.Name)
		if strings.HasSuffix(h.Name, "/") {
			if err := os.MkdirAll(name, perm); err != nil {
				return err
			}
			dirTimes[name] = h.ModTime
		} else {
			dir := filepath.Dir(name)
			if err := os.MkdirAll(dir, 0777); err != nil {
				return err
			}
			switch {
			case mode.IsRegular():
				f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, perm)
				if err != nil {
					return err
				}
				_, err = io.Copy(f, archive)
				err2 := f.Close()
				if err != nil || err2 != nil {
					return errors.Join(err, err2)
				}
				if err := os.Chtimes(name, time.Time{}, h.ModTime); err != nil {
					return err
				}
			case modeType&os.ModeNamedPipe != 0:
				if err := syscall.Mkfifo(name, uint32(perm)); err != nil {
					return err
				}
			case modeType&os.ModeSymlink != 0:
				if err := os.Symlink(h.Linkname, name); err != nil {
					return err
				}
			default:
				// ignore
			}
		}
	}
	dirs := maps.Keys(dirTimes)
	sort.Strings(dirs)
	slices.Reverse(dirs)
	for _, dir := range dirs {
		if err := os.Chtimes(dir, time.Time{}, dirTimes[dir]); err != nil {
			return err
		}
	}
	return nil
}
