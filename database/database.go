package database

import (
	"fmt"
	"github.com/jberkenbilt/qfs/traverse"
	"os"
	"strconv"
	"strings"
)

type Entry struct {
	FileType    traverse.FileType
	ModTime     int64 // milliseconds
	Size        int64
	Permissions uint16
	Uid         uint32
	Gid         uint32
	Special     string
}

func commonPrefix(b1 []byte, b2 []byte) int {
	n := min(len(b1), len(b2))
	for i := range n {
		if b1[i] != b2[i] {
			return i
		}
	}
	return n
}

func fileInfoToEntry(f *traverse.FileInfo) *Entry {
	var special string
	if f.FileType == traverse.TypeLink {
		special = f.Target
	} else if f.FileType == traverse.TypeBlockDev || f.FileType == traverse.TypeCharDev {
		special = fmt.Sprintf("%d,%d", f.Major, f.Minor)
	}
	return &Entry{
		FileType:    f.FileType,
		ModTime:     f.ModTime.UnixMilli(),
		Size:        f.Size,
		Permissions: f.Permissions,
		Uid:         f.Uid,
		Gid:         f.Gid,
		Special:     special,
	}
}

func newOrEmpty[T comparable](first bool, old *T, new T, s string) string {
	if first || *old != new {
		*old = new
		return s
	}
	return ""
}

func WriteDb(filename string, files *traverse.FileInfo) error {
	w, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create database \"%s\": %w", filename, err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.WriteString("QFS 1\n"); err != nil {
		return err
	}
	var lastLine []byte
	var lastMode uint16
	var lastUid uint32
	var lastGid uint32
	first := true
	err = files.Flatten(func(f *traverse.FileInfo) error {
		e := fileInfoToEntry(f)
		mode := newOrEmpty(first, &lastMode, e.Permissions, fmt.Sprintf("0%o", e.Permissions))
		uid := newOrEmpty(first, &lastUid, e.Uid, strconv.FormatInt(int64(e.Uid), 10))
		gid := newOrEmpty(first, &lastGid, e.Gid, strconv.FormatInt(int64(e.Gid), 10))
		first = false
		line := []byte(strings.Join([]string{
			f.Path,
			string(e.FileType),
			strconv.FormatInt(e.ModTime, 10),
			strconv.FormatInt(e.Size, 10),
			mode,
			uid,
			gid,
			e.Special,
		}, "\x00"))
		same := commonPrefix(lastLine, line)
		lastLine = line
		var sameStr string
		if same > 0 {
			sameStr = fmt.Sprintf("/%d", same)
		}
		_, err := w.WriteString(fmt.Sprintf("%d%s\x00%s\n", len(line)-same, sameStr, line[same:]))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return w.Close()
}
