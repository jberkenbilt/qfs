package database

import (
	"fmt"
	"github.com/jberkenbilt/qfs/fileinfo"
	"os"
	"strconv"
	"strings"
)

func commonPrefix(b1 []byte, b2 []byte) int {
	n := min(len(b1), len(b2))
	for i := range n {
		if b1[i] != b2[i] {
			return i
		}
	}
	return n
}

func newOrEmpty[T comparable](first bool, old *T, new T, s string) string {
	if first || *old != new {
		*old = new
		return s
	}
	return ""
}

func WriteDb(filename string, files fileinfo.Provider) error {
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
	err = files.ForEach(func(f *fileinfo.FileInfo) error {
		mode := newOrEmpty(first, &lastMode, f.Permissions, fmt.Sprintf("0%o", f.Permissions))
		uid := newOrEmpty(first, &lastUid, f.Uid, strconv.FormatInt(int64(f.Uid), 10))
		gid := newOrEmpty(first, &lastGid, f.Gid, strconv.FormatInt(int64(f.Gid), 10))
		first = false
		line := []byte(strings.Join([]string{
			f.Path,
			string(f.FileType),
			strconv.FormatInt(f.ModTime.UnixMilli(), 10),
			strconv.FormatInt(f.Size, 10),
			mode,
			uid,
			gid,
			f.Special,
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
