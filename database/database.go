package database

import (
	"bufio"
	"fmt"
	"github.com/jberkenbilt/qfs/traverse"
	"io"
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

func WriteQSyncDb(rawW io.Writer, files *traverse.FileInfo) error {
	w := bufio.NewWriter(rawW)
	defer func() { _ = w.Flush() }()
	if _, err := w.WriteString("SYNC_TOOLS_DB_VERSION 3\n"); err != nil {
		return err
	}
	var lastLine []byte
	var lastMode int64
	var lastUid int64 = -1
	var lastGid int64 = -1
	var lastLinkCount int64 = -1
	newOrEmpty := func(old *int64, new int64, s string) string {
		if *old == new {
			return ""
		} else {
			*old = new
		}
		return s
	}
	err := files.Flatten(func(f *traverse.FileInfo) error {
		mode := newOrEmpty(&lastMode, int64(f.UMode), fmt.Sprintf("0%o", f.UMode))
		uid := newOrEmpty(&lastUid, int64(f.Uid), strconv.FormatInt(int64(f.Uid), 10))
		gid := newOrEmpty(&lastGid, int64(f.Gid), strconv.FormatInt(int64(f.Gid), 10))
		linkCount := newOrEmpty(&lastLinkCount, int64(f.LinkCount), strconv.FormatInt(int64(f.LinkCount), 10))
		size := f.Size
		var special string
		if f.Mode.IsDir() {
			special = strconv.Itoa(len(f.Children))
			size = 0
		} else if f.Mode.Type() == os.ModeSymlink {
			special = f.Target
		} else if f.Mode.Type()&os.ModeDevice != 0 {
			if f.Mode.Type()&os.ModeCharDevice != 0 {
				special = fmt.Sprintf("c,%d,%d", f.Major, f.Minor)
			} else {
				special = fmt.Sprintf("b,%d,%d", f.Major, f.Minor)
			}
		}
		path := f.Path
		if path != "." {
			// Add leading ./ for compatibility with v1
			path = "./" + f.Path
		}
		line := []byte(fmt.Sprintf(strings.Join([]string{
			path,
			strconv.FormatInt(f.ModTime.Unix(), 10),
			strconv.FormatInt(size, 10),
			mode,
			uid,
			gid,
			linkCount,
			special,
			"",
		}, "\x00")))
		same := commonPrefix(lastLine, line)
		lastLine = line
		var sameStr string
		if same > 0 {
			sameStr = fmt.Sprintf("/%d", same)
		}
		_, err := w.WriteString(fmt.Sprintf("\x00%d%s\x00%s\n", len(line)-same, sameStr, line[same:]))
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return w.Flush()
}
