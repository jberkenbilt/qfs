package database

import (
	"bufio"
	"fmt"
	"github.com/jberkenbilt/qfs/fileinfo"
	"os"
	"strconv"
	"strings"
)

type Db struct {
	filename string
	format   dbFormat
	lineNo   int
	f        *os.File
	r        *bufio.Scanner
}

type dbFormat int

const (
	dbQSync = iota
	dbQfs
)

// Open opens an on-disk database. The resulting object is a fileinfo.Provider.
// You must call Close on the database.
func Open(filename string) (*Db, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open database %s: %w", filename, err)
	}
	r := bufio.NewScanner(f)
	db := &Db{
		filename: filename,
		f:        f,
		r:        r,
	}
	var first string
	if r.Scan() {
		first = r.Text()
		db.lineNo++
	} else {
		_ = f.Close()
		if err := r.Err(); err != nil {
			return nil, fmt.Errorf("%s: %w", filename, err)
		}
	}
	if first == "QFS 1" {
		db.format = dbQfs
	} else if first == "SYNC_TOOLS_DB_VERSION 3" {
		db.format = dbQSync
	} else {
		_ = f.Close()
		return nil, fmt.Errorf("%s is not a qfs database", filename)
	}
	return db, nil
}

func (db *Db) Close() error {
	return db.f.Close()
}

func (db *Db) ForEach(func(*fileinfo.FileInfo) error) error {
	for db.r.Scan() {
		line := db.r.Text()
		db.lineNo++
		fields := strings.Split(line, "\x00")
		switch db.format {
		case dbQSync:
			if err := db.handleQSync(fields); err != nil {
				return err
			}
		case dbQfs:
			if err := db.handleQfs(fields); err != nil {
				return err
			}
		}
	}
	if err := db.r.Err(); err != nil {
		return fmt.Errorf("%s: error after line %d: %w", db.filename, db.lineNo, err)
	}
	return nil
}

func (db *Db) handleQSync(fields []string) error {
	fmt.Printf("XXX qsync %v\n", fields[1])
	return nil // XXX
}

func (db *Db) handleQfs(fields []string) error {
	fmt.Printf("XXX qfs %v\n", fields[0])
	return nil // XXX
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
