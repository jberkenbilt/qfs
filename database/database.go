// Package database implements read/write support for QFS v1 databases and read
// support for qsync v3 databases. The database formats are similar with
// differences. See README.md in this source directory.
package database

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Options func(*Db)

type Db struct {
	filename   string
	format     dbFormat
	f          *os.File
	r          *bufio.Reader
	lastOffset uint64
	nextOffset uint64
	lastRow    []byte
	lastFields []string
	filters    []*filter.Filter
	filesOnly  bool
	noSpecial  bool
}

type dbFormat int

const (
	dbQSync = iota
	dbQfs
)

var lenRe = regexp.MustCompile(`^(\d+)(?:/?(\d+))?$`)

// Open opens an on-disk database. The resulting object is a fileinfo.Provider.
// You must call Close on the database.
func Open(filename string, options ...Options) (*Db, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open database %s: %w", filename, err)
	}
	db := &Db{
		filename: filename,
		f:        f,
	}
	if err := db.rewind(); err != nil {
		_ = f.Close()
		return nil, err
	}
	for _, fn := range options {
		fn(db)
	}
	return db, nil
}

func WithFilters(filters []*filter.Filter) func(*Db) {
	return func(db *Db) {
		db.filters = filters
	}
}

func WithFilesOnly(filesOnly bool) func(*Db) {
	return func(db *Db) {
		db.filesOnly = filesOnly
	}
}

func WithNoSpecial(noSpecial bool) func(*Db) {
	return func(db *Db) {
		db.noSpecial = noSpecial
	}
}

func (db *Db) rewind() error {
	_, err := db.f.Seek(0, io.SeekStart)
	if err != nil {
		// TEST: NOT COVERED
		return err
	}
	*db = Db{
		filename: db.filename,
		filters:  db.filters,
		f:        db.f,
		r:        bufio.NewReader(db.f),
	}
	first, err := db.readBytes('\n')
	if err != nil {
		return err
	}
	header := string(first)
	if header == "QFS 1" {
		db.format = dbQfs
	} else if header == "SYNC_TOOLS_DB_VERSION 3" {
		db.format = dbQSync
	} else {
		return fmt.Errorf("%s is not a qfs database", db.filename)
	}
	return nil
}

func (db *Db) readBytes(delimiter byte) ([]byte, error) {
	db.lastOffset = db.nextOffset
	data, err := db.r.ReadBytes(delimiter)
	if err != nil {
		return data, fmt.Errorf("%s at offset %d: %w", db.filename, db.lastOffset, err)
	}
	db.nextOffset += uint64(len(data))
	return data[:len(data)-1], nil
}

func (db *Db) read(data []byte) error {
	db.lastOffset = db.nextOffset
	n, err := io.ReadFull(db.r, data)
	if err != nil {
		return fmt.Errorf("%s at offset %d: %w", db.filename, db.lastOffset, err)
	}
	db.nextOffset += uint64(n)
	return nil
}

func (db *Db) skip(val byte) error {
	skip := make([]byte, 1)
	err := db.read(skip)
	if err != nil {
		return err
	}
	if skip[0] != val {
		return fmt.Errorf("%s: expected byte %d at offset %d", db.filename, val, db.lastOffset)
	}
	return nil
}

func (db *Db) Close() error {
	return db.f.Close()
}

func (db *Db) getRow() ([]byte, error) {
	if db.format == dbQSync {
		// Discard null character
		if err := db.skip(0); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			return nil, err
		}
	}
	start, err := db.readBytes(0)
	if err != nil {
		if db.format == dbQfs && len(start) == 0 && errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	m := lenRe.FindSubmatch(start)
	if len(m) == 0 {
		return nil, fmt.Errorf("%s at offset %d: expected length[/same]", db.filename, db.lastOffset)
	}
	length, _ := strconv.Atoi(string(m[1]))
	same := 0
	if m[2] != nil {
		same, _ = strconv.Atoi(string(m[2]))
		if len(db.lastRow) < same {
			return nil, fmt.Errorf("%s at offset %d: `same` value is too large", db.filename, db.lastOffset)
		}
	}
	data := make([]byte, length+same)
	copy(data, db.lastRow[:same])
	err = db.read(data[same:])
	if err != nil {
		return nil, err
	}
	err = db.skip('\n')
	if err != nil {
		return nil, err
	}
	db.lastRow = data
	return data, nil
}

func (db *Db) ForEach(fn func(*fileinfo.FileInfo) error) error {
	if db.lastRow != nil {
		if err := db.rewind(); err != nil {
			// TEST: NOT COVERED
			return err
		}
	}
	for {
		data, err := db.getRow()
		if err != nil {
			return err
		}
		if data == nil {
			break
		}
		fields := strings.Split(string(data), "\x00")
		var f *fileinfo.FileInfo
		switch db.format {
		case dbQSync:
			f, err = db.handleQSync(fields)
		case dbQfs:
			f, err = db.handleQfs(fields)
		}
		if err != nil {
			return fmt.Errorf("%s at offset %d: %w", db.filename, db.lastOffset, err)
		}
		db.lastFields = fields
		if f != nil {
			included, _ := filter.IsIncluded(f.Path, db.filters...)
			if included && (db.filesOnly || db.noSpecial) {
				switch f.FileType {
				case fileinfo.TypeBlockDev:
					included = false
				case fileinfo.TypeCharDev:
					included = false
				case fileinfo.TypeSocket:
					included = false
				case fileinfo.TypePipe:
					included = false
				case fileinfo.TypeDirectory:
					if db.filesOnly {
						included = false
					}
				default:
				}
			}
			if included {
				err = fn(f)
				if err != nil {
					return fmt.Errorf("%s at offset %d: %w", db.filename, db.lastOffset, err)
				}
			}
		}
	}
	return nil
}

func (db *Db) copyFieldIfEmpty(fields []string, n int) {
	if len(fields) > n && fields[n] == "" && len(db.lastFields) > n {
		fields[n] = db.lastFields[n]
	}
}

func (db *Db) handleQSync(fields []string) (*fileinfo.FileInfo, error) {
	if len(fields) != 9 {
		return nil, fmt.Errorf("wrong number of fields: %d, not 9", len(fields))
	}
	// 0    1     2    3    4   5   6          7
	// name mtime size mode uid gid link_count special
	db.copyFieldIfEmpty(fields, 3) // mode
	db.copyFieldIfEmpty(fields, 4) // uid
	db.copyFieldIfEmpty(fields, 5) // gid
	path := strings.TrimPrefix(fields[0], "./")
	mode, _ := strconv.ParseInt(fields[3], 8, 32)
	fType := mode & 0o170000
	perms := uint16(mode & 0o7777)
	fileType := fileinfo.TypeUnknown
	var size int64
	special := fields[7]
	if fType == 0o140000 {
		fileType = fileinfo.TypeSocket
	} else if fType == 0o120000 {
		fileType = fileinfo.TypeLink
	} else if fType == 0o100000 {
		fileType = fileinfo.TypeFile
		t, _ := strconv.Atoi(fields[2])
		size = int64(t)
	} else if fType == 0o060000 {
		fileType = fileinfo.TypeBlockDev
		special = strings.TrimPrefix(special, "b,")
	} else if fType == 0o040000 {
		fileType = fileinfo.TypeDirectory
		if special == "-1" {
			// qsync used this for pruned directories; qfs ignores them, so ignore for
			// compatibility.
			return nil, nil
		}
		special = ""
	} else if fType == 0o020000 {
		fileType = fileinfo.TypeCharDev
		special = strings.TrimPrefix(special, "c,")
	} else if fType == 0o010000 {
		fileType = fileinfo.TypePipe
	}
	uid, _ := strconv.Atoi(fields[4])
	gid, _ := strconv.Atoi(fields[5])
	seconds, _ := strconv.Atoi(fields[1])
	return &fileinfo.FileInfo{
		Path:        path,
		FileType:    fileType,
		ModTime:     time.Unix(int64(seconds), 0),
		Size:        size,
		Permissions: perms,
		Uid:         uint32(uid),
		Gid:         uint32(gid),
		Special:     special,
	}, nil
}

func (db *Db) handleQfs(fields []string) (*fileinfo.FileInfo, error) {
	if len(fields) != 8 {
		return nil, fmt.Errorf("wrong number of fields: %d, not 8", len(fields))
	}
	// 0    1     2     3    4    5   6   7
	// name fType mtime size mode uid gid special
	db.copyFieldIfEmpty(fields, 4) // mode
	db.copyFieldIfEmpty(fields, 5) // uid
	db.copyFieldIfEmpty(fields, 6) // gid
	path := fields[0]
	fileType := fileinfo.TypeUnknown
	if len(fields[1]) == 1 {
		fileType = fileinfo.FileType(fields[1][0])
	}
	milliseconds, _ := strconv.Atoi(fields[2])
	size, _ := strconv.Atoi(fields[3])
	mode, _ := strconv.ParseInt(fields[4], 8, 32)
	uid, _ := strconv.Atoi(fields[5])
	gid, _ := strconv.Atoi(fields[6])
	return &fileinfo.FileInfo{
		Path:        path,
		FileType:    fileType,
		ModTime:     time.UnixMilli(int64(milliseconds)),
		Size:        int64(size),
		Permissions: uint16(mode),
		Uid:         uint32(uid),
		Gid:         uint32(gid),
		Special:     fields[7],
	}, nil
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
		// TEST: NOT COVERED
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
			// TEST: NOT COVERED
			return err
		}
		return nil
	})
	if err != nil {
		// TEST: NOT COVERED. This would only happen from a write error, which is not
		// exercised.
		return err
	}
	return w.Close()
}
