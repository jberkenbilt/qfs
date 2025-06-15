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
	"github.com/jberkenbilt/qfs/localsource"
	"github.com/jberkenbilt/qfs/misc"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var CurUid = os.Getuid()
var CurGid = os.Getgid()

type Options func(*Loader)

type Loader struct {
	path       *fileinfo.Path
	format     DbFormat
	f          io.ReadCloser
	r          *bufio.Reader
	lastOffset uint64
	nextOffset uint64
	lastRow    []byte
	lastFields []string
	filters    []*filter.Filter
	repoRules  bool
	filesOnly  bool
	noSpecial  bool
}

type DbFormat int

const (
	DbQSync = iota
	DbQfs
	DbRepo
)

var lenRe = regexp.MustCompile(`^(\d+)(?:/?(\d+))?$`)

func LoadFile(path string, options ...Options) (Database, error) {
	return Load(fileinfo.NewPath(localsource.New(""), path), options...)
}

// Load opens a database. The resulting object is a fileinfo.Provider. You must
// call Close() on the database, which will close the `f` parameter. The
// `pathForErrors` parameter is just used for error messages. See also OpenFile.
func Load(path *fileinfo.Path, options ...Options) (Database, error) {
	f, err := path.Open()
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	ld := &Loader{
		path: path,
		f:    f,
		r:    bufio.NewReader(f),
	}
	if err := ld.readHeader(); err != nil {
		_ = f.Close()
		return nil, err
	}
	for _, fn := range options {
		fn(ld)
	}

	db := Database{}
	err = ld.forEachRow(func(info *fileinfo.FileInfo) {
		db[info.Path] = info
	})
	if err != nil {
		return nil, err
	}
	return db, nil
}

func WithFilters(filters []*filter.Filter) func(*Loader) {
	return func(ld *Loader) {
		ld.filters = filters
	}
}

func WithRepoRules(repoRules bool) func(*Loader) {
	return func(ld *Loader) {
		ld.repoRules = repoRules
	}
}

func WithFilesOnly(filesOnly bool) func(*Loader) {
	return func(ld *Loader) {
		ld.filesOnly = filesOnly
	}
}

func WithNoSpecial(noSpecial bool) func(*Loader) {
	return func(ld *Loader) {
		ld.noSpecial = noSpecial
	}
}

func (ld *Loader) readHeader() error {
	first, err := ld.readBytes('\n')
	if err != nil {
		return err
	}
	header := string(first)
	switch header {
	case "QFS 1":
		ld.format = DbQfs
	case "QFS REPO 1":
		ld.format = DbRepo
	case "SYNC_TOOLS_DB_VERSION 3":
		ld.format = DbQSync
	default:
		return fmt.Errorf("%s is not a qfs database", ld.path.Path())
	}
	return nil
}

func (ld *Loader) readBytes(delimiter byte) ([]byte, error) {
	ld.lastOffset = ld.nextOffset
	data, err := ld.r.ReadBytes(delimiter)
	if err != nil {
		return data, fmt.Errorf("%s at offset %d: %w", ld.path.Path(), ld.lastOffset, err)
	}
	ld.nextOffset += uint64(len(data))
	return data[:len(data)-1], nil
}

func (ld *Loader) read(data []byte) error {
	ld.lastOffset = ld.nextOffset
	n, err := io.ReadFull(ld.r, data)
	if err != nil {
		return fmt.Errorf("%s at offset %d: %w", ld.path.Path(), ld.lastOffset, err)
	}
	ld.nextOffset += uint64(n)
	return nil
}

func (ld *Loader) skip(val byte) error {
	skip := make([]byte, 1)
	err := ld.read(skip)
	if err != nil {
		return err
	}
	if skip[0] != val {
		return fmt.Errorf("%s: expected byte %d at offset %d", ld.path.Path(), val, ld.lastOffset)
	}
	return nil
}

func (ld *Loader) getRow() ([]byte, error) {
	if ld.format == DbQSync {
		// Discard null character
		if err := ld.skip(0); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			return nil, err
		}
	}
	start, err := ld.readBytes(0)
	if err != nil {
		if ld.format != DbQSync && len(start) == 0 && errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	m := lenRe.FindSubmatch(start)
	if len(m) == 0 {
		return nil, fmt.Errorf("%s at offset %d: expected length[/same]", ld.path.Path(), ld.lastOffset)
	}
	length, _ := strconv.Atoi(string(m[1]))
	same := 0
	if m[2] != nil {
		same, _ = strconv.Atoi(string(m[2]))
		if len(ld.lastRow) < same {
			return nil, fmt.Errorf("%s at offset %d: `same` value is too large", ld.path.Path(), ld.lastOffset)
		}
	}
	data := make([]byte, length+same)
	copy(data, ld.lastRow[:same])
	err = ld.read(data[same:])
	if err != nil {
		return nil, err
	}
	err = ld.skip('\n')
	if err != nil {
		return nil, err
	}
	ld.lastRow = data
	return data, nil
}

func (ld *Loader) forEachRow(fn func(*fileinfo.FileInfo)) error {
	for {
		data, err := ld.getRow()
		if err != nil {
			return err
		}
		if data == nil {
			break
		}
		fields := strings.Split(string(data), "\x00")
		var f *fileinfo.FileInfo
		switch ld.format {
		case DbQSync:
			f, err = ld.handleQSync(fields)
		case DbQfs:
			f, err = ld.handleQfs(fields)
		case DbRepo:
			f, err = ld.handleRepo(fields)
		}
		if err != nil {
			return fmt.Errorf("%s at offset %d: %w", ld.path.Path(), ld.lastOffset, err)
		}
		ld.lastFields = fields
		if f != nil {
			included, _ := filter.IsIncluded(f.Path, ld.repoRules, ld.filters...)
			if included && (ld.filesOnly || ld.noSpecial) {
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
					if ld.filesOnly {
						included = false
					}
				default:
				}
			}
			if included {
				fn(f)
			}
		}
	}
	return nil
}

func (ld *Loader) copyFieldIfEmpty(fields []string, n int) {
	if len(fields) > n && fields[n] == "" && len(ld.lastFields) > n {
		fields[n] = ld.lastFields[n]
	}
}

func (ld *Loader) handleQSync(fields []string) (*fileinfo.FileInfo, error) {
	if len(fields) != 9 {
		return nil, fmt.Errorf("wrong number of fields: %d, not 9", len(fields))
	}
	// 0    1     2    3    4   5   6          7
	// name mtime size mode uid gid link_count special
	ld.copyFieldIfEmpty(fields, 3) // mode
	ld.copyFieldIfEmpty(fields, 4) // uid
	ld.copyFieldIfEmpty(fields, 5) // gid
	path := strings.TrimPrefix(fields[0], "./")
	mode, _ := strconv.ParseInt(fields[3], 8, 32)
	fType := mode & 0o170000
	perms := uint16(mode & 0o7777)
	fileType := fileinfo.TypeUnknown
	var size int64
	special := fields[7]
	switch fType {
	case 0o140000:
		fileType = fileinfo.TypeSocket
	case 0o120000:
		fileType = fileinfo.TypeLink
	case 0o100000:
		fileType = fileinfo.TypeFile
		t, _ := strconv.Atoi(fields[2])
		size = int64(t)
	case 0o060000:
		fileType = fileinfo.TypeBlockDev
		special = strings.TrimPrefix(special, "b,")
	case 0o040000:
		fileType = fileinfo.TypeDirectory
		if special == "-1" {
			// qsync used this for pruned directories; qfs ignores them, so ignore for
			// compatibility.
			return nil, nil
		}
		special = ""
	case 0o020000:
		fileType = fileinfo.TypeCharDev
		special = strings.TrimPrefix(special, "c,")
	case 0o010000:
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
		Uid:         uid,
		Gid:         gid,
		Special:     special,
	}, nil
}

func (ld *Loader) handleQfs(fields []string) (*fileinfo.FileInfo, error) {
	if len(fields) != 8 {
		return nil, fmt.Errorf("wrong number of fields: %d, not 8", len(fields))
	}
	// 0    1     2     3    4    5   6   7
	// name fType mtime size mode uid gid special
	ld.copyFieldIfEmpty(fields, 4) // mode
	ld.copyFieldIfEmpty(fields, 5) // uid
	ld.copyFieldIfEmpty(fields, 6) // gid
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
		Uid:         uid,
		Gid:         gid,
		Special:     fields[7],
	}, nil
}

func (ld *Loader) handleRepo(fields []string) (*fileinfo.FileInfo, error) {
	if len(fields) != 6 {
		return nil, fmt.Errorf("wrong number of fields: %d, not 6", len(fields))
	}
	// 0    1     2     3    4    5
	// name fType mtime size mode special
	ld.copyFieldIfEmpty(fields, 4) // mode
	path := fields[0]
	fileType := fileinfo.TypeUnknown
	if len(fields[1]) == 1 {
		fileType = fileinfo.FileType(fields[1][0])
	}
	milliseconds, _ := strconv.Atoi(fields[2])
	size, _ := strconv.Atoi(fields[3])
	mode, _ := strconv.ParseInt(fields[4], 8, 32)
	return &fileinfo.FileInfo{
		Path:        path,
		FileType:    fileType,
		ModTime:     time.UnixMilli(int64(milliseconds)),
		Size:        int64(size),
		Permissions: uint16(mode),
		Uid:         CurUid,
		Gid:         CurGid,
		Special:     fields[5],
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

func WriteDb(filename string, files Database, format DbFormat) error {
	var header string
	switch format {
	case DbQSync:
		return errors.New("qsync format not supported for write")
	case DbQfs:
		header = "QFS 1\n"
	case DbRepo:
		header = "QFS REPO 1\n"
	}

	err := os.MkdirAll(filepath.Dir(filename), 0777)
	if err != nil {
		return fmt.Errorf("create database \"%s\": %w", filename, err)
	}
	w, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("create database \"%s\": %w", filename, err)
	}
	defer func() { _ = w.Close() }()
	if _, err := w.WriteString(header); err != nil {
		// TEST: NOT COVERED
		return err
	}
	var lastLine []byte
	var lastMode uint16
	var lastUid int
	var lastGid int
	first := true
	err = files.ForEach(func(f *fileinfo.FileInfo) error {
		mode := newOrEmpty(first, &lastMode, f.Permissions, fmt.Sprintf("%04o", f.Permissions))
		uid := newOrEmpty(first, &lastUid, f.Uid, strconv.FormatInt(int64(f.Uid), 10))
		gid := newOrEmpty(first, &lastGid, f.Gid, strconv.FormatInt(int64(f.Gid), 10))
		first = false
		var fields []string
		if format == DbQfs {
			fields = []string{
				f.Path,
				string(f.FileType),
				strconv.FormatInt(f.ModTime.UnixMilli(), 10),
				strconv.FormatInt(f.Size, 10),
				mode,
				uid,
				gid,
				f.Special,
			}
		} else {
			fields = []string{
				f.Path,
				string(f.FileType),
				strconv.FormatInt(f.ModTime.UnixMilli(), 10),
				strconv.FormatInt(f.Size, 10),
				mode,
				f.Special,
			}
		}
		line := []byte(strings.Join(fields, "\x00"))
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

type Database map[string]*fileinfo.FileInfo

func (db Database) ForEach(fn func(*fileinfo.FileInfo) error) error {
	keys := misc.SortedKeys(db)
	for _, k := range keys {
		if err := fn(db[k]); err != nil {
			return err
		}
	}
	return nil
}

func (db Database) Print(long bool) error {
	return db.ForEach(func(f *fileinfo.FileInfo) error {
		fmt.Printf("%013d %c %08d %04o", f.ModTime.UnixMilli(), f.FileType, f.Size, f.Permissions)
		if long {
			fmt.Printf(" %05d %05d", f.Uid, f.Gid)
		}
		fmt.Printf(" %s %s", misc.FormatTime(f.ModTime), f.Path)
		switch f.FileType {
		case fileinfo.TypeLink:
			fmt.Printf(" -> %s", f.Special)
		case fileinfo.TypeBlockDev, fileinfo.TypeCharDev:
			fmt.Printf(" %s", f.Special)
		}
		fmt.Println("")
		return nil
	})
}
