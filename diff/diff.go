package diff

import (
	"fmt"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/scan"
	"golang.org/x/exp/maps"
	"os"
	"sort"
	"strconv"
)

type Options func(*Diff)

type oldNew struct {
	fOld *fileinfo.FileInfo
	fNew *fileinfo.FileInfo
}

type Diff struct {
	filters      []*filter.Filter
	filesOnly    bool
	noSpecial    bool
	noDirTimes   bool
	noOwnerships bool
}

type Check struct {
	path    string
	modTime []int64
}

func (c *Check) String() string {
	s := "check"
	for _, m := range c.modTime {
		s += " " + strconv.Itoa(int(m))
	}
	s += " - " + c.path
	return s
}

type Result struct {
	Check      []*Check
	TypeChange []string // path
	Rm         []string // path
	Add        []*fileinfo.FileInfo
	Change     []*fileinfo.FileInfo
	MetaChange []string // {chmod mode path|[link]chown [uid]:[gid] path|mtime mtime dir}
}

func New(options ...Options) *Diff {
	d := &Diff{}
	for _, fn := range options {
		fn(d)
	}
	return d
}

func WithFilters(filters []*filter.Filter) func(*Diff) {
	return func(d *Diff) {
		d.filters = filters
	}
}

func WithNoSpecial(noSpecial bool) func(*Diff) {
	return func(d *Diff) {
		d.noSpecial = noSpecial
	}
}

func WithFilesOnly(filesOnly bool) func(*Diff) {
	return func(d *Diff) {
		d.filesOnly = filesOnly
	}
}

func WithNoOwnerships(noOwnerships bool) func(*Diff) {
	return func(d *Diff) {
		d.noOwnerships = noOwnerships
	}
}

func WithNoDirTimes(noDirTimes bool) func(*Diff) {
	return func(d *Diff) {
		d.noDirTimes = noDirTimes
	}
}

// RunFiles diffs the input sources.
func (d *Diff) RunFiles(input1, input2 string) (*Result, error) {
	s1, err := scan.New(
		input1,
		scan.WithFilters(d.filters),
		scan.WithFilesOnly(d.filesOnly),
		scan.WithNoSpecial(d.noSpecial),
	)
	if err != nil {
		// TEST: NOT COVERED
		return nil, err
	}
	s2, err := scan.New(
		input2,
		scan.WithFilters(d.filters),
		scan.WithFilesOnly(d.filesOnly),
		scan.WithNoSpecial(d.noSpecial),
	)
	if err != nil {
		// TEST: NOT COVERED
		return nil, err
	}
	files1, err := s1.Run()
	if err != nil {
		return nil, err
	}
	defer func() { _ = files1.Close() }()
	files2, err := s2.Run()
	if err != nil {
		return nil, err
	}
	defer func() { _ = files2.Close() }()
	return d.Run(files1, files2)
}

func workGet(work map[string]*oldNew, path string) *oldNew {
	entry, ok := work[path]
	if !ok {
		entry = &oldNew{}
		work[path] = entry
	}
	return entry
}

func (d *Diff) Run(files1, files2 fileinfo.Provider) (*Result, error) {
	work := map[string]*oldNew{}
	err := files1.ForEach(func(f *fileinfo.FileInfo) error {
		workGet(work, f.Path).fOld = f
		return nil
	})
	if err != nil {
		// TEST: NOT COVERED
		return nil, err
	}
	err = files2.ForEach(func(f *fileinfo.FileInfo) error {
		workGet(work, f.Path).fNew = f
		return nil
	})
	if err != nil {
		// TEST: NOT COVERED
		return nil, err
	}
	paths := maps.Keys(work)
	sort.Strings(paths)
	r := &Result{}
	for _, path := range paths {
		d.compare(r, path, work[path])
	}
	return r, nil
}

func (d *Diff) compare(r *Result, path string, data *oldNew) {
	if data.fNew == nil {
		// The file was removed. For regular files, make sure the file, if present, has
		// the right modification time.
		f := data.fOld
		if f.FileType == fileinfo.TypeFile {
			r.Check = append(r.Check, &Check{
				path:    f.Path,
				modTime: []int64{f.ModTime.UnixMilli()},
			})
		}
		r.Rm = append(r.Rm, f.Path)
	} else if data.fOld == nil {
		// The file is new. Allow it to already exist with the correct modification time.
		f := data.fNew
		r.Check = append(r.Check, &Check{
			path:    f.Path,
			modTime: []int64{f.ModTime.UnixMilli()},
		})
		r.Add = append(r.Add, f)
	} else {
		// The file has changed. Add data for conflict detection when the old file is a
		// regular file.
		if data.fOld.FileType == fileinfo.TypeFile {
			if data.fNew.ModTime != data.fOld.ModTime || data.fNew.FileType != fileinfo.TypeFile {
				// The file will be replaced or overwritten. Allow the file to have the old modification time.
				check := &Check{
					path: path,
					modTime: []int64{
						data.fOld.ModTime.UnixMilli(),
					},
				}
				if data.fNew.FileType == fileinfo.TypeFile {
					// The file is being overwritten. Allow the file to have either the old
					// modification, indicating that the file was not touched on the other side, or
					// the new modification time, indicating that it has already been updated.
					check.modTime = append(check.modTime, data.fNew.ModTime.UnixMilli())
				}
				r.Check = append(r.Check, check)
			}
		}
		if data.fOld.FileType != data.fNew.FileType {
			// The type has changed. Remove and add. Also indicate the type change for information.
			r.TypeChange = append(r.TypeChange, path)
			r.Rm = append(r.Rm, path)
			r.Add = append(r.Add, data.fNew)
		} else if data.fOld.Special != data.fNew.Special {
			// Special has changed, so this will need to be replaced.
			r.Change = append(r.Change, data.fNew)
		} else if data.fOld.ModTime != data.fNew.ModTime && data.fOld.FileType == fileinfo.TypeFile {
			// This is a plain file that has changed.
			r.Change = append(r.Change, data.fNew)
		} else {
			// The old and new file are the same type but not regular files. There will be
			// some metadata change. It's possible for more than one of these to happen.
			if !d.noDirTimes {
				if data.fOld.ModTime != data.fNew.ModTime && data.fOld.FileType == fileinfo.TypeDirectory {
					r.MetaChange = append(
						r.MetaChange,
						fmt.Sprintf("mtime %d %s", data.fNew.ModTime.UnixMilli(), path),
					)
				}
			}
			if data.fOld.Permissions != data.fNew.Permissions {
				r.MetaChange = append(
					r.MetaChange,
					fmt.Sprintf("chmod %04o %s", data.fNew.Permissions, path),
				)
			}
			if !d.noOwnerships {
				change := ":"
				if data.fOld.Uid != data.fNew.Uid {
					change = fmt.Sprintf("%d:", data.fNew.Uid)
				}
				if data.fOld.Gid != data.fNew.Gid {
					change = fmt.Sprintf("%s%d", change, data.fNew.Gid)
				}
				if change != ":" {
					r.MetaChange = append(
						r.MetaChange,
						fmt.Sprintf("chown %s %s", change, path),
					)
				}
			}
		}
	}
}

func (r *Result) WriteDiff(f *os.File, withChecks bool) error {
	if withChecks {
		for _, m := range r.Check {
			if _, err := fmt.Fprintln(f, m.String()); err != nil {
				// TEST: NOT COVERED
				return err
			}
		}
	}
	for _, m := range r.TypeChange {
		if _, err := fmt.Fprintf(f, "typechange %s\n", m); err != nil {
			// TEST: NOT COVERED
			return err
		}
	}
	for _, m := range r.Rm {
		if _, err := fmt.Fprintf(f, "rm %s\n", m); err != nil {
			// TEST: NOT COVERED
			return err
		}
	}
	for _, m := range r.Add {
		var cmd string
		if m.FileType == fileinfo.TypeDirectory {
			cmd = "mkdir"
		} else {
			cmd = "add"
		}
		if _, err := fmt.Fprintf(f, "%s %s\n", cmd, m.Path); err != nil {
			// TEST: NOT COVERED
			return err
		}
	}
	for _, m := range r.Change {
		if _, err := fmt.Fprintf(f, "change %s\n", m.Path); err != nil {
			// TEST: NOT COVERED
			return err
		}
	}
	for _, m := range r.MetaChange {
		if _, err := fmt.Fprintf(f, "%s\n", m); err != nil {
			// TEST: NOT COVERED
			return err
		}
	}
	return nil
}
