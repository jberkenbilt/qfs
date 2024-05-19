package diff

import (
	"fmt"
	"github.com/jberkenbilt/qfs/database"
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
	repoRules    bool
	filesOnly    bool
	noSpecial    bool
	nonFileTimes bool
	noOwnerships bool
}

type Check struct {
	Path    string
	ModTime []int64
}

func (c *Check) String() string {
	s := "check"
	for _, m := range c.ModTime {
		s += " " + strconv.Itoa(int(m))
	}
	s += " - " + c.Path + "\n"
	return s
}

type MetaChange struct {
	Info        *fileinfo.FileInfo
	Permissions *uint16
	Uid         *int
	Gid         *int
	DirTime     *int64
}

func (m *MetaChange) String() string {
	var s string
	if m.Permissions != nil {
		s += fmt.Sprintf("chmod %04o %s\n", *m.Permissions, m.Info.Path)
	}
	if m.Uid != nil || m.Gid != nil {
		s += "chown "
		if m.Uid != nil {
			s += strconv.Itoa(*m.Uid)
		}
		s += ":"
		if m.Gid != nil {
			s += strconv.Itoa(*m.Gid)
		}
		s += " " + m.Info.Path + "\n"
	}
	if m.DirTime != nil {
		s += fmt.Sprintf("mtime %d %s\n", *m.DirTime, m.Info.Path)
	}
	return s
}

type Result struct {
	Check      []*Check
	TypeChange []string // path
	Rm         []*fileinfo.FileInfo
	Add        []*fileinfo.FileInfo
	Change     []*fileinfo.FileInfo
	MetaChange []*MetaChange
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

func WithRepoRules(repoRules bool) func(*Diff) {
	return func(d *Diff) {
		d.repoRules = repoRules
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

func WithNonFileTimes(nonFileTimes bool) func(*Diff) {
	return func(d *Diff) {
		d.nonFileTimes = nonFileTimes
	}
}

// RunFiles generates a diff that, when applied to oldSrc, makes it look like newSrc.
func (d *Diff) RunFiles(oldSrc, newSrc string) (*Result, error) {
	s1, err := scan.New(
		oldSrc,
		scan.WithFilters(d.filters),
		scan.WithFilesOnly(d.filesOnly),
		scan.WithNoSpecial(d.noSpecial),
	)
	if err != nil {
		// TEST: NOT COVERED
		return nil, err
	}
	s2, err := scan.New(
		newSrc,
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
	files2, err := s2.Run()
	if err != nil {
		return nil, err
	}
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

// Run generates a diff that would make oldDb look like newDb, which means that
// typically oldDb is destination and newDb is the source.
func (d *Diff) Run(oldDb, newDb database.Database) (*Result, error) {
	work := map[string]*oldNew{}
	err := oldDb.ForEach(func(f *fileinfo.FileInfo) error {
		workGet(work, f.Path).fOld = f
		return nil
	})
	if err != nil {
		// TEST: NOT COVERED
		return nil, err
	}
	err = newDb.ForEach(func(f *fileinfo.FileInfo) error {
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
	if included, _ := filter.IsIncluded(path, d.repoRules, d.filters...); !included {
		return
	}
	if data.fNew == nil {
		// The file was removed. For regular files, make sure the file, if present, has
		// the right modification time.
		f := data.fOld
		if f.FileType == fileinfo.TypeFile {
			r.Check = append(r.Check, &Check{
				Path:    f.Path,
				ModTime: []int64{f.ModTime.UnixMilli()},
			})
		}
		r.Rm = append(r.Rm, f)
	} else if data.fOld == nil {
		// The file is new. Allow it to already exist with the correct modification time.
		f := data.fNew
		if f.FileType == fileinfo.TypeFile {
			r.Check = append(r.Check, &Check{
				Path:    f.Path,
				ModTime: []int64{f.ModTime.UnixMilli()},
			})
		}
		r.Add = append(r.Add, f)
	} else {
		// The file has changed. Add data for conflict detection when the old file is a
		// regular file.
		if data.fOld.FileType == fileinfo.TypeFile {
			if data.fNew.ModTime != data.fOld.ModTime || data.fNew.FileType != fileinfo.TypeFile {
				// The file will be replaced or overwritten. Allow the file to have the old modification time.
				check := &Check{
					Path: path,
					ModTime: []int64{
						data.fOld.ModTime.UnixMilli(),
					},
				}
				if data.fNew.FileType == fileinfo.TypeFile {
					// The file is being overwritten. Allow the file to have either the old
					// modification, indicating that the file was not touched on the other side, or
					// the new modification time, indicating that it has already been updated.
					check.ModTime = append(check.ModTime, data.fNew.ModTime.UnixMilli())
				}
				r.Check = append(r.Check, check)
			}
		}
		if data.fOld.FileType != data.fNew.FileType {
			// The type has changed. Remove and add. Also indicate the type change for information.
			r.TypeChange = append(r.TypeChange, path)
			r.Rm = append(r.Rm, data.fOld)
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
			m := &MetaChange{
				Info: data.fNew,
			}
			changes := false
			if d.nonFileTimes {
				if data.fOld.ModTime != data.fNew.ModTime && data.fOld.FileType != fileinfo.TypeFile {
					t := data.fNew.ModTime.UnixMilli()
					changes = true
					m.DirTime = &t
				}
			}
			if data.fOld.Permissions != data.fNew.Permissions {
				changes = true
				m.Permissions = &data.fNew.Permissions
			}
			if !d.noOwnerships {
				if data.fOld.Uid != data.fNew.Uid {
					changes = true
					m.Uid = &data.fNew.Uid
				}
				if data.fOld.Gid != data.fNew.Gid {
					changes = true
					m.Gid = &data.fNew.Gid
				}
			}
			if changes {
				r.MetaChange = append(r.MetaChange, m)
			}
		}
	}
}

func (r *Result) WriteDiff(f *os.File, withChecks bool) error {
	if withChecks {
		for _, m := range r.Check {
			if _, err := fmt.Fprint(f, m.String()); err != nil {
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
		if _, err := fmt.Fprintf(f, "rm %s\n", m.Path); err != nil {
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
		if _, err := fmt.Fprint(f, m.String()); err != nil {
			// TEST: NOT COVERED
			return err
		}
	}
	return nil
}
