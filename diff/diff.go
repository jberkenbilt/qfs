package diff

import (
	"fmt"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/scan"
	"golang.org/x/exp/maps"
	"os"
	"sort"
)

type Options func(*Diff)

type oldNew struct {
	fOld *fileinfo.FileInfo
	fNew *fileinfo.FileInfo
}

type Diff struct {
	input1       string
	input2       string
	filters      []*filter.Filter
	filesOnly    bool
	noSpecial    bool
	noDirTimes   bool
	noOwnerships bool
	checks       bool
}

type Result struct {
	Check      []string // mtime [ ... ] - path
	TypeChange []string // path
	Rm         []string // path
	Add        []*fileinfo.FileInfo
	Change     []*fileinfo.FileInfo
	MetaChange []string // {chmod mode path|[link]chown [uid]:[gid] path|mtime mtime dir}
}

func New(input1, input2 string, options ...Options) (*Diff, error) {
	q := &Diff{
		input1: input1,
		input2: input2,
	}
	for _, fn := range options {
		fn(q)
	}
	return q, nil
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

func WithChecks(checks bool) func(*Diff) {
	return func(d *Diff) {
		d.checks = checks
	}
}

func WithNoDirTimes(noDirTimes bool) func(*Diff) {
	return func(d *Diff) {
		d.noDirTimes = noDirTimes
	}
}

// Run diffs the input sources.
func (d *Diff) Run() (*Result, error) {
	s1, err := scan.New(
		d.input1,
		scan.WithFilters(d.filters),
		scan.WithFilesOnly(d.filesOnly),
		scan.WithNoSpecial(d.noSpecial),
	)
	if err != nil {
		return nil, err
	}
	s2, err := scan.New(
		d.input2,
		scan.WithFilters(d.filters),
		scan.WithFilesOnly(d.filesOnly),
		scan.WithNoSpecial(d.noSpecial),
	)
	if err != nil {
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
	r := &Result{}
	err = d.diff(r, files1, files2)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func workGet(work map[string]*oldNew, path string) *oldNew {
	entry, ok := work[path]
	if !ok {
		entry = &oldNew{}
		work[path] = entry
	}
	return entry
}

func (d *Diff) diff(r *Result, files1, files2 fileinfo.Provider) error {
	work := map[string]*oldNew{}
	err := files1.ForEach(func(f *fileinfo.FileInfo) error {
		workGet(work, f.Path).fOld = f
		return nil
	})
	if err != nil {
		return err
	}
	err = files2.ForEach(func(f *fileinfo.FileInfo) error {
		workGet(work, f.Path).fNew = f
		return nil
	})
	if err != nil {
		return err
	}
	paths := maps.Keys(work)
	sort.Strings(paths)
	for _, path := range paths {
		d.compare(r, path, work[path])
	}
	if !d.checks {
		r.Check = nil
	}
	return nil
}

func (d *Diff) compare(r *Result, path string, data *oldNew) {
	if data.fNew == nil {
		// The file was removed. For regular files, make sure the file, if present, has
		// the right modification time.
		f := data.fOld
		if f.FileType == fileinfo.TypeFile {
			r.Check = append(r.Check, fmt.Sprintf("%d - %s", f.ModTime.UnixMilli(), f.Path))
		}
		r.Rm = append(r.Rm, f.Path)
	} else if data.fOld == nil {
		// The file is new. Allow it to already exist with the correct modification time.
		f := data.fNew
		r.Check = append(r.Check, fmt.Sprintf("%d - %s", f.ModTime.UnixMilli(), f.Path))
		r.Add = append(r.Add, f)
	} else {
		// The file has changed. Add data for conflict detection when the old file is a
		// regular file.
		if data.fOld.FileType == fileinfo.TypeFile {
			if data.fNew.ModTime != data.fOld.ModTime || data.fNew.FileType != fileinfo.TypeFile {
				// The file will be replaced or overwritten.
				var check string
				if data.fNew.FileType == fileinfo.TypeFile {
					// The file is being overwritten. Allow the file to have either the old
					// modification, indicating that the file was not touched on the other side, or
					// the new modification time, indicating that it has already been updated.
					check = fmt.Sprintf(
						"%d %d - %s",
						data.fOld.ModTime.UnixMilli(),
						data.fNew.ModTime.UnixMilli(),
						path,
					)
				} else {
					// The file is being replaced. Allow the file to have the old modification time.
					check = fmt.Sprintf(
						"%d - %s",
						data.fOld.ModTime.UnixMilli(),
						path,
					)
				}
				if check != "" {
					r.Check = append(r.Check, check)
				}
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
				oldOwner := fmt.Sprintf("%d:%d", data.fOld.Uid, data.fOld.Gid)
				newOwner := fmt.Sprintf("%d:%d", data.fNew.Uid, data.fNew.Gid)
				if oldOwner != newOwner {
					r.MetaChange = append(
						r.MetaChange,
						fmt.Sprintf("chown %s %s", newOwner, path),
					)
				}
			}
		}
	}
}

func (r *Result) WriteDiff(f *os.File) error {
	for _, m := range r.Check {
		if _, err := fmt.Fprintf(f, "check %s\n", m); err != nil {
			return err
		}
	}
	for _, m := range r.TypeChange {
		if _, err := fmt.Fprintf(f, "typechange %s\n", m); err != nil {
			return err
		}
	}
	for _, m := range r.Rm {
		if _, err := fmt.Fprintf(f, "rm %s\n", m); err != nil {
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
			return err
		}
	}
	for _, m := range r.Change {
		if _, err := fmt.Fprintf(f, "change %s\n", m.Path); err != nil {
			return err
		}
	}
	for _, m := range r.MetaChange {
		if _, err := fmt.Fprintf(f, "%s\n", m); err != nil {
			return err
		}
	}
	return nil
}
