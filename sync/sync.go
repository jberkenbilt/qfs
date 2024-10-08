package sync

import (
	"errors"
	"fmt"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/diff"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/localsource"
	"github.com/jberkenbilt/qfs/misc"
	"github.com/jberkenbilt/qfs/scan"
	"io/fs"
	"os"
)

type Options func(*Sync)

type Sync struct {
	srcDir  string
	destDir string
	filters []*filter.Filter
	noOp    bool
}

func New(srcDir, destDir string, options ...Options) (*Sync, error) {
	s := &Sync{
		srcDir:  srcDir,
		destDir: destDir,
	}
	for _, fn := range options {
		fn(s)
	}
	for _, f := range s.filters {
		if f.HasImplicitIncludes() {
			// See README.md and filter.go -- search for fullPath
			return nil, fmt.Errorf("sync doesn't work with filters that have pattern or base include rules")
		}
	}
	return s, nil
}

func WithFilters(filters []*filter.Filter) Options {
	return func(s *Sync) {
		s.filters = filters
	}
}

func WithNoOp(noOp bool) Options {
	return func(s *Sync) {
		s.noOp = noOp
	}
}

func ApplyChanges(
	src fileinfo.Source,
	dest fileinfo.Source,
	diffResult *diff.Result,
	destDb database.Database,
	numWorkers int,
) error {
	// Apply changes. Possible enhancement: make sure every directory we have to
	// modify (by adding or removing files) is writable first, and if we change it,
	// change it back. For now, if we try to modify a read-only directory, it will be
	// an error. The user can change the permissions and run again. The pull
	// operation will restore the permissions.

	// Remove what needs to be removed, then add/modify, then apply permission
	// changes. We ignore ownerships, directory modification times, and special
	// files.
	for _, rm := range diffResult.Rm {
		path := fileinfo.NewPath(dest, rm.Path).Path()
		misc.Message("removing %s", rm.Path)
		if err := os.RemoveAll(path); err != nil {
			// TEST: NOT COVERED
			return fmt.Errorf("remove %s: %w", path, err)
		}
		if destDb != nil {
			delete(destDb, rm.Path)
		}
	}

	// Make sure files we are changing will be writable. We will set the correct
	// permissions when we replace them.
	for _, ch := range diffResult.Change {
		path := fileinfo.NewPath(dest, ch.Path).Path()
		if ch.FileType == fileinfo.TypeFile {
			err := os.Chmod(path, fs.FileMode(ch.Permissions|0o600))
			if err != nil {
				// TEST: NOT COVERED
				return fmt.Errorf("%s: make writable: %w", path, err)
			}
		}
	}

	// Concurrently pull changed files from the repository. This sets permissions and modification time.
	c := make(chan *fileinfo.FileInfo, numWorkers)
	var allErrors []error
	go func() {
		for _, info := range diffResult.Add {
			if destDb != nil {
				destDb[info.Path] = info
			}
			c <- info
		}
		for _, info := range diffResult.Change {
			if destDb != nil {
				destDb[info.Path] = info
			}
			c <- info
		}
		close(c)
	}()
	misc.DoConcurrently(
		func(c chan *fileinfo.FileInfo, errorChan chan error) {
			for info := range c {
				destPath := fileinfo.NewPath(dest, info.Path)
				downloaded, err := fileinfo.Retrieve(fileinfo.NewPath(src, info.Path), destPath)
				if err != nil {
					// TEST: NOT COVERED
					errorChan <- fmt.Errorf("retrieve %s: %w", info.Path, err)
				}
				if downloaded && info.FileType != fileinfo.TypeDirectory {
					misc.Message("copied %s", info.Path)
				}
			}
		},
		func(e error) {
			// TEST: NOT COVERED
			allErrors = append(allErrors, e)
		},
		c,
		numWorkers,
	)
	if len(allErrors) > 0 {
		// TEST: NOT COVERED
		return errors.Join(allErrors...)
	}
	for _, m := range diffResult.MetaChange {
		if m.Permissions == nil {
			// TEST: NOT COVERED -- we don't generate other kinds of changes in diff with sites
			continue
		}
		path := fileinfo.NewPath(dest, m.Info.Path).Path()
		misc.Message("chmod %04o %s", *m.Permissions, m.Info.Path)
		err := os.Chmod(path, os.FileMode(*m.Permissions))
		if err != nil {
			// TEST: NOT COVERED
			return fmt.Errorf("chmod %04o %s: %w", *m.Permissions, path, err)
		}
		if destDb != nil {
			destDb[m.Info.Path] = m.Info
		}
	}
	return nil
}

func (s *Sync) Sync() error {
	scanSrc, err := scan.New(
		s.srcDir,
		scan.WithFilters(s.filters),
		scan.WithNoSpecial(true),
	)
	if err != nil {
		return err
	}
	scanDest, err := scan.New(s.destDir)
	if err != nil {
		return err
	}
	dbSrc, err := scanSrc.Run()
	if err != nil {
		return err
	}
	dbDest, err := scanDest.Run()
	if err != nil {
		return err
	}
	d := diff.New(diff.WithNoOwnerships(true))
	diffResult, err := d.Run(dbDest, dbSrc)
	if err != nil {
		return err
	}
	if s.noOp {
		_ = diffResult.WriteDiff(os.Stdout, false)
	} else {
		err = ApplyChanges(
			localsource.New(s.srcDir),
			localsource.New(s.destDir),
			diffResult,
			nil,
			10,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
