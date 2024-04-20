package scan

import (
	"fmt"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/traverse"
	"os"
	"path/filepath"
)

type Options func(*Scan) error

type Scan struct {
	input   string
	filters []*filter.Filter
	sameDev bool
	cleanup bool
	db      string
	stdout  bool
	long    bool
}

func New(input string, options ...Options) (*Scan, error) {
	q := &Scan{
		input: input,
	}
	for _, fn := range options {
		if err := fn(q); err != nil {
			return nil, err
		}
	}
	return q, nil
}

func WithFilters(filters []*filter.Filter) func(*Scan) error {
	return func(s *Scan) error {
		s.filters = filters
		return nil
	}
}

func WithSameDev(sameDev bool) func(*Scan) error {
	return func(s *Scan) error {
		s.sameDev = sameDev
		return nil
	}
}

func WithCleanup(cleanup bool) func(*Scan) error {
	return func(s *Scan) error {
		s.cleanup = cleanup
		return nil
	}
}

func WithDb(db string) func(*Scan) error {
	return func(s *Scan) error {
		s.db = db
		return nil
	}
}

func WithStdout(stdout bool, long bool) func(*Scan) error {
	return func(s *Scan) error {
		s.stdout = stdout
		s.long = long
		return nil
	}
}

func (s *Scan) Run() error {
	progName := filepath.Base(os.Args[0])
	files, err := traverse.Traverse(
		s.input,
		s.filters,
		s.sameDev,
		s.cleanup,
		func(msg string) {
			_, _ = fmt.Fprintf(os.Stderr, "%s: %s\n", progName, msg)
		},
		func(err error) {
			_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", progName, err)
		},
	)
	if err != nil {
		return err
	}
	if s.db != "" {
		return database.WriteDb(s.db, files)
	} else if s.stdout {
		return files.ForEach(func(f *fileinfo.FileInfo) error {
			fmt.Printf("%013d %c %08d %04o", f.ModTime.UnixMilli(), f.FileType, f.Size, f.Permissions)
			if s.long {
				fmt.Printf(" %05d %05d", f.Uid, f.Gid)
			}
			fmt.Printf(" %s %s", f.ModTime.Format("2006-01-02 15:04:05.000Z07:00"), f.Path)
			if f.FileType == fileinfo.TypeLink {
				fmt.Printf(" -> %s", f.Special)
			} else if f.FileType == fileinfo.TypeBlockDev || f.FileType == fileinfo.TypeCharDev {
				fmt.Printf(" %s", f.Special)
			}
			fmt.Println("")
			return nil
		})
	}
	return nil
}
