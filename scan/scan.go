package scan

import (
	"fmt"
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

func (s *Scan) Run() (fileinfo.Provider, error) {
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
		return nil, err
	}
	return files, nil
}
