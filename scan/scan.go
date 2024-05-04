package scan

import (
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/traverse"
	"os"
)

type Options func(*Scan)

type Scan struct {
	input     string
	filters   []*filter.Filter
	sameDev   bool
	cleanup   bool
	filesOnly bool
	noSpecial bool
}

func New(input string, options ...Options) (*Scan, error) {
	q := &Scan{
		input: input,
	}
	for _, fn := range options {
		fn(q)
	}
	return q, nil
}

func WithFilters(filters []*filter.Filter) func(*Scan) {
	return func(s *Scan) {
		s.filters = filters
	}
}

func WithSameDev(sameDev bool) func(*Scan) {
	return func(s *Scan) {
		s.sameDev = sameDev
	}
}

func WithCleanup(cleanup bool) func(*Scan) {
	return func(s *Scan) {
		s.cleanup = cleanup
	}
}

func WithNoSpecial(noSpecial bool) func(*Scan) {
	return func(s *Scan) {
		s.noSpecial = noSpecial
	}
}

func WithFilesOnly(filesOnly bool) func(*Scan) {
	return func(s *Scan) {
		s.filesOnly = filesOnly
	}
}

// Run scans the input source per the scanner's configuration. The caller must
// call Close on the resulting provider.
func (s *Scan) Run() (fileinfo.Provider, error) {
	st, err := os.Stat(s.input)
	if err != nil {
		return nil, err
	}
	var files fileinfo.Provider
	if st.IsDir() {
		var tr *traverse.Traverser
		tr, err = traverse.New(
			s.input,
			traverse.WithFilters(s.filters),
			traverse.WithSameDev(s.sameDev),
			traverse.WithCleanup(s.cleanup),
			traverse.WithFilesOnly(s.filesOnly),
			traverse.WithNoSpecial(s.noSpecial),
		)
		if err != nil {
			// TEST: NOT COVERED. By this point, any error returned by Traverse has already
			// been caught.
			return nil, err
		}
		files, err = tr.Traverse(nil, nil)
	} else {
		files, err = database.OpenFile(
			s.input,
			database.WithFilters(s.filters),
			database.WithFilesOnly(s.filesOnly),
			database.WithNoSpecial(s.noSpecial),
		)
	}
	if err != nil {
		return nil, err
	}
	return files, nil
}
