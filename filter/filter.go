package filter

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
)

type filterGroup struct {
	path    map[string]struct{} // applies to full path
	base    map[string]struct{} // applies to last path element
	pattern []*regexp.Regexp    // applies to last path element
}

type Group int

// The number of groups must equal the size of the groups slice created in New.
const (
	Include Group = iota
	Exclude
	Prune
)

func newFilterGroup() *filterGroup {
	return &filterGroup{
		path: map[string]struct{}{},
		base: map[string]struct{}{},
	}
}

type Filter struct {
	groups         []*filterGroup
	junk           *regexp.Regexp
	defaultInclude bool
}

func New() *Filter {
	return &Filter{
		groups: []*filterGroup{
			newFilterGroup(),
			newFilterGroup(),
			newFilterGroup(),
		},
	}
}

func (f *Filter) AddPath(g Group, val string) {
	f.groups[g].path[val] = struct{}{}
}

func (f *Filter) AddBase(g Group, val string) {
	f.groups[g].base[val] = struct{}{}
}

func (f *Filter) AddPattern(g Group, val string) error {
	re, err := regexp.Compile(val)
	if err != nil {
		return fmt.Errorf("regexp error on %s: %w", val, err)
	}
	f.groups[g].pattern = append(f.groups[g].pattern, re)
	return nil
}

func (f *Filter) SetJunk(val string) error {
	if f.junk != nil {
		return errors.New("setJunk may only be called once")
	}
	re, err := regexp.Compile(val)
	if err != nil {
		return fmt.Errorf("regexp error on %s: %w", val, err)
	}
	f.junk = re
	return nil
}

func (f *Filter) SetDefaultInclude(val bool) {
	f.defaultInclude = val
}

func (fg *filterGroup) match(path string, base string) bool {
	if _, ok := fg.path[path]; ok {
		return true
	}
	if _, ok := fg.base[base]; ok {
		return true
	}
	for _, p := range fg.pattern {
		if p.MatchString(base) {
			return true
		}
	}
	return false
}

func (f *Filter) IsIncluded(path string) (included bool, junk bool) {
	// Iterate on the path, starting at the path and going up the directory
	// hierarchy, until there is a conclusive result. If none, use the default for
	// the filter. We check junk and prune all the way up first, then include and
	// exclude all the way up. This makes prune and junk strongest, followed by
	// include, and then exclude. So if you have the path `a/b/c/d`, if `a/b` is
	// pruned, it will not be considered even if `a/b/c` is included. If `a/b` is
	// excluded and `a/b/c` is included, `a/b/c` will be considered included, but
	// `a/b/x` would not. At each point, check explicit matches before patterns.

	if filepath.IsAbs(path) {
		panic("Filter.IsIncluded must be called with a relative path")
	}

	cur := path
	for {
		base := filepath.Base(cur)
		if f.groups[Prune].match(cur, base) {
			return false, false
		}
		if f.junk != nil && f.junk.MatchString(base) {
			return false, true
		}
		cur = filepath.Dir(cur)
		if cur == "." {
			break
		}
	}
	cur = path
	for {
		base := filepath.Base(cur)
		if f.groups[Include].match(cur, base) {
			return true, false
		}
		if f.groups[Exclude].match(cur, base) {
			return false, false
		}
		cur = filepath.Dir(cur)
		if cur == "." {
			break
		}
	}
	return f.defaultInclude, false
}
