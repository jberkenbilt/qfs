package filter

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type filterGroup struct {
	path    map[string]struct{} // applies to full path
	base    map[string]struct{} // applies to a single path element
	pattern []*regexp.Regexp    // applies to last path element
}

type Group int

// The number of groups must equal the size of the groups slice created in New.
// These constants are indexes into groups.
const (
	Prune Group = iota
	Include
	Exclude
)

// These constants are sentinels used/returned by filter status logic.
const (
	NoGroup Group = -1 - iota
	Junk
	Default
)

const (
	kwdPrune   = ":prune:"
	kwdInclude = ":include:"
	kwdExclude = ":exclude:"
	prefixRead = ":read:"
	prefixJunk = ":junk:"
	prefixRe   = ":re:"
	prefixBase = "*/"
	prefixExt  = "*."
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
	if val == "" {
		return fmt.Errorf("empty pattern not allowed")
	}
	re, err := regexp.Compile(val)
	if err != nil {
		return fmt.Errorf("regexp error on %s: %w", val, err)
	}
	f.groups[g].pattern = append(f.groups[g].pattern, re)
	return nil
}

func (f *Filter) SetJunk(val string) error {
	if f.junk != nil {
		return errors.New("only one junk directive allowed per filter (including nested)")
	}
	if val == "" {
		return fmt.Errorf("empty pattern not allowed")
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

// IsIncluded tests whether the path is included by all the given filters. The
// highest-priority matching group that caused the decision is returned. The
// groups in decreasing priority are Junk, Prune, Include, Exclude, and Default.
// Note that Junk applies only to the last path element.
func IsIncluded(path string, filters ...*Filter) (included bool, group Group) {
	// Iterate on the path, starting at the path and going up the directory
	// hierarchy, until there is a conclusive result. If none, use the default for
	// the filter. We check junk and prune all the way up first for all filters, then
	// include and exclude all the way up. This makes prune and junk strongest,
	// followed by include, and then exclude. So if you have the path `a/b/c/d`, if
	// `a/b` is pruned, it will not be considered even if `a/b/c` is included. If
	// `a/b` is excluded and `a/b/c` is included, `a/b/c` will be considered
	// included, but `a/b/x` would not. At each point, check explicit matches before
	// patterns.

	if filepath.IsAbs(path) {
		panic("Filter.IsIncluded must be called with a relative path")
	}
	if len(filters) == 0 {
		// No filters = include everything.
		return true, Default
	}

	base := filepath.Base(path)
	for _, f := range filters {
		if f.junk != nil && f.junk.MatchString(base) {
			return false, Junk
		}
	}

	// Check prune. Prune is checked at each path level. Nothing can override prune,
	// so we can return immediately if we get a match.
	cur := path
	for { // each path level
		base = filepath.Base(cur)
		for _, f := range filters {
			if f.groups[Prune].match(cur, base) {
				return false, Prune
			}
		}
		cur = filepath.Dir(cur)
		if cur == "." {
			break
		}
	}

	// Check include/exclude. A lower directory include can override a higher
	// directory exclude, and a path needs to be included by all filters to be
	// included.
	includeMatched := false
	defaultInclude := true
thisFilter:
	for _, f := range filters {
		if !f.defaultInclude {
			// If any filter has defaultInclude false, that becomes the overall default.
			defaultInclude = false
		}
		cur = path
		for {
			base = filepath.Base(cur)
			if f.groups[Include].match(cur, base) {
				// We can stop testing this filter, but the file could still be explicitly
				// excluded by a later filter.
				includeMatched = true
				break thisFilter
			}
			if f.groups[Exclude].match(cur, base) {
				return false, Exclude
			}
			cur = filepath.Dir(cur)
			if cur == "." {
				break
			}
		}
	}
	if includeMatched {
		// This was explicitly included by at least one filter and not explicitly
		// excluded by any filter.
		return true, Include
	}
	return defaultInclude, Default
}

func (f *Filter) ReadLine(group Group, line string) error {
	switch {
	case line == ".":
		if group == Exclude {
			f.SetDefaultInclude(false)
		} else if group == Include {
			f.SetDefaultInclude(true)
		} else {
			return errors.New("default path directive only allowed in include or exclude")
		}
	case strings.HasPrefix(line, prefixRe):
		if err := f.AddPattern(group, line[len(prefixRe):]); err != nil {
			return err
		}
	case strings.HasPrefix(line, prefixBase):
		f.AddBase(group, line[len(prefixBase):])
	case strings.HasPrefix(line, prefixExt):
		if err := f.AddPattern(group, regexp.QuoteMeta("."+line[len(prefixExt):])+`$`); err != nil {
			// Testing note: no way to actually get an error here...
			return err
		}
	default:
		f.AddPath(group, line)
	}
	return nil
}

func (f *Filter) ReadFile(filename string, pruneOnly bool) error {
	const (
		stTop = iota
		stGroup
		stIgnore
	)
	r, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open %s: %w", filename, err)
	}
	defer func() { _ = r.Close() }()
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)
	state := stTop
	group := NoGroup
	lineNo := 0
	if pruneOnly {
		f.SetDefaultInclude(true)
	}
	for scanner.Scan() {
		line := scanner.Text()
		lineNo++
		if strings.HasPrefix(line, "#") {
			// # is a comment character only at the beginning of the line
			continue
		}
		// If someone wants to end a file with a space on purpose, they can use a pattern
		// and wrap it in parentheses.
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		switch {
		case line == kwdPrune:
			state = stGroup
			group = Prune
		case line == kwdInclude:
			if pruneOnly {
				state = stIgnore
			} else {
				state = stGroup
				group = Include
			}
		case line == kwdExclude:
			if pruneOnly {
				state = stIgnore
			} else {
				state = stGroup
				group = Exclude
			}
		case strings.HasPrefix(line, prefixRead):
			toRead := line[len(prefixRead):]
			err := func() error {
				return f.ReadFile(toRead, pruneOnly)
			}()
			if err != nil {
				return fmt.Errorf("%s:%d: %w", filename, lineNo, err)
			}
		case strings.HasPrefix(line, prefixJunk):
			if err := f.SetJunk(line[len(prefixJunk):]); err != nil {
				return fmt.Errorf("%s:%d: %w", filename, lineNo, err)
			}
			state = stTop
		default:
			if state == stIgnore {
				continue
			} else if state != stGroup {
				return fmt.Errorf("%s:%d: path not expected here", filename, lineNo)
			}
			err = f.ReadLine(group, line)
			if err != nil {
				return fmt.Errorf("%s:%d: %w", filename, lineNo, err)
			}
		}
	}
	return nil
}
