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
	base    map[string]struct{} // applies to last path element
	pattern []*regexp.Regexp    // applies to last path element
}

type Group int

// The number of groups must equal the size of the groups slice created in New.
// These constants are indexes into groups.
const (
	Include Group = iota
	Exclude
	Prune
)

// These constants are sentinels used/returned by filter status logic.
const (
	NoGroup Group = -1 - iota
	Junk
	Default
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

func (f *Filter) IsIncluded(path string) (included bool, group Group) {
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
			return false, Prune
		}
		if f.junk != nil && f.junk.MatchString(base) {
			return false, Junk
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
			return true, Include
		}
		if f.groups[Exclude].match(cur, base) {
			return false, Exclude
		}
		cur = filepath.Dir(cur)
		if cur == "." {
			break
		}
	}
	return f.defaultInclude, Default
}

func (f *Filter) ReadFile(filename string, pruneOnly bool) error {
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
			switch {
			case line == ".":
				if group == Exclude {
					f.SetDefaultInclude(false)
				} else if group == Include {
					f.SetDefaultInclude(true)
				} else {
					return fmt.Errorf(
						"%s:%d: default path directive only allowed in include or exclude",
						filename,
						lineNo,
					)
				}
			case strings.HasPrefix(line, prefixRe):
				if err := f.AddPattern(group, line[len(prefixRe):]); err != nil {
					return fmt.Errorf("%s:%d: %w", filename, lineNo, err)
				}
			case strings.HasPrefix(line, prefixBase):
				f.AddBase(group, line[len(prefixBase):])
			case strings.HasPrefix(line, prefixExt):
				if err := f.AddPattern(group, regexp.QuoteMeta("."+line[len(prefixExt):])+`$`); err != nil {
					// Testing note: no way to actually get an error here...
					return fmt.Errorf("%s:%d: %w", filename, lineNo, err)
				}
			default:
				f.AddPath(group, line)
			}
		}
	}
	return nil
}
