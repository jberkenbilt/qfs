package filter

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/repofiles"
	"path/filepath"
	"regexp"
	"strings"
)

type filterGroup struct {
	fullPath map[string]struct{} // applies to full path; checked only for entire path
	path     map[string]struct{} // applies to full path; checked at each level
	base     map[string]struct{} // applies to a single path element
	pattern  []*regexp.Regexp    // applies to last path element
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
	RepoRule
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
		fullPath: map[string]struct{}{},
		path:     map[string]struct{}{},
		base:     map[string]struct{}{},
	}
}

type Filter struct {
	groups     []*filterGroup
	junk       *regexp.Regexp
	includeDot *bool
}

func (f *Filter) defaultInclude() bool {
	if f.includeDot != nil {
		return *f.includeDot
	}
	if len(f.groups[Include].path) == 0 &&
		len(f.groups[Include].base) == 0 &&
		len(f.groups[Include].pattern) == 0 {
		return true
	}
	return false
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
	if g == Include {
		// For any included path, include all ancestor directories as full paths only. We
		// don't have any way to do something like this for base or pattern because we
		// don't know in advance what the ancestor directories will be. That situation is
		// documented as a known issue in README.md and exercised in the test suite.
		cur := val
		for cur != "." {
			cur = filepath.Dir(cur)
			f.groups[g].fullPath[cur] = struct{}{}
		}
	}
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
	f.includeDot = &val
}

// HasImplicitIncludes indicates whether the filter has any pattern or base
// include rules. If so, the filter can't be used safely with sync. This is
// discussed in README.md and filter.go.
func (f *Filter) HasImplicitIncludes() bool {
	return len(f.groups[Include].base) > 0 || len(f.groups[Include].pattern) > 0
}

func (fg *filterGroup) match(path string, base string, checkFullPath bool) bool {
	if checkFullPath {
		if _, ok := fg.fullPath[path]; ok {
			return true
		}
	}
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
// Note that Junk applies only to the last path element. If override is not nil,
// it is called after junk, and if it returns true, the file is included without
// checking other filters.
func IsIncluded(
	path string,
	repoRules bool,
	filters ...*Filter,
) (included bool, group Group) {
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
	base := filepath.Base(path)
	for _, f := range filters {
		if f.junk != nil && f.junk.MatchString(base) {
			return false, Junk
		}
	}

	if repoRules {
		// When working with repositories, override the filters' treatment of the .qfs
		// directory. Most of the contents are specific to the local site, and it's
		// important for filters to be included across all sites.
		if strings.HasPrefix(path, repofiles.Filters+"/") {
			return true, RepoRule
		} else if path == repofiles.Top {
			return true, RepoRule
		} else if strings.HasPrefix(path, repofiles.Top+"/") {
			return false, RepoRule
		}
	}

	if len(filters) == 0 {
		// No filters = include everything.
		return true, Default
	}

	// Check prune. Prune is checked at each path level. Nothing can override prune,
	// so we can return immediately if we get a match.
	cur := path
	for { // each path level
		base = filepath.Base(cur)
		for _, f := range filters {
			if f.groups[Prune].match(cur, base, false) {
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
	usedFalseDefault := false
	for _, f := range filters {
		if !f.defaultInclude() {
			// If any filter has defaultInclude false, that becomes the overall default.
			defaultInclude = false
		}
		cur = path
	thisFilter:
		for {
			base = filepath.Base(cur)
			if f.groups[Include].match(cur, base, cur == path) {
				// We can stop testing this filter, but the file could still be explicitly
				// excluded by a later filter.
				includeMatched = true
				break thisFilter
			}
			if f.groups[Exclude].match(cur, base, false) {
				return false, Exclude
			}
			cur = filepath.Dir(cur)
			if cur == "." {
				if !f.defaultInclude() {
					usedFalseDefault = true
				}
				break
			}
		}
	}
	if includeMatched && !usedFalseDefault {
		// This was explicitly included by all filters.
		return true, Include
	}
	return defaultInclude, Default
}

func (f *Filter) ReadLine(group Group, line string) error {
	switch {
	case line == ".":
		switch group {
		case Exclude:
			f.SetDefaultInclude(false)
		case Include:
			f.SetDefaultInclude(true)
		default:
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
			// TEST: CAN'T COVER: the regexp will always be valid because it was constructed
			// with QuoteMeta. If this condition happens, it would indicate a bug in the
			// code.
			return err
		}
	default:
		f.AddPath(group, line)
	}
	return nil
}

func (f *Filter) ReadFile(path *fileinfo.Path, pruneOnly bool) error {
	const (
		stTop = iota
		stGroup
		stIgnore
	)
	r, err := path.Open()
	if err != nil {
		return fmt.Errorf("open %s: %w", path.Path(), err)
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
				// Read resolves filters relative to the current filter to enable filters to be
				// downloaded from the repository and applied in place of local filters.
				return f.ReadFile(path.Relative(toRead), pruneOnly)
			}()
			if err != nil {
				return fmt.Errorf("%s:%d: %w", path.Path(), lineNo, err)
			}
		case strings.HasPrefix(line, prefixJunk):
			if err := f.SetJunk(line[len(prefixJunk):]); err != nil {
				return fmt.Errorf("%s:%d: %w", path.Path(), lineNo, err)
			}
			state = stTop
		default:
			if state == stIgnore {
				continue
			} else if state != stGroup {
				return fmt.Errorf("%s:%d: path not expected here", path.Path(), lineNo)
			}
			err = f.ReadLine(group, line)
			if err != nil {
				return fmt.Errorf("%s:%d: %w", path.Path(), lineNo, err)
			}
		}
	}
	return nil
}
