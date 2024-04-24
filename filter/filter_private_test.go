package filter

import (
	"golang.org/x/exp/maps"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

func checkFile(
	t *testing.T,
	pruneOnly bool,
	filename string,
	prunePaths []string,
	pruneBase []string,
	prunePatterns []string,
	includePaths []string,
	includeBase []string,
	includePatterns []string,
	excludePaths []string,
	excludeBase []string,
	excludePatterns []string,
	junkRe string,
	defaultInclude bool,
) {
	t.Helper()
	f := New()
	if err := f.ReadFile(filename, pruneOnly); err != nil {
		t.Fatalf("read %s: %v", filename, err)
	}
	reString := func(re *regexp.Regexp) string {
		if re == nil {
			return ""
		}
		return re.String()
	}
	compareMap := func(what string, actual map[string]struct{}, exp []string) {
		actKeys := maps.Keys(actual)
		sort.Strings(actKeys)
		sort.Strings(exp)
		if !slices.Equal(actKeys, exp) {
			t.Errorf("%s: got %#v, wanted %#v", what, actKeys, exp)
		}
	}
	comparePatterns := func(what string, actual []*regexp.Regexp, exp []string) {
		var actKeys []string
		for _, p := range actual {
			actKeys = append(actKeys, reString(p))
		}
		sort.Strings(actKeys)
		sort.Strings(exp)
		if !slices.Equal(actKeys, exp) {
			t.Errorf("%s: got %#v, wanted %#v", what, actKeys, exp)
		}
	}

	compareMap("prune paths", f.groups[Prune].path, prunePaths)
	compareMap("prune base", f.groups[Prune].base, pruneBase)
	comparePatterns("prune patterns", f.groups[Prune].pattern, prunePatterns)
	compareMap("include paths", f.groups[Include].path, includePaths)
	compareMap("include base", f.groups[Include].base, includeBase)
	comparePatterns("include patterns", f.groups[Include].pattern, includePatterns)
	compareMap("exclude paths", f.groups[Exclude].path, excludePaths)
	compareMap("exclude base", f.groups[Exclude].base, excludeBase)
	comparePatterns("exclude patterns", f.groups[Exclude].pattern, excludePatterns)
	actJunk := reString(f.junk)
	if junkRe != actJunk {
		t.Errorf("junk: got %v, wanted %v", actJunk, junkRe)
	}
	if f.defaultInclude() != defaultInclude {
		t.Errorf("default include: got %v, wanted %v", f.defaultInclude(), defaultInclude)
	}
}

func TestFilter_ReadFile(t *testing.T) {
	checkFile(
		t,
		false,
		"testdata/filter1",
		[]string{ // prune paths
			"prune/this",
		},
		[]string{ // prune base
			"no-sync",
		},
		[]string{ // prune patterns
		},
		[]string{ // include paths
			"one/two",
			"three/four",
			"one/two/three/four",
		},
		[]string{ // include base
			"RCS",
		},
		[]string{ // include patterns
			`,v$`,
		},
		[]string{ // exclude paths
			"one/two/three",
		},
		[]string{ // exclude base
			"no-offsite",
		},
		[]string{ // exclude patterns
			`\.swp$`,
			`^cmake-build-.*$`,
		},
		`^\.?#|~$`, // junkRe
		true,       // defaultInclude
	)
	checkFile(
		t,
		true,
		"testdata/filter1",
		[]string{ // prune paths
			"prune/this",
		},
		[]string{ // prune base
			"no-sync",
		},
		[]string{ // prune patterns
		},
		[]string{ // include paths
		},
		[]string{ // include base
		},
		[]string{ // include patterns
		},
		[]string{ // exclude paths
		},
		[]string{ // exclude base
		},
		[]string{ // exclude patterns
		},
		`^\.?#|~$`, // junkRe
		true,       // defaultInclude
	)
	checkFile(
		t,
		false,
		"testdata/filter2",
		[]string{ // prune paths
		},
		[]string{ // prune base
		},
		[]string{ // prune patterns
		},
		[]string{ // include paths
		},
		[]string{ // include base
		},
		[]string{ // include patterns
			`,v$`,
		},
		[]string{ // exclude paths
		},
		[]string{ // exclude base
		},
		[]string{ // exclude patterns
			`^cmake-build-.*$`,
		},
		"",    // junkRe
		false, // defaultInclude
	)
}

func TestFileErrors(t *testing.T) {
	check := func(filename string, errPrefix string) {
		t.Helper()
		f := New()
		err := f.ReadFile(filename, false)
		if err == nil {
			t.Errorf("%s: no error", filename)
		} else if !strings.HasPrefix(err.Error(), errPrefix) {
			t.Errorf("%s: wrong error: %v", filename, err)
		}
	}
	check("does-not-exist", "open does-not-exist: ")
	check("testdata/bad1", "testdata/bad1:3: open testdata/does-not-exist: ")
	check("testdata/bad2", "testdata/bad2:4: path not expected here")
	check("testdata/bad3", "testdata/bad3:2: regexp error on ???*:")
	check("testdata/bad4", "testdata/bad4:3: only one junk directive")
	check("testdata/bad5", "testdata/bad5:3: default path directive only allowed in")
	check("testdata/bad6", "testdata/bad6:2: empty pattern not allowed")
	check("testdata/bad7", "testdata/bad7:1: empty pattern not allowed")
}

func TestDefault(t *testing.T) {
	f := New()
	if !f.defaultInclude() {
		t.Errorf("blank filter should have default include")
	}
	f.AddBase(Exclude, "z")
	f.AddBase(Prune, "y")
	if !f.defaultInclude() {
		t.Errorf("filter with no include rules should have default include")
	}
	f.AddBase(Include, "x")
	if f.defaultInclude() {
		t.Errorf("adding include rule should turn default to false")
	}
	if f.includeDot != nil {
		t.Errorf("includeDot was set, invalidating tests")
	}
	// Explicit toggling of default is tested in regular filter tests
}
