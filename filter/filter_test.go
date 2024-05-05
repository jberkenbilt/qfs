package filter_test

import (
	"github.com/jberkenbilt/qfs/filter"
	"strings"
	"testing"
)

func TestFilter(t *testing.T) {
	f1 := filter.New()
	// f1 has default include; check toggles f2 default include to exercise that only
	// one filter needs false for false to prevail
	check := func(p string, expIncluded bool, expGroup filter.Group) {
		t.Helper()
		for _, defaultInclude := range []bool{true, false} {
			f1.SetDefaultInclude(defaultInclude)
			included, group := filter.IsIncluded(p, false, f1)
			if group != expGroup {
				t.Errorf("%s: group = %v, wanted = %v", p, group, expGroup)
			} else if expGroup == filter.Default {
				if included != defaultInclude {
					t.Errorf("%s: included didn't match default include", p)
				}
			} else if included != expIncluded {
				t.Errorf("%s: included = %v, wanted = %v", p, included, expIncluded)
			}
		}
	}
	check("a/b/c", false, filter.Default)
	err := f1.SetJunk(`???*`)
	if err == nil || !strings.HasPrefix(err.Error(), "regexp error on ???*:") {
		t.Errorf("wrong error: %v", err)
	}
	err = f1.SetJunk(`^\.?#|~$`)
	if err != nil {
		t.Error(err.Error())
	}
	err = f1.SetJunk("oops")
	if err == nil || !strings.HasPrefix(err.Error(), "only one junk directive allowed") {
		t.Errorf("wrong error: %v", err)
	}
	check("one/two/three~", false, filter.Junk)
	check("one/two/#three", false, filter.Junk)
	check("one/two/.#three", false, filter.Junk)
	check("one/two/three.#four~five", false, filter.Default)

	err = f1.AddPattern(filter.Include, "???*")
	if err == nil || !strings.HasPrefix(err.Error(), "regexp error on ???*:") {
		t.Errorf("wrong error: %v", err)
	}

	addPattern := func(f *filter.Filter, g filter.Group, v string) {
		t.Helper()
		if err := f.AddPattern(g, v); err != nil {
			t.Fatalf("add pattern: %v", err)
		}
	}

	f1.AddPath(filter.Exclude, "one/exclude")
	f1.AddPath(filter.Include, "one/exclude/include")
	f1.AddPath(filter.Prune, "one/prune")
	f1.AddPath(filter.Include, "one/prune/include") // ignored
	f1.AddBase(filter.Prune, "no-sync")
	f1.AddBase(filter.Include, "RCS")
	f1.AddBase(filter.Exclude, "always-exclude")
	addPattern(f1, filter.Include, ",v$")
	// include overrides exclude
	check("one/exclude/include/yes", true, filter.Include)
	// junk only applies to last path element
	check("one/exclude/include/a~/yes", true, filter.Include)
	// simple path exclude
	check("one/exclude/nope/anything", false, filter.Exclude)
	// base include rule overrides exclude
	check("one/exclude/something/RCS/yes", true, filter.Include)
	// prune overrides include
	check("one/prune/include/nope", false, filter.Prune)
	// prune overrides base and pattern include
	check("one/prune/RCS/a,v", false, filter.Prune)
	// simple base prune
	check("a/no-sync/something", false, filter.Prune)
	// pattern include overrides exclude
	check("a/always-exclude/something/a,v", true, filter.Include)
	// simple base exclude
	check("a/always-exclude/something/else", false, filter.Exclude)
	// base include overrides base exclude
	check("a/always-exclude/RCS/yes", true, filter.Include)
	// not matched by any rule
	check("a/potato/salad/default", false, filter.Default)

	// No filters = include
	included, group := filter.IsIncluded("anything", false)
	if !(included && group == filter.Default) {
		t.Errorf("wrong behavior with no filters")
	}

	// Repo rules
	included, group = filter.IsIncluded(".qfs/pending", true)
	if included || group != filter.RepoRule {
		t.Errorf("wrong result for repo rules exclude")
	}
	included, group = filter.IsIncluded(".qfs/filters/x", true)
	if !included || group != filter.RepoRule {
		t.Errorf("wrong result for repo rules include")
	}

	// Multiple filters -- must be matched by all filters to be matched.
	f2 := filter.New()
	check2 := func(p string, f2Default bool, expGroup filter.Group) {
		t.Helper()
		f1.SetDefaultInclude(true)
		for _, defaultInclude := range []bool{true, false} {
			f2.SetDefaultInclude(defaultInclude)
			included, group = filter.IsIncluded(p, false, f1, f2)
			if f2Default && !defaultInclude {
				if included || group != filter.Default {
					t.Errorf("%s: wrong result when f2's default matched", p)
				}
			} else if group != expGroup {
				t.Errorf("%s: group = %v, wanted = %v", p, group, expGroup)
			} else if group == filter.Default {
				if included != defaultInclude {
					t.Errorf("%s: included != default include", p)
				}
			}
		}
	}

	f1.AddPath(filter.Include, "shared")
	f2.AddPath(filter.Include, "shared")
	f1.AddPath(filter.Include, "only-one")
	f2.AddPath(filter.Include, "only-two")
	// Matched by f1, default behavior from f2
	check2("a/always-exclude/RCS/yes", true, filter.Include)
	check2("only-one/anything", true, filter.Include)
	// Matched by both
	check2("shared/potato", false, filter.Include)
	// Matched by f2,default behavior from f1 (which is true)
	check2("only-two/potato", false, filter.Include)
	// Matched by both but expected by f1
	check2("shared/always-exclude/nope", false, filter.Exclude)

	gotPanic := ""
	func() {
		defer func() {
			gotPanic = recover().(string)
		}()
		_, _ = filter.IsIncluded("/oops", false, f1)
	}()
	if gotPanic != "Filter.IsIncluded must be called with a relative path" {
		t.Errorf("wrong panic: %s", gotPanic)
	}
}
