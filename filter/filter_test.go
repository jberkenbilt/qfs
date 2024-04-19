package filter_test

import (
	"github.com/jberkenbilt/qfs/filter"
	"strings"
	"testing"
)

func TestFilter(t *testing.T) {
	f1 := filter.New()
	f2 := filter.New()
	// f1 has default include; check toggles f2 default include to exercise that only
	// one filter needs false for false to prevail
	f1.SetDefaultInclude(true)
	check := func(p string, expIncluded bool, expGroup filter.Group) {
		t.Helper()
		f2.SetDefaultInclude(false)
		included, group := filter.IsIncluded(p, f1, f2)
		if included != expIncluded {
			t.Errorf("%s: included = %v, wanted = %v", p, included, expIncluded)
		}
		if group != expGroup {
			t.Errorf("%s: group = %v, wanted = %v", p, group, expGroup)
		}
		f2.SetDefaultInclude(true)
		newIncluded, newGroup := filter.IsIncluded(p, f1, f2)
		if group != newGroup {
			t.Errorf("%s: junk changed when default changed", p)
		}
		if (newIncluded != included) != (group == filter.Default) {
			t.Errorf("%s: unexpected default status", p)
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
	f2.AddPath(filter.Prune, "one/prune")
	f2.AddPath(filter.Include, "one/prune/include") // ignored
	f1.AddBase(filter.Prune, "no-sync")
	f1.AddBase(filter.Include, "RCS")
	f2.AddBase(filter.Exclude, "always-exclude")
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

	gotPanic := ""
	func() {
		defer func() {
			gotPanic = recover().(string)
		}()
		_, _ = filter.IsIncluded("/oops", f1)
	}()
	if gotPanic != "Filter.IsIncluded must be called with a relative path" {
		t.Errorf("wrong panic: %s", gotPanic)
	}

	gotPanic = ""
	func() {
		defer func() {
			gotPanic = recover().(string)
		}()
		_, _ = filter.IsIncluded("oops")
	}()
	if gotPanic != "Filter.IsIncluded must be passed at least one filter" {
		t.Errorf("wrong panic: %s", gotPanic)
	}
}
