package filter_test

import (
	"github.com/jberkenbilt/qfs/filter"
	"strings"
	"testing"
)

func TestFilter(t *testing.T) {
	f := filter.New()
	check := func(p string, expIncluded bool, expGroup filter.Group) {
		t.Helper()
		f.SetDefaultInclude(false)
		included, group := f.IsIncluded(p)
		if included != expIncluded {
			t.Errorf("%s: included = %v, wanted = %v", p, included, expIncluded)
		}
		if group != expGroup {
			t.Errorf("%s: group = %v, wanted = %v", p, group, expGroup)
		}
		f.SetDefaultInclude(true)
		newIncluded, newGroup := f.IsIncluded(p)
		if group != newGroup {
			t.Errorf("%s: junk changed when default changed", p)
		}
		if (newIncluded != included) != (group == filter.Default) {
			t.Errorf("%s: unexpected default status", p)
		}
	}
	check("a/b/c", false, filter.Default)
	err := f.SetJunk(`???*`)
	if err == nil || !strings.HasPrefix(err.Error(), "regexp error on ???*:") {
		t.Errorf("wrong error: %v", err)
	}
	err = f.SetJunk(`^\.?#|~$`)
	if err != nil {
		t.Error(err.Error())
	}
	err = f.SetJunk("oops")
	if err == nil || !strings.HasPrefix(err.Error(), "only one junk directive allowed") {
		t.Errorf("wrong error: %v", err)
	}
	check("one/two/three~", false, filter.Junk)
	check("one/two/#three", false, filter.Junk)
	check("one/two/.#three", false, filter.Junk)
	check("one/two/three.#four~five", false, filter.Default)

	addPattern := func(g filter.Group, v string) {
		t.Helper()
		if err := f.AddPattern(g, v); err != nil {
			t.Fatalf("add pattern: %v", err)
		}
	}

	err = f.AddPattern(filter.Include, "???*")
	if err == nil || !strings.HasPrefix(err.Error(), "regexp error on ???*:") {
		t.Errorf("wrong error: %v", err)
	}

	f.AddPath(filter.Exclude, "one/exclude")
	f.AddPath(filter.Include, "one/exclude/include")
	f.AddPath(filter.Prune, "one/prune")
	f.AddPath(filter.Include, "one/prune/include") // ignored
	f.AddBase(filter.Prune, "no-sync")
	f.AddBase(filter.Include, "RCS")
	f.AddBase(filter.Exclude, "always-exclude")
	addPattern(filter.Include, ",v$")
	check("one/exclude/include/yes", true, filter.Include)
	check("one/exclude/nope/anything", false, filter.Exclude)
	check("one/exclude/something/RCS/yes", true, filter.Include)
	check("one/prune/include/nope", false, filter.Prune)
	check("one/prune/RCS/a,v", false, filter.Prune)
	check("a/no-sync/something", false, filter.Prune)
	check("a/always-exclude/something/a,v", true, filter.Include)
	check("a/always-exclude/something/else", false, filter.Exclude)
	check("a/always-exclude/RCS/yes", true, filter.Include)
	check("a/potato/salad/default", false, filter.Default)

	gotPanic := ""
	func() {
		defer func() {
			gotPanic = recover().(string)
		}()
		_, _ = f.IsIncluded("/oops")
	}()
	if gotPanic != "Filter.IsIncluded must be called with a relative path" {
		t.Errorf("wrong panic: %s", gotPanic)
	}
}
