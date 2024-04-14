package filter_test

import (
	"github.com/jberkenbilt/qfs/filter"
	"strings"
	"testing"
)

func TestFilter(t *testing.T) {
	f := filter.New()
	check := func(p string, expIncluded, expJunk, byDefault bool) {
		t.Helper()
		f.SetDefaultInclude(false)
		included, junk := f.IsIncluded(p)
		if included != expIncluded {
			t.Errorf("%s: included = %v, wanted = %v", p, included, expIncluded)
		}
		if junk != expJunk {
			t.Errorf("%s: junk = %v, wanted = %v", p, junk, expJunk)
		}
		f.SetDefaultInclude(true)
		newIncluded, newJunk := f.IsIncluded(p)
		if junk != newJunk {
			t.Errorf("%s: junk changed when default changed", p)
		}
		if (newIncluded != included) != byDefault {
			t.Errorf("%s: unexpected default status", p)
		}
	}
	check("a/b/c", false, false, true)
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
	check("one/two/three~", false, true, false)
	check("one/two/#three", false, true, false)
	check("one/two/.#three", false, true, false)
	check("one/two/three.#four~five", false, false, true)

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
	check("one/exclude/include/yes", true, false, false)
	check("one/exclude/nope/anything", false, false, false)
	check("one/exclude/something/RCS/yes", true, false, false)
	check("one/prune/include/nope", false, false, false)
	check("one/prune/RCS/a,v", false, false, false)
	check("a/no-sync/something", false, false, false)
	check("a/always-exclude/something/a,v", true, false, false)
	check("a/always-exclude/something/else", false, false, false)
	check("a/always-exclude/RCS/yes", true, false, false)
	check("a/potato/salad/default", false, false, true)

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
