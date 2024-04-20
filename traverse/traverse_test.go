package traverse_test

import (
	"errors"
	"github.com/jberkenbilt/qfs/traverse"
	"golang.org/x/exp/maps"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTraverse(t *testing.T) {
	u, err := user.Current()
	if err != nil {
		t.Fatalf("unable to get current user: %v", err)
	}
	uid := func() uint32 {
		u, _ := strconv.Atoi(u.Uid)
		return uint32(u)
	}()
	gid := func() uint32 {
		u, _ := strconv.Atoi(u.Gid)
		return uint32(u)
	}()
	if uid == 0 || gid == 0 {
		t.Fatal("this test must not be run as root")
	}

	tmp := t.TempDir()
	err = os.WriteFile(filepath.Join(tmp, "potato"), []byte("salad"), 0644)
	if err != nil {
		t.Fatal(err.Error())
	}
	j := func(s string) string {
		return filepath.Join(tmp, s)
	}
	err = os.Symlink("potato", j("quack"))
	if err != nil {
		t.Errorf("symlink: %v", err)
	}
	err = os.Symlink("salad", j("baa"))
	if err != nil {
		t.Errorf("symlink: %v", err)
	}
	err = os.MkdirAll(j("one/two/three"), 0777)
	if err != nil {
		t.Errorf("mkdir: %v", err)
	}
	err = os.WriteFile(filepath.Join(tmp, "one/two/moo"), []byte("oink"), 0644)
	if err != nil {
		t.Errorf("write file: %v", err)
	}

	var allErrors []error
	errFn := func(err error) {
		allErrors = append(allErrors, err)
	}
	files, err := traverse.Traverse(tmp, nil, false, false, errFn)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(allErrors) > 0 {
		t.Errorf(errors.Join(allErrors...).Error())
	}
	all := map[string]*traverse.FileInfo{}
	var keys []string
	fn := func(f *traverse.FileInfo) error {
		all[f.Path] = f
		keys = append(keys, f.Path)
		return nil
	}
	_ = files.Flatten(fn)
	expKeys := []string{
		".",
		"potato",
		"quack",
		"baa",
		"one",
		"one/two",
		"one/two/three",
		"one/two/moo",
	}
	sort.Strings(expKeys)
	if !slices.Equal(expKeys, keys) {
		t.Errorf("wrong entries: %#v", keys)
	}
	if all["quack"].Target != "potato" || all["baa"].Target != "salad" {
		t.Errorf("wrong link targets: %#v, %#v", all["quack"], all["baaa"])
	}
	if all["one/two/moo"].Size != 4 {
		t.Error("wrong size for moo")
	}
	if time.Since(all["potato"].ModTime) > time.Second {
		t.Error("mod time is broken")
	}
	if !(all["potato"].Uid == uid && all["potato"].Gid == gid) {
		t.Errorf("uid/gid are broken: %#v", all["potato"])
	}
	_ = os.Chmod(j("one/two"), 0)
	defer func() { _ = os.Chmod(j("one/two"), 0755) }()
	files, err = traverse.Traverse(tmp, nil, false, false, errFn)
	if err != nil {
		t.Errorf("error returned: %v", err)
	}
	if len(allErrors) != 1 {
		t.Errorf("error wasn't reported")
	} else {
		err = allErrors[0]
		if !strings.HasPrefix(err.Error(), "read dir "+tmp+"/one/two:") {
			t.Errorf("wrong error: %v", err)
		}
	}
	maps.Clear(all)
	keys = nil
	_ = files.Flatten(fn)
	expKeys = []string{
		".",
		"potato",
		"quack",
		"baa",
		"one",
		"one/two",
	}
	sort.Strings(expKeys)
	if !slices.Equal(expKeys, keys) {
		t.Errorf("wrong entries: %#v", keys)
	}
}
