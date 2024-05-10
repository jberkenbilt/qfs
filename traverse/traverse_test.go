package traverse_test

import (
	"errors"
	"fmt"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/traverse"
	"golang.org/x/exp/maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestTraverse(t *testing.T) {
	uid := os.Getuid()
	gid := os.Getgid()
	if uid == 0 || gid == 0 {
		t.Fatal("this test must not be run as root")
	}

	tmp := t.TempDir()
	err := os.WriteFile(filepath.Join(tmp, "potato"), []byte("salad"), 0644)
	if err != nil {
		t.Fatal(err.Error())
	}
	j := func(s string) string {
		return filepath.Join(tmp, s)
	}
	err = os.Symlink("potato", j("quack"))
	if err != nil {
		t.Fatalf("symlink: %v", err)
	}
	err = os.Symlink("salad", j("baa"))
	if err != nil {
		t.Fatalf("symlink: %v", err)
	}
	err = os.MkdirAll(j("one/two/three"), 0777)
	if err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	err = os.WriteFile(filepath.Join(tmp, "one/two/moo"), []byte("oink"), 0644)
	if err != nil {
		t.Fatalf("write file: %v", err)
	}
	err = syscall.Mkfifo(j("one/flute"), 0666)
	if err != nil {
		t.Fatalf("mkfifo: %v", err)
	}
	socketPath := j("one/lost-sock")
	_ = os.Remove(socketPath)
	sock, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("unix-domain socket listen: %v", err)
	}
	defer func() { _ = sock.Close() }()
	var allErrors []error
	errFn := func(err error) {
		allErrors = append(allErrors, err)
	}
	var messages []string
	notifyFn := func(msg string) {
		messages = append(messages, msg)
	}
	tr, err := traverse.New(tmp)
	if err != nil {
		t.Fatal(err.Error())
	}
	files, err := tr.Traverse(notifyFn, errFn)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(allErrors) > 0 {
		t.Errorf(errors.Join(allErrors...).Error())
	}
	if len(messages) > 0 {
		t.Errorf("got messages: %#v", messages)
	}
	all, _ := files.Database()
	keys := maps.Keys(all)
	sort.Strings(keys)
	expKeys := []string{
		".",
		"potato",
		"quack",
		"baa",
		"one",
		"one/flute",
		"one/lost-sock",
		"one/two",
		"one/two/three",
		"one/two/moo",
	}
	sort.Strings(expKeys)
	if !slices.Equal(expKeys, keys) {
		t.Fatalf("wrong entries: %#v", keys)
	}
	if all["quack"].Special != "potato" || all["baa"].Special != "salad" {
		t.Errorf("wrong link targets: %#v, %#v", all["quack"], all["baa"])
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
	if !(all["quack"].FileType == fileinfo.TypeLink &&
		all["one/flute"].FileType == fileinfo.TypePipe &&
		all["one/lost-sock"].FileType == fileinfo.TypeSocket &&
		all["potato"].FileType == fileinfo.TypeFile &&
		all["one"].FileType == fileinfo.TypeDirectory) {
		t.Errorf("wrong file types")
	}
	restorePerms := func() {
		_ = os.Chmod(j("one/two"), 0755)
		_ = os.Chmod(j("baa"), 0644)
	}
	defer restorePerms()
	_ = os.Chmod(j("one/two"), 0)
	_ = os.Chmod(j("baa"), 0)
	tr, err = traverse.New(tmp)
	if err != nil {
		t.Errorf("error returned: %v", err)
	}
	files, err = tr.Traverse(notifyFn, errFn)
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
	if len(messages) > 0 {
		t.Errorf("got messages: %#v", messages)
	}
	all, _ = files.Database()
	keys = maps.Keys(all)
	sort.Strings(keys)
	expKeys = []string{
		".",
		"potato",
		"quack",
		"baa",
		"one",
		"one/flute",
		"one/lost-sock",
		"one/two",
	}
	sort.Strings(expKeys)
	if !slices.Equal(expKeys, keys) {
		t.Errorf("wrong entries: %#v", keys)
	}

	restorePerms()
	allErrors = nil
	messages = nil
	tr, err = traverse.New(tmp, traverse.WithFilesOnly(true))
	if err != nil {
		t.Errorf("error returned: %v", err)
	}
	files, err = tr.Traverse(notifyFn, errFn)
	if err != nil {
		t.Errorf("error returned: %v", err)
	}
	if len(allErrors) > 0 {
		t.Errorf("got errors: %#v", allErrors)
	}
	if len(messages) > 0 {
		t.Errorf("got messages: %#v", messages)
	}
	all, _ = files.Database()
	keys = maps.Keys(all)
	sort.Strings(keys)
	expKeys = []string{
		"potato",
		"quack",
		"baa",
		"one/two/moo",
	}
	sort.Strings(expKeys)
	if !slices.Equal(expKeys, keys) {
		t.Errorf("wrong entries: %#v", keys)
	}
}

func TestDevices(t *testing.T) {
	for _, noSpecial := range []bool{false, true} {
		tr, err := traverse.New(
			"/dev",
			traverse.WithSameDev(true),
			traverse.WithNoSpecial(noSpecial),
		)
		if err != nil {
			t.Fatal(err.Error())
		}
		files, err := tr.Traverse(
			func(string) {},
			func(error) {},
		)
		if err != nil {
			t.Fatalf("can't traverse /dev: %v", err)
		}
		foundChar := false
		foundBlock := false
		db, _ := files.Database()
		for _, f := range db {
			if f.FileType == fileinfo.TypeCharDev {
				foundChar = true
			}
			if f.FileType == fileinfo.TypeBlockDev {
				foundBlock = true
			}
			if foundBlock && foundChar {
				// Stop traversing -- we got what we need
				break
			}
		}
		if foundChar == noSpecial {
			t.Errorf("didn't find any character devices")
		}
		if foundBlock == noSpecial {
			t.Errorf("didn't find any block devices")
		}
	}
}

func TestNoRoot(t *testing.T) {
	_, err := traverse.New(
		"/does-not-exist",
	)
	if err == nil || !strings.HasPrefix(err.Error(), "lstat /does-not-exist:") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestFilterInteraction(t *testing.T) {
	f := filter.New()
	_ = f.SetJunk("~$")
	f.AddPath(filter.Prune, "prune")
	f.AddPath(filter.Exclude, "one")
	f.AddBase(filter.Include, "two")
	tmp := t.TempDir()
	j := func(p string) string {
		return filepath.Join(tmp, p)
	}
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err.Error())
		}
	}
	check(os.MkdirAll(j("one"), 0777))
	check(os.MkdirAll(j("prune/peach/plum"), 0777))
	// We don't traverse into prune, so we won't see junk there
	check(os.WriteFile(j("prune/peach/plum/ignored~"), []byte("not seen"), 0666))
	check(os.WriteFile(j("one/two"), []byte("potato"), 0666))
	check(os.MkdirAll(j("two"), 0777))
	check(os.WriteFile(j("two/712818281828459045"), []byte("not pi"), 0666))
	check(os.WriteFile(j("two/pie~"), []byte("not pi"), 0666))
	check(os.MkdirAll(j("three/four"), 0777))
	// Junk is removed from excluded directories
	check(os.WriteFile(j("three/1416~"), []byte("not pi"), 0666))
	check(os.WriteFile(j("three/four/five~"), []byte("permission denied"), 0666))
	defer func() { _ = os.Chmod(j("three/four"), 0755) }()
	check(os.Chmod(j("three/four"), 0555))
	var messages []string
	var allErrors []string
	tr, err := traverse.New(
		tmp,
		traverse.WithFilters([]*filter.Filter{f}),
		traverse.WithCleanup(true),
	)
	if err != nil {
		t.Fatal(err.Error())
	}
	files, err := tr.Traverse(
		func(msg string) {
			messages = append(messages, msg)
		},
		func(e error) {
			allErrors = append(allErrors, e.Error())
		},
	)
	if err != nil {
		t.Fatalf("traverse failed: %v", err)
	}
	allFiles, _ := files.Database()
	paths := maps.Keys(allFiles)
	expPaths := []string{
		"one/two",
		"two",
		"two/712818281828459045",
	}
	expMessages := []string{
		"removing two/pie~",
		"removing three/1416~",
	}
	sort.Strings(expPaths)
	sort.Strings(expMessages)
	sort.Strings(messages)
	// Paths wil already be sorted.
	if !slices.Equal(paths, expPaths) {
		t.Errorf("wrong paths: %#v", paths)
	}
	if !slices.Equal(messages, expMessages) {
		t.Errorf("wrong messages: %#v", messages)
	}
	if !(len(allErrors) == 1 && strings.HasPrefix(
		allErrors[0],
		fmt.Sprintf("remove junk %s:", j("three/four/five~")),
	)) {
		t.Errorf("wrong errors: %#v", allErrors)
	}
}
