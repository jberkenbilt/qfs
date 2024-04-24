package qfs_test

import (
	"bytes"
	_ "embed"
	"fmt"
	"github.com/jberkenbilt/qfs/gztar"
	"github.com/jberkenbilt/qfs/qfs"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

//go:embed testdata/all-types.out
var allTypesOut []byte

//go:embed testdata/all-types-long.out
var allTypesOutLong []byte

//go:embed testdata/files-no-link.out
var filesOut []byte

func toLines(out []byte) []string {
	var lines []string
	for _, line := range strings.Split(string(out), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func checkLines(t *testing.T, cmd []string, expLines []string) {
	t.Helper()
	stdout, stderr := withStdout(func() {
		check(t, qfs.Run(cmd))
	})
	if len(stderr) > 0 {
		t.Errorf("stderr: %s", stderr)
	}
	lines := toLines(stdout)
	if !slices.Equal(lines, expLines) {
		t.Error("wrong output")
		for _, line := range lines {
			t.Error(line)
		}
	}
}

func withStdout(fn func()) ([]byte, []byte) {
	originalStdout := os.Stdout
	originalStderr := os.Stderr
	r1, w1, _ := os.Pipe()
	os.Stdout = w1
	r2, w2, _ := os.Pipe()
	os.Stderr = w2
	var buf1, buf2 bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf1, r1)
		_ = r1.Close()
		_, _ = io.Copy(&buf2, r2)
		_ = r2.Close()
		close(done)
	}()
	fn()
	_ = w1.Close()
	_ = w2.Close()
	os.Stdout = originalStdout
	os.Stderr = originalStderr
	<-done
	return buf1.Bytes(), buf2.Bytes()
}

func TestWithStdout(t *testing.T) {
	b1, b2 := withStdout(func() {
		fmt.Println("potato")
		_, _ = fmt.Fprintln(os.Stderr, "quack")
		fmt.Println("salad")
	})
	if !slices.Equal(b1, []byte("potato\nsalad\n")) {
		t.Errorf("stdout capture failed")
	}
	if !slices.Equal(b2, []byte("quack\n")) {
		t.Errorf("stderr capture failed")
	}
}

func TestScanStdout(t *testing.T) {
	var err error
	data, _ := withStdout(func() {
		err = qfs.Run([]string{
			"qfs",
			"scan",
			"testdata/all-types.qfs",
		})
	})
	if err != nil {
		t.Errorf(err.Error())
	}
	if !slices.Equal(data, allTypesOut) {
		t.Errorf("got wrong output: %s", data)
	}

	data, _ = withStdout(func() {
		err = qfs.Run([]string{
			"qfs",
			"scan",
			"-long",
			"-xdev",
			"testdata/all-types.qfs",
		})
	})
	if err != nil {
		t.Errorf(err.Error())
	}
	if !slices.Equal(data, allTypesOutLong) {
		t.Errorf("got wrong output: %s", data)
	}
}

func TestScanError(t *testing.T) {
	err := qfs.Run([]string{
		"qfs",
		"scan",
		"/does/not/exist",
	})
	if err == nil || !strings.HasPrefix(err.Error(), "scan: stat /does/not/exist:") {
		t.Errorf("wrong error: %v", err)
	}
	err = qfs.Run([]string{
		"qfs",
		"scan",
		"/dev/null",
	})
	if err == nil || !strings.HasPrefix(err.Error(), "scan: /dev/null at offset 0:") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestDiffError(t *testing.T) {
	tmp := t.TempDir()
	err := qfs.Run([]string{
		"qfs",
		"diff",
		"/does/not/exist",
		tmp,
	})
	if err == nil || !strings.HasPrefix(err.Error(), "diff: stat /does/not/exist:") {
		t.Errorf("wrong error: %v", err)
	}
	err = qfs.Run([]string{
		"qfs",
		"diff",
		tmp,
		"/does/not/exist",
	})
	if err == nil || !strings.HasPrefix(err.Error(), "diff: stat /does/not/exist:") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestScanDir(t *testing.T) {
	tmp := t.TempDir()
	j := func(path string) string { return filepath.Join(tmp, path) }
	err := gztar.Extract("testdata/files.tar.gz", tmp)
	if err != nil {
		t.Fatal(err.Error())
	}
	_ = os.MkdirAll(j("files/x/one/two"), 0777)
	_ = os.WriteFile(j("files/x/one/a~"), []byte("moo"), 0666)
	_ = os.WriteFile(j("files/x/one/two/b~"), []byte("moo"), 0666)
	_ = os.Chmod(j("files/x/one/two"), 0555)
	defer func() { _ = os.Chmod(j("files/x/one/two"), 0777) }()
	top := j("files")
	stdout, stderr := withStdout(func() {
		err = qfs.Run([]string{
			"qfs",
			"scan",
			"-junk",
			"~$",
			"-cleanup",
			top,
		})
	})
	check(t, err)
	_ = stdout
	if !(strings.Contains(string(stderr), "removing x/one/a~") &&
		strings.Contains(string(stderr), "remove junk "+tmp+"/files/x/one/two/b~")) {
		t.Errorf("got wrong stderr: %s", stderr)
	}
	var lines []byte
	sawLink := false
	sawX := false
	for _, line := range strings.Split(string(stdout), "\n") {
		if strings.Contains(line, " -> ") {
			sawLink = true
		} else if strings.Contains(line, " x") {
			sawX = true
		} else if line != "" && !strings.HasSuffix(line, " .") {
			lines = append(lines, []byte(line)...)
			lines = append(lines, '\n')
		}
	}
	if !(slices.Equal(filesOut, lines) && sawX && sawLink) {
		t.Errorf("%v\n%v", filesOut, lines)
		t.Errorf("got wrong stdout: %v %v %s", sawX, sawLink, lines)
	}
	filesDb := "testdata/files.qfs"
	stdout, stderr = withStdout(func() {
		err = qfs.Run([]string{
			"qfs",
			"diff",
			filesDb,
			"-no-ownerships",
			j("files"),
		})
	})
	check(t, err)
	if len(stderr) > 0 {
		t.Errorf("stderr: %s", stderr)
	}
	sawDot := false
	lines = nil
	for _, line := range strings.Split(string(stdout), "\n") {
		if strings.HasPrefix(line, "mtime ") && strings.HasSuffix(line, " .") {
			sawDot = true
		} else if line != "" {
			lines = append(lines, []byte(line)...)
			lines = append(lines, '\n')
		}
	}
	diffOut := []byte(`mkdir x
mkdir x/one
mkdir x/one/two
add x/one/two/b~
`)
	if !(sawDot && slices.Equal(lines, diffOut)) {
		t.Errorf("diff output: %v %s", sawDot, lines)
	}
}

func TestDiff(t *testing.T) {
	tmp := t.TempDir()
	j := func(path string) string { return filepath.Join(tmp, path) }
	check(t, os.MkdirAll(j("top/d1"), 0777))
	check(t, os.WriteFile(j("top/f1"), []byte("file"), 0666))
	check(t, os.WriteFile(j("top/f2"), []byte("file"), 0666))
	check(t, os.WriteFile(j("top/f3"), []byte("file"), 0666))
	check(t, os.WriteFile(j("top/f4"), []byte("file"), 0666))
	check(t, os.Symlink("target", j("top/link")))
	check(t, qfs.Run([]string{
		"qfs",
		"scan",
		j("top"),
		"-db",
		j("1.qfs"),
	}))
	time.Sleep(20 * time.Millisecond)
	check(t, os.WriteFile(j("top/f1"), []byte("change"), 0666))
	check(t, os.Remove(j("top/f2")))
	check(t, os.Mkdir(j("top/f2"), 0777))
	check(t, os.Chmod(j("top/f3"), 0444))
	check(t, os.Remove(j("top/f4")))
	check(t, os.Chmod(j("top/d1"), 0744))
	check(t, os.WriteFile(j("top/f5"), []byte("new"), 0666))

	checkLines(
		t,
		[]string{
			"qfs",
			"diff",
			"--no-dir-times",
			j("1.qfs"),
			j("top"),
		},
		[]string{
			"typechange f2",
			"rm f2",
			"rm f4",
			"mkdir f2",
			"add f5",
			"change f1",
			"chmod 0744 d1",
			"chmod 0444 f3",
		})
	checkLines(
		t,
		[]string{
			"qfs",
			"diff",
			"--checks",
			"testdata/real.qfs",
			"testdata/changed.qfs",
		},
		[]string{
			"check 1617900183684 1717900183684 - RCS/.abcde.conf,v",
			"check 1345770149957 - RCS/.gtkrc-2.0,v",
			"rm RCS/.gtkrc-2.0,v",
			"change RCS/.abcde.conf,v",
			"change other/zero",
			"change scripts/apply_sync",
			"chown 517:1111 other/pipe",
			"chown 517: other/socket",
			"chown :617 qfs",
		})
	check(t, qfs.Run([]string{
		"qfs",
		"scan",
		"testdata/real.qfs",
		"-include",
		".",
		"-exclude",
		"RCS",
		"-exclude",
		"*/.idea",
		"-include",
		"*/.gitignore",
		"-junk",
		"~$",
		"-prune",
		"qfs/coverage",
		"-db",
		j("2.qfs"),
	}))
	checkLines(
		t,
		[]string{
			"qfs",
			"diff",
			"testdata/real.qfs",
			j("2.qfs"),
		},
		[]string{
			"rm RCS",
			"rm RCS/.abcde.conf,v",
			"rm RCS/.bash_logout,v",
			"rm RCS/.bash_profile,v",
			"rm RCS/.bashrc,v",
			"rm RCS/.boxworld,v",
			"rm RCS/.caffrc,v",
			"rm RCS/.cshrc,v.deleted",
			"rm RCS/.emacs,v",
			"rm RCS/.env.bash,v.deleted",
			"rm RCS/.env.zsh,v.deleted",
			"rm RCS/.gbp.conf,v",
			"rm RCS/.gitconfig,v",
			"rm RCS/.gitignore,v",
			"rm RCS/.gtkrc-2.0,v",
			"rm RCS/.inputrc,v",
			"rm RCS/.login,v.deleted",
			"rm RCS/.logout,v.deleted",
			"rm RCS/.pbuilderrc,v",
			"rm RCS/.rpmrc,v.deleted",
			"rm RCS/.screenrc,v",
			"rm RCS/.tcshrc,v.deleted",
			"rm RCS/.terraformrc,v",
			"rm RCS/.tmux.conf,v",
			"rm RCS/.twmrc,v",
			"rm RCS/.xinitrc,v.deleted",
			"rm RCS/.xscreensaver,v.deleted",
			"rm RCS/.xsessionrc,v",
			"rm RCS/.zlogin,v",
			"rm RCS/.zlogout,v",
			"rm RCS/.zshenv,v",
			"rm RCS/.zshrc,v",
			"rm RCS/startwm.sh,v",
			"rm filter~",
			"rm qfs/.idea",
			"rm qfs/.idea/dictionaries",
			"rm qfs/.idea/dictionaries/default_user.xml",
			"rm qfs/.idea/dictionaries/ejb.xml",
			"rm qfs/.idea/inspectionProfiles",
			"rm qfs/.idea/inspectionProfiles/Project_Default.xml",
			"rm qfs/.idea/modules.xml",
			"rm qfs/.idea/qfs.iml",
			"rm qfs/.idea/vcs.xml",
			"rm qfs/.idea/workspace.xml",
			"rm qfs/coverage",
			"rm qfs/coverage/coverage.cov",
			"rm qfs/coverage/coverage.html",
		},
	)
	check(t, qfs.Run([]string{
		"qfs",
		"scan",
		"testdata/real.qfs",
		"-f",
		"-db",
		j("2.qfs"),
	}))
	checkLines(
		t,
		[]string{
			"qfs",
			"diff",
			"testdata/real.qfs",
			j("2.qfs"),
		},
		[]string{
			"rm .",
			"rm RCS",
			"rm other",
			"rm other/loop1",
			"rm other/pipe",
			"rm other/socket",
			"rm other/zero",
			"rm qfs",
			"rm qfs/.idea",
			"rm qfs/.idea/dictionaries",
			"rm qfs/.idea/inspectionProfiles",
			"rm qfs/bin",
			"rm qfs/coverage",
			"rm qfs/database",
			"rm qfs/fileinfo",
			"rm qfs/filter",
			"rm qfs/filter/testdata",
			"rm qfs/qfs",
			"rm qfs/queue",
			"rm qfs/scan",
			"rm qfs/traverse",
			"rm qsync",
			"rm qsync/doc",
			"rm qsync/src",
			"rm qsync/src/tests",
			"rm qsync/src/tests/qsync",
			"rm qsync/src/tests/tools",
			"rm qsync/util",
			"rm qsync/util/qsutil_modules",
			"rm scripts",
			"rm yes",
		},
	)
	check(t, qfs.Run([]string{
		"qfs",
		"scan",
		"testdata/real.qfs",
		"-no-special",
		"-db",
		j("2.qfs"),
	}))
	checkLines(
		t,
		[]string{
			"qfs",
			"diff",
			"testdata/real.qfs",
			j("2.qfs"),
		},
		[]string{
			"rm other/loop1",
			"rm other/pipe",
			"rm other/socket",
			"rm other/zero",
		},
	)
	check(t, qfs.Run([]string{
		"qfs",
		"scan",
		"testdata/real.qfs",
		"-filter",
		"testdata/filter1",
		"-filter",
		"testdata/filter2",
		"-db",
		j("2.qfs"),
	}))
	checkLines(
		t,
		[]string{
			"qfs",
			"diff",
			"testdata/real.qfs",
			j("2.qfs"),
		},
		[]string{
			"rm .zlogin",
			"rm .zshrc",
			"rm scripts",
			"rm scripts/apply_sync",
			"rm scripts/make_sync",
			"rm scripts/qsutil_modules",
		},
	)
	checkLines(
		t,
		[]string{
			"qfs",
			"diff",
			"testdata/real.qfs",
			"-filter",
			"testdata/filter1",
			"-filter",
			"testdata/filter2",
			j("2.qfs"),
		},
		// When running diff with -filter, the filter is applied to both files, so there
		// is no expected difference since 2.qfs was created by applying those filters.
		nil,
	)
	check(t, qfs.Run([]string{
		"qfs",
		"scan",
		"testdata/real.qfs",
		"-filter-prune",
		"testdata/filter1",
		"-db",
		j("2.qfs"),
	}))
	checkLines(
		t,
		[]string{
			"qfs",
			"diff",
			"testdata/real.qfs",
			j("2.qfs"),
		},
		[]string{
			"rm scripts",
			"rm scripts/apply_sync",
			"rm scripts/make_sync",
			"rm scripts/qsutil_modules",
		},
	)
}

func TestCLI(t *testing.T) {
	checkCli := func(cmd []string, expErr string) {
		var err error
		stdout, stderr := withStdout(func() {
			err = qfs.Run(cmd)
		})
		if err == nil {
			t.Errorf("no error")
		} else if !strings.Contains(err.Error(), expErr) {
			t.Errorf("wrong error: %v", err)
		}
		if len(stdout) > 0 || len(stderr) > 0 {
			t.Errorf("stdout=%s, stderr=%s", stdout, stderr)
		}
	}
	checkCli(nil, "no arguments provided")
	checkCli([]string{"qfs"}, "run qfs --help for help")
	checkCli([]string{"qfs", "scan"}, "scan requires an input")
	checkCli([]string{"qfs", "scan", "a", "a"}, "an input has already been specified")
	checkCli([]string{"qfs", "diff", "a"}, "diff requires two inputs")
	checkCli([]string{"qfs", "diff", "a", "a", "a"}, "inputs have already been specified")
	checkCli([]string{"qfs", "scan", "-db"}, "db requires an argument")
	checkCli([]string{"qfs", "scan", "-include"}, "include requires an argument")
	checkCli([]string{"qfs", "scan", "-filter"}, "filter requires an argument")
	checkCli([]string{"qfs", "potato"}, "unknown subcommand")
	checkCli([]string{"qfs", "scan", "-potato"}, "unknown option")
	checkCli([]string{"qfs", "scan", "-junk", "??*"}, "regexp error on ??*")
	checkCli([]string{"qfs", "scan", "-filter", "testdata/bad-filter"}, "testdata/bad-filter:1: regexp error")
}

func TestHelpVersion(t *testing.T) {
	cases := map[string]string{
		"--version": "qfs version ",
		"--help":    "Usage: qfs",
	}
	for arg, text := range cases {
		t.Run(arg, func(t *testing.T) {
			stdout, stderr := withStdout(func() {
				defer func() {
					_ = recover()
				}()
				check(t, qfs.Run([]string{"qfs", arg}))
			})
			if !strings.Contains(string(stdout), text) {
				t.Errorf("didn't see expected text")
			}
			if len(stderr) > 0 {
				t.Errorf("stderr: %s", stderr)
			}

		})
	}
}
