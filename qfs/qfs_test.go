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
	stdout, stderr := withStdout(func() {
		check(t, qfs.Run([]string{
			"qfs",
			"diff",
			"-no-dir-times",
			j("1.qfs"),
			j("top"),
		}))
	})
	if len(stderr) > 0 {
		t.Errorf("stderr: %s", stderr)
	}
	exp := []string{
		"typechange f2",
		"rm f2",
		"rm f4",
		"mkdir f2",
		"add f5",
		"change f1",
		"chmod 0744 d1",
		"chmod 0444 f3",
	}
	lines := toLines(stdout)
	if !slices.Equal(lines, exp) {
		t.Errorf("wrong output: %#v", lines)
	}

	stdout, stderr = withStdout(func() {
		check(t, qfs.Run([]string{
			"qfs",
			"diff",
			"testdata/real.qfs",
			"testdata/changed.qfs",
		}))
	})
	if len(stderr) > 0 {
		t.Errorf("stderr: %s", stderr)
	}
	exp = []string{
		"change other/zero",
		"change scripts/apply_sync",
		"chown 517:1111 other/pipe",
		"chown 517: other/socket",
		"chown :617 qfs",
	}
	lines = toLines(stdout)
	if !slices.Equal(lines, exp) {
		t.Errorf("wrong output: %#v", lines)
	}
}

// diff/scan positional
// help, version, unknown
// -no-special
// -f
// -checks
// -db no-arg
// -filter, -include, -exclude including invalid
// --arg
// unknown opt
// no subcommand
