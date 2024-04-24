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
)

//go:embed testdata/all-types.out
var allTypesOut []byte

//go:embed testdata/all-types-long.out
var allTypesOutLong []byte

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
	// XXX Strategy: implement diff, then diff top and filesDb with various filters
	filesDb := "testdata/files.qfs"
	top := j("files")
	err = qfs.Run([]string{
		"qfs",
		"scan",
		"-junk",
		"~$",
		"-cleanup",
		top,
	})
	check(t, err)
	_ = filesDb // XXX
}
