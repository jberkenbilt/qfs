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

func withStdout(fn func()) []byte {
	originalStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf, r)
		_ = r.Close()
		close(done)
	}()
	fn()
	_ = w.Close()
	os.Stdout = originalStdout
	<-done
	return buf.Bytes()
}

func TestWithStdout(t *testing.T) {
	b := withStdout(func() {
		fmt.Println("potato")
		fmt.Println("salad")
	})
	if !slices.Equal(b, []byte("potato\nsalad\n")) {
		t.Errorf("stdout capture failed")
	}
}

func TestStdout(t *testing.T) {
	var err error
	data := withStdout(func() {
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

	data = withStdout(func() {
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

func TestError(t *testing.T) {
	err := qfs.Run([]string{
		"qfs",
		"scan",
		"/does/not/exist",
	})
	if err == nil || strings.HasPrefix(err.Error(), "scan: stat /does not exist:") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestDir(t *testing.T) {
	tmp := t.TempDir()
	err := gztar.Extract("testdata/files.tar.gz", tmp)
	if err != nil {
		t.Fatal(err.Error())
	}
	// XXX Strategy: implement diff, then diff top and filesDb with various filters
	filesDb := "testdata/files.qfs"
	top := filepath.Join(tmp, "files")
	err = qfs.Run([]string{
		"qfs",
		"scan",
		top,
	})
	check(t, err)
	_ = filesDb // XXX
}
