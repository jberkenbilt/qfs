package fileinfo_test

import (
	"fmt"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/localsource"
	"github.com/jberkenbilt/qfs/testutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Most of fileinfo is tested through other packages.

func TestRequiresCopy(t *testing.T) {
	tmp := t.TempDir()
	j := func(path string) string {
		return filepath.Join(tmp, path)
	}
	testutil.Check(t, os.WriteFile(j("one"), []byte("potato"), 0666))
	local := localsource.New(tmp)
	srcPath := fileinfo.NewPath(local, "one")
	x, err := fileinfo.RequiresCopy(srcPath, srcPath)
	if err != nil {
		t.Error(err.Error())
	}
	if x {
		t.Error("requires copy")
	}
	x, err = fileinfo.RequiresCopy(srcPath, fileinfo.NewPath(local, "two"))
	if err != nil {
		t.Errorf("%v %v", err.Error(), os.IsNotExist(err))
	}
	if !x {
		t.Error("didn't require")
	}
	x, err = fileinfo.RequiresCopy(fileinfo.NewPath(local, ""), fileinfo.NewPath(local, "two"))
	if err != nil {
		t.Errorf(err.Error())
	} else if x {
		t.Error("required with source as directory")
	}
	_, err = fileinfo.RequiresCopy(srcPath, fileinfo.NewPath(local, ""))
	if err == nil || err.Error() != fmt.Sprintf("%s exists and is not a plain file", tmp) {
		t.Errorf("wrong error: %v", err)
	}
	_, err = fileinfo.RequiresCopy(fileinfo.NewPath(local, "two"), fileinfo.NewPath(local, "two"))
	if err == nil || !strings.Contains(err.Error(), "/two") {
		t.Errorf("wrong error: %v", err)
	}
}
