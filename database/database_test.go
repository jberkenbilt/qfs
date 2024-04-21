package database_test

import (
	"errors"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func check(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e.Error())
	}
}

func checkError(t *testing.T, e error, text string) {
	t.Helper()
	if e == nil || !strings.Contains(e.Error(), text) {
		t.Errorf("wrong error: %v", e)
	}
}

func TestRoundTrip(t *testing.T) {
	// Read qsync, write qfs, read resulting qfs. The results should be identical.
	tmp := t.TempDir()
	j := func(path string) string {
		return filepath.Join(tmp, path)
	}
	db1, err := database.Open("testdata/real.qsync")
	check(t, err)
	defer func() { _ = db1.Close() }()
	err = database.WriteDb("/does/not/exist", db1)
	if err == nil || !strings.HasPrefix(err.Error(), "create database \"/does/not/exist\": ") {
		t.Errorf("wrong error: %v", err)
	}
	err = database.WriteDb(j("qsync-to-qfs"), db1)
	check(t, err)
	db2, err := database.Open(j("qsync-to-qfs"))
	check(t, err)
	defer func() { _ = db2.Close() }()
	var records []*fileinfo.FileInfo
	load := func(f *fileinfo.FileInfo) error {
		records = append(records, f)
		return nil
	}
	err = db1.ForEach(func(*fileinfo.FileInfo) error {
		return errors.New("propagated")
	})
	if err == nil || !strings.HasPrefix(err.Error(), "testdata/real.qsync at offset 84: propagated") {
		t.Errorf("error did not propagate from callback: %v", err)
	}
	err = db1.ForEach(load)
	check(t, err)
	all1 := records
	records = nil
	err = db2.ForEach(load)
	check(t, err)
	all2 := records
	if !reflect.DeepEqual(all1, all2) {
		t.Error("round trip failed")
	}
}

func TestErrors(t *testing.T) {
	cases := map[string]string{
		"/does/not/exist":     "open database /does/not/exist:",
		"database.go":         "database.go is not a qfs database",
		"testdata/no-newline": "testdata/no-newline at offset 0: EOF",
		"testdata/bad1":       "testdata/bad1 at offset 6: expected length[/same]",
		"testdata/bad2":       "testdata/bad2 at offset 6: `same` value is too large",
		"testdata/bad3":       "testdata/bad3 at offset 9: ",
		"testdata/bad4":       "testdata/bad4: expected byte 10 at offset 42",
		"testdata/bad5":       "testdata/bad5: expected byte 0 at offset 24",
		"testdata/bad6":       "testdata/bad6 at offset 6: EOF",
		"testdata/bad7":       "testdata/bad7 at offset 42: wrong number of fields: 7, not 8",
		"testdata/bad8":       "testdata/bad8 at offset 84: wrong number of fields: 8, not 9",
	}
	for filename, text := range cases {
		t.Run(filename, func(t *testing.T) {
			err := func() error {
				db, err := database.Open(filename)
				if err != nil {
					return err
				}
				defer func() { _ = db.Close() }()
				return db.ForEach(func(*fileinfo.FileInfo) error {
					return nil
				})
			}()
			checkError(t, err, text)

		})
	}
}
