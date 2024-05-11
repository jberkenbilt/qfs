package database_test

import (
	"errors"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/testutil"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

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
	db1, err := database.LoadFile("testdata/real.qsync")
	testutil.Check(t, err)
	err = database.WriteDb("/does/not/exist", db1, database.DbQSync)
	if err == nil || !strings.Contains(err.Error(), "qsync format not supported for write") {
		t.Errorf("wrong error: %v", err)
	}
	err = database.WriteDb("/does/not/exist", db1, database.DbQfs)
	if err == nil || !strings.HasPrefix(err.Error(), "create database \"/does/not/exist\": ") {
		t.Errorf("wrong error: %v", err)
	}
	err = database.WriteDb("/etc/no-permission", db1, database.DbQfs)
	if err == nil || !strings.HasPrefix(err.Error(), "create database \"/etc/no-permission\": ") {
		t.Errorf("wrong error: %v", err)
	}
	err = database.WriteDb(j("qsync-to-qfs"), db1, database.DbQfs)
	testutil.Check(t, err)
	db2, err := database.LoadFile(j("qsync-to-qfs"))
	testutil.Check(t, err)
	var records []*fileinfo.FileInfo
	load := func(f *fileinfo.FileInfo) error {
		records = append(records, f)
		return nil
	}
	db1, _ = database.LoadFile("testdata/real.qsync")
	err = db1.ForEach(func(*fileinfo.FileInfo) error {
		return errors.New("propagated")
	})
	if err == nil || err.Error() != "propagated" {
		t.Errorf("error did not propagate from callback: %v", err)
	}
	db1, _ = database.LoadFile("testdata/real.qsync")
	err = db1.ForEach(load)
	testutil.Check(t, err)
	all1 := records
	records = nil
	err = db2.ForEach(load)
	testutil.Check(t, err)
	all2 := records
	if !reflect.DeepEqual(all1, all2) {
		t.Error("round trip failed")
	}
}

func TestPartialFiles(t *testing.T) {
	noSpecial := false
	filesOnly := false
	var expFileKeys []string
	for i := range 3 {
		if i == 1 {
			filesOnly = true
		}
		if i == 2 {
			noSpecial = true
		}
		db, err := database.LoadFile(
			"testdata/real.qfs",
			database.WithFilesOnly(filesOnly),
			database.WithNoSpecial(noSpecial),
		)
		testutil.Check(t, err)
		var fileKeys []string
		sawSpecial := false
		sawDir := false
		err = db.ForEach(func(f *fileinfo.FileInfo) error {
			switch f.FileType {
			case fileinfo.TypeBlockDev:
				sawSpecial = true
			case fileinfo.TypeCharDev:
				sawSpecial = true
			case fileinfo.TypeSocket:
				sawSpecial = true
			case fileinfo.TypePipe:
				sawSpecial = true
			case fileinfo.TypeDirectory:
				sawDir = true
			default:
				fileKeys = append(fileKeys, f.Path)
			}
			return nil
		})
		testutil.Check(t, err)
		if i == 0 {
			expFileKeys = fileKeys
		} else {
			if !slices.Equal(expFileKeys, fileKeys) {
				t.Errorf("saw wrong keys")
			}
		}
		if sawDir == filesOnly {
			t.Errorf("saw unexpected directories")
		}
		if sawSpecial == (noSpecial || filesOnly) {
			t.Error("saw unexpected special files")
		}
	}
}

func TestErrors(t *testing.T) {
	cases := map[string]string{
		"/does/not/exist":     "open /does/not/exist:",
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
		"testdata/bad9":       "testdata/bad9 at offset 46: wrong number of fields: 5, not 6",
	}
	for filename, text := range cases {
		t.Run(filename, func(t *testing.T) {
			err := func() error {
				db, err := database.LoadFile(filename)
				if err != nil {
					return err
				}
				return db.ForEach(func(*fileinfo.FileInfo) error {
					return nil
				})
			}()
			checkError(t, err, text)
		})
	}
}
