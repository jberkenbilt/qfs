package qfs

import (
	"fmt"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/traverse"
	"os"
)

const Version = "0.0"

type Options func(*Qfs) error

type Qfs struct {
	// XXX Not really
	dir     string
	filters []*filter.Filter
	db      string
}

func New(options ...Options) (*Qfs, error) {
	q := &Qfs{}
	for _, fn := range options {
		if err := fn(q); err != nil {
			return nil, err
		}
	}
	return q, nil
}

func (q *Qfs) Run() error {
	files, err := traverse.Traverse(q.dir, q.filters, func(err error) {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err.Error())
	})
	if err != nil {
		return err
	}
	if q.db != "" {
		return database.WriteDb(q.db, files)
	}
	return files.Flatten(func(f *traverse.FileInfo) error {
		fmt.Printf("%010d %c %08d %04o", f.ModTime.UnixMilli(), f.FileType, f.Size, f.Permissions)
		// XXX Long
		fmt.Printf(" %s %s", f.ModTime.Format("2006-01-02T15:04:05.000Z07:00"), f.Path)
		if f.FileType == traverse.TypeLink {
			fmt.Printf(" %s", f.Target)
		} else if f.FileType == traverse.TypeBlockDev || f.FileType == traverse.TypeCharDev {
			fmt.Printf(" %d,%d", f.Major, f.Minor)
		}
		fmt.Println("")
		return nil
	})
}
