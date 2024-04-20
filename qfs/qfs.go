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

type actionKey int

const (
	actScan actionKey = iota
)

type Qfs struct {
	action actionKey
	input1 *Input
	db     string
	long   bool
}

type Input struct {
	Input   string
	Filters []*filter.Filter
	XDev    bool
	Cleanup bool
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

func WithScan(input *Input, db string, long bool) func(*Qfs) error {
	return func(q *Qfs) error {
		q.action = actScan
		q.input1 = input
		q.db = db
		q.long = long
		return nil
	}
}

func (q *Qfs) Scan() error {
	files, err := traverse.Traverse(
		q.input1.Input,
		q.input1.Filters,
		q.input1.XDev,
		q.input1.Cleanup,
		func(err error) {
			_, _ = fmt.Fprintf(os.Stderr, "%v\n", err.Error())
		},
	)
	if err != nil {
		return err
	}
	if q.db != "" {
		return database.WriteDb(q.db, files)
	}
	return files.Flatten(func(f *traverse.FileInfo) error {
		fmt.Printf("%013d %c %08d %04o", f.ModTime.UnixMilli(), f.FileType, f.Size, f.Permissions)
		if q.long {
			fmt.Printf(" %05d %05d", f.Uid, f.Gid)
		}
		fmt.Printf(" %s %s", f.ModTime.Format("2006-01-02 15:04:05.000Z07:00"), f.Path)
		if f.FileType == traverse.TypeLink {
			fmt.Printf(" -> %s", f.Target)
		} else if f.FileType == traverse.TypeBlockDev || f.FileType == traverse.TypeCharDev {
			fmt.Printf(" %d,%d", f.Major, f.Minor)
		}
		fmt.Println("")
		return nil
	})
}

func (q *Qfs) Run() error {
	switch q.action {
	case actScan:
		return q.Scan()
	}
	return nil
}
