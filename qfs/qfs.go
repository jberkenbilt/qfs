package qfs

import (
	"fmt"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/traverse"
	"os"
)

const Version = "0.0"

type Options func(*Qfs) error

type Qfs struct {
	dir string
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
	files, err := traverse.Traverse(q.dir, func(err error) {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err.Error())
	})
	if err != nil {
		return err
	}
	return database.WriteDb(os.Stdout, files)
}
