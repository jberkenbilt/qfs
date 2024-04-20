// Package qfs implements the qfs command-line interface.
package qfs

import (
	"errors"
	"fmt"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/scan"
	"os"
	"path/filepath"
	"strings"
)

type Qfs struct {
	progName      string
	argTable      argTableIdx
	args          []string
	arg           int
	action        actionKey
	dir           string
	filters       []*filter.Filter
	dynamicFilter *filter.Filter
	db            string
	long          bool
	cleanup       bool
	sameDev       bool
}

// Our command-line syntax is complex and not well-suited to something like
// go-arg or flag, so we parse arguments by hand. We implement a simple state
// machine that maps options to handlers. If an argument starts with `-` or `--`,
// the option's entry is called. Otherwise, the `""` entry is called for
// positional options.

const Version = "0.0"

type argHandler func(*Qfs, string) error
type argTableIdx int

const (
	atTop argTableIdx = iota
	atScan
)

type actionKey int

const (
	actNone actionKey = iota
	actScan
)

var argTables = func() map[argTableIdx]map[string]argHandler {
	var filterArgs = map[string]argHandler{
		"filter":       argFilter,
		"filter-prune": argFilter,
		"include":      argDynamicFilter,
		"exclude":      argDynamicFilter,
		"prune":        argDynamicFilter,
		"junk":         argDynamicFilter,
	}
	a := map[argTableIdx]map[string]argHandler{
		atTop: {
			"":        argSubcommand,
			"help":    argHelp,
			"version": argVersion,
		},
		atScan: {
			"":        argDir,
			"long":    argLong,
			"db":      argDb,
			"cleanup": argCleanup,
			"xdev":    argXDev,
		},
	}
	for _, i := range []argTableIdx{atScan} {
		for arg, fn := range filterArgs {
			a[i][arg] = fn
		}
	}
	return a
}()

func (q *Qfs) check() error {
	switch q.argTable {
	case atTop:
		return fmt.Errorf("run %s --help for help", q.progName)
	case atScan:
		if q.dir == "" {
			return errors.New("scan requires a directory")
		}
	}
	return nil
}

func argHelp(q *Qfs, _ string) error {
	fmt.Printf(`
Usage: %s

XXX

`,
		q.progName,
	)
	os.Exit(0)
	return nil
}

func argVersion(q *Qfs, _ string) error {
	fmt.Printf("%s version %s\n", q.progName, Version)
	os.Exit(0)
	return nil
}

func argSubcommand(q *Qfs, arg string) error {
	switch arg {
	case "scan":
		q.argTable = atScan
		q.action = actScan
	default:
		return fmt.Errorf("unknown subcommand \"%s\"", arg)
	}
	return nil
}

func argDir(q *Qfs, arg string) error {
	if q.dir != "" {
		return fmt.Errorf("at argument \"%s\": a directory has already been specified", arg)
	}
	q.dir = arg
	return nil
}

func argDb(q *Qfs, arg string) error {
	if q.arg >= len(q.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	// If specified multiple times, later overrides earlier.
	q.db = q.args[q.arg]
	q.arg++
	return nil
}

func argLong(q *Qfs, _ string) error {
	q.long = true
	return nil
}

func argCleanup(q *Qfs, _ string) error {
	q.cleanup = true
	return nil
}

func argXDev(q *Qfs, _ string) error {
	q.sameDev = true
	return nil
}

func argFilter(q *Qfs, arg string) error {
	if q.arg >= len(q.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	pruneOnly := false
	if arg == "filter-prune" {
		pruneOnly = true
	}
	filename := q.args[q.arg]
	q.arg++
	f := filter.New()
	err := f.ReadFile(filename, pruneOnly)
	if err != nil {
		return err
	}
	q.filters = append(q.filters, f)
	return nil
}

func argDynamicFilter(q *Qfs, arg string) error {
	if q.arg >= len(q.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	parameter := q.args[q.arg]
	q.arg++
	f := q.dynamicFilter
	if f == nil {
		f = filter.New()
	}
	group := filter.NoGroup
	switch arg {
	case "include":
		group = filter.Include
	case "exclude":
		group = filter.Exclude
	case "prune":
		group = filter.Prune
	case "junk":
		group = filter.Junk
	default:
		panic("argDynamicFilter called with invalid argument")
	}
	err := func() error {
		if group == filter.Junk {
			return f.SetJunk(parameter)
		}
		return f.ReadLine(group, parameter)
	}()
	if err != nil {
		return err
	}
	q.dynamicFilter = f
	return nil
}

func (q *Qfs) handleArg(p *Qfs) error {
	var opt string
	arg := p.args[p.arg]
	p.arg++
	if strings.HasPrefix(arg, "--") {
		opt = arg[2:]
	} else if strings.HasPrefix(arg, "-") {
		opt = arg[1:]
	}
	handler, ok := argTables[p.argTable][opt]
	if !ok {
		if opt == "" {
			return fmt.Errorf("unexpected positional argument \"%s\"", arg)
		}
		return fmt.Errorf("unknown option \"%s\"", arg)
	}
	if opt == "" {
		return handler(p, arg)
	}
	return handler(p, opt)
}

func Run(args []string) error {
	if len(args) == 0 {
		return errors.New("no arguments provided")
	}
	q := &Qfs{
		progName: filepath.Base(args[0]),
		argTable: atTop,
		args:     args[1:],
		arg:      0,
		action:   actNone,
	}
	for q.arg < len(q.args) {
		if err := q.handleArg(q); err != nil {
			return err
		}
	}
	if err := q.check(); err != nil {
		return err
	}
	if q.dynamicFilter != nil {
		q.filters = append(q.filters, q.dynamicFilter)
	}
	switch q.action {
	case actScan:
		scanner, err := scan.New(
			q.dir,
			scan.WithFilters(q.filters),
			scan.WithSameDev(q.sameDev),
			scan.WithCleanup(q.cleanup),
		)
		if err != nil {
			return fmt.Errorf("crate scanner: %w", err)
		}
		files, err := scanner.Run()
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		defer func() { _ = files.Close() }()
		if q.db != "" {
			return database.WriteDb(q.db, files)
		}
		return files.ForEach(func(f *fileinfo.FileInfo) error {
			fmt.Printf("%013d %c %08d %04o", f.ModTime.UnixMilli(), f.FileType, f.Size, f.Permissions)
			if q.long {
				fmt.Printf(" %05d %05d", f.Uid, f.Gid)
			}
			fmt.Printf(" %s %s", f.ModTime.Format("2006-01-02 15:04:05.000Z07:00"), f.Path)
			if f.FileType == fileinfo.TypeLink {
				fmt.Printf(" -> %s", f.Special)
			} else if f.FileType == fileinfo.TypeBlockDev || f.FileType == fileinfo.TypeCharDev {
				fmt.Printf(" %s", f.Special)
			}
			fmt.Println("")
			return nil
		})

	default:
		return fmt.Errorf("no action specified; use %s --help for help", q.progName)
	}
}
