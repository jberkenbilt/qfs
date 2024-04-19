package qfs

import (
	"errors"
	"fmt"
	"github.com/jberkenbilt/qfs/filter"
	"os"
	"path/filepath"
	"strings"
)

// Command-line Parsing -- our command-line syntax is complex and not well-suited
// to something like go-arg or flag, so parse arguments by hand. We implement a
// simple state machine that maps options to handlers. If an argument starts with
// `-` or `--`, the option's entry is called. Otherwise, the `""` entry is called
// for positional options. The argParser object handles the while parsing.
// WithCliArgs drives the argument parsing and translates the results into
// additional qfs configuration.

type argHandler func(*argParser, string) error
type argTableIdx int

const (
	atTop argTableIdx = iota
	atScan
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

type argParser struct {
	progName      string
	argTable      argTableIdx
	args          []string
	arg           int
	dir           string
	filters       []*filter.Filter
	dynamicFilter *filter.Filter
	db            string
	long          bool
	cleanup       bool
	xDev          bool
}

func (p *argParser) check() error {
	switch p.argTable {
	case atTop:
		return fmt.Errorf("run %s --help for help", p.progName)
	case atScan:
		if p.dir == "" {
			return errors.New("scan requires a directory")
		}
	}
	return nil
}

func argHelp(p *argParser, _ string) error {
	fmt.Printf(`
Usage: %s

XXX

`,
		p.progName,
	)
	os.Exit(0)
	return nil
}

func argVersion(p *argParser, _ string) error {
	fmt.Printf("%s version %s\n", p.progName, Version)
	os.Exit(0)
	return nil
}

func argSubcommand(p *argParser, arg string) error {
	switch arg {
	case "scan":
		p.argTable = atScan
	default:
		return fmt.Errorf("unknown subcommand \"%s\"", arg)
	}
	return nil
}

func argDir(p *argParser, arg string) error {
	if p.dir != "" {
		return fmt.Errorf("at argument \"%s\": a directory has already been specified", arg)
	}
	p.dir = arg
	return nil
}

func argDb(p *argParser, arg string) error {
	if p.arg >= len(p.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	// If specified multiple times, later overrides earlier.
	p.db = p.args[p.arg]
	p.arg++
	return nil
}

func argLong(p *argParser, _ string) error {
	p.long = true
	return nil
}

func argCleanup(p *argParser, _ string) error {
	p.cleanup = true
	return nil
}

func argXDev(p *argParser, _ string) error {
	p.xDev = true
	return nil
}

func argFilter(p *argParser, arg string) error {
	if p.arg >= len(p.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	pruneOnly := false
	if arg == "filter-prune" {
		pruneOnly = true
	}
	filename := p.args[p.arg]
	p.arg++
	f := filter.New()
	err := f.ReadFile(filename, pruneOnly)
	if err != nil {
		return err
	}
	p.filters = append(p.filters, f)
	return nil
}

func argDynamicFilter(p *argParser, arg string) error {
	if p.arg >= len(p.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	parameter := p.args[p.arg]
	p.arg++
	f := p.dynamicFilter
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
	p.dynamicFilter = f
	return nil
}

func (q *Qfs) arg(p *argParser) error {
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

func WithCliArgs(args []string) Options {
	return func(q *Qfs) error {
		if len(args) == 0 {
			return errors.New("no arguments provided")
		}
		parser := &argParser{
			progName: filepath.Base(args[0]),
			argTable: atTop,
			args:     args[1:],
			arg:      0,
		}
		for parser.arg < len(parser.args) {
			if err := q.arg(parser); err != nil {
				return err
			}
		}
		if err := parser.check(); err != nil {
			return err
		}
		if parser.dynamicFilter != nil {
			parser.filters = append(parser.filters, parser.dynamicFilter)
		}
		// XXX
		return WithScan(
			&Input{
				Input:   parser.dir,
				Filters: parser.filters,
				XDev:    parser.xDev,
				Cleanup: parser.cleanup,
			},
			parser.db,
			parser.long,
		)(q)
	}
}
