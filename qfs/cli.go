package qfs

import (
	"errors"
	"fmt"
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

var argTables = map[argTableIdx]map[string]argHandler{
	atTop: {
		"":        argSubcommand,
		"help":    argHelp,
		"version": argVersion,
	},
	atScan: {
		"": argDir,
	},
}

type argParser struct {
	progName string
	argTable argTableIdx
	dir      string
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

func (q *Qfs) arg(p *argParser, arg string) error {
	var opt string
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
	return handler(p, arg)
}

func WithCliArgs(args []string) Options {
	return func(q *Qfs) error {
		if len(args) == 0 {
			return errors.New("no arguments provided")
		}
		parser := &argParser{
			progName: filepath.Base(args[0]),
			argTable: atTop,
		}
		for _, arg := range args[1:] {
			if err := q.arg(parser, arg); err != nil {
				return err
			}
		}
		if err := parser.check(); err != nil {
			return err
		}
		q.dir = parser.dir
		return nil
	}
}
