// Package qfs implements the qfs command-line interface.
package qfs

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/diff"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/repo"
	"github.com/jberkenbilt/qfs/scan"
	"os"
	"path/filepath"
	"strings"
)

var S3Client *s3.Client // Overridden in test suite

type parser struct {
	progName      string
	argTable      argTableIdx
	args          []string
	arg           int
	action        actionKey
	top           string // local root directory instead of current directory
	input1        string
	input2        string
	filters       []*filter.Filter
	dynamicFilter *filter.Filter
	db            string
	long          bool
	cleanup       bool
	sameDev       bool
	filesOnly     bool
	noSpecial     bool
	noDirTimes    bool
	noOwnerships  bool
	checks        bool
}

// Our command-line syntax is complex and not well-suited to something like
// go-arg or flag, so we parse arguments by hand. We implement a simple state
// machine that maps options to handlers. If an argument starts with `-` or `--`,
// the option's entry is called. Otherwise, the `""` entry is called for
// positional options.

const Version = "0.0"

type argHandler func(*parser, string) error
type argTableIdx int

const (
	atTop argTableIdx = iota
	atScan
	atDiff
	atInitRepo
)

type actionKey int

const (
	actNone actionKey = iota
	actScan
	actDiff
	actInitRepo
)

var argTables = func() map[argTableIdx]map[string]argHandler {
	var filterArgs = map[string]argHandler{
		"filter":       argFilter,
		"filter-prune": argFilter,
		"include":      argDynamicFilter,
		"exclude":      argDynamicFilter,
		"prune":        argDynamicFilter,
		"junk":         argDynamicFilter,
		"f":            argFilesOnly,
		"no-special":   argNoSpecial,
	}
	a := map[argTableIdx]map[string]argHandler{
		atTop: {
			"":        argSubcommand,
			"help":    argHelp,
			"version": argVersion,
		},
		atScan: {
			"":        argScanPositional,
			"long":    argLong,
			"db":      argDb,
			"cleanup": argCleanup,
			"xdev":    argXDev,
		},
		atDiff: {
			"":              argDiffPositional,
			"no-dir-times":  argNoDirTimes,
			"no-ownerships": argNoOwnerships,
			"checks":        argChecks,
		},
		atInitRepo: {
			"top": argTop,
		},
	}
	for _, i := range []argTableIdx{atScan, atDiff} {
		for arg, fn := range filterArgs {
			a[i][arg] = fn
		}
	}
	return a
}()

func (p *parser) check() error {
	switch p.argTable {
	case atTop:
		return fmt.Errorf("run %s --help for help", p.progName)
	case atScan:
		if p.input1 == "" {
			return errors.New("scan requires an input")
		}
	case atDiff:
		if p.input2 == "" {
			return errors.New("diff requires two inputs")
		}
	case atInitRepo:
	}
	return nil
}

func argHelp(q *parser, _ string) error {
	fmt.Printf(`
Usage: %s

XXX

`,
		q.progName,
	)
	os.Exit(0)
	return nil
}

func argVersion(q *parser, _ string) error {
	fmt.Printf("%s version %s\n", q.progName, Version)
	os.Exit(0)
	return nil
}

func argTop(q *parser, arg string) error {
	if q.arg >= len(q.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	q.top = q.args[q.arg]
	q.arg++
	return nil
}

func argSubcommand(q *parser, arg string) error {
	switch arg {
	case "scan":
		q.argTable = atScan
		q.action = actScan
	case "diff":
		q.argTable = atDiff
		q.action = actDiff
	case "init-repo":
		q.argTable = atInitRepo
		q.action = actInitRepo
	default:
		return fmt.Errorf("unknown subcommand \"%s\"", arg)
	}
	return nil
}

func argFilesOnly(q *parser, _ string) error {
	q.filesOnly = true
	return nil
}

func argNoSpecial(q *parser, _ string) error {
	q.noSpecial = true
	return nil
}

func argNoDirTimes(q *parser, _ string) error {
	q.noDirTimes = true
	return nil
}

func argNoOwnerships(q *parser, _ string) error {
	q.noOwnerships = true
	return nil
}

func argChecks(q *parser, _ string) error {
	q.checks = true
	return nil
}

func argScanPositional(q *parser, arg string) error {
	if q.input1 != "" {
		return fmt.Errorf("at argument \"%s\": an input has already been specified", arg)
	}
	q.input1 = arg
	return nil
}

func argDiffPositional(q *parser, arg string) error {
	if q.input2 != "" {
		return fmt.Errorf("at argument \"%s\": inputs have already been specified", arg)
	}
	if q.input1 != "" {
		q.input2 = arg
	} else {
		q.input1 = arg
	}
	return nil
}

func argDb(q *parser, arg string) error {
	if q.arg >= len(q.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	// If specified multiple times, later overrides earlier.
	q.db = q.args[q.arg]
	q.arg++
	return nil
}

func argLong(q *parser, _ string) error {
	q.long = true
	return nil
}

func argCleanup(q *parser, _ string) error {
	q.cleanup = true
	return nil
}

func argXDev(q *parser, _ string) error {
	q.sameDev = true
	return nil
}

func argFilter(q *parser, arg string) error {
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
	err := f.ReadFile(fileinfo.NewPath(fileinfo.NewLocal(""), filename), pruneOnly)
	if err != nil {
		return err
	}
	q.filters = append(q.filters, f)
	return nil
}

func argDynamicFilter(q *parser, arg string) error {
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
		// TEST: NOT COVERED. Not possible unless we messed up statically creating the
		// arg tables.
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

func (p *parser) handleArg() error {
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

func (p *parser) doScan() error {
	scanner, err := scan.New(
		p.input1,
		scan.WithFilters(p.filters),
		scan.WithSameDev(p.sameDev),
		scan.WithCleanup(p.cleanup),
		scan.WithFilesOnly(p.filesOnly),
		scan.WithNoSpecial(p.noSpecial),
	)
	if err != nil {
		// TEST: NOT COVERED. scan.New never returns an error.
		return fmt.Errorf("create scanner: %w", err)
	}
	files, err := scanner.Run()
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}
	defer func() { _ = files.Close() }()
	if p.db != "" {
		return database.WriteDb(p.db, files, database.DbQfs)
	}
	return fileinfo.PrintDb(files, p.long)
}

func (p *parser) doDiff() error {
	d, err := diff.New(
		p.input1,
		p.input2,
		diff.WithFilters(p.filters),
		diff.WithFilesOnly(p.filesOnly),
		diff.WithNoSpecial(p.noSpecial),
		diff.WithNoDirTimes(p.noDirTimes),
		diff.WithNoOwnerships(p.noOwnerships),
	)
	if err != nil {
		// TEST: NOT COVERED. diff.New never returns an error.
		return fmt.Errorf("create diff: %w", err)
	}
	r, err := d.Run()
	if err != nil {
		return fmt.Errorf("diff: %w", err)
	}
	err = r.WriteDiff(os.Stdout, p.checks)
	if err != nil {
		// TEST: NOT COVERED
		return err
	}

	return nil
}

func (p *parser) doInitRepo() error {
	r, err := repo.New(
		repo.WithLocalTop(p.top),
		repo.WithS3Client(S3Client),
	)
	if err != nil {
		return err
	}
	return r.Init()
}

func Run(args []string) error {
	if len(args) == 0 {
		return errors.New("no arguments provided")
	}
	p := &parser{
		progName: filepath.Base(args[0]),
		argTable: atTop,
		args:     args[1:],
		arg:      0,
		action:   actNone,
	}
	for p.arg < len(p.args) {
		if err := p.handleArg(); err != nil {
			return err
		}
	}
	if err := p.check(); err != nil {
		return err
	}
	if p.dynamicFilter != nil {
		p.filters = append(p.filters, p.dynamicFilter)
	}
	switch p.action {
	case actNone:
		// TEST: NOT COVERED. Can't actually happen.
		return fmt.Errorf("no action specified; use %s --help for help", p.progName)
	case actScan:
		return p.doScan()
	case actDiff:
		return p.doDiff()
	case actInitRepo:
		return p.doInitRepo()
	}
	// TEST: NOT COVERED (not reachable, but go 1.22 doesn't see it)
	return nil
}
