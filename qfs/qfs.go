// Package qfs implements the qfs command-line interface.
package qfs

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/diff"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/localsource"
	"github.com/jberkenbilt/qfs/repo"
	"github.com/jberkenbilt/qfs/s3lister"
	"github.com/jberkenbilt/qfs/scan"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var S3Client *s3.Client // Overridden in test suite
var s3Re = regexp.MustCompile(`^s3://([^/]+)(?:/(.*))?$`)

type parser struct {
	progName      string
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
	noOp          bool
	localFilter   bool
	cleanRepo     bool
}

// Our command-line syntax is complex and not well-suited to something like
// go-arg or flag, so we parse arguments by hand. We implement a simple state
// machine that maps options to handlers. If an argument starts with `-` or `--`,
// the option's entry is called. Otherwise, the `""` entry is called for
// positional options.

const Version = "0.0"

type argHandler func(*parser, string) error

type actionKey int

const (
	actNone actionKey = iota
	actScan
	actDiff
	actInitRepo
	actPush
	actPull
	actPushDb
)

var argTables = func() map[actionKey]map[string]argHandler {
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
	a := map[actionKey]map[string]argHandler{
		actNone: {
			"":        argSubcommand,
			"help":    argHelp,
			"version": argVersion,
		},
		actScan: {
			"":        argScanPositional,
			"long":    argLong,
			"db":      argDb,
			"cleanup": argCleanup,
			"xdev":    argXDev,
			"top":     argTop, // only with repo:...
		},
		actDiff: {
			"":              argDiffPositional,
			"no-dir-times":  argNoDirTimes,
			"no-ownerships": argNoOwnerships,
			"checks":        argChecks,
		},
		actInitRepo: {
			"top":        argTop,
			"clean-repo": argCleanRepo,
		},
		actPush: {
			"top":     argTop,
			"cleanup": argCleanup,
			"n":       argNoOp,
		},
		actPull: {
			"top":          argTop,
			"n":            argNoOp,
			"local-filter": argLocalFilter,
		},
		actPushDb: {
			"top": argTop,
		},
	}
	for _, i := range []actionKey{actScan, actDiff} {
		for arg, fn := range filterArgs {
			a[i][arg] = fn
		}
	}
	return a
}()

func (p *parser) check() error {
	switch p.action {
	case actNone:
		return fmt.Errorf("run %s --help for help", p.progName)
	case actScan:
		if p.input1 == "" {
			return errors.New("scan requires an input")
		}
	case actDiff:
		if p.input2 == "" {
			return errors.New("diff requires two inputs")
		}
	case actInitRepo:
	case actPush:
	case actPull:
	case actPushDb:
	}
	return nil
}

func argHelp(p *parser, _ string) error {
	fmt.Printf(`
Usage: %s

XXX -- generate usage and also shell completion

`,
		p.progName,
	)
	os.Exit(0)
	return nil
}

func argVersion(p *parser, _ string) error {
	fmt.Printf("%s version %s\n", p.progName, Version)
	os.Exit(0)
	return nil
}

func argTop(p *parser, arg string) error {
	if p.arg >= len(p.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	p.top = p.args[p.arg]
	p.arg++
	return nil
}

func argCleanRepo(p *parser, _ string) error {
	p.cleanRepo = true
	return nil
}

func argSubcommand(p *parser, arg string) error {
	switch arg {
	case "scan":
		p.action = actScan
	case "diff":
		p.action = actDiff
	case "init-repo":
		p.action = actInitRepo
	case "push":
		p.action = actPush
	case "pull":
		p.action = actPull
	case "push-db":
		p.action = actPushDb
	default:
		return fmt.Errorf("unknown subcommand \"%s\"", arg)
	}
	return nil
}

func argFilesOnly(p *parser, _ string) error {
	p.filesOnly = true
	return nil
}

func argNoSpecial(p *parser, _ string) error {
	p.noSpecial = true
	return nil
}

func argNoDirTimes(p *parser, _ string) error {
	p.noDirTimes = true
	return nil
}

func argNoOwnerships(p *parser, _ string) error {
	p.noOwnerships = true
	return nil
}

func argChecks(p *parser, _ string) error {
	p.checks = true
	return nil
}

func argScanPositional(p *parser, arg string) error {
	if p.input1 != "" {
		return fmt.Errorf("at argument \"%s\": an input has already been specified", arg)
	}
	p.input1 = arg
	return nil
}

func argDiffPositional(p *parser, arg string) error {
	if p.input2 != "" {
		return fmt.Errorf("at argument \"%s\": inputs have already been specified", arg)
	}
	if p.input1 != "" {
		p.input2 = arg
	} else {
		p.input1 = arg
	}
	return nil
}

func argDb(p *parser, arg string) error {
	if p.arg >= len(p.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	// If specified multiple times, later overrides earlier.
	p.db = p.args[p.arg]
	p.arg++
	return nil
}

func argLong(p *parser, _ string) error {
	p.long = true
	return nil
}

func argCleanup(p *parser, _ string) error {
	p.cleanup = true
	return nil
}

func argNoOp(p *parser, _ string) error {
	p.noOp = true
	return nil
}

func argLocalFilter(p *parser, _ string) error {
	p.localFilter = true
	return nil
}

func argXDev(p *parser, _ string) error {
	p.sameDev = true
	return nil
}

func argFilter(p *parser, arg string) error {
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
	err := f.ReadFile(fileinfo.NewPath(localsource.New(""), filename), pruneOnly)
	if err != nil {
		return err
	}
	p.filters = append(p.filters, f)
	return nil
}

func argDynamicFilter(p *parser, arg string) error {
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
	p.dynamicFilter = f
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
	handler, ok := argTables[p.action][opt]
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
	s3Match := s3Re.FindStringSubmatch(p.input1)
	if s3Match != nil {
		bucket := s3Match[1]
		prefix := s3Match[2]
		ls, err := s3lister.New(s3lister.WithS3Client(S3Client))
		if err != nil {
			return err
		}
		input := &s3.ListObjectsV2Input{
			Bucket: &bucket,
			Prefix: &prefix,
		}
		err = ls.List(context.Background(), input, func(objects []types.Object) {
			for _, obj := range objects {
				if p.long {
					fmt.Printf("%d %d %s\n", obj.LastModified.UnixMilli(), *obj.Size, *obj.Key)
				} else {
					fmt.Println(*obj.Key)
				}
			}
		})
		if err != nil {
			return err
		}
		return nil
	}
	var files database.Database
	if strings.HasPrefix(p.input1, repo.ScanPrefix) {
		r, err := repo.New(
			repo.WithLocalTop(p.top),
			repo.WithS3Client(S3Client),
		)
		if err != nil {
			return err
		}
		files, err = r.Scan(p.input1, p.filters)
		if err != nil {
			return err
		}
	} else {
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
		files, err = scanner.Run()
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}
	}
	if p.db != "" {
		return database.WriteDb(p.db, files, database.DbQfs)
	}
	return files.Print(p.long)
}

func (p *parser) doDiff() error {
	d := diff.New(
		diff.WithFilters(p.filters),
		diff.WithFilesOnly(p.filesOnly),
		diff.WithNoSpecial(p.noSpecial),
		diff.WithNoDirTimes(p.noDirTimes),
		diff.WithNoOwnerships(p.noOwnerships),
	)
	r, err := d.RunFiles(p.input1, p.input2)
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
	return r.Init(p.cleanRepo)
}

func (p *parser) doPull() error {
	r, err := repo.New(
		repo.WithLocalTop(p.top),
		repo.WithS3Client(S3Client),
	)
	if err != nil {
		return err
	}
	return r.Pull(&repo.PullConfig{
		NoOp:        p.noOp,
		LocalFilter: p.localFilter,
		SiteTar:     "", // XXX
	})
}

func (p *parser) doPush() error {
	r, err := repo.New(
		repo.WithLocalTop(p.top),
		repo.WithS3Client(S3Client),
	)
	if err != nil {
		return err
	}
	return r.Push(&repo.PushConfig{
		Cleanup:     p.cleanup,
		NoOp:        p.noOp,
		LocalTar:    "", // XXX
		SaveSite:    "", // XXX
		SaveSiteTar: "", // XXX
	})
}

func (p *parser) doPushDb() error {
	r, err := repo.New(
		repo.WithLocalTop(p.top),
		repo.WithS3Client(S3Client),
	)
	if err != nil {
		return err
	}
	return r.PushDb()
}

func Run(args []string) error {
	if len(args) == 0 {
		return errors.New("no arguments provided")
	}
	p := &parser{
		progName: filepath.Base(args[0]),
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
	case actPush:
		return p.doPush()
	case actPull:
		return p.doPull()
	case actPushDb:
		return p.doPushDb()
	}
	// TEST: NOT COVERED (not reachable, but go 1.22 doesn't see it)
	return nil
}
