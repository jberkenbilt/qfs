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
	"github.com/jberkenbilt/qfs/misc"
	"github.com/jberkenbilt/qfs/repo"
	"github.com/jberkenbilt/qfs/s3lister"
	"github.com/jberkenbilt/qfs/scan"
	"github.com/jberkenbilt/qfs/sync"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var S3Client *s3.Client // Overridden in test suite
var s3Re = regexp.MustCompile(`^s3://([^/]+)(?:/(.*))?$`)
var epochRe = regexp.MustCompile(`^\d+$`)
var dateRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
var dateTimeRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}(?:\.\d{3})?$`)

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
	nonFileTimes  bool
	noOwnerships  bool
	checks        bool
	noOp          bool
	localFilter   bool
	initMode      repo.InitMode
	timestamp     time.Time
}

// Our command-line syntax is complex and not well-suited to something like
// go-arg or flag, so we parse arguments by hand. We implement a simple state
// machine that maps options to handlers. If an argument starts with `-` or `--`,
// the option's entry is called. Otherwise, the `""` entry is called for
// positional options.

const Version = "0.1.4"

type argHandler struct {
	fn   func(*parser, string) error
	help string
}

type actionKey int

const (
	actNone actionKey = iota
	actScan
	actDiff
	actInitRepo
	actPush
	actPull
	actPushDb
	actSync
	actPushTimes
	actListVersions
	actGet
)

func arg(fn func(*parser, string) error, help string) argHandler {
	return argHandler{
		fn:   fn,
		help: help,
	}
}

var argTables = func() map[actionKey]map[string]argHandler {
	var filterArgs = map[string]argHandler{
		"filter":       arg(argFilter, "filter file"),
		"filter-prune": arg(argFilter, "filter file -- read prune/junk only"),
		"include":      arg(argDynamicFilter, "include directive for dynamic filter"),
		"exclude":      arg(argDynamicFilter, "exclude directive for dynamic filter"),
		"prune":        arg(argDynamicFilter, "prune directive for dynamic filter"),
		"junk":         arg(argDynamicFilter, "junk directive for dynamic filter"),
		"f":            arg(argFilesOnly, "files and symbolic links only"),
		"no-special":   arg(argNoSpecial, "omit pipes, sockets, and devices"),
	}
	a := map[actionKey]map[string]argHandler{
		actNone: {
			"":        arg(argSubcommand, "subcommand"),
			"version": arg(argVersion, "show version and exit"),
			// help is added in init to avoid circular initialization reference
		},
		actScan: {
			"":        arg(argOneInput, "scan-input"),
			"long":    arg(argLong, "show ownerships"),
			"db":      arg(argDb, "write to specified database file"),
			"cleanup": arg(argCleanup, "remove junk files"),
			"xdev":    arg(argXDev, "don't cross device boundaries"),
			"top":     arg(argTop, "with repo: or repo:site, specific top-level directory"),
		},
		actDiff: {
			"":               arg(argTwoInputs, "old-scan-input new-scan-input"),
			"non-file-times": arg(argNonFileTimes, "show modification time changes in non-files"),
			"no-ownerships":  arg(argNoOwnerships, "don't show ownership changes"),
			"checks":         arg(argChecks, "include information about \"old\" version for checking"),
		},
		actInitRepo: {
			"top":        arg(argTop, "local repository top-level directory"),
			"clean-repo": arg(argCleanRepo, "remove objects not included by filters"),
			"migrate":    arg(argMigrate, "migrate from aws s3 sync"),
		},
		actPush: {
			"top":     arg(argTop, "local repository top-level directory"),
			"cleanup": arg(argCleanup, "remove junk files while scanning"),
			"n":       arg(argNoOp, "don't modify the repository"),
		},
		actPull: {
			"top":          arg(argTop, "local repository top-level directory"),
			"n":            arg(argNoOp, "don't modify the local site"),
			"local-filter": arg(argLocalFilter, "use the local copy of the site filter"),
		},
		actPushDb: {
			"top": arg(argTop, "local repository top-level directory"),
		},
		actSync: {
			"":  arg(argTwoInputs, "source-path dest-path"),
			"n": arg(argNoOp, "show changes without modifying destination"),
		},
		actPushTimes: {
			"top": arg(argTop, "local repository top-level directory"),
		},
		actListVersions: {
			"":      arg(argOneInput, "path within repository"),
			"top":   arg(argTop, "local repository top-level directory"),
			"as-of": arg(argTimestamp, "ignore anything newer than specified timestamp"),
			"long":  arg(argLong, "include S3 version identifiers"),
		},
		actGet: {
			"":      arg(argTwoInputs, "repository-path local-path"),
			"top":   arg(argTop, "local repository top-level directory"),
			"as-of": arg(argTimestamp, "ignore anything newer than specified timestamp"),
		},
	}
	for _, i := range []actionKey{actScan, actDiff, actSync, actListVersions, actGet} {
		for arg, fn := range filterArgs {
			a[i][arg] = fn
		}
	}
	return a
}()

func init() {
	// We have to plug argHelp in here to avoid a circular initialization reference.
	argTables[actNone]["help"] = arg(argHelp, "show help and exit")
}

type subcommandHandler struct {
	action actionKey
	help   string
}

func subcommand(action actionKey, help string) subcommandHandler {
	return subcommandHandler{
		action: action,
		help:   help,
	}
}

var subcommands = map[string]subcommandHandler{
	"scan": subcommand(actScan, `
Scan a directory, database, repository, or location in S3, applying all
specified filters. scan-input may be one of

* directory - a local directory
* db - path to local qfs database
* repo - the repository indicated by .qfs/repo
* repo:$site - the repository copy of the database for site $site

If -db is given, the result is written to the specified database.
Otherwise, output is written to standard output.
`),
	"diff": subcommand(actDiff, `
Compare two scan inputs, applying all specified filters.
`),
	"init-repo": subcommand(actInitRepo, `
Initialize a repository.
`),
	"push": subcommand(actPush, `
Push changes from the local site to the repository.
`),
	"pull": subcommand(actPull, `
Pull changes from the repository to the local site.
`),
	"push-db": subcommand(actPushDb, `
Regenerate the local site database and write it to the repository,
overriding the repository's record of the local site's contents. This can
be useful after restoring a site to replace outdated information in the
repository.
`),
	"sync": subcommand(actSync, `
Synchronize a destination directory with the contents of a source directory
subject to the given filters. Similar in spirit to a local rsync using qfs
filters.
`),
	"push-times": subcommand(actPushTimes, `
List the timestamps of all known pushes.
`),
	"list-versions": subcommand(actListVersions, `
List all the versions in the repository of all the files at or below a
specified location.
`),
	"get": subcommand(actGet, `

Retrieve files from the repository; useful for ad-hoc retrieval of files
that are not included by the filter or recovering files that were changed
locally and haven't been pushed.
`),
}

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
	case actSync:
		if p.input2 == "" {
			return errors.New("sync requires two inputs")
		}
	case actPushTimes:
	case actListVersions:
		if p.input1 == "" {
			return errors.New("list-versions requires a path")
		}
	case actGet:
		if p.input2 == "" {
			return errors.New("get requires a path and a save location")
		}
	}
	if p.noOp {
		p.cleanup = false
	}
	return nil
}

func argHelp(p *parser, _ string) error {
	fmt.Printf(`
Usage:
%s top-level-option
OR
%[1]s subcommand [options]

Top-level options:
`,
		p.progName,
	)
	keys := misc.SortedKeys(argTables[actNone])
	for _, a := range keys {
		if a == "" {
			continue
		}
		fmt.Printf("  --%s: %s\n", a, argTables[actNone][a].help)
	}
	fmt.Printf("\nSubcommands:\n")
	for s, sData := range subcommands {
		args, ok := argTables[sData.action]
		if !ok {
			panic("no args for " + s)
		}
		pos, ok := args[""]
		if ok {
			fmt.Printf("\n%s %s {%s} [options]\n", p.progName, s, pos.help)
		} else {
			fmt.Printf("\n%s %s [options]\n", p.progName, s)
		}
		fmt.Println(sData.help)
		keys = misc.SortedKeys(args)
		fmt.Printf("%s options:\n", s)
		for _, a := range keys {
			if a == "" {
				continue
			}
			fmt.Printf("  --%s: %s\n", a, args[a].help)
		}
	}

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
	if p.initMode != repo.InitNormal {
		return fmt.Errorf("only one init-repo mode option may be given")
	}
	p.initMode = repo.InitCleanRepo
	return nil
}

func argMigrate(p *parser, _ string) error {
	if p.initMode != repo.InitNormal {
		return fmt.Errorf("only one init-repo mode option may be given")
	}
	p.initMode = repo.InitMigrate
	return nil
}

func argTimestamp(p *parser, arg string) error {
	if p.arg >= len(p.args) {
		return fmt.Errorf("%s requires an argument", arg)
	}
	timestamp := p.args[p.arg]
	p.arg++
	if epochRe.MatchString(timestamp) {
		t, err := strconv.Atoi(timestamp)
		if err != nil {
			return fmt.Errorf("error parsing %s as epoch timestamp: %w", timestamp, err)
		}
		if len(timestamp) > 10 {
			p.timestamp = time.UnixMilli(int64(t))
		} else {
			p.timestamp = time.Unix(int64(t), 0)
		}
	} else if dateRe.MatchString(timestamp) {
		t, err := time.ParseInLocation(misc.DateFormat, timestamp, time.Local)
		if err != nil {
			return fmt.Errorf("error parsing %s as YYYY-MM-DD: %w", timestamp, err)
		}
		p.timestamp = t
	} else if dateTimeRe.MatchString(timestamp) {
		// Parse accepts optional milliseconds when omitted from the format.
		t, err := time.ParseInLocation(misc.TimeFormatNoMs, timestamp, time.Local)
		if err != nil {
			return fmt.Errorf("error parsing %s as YYYY-MM-DD_hh:mm:ss[.sss]: %w", timestamp, err)
		}
		p.timestamp = t
	} else {
		return fmt.Errorf("timestamp must be epoch time (second or millisecond) or YYYY-MM-DD[_hh:mm:ss[.sss]]")
	}
	return nil
}

func argSubcommand(p *parser, arg string) error {
	if action, ok := subcommands[arg]; ok {
		p.action = action.action
	} else {
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

func argNonFileTimes(p *parser, _ string) error {
	p.nonFileTimes = true
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

func argOneInput(p *parser, arg string) error {
	if p.input1 != "" {
		return fmt.Errorf("at argument \"%s\": an input has already been specified", arg)
	}
	p.input1 = arg
	return nil
}

func argTwoInputs(p *parser, arg string) error {
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
		return handler.fn(p, arg)
	}
	return handler.fn(p, opt)
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
		diff.WithNonFileTimes(p.nonFileTimes),
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
	return r.Init(p.initMode)
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
		Cleanup: p.cleanup,
		NoOp:    p.noOp,
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

func (p *parser) doSync() error {
	s, err := sync.New(
		p.input1,
		p.input2,
		sync.WithFilters(p.filters),
		sync.WithNoOp(p.noOp),
	)
	if err != nil {
		return err
	}
	return s.Sync()
}

func (p *parser) doPushTimes() error {
	r, err := repo.New(
		repo.WithLocalTop(p.top),
		repo.WithS3Client(S3Client),
	)
	if err != nil {
		return err
	}
	return r.PushTimes()
}

func (p *parser) doListVersions() error {
	r, err := repo.New(
		repo.WithLocalTop(p.top),
		repo.WithS3Client(S3Client),
	)
	if err != nil {
		return err
	}
	return r.ListVersions(p.input1, &repo.ListVersionsConfig{
		AsOf:    p.timestamp,
		Long:    p.long,
		Filters: p.filters,
	})
}

func (p *parser) doGet() error {
	r, err := repo.New(
		repo.WithLocalTop(p.top),
		repo.WithS3Client(S3Client),
	)
	if err != nil {
		return err
	}
	return r.Get(p.input1, p.input2, &repo.GetConfig{
		AsOf:    p.timestamp,
		Filters: p.filters,
	})
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
	case actSync:
		return p.doSync()
	case actPushTimes:
		return p.doPushTimes()
	case actListVersions:
		return p.doListVersions()
	case actGet:
		return p.doGet()
	}
	// TEST: NOT COVERED (not reachable, but go 1.22 doesn't see it)
	return nil
}
