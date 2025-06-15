// Package qfs implements the qfs command-line interface.
package qfs

import (
	"context"
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
	"github.com/spf13/cobra"
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
	initCleanRepo bool
	initMigrate   bool
	initMode      repo.InitMode
	timestamp     time.Time
}

// Our command-line syntax is complex and not well-suited to something like
// go-arg or flag, so we parse arguments by hand. We implement a simple state
// machine that maps options to handlers. If an argument starts with `-` or `--`,
// the option's entry is called. Otherwise, the `""` entry is called for
// positional options.

const Version = "0.2.0"

type argHandler struct {
	fn   func(*parser, *cobra.Command, string, string)
	help string
}

const (
	actScan         = "scan"
	actDiff         = "diff"
	actInitRepo     = "init-repo"
	actPush         = "push"
	actPull         = "pull"
	actPushDb       = "push-db"
	actSync         = "sync"
	actPushTimes    = "push-times"
	actListVersions = "list-versions"
	actGet          = "get"
)

func arg(fn func(*parser, *cobra.Command, string, string), help string) argHandler {
	return argHandler{
		fn:   fn,
		help: help,
	}
}

var argTables = func() map[string]map[string]argHandler {
	// Note: some of the arg functions have hard-coded shortcuts. This arg table
	// predates use of cobra. The code is implemented to allow arg functions to be
	// conditional on the flag, but not all of them are.
	var filterArgs = map[string]argHandler{
		"filter":       arg(argFilter, "filter file"),
		"filter-prune": arg(argFilter, "filter file -- read prune/junk only"),
		"include":      arg(argDynamicFilter, "include directive for dynamic filter"),
		"exclude":      arg(argDynamicFilter, "exclude directive for dynamic filter"),
		"prune":        arg(argDynamicFilter, "prune directive for dynamic filter"),
		"junk":         arg(argDynamicFilter, "junk directive for dynamic filter"),
		"files-only":   arg(argFilesOnly, "files and symbolic links only"),
		"no-special":   arg(argNoSpecial, "omit pipes, sockets, and devices"),
	}
	a := map[string]map[string]argHandler{
		actScan: {
			"long":    arg(argLong, "show ownerships"),
			"db":      arg(argDb, "write to specified database file"),
			"cleanup": arg(argCleanup, "remove junk files"),
			"xdev":    arg(argXDev, "don't cross device boundaries"),
			"top":     arg(argTop, "with repo: or repo:site, specific top-level directory"),
		},
		actDiff: {
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
			"no-op":   arg(argNoOp, "don't modify the repository"),
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
			"no-op": arg(argNoOp, "show changes without modifying destination"),
		},
		actPushTimes: {
			"top": arg(argTop, "local repository top-level directory"),
		},
		actListVersions: {
			"top":   arg(argTop, "local repository top-level directory"),
			"as-of": arg(argTimestamp, "ignore anything newer than specified timestamp"),
			"long":  arg(argLong, "include S3 version identifiers"),
		},
		actGet: {
			"top":   arg(argTop, "local repository top-level directory"),
			"as-of": arg(argTimestamp, "ignore anything newer than specified timestamp"),
		},
	}
	for _, i := range []string{actScan, actDiff, actSync, actListVersions, actGet} {
		for arg, fn := range filterArgs {
			a[i][arg] = fn
		}
	}
	return a
}()

func (p *parser) subcommand(
	rootCmd *cobra.Command,
	name string,
	positionalArgs string,
	short string,
	long string,
	run func() error,
) {
	usage := name
	args := cobra.NoArgs
	if len(positionalArgs) > 0 {
		usage += " " + positionalArgs
		n := len(strings.Split(positionalArgs, " "))
		args = argPositional(n, positionalArgs)
	}
	cmd := &cobra.Command{
		Use:   usage,
		Short: short,
		Long:  long,
		Args:  args,
		RunE: func(_cmd *cobra.Command, _args []string) error {
			return run()
		},
	}
	rootCmd.AddCommand(cmd)
	cmdArgs, ok := argTables[name]
	if !ok {
		panic("subcommand called on unknown action " + name)
	}
	for cmdArg, handler := range cmdArgs {
		handler.fn(p, cmd, cmdArg, handler.help)
	}
}

func (p *parser) preRun(_ *cobra.Command, args []string) error {
	if len(args) >= 1 {
		p.input1 = args[0]
	}
	if len(args) >= 2 {
		p.input2 = args[1]
	}
	if p.noOp {
		p.cleanup = false
	}
	if p.initMigrate {
		if p.initCleanRepo {
			return fmt.Errorf("only one init-repo mode option may be given")
		}
		p.initMode = repo.InitMigrate
	} else if p.initCleanRepo {
		p.initMode = repo.InitCleanRepo
	}
	if p.dynamicFilter != nil {
		p.filters = append(p.filters, p.dynamicFilter)
	}
	return nil
}

func argTop(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().StringVar(&p.top, arg, "", help)
}

func argCleanRepo(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVar(&p.initCleanRepo, arg, false, help)
}

func argMigrate(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVar(&p.initMigrate, arg, false, help)
}

func argTimestamp(p *parser, cmd *cobra.Command, arg string, help string) {
	v := newValidator("timestamp", func(timestamp string) error {
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
	})
	cmd.PersistentFlags().Var(v, arg, help)
}

func argFilesOnly(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVarP(&p.filesOnly, arg, "f", false, help)
}

func argNoSpecial(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVar(&p.noSpecial, arg, false, help)
}

func argNonFileTimes(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVar(&p.nonFileTimes, arg, false, help)
}

func argNoOwnerships(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVar(&p.noOwnerships, arg, false, help)
}

func argChecks(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVar(&p.checks, arg, false, help)
}

func argPositional(n int, description string) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) < n {
			return fmt.Errorf("%s must be specified", description)
		} else if len(args) > n {
			have := func() string {
				if len(strings.Split(description, " ")) > 1 {
					return "have"
				} else {
					return "has"
				}
			}()
			return fmt.Errorf("%s %s already been specified", description, have)
		}
		return nil
	}
}

func argDb(p *parser, cmd *cobra.Command, arg string, help string) {
	// If specified multiple times, later overrides earlier.
	cmd.PersistentFlags().StringVarP(&p.db, arg, "d", "", help)
}

func argLong(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVar(&p.long, arg, false, help)
}

func argCleanup(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVar(&p.cleanup, arg, false, help)
}

func argNoOp(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVarP(&p.noOp, arg, "n", false, help)
}

func argLocalFilter(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVar(&p.localFilter, arg, false, help)
}

func argXDev(p *parser, cmd *cobra.Command, arg string, help string) {
	cmd.PersistentFlags().BoolVar(&p.sameDev, arg, false, help)
}

func argFilter(p *parser, cmd *cobra.Command, arg string, help string) {
	v := newValidator("filter-file", func(filename string) error {
		pruneOnly := arg == "filter-prune"
		f := filter.New()
		err := f.ReadFile(fileinfo.NewPath(localsource.New(""), filename), pruneOnly)
		if err != nil {
			return err
		}
		p.filters = append(p.filters, f)
		return nil
	})
	cmd.PersistentFlags().Var(v, arg, help)
}

func argDynamicFilter(p *parser, cmd *cobra.Command, arg string, help string) {
	v := newValidator("dynamic-filter", func(parameter string) error {
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
	})
	cmd.PersistentFlags().Var(v, arg, help)
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

func RunWithArgs(args []string) error {
	os.Args = args
	return Run()
}

func Run() error {
	p := &parser{}
	rootCmd := &cobra.Command{
		Use:           filepath.Base(os.Args[0]),
		SilenceErrors: true,
		SilenceUsage:  true,
		Short:         "Manage flat databases of files",
		Long: `qfs creates a flat-file database of the state of a directory the in
the local file system. The state includes the output of lstat on the
directory and all its contents. qfs has the following capabilities:

* Generation of qfs _databases_ (which are efficient flat files) with
  the optional application of filters
* Comparison of live file systems or databases with each other to
  generate a list of adds, removals, and changes between one file
  system or another. You can compare two file systems, two databases,
  or a file system and a database.
* The concept of a repository and sites, implemented as a location in
  Amazon S3 (or an API-compatible storage location) that serves as a
  backup and allows synchronization
* Synchronization: the ability to _push_ local changes to a repository
  and to _pull_ changes from the repository with the local file system
  with conflict detection, along with the ability to create local
  backups or helper files for moving directly to a different site`,
		Args:              cobra.NoArgs,
		Version:           Version,
		PersistentPreRunE: p.preRun,
	}

	p.subcommand(
		rootCmd,
		actScan,
		"scan-input",
		"Scan a directory, database, or S3 location",
		`Scan a directory, database, repository, or location in S3, applying all
specified filters. scan-input may be one of

* directory - a local directory
* db - path to local qfs database
* repo - the repository indicated by .qfs/repo
* repo:$site - the repository copy of the database for site $site

If -db is given, the result is written to the specified database.
Otherwise, output is written to standard output.
`,
		p.doScan)
	p.subcommand(
		rootCmd,
		actDiff,
		"old-scan-input new-scan-input",
		"Compare two scan inputs, applying all specified filters",
		"",
		p.doDiff)
	p.subcommand(
		rootCmd,
		actInitRepo,
		"",
		"Initialize a repository",
		"",
		p.doInitRepo,
	)
	p.subcommand(
		rootCmd,
		actPush,
		"",
		"Push changes from the local site to the repository",
		"",
		p.doPush,
	)
	p.subcommand(
		rootCmd,
		actPull,
		"",
		"Pull changes from the repository to the local site",
		"",
		p.doPull,
	)
	p.subcommand(
		rootCmd,
		actPushDb,
		"",
		"Regenerate the local site database and write it to the repository",
		`Regenerate the local site database and write it to the repository,
overriding the repository's record of the local site's contents. This can
be useful after restoring a site to replace outdated information in the
repository.`,
		p.doPushDb,
	)
	p.subcommand(
		rootCmd,
		actSync,
		"source-path dest-path",
		"Synchronize a destination with a source",
		`Synchronize a destination directory with the contents of a source directory
subject to the given filters. Similar in spirit to a local rsync using qfs
filters.`,
		p.doSync,
	)
	p.subcommand(
		rootCmd,
		actPushTimes,
		"",
		"List the timestamps of all known pushes",
		"",
		p.doPushTimes,
	)
	p.subcommand(
		rootCmd,
		actListVersions,
		"path-within-repository",
		"List versions of a file",
		`List all the versions in the repository of all the files at or below a
specified location.`,
		p.doListVersions,
	)
	p.subcommand(
		rootCmd,
		actGet,
		"repository-path local-path",
		"Retrieve files from the repository",
		`Retrieve files from the repository; useful for ad-hoc retrieval of files
that are not included by the filter or recovering files that were changed
locally and haven't been pushed.`,
		p.doGet,
	)

	return rootCmd.Execute()
}
