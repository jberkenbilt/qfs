# QFS

Last full review: 2024-05-06

# XXX work in

Refactor
* Work on the push/pull lifecycle tests first so we don't have to keep retesting
* Change database.Memory to maybe fileinfo.Database
* Scanner interface with Scan() that returns fileinfo.Database
  * File system implements existing traverse
  * S3 implements ~/source/s3-list-bucket/
  * Database reads the database
* RepoDb doesn't need s3 timestamp. When listing the bucket, if there are conflicts, delete all but
  the one with the most recent timestamp.
* Keep the local copy of the repo db in sync incrementally as originally planned.
* Repo structure
  prefix/
    .@d,modtime,permissions -- only for root
    some/path@[fld],modtime,[permissions|target]

  So if you have
    login/
      config/
        env
      file
      link -> file

  You would get
    login/.@d,modtime,permissions
    login/config@d,modtime,permissions
    login/config/env@f,modtime,permissions
    login/file@f,modtime,permissions
    login/link@l,modtime,target

  To get versions, do list-object-versions on path@ (including .@) and sort by descending s3
  timestamp across differing keys including delete markers. If a delete marker and a non-deleted
  version have the same timestamp, favor the new version. Ignore anything whose key doesn't match
  after truncating everything after the last @. (Be sure to test with something with @ in the file
  name.) Only one should actually exist, meaning the latest of all except the newest item should be
  deleted. In other words, IsLatest is only allowed be true for a Version (rather than a
  DeleteMarker; indicated by '*' below) for the newest item. Otherwise, the item should be deleted.

```
#!/usr/bin/env python3
import json
from operator import itemgetter

with open('/tmp/a.json', 'r') as f:
    data = json.loads(f.read())

all = []

for i in data['Versions']:
    all.append([
        [i['LastModified'], 1],
        i['Key'],
        i['VersionId'],
        '*' if i['IsLatest'] else '',
    ])
for i in data['DeleteMarkers']:
    all.append([
        [i['LastModified'], 0],
        i['Key'],
        i['VersionId'],
        '',
    ])

all.sort(key=itemgetter(0), reverse=True)
for i in all:
    print(i)
```

* TO DO
  * Document how to remove things. Removing something from a filter causes it to be untracked, not
    to disappear. So
    * If you remove a tracked directory and push/pull, it disappears
    * If you remove something from a filter and push/pull, it stays
    * If you pushed/pulled something by mistake and remove it from the filter, you have to manually
      remove it.
    * We could either add a `repo-rm` or document that you have to remove the files from s3
      (including the directory marker) and re-initialize. Alternatively, push the filter change and
      then run `clean-repo`.
  * implement lifecycle test on what's there so far
  * local-tar
  * clean-repo
  * push-db
  * pull-repo
  * list
  * get
  * rsync
* Decide if site tar is worth it. If not, search for `-save-site` and `-site-file` and move the
  design of that feature to a separate part of the document in case it ever comes back.
* Lifecycle tests
  * remember to check contents of `push` and `pull` files at least once
  * Include a file whose name looks like repo storage e.g. path@d,123,123
  * bootstrap
    * create site1
    * init-repo
    * initial push
    * second push (no changes)
    * pull (no changes)
    * XXX HERE -- decide on what all the files will be
    * create site2 with no filter defined
    * pull
    * create filter locally with some things not in site1's filter
    * pull
    * pull again (no changes)
    * push (get new things)
    * pull (no changes)
    * pull again (no changes)
  * changes
    * change a file in site1
    * change a different file in site2
      * modify a file without changing permissions
      * modify a file and change permissions
      * modify a read-only file
      * change a symlink
      * replace a file with a directory
      * replace a directory with a file
      * replace a file with a link
      * replace a link with a file
      * replace a directory with a link
      * replace a link with a directory
      * change a file's permissions
      * change a directory's permissions
      * remove a file
      * remove a directory
      * remove a link
      * create a file
      * create a directory
      * create a link
    * push site2 -n
    * push site2
    * pull site1 -n (should not see site1 change)
    * push site1 -n (should only see site1 change)
    * pull
    * pull (no changes)
    * push
    * site2
    * push -n (no changes)
    * pull (get change from site1)
    * push (no changes)
  * conflicts
    * site1 change file1 and file2
    * site2 change file1, replace file2 with directory
    * site2 push -n (no conflicts)
    * site1 push -n (no conflicts)
    * site1 push
    * site2 pull -n (conflicts)
    * site2 push -n (conflicts)
    * site2 push (override)
    * site1 pull (take changes from site2)
    * site1 pull (no changes)
    * site2 pull (no changes)
  * exclude something in repo filter, run `clean-repo`, and watch it disappear
  * recreate site
    * delete site2 except .qfs/{repo,site}
    * delete site2 db in repo
    * pull -- should fully reconstruct site
  * revert
    * make change on site1
    * pull (no changes)
    * `push-db`
    * pull (revert change)
  * regenerate db
    * delete object in repo
    * push (no changes)
    * init-db with regeneration
    * push -n (restore deleted file)
    * pull -n (delete local file)
    * push (restore file)
    * pull (no changes)

----------

`qfs` is a tool that allows creation of flat data files that encapsulate the state of a directory in
the local file system. The state includes the output of _lstat_ on the directory and all its
contents. `qfs` includes the following capabilities:
* Generation of qfs _databases_ (which are efficient flat files) with the optional application of
  filters
* Comparison of live file systems or databases with each other to generate a list of adds, removals,
  and changes between one file system or another. You can compare two file systems, two databases,
  or a file system and a database.
* The concept of a repository and sites, implemented as a location in Amazon S3 (or an
  API-compatible storage location) that serves as a backup and allows synchronization
* Synchronization: the ability to _push_ local changes to a repository and to _pull_ changes from
  the repository with the local file system with conflict detection, along with the ability to
  create local backups or helper files for moving directly to a different site

Do you need `qfs`?
* If you are a Windows user and you're just trying to keep documents in sync, use One Drive.
* If you are looking for a way to keep your photos or other binary data in sync, use Dropbox or any
  other similar service.
* If you are a developer trying to keep track of source code, use git.
* If you are a Linux or Mac user who is managing lots of UNIX text and configuration files across
  multiple environments and those files are commingled with files you don't synchronize (browser
  cache, temporary files, or any number of other things that get dumped into your home directory)
  and you are trying to keep those core files in sync seamlessly across multiple computers, `qfs` is
  for you. It is a niche tool for a narrow audience.
* If you are a system administrator who wants to do something and see what changed, `qfs` may be a
  great power tool, though you could achieve the same effect with less elegance and efficiency using
  `find` and other standard shell tools.

You can think of `qfs` repositories as like a cross between `git` and `Dropbox`. Suppose you have a
core collection of files in your home directory, such as "dot files" (shell init files), task lists,
project files, or other files that you want to keep synchronized across multiple systems. If you
only needed that, you could use Dropbox, One Drive, or similar services. But what if those core
files are intermingled with files you don't care about? Managing that can become complex. You could
use `git`, but you'd have a lot of work keeping your `.gitignore` file updated, or you would have to
add a lot of files to the repository that you didn't want. You would have to remember to commit, and
you could never run `git clean`. What if some of the files you want to synchronize are themselves
git repositories? This is where `qfs` comes in. `qfs` is a rewrite in `go`, with enhancements, of
`qsync`, a tool the author wrote in the early 1990s and has used nearly every day since.

Here is a sample workflow for `qfs`. Suppose you have a the following:
* A desktop computer, which we'll call your _home system_, that has all of your important files on
  it
* A laptop that has a subset of files
* A work computer that has a smaller subset of files

Your standard way of working is to sit in front of one of those systems for an extended period of
time and work on stuff. In the process of doing this, you edit some files. Your workflow would like
something like this:

* Sit in front of a particular system, such as your home system, and work on something.
* Run `qfs push` to push your changes to a qfs repository, which also backs them up off-site.
* Go to a different computer, such as your work computer, and run `qfs pull`. No do work on that
  computer.
* Run `qfs push` there, switch to a different system, and run `qfs pull`.

Running `qfs push` at the end of every session and `qfs pull` at the beginning of every session will
help ensure that you always have your files where you want them. It's a lot like Dropbox or One
Drive, but with a lot more control, a command-line interface, and more flexibility -- all stuff that
may appeal to you if you have chosen to live in a Linux or other UNIX environment directly and not
rely on graphical configuration tools, etc.

Even if you don't use repositories, `qfs` is still a useful tool. If you ever want to run something
and see what files it changed, you can use `qfs` to create a before and after "database" and then
diff them and get a list of what changed. You could also do this by running a `find` command before
and after and using regular `diff`, but `qfs diff` produces output that is a little more direct in
telling you what changed.

# Known Issues

* If you exclude a directory and include something below the directory, the directory itself will
  not be included, so its permissions will not be tracked. This is not necessarily a problem.
* At present, pulling contents into read-only directories will not work.
* The `push -save-site site file.tar.gz` and `pull -site-file file.tar.gz` features are not
  implemented because the are complex, error-prone, and probably not necessary. If time proves that
  to be correct, I will remove it from the main documentation and add it to a separate section.
  Otherwise, I will implement it.

# CLI

The `qfs` command is run as
```
qfs subcommand [options]
```

* All options can be `-opt` or `--opt`
* Some commands accept filtering options:
  * One or more filters (see [Filters](#filters) may be given with `-filter` or `-filter-prune`.
    When multiple filters are given, a file must be included by all of them to be included.
  * Explicit rules create a dynamic filter. If any dynamic rules are given, the single dynamic
    filter is used alongside any explicit filters.
  * These arguments are allowed wherever _filter options_ appears
    * `-filter f` -- specify a filter file
    * `-filter-prune f` -- specify a filter file from which only prune and junk directives are read
    * `-include x` -- add an include directive to the dynamic filter
    * `-exclude x` -- add an exclude directive to the dynamic filter
    * `-prune x` -- add a prune directive to the dynamic filter
    * `-junk x` -- add a junk directive to the dynamic filter
    * Options that only apply when scanning a file system (not a database):
      * `-cleanup` -- remove any plain file that is classified as junk by any of the filters
      * `-xdev` -- don't cross device boundaries

## qfs Subcommands

* `scan` -- scan and filter file system or database (replaces `qsfiles` and `qsprint`)
  * Positional: directory or qfs database file
  * _filter options_
  * `-db` -- optionally specify an output database; if not specified, write to stdout
  * `-f` -- include only files and symlinks (same as `-no-special -no-dir`)
  * `-no-special` -- omit special files (devices, pipes, sockets)
  * Only when output is stdout (not a database):
    * `-long` -- if writing to stdout, include uid/gid data, which is usually omitted
* `diff` -- compare two inputs, possibly applying additional filters (replaces `qsdiff`)
  * See [Diff Format](#diff-format)
  * Positional: twice: input, then output directory or database
  * _filter options_
  * `-no-dir-times` -- ignore directory modification time changes
  * `-no-ownerships` -- ignore uid/gid changes
  * `-checks` -- output conflict checking data
* `init-repo` -- initialize a repository
  * See [Sites](#sites)
* `init-site` -- initialize a new site
  * See [Sites](#sites)
* `push`
  * See [Sites](#sites)
  * `-cleanup` -- cleans junk files
  * `-n` -- perform conflict checking but make no changes
  * `-local file.tar.gz` -- save a tar file with changes instead of pushing; useful for backups if offline
  * `-save-site site file.tar.gz` -- save a tar file with changes for site; see [Sites](#sites)
* `pull`
  * See [Sites](#sites)
  * `-n` -- perform conflict checking but make no changes
  * `-local-filter` -- use the local filter; useful for pulling after a filter change
  * `-site-file file.tar.gz` -- use the specified tar file as a source of files that would be
    pulled. This is the file created by `push -save-site`.
* `push-db` -- regenerate local db and push to repository
  * When followed by `pull`, this can be used to revert a site to the state of the repo.
* `pull-repo` -- pull the latest repository database
  * When followed by `push`, this can be used to revert the repository to the state of a site.
* `clean-repo` -- remove files from the repository that are not matched by the filter
  * This can be useful if things are pushed by mistake and then the repo filter is adjusted to
    exclude them.
* `list file` -- list all known versions of a file in the repository
  * For best results, use bucket versioning
* `get file` -- copy a file from the repository
  * `-version v` -- copy the specified version; useful for restoring an old version of a file
  * `-out path` -- write the output to the given location
  * `-replace` -- replace the file with the retrieved version
  * One of `-out` or `-replace` must be given.
* `rsync` -- create equivalent (as much as possible) rsync rules files (replaces `qsync_to_rsync`)
  * _filter options_

# Filters

qfs uses filters to determine which files from a database or directory are relevant for a given
operation. For example, filters can be used to create a database of subset of the files in a
directory or even a subset of the files of another database, and they can be used to narrow to a
common subset of the entries of two databases when generating diffs.

Using a filter, qfs decides whether a file is _included_. A filter is made up of a list of
`include`, `exclude`, and `prune` directives with the following meanings:
* `prune` -- the matching item is omitted recursively, overriding other rules
* `exclude` -- the matching item is excluded, but if it is a directory, its contents may be
  selectively included
* `include` -- the item is included recursively subject to overrides from `prune` or `exclude`

This means that includes and excludes can override each other. Filters rules are applied to paths
from the full path on up to the root. There are three kinds of targets for filter rules:
* _path_ -- the rule applies to the whole path relative to the root of the file collection
* _base_ -- the rule applies to a path whose base (last path element) is the given value
* _pattern_ -- the rule applies to a path whose base (last path element) matches the given regular
  expression

A filter includes two additional pieces of information:
* Whether files are included or excluded by default. If not specified, files are excluded by default
  if there are any `include` directives and are otherwise included by default. This is almost always
  the correct behavior, so it is seldom necessary to explicitly specify the default.
* A pattern matching "junk" files. This is a regular expression applied to the base of each path for
  regular files (not directories, links, or specials) only. Files matching the junk pattern are
  excluded but are also marked as junk, which enables the `-cleanup` option to remove them. This can
  be used for things like editor backup files.

A qfs filter file is a simple text file containing directives and lists of files:
```
:directive:
file
...
```

The following directives are supported:
* `:include:` -- indicates that subsequent files are to be included
* `:exclude:` -- indicates that subsequent files are to be excluded
* `:prune:` -- indicates that subsequent files are to be pruned
* `:junk:regexp` -- sets the junk pattern
* `:read:relative-path` -- lexically includes another filter whose path is given relative to current
  filter

Files may be one of the following:
* An ordinary path
* `*/base` -- indicates a _base_ rule
* `:re:regexp` -- indicates a _pattern_ rule
* `*.ext` -- matches plain files that end with `.ext` (internally generates a pattern rule)
* `.` -- for `:include:` and `:exclude:` only, this overrides the default inclusion behavior.

Paths are always interpreted as relative to the working directory in which `qfs` was started, but
the argument to `:read:` is interpreted as relative to the filter being read. The reason is that it
allows filters to be read directly from the repository (see [Sites](#sites)) for pull operations,
and it also allows filters to be read from portable locations outside the file collection.

## Filter inclusion algorithm

* When there are multiple filters, a path must be included by all filters to be included.
* If a path or any ancestor directory matches a `prune` directive, the file is excluded.
* Otherwise, if the last path element matches a `junk` rule, it is excluded.
* Otherwise, if a path or any parent matches an `include` directive, the file is included.
* Otherwise, if a path or any parent matches an `excluded` directive, the file is excluded.
* Otherwise, the file's status is the default include status.

That means that, with this filter:
```
:prune:
a/prune
:include:
*/include
:exclude:
a/exclude
```
* `include/x` is included because of `*/include`
* `a/prune/x` is excluded because of `a/prune`
* `a/prune/include/x` is excluded because of `a/prune` since prune is stronger than include
* `a/exclude/x` is excluded because of `a/exclude`
* `a/exclude/include/x` is included because of `*/include` since the include rule matches a longer
  portion of the path than the exclude rule
* `a/x` is excluded because of the presence of `include` rules means files are excluded by default

# Diff Format

The `qfs diff` command generates output consisting of lines that provide information and are also
used for [conflict detection](#conflict-detection). Each line is one of the following:

* `check [mtime] ...] - filename` -- Verify that the file has one of the listed modification time
  values or doesn't exist. If none given, verify that the file does not exist. Only if `-checks` is
  specified.
* `typechange filename` -- the type of a file changed; this is strictly informational as `rm` and
  `mkdir/add` directives will also appear
* `change filename` -- the file type is the same but the content or special (link target, device
  numbers) changed
* `rm file` -- a file, link, directory, or special has disappeared
* `mkdir dir` -- directory was added
* `add filename` -- file, link, or special was added
* `chmod nnnn filename` -- mode change without content change
* `chown [nnnn]:[nnnn] filename` -- uid/gid change without content change. Omitted with
  `-no-ownerships`.
* `mtime dir` -- a directory's modification time changed. Omitted with `-no-dir-times`.

# Database

qfs uses a simple flat file database format for simplicity and efficiency. qfs can read qsync v3
databases and read and write qfs databases.

```
QFS 1
len[/same] @ path @ file-type @ mtime @ size @ mode @ uid @ gid @ special \n
```
Changes:
* no delimiter at beginning or end of line
* path is not prepended by `./`; root is still `.`
* file-type is one of
  * f = file
  * d = directory
  * l = link
  * s = socket
  * p = pipe
  * b = block device
  * c = character device
  * x = unknown
* modtime is millisecond -- use pax format when writing tar files
* mode is just 4-digit octal
* special is major,minor for block and character, target for symlink
* size is 0 for non-files
* dropping special for directories
* dropping DOS attribute support
* dropping link count

Note that when sites are being used, the current site's database is omitted from itself. The site
algorithms deal with this.

# Sites

qfs implements the concept of sites, which use the core `scan` and `diff` features to push and pull
changes to a central repository with conflict detection.

## Site Concepts

* File collection -- a group of files, e.g., a home directory, that is backed up to S3 and fully or
  partially shared across multiple locations (e.g., laptop, work computer, other systems). A file
  collection is associated with a global filter.
* Site -- a specific instance of a subset of the file collection at a specific location. Each site
  is defined by a filter that defines a subset of the entire collection of files. When working with
  a site, the global filter and site's filter are both in effect.
* Repository -- an area in S3 that contains all the files across all sites. Every site exchanges all
  files in the intersection of the site filter and the global filter to the repository. All files in
  the collection exist in the repository even if no single site contains all the files.

A file collection is defined by containing `.qfs` directory. There are some files that are
synchronized between local sites and the repository, some that exist only in the repository, and
some that exist only in local sites.
```
.qfs/

  # Items on sites and the repository
  filters/
    repo -- the gloabl filter
    $site -- the filter for the site called $site
    ... -- other files (version control, fragments included by filters, etc.)
  db/
    repo -- repository database
    $site -- site database; each site contains only its own database

  # Items only on sites
  repo -- location of the repo as s3://bucket/prefix
  site -- contains name of current site
  db/
    $site.tmp -- working copy of repo's copy of site db; uploaded to repo after pull
    repo.tmp -- pending copy of repo db; uploaded to repo after push
  push -- diff output for most recent push; indicates push without pull; deleted by pull
  pull -- diff output from most recent pull; kept for future reference

  # Items only in the repository
  busy -- exists while the repository is being updated, indicating db may be stale
```

qfs does not support syncing directly from one site to another. Everything goes through the
repository. See [Fully Offline Operation](#fully-offline-operation) for a discussion of how this
could be supported if needed.

## Repository Details

A repository resides in an S3 bucket. There is no strong concurrency protection. This is intended to
be used by one person, one site at a time. It is not a content management or version control system.
The repository stores information about the files in S3 object metadata. It also stores a repository
database that allows qfs to obtain information about repository files from a combination of the
output of `list-objects-v2` and the database. There is minimal locking in the form of a `busy`
object that can be used to detect when the repository is in an inconsistent state. If a push
operation is interrupted, the repository state can be repaired by regenerating the database from
object metadata. The `busy` object is not sufficient to protect against race conditions from
multiple simultaneous updaters, but on a human timescale, it can protect against accidental
concurrent use or detect if an operation failed before completing.

The repository contains a key for each file in the collection with the following specifics:
* `.qfs/filters` is always included, but the rest of `.qfs` is excluded, regardless of filters
* Files in `.qfs/db` are copied to the repository explicitly and do not appear in databases
* Only files, links, and directories are represented
* Directory keys end with `/`, and directory objects are zero-length
* Symbolic link objects are zero-length
* All keys have object metadata containing a `qfs` key whose value is `modtime mode` for files and
  directories, and `modtime ->target` for symbolic links, where `modtime` is a
  millisecond-granularity timestamp, and `mode` is a four-digital octal mode.

The repository database looks like a qfs database with the following exceptions:
* The header is the line `QFS REPO 1`
* The `uid` and `gid` fields are omitted. In their place, the last modified time of the object in S3
  is stored as a millisecond-granularity timestamp.
* When reading a repository database, the `uid` and `gid` values for every row are set to the
  current user and group ID.

The repository and site databases is stored under `.qfs/db`. Although databases never contain
information about themselves, their object metadata stores the modification time of the local copy
on the system that most recently pushed. If necessary, it is possible to entirely reconstruct the
database by recursively listing the prefix and reading the `qfs` metadata key. This can be done
using the `init-repo` command.

When qfs begins making changes to a repository that cause drift between the actual state and the
database, it creates an object called `.qfs/busy`. When it has successfully updated the repository,
it removes `.qfs/busy`. If a push or pull operation detects the presence of `.qfs/busy`, it requires
the user to reconcile the database first before it does anything.

Many operations operate on behalf of one or more sites and the repository. The following filtering
rules are always implied:
* Always use the global filter.
* Use the filters for each site. For push or pull, that means the main site's filter. For
  `-save-site`, also use the other site's filter.
* In `.qfs`, `filters/` is always included, and everything else is excluded. Database files are
  handled explicitly.

## Operations

### Note about diff

Many site operations create diffs. For site operations, all diffs are generated with
`-no-ownerships`, `-no-dir-times`, `-no-special`, and `-checks`.

### Initialize/Repair Repository

To initialize a new repository or reconstruct the database if it becomes damaged, create the
`.qfs/repo` file locally, and run `qfs init-repo`. This does the following:
* If `.qfs/repo` exists, prompt for confirmation before regenerating the database.
* Create `.qfs/busy` if not already present
* Generate `.qfs/db/repo.tmp` locally by scanning the repository in S3 and uploads it to the
  repository as `.qfs/db/repo` with correct metadata. No filters are used when scanning the
  repository's contents to generate its database as it is assumed that the repository contains no
  extraneous files.
* Rename `.qfs/db/repo.tmp` to `.qfs/db/repo` locally
* Remove `.qfs/busy` from the repository

After this, it is possible to add sites and start pushing and pulling. You will need to create
`.qfs/filters/repo` before the first push.

### Add/Repair Site

To set up a new site, do the following on the site:
* Write the repository locaton to `.qfs/repo`
* Write the site's name to `.qfs/site`
* If you are recreating a site that previously existed, remove any existing `.qfs/sites/$site/db`
  file. This tells the repository that the site has no contents.
* Run `qfs pull`. If there is no filter for the site, this will only pull the `.qfs/filters`
  directory. In that case, you can create a local filter and run `qfs pull` again.

Note that bad things will happen if you have two simultaneously existing sites with the same name.
If you need to recreate a previously existing site, such as if you lose a site and want to pull its
files down again, you should remove `.qfs/sites/$site/db` from the repository.

Note that you can also recover a site by restoring the site from some other backup, running `qfs
push-db`, and running `qfs pull`. This could be useful if you have a local backup that is faster to
restore from than pulling everything from the repository.

### Push

Run `qfs push`. This does the following:
* If `.qfs/busy` exists in the repository, stop and tell the user to repair the database with `qfs
  init-db`.
* Regenerate the local database as `.qfs/db/$site`, applying only prune (and junk) directives from
  the repository and site filters, omitting special files, and automatically handling `.qfs` subject
  to the rules above. Using only prune entries makes the site database more useful and also improves
  the behavior of when filters are updated after a site has been in use for a while. For example, if
  there are files in the repository that you had locally but had not included in the filter, if you
  subsequently add them to the filter, the next `qfs pull` operation will have correct knowledge of
  what you already had.
* Diff the local site's database with the _local copy_ of the repository database. Use the site's
  filter and the repository filter with the automatic settings for the `.qfs` directory as described
  above. Using the local copy of the repository's database makes it safe to run multiple push
  operations in succession without doing an intervening pull, enabling conflict detection to work
  properly. This is discussed in more detail below.
* Store the diff as `.qfs/push`. In addition to be informational, this may be included in a local
  tar file if requested, and it also indicates that a push has been done without a pull.
* Create a working repository database by loading the repository's copy of its database into memory.
  If the metadata on the repository database matches the local copy, load the local copy.
* Perform conflict checking
  * Check against the working repository database to make sure that, for each `check` statement, the
    file either does not exist or has one of the listed modification times.
  * If conflicts are found, offer to abort or override.
* If `-n` was given, stop
* If `-local` was given
  * Create a tar file whose first entries are `.qfs/push` and `.qfs/db/$site` and which subsequently
    contains the files that would be pushed
* Otherwise, update the repository:
  * Create `.qfs/busy` on the repository
  * Apply changes by processing the diff. All changes are made to the repository. Ideally, changes
    would be made to the working database, but this turns out to be impractical because of issues
    with S3 timestamp granularity. See notes.
    * Recursively remove anything marked `rm` from s3
    * For each added or changed file, including metadata changes, upload a new version with
      appropriate metadata.
  * Regenerate the repository database, writing locally to `.qfs/db/repo.tmp`
  * Upload `.qfs/db/repo.tmp` to `.qfs/db/repo` with correct metadata
  * Upload `.qfs/db/$site` with correct metadata
  * Move `.qfs/db/repo.tmp` to `.qfs/db/repo` locally
  * Delete `.qfs/busy` from the repository
  * If `-save-site` was given
    * Diff the site database with the _local copy_ of the other site's database, applying the global
      filter and both site filters. Include the `.qfs` directory per above rules, excluding the
      other site's database. Save to a temporary location.
    * Write a tar file whose first entry is the contents of the diff as
      `.qfs/pending/$othersite/diff` and which contains any files that differ between the current
      state and the local record of the other site's state. Note that the file does not exist
      locally. It is just in the tar file. This serves the dual purpose of allowing the recipient to
      make sure it is the intended target and of causing the file to go in a sensible place if the
      tar file is extracted manually.
    * This can be applied locally on the other site prior to the other site doing a pull and may reduce
      S3 traffic. See [Pull](#pull) for a description of how this is used.

Notes:
* The `-save-site` only applies when pushing to a repository.
* Sites always use their locally cached copies of remote state.

For an explanation of these behaviors, see [Conflict Detection](#conflict-detection) below.

The handling of the repository database is particularly important. Using our local cache of the repo
database means that we will only send files that we changed locally relative to their state in the
repository since our last pull. This prevents us from reverting changes pushed by someone else in
the event that we do a push without doing a pull first, which is an explicitly supported thing to
do. Using the repository's copy of the database as the basis for the working repository database is
also important since, otherwise, multiple pushes from different sites would cause the repository's
database to drift.

### Pull

Run `qfs pull`. This does the following:
* If `.qfs/busy` exists in the repository, stop and tell the user to repair the database with `qfs
  init-db`.
* Get the current site from `.qfs/site`
* If there is a site tar file:
  * Make sure the first entry is `.qfs/pending/$site/diff`. If not, abort.
  * Read the diff into memory and perform conflict checking. See [Conflict Detection](#Conflict
    Detection).
  * If `-n` was given, skip ahead to "Continue from here if `-n` was given.". Otherwise, continue
    following these steps.
  * If there were any conflicts, offer to abort or override.
  * Apply changes by processing the diff.
    * Using the diff from the site tar file (if any):
      * Recursively remove anything marked `rm`
      * For each added or changed file
        * If the old file already has the correct modification time, or if it is a link that already
          has the right target, leave it alone and record that it doesn't have to be extracted.
        * Otherwise, make sure it is writable by temporarily overriding its and/or its parents'
          permissions for the duration of the write. The parent directory has to be writable by the
          user in all cases, and for files, the file also has to be user-writable.
          * Note: in the initial implementation, if the directory is not writable, it will be an
            error. The user can change the permissions and rerun pull, at which point pull will
            restore the correct permissions.
      * For each changed or added link, delete the old link.
      * Extract the site tar file, keeping track of end directory permissions, and skipping any
        files or links that we determined already to be up to date.
      * From bottom to top, process any chmod operations. For directories, in priority order, use the
        value from an explicit chmod or the value in the tar header.
* Continue from here if `-n` was given.
* Download the repository's copy of its own database to `.qfs/db/repo.tmp`
* Read the repository's copy of the current site's database into memory, and diff it against the
  repository's copy of its own database (which we just downloaded) using the repository's copies of
  the global filter and the site's filter. If `-local-filter` was given, use the local filter
  instead of the repository filter. This is useful if you modify the filter to include
  previously-excluded items that are present on the repository and want to pull again to download
  them. For bootstrapping to work:
  * If the repository doesn't have a copy of the site's database, treat it as empty.
  * If the repository doesn't have a copy of the site's filter, check locally. If there is no local
    filter either, then treat the filter as one that excludes everything, which means only the
    `.qfs/filters` directory will be included. This behavior is for bootstrapping.
* Perform conflict checking. Note that if files were extracted from the tar file, most of the local
  files will already have their desired end states.
* If `-n` was given, stop.
* If there were any conflicts, offer to abort or override.
* Apply changes by downloading from the repository using the same steps as above. Keep the local
  (in-memory) copy of the repository's copy of the site's database in sync so that it is updated
  with only the changes that were pulled.
  * A key point here is that, if a site tar was used, many (or even all) of the differences will
    already have been applied. There may be changes pushed from other sites (or even the same remote
    site) that also need to be applied, though pushing from one site with a site tar and then
    pushing from the same site without a site-tar is likely to result in false positives with
    conflict checking.
* Write the updated repository's site database to `.qfs/db/$site.tmp` and uploaded it to the
  repository as `.qfs/db/$site`. This makes it safe to do multiple pulls on a site without doing any
  intervening pushes.
* Move `.qfs/db/repo.tmp` to `.qfs/db/repo`
* Remove `.qfs/push`. We leave `.qfs/pull` and `.qfs/db/$site.tmp` in place for future reference.

### Working with individual files

Using the `qfs list` and `qfs get` commands, it is possible to view and retrieve old versions of
files. By using bucket versioning with suitable life cycle rules, we can have a rich version history
for every file much as would be the case with something like Dropbox.

There is facility for manually pushing a single file to a repository. This would be hard to do while
keeping databases in sync and avoiding drift. If things need to be restored, fix the files locally
and then run a push.

## Conflict Detection

An important part of qfs sites is the ability to detect conflicts. This is similar to conflicts that
appear in a version control system, and is perhaps the most powerful aspect of qfs. The key to
implementing conflict detection is that all site-related diff operations compare the current site's
state with the current site's copy of the remote site's state rather than using the actual remote
site's state. The intention is that it should be safe to interleave pushes and pulls from multiple
sites, including doing multiple pushes without an intervening pull, and always have working conflict
detection without the risk of one site reverting another site's change. While this isn't an ordinary
"standard" workflow, there are times when it might make sense, and it is easy to do it accidentally.
Accidentally running operations out of order should never result in loss of data.

If you have files distributed across multiple sites (e.g., home system, work laptop), a standard
workflow might this:
* Everything is in sync
* Make changes on home system
* `qfs push` on home system
* Go to work system
* `qfs pull` -- this retrieves changes made on home system
* Make changes on work system
* `qfs push`
* Go to home system
* `qfs pull` -- this retrieves changes made on work system

The above could be achieved with Dropbox or rsync. What qfs enables is something like this:

* Everything is in sync
* Make changes on home system
* `qfs push` on home system
* Go to work system
* `qfs pull` -- this retrieves changes made on home system
* Make changes on work system
* Connect remotely to home system and update one file there that is shared but has not been modified
  on the work system
* `qfs push` on home system -- this sends only the newly changed file since it compares against our
  last known state of the repository. Note that there were two pushes from the home system without
  an intervening pull. When the home system updates the repository, it downloads the repository copy
  of the database and updates only the parts that it is changing, thus ensuring that the repository
  copy of the database does not drift.
* `qfs pull` on work system -- pulls down only the newly updated file since it uses the repo's last
  copy of the work site's database; also updates our local copy of the repository database
* `qfs push` on work system -- the file pulled from home is all set and is not considered out of
  date
* Go to home system
* `qfs pull` -- this retrieves changes made on work system

Note that the presence of the `.qfs/push` file indicates that you have done a push without doing a
pull. If you are starting a work session on that site as your primary, you should generally do a
pull to have the latest files, though `qfs` makes it safe to do multiple pulls without intervening
pushes, we can be useful in rare situation when working on more than one system at a time or if you
forgot to do a push from another system. Under steady state, all but one site would have this file.
The one that doesn't have it is the active site. It's possible to push from the active site and then
switch to any other site without having to know in advance which one it is going to be. If you do
know which site you're going to, you can use the `-save-site` option to send the files locally and
save network traffic.

Consider the following specific scenarios:

* All sites start off in sync.
* Site A updates file `project/file1`
* Site B updates file `project/file2`
* Site A does a push. This updates `project/file1` on the repository.

Now consider what happens when site B does a push without first doing a pull. If site B had the
latest copy of the repository state, it would see its copy of `project/file1` as different from the
repository copy and would revert A's change. Instead, it sees its copy as identical to what it
thinks the repository should have. Therefore:

* Site B does a push. This updates `project/file2` on the repository. `project/file1` is not
  touched.
* Site A does a pull. This updates site A's copy of `project/file2` and also its copy of the
  repository's state.
* Site B does a pull. This updates site B's copy of `project/file1` and also its copy of the
  repository's state.

Now consider what would happen if site B did a pull before its push. In this case:

* Site B does a pull. This pulls `project/file1` from the repository as well as the repository's
  state.
* Site B does a push. As before, this only updates `project/file2`, but this time, it's because both
  `project/file1` and the repository state have been downloaded since A's last push.

The use of local copies of state makes it possible for everything to work seamlessly when different
sites update different files.

Now consider a different scenario.

* Site A updates `project/file1`
* Site B updates `project/file1` (the same file -- this is a conflict)
* Site A does a push, updating `project/file1` on the repository.

Now, if site B does a push, qfs will detect that the state of the file in the repository matches
neither what was site B started with nor its current state, so this is detected as a conflict.

What happens if site B does a pull? In this case, qfs will see that the local copy matches neither
the repository state nor the repository's last record of the file as it existed on B, so this is
also detected as a conflict.

For the next scenario, we can consider what happens when `-save-site` is used. This is a helper
feature to save bandwidth when transferring large amounts of data within the local network. Keep in
mind that `diff` considers a file to not be a conflict if the destination file is either not present
at all or if it matches _either_ the last known state of the file or the desired state. This makes
it possible to momentarily short-circuit push and pull. This short-circuit can be done using a
helper tar file as created with `-save-site`, or it can be even be done by using rsync or scp to
copy individual files. Consider this:

* Site A updates project/file1
* Site A does a push with `-save-site B`. This pushes `project/file1` to the repository and also
  includes it in a tar file since A's record of B's state shows that the file has changed.
* The tar file is copied to site B and applied there.
* Site B does a pull.
* The pull believes that `project/file1` has changed since the repository's last record of B's
  state, but, when doing conflict checking, it sees that the state actually matches the desired
  state, so it doesn't have to download it.
* Site B does a push. At this point, `project/file1` matches the repository's state, so it is not
  included in the push. The push updates the repository's copy of site B's state.
* This means that the state of A, B, and the repository is identical to what it would have been
  without use of the helper file, but we avoided copying `project/file1` from S3 to site B, which
  can be a big savings if A and B are on the same network.

## Fully Offline Operation

With qsync, there was no central repository, and all syncing was done pairwise between sites. With
the qfs repository, we have a lot more flexibility in syncing at the expense of losing fully
offline operation. Such offline operation could be useful if syncing in one direction to a file that
never has Internet connectivity, such as a system in a classified environment. If we wanted to
support this mode of operation, it could be done with the following changes:

* When generating a tar file for `-save-site`, create and update a working copy of the remote site's
  database and store it in `.qfs/pending/$othersite/db` just as we do for the pending copy of the
  locally updated repository database
* When doing a subsequent push with the same `-save-site`, if `.qfs/pending/$othersite/db` already
  exists, offer to move it to `.qfs/sites/$othersite/db`, which would indicate that the changes had
  been applied, or ignore it.
* When doing a pull, skip all the parts that interact with the repository and keep the parts that
  interact with the tar file.

This would give us the following workflow:
* On a connected site, run `qfs push -save-site disconnected /tmp/sync-disconnected.tar.gz`
* Copy the tar file to the disconnected site using some other means such as writing to removable
  media
* On the disconnected site, run `qfs pull -site-file /tmp/sync-disconnected.tar.gz`
* On the connected site, move `.qfs/pending/$othersite/db` to `.qfs/sites/$othersite/db`

There would likely need to be some indicator on the disconnected site that it was an offline site,
perhaps by having a special value, such as `offline` as the content of `.qfs/repo/config`.

# Bootstrap Walk-through

* Initialize the repository
  ```
  mkdir .qfs
  echo s3://bucket/prefix > .qfs/repo
  qfs init-repo
  ```
* Create the repo filter in `.qfs/filters/repo`
* Initialize site
  ```
  echo site1 > .qfs/site
  ```
* Create site filter in `.qfs/filters/site1`
* `qfs push`


# Other Notes

Use [Minio](https://min.io) for testing or to create a local S3 API-compatible storage area for a
local repository.

For `push` and `pull`, all prompts are structured so that `y` is the safe answer. That helps protect
against muscle-memory `y` responses to abnormal situations such as conflicts.

The `pull` operation modifies an in-memory copy of the site's database as last known by the
repository and pushes it back to the site. The file then represents the repository's concept of the
site's contents, which includes changes just pulled but not other changes that haven't yet been
pushed. This makes a subsequent pull not include the files that were just pulled without causing it
to revert local changes that haven't been pushed.

The original hope was that `push` could incrementally update its local copy of the repository's
database and push that back, but this turns out to be impractical. When you do a `put-object` in S3,
the only way to get the object's granular timestamp is to do a `list-objects-v2` on the
just-uploaded object since `put-object` doesn't return the timestamp, and `head-object` returns it
with second-granularity instead of millisecond-granularity. It's not that efficient to call
`list-objects-v2` on lots of individual keys, so instead, we just re-traverse to update the
database. That traversal is done with the old database as a reference, so `head-object` is only
called on newly created files.

# Comparison with qsync

Unless you are the author of `qfs` or one of a small handful of people who knew the author
personally. You probably don't use `qsync`, and you can skip this.

* `qfs` replaces `qsfiles`, `qsdiff`, `qsprint`, `qsync_to_rsync`, `make_sync`, and `apply_sync`.
  The rest of qsync has been retired.
* qfs uses the qsync filter format with the addition of
  ```
  :junk:(junk-regexp)
  :re:(pattern-rule)
  ```
  and with the difference that the argument to `:read:` is interpreted as relative to the filter.
  Paths are are always interpreted as relative to the working directory in which `qfs` was started.
  Also, pruned directories are entirely omitted from the database. In qsync, they appeared with a
  "special" indicating an entry count of `-1`.
* The `diff` format is completely different.
