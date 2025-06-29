# QFS

Last full review: 2024-06-10

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
* If you are a regular Mac or Windows user and you're just trying to keep documents in sync, use
  OneDrive, Dropbox, or similar.
* If you are a developer trying to keep track of source code, use git.
* If you are a Linux or Mac user who is managing lots of UNIX text and configuration files across
  multiple environments and those files are commingled with files you don't synchronize (browser
  cache, temporary files, or any number of other things that get dumped into your home directory)
  and you are trying to keep those core files in sync seamlessly across multiple computers using the
  command line, `qfs` is for you. It is a niche tool for a narrow audience.
* If you are a system administrator who wants to do something and see what changed, `qfs` may be a
  great power tool, though you could achieve the same effect with less elegance and efficiency using
  `find` and other standard shell tools.

You can think of `qfs` repositories as like a cross between `git` and `Dropbox`. Suppose you have a
core collection of files in your home directory, such as "dot files" (shell init files), task lists,
project files, or other files that you want to keep synchronized across multiple systems. If you
only needed that, you could use Dropbox, OneDrive, or similar services. But what if those core
files are intermingled with files you don't care about? Managing that can become complex. You could
use `git`, but you'd have a lot of work keeping your `.gitignore` file updated, or you would have to
add a lot of files to the repository that you didn't want. You would have to remember to commit, and
you could never run `git clean`. What if some of the files you want to synchronize are themselves
git repositories? This is where `qfs` comes in. `qfs` is a rewrite in `go`, with enhancements, of
`qsync`, a tool the author wrote in the early 1990s and used nearly every day until `qfs` came
around. `qfs` fills in all the gaps I felt with `sync` and switches to an online repository rather
than completely disconnected pair-wise synchronization. (When I first wrote `qsync`, I kept things
synchronized across multiple disconnected environments using floppies and zip disks, but I never do
that anymore.)

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
help ensure that you always have your files where you want them. It's a lot like Dropbox or
OneDrive, but with a lot more control, a command-line interface, and more flexibility -- all stuff
that may appeal to you if you have chosen to live in a Linux or other UNIX environment directly and
not rely on graphical configuration tools, etc.

Even if you don't use repositories, `qfs` is still a useful tool. If you ever want to run something
and see what files it changed, you can use `qfs` to create a before and after "database" and then
diff them and get a list of what changed. You could also do this by running a `find` command before
and after and using regular `diff`, but `qfs diff` produces output that is a little more direct in
telling you what changed.

# Known Issues

* If you include a directory by pattern or base, otherwise excluded ancestor directories will not be
  included. This usually isn't a problem, but it could cause an issue with `sync` if any pattern or
  base includes are used. See comments in filter.go around fullPath. This is hard to resolve because
  we don't know the list of paths in advance. It might be possible to deal with this by
  post-processing the diff result used with sync.
* It would be nice if pushing to the repository would remove anything on the repository that is not
  included by the repository filter. An initial attempt was made by adding the concept of a
  `destFilter` that would would be checked separately in diff.compare with the behavior that items
  that exist in the destination and are excluded by the destination filter would be added to `Rm`.
  This ran into two problems:
  * Since the destination database was generated with filters applied, those files are never even
    seen by diff
  * Without implicit descendant inclusion, this would cause removal of directories that contain
    included files but are not themselves included.
* At present, pulling contents into read-only directories will not work.

# CLI

The `qfs` command is run as
```
qfs subcommand [options]
```

* All dates and times options are local times represented as `yyyy-mm-dd[_hh:mm:ss[.sss]]`.
* Some commands accept filtering options:
  * One or more filters (see [Filters](#filters) may be given with `--filter` or `--filter-prune`.
    When multiple filters are given, a file must be included by all of them to be included.
  * Explicit rules create a dynamic filter. If any dynamic rules are given, the single dynamic
    filter is used alongside any explicit filters.
  * These arguments are allowed wherever _filter options_ appears
    * `--filter f` -- specify a filter file
    * `--filter-prune f` -- specify a filter file from which only prune and junk directives are read
    * `--include x` -- add an include directive to the dynamic filter
    * `--exclude x` -- add an exclude directive to the dynamic filter
    * `--prune x` -- add a prune directive to the dynamic filter
    * `--junk x` -- add a junk directive to the dynamic filter
    * Options that only apply when scanning a file system (not a database):
      * `--cleanup` -- remove any plain file that is classified as junk by any of the filters
      * `--xdev` -- don't cross device boundaries
* All commands that operate on the repository look for a directory called `.qfs` in the current
  directory and accept `--top path` to specify a different top-level directory of the repository.

## qfs Subcommands

* `scan` -- scan and filter file system or database (replaces `qsfiles` and `qsprint`)
  * Positional: one of
    * local directory
    * local database file
    * `repo:` -- scan repository with repo encoding awareness
    * `repo:$site` -- scan repository copy of site database for given site
      * Example: to see what a different site may have in a particular directory, you could run
      `qfs scan repo:other-site --include some/path`
    * `s3://bucket[/prefix]` -- general concurrent S3 scan, much faster than `aws s3 ls`
      * `--db` is ignored
      * With `--long`, output `mtime size key`; otherwise, just output `key`
      * Output order is non-deterministic
  * _filter options_
  * `--db` -- optionally specify an output database; if not specified, write to stdout in
    human-readable form
  * `-f|--files-only` -- include only files and symlinks
  * `--no-special` -- omit special files (devices, pipes, sockets)
  * `--top path` -- specify top-level directory of repository for `repo:...` only
  * Only when output is stdout (not a database):
    * `--long` -- if writing to stdout, include uid/gid data, which is usually omitted
* `diff` -- compare two inputs, possibly applying additional filters (replaces `qsdiff`)
  * See [Diff Format](#diff-format)
  * Positional: twice: input, then output directory or database
  * _filter options_
  * `--non-file-times` -- include modification time changes of non-files, which are usually ignored
  * `--no-ownerships` -- ignore uid/gid changes
  * `--checks` -- output conflict checking data
* `init-repo` -- initialize a repository
  * See [Sites](#sites)
  * `--clean-repo` -- removes all objects under the prefix that are not included by the filter. This
    includes objects that weren't put there by qfs.
  * `--migrate` -- converts an area in S3 populated by `aws s3 sync` to qfs -- see [Migration From S3
    Sync](#migration-from-s3-sync).
* `init-site` -- initialize a new site
  * See [Sites](#sites)
* `push`
  * See [Sites](#sites)
  * `--cleanup` -- cleans junk files
  * `-n|--no-op` -- perform conflict checking but make no changes
* `pull`
  * See [Sites](#sites)
  * `-n|--no-op` -- perform conflict checking but make no changes
  * `--local-filter` -- use the local filter; useful for pulling after a filter change
* `push-db` -- regenerate local db and push to repository
  * When followed by `pull`, this can be used to revert a site to the state of the repo.
* `push-times` -- list the times at which pushes were made; useful for `list-versions` and `get`
* `list-versions path` -- list all known versions of file in the repository at or below a specified
  path. For this to be useful, bucket versioning should be enabled.
  * _filter options_
  * `--not-after timestamp` -- list versions no later than the given time. The timestamp may be
    specified as either an epoch time with second or millisecond granularity or a string of the form
    `yyyy-mm-dd` or `yyyy-mm-dd_hh:mm:ss`. Epoch times are always interpreted as UTC. The other
    format is interpreted as local time. Note that S3 version timestamp granularity is one second.
  * `--long` --show key and version
* `get path save-location` -- copy a file/directory from the repository and save relative to the
  specified location; `save-location/path` must not exist.
  * _filter options_
  * `--as-of timestamp` -- get the file as it existed in the repository at the given time. The
    timestamp has the same format as `--not-after` for `list-versions`.
* `sync src dest` -- synchronize the destination directory with the source directory subject to
  filtering rules. Files are added, updated, or removed from dest so that dest contains only files
  from src that are included by the filters.
  * _filter options_
  * `-n|--no-op` -- report what would be done without doing it

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
  values or doesn't exist. If none given, verify that the file does not exist. Only if `--checks` is
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
  `--no-ownerships`.
* `mtime dir` -- a modification time changed of other than a file; only with `--non-file-times`.

# Database

`qfs` uses a simple flat file database format for simplicity and efficiency. `qfs` can read qsync v3
databases and read and write qfs databases. Below, `@` represents a null character, and the spaces
appear for clarity.

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
    repo -- the global filter
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
repository. If we wanted to support that in the future, it could be done by adding the ability to
create a tarfile (for example) of all the changes from one database to another and having a program
that extracted that and made any desired modifications, which is basically how qsync worked.

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

The repository contains a key for each file in the collection under the specified prefix. A file,
directory, or link on the site is represented in the repository by the key
`localpath@type,modtime,{permissions|target}`, where `type` is one of `d`, `f`, or `l`, `modtime` is
a millisecond-granularity timestamp, `permissions` is a four-digit octal value (for directories and
files), and `target` is the target of a link. Any `@` that appears in the path or link target is
doubled. Directories and links are zero-length objects.

Examples:
* In repository whose prefix is `prefix`, a symbolic link called `one/two@three` that pointed to
  `../four@five` would be represented by the zero-length object with key
  `prefix/one/two@@three@l,modtime,../four@@five`
* If you had this structure:
  ```
  login/
    config/
      env
    file
    link -> file
  ```
  the repository would contain
  ```
  prefix/login/.@d,modtime,permissions
  prefix/login/config@d,modtime,permissions
  prefix/login/config/env@f,modtime,permissions
  prefix/login/file@f,modtime,permissions
  prefix/login/link@l,modtime,target
  ```

Why do we use this scheme instead of storing metadata on the object using S3 object metadata? There
are a few reasons:
* The scheme used by `qfs` allows us to determine whether a file is up-to-date in the repository
  using the output of `ListObjectsV2` only. `qfs` incorporates a concurrent S3 bucket listing
  algorithm that enables it to do this many times faster than a sequential bucket listing.
* As of initial writing (May 2024), objects are stored in S3 with millisecond granularity, but only
  `ListObjectsV2` reveals this. Other operations, including `ListObjectVersions`, `HeadObject`, and
  `GetObject`, return the last-modified time with second granularity. If we used object metadata, a
  `HeadObject` call would be required to read object metadata. This would vastly increase the number
  of API calls and make `qfs` very slow. We could store the S3 time in the qfs repository database
  and use cached information if we have it, but maintaining this cache is expensive since we don't
  know the S3 time of a file that we have just written without calling `ListObjectsV2` to get it.
  This vastly increases the number of API calls required when storing files.

The the `.qfs/busy` object is just `.qfs/busy`, but all other repository files, including `.` and
databases, are encoded as above. Regardless of filters, `.qfs/filters` is always included, and
everything else in `.qfs` is excluded. The repository and site databases are copied to and from the
repository explicitly.

The repository database looks like a qfs database with the following exceptions:
* The header is the line `QFS REPO 1`
* The `uid` and `gid` fields are omitted.
* When reading a repository database, the `uid` and `gid` values for every row are set to the
  current user and group ID.

When qfs begins making changes to a repository that cause drift between the actual state and the
database, it creates an object called `.qfs/busy`. When it has successfully updated the repository,
it removes `.qfs/busy`. If a push or pull operation detects the presence of `.qfs/busy`, it requires
the user to reconcile the database first before it does anything.

When doing push or pull operations, the repository filter and the site filter are always both used,
so a file has to be included by both filters to be considered. This means that excluding a
previously included item in a filter does not cause the item to disappear on the next push or pull.
It just causes the item to be untracked. You can use `qfs init-repo --clean-repo` to force excluded
files to be removed from the repository. To clarify:
* If `dir` exists and is included by the filter, `push` will push it to the repository.
* If `dir` is removed but it is still included by the filter, `push` will remove it from the
  repository.
* If `dir` is removed locally and the filter is modified so it is no longer included, `push` will do
  nothing, and the files will stay in the repository.
* `pull` behaves the same way.

## Operations

### Note about diff

Many site operations create diffs. For site operations, all diffs are generated with
`--no-ownerships`, `--no-special`, and `--checks`.

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
* Write the repository location to `.qfs/repo`
* Write the site's name to `.qfs/site`
* If you are recreating a site that previously existed, remove any existing `.qfs/sites/$site/db`
  file. This tells the repository that the site has no contents. Alternatively, if you have some
  subset of the files because you ran `rsync` or `qfs get` or restored from a backup, you can use
  `qfs push-db` to update the repository's copy of the site database.
* Run `qfs pull`. If there is no filter for the site, this will only pull the `.qfs/filters`
  directory. In that case, you can create a local filter and run `qfs pull` again.

Note that bad things will happen if you have two simultaneously existing sites with the same name.
If you need to recreate a previously existing site, such as if you lose a site and want to pull its
files down again, you should remove `.qfs/sites/$site/db` from the repository.

Note that you can also recover a site by restoring the site from some other backup, running `qfs
push-db`, and running `qfs pull`. This could be useful if you have a local backup that is faster to
restore from than pulling everything from the repository. This effectively reverts a site by storing
its current state as the "old" state in the repository, causing the repository state to override
local state. While much less common, you could also revert the repository in a similar fashion: run
`qfs pull -n`, which will pull the repository's copy of its database as `.qfs/db/repo.tmp`. Then you
could move `.qfs/db/repo.tmp` to `.qfs/db/repo` and run `qfs push`.

### Push

`qfs push` reads the most recent local record of the repository's contents and applies any local
changes from that state to the repository, thus synchronizing the repository with local state. It
only sends files that have changed according to its local copy of the state, and it does conflict
detection. This makes it safe to run regardless of any other intervening push or pull operations
from this site or other sites.

Run `qfs push`. This does the following:
* If `.qfs/busy` exists in the repository, stop and tell the user to repair the database with `qfs
  init-repo`.
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
* Store the diff as `.qfs/push`. The presence of this file, in addition to being informational,
  indicates that a push has been done without a pull. You can detect this file on login and use it
  to trigger a reminder to do a `qfs pull`.
* Create a working repository database by loading the repository's copy of its database into memory.
  If the metadata on the repository database matches the local copy, load the local copy.
* Perform conflict checking
  * Check against the working repository database to make sure that, for each `check` statement, the
    file either does not exist or has one of the listed modification times.
  * If conflicts are found, offer to abort or override.
* If `-n` was given, stop
* Otherwise, update the repository:
  * Prompt for confirmation, exiting if not given
  * Create `.qfs/busy` on the repository
  * Apply changes by processing the diff. All changes are made to the repository and also to a
    local, in-memory copy of the repository database.
    * Recursively remove anything marked `rm` from s3
    * For each added or changed file, including metadata changes, upload a new version with
      appropriate metadata.
  * Write the locally updated repository database to `.qfs/db/repo.tmp`
  * Upload `.qfs/db/repo.tmp` to `.qfs/db/repo` with correct metadata
  * Upload `.qfs/db/$site` with correct metadata
  * Move `.qfs/db/repo.tmp` to `.qfs/db/repo` locally
  * Delete `.qfs/busy` from the repository

For an explanation of these behaviors, see [Conflict Detection](#conflict-detection) below.

The handling of the repository database is particularly important. Using our local cache of the repo
database means that we will only send files that we changed locally relative to their state in the
repository since our last pull. This prevents us from reverting changes pushed by someone else in
the event that we do a push without doing a pull first, which is an explicitly supported thing to
do. Using the repository's copy of the database as the basis for the working repository database is
also important since, otherwise, multiple pushes from different sites would cause the repository's
database to drift.

### Pull

`qfs pull` reads the most recent repository copy of the local site's and applies any differences to
the local site, thus synchronizing the local site with the repository. It only receives files that
have changed according to the repository's copy of the state, and it does conflict detection. This
makes it safe to run regardless of any other intervening push or pull operations from this site or
other sites.

Run `qfs pull`. This does the following:
* If `.qfs/busy` exists in the repository, stop and tell the user to repair the database with `qfs
  init-repo`.
* Get the current site from `.qfs/site`
* Download the repository's copy of its own database to `.qfs/db/repo.tmp`
* Read the repository's copy of the current site's database into memory, and diff it against the
  repository's copy of its own database (which we just downloaded) using the repository's copies of
  the global filter and the site's filter. If `--local-filter` was given, use the local filter
  instead of the repository filter. This is useful if you modify the filter to include
  previously-excluded items that are present on the repository and want to pull again to download
  them. For bootstrapping to work:
  * If the repository doesn't have a copy of the site's database, treat it as empty.
  * If the repository doesn't have a copy of the site's filter, check locally. If there is no local
    filter either, then treat the filter as one that excludes everything, which means only the
    `.qfs/filters` directory will be included.
* Perform conflict checking.
* If `-n` was given, stop.
* If there were any conflicts, offer to abort or override; otherwise, get confirmation
* Apply changes by downloading from the repository. Keep the local (in-memory) copy of the
  repository's copy of the site's database in sync so that it is updated with only the changes that
  were pulled.
  * Recursively remove anything marked `rm`
  * For each added or changed file
    * If the old file already has the correct modification time, or if it is a link that already has
      the right target, leave it alone and don't download the remote file.
    * Otherwise, make sure it is writable by temporarily overriding it
      permissions for the duration of the write.
      * Note: in the initial implementation, if the directory is not writable, it will be an error.
        The user can change the permissions and rerun pull, at which point pull will restore the
        correct permissions. It would be possible to explicitly check/add write permissions to the
        parent directory (like rsync does). This could be added if the functionality is important.
  * For each changed or added link, delete the old link.
  * Apply changes to permissions.
* Write the updated repository's site database to `.qfs/db/$site.tmp` and uploaded it to the
  repository as `.qfs/db/$site`. This makes it safe to do multiple pulls on a site without doing any
  intervening pushes.
* Move `.qfs/db/repo.tmp` to `.qfs/db/repo`, which updates our local copy of the repository state.
* Remove `.qfs/push`. We leave `.qfs/pull` and `.qfs/db/$site.tmp` in place for future reference.

### Working with individual files

Using the `qfs list-versions` and `qfs get` commands, it is possible to view and retrieve old
versions of files. By using bucket versioning with suitable life cycle rules, we can have a rich
version history for every file much as would be the case with something like Dropbox.

There is no facility for manually pushing a single file to a repository. This would be hard to do
while keeping databases in sync and avoiding drift. If things need to be restored, fix the files
locally and then run a push.

### Migration From S3 Sync

If you have a collection of files that you have been backing up to S3 with `aws s3 sync`, you can
use the `--migrate` option to `init-repo` to convert it to a `qfs` repository. You may want to
temporarily suspend S3 versioning for this option to avoid creating an unnecessary copy of
everything in the bucket. `aws s3 sync` copies a local file into the S3 bucket if the local file's
modification time is more recent than the last-modified time in S3. As such, it works fine if you
only sync from one location and never change a file in a manner that sets its modification time to a
time earlier than the last time you ran `aws s3 sync`. For example, if you restore a file from a
backup and its time is in the past, `aws s3 sync` would not notice that file as having changed.

In migrate mode, `init-repo` scans the existing contents of the repository area, and if it finds a
file whose key matches a local regular file and whose last-modified time is newer than the local
file's modification time, it will call `CopyObject` on the key to copy it to the name `qfs` would
use (with the modification time and permissions) followed by a `DeleteObject` on the original key.
This prevents you from having to re-upload the file. A typical workflow would be
* Suspend versioning on the S3 bucket.
* Run `qfs init-repo --migrate`, which will move any existing keys that `aws s3 sync` would consider
  current so that `qfs` will also consider them current.
* Re-enable versioning on the S3 bucket.
* Set up the repository and site filters.
* Run `qfs push`. This will push everything that wasn't migrated, including directories, links, and
  qfs filters.
* Run `qfs init-repo --clean-repo`. This will remove any stray files including files that `aws s3
  sync` would have considered to be out-of-date. This has to be done after the initial push so the
  repository filter is there.

After this, you can use `qfs` instead of `aws s3 sync` to keep the area backed up while efficiently
maintaining file metadata.

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
switch to any other site without having to know in advance which one it is going to be.

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
* `qfs pull`

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

The `push` operation modifies an in-memory copy of the repository's database and pushes that back,
which means that we can keep the repository database up-to-date with the changes we made without
having to rescan the repository.

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
* The `sync` command more or less replaces `qsync_to_rsync`. Implementing the equivalent of
  `qsync_to_rsync` with `qfs` is hard because of supporting regular expressions in filters and
  combining multiple filters. Specifically, the logic for creating rsync include rules such that
  something is included only when matched by all given filters is hard.
