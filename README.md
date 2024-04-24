# QFS

`qfs` is a system for synchronizing files across multiple sites.

XXX Rework this document so it is useful when read from beginning to end by someone who has no
prior familiarity so this evolves to the project documentation.

# Filters

qfs will use the qsync filter format with the addition of

```
:junk:(junk-regexp)
:re:(pattern-rule)
```

and with the difference that the argument to `:read:` is interpreted as relative to the filter.
Paths are are always interpreted as relative to the working directory in which `qfs` was started.
The rationale for `:read:` using paths relative to the filter is that it allows the `filters`
"directory" to be downloaded from the qfs repository and read locally, which allows a `pull`
operation to use the latest filters from the repository. This is explained in [Sites](#sites) below.
Also, pruned directories are entirely omitted from the database. In qsync, they appeared with a
"special" indicating an entry count of `-1`. `qfs` skips these when reading a qsync database.

This means
* Lines starting with `*/base` will be treated as `base` rules
* Lines of the form `*.x` will be treated as `pattern` rules and will become `\.x$`
* qsync v1 will just treat the `:junk:` and `:re:` rules as files, effectively ignoring them

Filter inclusion algorithm:
* When there are multiple filters, a path must be included by all filters to be included.
* If a path or any parent directory matches a `prune` or `junk` directive, the file is excluded.
* Otherwise, if a path or any parent matches an `include` directive, the file is included.
* Otherwise, if a path or any parent matches an `excluded` directive, the file is excluded.
* Otherwise, the file's status is the default include status.
  * If no default include status was explicitly set, it is `false` if any include rules were
    specified and `false` otherwise.

That means that, with this filter:
```
:prune:
a/prune
:include:
*/include
:exclude:
a/exclude
.
```
* `a/prune/x` is excluded because of `a/prune`
* `a/prune/include/x` is excluded because of `a/prune` since prune is stronger than include
* `a/exclude/x` is excluded because of `a/exclude`
* `a/include/x` is included because of `*/exclude` since include is stronger than exclude
* `a/x` is excluded because of the default exclude rule (`.`)

# CLI

Syntax is similar to but not identical to qsync.

* All options can be `-opt` or `--opt`
* Filtering options (common):
  * One or more filters may be given with `-filter` or `-filter-prune`
  * Explicit rules create a dynamic filter
  * For a file to be included, it must be included by all filters including the dynamic filter and
    any file filters
  * These arguments are allowed wherever _filter options_ appears
    * `-filter f` -- specify a filter file
    * `-filter-prune f` -- specify a filter file from which only prune and junk are read
    * `-include x` -- add an include directive to the dynamic filter
    * `-exclude x` -- add an exclude directive to the dynamic filter
    * `-prune x` -- add a prune directive to the dynamic filter
    * `-junk x` -- add a junk directive to the dynamic filter
    * Options that only apply when scanning a file system (not a database):
      * `-cleanup` -- remove any plain file that is classified as junk by any of the filters
      * `-xdev` -- don't cross device boundaries
* `qfs subcommand ....`
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
  * Positional: twice: directory or database
  * _filter options_
  * `-no-dir-times` -- ignore directory modification time changes
  * `-no-ownerships` -- ignore uid/gid changes
  * `-check` -- output conflict checking data
    files
* `push`
  * See [Sites](#sites)
  * `-cleanup` -- cleans junk files
  * `-n` -- perform conflict checking but make no changes
  * `-local tarfile` -- save a tar file with changes instead of pushing; useful for backups if offline
  * `-save-site site tarfile` -- save a tar file with changes for site; see [Sites](#sites)
* `pull`
  * See [Sites](#sites)
  * `-n` -- perform conflict checking but make no changes
  * `-site-files tarfile` -- use the specified tar file as a source of files that would be pulled.
    This is the file created by `push -save-site`.
* `list file` -- list all known versions of a file in the repository
  * For best results, use bucket versioning
* `get file` -- copy a file from the repository
  * `-version v` -- copy the specified version; useful for restoring an old version of a file
  * `-out path` -- write the output to the given location
  * `-replace` -- replace the file with the retrieved version
  * One of `-out` or `-replace` must be given.
* `rsync` -- create equivalent (as much as possible) rsync rules files (replaces `qsync_to_rsync`)
  * _filter options_

# Diff Format

This is different from qsync v1. Each line is one of

* `check [mtime] ...] - filename` -- Verify that the file has one of the listed modification time
  values or doesn't exist. If none given, verify that the file does not exist. Only if `-checks` is
  specified.
* `typechange filename` -- the type of a file changed; `rm` and `mkdir/add` directives will
  also appear
* `change filename` -- the file type is the same but something the content or special changed
* `rm file` -- a file, link, directory, or special has disappeared
* `mkdir dir` -- directory was added
* `add filename` -- file, link, or special was added
* `chmod nnnn filename` -- mode change
* `chown [nnnn]:[nnnn] filename` -- uid/gid change. Omitted with `-no-ownerships`.
* `mtime dir` -- a directory's modification time changed. Omitted with `-no-dir-times`.

# Database

Continue to use a proprietary database format for simplicity. qfs should be able to read qsync v3
and qfs v1 but only write qfs v1.

```
QFS 1
len[/same] @ path @ ftype @ mtime @ size @ mode @ uid @ gid @ special \n
```
Changes:
* no delimiter at beginning or end of line
* path is not prepended by `./`; root is still `.`
* ftype is one of
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
* Site -- a specific instance of a subset of the file collection at a specific location.
  machine. Each site is defined by a filter that defines a subset of the entire collection of files.
  When working with a site, the global filter and site's filter are both in effect.
* Repository -- an area in S3 that contains all the files across all sites. Every site exchanges all
  files in the intersection of the site filter and the global filter to the repository.

A file collection is defined by containing `.qfs` directory, which has the following layout:
* `repo/`
  * `config` -- s3 bucket and prefix with other optional information (e.g. endpoint)
  * `db` -- local cache of the repository's database
* `filters/`
  * `repo` -- a global filter that defines the file collection as a subset of the file collection
  * `$site` -- top-level filter for a site; any `:read`: will be resolved relative to the existing
    file
  * Other files may contain filter fragments that are included. They can be anywhere under the
    `filters` directory as long as they don't conflict with the name of a site. You can't have a
    site named `repo`.
* `site` -- a single-line file containing the name of the current site
* `sites/`
  * `$site/` -- directory for each site
    * `db` -- the most recent known database for a specific site

Unlike qsync, qfs does not support syncing from one site to another. Everything goes through the
repository. It is possible to create a tar file that is akin to a qsync sync package when pushing
and to copy the file to another site to avoid extra traffic to s3. This can also be used for one-way
syncs to sites that never have Internet connectivity. These workflows are discussed below.

## Repository Details

A repository resides in an S3 bucket. There is no locking or concurrency protection. This is
intended to be used by one person, one site at a time. It is not a content management or version
control system. The repository stores information about the files in S3 object metadata. It also
stores a repository database that allows qfs to obtain information about repository files from a
combination of the output of `list-objects-v2` and the database. There is minimal locking in the
form of a `busy` file that can be used to detect when the repository is in an inconsistent state. If
a push operation is interrupted, the repository state can be repaired by regenerating the database
from object metadata.

The repository contains a key for each file in the collection including the contents of the `.qfs`
directory with the following specifics:
* `.qfs/site` is never copied to a repository since this differs across sites
* `.qfs/busy` is never created locally
* `.qfs/pending` is never synchronized but may occur in a tar file
* Directory keys end with `/` and are zero-length
* Symbolic link keys are empty
* Only files, links, and directories are represented
* All keys have object metadata containing a `qfs` key whose value is `modtime mode` for files and
  directories, and `modtime ->target` for symbolic links, where `modtime` is a
  millisecond-granularity timestamp, and `mode` is a four-digital octal mode.

The repository database looks like a qfs database with the following exceptions:
* The header is the line `QFS REPO 1`
* The `uid` and `gid` fields are omitted. In their place, the last modified time of the object in S3
  is stored as a millisecond-granularity timestamp.
* When reading a repository database, the `uid` and `gid` values for every row are set to the
  current user and group ID.

The repository database is stored under `.qfs/repo/db`. If necessary, it is possible to entirely
reconstruct the database by recursively listing the prefix and reading the `qfs` metadata key. To
incrementally build or validate the database, retrieve the database, and do a recursive listing. If
any key's modification time matches what is stored in the database, assume it is up-to-date (since
it is not possible to modify an S3 object without changing its modification time). This makes it
easy to reconcile a database with the repository in the event that it should become damaged without
having to look at all the keys again.

When qfs updates the repository, it incrementally updates a local copy of the database and pushes it
back up.

When qfs begins making changes to a repository that cause drift between the actual state and the
database, it creates a key called `.qfs/busy`. When it has successfully updated the database's copy
of `.qfs/db`, it removes `.qfs/busy`. If a push or pull operation detects the presence of
`.qfs/busy`, it should start by reconciling the database.

Many operations operate on behalf of one or more sites and the repository. The following filtering
rules are always implied:
* Always use the global filter.
* Use the filters for each site. For push or pull, that means the main site's filter. For
  `-save-site`, also use the other site's filter.
* Always include the `.qfs` directory with the additional rules:
  * Prune the `.qfs/pending` directory
  * Exclude `.qfs/site`, `.qfs/busy`, and `.qfs/repo/db`
  * Always include a qfs database in itself, but still write/copy/store the file. For example, when
    writing to a repository, create `.qfs/repo/db` in the repository with the correct `qfs`
    metadata, but don't information about a partially written `.qfs/repo/df` file in `.qfs/repo/db`
    itself. The active db file must always be handled as a special case.

## Operations

### Note about diff

Many site operations create diffs. For site operations, all diffs should be generated with
`-no-ownerships`, `-no-dir-times`, `-no-special`, and `-checks`.

### Initialize Repository

To initialize a new repository, create the `.qfs/repo/config` file locally, and run `qfs init-repo`.
This does the following:
* Ensures that nothing exists under the specified bucket/prefix
* Copies `.qfs/repo/config` to the repository with the appropriate `qfs` metadata
* Generates `.qfs/repo/db` locally and uploads it to the repository.

After this, it is possible to add sites and start pushing and pulling. You will need to create
`.qfs/filters/repo` before the first push.

As with regular site databases, `.qfs/repo/db` will not include itself, but the `qfs` metadata on
`.qfs/repo/db` will reflect a modification time and mode that matches the local copy when it was
generated. As such, when checking whether a local `.qfs/repo/db` is up-to-date, it will always be
necessary to necessary to use `head-object`. This is analogous to a site's local database, which is
also omitted from itself.

### Add Site

To set up a new site, do the following on the site:
* Locally create `.qfs/repo/config`
* Write the site's name to `.qfs/site`
* Run `qfs init-site`, which does the following:
  * If `.qfs/sites/$site/db` already exists on the repository, offer to remove it. If it is not
    removed, fail.
  * Pull the `.qfs` directory from the repository.
  * If `.qfs/sites/$site/filter` already exists on the repository, automatically run `qfs pull`. If
    not, you can now create `.qfs/sites/$site/filter` and run `qfs pull`.

Note that bad things will happen if you have two simultaneously existing sites with the same name.
If you need to recreate a previously existing site, such as if you lose a site and want to pull its
files down again, you should remove `.qfs/sites/$site/db` from the repository.

### Repair Database

When a repository is initialized, it gets an empty database. The database is kept updated
incrementally by each push operation. If the database is damaged, it can be rebuilt by running `qfs
rebuild-repo-db`. This does the following:
* Recursively list the repository contents
  * For each object that has a `qfs` key, create a local database entry
  * Always exclude `.qfs/repo/db`
* Write `.qfs/repo/db`
* Remove `.qfs/busy`

### Push

Run `qfs push`. This does the following:
* If `.qfs/busy` exists in the repository, stop and tell the user to regenerate the repair the
  database.
* Regenerate the local database as `.qfs/sites/$site/db`, applying only prune (and junk) directives
  from the repository filter and site filters, and automatically including `.qfs` as described above
  (except `qfs/site`, `.qfs/busy`, `.qfs/pending`, `.qfs/repo/db`, and `.qfs/sites/$site/db`. Using
  only prune entries makes the site database more useful and also improves the behavior of when
  filters are updated after a site has been in use for a while.
* Diff the local site's database with the _local copy_ of the repository database. Use the site's
  filter and the repository filter with the automatic settings for the `.qfs` directory as described
  above. Using the local copy of the repository's database makes it safe to run multiple push
  operations in succession without doing an intervening pull, enabling conflict detection to work
  properly.
* Store the diff as `.qfs/pending/repo/diff`. In addition to be informational, this may be included
  in a local tar file if requested, and it also indicates that a push has been done without a pull.
* Download the repository's copy of the database into a temporary location.
* Perform conflict checking
  * Check against the repository's copy of the database to make sure that, for each `check`
    statement, the file either does not exist or has one of the listed modification times.
  * If conflicts, offer to abort or override.
* If `-n` was given, stop
* If `-local` was given
  * Create a tar file whose first entry is the contents of the diff stored as `.qfs-diff` and which
    otherwise contains the files that would be pushed
* Otherwise, update the repository:
  * Create `.qfs/busy` on the repository
  * Apply changes by processing the diff. For each change, also update an in-memory copy of the
    repository copy of the database.
    * Recursively remove anything marked `rm` from s3 and also remove its entry from the in-memory
      database.
    * For each added or changed file, upload a new version with appropriate metadata. Once uploaded,
      do an immediate head-object (S3 has strong read-after-write consistency) so the in-memory
      database update can include the object's modification time.
  * Upload the updated repository database (its copy) with correct metadata
  * Delete `.qfs/busy`
  * If `-save-site` was given
    * Diff the site database with the local copy of the other site's database, applying the repo
      filter and both site filters along with the `.qfs` directories, excluding the other site's
      database. Save to a temporary location.
    * Write a tar file whose first entry is the contents of the diff as `.qfs/pending/$site/diff`
      and which contains any files that differ between the current state and the local record of the
      other site's state. Note that the file does not exist locally. It is just in the tar file.
      This serves the dual purpose of allowing the recipient to make sure it is the intended target
      and of causing the file to go in a sensible place if the tar file is extracted manually.
    * This can be applied locally on the other site prior to the other site doing a pull and may reduce
      S3 traffic.

Notes:
* The `-save-site` only applies when pushing to a repository.
* Sites always use their locally cached copies of remote state.

For an explanation of these behaviors, see [Conflict Detection](#conflict-detection) below.

The handling of the repository database is particularly important. Using our local cache of the repo
database means that we will only send files that we changed locally relative to their state in the
repository since our last pull. This prevents us from reverting changes pushed by someone else in
the event that we do a push without doing a pull first. This is an explicitly supported thing to do.
Using the repository's copy of the database for pushing updates after we apply changes is important
since, otherwise, multiple pushes from different sites would cause the repository's database to
drift.

### Pull

Run `qfs pull`. This does the following:
* If `.qfs/busy` exists in the repository, stop and tell the user to regenerate the repair the
  database.
* Get the current site from `.qfs/site`
* In a temporary work area:
  * If there is a site tar make sure the first entry is `.qfs/pending/$site/diff`. If not, abort.
  * From the repo, pull `.qfs/repo/db`, `.qfs/sites/$site/db`, and `.qfs/filters`. Bootstrapping
    note: if the site database doesn't exist in the repository, it means the site has never been
    pushed. In this case, use an empty database as the site database. In this case, it is likely
    that the site filter may also not exist. In this case, if there is a local site filter, use it.
    If not, abort. The rationale for this behavior is that it makes it possible to bootstrap a new
    site by creating the filter file on the new site after running `qfs init-site`.
  * Generate a diff from the repository's copy of its database and its copy of the site database,
    applying the site's copy of the filters.
* Perform conflict checking. Using the diff files in the previous step, check the local file system
  for conflicts. Each file must either not exist or have one of the listed modification times.
* If conflicts are found, allow the user to override. If the user doesn't override, stop.
* If `-n` was given, stop.
* Apply changes by processing the diff.
  * Using the diff from the site tar file (if any):
    * Recursively remove anything marked `rm`
    * For each added or changed file
      * If the old file already has the correct modification time, or if it is a link that already
        has the right target, leave it alone and record that it doesn't have to be extracted.
      * Otherwise, make sure it is writable by temporarily overriding its and/or its parents'
        permissions for the duration of the write. The parent directory has to be writable by the
        user in all cases, and for files, the file also has to be user-writable.
    * For each changed or added link, delete the old link.
    * Extract the site tar file, keeping track of end directory permissions, and skipping any files or
      links that we determined already to be up to date.
  * Using the diff from the repository, repeat the above process. A key point here is that, if a
    site tar was used, many (or even all) of the differences will already have been applied. There
    may be changes pushed from other sites (or even the same remote site) that also need to be
    applied, though pushing from one site with a site tar and then pushing from the same site
    without a site-tar is likely to result in false positives with conflict checking.
  * From bottom to top, process any chmod operations. For directories, in priority order, use the
    value from an explicitly chmod, the value in the tar header, or the original permissions if we
    made a change. If we didn't change directory permissions and the directory was not mentioned
    anywhere, it never has to be touched.
* Replace the local site's `.qfs/repo/db` with the copy that was download from the repository.
* Recursively remove `.qfs/repo/pending`

### Work with individual files

Using the `qfs list` and `qfs get` commands, it is possible to view and retrieve old versions of
files. By using bucket versioning with suitable life cycle rules, we can have a rich version history
for every file much as would be the case with something like Dropbox.

## Conflict Detection

An important part of qfs sites is the ability to detect conflicts. This is similar to conflicts that
appear in a version control system, and is perhaps the most powerful aspect of qfs. The key to
implementing conflict detection is that all site-related diff operations compare the current site's
state with the current site's copy of the remote site's state rather than using the actual remote
site's state. The intention is that it should be safe to interleave pushes and pulls from multiple
sites, including doing multiple pushes without an intervening pull, and always have working conflict
detection without the risk of one site reverting another site's change.

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

Note that the presence of the `.qfs/pending/repo/diff` file indicates that you have done a push
without doing a pull. If you are starting a work session on that site as your primary, you should
ways do a pull to have the latest files. Under steady state, all but one site would have this file.
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

The use of local cached state makes it possible for everything to work seamlessly when different
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
mind that `diff` consider a file to not be a conflict if the destination file is either not present
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

# Other Notes

Use [Minio](https://min.io) for testing or to create a local S3 API-compatible storage area for a
local repository.
