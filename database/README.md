# QFS Database Format

The `database` package can read QFS v1 and QSYNC v3 database formats. The formats are similar with
some differences.

## Common Features

* The first line is a header. Subsequently, the database is record-based.
* Each record starts with a length indicator of the form `length[/same]`, whre `length` is the
  number of bytes after the length indicator for the rest of the record, and `same` is the number of
  initial bytes from the previous record that should be prepended. This means that if you had
  something like
  ```
  10:abcdefghij
  4/6:qrst
  ```
  it would indicate that the first row was `abcdefghij` and the second row was `abcdefqrst`
* A record consists of fields separated by the null character and terminated by a newline

## Differences

* qsync surrounds each record by null characters. qfs omits the first and last null.
* The fields have slightly different meanings:
  * qsync fields: name mtime size mode uid gid linkCount special
  * qfs fields: name fileType mtime size mode uid gid special
  * qfs does not track link counts at all
  * qsync stores the Unix mode from stat; qfs stores a single-character file type and the
    permissions section of the mode
  * There are differences meaning of `special`
    * directories: qsync: number of entries; qfs: empty
    * block devices: qsync: b,major,minor; qfs: major,minor
    * character devices: qsync: ,major,minor; qfs: major,minor
