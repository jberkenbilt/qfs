# Maintenance Reminders

```
./build_all
./test_all
```

Periodically

```
go get -u ./...
go mod tidy
```

# QfsMinVersion

The file `repo.go` defines `QfsMinVersion` to the minimum version of qfs that's safe to use for a repository. If a specific version of qfs makes a change to the way keys are stored in the repository that make older versions of qfs unsafe to run, it should bump that to its own version.

# Release

Update version number in qfs/qfs.go

```shell
version=x.y.z
git tag -s v$version @ -m"qfs $version"
git push qfs v$version
```

# Cobra/layout

The `start-test-s3` command is in `start-test-s3`. It has its own main. The `cmd` directory is the `cmd` package used by [cobra](https://github.com/spf13/cobra/).
