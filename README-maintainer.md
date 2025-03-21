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

# Release

Update version number in qfs/qfs.go

```shell
version=x.y.z
git tag -s v$version @ -m"qfs $version"
git push qfs v$version
```

# Cobra/layout

The `start-minio` command is in `start-minio`. It has its own main. The `cmd` directory is the `cmd`
package used by [cobra](https://github.com/spf13/cobra/).
