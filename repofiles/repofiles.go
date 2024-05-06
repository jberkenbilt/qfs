package repofiles

const (
	RepoSite   = "repo"
	Top        = ".qfs"
	Filters    = ".qfs/filters"
	RepoConfig = ".qfs/repo"
	Site       = ".qfs/site"
	Busy       = ".qfs/busy"
	Push       = ".qfs/push"
	Pull       = ".qfs/pull"
)

func SiteDb(site string) string {
	return ".qfs/db/" + site
}

func RepoDb() string {
	return SiteDb(RepoSite)
}

func TempSiteDb(site string) string {
	return ".qfs/db/" + site + ".tmp"
}

func TempRepoDb() string {
	return TempSiteDb(RepoSite)
}

func SiteFilter(site string) string {
	return ".qfs/filters/" + site
}
