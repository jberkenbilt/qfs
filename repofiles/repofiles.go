package repofiles

const (
	RepoSite   = "repo"
	Top        = ".qfs"
	Filters    = ".qfs/filters"
	RepoConfig = ".qfs/repo/config"
	Pending    = ".qfs/pending"
	Busy       = ".qfs/busy"
	Site       = ".qfs/site"
)

func PendingDir(site string) string {
	return Pending + "/" + site
}

func PendingDb(site string) string {
	return PendingDir(site) + "/db"
}

func PendingDiff(site string) string {
	return PendingDir(site) + "/diff"
}

func SiteDb(site string) string {
	if site == RepoSite {
		return ".qfs/repo/db"
	} else {
		return ".qfs/sites/" + site + "/db"
	}
}

func RepoDb() string {
	return SiteDb(RepoSite)
}

func SiteFilter(site string) string {
	return ".qfs/filters/" + site
}
