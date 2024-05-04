package repofiles

const (
	RepoSite   = "repo"
	TopPrefix  = ".qfs/"
	RepoConfig = ".qfs/repo/config"
	Pending    = ".qfs/pending"
	Busy       = ".qfs/busy"
	Site       = ".qfs/site"
)

func PendingDb(site string) string {
	return ".qfs/pending/" + site + "/db"
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
