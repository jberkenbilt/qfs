package database

import (
	"github.com/jberkenbilt/qfs/fileinfo"
	"golang.org/x/exp/maps"
	"sort"
)

type Memory map[string]*fileinfo.FileInfo

func (m Memory) ForEach(fn func(*fileinfo.FileInfo) error) error {
	keys := maps.Keys(m)
	sort.Strings(keys)
	for _, k := range keys {
		if err := fn(m[k]); err != nil {
			return err
		}
	}
	return nil
}

func (m Memory) Close() error {
	return nil
}

func (m Memory) Load(p fileinfo.Provider) error {
	return p.ForEach(func(info *fileinfo.FileInfo) error {
		m[info.Path] = info
		return nil
	})
}
