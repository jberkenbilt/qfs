// Package traverse traverses a file system in multiple concurrent
// goroutines. It is faster than traversing in a single thread.
package traverse

import (
	"container/list"
	"context"
	"fmt"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/queue"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type FileType rune

const (
	TypeFile      FileType = 'f'
	TypeDirectory FileType = 'd'
	TypeLink      FileType = 'l'
	TypeCharDev   FileType = 'c'
	TypeBlockDev  FileType = 'b'
	TypePipe      FileType = 'p'
	TypeSocket    FileType = 's'
	TypeUnknown   FileType = 'x'
)

type FileInfo struct {
	Path        string
	FileType    FileType
	ModTime     time.Time
	Size        int64
	Permissions uint16
	Uid         uint32
	Gid         uint32
	Target      string
	Major       uint32
	Minor       uint32
	Children    []*FileInfo
	Included    bool
}

type traverser struct {
	root       string
	rootDev    uint64
	filters    []*filter.Filter
	xDev       bool
	cleanup    bool
	errChan    chan error
	notifyChan chan string
	workChan   chan *FileInfo
	pending    atomic.Int64
	zero       chan struct{}
	q          *queue.Queue[*FileInfo]
}

func (tr *traverser) getFileInfo(node *FileInfo) error {
	path := filepath.Join(tr.root, node.Path)
	included, group := filter.IsIncluded(node.Path, tr.filters...)
	node.Included = included
	lst, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("lstat %s: %w", path, err)
	}
	node.ModTime = lst.ModTime()
	mode := lst.Mode()
	node.Permissions = uint16(mode.Perm())
	st, ok := lst.Sys().(*syscall.Stat_t)
	if ok && st != nil {
		node.Uid = st.Uid
		node.Gid = st.Gid
		node.Major = uint32(st.Rdev >> 8 & 0xfff)
		node.Minor = uint32(st.Rdev&0xff | (st.Rdev >> 12 & 0xfff00))
	}
	modeType := mode.Type()
	switch {
	case mode.IsRegular():
		node.FileType = TypeFile
		node.Size = lst.Size()
		if group == filter.Junk && tr.cleanup {
			node.Included = false
			if err = os.Remove(filepath.Join(tr.root, node.Path)); err != nil {
				return fmt.Errorf("remove junk %s: %w", path, err)
			} else {
				tr.notifyChan <- fmt.Sprintf("removing: %s", node.Path)
			}
		}
	case modeType&os.ModeDevice != 0:
		if modeType&os.ModeCharDevice != 0 {
			node.FileType = TypeCharDev
		} else {
			node.FileType = TypeBlockDev
		}
	case modeType&os.ModeSocket != 0:
		node.FileType = TypeSocket
	case modeType&os.ModeNamedPipe != 0:
		node.FileType = TypePipe
	case modeType&os.ModeSymlink != 0:
		node.FileType = TypeLink
		target, err := os.Readlink(path)
		if err != nil {
			return fmt.Errorf("readlink %s: %w", path, err)
		}
		node.Target = target
	case mode.IsDir():
		if !included && group == filter.Prune {
			// Don't traverse into pruned directories
			break
		}
		if tr.xDev && st != nil && tr.rootDev != st.Dev {
			// This is on a different device. Exclude and don't traverse into it.
			node.Included = false
			break
		}
		node.FileType = TypeDirectory
		entries, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("read dir %s: %w", path, err)
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})
		for _, e := range entries {
			node.Children = append(node.Children, &FileInfo{Path: filepath.Join(node.Path, e.Name())})
		}
	default:
		// Not possible to exercise in test suite
		node.FileType = TypeUnknown
	}
	return nil
}

func (tr *traverser) worker() {
	for node := range tr.workChan {
		if err := tr.getFileInfo(node); err != nil {
			tr.errChan <- err
		}
		tr.q.Push(node.Children...)
		if tr.pending.Add(int64(len(node.Children))-1) == 0 {
			select {
			case tr.zero <- struct{}{}:
			default:
			}
		}
	}
}

func (tr *traverser) getWork() []*FileInfo {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := make(chan []*FileInfo, 1)
	go func() {
		c <- tr.q.GetAll(ctx)
	}()
	select {
	case result := <-c:
		return result
	case <-tr.zero:
		return nil
	}
}

func (tr *traverser) traverse(node *FileInfo) {
	toDo := []*FileInfo{node}
	tr.pending.Add(1)
	for toDo != nil {
		for _, r := range toDo {
			tr.workChan <- r
		}
		toDo = tr.getWork()
	}
}

// Traverse traverses a file system starting from to given path and returns a
// FileInfo, which represents a tree of the file system. Call the Flatten method
// on the resulting FileInfo to walk through all the items included by the
// filters. Note that a specific FileInfo has an Included field indicating
// whether the item is included. Pruned directories' children are not included,
// but regular excluded directories are present in case they have included
// children.
func Traverse(
	root string,
	filters []*filter.Filter,
	xDev bool,
	cleanup bool,
	notifyFn func(string),
	errFn func(error),
) (*FileInfo, error) {
	numWorkers := 5 * runtime.NumCPU()
	tr := &traverser{
		root:       root,
		filters:    filters,
		xDev:       xDev,
		cleanup:    cleanup,
		errChan:    make(chan error, numWorkers),
		notifyChan: make(chan string, numWorkers),
		workChan:   make(chan *FileInfo, numWorkers),
		zero:       make(chan struct{}, 1),
		q:          queue.New[*FileInfo](),
	}

	lst, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("lstat %s: %w", root, err)
	}
	st, ok := lst.Sys().(*syscall.Stat_t)
	if ok && st != nil {
		tr.rootDev = st.Dev
	}

	var workerWait sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workerWait.Add(1)
		go func() {
			defer workerWait.Done()
			tr.worker()
		}()
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range tr.errChan {
			errFn(e)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range tr.notifyChan {
			notifyFn(msg)
		}
	}()

	tree := &FileInfo{Path: "."}
	tr.traverse(tree)
	close(tr.workChan)
	workerWait.Wait()
	close(tr.errChan)
	close(tr.notifyChan)
	wg.Wait()
	return tree, nil
}

// Flatten traverses the FileInfo and calls the function for each item in lexical
// order. If the function returns an error, traversal is stopped, and the error
// is returned. Items that were excluded by the filter are skipped.
func (f *FileInfo) Flatten(fn func(f *FileInfo) error) error {
	q := list.New()
	q.PushFront(f)
	for q.Len() > 0 {
		front := q.Front()
		q.Remove(front)
		cur := front.Value.(*FileInfo)
		if cur.Included {
			if err := fn(cur); err != nil {
				return err
			}
		}
		n := len(cur.Children)
		for i := range cur.Children {
			q.PushFront(cur.Children[n-i-1])
		}
	}
	return nil
}
