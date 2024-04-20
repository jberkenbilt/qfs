// Package traverse traverses a file system in multiple concurrent
// goroutines. It is faster than traversing in a single thread.
package traverse

import (
	"container/list"
	"context"
	"fmt"
	"github.com/jberkenbilt/qfs/fileinfo"
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

type Result struct {
	tree *treeNode
}

type treeNode struct {
	path        string
	fileType    fileinfo.FileType
	modTime     time.Time
	size        int64
	permissions uint16
	uid         uint32
	gid         uint32
	target      string
	major       uint32
	minor       uint32
	children    []*treeNode
	included    bool
}

type traverser struct {
	root       string
	rootDev    uint64
	filters    []*filter.Filter
	xDev       bool
	cleanup    bool
	errChan    chan error
	notifyChan chan string
	workChan   chan *treeNode
	pending    atomic.Int64
	zero       chan struct{}
	q          *queue.Queue[*treeNode]
}

func (n *treeNode) toFileInfo() *fileinfo.FileInfo {
	var special string
	if n.fileType == fileinfo.TypeLink {
		special = n.target
	} else if n.fileType == fileinfo.TypeBlockDev || n.fileType == fileinfo.TypeCharDev {
		special = fmt.Sprintf("%d,%d", n.major, n.minor)
	}
	return &fileinfo.FileInfo{
		Path:        n.path,
		FileType:    n.fileType,
		ModTime:     n.modTime,
		Size:        n.size,
		Permissions: n.permissions,
		Uid:         n.uid,
		Gid:         n.gid,
		Special:     special,
	}
}

func (tr *traverser) getFileInfo(node *treeNode) error {
	path := filepath.Join(tr.root, node.path)
	included, group := filter.IsIncluded(node.path, tr.filters...)
	node.included = included
	node.fileType = fileinfo.TypeUnknown
	lst, err := os.Lstat(path)
	if err != nil {
		// TEST: CAN'T COVER. There is way to intentionally create a file that we can see
		// in its directory but can't lstat, so this is not exercised.
		return fmt.Errorf("lstat %s: %w", path, err)
	}
	node.modTime = lst.ModTime()
	mode := lst.Mode()
	node.permissions = uint16(mode.Perm())
	st, ok := lst.Sys().(*syscall.Stat_t)
	if ok && st != nil {
		node.uid = st.Uid
		node.gid = st.Gid
		node.major = uint32(st.Rdev >> 8 & 0xfff)
		node.minor = uint32(st.Rdev&0xff | (st.Rdev >> 12 & 0xfff00))
	}
	modeType := mode.Type()
	switch {
	case mode.IsRegular():
		node.fileType = fileinfo.TypeFile
		node.size = lst.Size()
		if group == filter.Junk && tr.cleanup {
			node.included = false
			if err = os.Remove(filepath.Join(tr.root, node.path)); err != nil {
				return fmt.Errorf("remove junk %s: %w", path, err)
			} else {
				tr.notifyChan <- fmt.Sprintf("removing: %s", node.path)
			}
		}
	case modeType&os.ModeDevice != 0:
		if modeType&os.ModeCharDevice != 0 {
			node.fileType = fileinfo.TypeCharDev
		} else {
			node.fileType = fileinfo.TypeBlockDev
		}
	case modeType&os.ModeSocket != 0:
		node.fileType = fileinfo.TypeSocket
	case modeType&os.ModeNamedPipe != 0:
		node.fileType = fileinfo.TypePipe
	case modeType&os.ModeSymlink != 0:
		node.fileType = fileinfo.TypeLink
		target, err := os.Readlink(path)
		if err != nil {
			// TEST: CAN'T COVER. We have no way to create a link we can lstat but for which
			// readlink fails.
			return fmt.Errorf("readlink %s: %w", path, err)
		}
		node.target = target
	case mode.IsDir():
		if !included && group == filter.Prune {
			// Don't traverse into pruned directories
			break
		}
		if tr.xDev && st != nil && tr.rootDev != st.Dev {
			// TEST: CAN'T COVER. This is on a different device. Exclude and don't traverse
			// into it. This is not exercised in the test suite as it is difficult without
			// root/admin privileges to construct a scenario where crossing device boundaries
			// will happen.
			node.included = false
			break
		}
		node.fileType = fileinfo.TypeDirectory
		entries, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("read dir %s: %w", path, err)
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})
		for _, e := range entries {
			node.children = append(node.children, &treeNode{path: filepath.Join(node.path, e.Name())})
		}
	}
	return nil
}

func (tr *traverser) worker() {
	for node := range tr.workChan {
		if err := tr.getFileInfo(node); err != nil {
			tr.errChan <- err
		}
		tr.q.Push(node.children...)
		if tr.pending.Add(int64(len(node.children))-1) == 0 {
			select {
			case tr.zero <- struct{}{}:
			default:
			}
		}
	}
}

func (tr *traverser) getWork() []*treeNode {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := make(chan []*treeNode, 1)
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

func (tr *traverser) traverse(node *treeNode) {
	toDo := []*treeNode{node}
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
) (*Result, error) {
	numWorkers := 5 * runtime.NumCPU()
	tr := &traverser{
		root:       root,
		filters:    filters,
		xDev:       xDev,
		cleanup:    cleanup,
		errChan:    make(chan error, numWorkers),
		notifyChan: make(chan string, numWorkers),
		workChan:   make(chan *treeNode, numWorkers),
		zero:       make(chan struct{}, 1),
		q:          queue.New[*treeNode](),
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

	tree := &treeNode{path: "."}
	tr.traverse(tree)
	close(tr.workChan)
	workerWait.Wait()
	close(tr.errChan)
	close(tr.notifyChan)
	wg.Wait()
	return &Result{
		tree: tree,
	}, nil
}

// ForEach traverses the traversal result and calls the function for each item in
// lexical order. If the function returns an error, traversal is stopped, and the
// error is returned. This implements the fileinfo.FileProvider interface.
func (r *Result) ForEach(fn func(f *fileinfo.FileInfo) error) error {
	q := list.New()
	q.PushFront(r.tree)
	for q.Len() > 0 {
		front := q.Front()
		q.Remove(front)
		cur := front.Value.(*treeNode)
		if cur.included {
			if err := fn(cur.toFileInfo()); err != nil {
				return err
			}
		}
		n := len(cur.children)
		for i := range cur.children {
			q.PushFront(cur.children[n-i-1])
		}
	}
	return nil
}

// Close is needed by fileinfo.Provider
func (r *Result) Close() error {
	return nil
}
