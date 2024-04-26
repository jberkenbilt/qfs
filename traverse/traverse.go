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

var numWorkers = 5 * runtime.NumCPU()

type Options func(*Traverser)

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

type Traverser struct {
	fs         fileinfo.Source
	root       string
	errChan    chan error
	notifyChan chan string
	workChan   chan *treeNode
	pending    atomic.Int64
	zero       chan struct{}
	q          *queue.Queue[*treeNode]
	rootDev    uint64
	filters    []*filter.Filter
	sameDev    bool
	cleanup    bool
	filesOnly  bool
	noSpecial  bool
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

func (tr *Traverser) getNode(node *treeNode) error {
	path := filepath.Join(tr.root, node.path)
	included, group := filter.IsIncluded(node.path, tr.filters...)
	node.included = included
	node.fileType = fileinfo.TypeUnknown
	lst, err := tr.fs.Lstat(path)
	if err != nil {
		// TEST: CAN'T COVER. There is way to intentionally create a file that we can see
		// in its directory but can't lstat, so this is not exercised.
		return fmt.Errorf("lstat %s: %w", path, err)
	}
	node.modTime = lst.ModTime().Truncate(time.Millisecond)
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
	isSpecial := false
	switch {
	case mode.IsRegular():
		node.fileType = fileinfo.TypeFile
		node.size = lst.Size()
		if group == filter.Junk && tr.cleanup {
			node.included = false
			if err = os.Remove(filepath.Join(tr.root, node.path)); err != nil {
				return fmt.Errorf("remove junk %s: %w", path, err)
			} else {
				tr.notifyChan <- fmt.Sprintf("removing %s", node.path)
			}
		}
	case modeType&os.ModeDevice != 0:
		isSpecial = true
		if modeType&os.ModeCharDevice != 0 {
			node.fileType = fileinfo.TypeCharDev
		} else {
			node.fileType = fileinfo.TypeBlockDev
		}
	case modeType&os.ModeSocket != 0:
		isSpecial = true
		node.fileType = fileinfo.TypeSocket
	case modeType&os.ModeNamedPipe != 0:
		isSpecial = true
		node.fileType = fileinfo.TypePipe
	case modeType&os.ModeSymlink != 0:
		node.fileType = fileinfo.TypeLink
		target, err := tr.fs.Readlink(path)
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
		if tr.sameDev && st != nil && tr.rootDev != st.Dev {
			// TEST: CAN'T COVER. This is on a different device. Exclude and don't traverse
			// into it. This is not exercised in the test suite as it is difficult without
			// root/admin privileges to construct a scenario where crossing device boundaries
			// will happen.
			node.included = false
			break
		}
		node.fileType = fileinfo.TypeDirectory
		entries, err := tr.fs.ReadDir(path)
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
	if isSpecial && (tr.noSpecial || tr.filesOnly) {
		node.included = false
	}
	if node.fileType == fileinfo.TypeDirectory && tr.filesOnly {
		// Special are excluded above, and links are included with filesOnly.
		node.included = false
	}
	return nil
}

func (tr *Traverser) worker() {
	for node := range tr.workChan {
		if err := tr.getNode(node); err != nil {
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

func (tr *Traverser) getWork() []*treeNode {
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

func (tr *Traverser) traverse(node *treeNode) {
	toDo := []*treeNode{node}
	tr.pending.Add(1)
	for toDo != nil {
		for _, r := range toDo {
			tr.workChan <- r
		}
		toDo = tr.getWork()
	}
}

func New(root string, options ...Options) (*Traverser, error) {
	tr := &Traverser{
		root:       root,
		errChan:    make(chan error, numWorkers),
		notifyChan: make(chan string, numWorkers),
		workChan:   make(chan *treeNode, numWorkers),
		zero:       make(chan struct{}, 1),
		q:          queue.New[*treeNode](),
	}
	for _, fn := range options {
		fn(tr)
	}
	if tr.fs == nil {
		tr.fs = local
	}

	if tr.fs.HasStDev() {
		lst, err := tr.fs.Lstat(root)
		if err != nil {
			return nil, fmt.Errorf("lstat %s: %w", root, err)
		}
		st, ok := lst.Sys().(*syscall.Stat_t)
		if ok && st != nil {
			tr.rootDev = st.Dev
		}
	}
	return tr, nil
}

func WithFilters(filters []*filter.Filter) func(*Traverser) {
	return func(tr *Traverser) {
		tr.filters = filters
	}
}

func WithSameDev(sameDev bool) func(*Traverser) {
	return func(tr *Traverser) {
		tr.sameDev = sameDev
	}
}

func WithCleanup(cleanup bool) func(*Traverser) {
	return func(tr *Traverser) {
		tr.cleanup = cleanup
	}
}

func WithFilesOnly(filesOnly bool) func(*Traverser) {
	return func(tr *Traverser) {
		tr.filesOnly = filesOnly
	}
}

func WithNoSpecial(noSpecial bool) func(*Traverser) {
	return func(tr *Traverser) {
		tr.noSpecial = noSpecial
	}
}

// Traverse traverses a file system starting from to given path and returns a
// FileInfo, which represents a tree of the file system. Call the Flatten method
// on the resulting FileInfo to walk through all the items included by the
// filters. Note that a specific FileInfo has an Included field indicating
// whether the item is included. Pruned directories' children are not included,
// but regular excluded directories are present in case they have included
// children.
func (tr *Traverser) Traverse(
	notifyFn func(string),
	errFn func(error),
) (*Result, error) {
	numWorkers := 5 * runtime.NumCPU()
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
		// If a directory is excluded but some of its descendants are included, the
		// directory itself won't appear. This could be changed if desired, but it would
		// involve an extra tree traversal.
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
