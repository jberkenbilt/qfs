// Package traverse traverses a file system in multiple concurrent
// goroutines. It is faster than traversing in a single thread.
package traverse

import (
	"container/list"
	"context"
	"fmt"
	"github.com/jberkenbilt/qfs/database"
	"github.com/jberkenbilt/qfs/fileinfo"
	"github.com/jberkenbilt/qfs/filter"
	"github.com/jberkenbilt/qfs/queue"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var numWorkers = 5 * runtime.NumCPU()

type Options func(*Traverser)

type Result struct {
	tree *treeNode
}

type treeNode struct {
	path     string
	s3Dir    bool
	info     *fileinfo.FileInfo
	children []*treeNode
	included bool
}

type Traverser struct {
	fs         fileinfo.Source
	root       *fileinfo.Path
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

func (tr *Traverser) getNode(node *treeNode) error {
	path := tr.root.Join(node.path)
	included, group := filter.IsIncluded(node.path, tr.filters...)
	node.included = included
	var err error
	if node.s3Dir {
		// qfs stores an empty directory marker ending with a slash for directories. If
		// we have x/ and x/y, when we list with / as the delimiter, we will see x/ as a
		// prefix, but we won't see the x/ key until we list x/, and therefore we won't
		// know the s3 modification time. This means we can never get a cache hit on the
		// database, which means we'd have to make a HeadObject call for every directory.
		// If we mark something as an S3 directory, this tells us that we can get info as
		// we traverse the children. Create a place-holder that we will fill in if we
		// can.
		node.info = &fileinfo.FileInfo{
			Path:        node.path,
			FileType:    fileinfo.TypeDirectory,
			ModTime:     time.Now(),
			Permissions: 0755,
			Uid:         database.CurUid,
			Gid:         database.CurGid,
		}
	} else {
		node.info, err = path.FileInfo()
		if err != nil {
			// TEST: NOT COVERED. This would mean we couldn't get FileInfo for a file we
			// encountered during directory traversal.
			return err
		}
	}
	ft := node.info.FileType
	isSpecial := !(ft == fileinfo.TypeFile || ft == fileinfo.TypeDirectory || ft == fileinfo.TypeLink)
	if ft == fileinfo.TypeFile {
		if group == filter.Junk && tr.cleanup {
			node.included = false
			if err = tr.root.Join(node.path).Remove(); err != nil {
				return fmt.Errorf("remove junk %s: %w", path.Path(), err)
			} else {
				tr.notifyChan <- fmt.Sprintf("removing %s", node.path)
			}
		}
	} else if ft == fileinfo.TypeDirectory {
		skip := false
		if !included && group == filter.Prune {
			// Don't traverse into pruned directories
			skip = true
		}
		if tr.sameDev && tr.fs.HasStDev() && tr.rootDev != node.info.Dev {
			// TEST: CAN'T COVER. This is on a different device. Exclude and don't traverse
			// into it. This is not exercised in the test suite as it is difficult without
			// root/admin privileges to construct a scenario where crossing device boundaries
			// will happen.
			node.included = false
			skip = true
		}
		if !skip {
			entries, err := path.DirEntries()
			if err != nil {
				return fmt.Errorf("read dir %s: %w", path.Path(), err)
			}

			if node.s3Dir {
				// Now that we've read the directory entries, we should be able to get a cache it
				// on the info call. An error here would mean that the directory marker itself
				// was missing. In that case, we'll consider the directory to be not included.
				// This is better than skipping the descendents.
				info, err := path.FileInfo()
				if err == nil {
					node.info = info
				} else {
					node.included = false
				}
			}
			sort.Slice(entries, func(i, j int) bool {
				return entries[i].Name < entries[j].Name
			})
			for _, e := range entries {
				node.children = append(node.children, &treeNode{
					path:  filepath.Join(node.path, e.Name),
					s3Dir: e.S3Dir,
				})
			}
		}
	}
	if isSpecial && (tr.noSpecial || tr.filesOnly) {
		node.included = false
	}
	if ft == fileinfo.TypeDirectory && tr.filesOnly {
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
		tr.fs = fileinfo.NewLocal(root)
	}
	tr.root = fileinfo.NewPath(tr.fs, ".")

	if tr.fs.HasStDev() {
		fi, err := tr.root.FileInfo()
		if err != nil {
			return nil, err
		}
		tr.rootDev = fi.Dev
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

func WithSource(fs fileinfo.Source) func(*Traverser) {
	return func(tr *Traverser) {
		tr.fs = fs
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

	tree := &treeNode{
		path:  ".",
		s3Dir: tr.fs.IsS3(),
	}
	tr.traverse(tree)
	close(tr.workChan)
	workerWait.Wait()
	close(tr.errChan)
	close(tr.notifyChan)
	wg.Wait()
	tr.fs.Finish()
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
			if err := fn(cur.info); err != nil {
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
