// Package traverse traverses a file system in multiple concurrent
// goroutines. It is faster than traversing in a single thread.
package traverse

import (
	"container/list"
	"context"
	"fmt"
	"github.com/jberkenbilt/qfs/queue"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type FileInfo struct {
	Path     string
	Size     int64
	ModTime  time.Time
	Mode     os.FileMode
	Uid      uint32
	Gid      uint32
	Target   string
	Children []*FileInfo
}

type traverser struct {
	root     string
	errChan  chan error
	workChan chan *FileInfo
	pending  atomic.Int64
	zero     chan struct{}
	q        *queue.Queue[*FileInfo]
}

func getFileInfo(top string, node *FileInfo) error {
	path := filepath.Join(top, node.Path)
	lst, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("lstat %s: %w", path, err)
	}
	node.Size = lst.Size()
	node.Mode = lst.Mode()
	node.ModTime = lst.ModTime()
	st, ok := lst.Sys().(*syscall.Stat_t)
	if ok && st != nil {
		node.Uid = st.Uid
		node.Gid = st.Gid
	}
	if node.Mode&fs.ModeSymlink != 0 {
		target, err := os.Readlink(path)
		if err != nil {
			return fmt.Errorf("readlink %s: %w", path, err)
		}
		node.Target = target
	} else if node.Mode.IsDir() {
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
	}
	return nil
}

func (tr *traverser) worker() {
	for node := range tr.workChan {
		if err := getFileInfo(tr.root, node); err != nil {
			tr.errChan <- err
		}
		var toAdd []*FileInfo
		for _, c := range node.Children {
			toAdd = append(toAdd, c)
		}
		tr.q.Push(toAdd...)
		if tr.pending.Add(int64(len(toAdd))-1) == 0 {
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
// FileInfo, which represents a tree of the file system. Call the Traverse method
// on the resulting FileInfo to walk through all the items.
func Traverse(root string, errFn func(error)) (*FileInfo, error) {
	numWorkers := 5 * runtime.NumCPU()
	tr := &traverser{
		root:     root,
		errChan:  make(chan error, numWorkers),
		workChan: make(chan *FileInfo, numWorkers),
		zero:     make(chan struct{}, 1),
		q:        queue.New[*FileInfo](),
	}
	var workerWait sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workerWait.Add(1)
		go func() {
			defer workerWait.Done()
			tr.worker()
		}()
	}
	errWait := make(chan struct{}, 1)
	go func() {
		defer close(errWait)
		for e := range tr.errChan {
			errFn(e)
		}
	}()

	tree := &FileInfo{Path: "."}
	tr.traverse(tree)
	close(tr.workChan)
	workerWait.Wait()
	close(tr.errChan)
	<-errWait
	return tree, nil
}

// Flatten traverses the FileInfo and calls the function for each item in lexical order.
func (f *FileInfo) Flatten(fn func(f *FileInfo)) {
	q := list.New()
	q.PushFront(f)
	for q.Len() > 0 {
		front := q.Front()
		q.Remove(front)
		cur := front.Value.(*FileInfo)
		fn(cur)
		n := len(cur.Children)
		for i := range cur.Children {
			q.PushFront(cur.Children[n-i-1])
		}
	}
}
