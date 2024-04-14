package traverse

// This code traverses a file system in multiple concurrent goroutines. It is
// faster than traversing in a single thread. Adding I/O to print the results
// slows it down a lot though.

import (
	"context"
	"fmt"
	"github.com/jberkenbilt/qfs/queue"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type FileInfo struct {
	Name     string
	Size     int64
	ModTime  time.Time
	Mode     os.FileMode
	Uid      uint32
	Gid      uint32
	Target   string
	Children []*FileInfo
}

type traverser struct {
	errChan  chan error
	workChan chan *request
	pending  atomic.Int64
	zero     chan struct{}
	q        *queue.Queue[*request]
}

type request struct {
	path string
	node *FileInfo
}

func getFileInfo(path string, node *FileInfo) error {
	lst, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("lstat %s: %w", path, err)
	}
	node.Size = lst.Size()
	node.Mode = lst.Mode()
	node.ModTime = lst.ModTime()
	st, ok := lst.Sys().(syscall.Stat_t)
	if ok {
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
		for _, e := range entries {
			if e.Name() == "." || e.Name() == ".." {
				continue
			}
			node.Children = append(node.Children, &FileInfo{Name: e.Name()})
		}
	}
	return nil
}

func (tr *traverser) worker() {
	for req := range tr.workChan {
		if err := getFileInfo(req.path, req.node); err != nil {
			tr.errChan <- err
		}
		var toAdd []*request
		for _, c := range req.node.Children {
			toAdd = append(toAdd, &request{
				path: req.path + "/" + c.Name,
				node: c,
			})
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

func (tr *traverser) getWork() []*request {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := make(chan []*request, 1)
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

func (tr *traverser) traverse(root string, node *FileInfo) {
	toDo := []*request{
		{
			path: root,
			node: node,
		},
	}
	tr.pending.Add(1)
	for toDo != nil {
		for _, r := range toDo {
			tr.workChan <- r
		}
		toDo = tr.getWork()
	}
}

func Traverse(root string) (*FileInfo, error) {
	numWorkers := 5 * runtime.NumCPU()
	tr := &traverser{
		errChan:  make(chan error, numWorkers),
		workChan: make(chan *request, numWorkers),
		zero:     make(chan struct{}, 1),
		q:        queue.New[*request](),
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
			_, _ = fmt.Fprintln(os.Stderr, e.Error())
		}
	}()

	tree := &FileInfo{Name: "."}
	tr.traverse(root, tree)
	close(tr.workChan)
	workerWait.Wait()
	close(tr.errChan)
	<-errWait
	return tree, nil
}

// Traverse traverses the FileInfo and calls the function for each item.
func (f *FileInfo) Traverse(fn func(path string, f *FileInfo)) {
	type node struct {
		path string
		f    *FileInfo
	}
	q := []node{
		{
			path: ".",
			f:    f,
		},
	}
	i := 0
	for i < len(q) {
		cur := q[i]
		path := filepath.Join(cur.path, cur.f.Name)
		fn(path, cur.f)
		for _, child := range cur.f.Children {
			q = append(q, node{
				path: path,
				f:    child,
			})
		}
		i++
	}
}
