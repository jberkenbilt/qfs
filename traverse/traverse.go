// Package traverse traverses a file system in multiple concurrent
// goroutines. It is faster than traversing in a single thread.
package traverse

import (
	"container/list"
	"context"
	"fmt"
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
		if err := getFileInfo(tr.root, node); err != nil {
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

// Flatten traverses the FileInfo and calls the function for each item in lexical
// order. If the function returns an error, traversal is stopped, and the error
// is returned.
func (f *FileInfo) Flatten(fn func(f *FileInfo) error) error {
	q := list.New()
	q.PushFront(f)
	for q.Len() > 0 {
		front := q.Front()
		q.Remove(front)
		cur := front.Value.(*FileInfo)
		if err := fn(cur); err != nil {
			return err
		}
		n := len(cur.Children)
		for i := range cur.Children {
			q.PushFront(cur.Children[n-i-1])
		}
	}
	return nil
}
