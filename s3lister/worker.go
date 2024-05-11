package s3lister

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log/slog"
	"sync"
	"time"
)

type worker struct {
	mutex      sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	config     workerConfig
	head       *node
	upperBound string
}

type workerConfig struct {
	Logger            *slog.Logger
	InitialUpperBound string
	Ctx               context.Context
	S3Client          s3.ListObjectsV2APIClient
	Input             s3.ListObjectsV2Input
	S3Options         []func(*s3.Options)
	OutputFn          func([]types.Object)
}

type node struct {
	w        *worker
	startKey string // original starting key
	lastKey  string // the last key read
	next     *node
	prev     *node
}

func newWorker(config workerConfig) (*worker, error) {
	parent := config.Ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	w := &worker{
		config:     config,
		ctx:        ctx,
		cancel:     cancel,
		upperBound: config.InitialUpperBound,
	}
	w.head = &node{
		w: w,
	}
	return w, nil
}

func (n *node) logger() *slog.Logger {
	if n.w.config.Logger == nil {
		return slog.Default()
	}
	return n.w.config.Logger
}

func (n *node) debug(msg string, args ...any) {
	n.logger().Debug(msg, args...)
}

// endKey returns the inclusive upper bound of the range.
func (n *node) endKey() string {
	if n.next == nil {
		return n.w.upperBound
	}
	return n.next.startKey
}

// bisect must be called with the mutex locked
func (n *node) bisect() *node {
	n.debug("bisecting", "node", n)
	n.debug("  before", "state", n.w)
	midpoint := stringMidpoint(n.lastKey, n.endKey())
	if midpoint == n.lastKey {
		// Too close to bisect
		return nil
	}
	// Create a new node between this node and its successor whose starting point is
	// halfway between this node's current location and end key.
	newNode := &node{
		w:        n.w,
		startKey: midpoint,
		lastKey:  midpoint,
		next:     n.next,
		prev:     n,
	}
	if n.next != nil {
		n.next.prev = newNode
	}
	n.next = newNode
	n.debug("  after", "state", n.w)
	return newNode
}

// mergeWithNext must be called with the mutex locked.
func (n *node) mergeWithNext() {
	n.debug("merging", "node", n)
	n.debug(" before", "state", n.w)
	if n.next != nil {
		n.next.startKey = n.startKey
		n.next.prev = n.prev
	}
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		n.w.head = n.next
	}
	n.debug(" after", "state", n.w)
}

func (n *node) run(started chan<- struct{}) error {
	defer close(started)
	input := n.w.config.Input
	first := true
	for {
		// Read the next page of keys. We will get between 0 and MaxKeys. Truncated
		// indicates whether we actually reached the end of the bucket. It's possible to
		// get fewer than MaxKeys even if there are more keys.
		n.w.mutex.Lock()
		input.StartAfter = aws.String(n.lastKey)
		n.w.mutex.Unlock()
		var output *s3.ListObjectsV2Output
		err := retryOnError(n.logger(), "list objects", 3, time.Second, func() error {
			var err error
			output, err = n.w.config.S3Client.ListObjectsV2(n.w.ctx, &input, n.w.config.S3Options...)
			return err
		})
		if err != nil {
			return fmt.Errorf("read from s3: %w", err)
		}

		// Grab objects that are within our range, and detect completion. The mutex must
		// be locked to prevent other nodes from changing start/end values.
		var objects []types.Object
		reachedEndOfRange := false
		func() {
			n.w.mutex.Lock()
			defer n.w.mutex.Unlock()
			endKey := n.endKey()
			for _, obj := range output.Contents {
				if n.startKey == "" {
					// This is the actual first key. Having it prevents us from bisecting into the
					// range of non-printable characters.
					n.startKey = *obj.Key
				}
				if first {
					first = false
					started <- struct{}{}
				}
				if *obj.Key > endKey {
					reachedEndOfRange = true
					break
				} else {
					objects = append(objects, obj)
				}
			}
			if len(objects) > 0 {
				n.lastKey = *objects[len(objects)-1].Key
			}
			if !*output.IsTruncated {
				reachedEndOfRange = true
			}
			if reachedEndOfRange {
				// If we have reached the end of our range and we are the last node, then our
				// start key is a tighter upper bound for unread keys. Only do this if we are the
				// last node to prevent our successor from stopping early.
				if n.next == nil {
					n.w.upperBound = n.startKey
				}
				if n.prev != nil && n.lastKey == n.startKey {
					// If we have reached the end and didn't find anything in this range, slide the
					// start key earlier and try again.
					midpoint := stringMidpoint(n.prev.lastKey, n.startKey)
					if midpoint < n.startKey {
						n.debug("adjusting start", "new", escapeUnicode(midpoint), "node", n)
						n.startKey = midpoint
						n.lastKey = n.startKey
						reachedEndOfRange = false
					}
				}
				if reachedEndOfRange {
					// There's nothing else for us to do. If we have a successor, give it credit for
					// our work by setting its start key to our start key and adjusting the links.
					n.mergeWithNext()
				}
			}
		}()
		if len(objects) > 0 {
			n.w.config.OutputFn(objects)
		}
		if reachedEndOfRange {
			break
		}
	}
	return nil
}

func (n *node) isEmpty() bool {
	return n.startKey == n.lastKey
}

func (w *worker) insertNode() *node {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	var nodeWithBiggestGap *node
	var maxGap uint64
	for n := w.head; n != nil; n = n.next {
		if n.isEmpty() || (n.next != nil && n.next.isEmpty()) {
			// Don't let bisection create two consecutive nodes with no keys.
			continue
		}
		gap := stringDistance(n.lastKey, n.endKey())
		if nodeWithBiggestGap == nil || gap > maxGap {
			nodeWithBiggestGap = n
			maxGap = gap
		}
	}
	if nodeWithBiggestGap == nil {
		return nil
	}
	return nodeWithBiggestGap.bisect()
}

func (n *node) goRun(c chan<- error) {
	started := make(chan struct{}, 1)
	go func() {
		err := n.run(started)
		if err != nil && n.w.ctx.Err() == nil {
			n.w.cancel()
		}
		c <- err
	}()
	<-started
}

func (n *node) String() string {
	return fmt.Sprintf("[%s..%s]", escapeUnicode(n.startKey), escapeUnicode(n.lastKey))
}

func (w *worker) String() string {
	var state string
	for n := w.head; n != nil; n = n.next {
		state += n.String() + " -> "
	}
	return state + w.upperBound
}

func (w *worker) run(c chan<- error) {
	w.head.goRun(c)
}

func (w *worker) addWorker(c chan<- error) bool {
	n := w.insertNode()
	if n == nil {
		return false
	}
	n.goRun(c)
	return true
}

func (w *worker) done() bool {
	if w.ctx.Err() != nil {
		return true
	}
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.head == nil
}
