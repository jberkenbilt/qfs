package queue

import (
	"context"
	"sync"
)

type Queue[T any] struct {
	mu    sync.Mutex
	data  []T
	ready chan struct{}
}

func New[T any]() *Queue[T] {
	return &Queue[T]{
		ready: make(chan struct{}, 1),
	}
}

func (q *Queue[T]) Push(newData ...T) {
	q.mu.Lock()
	q.data = append(q.data, newData...)
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *Queue[T]) GetAll(ctx context.Context) []T {
	var result []T
	for result == nil {
		q.mu.Lock()
		result = q.data
		q.data = nil
		q.mu.Unlock()
		if result != nil {
			break
		}
		select {
		case _, more := <-q.ready:
			if !more {
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
	return result
}

func (q *Queue[T]) Close() {
	close(q.ready)
}
