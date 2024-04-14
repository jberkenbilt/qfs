package queue_test

import (
	"context"
	"github.com/jberkenbilt/qfs/queue"
	"slices"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	bg := context.Background()
	q := queue.New[string]()
	q.Push("one", "two")
	data := q.GetAll(bg)
	if !slices.Equal(data, []string{"one", "two"}) {
		t.Errorf("wrong result: %#v", data)
	}
	c := make(chan []string, 1)
	go func() {
		c <- q.GetAll(bg)
	}()
	select {
	case <-c:
		t.Errorf("failed")
	case <-time.After(10 * time.Millisecond):
	}
	cancelled, cancel := context.WithCancel(bg)
	cancel()
	if q.GetAll(cancelled) != nil {
		t.Errorf("wait cancelled failed")
	}
	q.Push("three", "four")
	select {
	case data = <-c:
	case <-time.After(10 * time.Millisecond):
		t.Errorf("no data")
	}
	if !slices.Equal(data, []string{"three", "four"}) {
		t.Errorf("wrong result: %#v", data)
	}
	q.Close()
	if q.GetAll(bg) != nil {
		t.Errorf("close failed")
	}
}
