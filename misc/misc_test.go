package misc_test

import (
	"fmt"
	"github.com/jberkenbilt/qfs/misc"
	"github.com/jberkenbilt/qfs/testutil"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type workBatch struct {
	s string
	i int
}

func TestDoConcurrently(t *testing.T) {
	var m sync.Mutex
	data := map[string]int{}
	c := make(chan workBatch, 10)

	// Worker function
	work := func(c chan workBatch, e chan error) {
		for b := range c {
			if b.i%7 == 0 {
				e <- fmt.Errorf("generated: %d", b.i)
			} else {
				m.Lock()
				data[b.s] = b.i
				m.Unlock()
			}
		}
	}

	// Error handler
	errorsSeen := map[string]bool{}
	handleError := func(err error) {
		errorsSeen[err.Error()] = true
	}

	// Submit work in the background.
	go func() {
		for i := 1; i <= 20; i++ {
			c <- workBatch{s: fmt.Sprintf("this is %d", i), i: i}
		}
		close(c)
	}()

	// Invoke the pool
	misc.DoConcurrently(work, handleError, c, 10)

	// Check
	if !reflect.DeepEqual(errorsSeen, map[string]bool{
		"generated: 7":  true,
		"generated: 14": true,
	}) {
		t.Errorf("actual errors: %#v", errorsSeen)
	}
	exp := map[string]int{}
	for i := 1; i <= 20; i++ {
		if i%7 != 0 {
			exp[fmt.Sprintf("this is %d", i)] = i
		}
	}
	if !reflect.DeepEqual(exp, data) {
		t.Errorf("data: %#v", data)
	}
}

func TestBlockedPrompt(t *testing.T) {
	defer func() {
		misc.TestPromptChannel = nil
	}()
	var panicMessage string
	stdout, _ := testutil.WithStdout(func() {
		defer func() {
			panicMessage = recover().(string)
		}()
		misc.TestPromptChannel = make(chan string)
		_ = misc.Prompt("Potato?")
		t.Errorf("didn't panic")
	})
	if !strings.Contains(panicMessage, "empty") {
		t.Errorf("wrong message: %v", panicMessage)
	}
	if string(stdout) != "prompt: Potato?\n" {
		t.Errorf("didn't see prompt: |%s|", stdout)
	}
}

func TestMessagePromptChannels(t *testing.T) {
	defer func() {
		misc.TestPromptChannel = nil
		misc.TestMessageChannel = nil
	}()
	misc.TestPromptChannel = make(chan string, 1)
	misc.TestMessageChannel = make(chan string, 1)
	misc.TestPromptChannel <- "y"
	stdout, _ := testutil.WithStdout(func() {
		misc.Message("quack")
		if !misc.Prompt("Moo?") {
			t.Errorf("prompt didn't work")
		}
		if m := <-misc.TestMessageChannel; m != "quack" {
			t.Errorf("message didn't work: |%s|", m)
		}
	})
	if string(stdout) != "prompt: Moo?\n" {
		t.Errorf("wrong output: %s", stdout)
	}
}
