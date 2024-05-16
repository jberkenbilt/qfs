package misc

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var progName = filepath.Base(os.Args[0])

var TestMessageChannel chan string // If defined, Message writes to this channel
var TestPromptChannel chan string  // If defined, Prompt reads from this channel

// DoConcurrently is a simple worker pool implementation. It starts up numWorkers
// goroutines and, in each, calls `work(c, errorChan)`. Any errors that `work`
// writes to errorChan are passed to handleError(). DoConcurrently returns when
// all the workers have exited.
func DoConcurrently[T any, errorT any](
	work func(c chan T, errorChan chan errorT),
	handleError func(e errorT),
	c chan T,
	numWorkers int,
) {
	errorChan := make(chan errorT, 1)
	errorDone := make(chan struct{}, 1)
	go func() {
		for err := range errorChan {
			handleError(err)
		}
		errorDone <- struct{}{}
	}()
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			work(c, errorChan)
		}()
	}
	wg.Wait()
	close(errorChan)
	<-errorDone
}

// Prompt asks a yes/no question. It appends ` [y/n] ` to the prompt.
func Prompt(prompt string) bool {
	var answer string
	if TestPromptChannel != nil {
		fmt.Printf("prompt: %s\n", prompt)
		select {
		case answer = <-TestPromptChannel:
		default:
			_, _ = fmt.Fprintf(os.Stderr, "prompt called with empty TestPromptChannel: "+prompt)
		}
	} else {
		fmt.Printf("%s [y/n] ", prompt)
		_, _ = fmt.Scanln(&answer)
	}
	return answer == "y"
}

// Message prepends the program name and appends a newline to whatever message is
// passed in, then writes it standard output.
func Message(format string, args ...any) {
	if TestMessageChannel != nil {
		TestMessageChannel <- fmt.Sprintf(format, args...)
	} else {
		fmt.Printf("%s: %s\n", progName, fmt.Sprintf(format, args...))
	}
}

// RemovePrefix removes prefix/ from the beginning of a key that is known to
// start with prefix/.
func RemovePrefix(key string, prefix string) string {
	if prefix == "" {
		return key
	}
	prefix += "/"
	if !strings.HasPrefix(key, prefix) {
		// TEST: NOT COVERED. ListObjectsV2 won't return a key that doesn't start with
		// the requested prefix.
		panic("key doesn't start with prefix")
	}
	return key[len(prefix):]
}
