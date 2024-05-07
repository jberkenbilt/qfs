package misc

import (
	"fmt"
	"os"
	"path/filepath"
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
	fmt.Printf("%s [y/n] ", prompt)
	var answer string
	if TestPromptChannel != nil {
		fmt.Println("")
		select {
		case answer = <-TestPromptChannel:
		default:
			panic("prompt called with empty TestPromptChannel")
		}
	} else {
		_, _ = fmt.Scanln(&answer)
	}
	return answer == "y"
}

// Message prepends the program name and appends a newline to whatever message is
// passed in, then writes it standard output.
func Message(format string, args ...any) {
	if TestMessageChannel != nil {
		select {
		case TestMessageChannel <- fmt.Sprintf(format, args...):
		default:
			panic("message called with full TestMessageChannel")
		}
	} else {
		fmt.Printf("%s: %s\n", progName, fmt.Sprintf(format, args...))
	}
}
