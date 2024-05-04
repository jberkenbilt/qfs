package misc

import "sync"

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
