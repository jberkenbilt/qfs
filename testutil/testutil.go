package testutil

import (
	"bytes"
	"github.com/jberkenbilt/qfs/misc"
	"github.com/jberkenbilt/qfs/qfs"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
)

func ToLines(out []byte) []string {
	var lines []string
	for _, line := range strings.Split(string(out), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func Check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err.Error())
	}
}

func checkLinesInternal(t *testing.T, sorted bool, filter func(string) string, cmd []string, expLines []string) {
	stdout, stderr := WithStdout(func() {
		Check(t, qfs.Run(cmd))
	})
	if len(stderr) > 0 {
		t.Errorf("stderr: %s", stderr)
	}
	lines := ToLines(stdout)
	if filter != nil {
		var t []string
		for _, line := range lines {
			t = append(t, filter(line))
		}
		lines = t
	}
	if sorted {
		sort.Strings(lines)
		sort.Strings(expLines)
	}
	if !slices.Equal(lines, expLines) {
		t.Error("wrong output")
		for _, line := range lines {
			t.Error(line)
		}
	}
}

func CheckLines(t *testing.T, cmd []string, expLines []string) {
	t.Helper()
	checkLinesInternal(t, false, nil, cmd, expLines)
}

func CheckLinesSorted(t *testing.T, filter func(string) string, cmd []string, expLines []string) {
	t.Helper()
	checkLinesInternal(t, true, filter, cmd, expLines)
}

func WithStdout(fn func()) ([]byte, []byte) {
	originalStdout := os.Stdout
	originalStderr := os.Stderr
	r1, w1, _ := os.Pipe()
	os.Stdout = w1
	r2, w2, _ := os.Pipe()
	os.Stderr = w2
	var buf1, buf2 bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf1, r1)
		_ = r1.Close()
		_, _ = io.Copy(&buf2, r2)
		_ = r2.Close()
		close(done)
	}()
	fn()
	_ = w1.Close()
	_ = w2.Close()
	os.Stdout = originalStdout
	os.Stderr = originalStderr
	<-done
	return buf1.Bytes(), buf2.Bytes()
}

func ExpStdout(t *testing.T, fn func(), expStdout, expStderr string) {
	t.Helper()
	stdout, stderr := WithStdout(fn)
	if expStdout != string(stdout) {
		t.Errorf("wrong stdout: %s", stdout)
	}
	if expStderr != string(stderr) {
		t.Errorf("wrong stderr: %s", stderr)
	}
}

func CaptureMessages() (cleanup func(), checkMessages func(*testing.T, []string)) {
	// Monitor messages. Send a magic string to catch up send messages accumulated so
	// far.
	msgChan := make(chan []string, 1)
	misc.TestMessageChannel = make(chan string, 5)
	const MsgCatchup = "!CHECK!"
	go func() {
		var accumulated []string
		for m := range misc.TestMessageChannel {
			if m == MsgCatchup {
				msgChan <- accumulated
				accumulated = nil
			} else {
				accumulated = append(accumulated, m)
			}
		}
	}()
	getMessages := func() []string {
		misc.Message(MsgCatchup)
		return <-msgChan
	}
	cleanup = func() {
		close(misc.TestMessageChannel)
		misc.TestMessageChannel = nil
	}
	checkMessages = func(t *testing.T, exp []string) {
		t.Helper()
		messages := getMessages()
		mActual := map[string]struct{}{}
		for _, m := range messages {
			mActual[m] = struct{}{}
		}
		mExp := map[string]struct{}{}
		for _, m := range exp {
			if _, ok := mActual[m]; !ok {
				t.Errorf("missing message: %s", m)
			}
			mExp[m] = struct{}{}
		}
		for _, m := range messages {
			if _, ok := mExp[m]; !ok {
				t.Errorf("extra message: %s", m)
			}
		}
	}
	return
}
