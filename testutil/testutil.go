package testutil

import (
	"bytes"
	"github.com/jberkenbilt/qfs/qfs"
	"io"
	"os"
	"slices"
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

func CheckLines(t *testing.T, cmd []string, expLines []string) {
	t.Helper()
	stdout, stderr := WithStdout(func() {
		Check(t, qfs.Run(cmd))
	})
	if len(stderr) > 0 {
		t.Errorf("stderr: %s", stderr)
	}
	lines := ToLines(stdout)
	if !slices.Equal(lines, expLines) {
		t.Error("wrong output")
		for _, line := range lines {
			t.Error(line)
		}
	}
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

func ExpStdout(t *testing.T, fn func(), expStdoutContains, expStderrContains string) {
	t.Helper()
	stdout, stderr := WithStdout(fn)
	if !strings.Contains(string(stdout), expStdoutContains) {
		t.Errorf("wrong stdout: %s", stdout)
	}
	if !strings.Contains(string(stderr), expStderrContains) {
		t.Errorf("wrong stdout: %s", stderr)
	}
}
