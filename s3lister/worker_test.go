package s3lister

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestNode(t *testing.T) {
	w, err := newWorker(workerConfig{
		InitialUpperBound: "\u0200",
	})
	if err != nil {
		t.Fatalf("create worker: %v", err)
	}
	if e := w.head.endKey(); e != "\u0200" {
		t.Errorf("wrong end key: %s", e)
	}
	w.head.lastKey = "\x40"
	n1 := w.insertNode()
	if !(w.head.next == n1 && n1.prev == w.head) {
		t.Fatalf("linked list is messed up")
	}
	if e := w.head.endKey(); e != "\u0120" {
		t.Errorf("wrong head end key: %s", e)
	}
	if e := n1.endKey(); e != "\u0200" {
		t.Errorf("wrong new end key: %s", e)
	}
	w.head.lastKey = "\u0080"
	n1.lastKey += "."
	n2 := w.insertNode()
	if !(n1.next == n2 && n2.prev == n1) {
		t.Errorf("new node got inserted in the wrong place")
	}

	n1.mergeWithNext()
	if !(w.head.next == n2 && n2.prev == w.head) {
		t.Errorf("linked list is messed up")
	}

	if !(w.head.lastKey == "\u0080" && w.head.endKey() == "\u0120") {
		t.Errorf("keys are wrong")
	}
	w.head.mergeWithNext()
	if w.head != n2 {
		t.Errorf("merging head failed")
	}
	w.head.mergeWithNext()
	if w.head != nil {
		t.Errorf("merging only remaining node failed")
	}
	if w.insertNode() != nil {
		t.Errorf("oops, inserted node after finishing")
	}
}

func TestRun(t *testing.T) {
	b := &fakeListClient{}
	objects := []types.Object{
		{
			Key:          aws.String("one"),
			LastModified: aws.Time(time.Now()),
			Size:         aws.Int64(1),
		},
		{
			Key:          aws.String("two"),
			LastModified: aws.Time(time.Now().Add(-time.Hour)),
			Size:         aws.Int64(2),
		},
		{
			Key:          aws.String("three"),
			LastModified: aws.Time(time.Now().Add(-2 * time.Hour)),
			Size:         aws.Int64(3),
		},
		{
			Key:          aws.String("four"),
			LastModified: aws.Time(time.Now().Add(-3 * time.Hour)),
			Size:         aws.Int64(3),
		},
	}
	b.addObjects(objects...)
	called := 0
	var received []types.Object
	callback := func(ob []types.Object) {
		received = append(received, ob...)
		called++
	}
	w, _ := newWorker(workerConfig{
		InitialUpperBound: "three+",
		Ctx:               context.Background(),
		S3Client:          b,
		Input:             s3.ListObjectsV2Input{MaxKeys: aws.Int32(2)},
		OutputFn:          callback,
	})
	err := w.head.run(make(chan struct{}, 1))
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	// At the end, the upper bound should be the first key in the bucket.
	if w.upperBound != "four" {
		t.Errorf("didn't find end: %s", escapeUnicode(w.upperBound))
	}
	// Expect objects lexically below "three+" in lexical order over two calls
	exp := []types.Object{
		objects[3], // "four"
		objects[0], // "one"
		objects[2], // "three"
	}
	if !slices.EqualFunc(received, exp, func(o1 types.Object, o2 types.Object) bool {
		return *o1.Key == *o2.Key && o1.Size == o2.Size && o1.LastModified.Equal(*o2.LastModified)
	}) {
		t.Error("got wrong results")
		for _, o := range received {
			t.Errorf("  %s", *o.Key)
		}
	}
	if called != 2 {
		t.Errorf("called wrong number of times: %d", called)
	}
}

func TestFirstDifference(t *testing.T) {
	type Data struct {
		s1  string
		s2  string
		exp int
	}
	tests := []Data{
		{"", "", 0},
		{"a", "", 0},
		{"", "a", 0},
		{"a", "a", 1},
		{"a", "b", 0},
		{"012π4", "012π6", 4},
		{"012π4", "012π456", 5},
	}
	for _, d := range tests {
		t.Run(fmt.Sprintf("%s/%s", d.s1, d.s2), func(t *testing.T) {
			if r := firstDifference([]rune(d.s1), []rune(d.s2)); r != d.exp {
				t.Errorf("got %d, wanted %d", r, d.exp)
			}
		})
	}
}

func TestStringDistance(t *testing.T) {
	type Data struct {
		s1  string
		s2  string
		exp uint64
	}
	tests := []Data{
		{"", "", 0},
		{"a", "a", 0},
		{"a", "", 0x6100000000},
		{"", "a", 0x6100000000},
		{"a", "b", 0x100000000},
		{"ab", "ba", 0xffffffff},
		{"012π4", "012π6", 0x200000000},
		{"012π4", "012π456", 0x3500000036},
		{"wwww012π4", "wwww012π456", 0x3500000036},
		{"wwww012π4", "wwww012ς856", 0x200000004},
		{"wwww012π4", "wwww012@856", 0x37ffffffffc},
	}
	for _, d := range tests {
		t.Run(fmt.Sprintf("%s/%s", d.s1, d.s2), func(t *testing.T) {
			if r := stringDistance(d.s1, d.s2); r != d.exp {
				t.Errorf("got %x, wanted %x", r, d.exp)
			}
		})
	}
}

func TestStringMidpoint(t *testing.T) {
	type Data struct {
		s1  string
		s2  string
		exp string
	}
	tests := []Data{
		{"", "", ""},
		{"a", "a", "a"},
		{"a", "", "@"},
		{"", "a", "@"},
		{"a", "b", "aO"},
		{"b", "a~", "a\u00cd"},
		{"aa", "b", "a\u00b0"},
		{"a", "a ", "a"},
		{"012π4", "012π6", "012π5"},
		{"potato", "salad", "q"},
		{"qww÷π", "qww÷🥔", "qww÷\uFE8A"},
		{"eft", "eg+", "ef\u00c3"},
		// Exercise forbidden ranges -- the same midpoints offset by 0x1000 land in their
		// real midpoints, but the forbidden ones land outside the restricted range.
		{"x", "x-", "x&"},
		{"\uEDC0", "\uEDFF", "\uEDDF"},
		{"\uFDC0", "\uFDFF", "\uFDCF"},
		{"\uEDC0", "\uEE00", "\uEDE0"},
		{"\uFDC0", "\uFE00", "\uFDF0"},
		{"\uC700", "\uD000", "\uCB80"},
		{"\uD700", "\uE000", "\uD7FF"},
		{"\uC700", "\uD200", "\uCC80"},
		{"\uD700", "\uE200", "\uE000"},
		{"\uEFFC", "\uF000", "\uEFFE"},
		{"\uFFFC", "\U00010000", "\uFFFD"},
		{"\uEFFC", "\uF002", "\uEFFF"},
		{"\uFFFC", "\U00010002", "\U00010000"},
	}
	for _, d := range tests {
		label := escapeUnicode(d.s1) + "/" + escapeUnicode(d.s2)
		t.Run(label, func(t *testing.T) {
			if r := stringMidpoint(d.s1, d.s2); r != d.exp {
				t.Errorf("got %s, wanted %s", escapeUnicode(r), escapeUnicode(d.exp))
			}
		})
	}
}

func TestRetryOnError(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{})
	logger := slog.New(h)
	count := 0
	if err := retryOnError(logger, "test", 3, time.Millisecond, func() error {
		count++
		return nil
	}); err != nil {
		t.Error(err.Error())
	}
	if count != 1 {
		t.Errorf("called too many times")
	}
	count = 0
	if err := retryOnError(logger, "test", 3, time.Millisecond, func() error {
		count++
		if count == 1 {
			return fmt.Errorf("oops")
		}
		return nil
	}); err != nil {
		t.Error(err.Error())
	}
	if count != 2 {
		t.Errorf("called wrong number of times")
	}
	count = 0
	err := retryOnError(logger, "test", 3, time.Millisecond, func() error {
		count++
		return fmt.Errorf("oops")
	})
	if err == nil {
		t.Error("no error")
	} else if err.Error() != "error from test: oops" {
		t.Errorf("wrong error: %v", err)
	}
	if count != 3 {
		t.Errorf("called wrong number of times")
	}
	logs := buf.String()
	if !strings.Contains(logs, "error from test; retrying") {
		t.Errorf("didn't get expected log: %v", logs)
	}
}

func TestEscapeUnicode(t *testing.T) {
	if s := escapeUnicode("π"); s != `\u03c0` {
		t.Errorf("bad 4-digit hex: %v", s)
	}
	if s := escapeUnicode("🥔"); s != `\U0001f954` {
		t.Errorf("bad 8-digit hex: %v", s)
	}
}
