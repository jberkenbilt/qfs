package s3lister

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"strings"
	"testing"
	"time"
)

func TestKeyUpperBound(t *testing.T) {
	ctx := context.Background()
	b := &fakeListClient{}
	b.addObjects(types.Object{Key: aws.String("potato")})
	upperBound, _ := KeyUpperBound(ctx, "", b)
	if upperBound != "~" {
		t.Errorf("upper bound: %v", upperBound)
	}
	b.addObjects(types.Object{Key: aws.String("π")})
	upperBound, _ = KeyUpperBound(ctx, "", b)
	if upperBound != "\u0400" {
		t.Errorf("upper bound: %v", upperBound)
	}
	b.addObjects(types.Object{Key: aws.String("🥔")})
	upperBound, _ = KeyUpperBound(ctx, "", b)
	if upperBound != "\U00020000" {
		t.Errorf("upper bound: %v", upperBound)
	}
	b.addObjects(types.Object{Key: aws.String("\U0010FFFE")})
	upperBound, _ = KeyUpperBound(ctx, "", b)
	if upperBound != "\U0010FFFF" {
		t.Errorf("upper bound: %v", upperBound)
	}
	b.addObjects(types.Object{Key: aws.String("\U0010FFFF.")})
	upperBound, err := KeyUpperBound(ctx, "", b)
	if !(err != nil && strings.Contains(err.Error(), "can't handle")) {
		t.Errorf("upperBound=%v, error: %v", upperBound, err)
	}
}

func TestLister(t *testing.T) {
	// Create a random collection of keys distributed over a narrow key range, and
	// make sure we get them all.
	makeKey := func(n int) string {
		data := []byte(fmt.Sprintf("%d %v", n, time.Now()))
		return fmt.Sprintf("%x", md5.Sum(data))
	}
	b := &fakeListClient{}
	keys := map[string]int{}
	for i := 0; i < 500000; i++ {
		k := makeKey(i)
		keys[k] = 1
		b.addObjects(types.Object{Key: aws.String(k)})
	}
	actual := map[string]int{}
	const threads = 20
	c := make(chan []types.Object, 10*threads)
	done := make(chan struct{}, 1)
	go func() {
		for objects := range c {
			for _, obj := range objects {
				actual[*obj.Key]++
			}
		}
		done <- struct{}{}
	}()
	fn := func(objects []types.Object) {
		c <- objects
	}
	lister, err := New(
		WithThreads(threads),
		WithS3Client(b),
		WithDebug(false), // pass true of logging is needed
	)
	if err != nil {
		t.Errorf("create lister: %v", err)
	}
	err = lister.List(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("any")}, fn)
	if err != nil {
		t.Errorf("lister failed: %v", err)
	}
	close(c)
	<-done
	for k := range keys {
		if _, ok := actual[k]; !ok {
			t.Errorf("missing: %s", k)
		}
	}
	for k, v := range actual {
		if v > 1 {
			t.Errorf("duplicated: %s (%d)", k, v)
		}
	}
}
