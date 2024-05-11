package s3lister

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"slices"
	"testing"
)

func keys(objects []types.Object) []string {
	var result []string
	for _, o := range objects {
		result = append(result, *o.Key)
	}
	return result
}

func newForTest() *fakeListClient {
	b := &fakeListClient{}
	objects := []types.Object{
		{Key: aws.String("🥔🥗")},
		{Key: aws.String("π")},
	}
	for i := 0; i <= 3333; i++ {
		var prefix string
		switch i % 3 {
		case 0:
			prefix = "zero/"
		case 1:
			prefix = "one/"
		default:
		}
		objects = append(objects, types.Object{Key: aws.String(fmt.Sprintf("%skey%04d", prefix, i))})
	}
	b.addObjects(objects...)
	return b
}

func TestFakeBucket(t *testing.T) {
	b := newForTest()
	ctx := context.Background()

	input := &s3.ListObjectsV2Input{
		Bucket:     aws.String("not-used"),
		StartAfter: aws.String("zero/key3330"),
	}
	output, _ := b.ListObjectsV2(ctx, input)
	actual := keys(output.Contents)
	exp := []string{"zero/key3333", "π", "🥔🥗"}
	if !slices.Equal(actual, exp) {
		t.Errorf("%#v", actual)
	}

	input.StartAfter = nil
	input.Prefix = aws.String("one/")
	input.MaxKeys = aws.Int32(5)
	output, _ = b.ListObjectsV2(ctx, input)
	actual = keys(output.Contents)
	exp = []string{"one/key0001", "one/key0004", "one/key0007", "one/key0010", "one/key0013"}
	if !slices.Equal(actual, exp) {
		t.Errorf("%#v", actual)
	}

	input.StartAfter = nil
	input.Prefix = nil
	input.MaxKeys = aws.Int32(0)
	output, _ = b.ListObjectsV2(ctx, input)
	actual = keys(output.Contents)
	if !(len(actual) == 1000 && actual[0] == "key0002" && actual[999] == "key2999") {
		t.Error(actual[0], actual[999])
	}
}
