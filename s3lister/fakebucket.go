package s3lister

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"sort"
	"strings"
)

type fakeListClient struct {
	objects []types.Object
}

// ListObjectsV2 does not fully emulate the real one. It just returns enough
// information for testing.
func (b *fakeListClient) ListObjectsV2(
	_ context.Context,
	input *s3.ListObjectsV2Input,
	_ ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	sort.Slice(b.objects, func(i, j int) bool {
		return *b.objects[i].Key < *b.objects[j].Key
	})
	var maxKeys int32
	if input.MaxKeys == nil {
		maxKeys = 1000
	} else {
		maxKeys = *input.MaxKeys
	}
	if maxKeys == 0 || maxKeys > 1000 {
		maxKeys = 1000
	}
	var contents []types.Object
	truncated := true
	token := input.ContinuationToken
	if token == nil {
		token = input.StartAfter
	}
	var nextToken *string
	for i, obj := range b.objects {
		if i == len(b.objects)-1 {
			truncated = false
		}
		nextToken = aws.String(*obj.Key)
		if input.Prefix != nil && !strings.HasPrefix(*obj.Key, *input.Prefix) {
			continue
		}
		if token != nil && *obj.Key <= *token {
			continue
		}
		contents = append(contents, obj)
		if int32(len(contents)) == maxKeys {
			break
		}
	}
	if !truncated {
		nextToken = nil
	}
	result := s3.ListObjectsV2Output{
		Contents:              contents,
		KeyCount:              aws.Int32(int32(len(contents))),
		MaxKeys:               aws.Int32(maxKeys),
		Name:                  input.Bucket,
		Prefix:                input.Prefix,
		StartAfter:            input.StartAfter,
		ContinuationToken:     input.ContinuationToken,
		NextContinuationToken: nextToken,
		IsTruncated:           aws.Bool(truncated),
	}
	return &result, nil
}

func (b *fakeListClient) addObjects(objects ...types.Object) {
	b.objects = append(b.objects, objects...)
}
