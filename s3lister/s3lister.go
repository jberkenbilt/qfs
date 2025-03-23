package s3lister

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log/slog"
	"os"
	"reflect"
	"time"
)

const DefaultThreads = 32

type Options func(*Lister)

type Lister struct {
	logger   *slog.Logger
	threads  int
	s3Client s3.ListObjectsV2APIClient
}

// WithoutChecksumWarnings can be passed as an options function when creating an
// S3 client. Starting in December 2024, all uploaded objects get checksums. When
// objects uploaded prior to that, if those objects don't have checksums, the SDK
// gives a warning. This suppresses the warning when the validation is checked
// because of lack of checksums. Without this, we get a warning on every
// GetObject from an older upload.
func WithoutChecksumWarnings(options *s3.Options) {
	options.DisableLogOutputChecksumValidationSkipped = true
}

func New(options ...Options) (*Lister, error) {
	l := &Lister{}
	for _, fn := range options {
		fn(l)
	}
	if l.logger == nil {
		l.logger = slog.Default()
	}
	if l.threads == 0 {
		l.threads = DefaultThreads
	}
	if reflect.ValueOf(l.s3Client).IsNil() {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, err
		}
		l.s3Client = s3.NewFromConfig(cfg, WithoutChecksumWarnings)
	}
	return l, nil
}

func WithThreads(threads int) func(*Lister) {
	return func(l *Lister) {
		l.threads = threads
	}
}

func WithDebug(debug bool) func(*Lister) {
	return func(l *Lister) {
		if debug {
			h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
			l.logger = slog.New(h)
		} else {
			l.logger = nil
		}
	}
}

func WithS3Client(s3Client s3.ListObjectsV2APIClient) func(*Lister) {
	return func(l *Lister) {
		l.s3Client = s3Client
	}
}

func KeyUpperBound(ctx context.Context, bucketName string, s3Client s3.ListObjectsV2APIClient) (string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:     &bucketName,
		MaxKeys:    aws.Int32(1),
		StartAfter: aws.String(""),
	}
	for {
		switch *input.StartAfter {
		case "":
			input.StartAfter = aws.String("~")
		case "~":
			input.StartAfter = aws.String("\u0100")
		case "\U00100000":
			input.StartAfter = aws.String("\U0010FFFF")
		case "\U0010FFFF":
			return "", fmt.Errorf("can't handle keys lexically after U+0010FFFF")
		default:
			t := []rune(*input.StartAfter)[0]
			t *= 2
			input.StartAfter = aws.String(string([]rune{t}))
		}
		output, err := s3Client.ListObjectsV2(ctx, input)
		if err != nil {
			return "", fmt.Errorf("list S3 bucket %s: %w", bucketName, err)
		}
		if len(output.Contents) == 0 {
			return *input.StartAfter, nil
		}
	}
}

// List lists all the keys in a bucket. For each response to ListObjectsV2, outFn
// is called. It must handle being called concurrently.
func (l *Lister) List(
	ctx context.Context,
	input *s3.ListObjectsV2Input,
	outFn func([]types.Object),
	options ...func(*s3.Options),
) error {
	upperBound, err := KeyUpperBound(ctx, *input.Bucket, l.s3Client)
	if err != nil {
		return err
	}
	w, err := newWorker(workerConfig{
		Logger:            l.logger,
		InitialUpperBound: upperBound,
		Ctx:               ctx,
		S3Client:          l.s3Client,
		Input:             *input,
		OutputFn:          outFn,
		S3Options:         options,
	})
	if err != nil {
		return err
	}

	c := make(chan error, 2*l.threads)
	w.run(c)
	active := 1
	for i := 1; i < l.threads; i++ {
		if w.addWorker(c) {
			active++
		}
	}
	var allErrors []error
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-c:
			active--
			if err != nil {
				allErrors = append(allErrors, err)
			}
			if !w.done() && w.addWorker(c) {
				active++
			}
		case <-ticker.C:
			l.logger.Debug("active threads", "threads", active)
		}
		if active == 0 {
			break
		}
		if !w.done() && active < l.threads && w.addWorker(c) {
			active++
		}
	}
	return errors.Join(allErrors...)
}
