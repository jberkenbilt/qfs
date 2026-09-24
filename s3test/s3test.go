package s3test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jberkenbilt/qfs/s3lister"
)

const (
	accessKey  = "qfs_demo_access_key"
	secretKey  = "qfs_demo_long_enough_secret_key"
	testRegion = "us-east-1"
	noEndpoint = "-none-"
)

type EnvVar struct {
	Key string
	Val string
}

type S3Test struct {
	serverCmd *exec.Cmd
	serverDir string
	endpoint  string
	env       []EnvVar
	s3Client  *s3.Client
}

func New() (*S3Test, error) {
	if _, err := exec.LookPath("weed"); err != nil {
		return nil, errors.New("'weed' was not found in path")
	}
	return &S3Test{}, nil
}

func unusedPort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("listen: %v", err))
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

// Running tests whether an externally started test server is running.
// If so, the endpoint URL is returned. If there are no errors but the
// server is not found, the URL is returned as the empty string.
func (s *S3Test) Running() (endpointUrl string, err error) {
	var ok bool
	if _, ok = os.LookupEnv("QFS_TEST_REAL_S3"); ok {
		return noEndpoint, nil
	}
	endpointUrl, ok = os.LookupEnv("AWS_ENDPOINT_URL")
	if !ok {
		return "", nil
	}
	u, err := url.Parse(endpointUrl)
	if err != nil {
		return "", nil
	}
	_, err = net.Dial("tcp", "127.0.0.1:"+u.Port())
	if err != nil {
		return "", nil
	}
	return endpointUrl, nil
}

// Start starts the test server if not already running and returns an
// indicator of whether it started it.
func (s *S3Test) Start() (bool, error) {
	var started bool
	var err error
	started, err = s.serverStart()
	if err != nil {
		return false, err
	}
	s.SetEnvironmentVariables()
	var cfg aws.Config
	if s.endpoint == noEndpoint {
		cfg, err = config.LoadDefaultConfig(context.Background())
	} else {
		cfg, err = config.LoadDefaultConfig(
			context.Background(),
			config.WithRegion(testRegion),
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
			),
		)
	}
	if err != nil {
		return false, fmt.Errorf("load aws config: %w", err)
	}
	s.s3Client = s3.NewFromConfig(
		cfg,
		func(options *s3.Options) {
			if s.endpoint != noEndpoint {
				options.BaseEndpoint = &s.endpoint
				options.UsePathStyle = true
			}
		},
		s3lister.WithoutChecksumWarnings,
	)

	return started, nil

}

func (s *S3Test) SetEnvironmentVariables() {
	for _, v := range s.env {
		if err := os.Setenv(v.Key, v.Val); err != nil {
			panic(err.Error())
		}
	}
}

func (s *S3Test) serverStart() (bool, error) {
	endpointUrl, err := s.Running()
	if err != nil {
		return false, err
	}
	started := false
	if endpointUrl == "" {
		serverDir, err := os.MkdirTemp("", "qfs-s3-test")
		if err != nil {
			return false, err
		}
		s.serverDir = serverDir
		s3Port := unusedPort()
		s.endpoint = fmt.Sprintf("http://localhost:%v", s3Port)
		s.env = []EnvVar{
			{Key: "AWS_ACCESS_KEY_ID", Val: accessKey},
			{Key: "AWS_SECRET_ACCESS_KEY", Val: secretKey},
			{Key: "AWS_ENDPOINT_URL", Val: s.endpoint},
		}
		cmd := exec.Command(
			"env",
			"AWS_ACCESS_KEY_ID="+accessKey,
			"AWS_SECRET_ACCESS_KEY="+secretKey,
			"weed",
			"mini",
			fmt.Sprintf("-s3.port=%d", s3Port),
			"-dir="+serverDir,
		)
		err = cmd.Start()
		if err != nil {
			return false, err
		}
		s.serverCmd = cmd
		started = true
		tries := 0
		for {
			_, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s3Port))
			if err == nil {
				break
			}
			if tries >= 20 {
				panic("timed out waiting for SeaweedFS")
			}
			tries++
			time.Sleep(500 * time.Millisecond)
		}
	} else {
		s.endpoint = endpointUrl
	}
	return started, nil
}

// Stop stops the server.
func (s *S3Test) Stop() error {
	return s.serverStop()
}

func (s *S3Test) serverStop() error {
	if s.serverCmd != nil {
		_ = s.serverCmd.Process.Kill()
		_ = s.serverCmd.Wait()
	}
	if s.serverDir != "" {
		_ = os.RemoveAll(s.serverDir)
	}
	return nil
}

func (s *S3Test) Env() string {
	var env strings.Builder
	for _, v := range s.env {
		fmt.Fprintf(&env, "export %s=%s\n", v.Key, v.Val)
	}
	return env.String()
}

func (s *S3Test) Client() *s3.Client {
	return s.s3Client
}
