package s3test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"net"
	"os/exec"
	"strconv"
	"time"
)

const (
	user      = "qfs-test-root"
	password  = "qfs-test-pass"
	accessKey = "qfs-demo-access-key"
	secretKey = "qfs-demo-secret-key"
)

type portInfo struct {
	NetworkSettings struct {
		Ports struct {
			Tcp []struct {
				HostIp   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"9000/tcp"`
		} `json:"Ports"`
	} `json:"NetworkSettings"`
}

type S3Test struct {
	name     string
	port     int
	endpoint string
	env      string
	s3Client *s3.Client
}

func New(name string) (*S3Test, error) {
	return &S3Test{
		name: name,
	}, nil
}

func runCmd(args ...string) error {
	cmd := exec.Command(args[0], args[1:]...)
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
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

// Running tests whether the test container is running. If so, the port is
// returned. If there are no errors but the image is not found, the port is
// returned as 0.
func (s *S3Test) Running() (int, error) {
	cmd := exec.Command("docker", "inspect", s.name)
	stdOut, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}
	var buf bytes.Buffer
	done := make(chan error)
	go func() {
		_, err := io.Copy(&buf, stdOut)
		if err != nil {
			done <- err
		}
		done <- nil
	}()
	if err = cmd.Start(); err != nil {
		return 0, err
	}
	<-done
	err = cmd.Wait()
	if err != nil {
		return 0, nil
	}
	var info []portInfo
	if err = json.Unmarshal(buf.Bytes(), &info); err != nil {
		return 0, fmt.Errorf("unable to interpret docker inspect %s: %w", s.name, err)
	}
	if len(info) == 0 {
		return 0, fmt.Errorf("no port info for %s", s.name)
	}
	if len(info[0].NetworkSettings.Ports.Tcp) == 0 {
		return 0, fmt.Errorf("no exposed ports for %s", s.name)
	}
	port, err := strconv.Atoi(info[0].NetworkSettings.Ports.Tcp[0].HostPort)
	if err != nil {
		return 0, fmt.Errorf("can't interpret port for %s: %w", s.name, err)
	}
	return port, nil
}

// Start starts the test container if not already running and returns an
// indicator of whether it started it.
func (s *S3Test) Start() (bool, error) {
	port, err := s.Running()
	if err != nil {
		return false, err
	}
	started := false
	if port == 0 {
		port = unusedPort()
		err = runCmd(
			"docker", "run", "-d", "--rm",
			"-p", fmt.Sprintf("%d:9000", port),
			"-e", "MINIO_ROOT_USER="+user,
			"-e", "MINIO_ROOT_PASSWORD="+password,
			"-v", s.name+"-vol:/data",
			"--name", s.name, "minio/minio",
			"server", "/data",
		)
		if err != nil {
			return false, err
		}
		started = true
	}
	s.port = port
	s.endpoint = fmt.Sprintf("http://localhost:%d", port)
	s.env = fmt.Sprintf(`export AWS_ACCESS_KEY_ID=%s
export AWS_SECRET_ACCESS_KEY=%s
export AWS_SESSION_TOKEN=
export AWS_ENDPOINT_URL=%s
export AWS_DEFAULT_REGION=us-east-1
`,
		accessKey,
		secretKey,
		s.endpoint,
	)
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	if err != nil {
		return false, fmt.Errorf("load aws config: %w", err)
	}
	s.s3Client = s3.NewFromConfig(
		cfg,
		func(options *s3.Options) {
			options.BaseEndpoint = &s.endpoint
			options.UsePathStyle = true
		},
	)

	return started, nil
}

// Stop stops the container.
func (s *S3Test) Stop() error {
	var allErrors []error
	if err := runCmd("docker", "rm", "-f", s.name); err != nil {
		allErrors = append(allErrors, fmt.Errorf("remove container: %w", err))
	}
	if err := runCmd("docker", "volume", "rm", s.name+"-vol"); err != nil {
		allErrors = append(allErrors, fmt.Errorf("remove volume: %w", err))
	}
	return errors.Join(allErrors...)
}

func (s *S3Test) Init() error {
	tries := 0
	for {
		err := runCmd(
			"docker",
			"exec",
			s.name,
			"mc",
			"alias",
			"set",
			"qfs",
			"http://localhost:9000",
			user,
			password,
		)
		if err != nil {
			if tries >= 20 {
				return fmt.Errorf("set alias: %w", err)
			}
			tries++
			time.Sleep(500 * time.Millisecond)
		} else {
			break
		}
	}
	err := runCmd(
		"docker",
		"exec",
		s.name,
		"mc",
		"admin",
		"user",
		"svcacct",
		"add",
		"qfs",
		user,
		"--access-key",
		accessKey,
		"--secret-key",
		secretKey,
	)
	if err != nil {
		return fmt.Errorf("create keys: %w", err)
	}
	return nil
}

func (s *S3Test) Env() string {
	return s.env
}

func (s *S3Test) Client() *s3.Client {
	return s.s3Client
}

func (s *S3Test) Endpoint() string {
	return s.endpoint
}
