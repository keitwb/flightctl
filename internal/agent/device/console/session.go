package console

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"os/user"
	"strings"
	"sync"
	"time"

	"github.com/creack/pty"
	grpc_v1 "github.com/flightctl/flightctl/api/grpc/v1"
	api "github.com/flightctl/flightctl/api/v1beta1"
	"github.com/flightctl/flightctl/pkg/executer"
	"github.com/flightctl/flightctl/pkg/log"
)

var (
	homedir     string
	homedirOnce sync.Once
)

type session struct {
	id                string
	log               *log.PrefixLogger
	streamClient      grpc_v1.RouterService_StreamClient
	executor          executer.Executer
	inactiveTimestamp time.Time
	user              string
}

func (s *session) getHomedir() (string, error) {
	// It is best to look this up each time it is needed since it can change dynamically at runtime.
	u, err := user.Lookup(s.user)
	if err != nil {
		return "", fmt.Errorf("failed to lookup console user %s: %w", s.user, err)
	}
	homedir = u.HomeDir
	if homedir == "" {
		return "", fmt.Errorf("home dir is not set for user %s", s.user)
	}
	return homedir, nil
}

func (s *session) close() error {
	var err error
	if s.streamClient != nil {
		if err = s.streamClient.CloseSend(); err != nil {
			s.log.Errorf("failed closing stream clean: %v", err)
		}
		s.streamClient = nil
	}
	s.inactiveTimestamp = time.Now()
	return err
}

func (s *session) buildBashCommand(ctx context.Context, metadata *api.DeviceConsoleSessionMetadata) *exec.Cmd {
	args := []string{
		"sudo",
		"-u", s.user,
	}

	if metadata.TTY {
		args = append(args, "--login")
	}

	if metadata.Command != nil && metadata.Command.Command != "" {
		args = append(args, strings.Join(append([]string{metadata.Command.Command}, metadata.Command.Args...), " "))
	}

	ret := s.executor.CommandContext(ctx, args[0], args[1:]...)

	if metadata.Term != nil {
		ret.Env = append(ret.Env, "TERM="+*metadata.Term)
	}

	h, err := s.getHomedir()
	if err == nil {
		ret.Dir = h
		ret.Env = append(ret.Env, "HOME="+h)
	}

	return ret
}

func (s *session) startProcess(metadata *api.DeviceConsoleSessionMetadata, cmd *exec.Cmd) (stdin io.WriteCloser, stdout, stderr io.ReadCloser, fd uintptr, err error) {
	if metadata.TTY {
		// create a new PTY
		p, err := pty.Start(cmd)
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("error starting pty: %w", err)
		}
		if metadata.InitialDimensions != nil {
			if err = setSize(p.Fd(), *metadata.InitialDimensions); err != nil {
				return nil, nil, nil, 0, fmt.Errorf("error setting initial dimesions: %w", err)
			}
		}
		return p, p, nil, p.Fd(), nil
	} else {
		stdin, err = cmd.StdinPipe()
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("error getting stdin pipe: %w", err)
		}
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("error getting stdout pipe: %w", err)
		}
		stderr, err = cmd.StderrPipe()
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("error getting stderr pipe: %w", err)
		}
		err = cmd.Start()
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("error starting process: %w", err)
		}
		return
	}
}

func (s *session) initialize(ctx context.Context, cancel context.CancelFunc, metadata *api.DeviceConsoleSessionMetadata) (*incomingStreams, *outgoingStreams, error) {
	cmd := s.buildBashCommand(ctx, metadata)
	stdin, stdout, stderr, resizeFd, err := s.startProcess(metadata, cmd)
	if err != nil {
		return nil, nil, err
	}
	closers := map[byte]io.Closer{
		StdinID:  stdin,
		StdoutID: stdout,
	}
	if stderr != nil {
		closers[StderrID] = stderr
	}
	iStreams := newIncomingStreams(s.streamClient, stdin, resizeFd, closers, cancel, s.log)
	oStreams := newOutgoingStreams(s.streamClient, cmd, stdout, stderr, s.log)
	return iStreams, oStreams, nil
}

func (s *session) run(ctx context.Context, metadata *api.DeviceConsoleSessionMetadata) {
	defer func() {
		_ = s.streamClient.CloseSend()
	}()
	defer s.log.Debugf("session %s finished", s.id)
	s.log.Debugf("session %s started", s.id)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	iStreams, oStreams, err := s.initialize(ctx, cancel, metadata)
	if err != nil {
		s.log.WithError(err).Error("initializing console session")
		return
	}
	var wg sync.WaitGroup
	iStreams.start(&wg)
	oStreams.start(&wg)
	wg.Wait()
}
