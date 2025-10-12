package nbdkit

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"time"
)

type NbdkitSocket struct {
	cmd     *exec.Cmd
	socket  string
	pidFile string
}

func (s *NbdkitSocket) Start() error {
	s.cmd.Stdout = os.Stdout
	s.cmd.Stderr = os.Stderr

	slog.Debug("Running command", "command", s.cmd)
	if err := s.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start nbdkit server: %w", err)
	}

	pidFileTimeout := time.After(10 * time.Second)
	tick := time.Tick(100 * time.Millisecond)

	for {
		select {
		case <-pidFileTimeout:
			s.cmd.Process.Kill()
			return fmt.Errorf("timeout waiting for pidfile to appear: %s", s.pidFile)
		case <-tick:
			if _, err := os.Stat(s.pidFile); err == nil {
				return nil
			}
		}
	}
}

func (s *NbdkitSocket) Stop() error {
	if err := s.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("failed to stop nbdkit server: %w", err)
	}

	os.Remove(s.socket)
	return nil
}

func (s *NbdkitSocket) Socket() string {
	return s.socket
}

func (s *NbdkitSocket) LibNBDExportName() string {
	return fmt.Sprintf("nbd+unix:///?socket=%s", s.socket)
}
