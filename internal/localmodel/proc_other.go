//go:build !unix

package localmodel

import (
	"os"
	"os/exec"
	"time"
)

func configureChild(cmd *exec.Cmd) {}
func lowerPriority(pid int)        {}

func terminateProcess(cmd *exec.Cmd, pid int, grace time.Duration) {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

func killPID(pid int, grace time.Duration) {
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Kill()
	}
}

// Without a portable way to verify the command line, never signal a stale PID.
func processLooksLike(pid int, needle string) bool { return false }

func (m *Manager) checkDisk(dir string, need int64) error { return nil }
