//go:build !unix

package autoopt

import (
	"os"
	"os/exec"
	"time"
)

// Non-unix (Windows): no process-group handling; Run is Start+Wait with the
// default context-kill behaviour, and Release can only kill a still-registered
// leader process.
func prepareCmd(cmd *exec.Cmd) bool { return false }

func terminateGroup(pgid int, grace time.Duration) (bool, error) { return false, nil }

func terminateTracked(p *trackedProc, grace time.Duration) error {
	proc, err := os.FindProcess(p.pid)
	if err != nil {
		return err
	}
	return proc.Kill()
}

func processAlive(p *trackedProc) bool { return false }
