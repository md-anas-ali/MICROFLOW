//go:build unix

package autoopt

import (
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

// prepareCmd puts the command in its own process group and makes a context
// timeout/cancel kill that whole group (the default only kills the leader).
// Returns whether grouping is active. A caller-configured Setsid is respected
// (no grouping then).
func prepareCmd(cmd *exec.Cmd) bool {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	if cmd.SysProcAttr.Setsid || cmd.SysProcAttr.Setpgid {
		return false
	}
	cmd.SysProcAttr.Setpgid = true
	if cmd.Cancel != nil { // set by exec.CommandContext
		cmd.Cancel = func() error {
			if cmd.Process == nil {
				return nil
			}
			if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
				return cmd.Process.Kill() // same as the default behaviour
			}
			return nil
		}
	}
	return true
}

func groupAlive(pgid int) bool {
	err := syscall.Kill(-pgid, 0)
	if err != nil && err != syscall.EPERM {
		return false
	}
	return groupHasLiveMember(pgid) // ignore unreaped zombies (linux)
}

// reapZombies collects already-dead members of the group that were re-parented
// to this process (MacroFlow is PID 1 in the Docker image, so orphans become its
// children). ECHILD (not our children) simply ends the loop.
func reapZombies(pgid int) {
	for i := 0; i < 64; i++ {
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-pgid, &ws, syscall.WNOHANG, nil)
		if err != nil || pid <= 0 {
			return
		}
	}
}

func waitGroupGone(pgid int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		reapZombies(pgid)
		if !groupAlive(pgid) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// terminateGroup: graceful SIGTERM, wait up to grace, then SIGKILL. found
// reports whether any member existed at all. Only the given group is signalled.
func terminateGroup(pgid int, grace time.Duration) (found bool, err error) {
	if pgid <= 1 {
		return false, nil // never signal init / our own group by accident
	}
	reapZombies(pgid)
	if !groupAlive(pgid) {
		return false, nil
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	if waitGroupGone(pgid, grace) {
		return true, nil
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	if waitGroupGone(pgid, 2*time.Second) {
		return true, nil
	}
	return true, fmt.Errorf("process group %d still present after SIGKILL", pgid)
}

// terminateTracked terminates a process that was still registered (its leader
// had not been reaped) when the Service was released.
func terminateTracked(p *trackedProc, grace time.Duration) error {
	if p.grouped {
		_, err := terminateGroup(p.pid, grace)
		return err
	}
	if p.pid <= 1 {
		return nil
	}
	_ = syscall.Kill(p.pid, syscall.SIGTERM)
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if syscall.Kill(p.pid, 0) != nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(p.pid, syscall.SIGKILL)
	return nil
}

func processAlive(p *trackedProc) bool {
	if p.pid <= 1 {
		return false
	}
	if p.grouped {
		reapZombies(p.pid)
		return groupAlive(p.pid)
	}
	return syscall.Kill(p.pid, 0) == nil
}
