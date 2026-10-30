//go:build unix

package localmodel

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

func configureChild(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	setPdeathsig(cmd.SysProcAttr)
}

func lowerPriority(pid int) { _ = syscall.Setpriority(syscall.PRIO_PROCESS, pid, 10) }

func terminateProcess(cmd *exec.Cmd, pid int, grace time.Duration) {
	if pid <= 1 {
		return
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if syscall.Kill(-pid, 0) != nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}

func killPID(pid int, grace time.Duration) {
	p, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	_ = p.Signal(syscall.SIGTERM)
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if p.Signal(syscall.Signal(0)) != nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = p.Signal(syscall.SIGKILL)
}

// processLooksLike confirms pid is alive and its command line mentions needle
// (so a recycled PID of an unrelated program is never signalled).
func processLooksLike(pid int, needle string) bool {
	out, err := exec.Command("ps", "-p", itoa(pid), "-o", "command=").Output()
	return err == nil && strings.Contains(string(out), needle)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func (m *Manager) checkDisk(dir string, need int64) error {
	if need <= 0 {
		return nil
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return nil // cannot tell: do not block
	}
	free := int64(st.Bavail) * int64(st.Bsize)
	if free < need+(256<<20) {
		return &diskError{free: free, need: need}
	}
	return nil
}
