//go:build unix

package autoopt

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// pidAlive: running and not a zombie (an unreaped zombie is dead for our purposes).
func pidAlive(pid int) bool {
	if b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat"); err == nil {
		s := string(b)
		if i := strings.LastIndexByte(s, ')'); i >= 0 && i+2 < len(s) {
			return s[i+2] != 'Z' && s[i+2] != 'X'
		}
	}
	return syscall.Kill(pid, 0) == nil
}

func waitDead(pid int, d time.Duration) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if !pidAlive(pid) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return !pidAlive(pid)
}

// Test 1 (process side): Run behaves like cmd.Run for success and for a
// non-zero exit (same *exec.ExitError, same exit code).
func TestRunParityWithCmdRun(t *testing.T) {
	o, _ := newTestOpt(t)
	sc := o.Begin("e", "wf")
	defer sc.Release()

	var out bytes.Buffer
	c := exec.Command("sh", "-c", "echo hello")
	c.Stdout = &out
	if err := sc.Run(c); err != nil || strings.TrimSpace(out.String()) != "hello" {
		t.Fatalf("success path: err=%v out=%q", err, out.String())
	}
	c = exec.Command("sh", "-c", "exit 3")
	err := sc.Run(c)
	var ee *exec.ExitError
	if !errors.As(err, &ee) || c.ProcessState.ExitCode() != 3 {
		t.Fatalf("exit-code path: err=%v", err)
	}
	if err := sc.Run(exec.Command("/definitely/not/here")); err == nil {
		t.Fatal("start failure must be returned")
	}
}

// Test 3a: on timeout the WHOLE group dies (not only the leader) and the call
// returns promptly with the context's deadline, as with the default behaviour.
func TestRunTimeoutKillsWholeGroup(t *testing.T) {
	o, _ := newTestOpt(t)
	sc := o.Begin("e", "wf")
	defer sc.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	// leader shell + a grandchild that keeps running; grandchild's stdio is
	// detached so Wait is not held open by the pipe.
	c := exec.CommandContext(ctx, "sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $!; wait")
	var out bytes.Buffer
	c.Stdout = &out
	start := time.Now()
	err := sc.Run(c)
	if err == nil {
		t.Fatal("expected an error after the timeout kill")
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("deadline should have fired, ctx.Err=%v", ctx.Err())
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("Run did not return promptly after the timeout")
	}
	pid, perr := strconv.Atoi(strings.TrimSpace(out.String()))
	if perr != nil {
		t.Fatalf("could not read grandchild pid from %q", out.String())
	}
	if !waitDead(pid, 5*time.Second) {
		t.Fatalf("grandchild %d survived the timeout kill", pid)
	}
}

// Test 3b: a grandchild that outlives the finished command is detected, asked
// to stop (TERM), forced if needed (KILL), and verified gone -- while an
// unrelated process in a different group is left alone.
func TestRunReapsOrphanAndSparesUnrelated(t *testing.T) {
	// An unrelated process (its own group, not started via Run): must survive.
	other := exec.Command("sleep", "60")
	other.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := other.Start(); err != nil {
		t.Skipf("cannot start helper: %v", err)
	}
	defer func() { _ = other.Process.Kill(); _, _ = other.Process.Wait() }()

	o, ls := newTestOpt(t)
	sc := o.Begin("e", "wf")

	var out bytes.Buffer
	// Grandchild ignores SIGTERM so the TERM -> grace -> KILL fallback is exercised.
	c := exec.Command("sh", "-c", "(trap '' TERM; exec sleep 60) >/dev/null 2>&1 & echo $!; exit 0")
	c.Stdout = &out
	if err := sc.Run(c); err != nil {
		t.Fatalf("Run: %v", err)
	}
	pid, perr := strconv.Atoi(strings.TrimSpace(out.String()))
	if perr != nil {
		t.Fatalf("could not read grandchild pid from %q", out.String())
	}
	if !waitDead(pid, 8*time.Second) {
		t.Fatalf("orphaned grandchild %d was not terminated", pid)
	}
	if !pidAlive(other.Process.Pid) {
		t.Fatal("an unrelated process was terminated")
	}
	sc.Release()
	o.PostService(context.Background())
	if !strings.Contains(ls.text(), "left child process(es) running") {
		t.Fatalf("the orphan cleanup should be logged:\n%s", ls.text())
	}
}

// Test 3c: a process still registered when the Service is released (leader not
// yet reaped) is terminated by Release and verified.
func TestReleaseTerminatesRegisteredProcess(t *testing.T) {
	o, _ := newTestOpt(t)
	sc := o.Begin("e", "wf")

	c := exec.Command("sleep", "60")
	grouped := prepareCmd(c)
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	tp := &trackedProc{name: "sleep", pid: c.Process.Pid, grouped: grouped, done: make(chan struct{})}
	sc.mu.Lock()
	sc.procs[tp] = struct{}{}
	sc.procsRun++
	sc.mu.Unlock()
	go func() { _ = c.Wait(); close(tp.done) }() // plays the node's own Wait

	sc.Release()
	select {
	case <-tp.done:
	case <-time.After(8 * time.Second):
		t.Fatal("registered process was not terminated by Release")
	}
	if pidAlive(tp.pid) {
		t.Fatal("process still alive after Release")
	}
}
