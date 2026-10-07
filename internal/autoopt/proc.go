package autoopt

import (
	"os/exec"
	"path/filepath"
)

// trackedProc is one child process MacroFlow started for a Service.
type trackedProc struct {
	name    string // binary base name only (never arguments -- they may hold secrets)
	pid     int
	grouped bool          // started in its own process group (unix)
	done    chan struct{} // closed once the leader exited and was reaped by Wait
}

// Run is a drop-in replacement for cmd.Run() for processes MacroFlow starts on
// behalf of this Service. It behaves exactly like cmd.Run() (Start + Wait, same
// error values, same context-timeout kill), and additionally:
//
//   - records the process in this Service's registry while it is alive, and
//   - on unix puts it in its own process group, so a timeout kills the whole
//     group (not just the leader) and any grandchildren still left after the
//     leader exited (orphans) are terminated: SIGTERM -> grace -> SIGKILL.
//
// Only the group this call created is ever signalled -- never an unrelated
// process. Nil-safe: with a nil/closed context it is just cmd.Run().
func (sc *ServiceContext) Run(cmd *exec.Cmd) error {
	if sc == nil || !sc.accepting() {
		return cmd.Run()
	}
	grouped := prepareCmd(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	tp := &trackedProc{name: filepath.Base(cmd.Path), pid: cmd.Process.Pid, grouped: grouped, done: make(chan struct{})}
	sc.mu.Lock()
	sc.procs[tp] = struct{}{}
	sc.procsRun++
	sc.mu.Unlock()

	err := cmd.Wait() // leader has exited and been reaped when this returns
	close(tp.done)
	sc.mu.Lock()
	delete(sc.procs, tp)
	sc.mu.Unlock()

	if grouped {
		sc.reapStragglers(tp)
	}
	return err
}

// reapStragglers terminates members of the process group that outlived the
// leader. It runs immediately after the leader was reaped (so the group id
// cannot have been recycled for an unrelated process yet) and does nothing when
// the group is already empty -- the normal case.
func (sc *ServiceContext) reapStragglers(tp *trackedProc) {
	found, err := terminateGroup(tp.pid, sc.opt.cfg.TermGrace)
	if !found {
		return
	}
	sc.mu.Lock()
	sc.straggled++
	if err != nil {
		sc.warns = append(sc.warns, "leftover child process of "+tp.name+": "+err.Error())
	}
	sc.mu.Unlock()
	sc.opt.logf("Cleanup warning: %s left child process(es) running after it finished; terminated", tp.name)
}
