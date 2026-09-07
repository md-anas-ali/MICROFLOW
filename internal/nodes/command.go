package nodes

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"microflow/internal/engine"
	"microflow/internal/expr"
	"microflow/internal/model"
)

// maxCaptureBytes bounds how much of a child process's stdout/stderr
// boundedWriter will actually buffer. FFmpeg in particular can emit
// megabytes of progress/frame lines on stderr for a long render; the
// old code captured all of it into a bytes.Buffer and only truncated
// the string afterward, so peak RSS during a render tracked the full
// (unbounded) output size, not the ~8KB actually kept. boundedWriter
// keeps writing past the cap cheap (just counts bytes) instead of
// allocating for them.
const maxCaptureBytes = 16 * 1024

// boundedWriter is an io.Writer that keeps at most maxCaptureBytes and
// silently drops (without allocating for) anything past that, while
// still reporting the total bytes seen via Truncated().
type boundedWriter struct {
	buf   []byte
	limit int
	total int
}

func newBoundedWriter(limit int) *boundedWriter {
	return &boundedWriter{limit: limit}
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	w.total += len(p)
	if room := w.limit - len(w.buf); room > 0 {
		if room > len(p) {
			room = len(p)
		}
		w.buf = append(w.buf, p[:room]...)
	}
	return len(p), nil
}

func (w *boundedWriter) String() string {
	s := string(w.buf)
	if w.total > len(w.buf) {
		s += "...[truncated]"
	}
	return s
}

// ExecuteCommandExecutor runs the workflow's shell steps: cleanup,
// TTS (Edge-TTS with silent-audio fallback), FFmpeg clip/concat/
// subtitle/thumbnail rendering, image/voice validation, QC frame
// extraction.
//
// Security posture (rule 8/22): this is deliberately NOT arbitrary
// command execution. `command` in node parameters must name a *logical*
// binary present in AllowedBinaries (populated from server config, e.g.
// {"ffmpeg": "/usr/bin/ffmpeg", "edge-tts": "/usr/local/bin/edge-tts",
// "python3": "/usr/bin/python3"}); MicroFlow resolves it to the real
// path itself rather than trusting a path from the workflow JSON. Args
// come from the workflow but are passed as an argv array (no shell
// interpolation), so there is no command-injection surface even though
// individual arguments may themselves be built from workflow
// expressions.
type ExecuteCommandExecutor struct {
	AllowedBinaries map[string]string
	ScratchRoot     string
	// Timeout bounds a single command's wall-clock run (FFmpeg renders
	// can legitimately take a while; default is generous but finite).
	Timeout time.Duration
}

func (e *ExecuteCommandExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	timeout := e.Timeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}

	var out []model.Item
	for _, it := range flatten(input) {
		exprCtx := rc.ExprContext(it.JSON)

		// legacyArgs holds the args re-synthesized from the legacy
		// "command" string's remaining tokens (see below). It's a
		// plain local variable -- deliberately NOT written back onto
		// node.Parameters, which is shared engine.RunContext-wide
		// state on the *model.Node loaded for this workflow. Two
		// concurrent executions of the same workflow (e.g. two
		// webhook calls, or a webhook racing a schedule -- see the
		// Runner.WithConcurrencyLimit fix that now also bounds those)
		// hitting this same executeCommand node used to read and
		// write that shared map with no synchronization: an
		// unsynchronized concurrent map write in Go is not just "may
		// pick up the wrong args", it can crash the whole process
		// with "fatal error: concurrent map writes". Keeping this
		// per-call and per-item local removes that shared-state
		// surface entirely.
		var legacyArgs []string

		logical := node.ParamString("binary", "")
		if logical == "" {
			// fall back to first token of the legacy n8n "command" string
			cmdTmpl := node.ParamString("command", "")
			resolved, err := expr.Eval(cmdTmpl, exprCtx)
			if err != nil {
				return nil, err
			}
			fields := strings.Fields(resolved)
			if len(fields) == 0 {
				return nil, fmt.Errorf("executeCommand %q: empty command", node.Name)
			}
			logical = filepath.Base(fields[0])
			// re-synthesize args from the remainder for the legacy path
			legacyArgs = fields[1:]
		}

		path, ok := e.AllowedBinaries[logical]
		if !ok {
			return nil, fmt.Errorf("executeCommand %q: binary %q is not in the allowlist (security rule: no unrestricted command execution)", node.Name, logical)
		}

		var args []string
		if rawArgs, ok := node.Parameters["args"].([]any); ok {
			for _, a := range rawArgs {
				s, _ := a.(string)
				resolved, err := expr.Eval(s, exprCtx)
				if err != nil {
					return nil, err
				}
				args = append(args, resolved)
			}
		} else if legacyArgs != nil {
			args = legacyArgs
		}

		// Back off first if we're already over the RAM ceiling (rule 19):
		// existing runs finish, but a new FFmpeg/TTS process is delayed a
		// few hundred ms at a time (bounded, never dropped) to give GC a
		// chance before adding another process's RSS on top.
		rc.MemGuard.WaitIfThrottled(ctx)
		rc.HeavyWorkGate <- struct{}{} // bound concurrent FFmpeg/TTS processes (rule 20)
		cctx, cancel := context.WithTimeout(ctx, timeout)
		cmd := exec.CommandContext(cctx, path, args...)
		cmd.Dir = rc.ScratchDir

		stdout := newBoundedWriter(maxCaptureBytes)
		stderr := newBoundedWriter(maxCaptureBytes)
		cmd.Stdout = stdout
		cmd.Stderr = stderr

		runErr := cmd.Run()
		cancel()
		<-rc.HeavyWorkGate

		exitCode := 0
		if cmd.ProcessState != nil {
			exitCode = cmd.ProcessState.ExitCode()
		}

		result := map[string]any{
			"stdout":   truncate(stdout.String(), 8000), // bounded logs (rule 18: limited logs)
			"stderr":   truncate(stderr.String(), 8000),
			"exitCode": exitCode,
		}
		// Bug fix: the previous check was `cctx.Err() != nil`, but cancel()
		// is called unconditionally right after cmd.Run() returns above --
		// that call itself makes cctx.Err() non-nil (context.Canceled) even
		// on a completely ordinary failure (bad binary args, a missing
		// working directory, etc.), so EVERY command failure was being
		// misreported as "timed out after 5m0s" regardless of the real
		// cause. context.DeadlineExceeded is the one value that latches in
		// permanently once the timeout actually fires and is never
		// overwritten by our own subsequent cancel() call, so it's the
		// only reliable signal that this was a real timeout.
		if runErr != nil && cctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("executeCommand %q: timed out after %s", node.Name, timeout)
		}
		if runErr != nil && exitCode == 0 {
			return nil, fmt.Errorf("executeCommand %q: %w", node.Name, runErr)
		}
		out = append(out, model.Item{JSON: result})
	}
	return model.NodeOutput{out}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...[truncated]"
}
