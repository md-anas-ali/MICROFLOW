// Package autoopt is MacroFlow's Auto Optimize layer: a best-effort,
// additive cleanup / stabilisation step that surrounds each Service run.
//
// What it does (and does NOT do) -- no guarantees are made beyond this:
//
//   - It attempts to release resources that MacroFlow itself owns for one
//     Service run (child processes it started, HTTP response bodies it opened,
//     registered closers/timers/temp paths) and gives the next Service a
//     stabilisation window (the cooldown, wired in internal/scheduler).
//   - It does NOT promise that RAM or CPU drop to zero, that OS caches are
//     cleared, that HTTPS connections are fully reset, or that the system is in
//     a "fresh" state after the cooldown. The Go runtime, the allocator and the
//     OS decide how much memory is actually returned.
//
// Design rules:
//
//   - Ownership: every tracked resource belongs to exactly one ServiceContext
//     (one per execution). Release only ever touches that context's resources.
//   - Never a single point of failure: every phase is time-boxed, panics are
//     recovered, failures are logged as warnings and the next phase still runs.
//   - Additive: a nil *ServiceContext / nil *Optimizer is a valid no-op, and
//     MICROFLOW_AUTO_OPTIMIZE=0 turns the whole layer off.
//   - Leaf package (standard library only) so engine, nodes, runner and
//     scheduler can all import it without cycles.
package autoopt

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Phase is one step of the deterministic cleanup order.
type Phase int

const (
	PhaseStopNewWork       Phase = iota + 1 // 1. refuse new tracked work
	PhaseCancelBackground                   // 2. cancel Service-owned background tasks
	PhaseWaitGraceful                       // 3. give tracked processes a moment to end on their own
	PhaseTerminateProcs                     // 4. terminate leftover child processes (TERM -> KILL)
	PhaseCloseHTTPBodies                    // 5. close unclosed HTTP response bodies
	PhaseReleaseHTTP                        // 6. release HTTP client/session resources
	PhaseReleaseSockets                     // 7. release sockets/streams
	PhaseTimersListeners                    // 8. stop timers/listeners
	PhaseTempResources                      // 9. remove Service-owned temp paths
	PhaseReleaseMemory                      // 10. drop large temporary references
	PhaseRuntimeCleanup                     // 11. runtime cleanup / GC (global steps)
	PhaseVerify                             // 12. verify
)

func (p Phase) String() string {
	switch p {
	case PhaseStopNewWork:
		return "stop-new-work"
	case PhaseCancelBackground:
		return "cancel-background"
	case PhaseWaitGraceful:
		return "wait-graceful"
	case PhaseTerminateProcs:
		return "terminate-processes"
	case PhaseCloseHTTPBodies:
		return "close-http-bodies"
	case PhaseReleaseHTTP:
		return "release-http"
	case PhaseReleaseSockets:
		return "release-sockets"
	case PhaseTimersListeners:
		return "timers-listeners"
	case PhaseTempResources:
		return "temp-resources"
	case PhaseReleaseMemory:
		return "release-memory"
	case PhaseRuntimeCleanup:
		return "runtime-cleanup"
	case PhaseVerify:
		return "verify"
	}
	return "phase-" + strconv.Itoa(int(p))
}

// Config tunes the optimizer. Zero values fall back to safe defaults.
type Config struct {
	Enabled bool
	// PhaseTimeout bounds each cleanup phase / global step (default 10s).
	PhaseTimeout time.Duration
	// TermGrace is how long a child process (group) gets after SIGTERM before
	// SIGKILL (default 3s).
	TermGrace time.Duration
	// TempRoots are the only directories under which TrackTemp paths may be
	// removed (a path outside them is refused, never deleted).
	TempRoots []string
	// Logf receives every log line (default: log.Printf).
	Logf func(format string, args ...any)
}

// ConfigFromEnv reads MICROFLOW_AUTO_OPTIMIZE (default on; "0"/"false"/"off"
// disables), MICROFLOW_AUTO_OPTIMIZE_PHASE_TIMEOUT_SECONDS (1..300) and
// MICROFLOW_AUTO_OPTIMIZE_TERM_GRACE_SECONDS (1..60).
func ConfigFromEnv() Config {
	c := Config{Enabled: true, PhaseTimeout: 10 * time.Second, TermGrace: 3 * time.Second}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("MICROFLOW_AUTO_OPTIMIZE"))) {
	case "0", "false", "off", "no":
		c.Enabled = false
	}
	if n, err := strconv.Atoi(os.Getenv("MICROFLOW_AUTO_OPTIMIZE_PHASE_TIMEOUT_SECONDS")); err == nil && n >= 1 && n <= 300 {
		c.PhaseTimeout = time.Duration(n) * time.Second
	}
	if n, err := strconv.Atoi(os.Getenv("MICROFLOW_AUTO_OPTIMIZE_TERM_GRACE_SECONDS")); err == nil && n >= 1 && n <= 60 {
		c.TermGrace = time.Duration(n) * time.Second
	}
	return c
}

// Step is a global (not per-Service) cleanup action, e.g. the runner's own
// scratch cleanup. Registered by the application, run in Phase order.
type step struct {
	phase   Phase
	name    string
	timeout time.Duration // 0 = Config.PhaseTimeout
	fn      func(ctx context.Context) error
}

type verifier struct {
	name string
	fn   func() error
}

// ReleaseReport is what one ServiceContext.Release found/did. It is kept (bounded)
// and summarised in the next PostService log block.
type ReleaseReport struct {
	ID       string
	Label    string
	Profile  string // "process-heavy" | "http-heavy" | "light"
	Warnings []string

	ProcsTracked   int
	ProcsLeftover  int
	BodiesTracked  int
	BodiesLeftover int
	ClosersClosed  int
	StopsRun       int
	TempRemoved    int
}

// Optimizer owns the registry of live ServiceContexts and the global steps.
type Optimizer struct {
	cfg Config

	mu        sync.Mutex
	live      map[string]*ServiceContext
	steps     []step
	verifiers []verifier
	health    []verifier
	pending   []ReleaseReport // released contexts not yet summarised in a log block
}

// New builds an Optimizer. A disabled optimizer is valid: Begin returns nil and
// PostService / PreServiceCheck do nothing.
func New(cfg Config) *Optimizer {
	if cfg.PhaseTimeout <= 0 {
		cfg.PhaseTimeout = 10 * time.Second
	}
	if cfg.TermGrace <= 0 {
		cfg.TermGrace = 3 * time.Second
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	return &Optimizer{cfg: cfg, live: map[string]*ServiceContext{}}
}

// Enabled reports whether Auto Optimize is active. Safe on nil.
func (o *Optimizer) Enabled() bool { return o != nil && o.cfg.Enabled }

func (o *Optimizer) logf(format string, args ...any) {
	if o == nil || o.cfg.Logf == nil {
		return
	}
	o.cfg.Logf("[AutoOptimize] "+format, args...)
}

// Logf is the exported log helper for hooks wired outside this package (the
// scheduler's cooldown hooks). Prefix is added here.
func (o *Optimizer) Logf(format string, args ...any) { o.logf(format, args...) }

// AddStep registers a global cleanup step that PostService runs in phase
// order. Steps registered for the same phase run in registration order.
func (o *Optimizer) AddStep(phase Phase, name string, fn func(ctx context.Context) error) {
	o.AddStepWithTimeout(phase, name, 0, fn)
}

// AddStepWithTimeout is AddStep with its own time box (0 = Config.PhaseTimeout).
func (o *Optimizer) AddStepWithTimeout(phase Phase, name string, timeout time.Duration, fn func(ctx context.Context) error) {
	if o == nil || fn == nil {
		return
	}
	o.mu.Lock()
	o.steps = append(o.steps, step{phase: phase, name: name, timeout: timeout, fn: fn})
	o.mu.Unlock()
}

// AddVerifier registers a cheap check run at the end of PostService; a returned
// error is logged as a cleanup warning.
func (o *Optimizer) AddVerifier(name string, fn func() error) {
	if o == nil || fn == nil {
		return
	}
	o.mu.Lock()
	o.verifiers = append(o.verifiers, verifier{name: name, fn: fn})
	o.mu.Unlock()
}

// AddHealthCheck registers a check for PreServiceCheck (internal-state
// consistency). It must be cheap and side-effect free.
func (o *Optimizer) AddHealthCheck(name string, fn func() error) {
	if o == nil || fn == nil {
		return
	}
	o.mu.Lock()
	o.health = append(o.health, verifier{name: name, fn: fn})
	o.mu.Unlock()
}

// Begin opens the ServiceContext that owns every resource of one execution.
// id must be unique per execution. Returns nil when disabled (nil is a no-op).
func (o *Optimizer) Begin(id, label string) *ServiceContext {
	if !o.Enabled() {
		return nil
	}
	sc := &ServiceContext{opt: o, ID: id, Label: label, started: time.Now(), procs: map[*trackedProc]struct{}{}}
	o.mu.Lock()
	o.live[id] = sc
	o.mu.Unlock()
	return sc
}

// runBounded runs fn with a hard time box and panic recovery. It returns an
// error when fn failed, panicked or did not finish in time (fn's goroutine may
// then still be running; we simply stop waiting for it).
func (o *Optimizer) runBounded(name string, timeout time.Duration, fn func() error) (err error) {
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic: %v", r)
			}
		}()
		done <- fn()
	}()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case err = <-done:
		return err
	case <-t.C:
		return fmt.Errorf("%s: timed out after %s (continuing)", name, timeout)
	}
}

// PostService runs after a Service finished (success, error, timeout, cancel or
// panic). It first releases any context that is still live (safety net -- the
// per-execution Release normally already ran), then the global steps in phase
// order, then verification. It never panics and never blocks beyond
// (#steps x PhaseTimeout); a cancelled ctx skips the remaining steps.
func (o *Optimizer) PostService(ctx context.Context) {
	if !o.Enabled() {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			o.logf("Cleanup failed: unexpected panic: %v", r)
		}
	}()

	// Safety net: contexts the execution's own finally did not release.
	o.mu.Lock()
	stale := make([]*ServiceContext, 0, len(o.live))
	for _, sc := range o.live {
		stale = append(stale, sc)
	}
	o.mu.Unlock()
	for _, sc := range stale {
		o.logf("Cleanup warning: execution %s was still registered after its Service ended; releasing it", sc.ID)
		sc.Release()
	}

	o.mu.Lock()
	reports := o.pending
	o.pending = nil
	steps := append([]step(nil), o.steps...)
	verifiers := append([]verifier(nil), o.verifiers...)
	o.mu.Unlock()

	if len(reports) == 0 {
		o.logf("Service completed")
	}
	for _, r := range reports {
		o.logf("Service completed (execution=%s workflow=%s profile=%s)", r.ID, r.Label, r.Profile)
	}
	o.logf("Starting cleanup")

	var procLeft, bodyLeft, closed, stops, temp int
	var warns []string
	for _, r := range reports {
		procLeft += r.ProcsLeftover
		bodyLeft += r.BodiesLeftover
		closed += r.ClosersClosed
		stops += r.StopsRun
		temp += r.TempRemoved
		warns = append(warns, r.Warnings...)
	}
	for _, w := range warns {
		o.logf("Cleanup warning: %s", w)
	}
	o.logf("Child processes checked (leftover terminated: %d)", procLeft)
	o.logf("HTTP resources released (unclosed bodies closed: %d)", bodyLeft)
	o.logf("Network resources released (closers released: %d)", closed)
	o.logf("Timers/background tasks checked (stopped: %d)", stops)
	o.logf("Temporary resources cleaned (Service-owned paths removed: %d)", temp)

	sort.SliceStable(steps, func(i, j int) bool { return steps[i].phase < steps[j].phase })
	for _, st := range steps {
		if ctx.Err() != nil {
			o.logf("Cleanup warning: %s skipped (cleanup budget exhausted)", st.name)
			continue
		}
		limit := st.timeout
		if limit <= 0 {
			limit = o.cfg.PhaseTimeout
		}
		if err := o.runBounded(st.name, limit, func() error { return st.fn(ctx) }); err != nil {
			o.logf("Cleanup warning: %s: %v", st.name, err)
		}
	}

	failed := 0
	for _, v := range verifiers {
		if err := o.runBounded(v.name, o.cfg.PhaseTimeout, v.fn); err != nil {
			failed++
			o.logf("Cleanup warning: verify %s: %v", v.name, err)
		}
	}
	if failed == 0 && len(warns) == 0 {
		o.logf("Cleanup verification completed")
	} else {
		o.logf("Cleanup verification completed with %d warning(s)", failed+len(warns))
	}
}

// PreServiceCheck verifies MacroFlow's own state right before the next Service
// starts and performs safe recovery (releasing contexts left registered by a
// previous Service). It NEVER blocks the Service: an unrecoverable problem is
// logged and the Service still starts, matching MacroFlow's existing policy of
// not refusing scheduled work.
func (o *Optimizer) PreServiceCheck(ctx context.Context) {
	if !o.Enabled() {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			o.logf("Cleanup failed: pre-service health check panic: %v", r)
		}
	}()
	issues := 0

	o.mu.Lock()
	stale := make([]*ServiceContext, 0, len(o.live))
	for _, sc := range o.live {
		stale = append(stale, sc)
	}
	o.mu.Unlock()
	for _, sc := range stale {
		issues++
		o.logf("Cleanup warning: stale Service resources from execution %s found; recovering", sc.ID)
		sc.Release()
	}
	o.mu.Lock()
	o.pending = nil // recovered above; nothing more to report
	checks := append([]verifier(nil), o.health...)
	o.mu.Unlock()

	for _, c := range checks {
		if ctx.Err() != nil {
			break
		}
		if err := o.runBounded(c.name, o.cfg.PhaseTimeout, c.fn); err != nil {
			issues++
			o.logf("Cleanup warning: health %s: %v", c.name, err)
		}
	}
	if issues == 0 {
		o.logf("Pre-service health check passed")
	} else {
		o.logf("Pre-service health check passed with %d warning(s); proceeding", issues)
	}
	o.logf("Next service allowed")
}

func (o *Optimizer) forget(id string, rep ReleaseReport) {
	o.mu.Lock()
	delete(o.live, id)
	o.pending = append(o.pending, rep)
	if len(o.pending) > 16 {
		o.pending = append([]ReleaseReport(nil), o.pending[len(o.pending)-16:]...)
	}
	o.mu.Unlock()
}

// LiveContexts is for tests/diagnostics: number of unreleased contexts.
func (o *Optimizer) LiveContexts() int {
	if o == nil {
		return 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.live)
}

// ---------------------------------------------------------------------------
// ServiceContext
// ---------------------------------------------------------------------------

type namedCloser struct {
	name string
	c    io.Closer
}

type namedStop struct {
	name string
	fn   func()
}

// ServiceContext is the ownership record for one execution's resources.
// All methods are safe on a nil receiver (no-op) and for concurrent use.
type ServiceContext struct {
	opt     *Optimizer
	ID      string
	Label   string
	started time.Time

	mu       sync.Mutex
	closed   bool // set by Release: no new tracked work is accepted
	released bool

	procs    map[*trackedProc]struct{}
	procsRun int
	bodies   map[*trackedBody]struct{}
	bodiesN  int
	cancels  []namedStop // background-task cancel funcs
	stops    []namedStop // timers / listeners
	closers  []namedCloser
	temps    []string
	releases []namedStop // large temporary references

	straggled int      // leftover process groups reaped right after a node's command ended
	warns     []string // warnings recorded outside Release (e.g. by Run)
}

func (sc *ServiceContext) accepting() bool {
	if sc == nil {
		return false
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return !sc.closed
}

// TrackCancel registers a cancel func for a Service-owned background task.
func (sc *ServiceContext) TrackCancel(name string, cancel func()) {
	if sc == nil || cancel == nil {
		return
	}
	sc.mu.Lock()
	if !sc.closed {
		sc.cancels = append(sc.cancels, namedStop{name, cancel})
	}
	sc.mu.Unlock()
}

// TrackStop registers a timer/listener/callback stopper.
func (sc *ServiceContext) TrackStop(name string, stop func()) {
	if sc == nil || stop == nil {
		return
	}
	sc.mu.Lock()
	if !sc.closed {
		sc.stops = append(sc.stops, namedStop{name, stop})
	}
	sc.mu.Unlock()
}

// TrackCloser registers a socket/stream/session closer. Returns an untrack func
// to call once the resource was closed normally (idempotent).
func (sc *ServiceContext) TrackCloser(name string, c io.Closer) (untrack func()) {
	if sc == nil || c == nil {
		return func() {}
	}
	nc := namedCloser{name: name, c: c}
	sc.mu.Lock()
	if sc.closed {
		sc.mu.Unlock()
		return func() {}
	}
	sc.closers = append(sc.closers, nc)
	sc.mu.Unlock()
	return func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		for i := range sc.closers {
			if sc.closers[i].c == c {
				sc.closers = append(sc.closers[:i], sc.closers[i+1:]...)
				return
			}
		}
	}
}

// TrackTemp registers a Service-owned temp file/dir. It is removed at Release
// only if it lies strictly inside one of Config.TempRoots.
func (sc *ServiceContext) TrackTemp(path string) {
	if sc == nil || path == "" {
		return
	}
	sc.mu.Lock()
	if !sc.closed {
		sc.temps = append(sc.temps, path)
	}
	sc.mu.Unlock()
}

// TrackRelease registers a func that drops a large temporary reference.
func (sc *ServiceContext) TrackRelease(name string, fn func()) {
	if sc == nil || fn == nil {
		return
	}
	sc.mu.Lock()
	if !sc.closed {
		sc.releases = append(sc.releases, namedStop{name, fn})
	}
	sc.mu.Unlock()
}

// trackedBody wraps an HTTP response body so an unclosed body can be found and
// closed at Release. Normal Close() untracks it; reads are passed through
// untouched, so request behaviour is unchanged.
type trackedBody struct {
	io.ReadCloser
	sc   *ServiceContext
	once sync.Once
}

func (b *trackedBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() {
		b.sc.mu.Lock()
		delete(b.sc.bodies, b)
		b.sc.mu.Unlock()
	})
	return err
}

// WrapBody tracks an HTTP response body owned by this Service. The returned
// reader behaves exactly like body. Nil-safe: returns body unchanged.
func (sc *ServiceContext) WrapBody(body io.ReadCloser) io.ReadCloser {
	if sc == nil || body == nil {
		return body
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed {
		return body
	}
	if sc.bodies == nil {
		sc.bodies = map[*trackedBody]struct{}{}
	}
	tb := &trackedBody{ReadCloser: body, sc: sc}
	sc.bodies[tb] = struct{}{}
	sc.bodiesN++
	return tb
}

func (sc *ServiceContext) profile() string {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	switch {
	case sc.procsRun >= 3 && sc.procsRun >= sc.bodiesN:
		return "process-heavy"
	case sc.bodiesN >= 10:
		return "http-heavy"
	case sc.procsRun > 0:
		return "process-heavy"
	case sc.bodiesN > 0:
		return "http-heavy"
	}
	return "light"
}

// Release runs the deterministic cleanup order for this context's own
// resources. Idempotent; safe if called from a deferred function.
func (sc *ServiceContext) Release() {
	if sc == nil {
		return
	}
	sc.mu.Lock()
	if sc.released {
		sc.mu.Unlock()
		return
	}
	sc.released = true
	sc.closed = true // phase 1: stop new work
	sc.mu.Unlock()

	o := sc.opt
	rep := ReleaseReport{ID: sc.ID, Label: sc.Label, Profile: sc.profile()}
	timeout := o.cfg.PhaseTimeout
	// Adaptive: the dominant resource kind gets a doubled time box.
	procT, httpT := timeout, timeout
	switch rep.Profile {
	case "process-heavy":
		procT = 2 * timeout
	case "http-heavy":
		httpT = 2 * timeout
	}
	// Phases run time-boxed in goroutines that may outlive a timeout, so every
	// write to rep goes through repMu (the final copy is taken under it too).
	var repMu sync.Mutex
	mut := func(f func(r *ReleaseReport)) { repMu.Lock(); f(&rep); repMu.Unlock() }
	warn := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		mut(func(r *ReleaseReport) { r.Warnings = append(r.Warnings, msg) })
	}
	phase := func(p Phase, t time.Duration, fn func() error) {
		if err := o.runBounded(p.String(), t, fn); err != nil {
			warn("%s: %v", p, err)
		}
	}

	// 2. cancel Service-owned background tasks
	sc.mu.Lock()
	cancels := sc.cancels
	sc.cancels = nil
	sc.mu.Unlock()
	if len(cancels) > 0 {
		phase(PhaseCancelBackground, timeout, func() error {
			for _, c := range cancels {
				c.fn()
			}
			return nil
		})
		mut(func(r *ReleaseReport) { r.StopsRun += len(cancels) })
	}

	// 3+4. processes: brief wait, then TERM -> KILL, per tracked process
	sc.mu.Lock()
	procs := make([]*trackedProc, 0, len(sc.procs))
	for p := range sc.procs {
		procs = append(procs, p)
	}
	mut(func(r *ReleaseReport) {
		r.ProcsTracked = sc.procsRun
		r.ProcsLeftover += sc.straggled
		r.Warnings = append(r.Warnings, sc.warns...)
	})
	sc.mu.Unlock()
	if len(procs) > 0 {
		phase(PhaseWaitGraceful, o.cfg.TermGrace+time.Second, func() error {
			deadline := time.After(o.cfg.TermGrace)
			for _, p := range procs {
				select {
				case <-p.done:
				case <-deadline:
					return nil
				}
			}
			return nil
		})
		phase(PhaseTerminateProcs, procT, func() error {
			for _, p := range procs {
				select {
				case <-p.done:
					continue // ended by itself during the grace period
				default:
				}
				mut(func(r *ReleaseReport) { r.ProcsLeftover++ })
				if err := terminateTracked(p, o.cfg.TermGrace); err != nil {
					warn("process %s (pid %d): %v", p.name, p.pid, err)
				}
			}
			return nil
		})
	}

	// 5. unclosed HTTP bodies
	sc.mu.Lock()
	bodies := make([]*trackedBody, 0, len(sc.bodies))
	for b := range sc.bodies {
		bodies = append(bodies, b)
	}
	sc.mu.Unlock()
	if len(bodies) > 0 {
		mut(func(r *ReleaseReport) { r.BodiesTracked = len(bodies) })
		phase(PhaseCloseHTTPBodies, httpT, func() error {
			for _, b := range bodies {
				_ = b.Close()
				mut(func(r *ReleaseReport) { r.BodiesLeftover++ })
			}
			return nil
		})
	}

	// 6+7. closers (HTTP sessions / sockets / streams)
	sc.mu.Lock()
	closers := sc.closers
	sc.closers = nil
	sc.mu.Unlock()
	if len(closers) > 0 {
		phase(PhaseReleaseSockets, httpT, func() error {
			var first error
			for _, c := range closers {
				if err := c.c.Close(); err != nil && first == nil {
					first = fmt.Errorf("close %s: %w", c.name, err)
				}
				mut(func(r *ReleaseReport) { r.ClosersClosed++ })
			}
			return first
		})
	}

	// 8. timers / listeners
	sc.mu.Lock()
	stops := sc.stops
	sc.stops = nil
	sc.mu.Unlock()
	if len(stops) > 0 {
		phase(PhaseTimersListeners, timeout, func() error {
			for _, s := range stops {
				s.fn()
			}
			return nil
		})
		mut(func(r *ReleaseReport) { r.StopsRun += len(stops) })
	}

	// 9. Service-owned temp paths (only strictly inside the configured roots)
	sc.mu.Lock()
	temps := sc.temps
	sc.temps = nil
	sc.mu.Unlock()
	if len(temps) > 0 {
		phase(PhaseTempResources, timeout, func() error {
			var first error
			for _, p := range temps {
				if !withinRoots(p, o.cfg.TempRoots) {
					if first == nil {
						first = fmt.Errorf("refusing to remove %q: outside the allowed temp roots", filepath.Base(p))
					}
					continue
				}
				if err := os.RemoveAll(p); err != nil && first == nil {
					first = err
					continue
				}
				mut(func(r *ReleaseReport) { r.TempRemoved++ })
			}
			return first
		})
	}

	// 10. drop large temporary references
	sc.mu.Lock()
	rels := sc.releases
	sc.releases = nil
	sc.mu.Unlock()
	if len(rels) > 0 {
		phase(PhaseReleaseMemory, timeout, func() error {
			for _, r := range rels {
				r.fn()
			}
			return nil
		})
	}

	// 12. verify this context's own resources
	phase(PhaseVerify, timeout, func() error {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		for p := range sc.procs {
			if processAlive(p) {
				return fmt.Errorf("process %s (pid %d) still alive after termination", p.name, p.pid)
			}
		}
		return nil
	})
	sc.mu.Lock()
	sc.procs = map[*trackedProc]struct{}{}
	sc.bodies = nil
	sc.mu.Unlock()

	repMu.Lock()
	final := rep
	final.Warnings = append([]string(nil), rep.Warnings...)
	repMu.Unlock()
	o.forget(sc.ID, final)
}

// withinRoots reports whether path lies strictly inside one of roots.
func withinRoots(path string, roots []string) bool {
	p := filepath.Clean(path)
	for _, r := range roots {
		if r == "" {
			continue
		}
		rr := filepath.Clean(r)
		if p != rr && strings.HasPrefix(p, rr+string(filepath.Separator)) {
			return true
		}
	}
	return false
}
