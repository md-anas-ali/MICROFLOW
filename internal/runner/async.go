package runner

// Manager runs workflow executions asynchronously: Start persists a
// StatusQueued execution and returns its id immediately (never blocks
// on the run itself -- spec section M), while a bounded worker pool
// (section Q: "bounded execution queue... bounded worker pool") does
// the actual engine.Run work in the background via Runner.runOnce, the
// same code path RunFromNode uses synchronously for scheduler/webhook
// triggers. Per-execution progress is delivered through a bounded,
// non-blocking broadcaster (section N/rule 10: SSE must never grow
// memory unboundedly for a slow client) fed by RunContext.OnNodeRun.
//
// IMPLEMENTED here: async accept/return, bounded concurrency +
// bounded queue with backpressure, live event broadcast with
// drop-oldest slow-subscriber handling and a small replay buffer for
// reconnect, cancellation (queued or running), bounded in-memory
// execution registry with time-based eviction (falls back to the
// store's durable copy after eviction -- see internal/api).
//
// NOT YET VERIFIED (can't be, without a Go toolchain in this
// sandbox -- see STATUS.md): behavior under `go test -race`, actual
// cancellation propagation all the way into already-running
// executeCommand/HTTP child processes (that plumbing predates this
// file; Cancel here only guarantees the *engine loop* observes
// ctx.Done() before/between node executions, which is what
// engine.Run already checked), and real memory behavior under a
// constrained container.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"microflow/internal/model"
)

type EventType string

const (
	EventExecutionCreated   EventType = "execution.created"
	EventExecutionStarted   EventType = "execution.started"
	EventNodeCompleted      EventType = "node.completed"
	EventNodeFailed         EventType = "node.failed"
	EventExecutionWaiting   EventType = "execution.waiting"
	EventExecutionCompleted EventType = "execution.completed"
	EventExecutionFailed    EventType = "execution.failed"
	EventExecutionCancelled EventType = "execution.cancelled"
)

// Event is one SSE message. Metadata only -- Node embeds the same
// NodeRunResult the execution panel already renders (input/output are
// n8n Items: small JSON plus binary *references*, never raw media
// bytes -- see internal/nodes/binary.go), never a raw file payload
// (rule: "NEVER send video/audio/image/binary data through SSE").
type Event struct {
	Type        EventType             `json:"type"`
	ExecutionID string                `json:"executionId"`
	Time        time.Time             `json:"time"`
	Status      model.ExecutionStatus `json:"status,omitempty"`
	Node        *model.NodeRunResult  `json:"node,omitempty"`
	Error       string                `json:"error,omitempty"`
}

var (
	// ErrQueueFull is returned by Start when the bounded queue is
	// already at capacity -- the caller (API layer) should map this to
	// HTTP 429, not silently accept unbounded work (rule 19).
	ErrQueueFull = errors.New("runner: execution queue is full, try again shortly")
	// ErrExecutionNotFound is returned by Get/Cancel/Subscribe for an
	// execution id the Manager has no in-memory record of -- either it
	// never existed, or it finished long enough ago to have been
	// evicted (see finishedRetention); callers should fall back to the
	// durable store for the latter case.
	ErrExecutionNotFound = errors.New("runner: execution not found")
)

const (
	subscriberBufSize = 32 // per-SSE-client bounded event buffer (rule 10)
	replayBufSize     = 20 // small bounded replay window for reconnect
	// finishedRetention bounds how long a finished execution's
	// in-memory record (and its event replay buffer) is kept before
	// eviction, so the registry can't grow without bound over a long
	// server lifetime (rule 13). The durable copy in Postgres (written
	// by runOnce's SaveExecution) is unaffected -- GET falls back to it.
	finishedRetention = 10 * time.Minute
)

// broadcaster fans one execution's events out to any number of SSE
// subscribers without ever blocking the publisher (the engine's
// OnNodeRun callback, which MUST return quickly) and without ever
// growing a slow subscriber's buffer unboundedly: a full subscriber
// channel has its oldest queued event dropped to make room for the
// newest one, rather than blocking or growing (rule 10).
type broadcaster struct {
	mu     sync.Mutex
	subs   map[int]chan Event
	nextID int
	replay []Event
	closed bool
}

func newBroadcaster() *broadcaster { return &broadcaster{subs: map[int]chan Event{}} }

func (b *broadcaster) publish(ev Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.replay = append(b.replay, ev)
	if extra := len(b.replay) - replayBufSize; extra > 0 {
		b.replay = b.replay[extra:]
	}
	for _, ch := range b.subs {
		select {
		case ch <- ev:
		default:
			// Slow/stalled subscriber: drop the oldest queued event to
			// make room, then retry once, non-blocking either way.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- ev:
			default:
			}
		}
	}
}

func (b *broadcaster) subscribe() (id int, ch chan Event, replay []Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id = b.nextID
	b.nextID++
	ch = make(chan Event, subscriberBufSize)
	b.subs[id] = ch
	replay = append([]Event(nil), b.replay...)
	return id, ch, replay
}

func (b *broadcaster) unsubscribe(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.subs[id]; ok {
		delete(b.subs, id)
		close(ch)
	}
}

// close closes every current subscriber channel (signaling "no more
// events, execution finished") and marks the broadcaster closed so a
// subscriber connecting after the terminal event still gets a clean
// end-of-stream instead of hanging forever.
func (b *broadcaster) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for id, ch := range b.subs {
		delete(b.subs, id)
		close(ch)
	}
}

// runState is one execution's live, lock-guarded record: the
// in-memory model.Execution the API's GET endpoint reads (via
// snapshot, which always returns a defensive copy -- readers must
// never see a torn/partially-updated NodeRuns slice while a run is in
// progress), its cancel func, and its broadcaster.
type runState struct {
	mu     sync.Mutex
	ex     *model.Execution
	cancel context.CancelFunc
	bcast  *broadcaster
	done   bool
}

func (rs *runState) snapshot() *model.Execution {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.snapshotLocked()
}

func (rs *runState) snapshotLocked() *model.Execution {
	cp := *rs.ex
	cp.NodeRuns = append([]model.NodeRunResult(nil), rs.ex.NodeRuns...)
	return &cp
}

// Manager runs executions asynchronously with bounded concurrency
// (sem) and a bounded accepted-but-not-yet-finished count (queued vs
// maxQueued) -- see Start. Construct with NewManager; safe for
// concurrent use.
type Manager struct {
	r         *Runner
	sem       chan struct{}
	queued    int32
	maxQueued int32

	mu     sync.Mutex
	states map[string]*runState
	all    *broadcaster // process-wide stream for the live executions monitor
}

// NewManager wires an async Manager around an existing *Runner (same
// Workflows/Execs/Engine/ScratchRoot/Timeout it already has -- this
// package deliberately doesn't duplicate that wiring). maxConcurrent
// bounds simultaneous engine.Run executions (independent of, and on
// top of, Engine.MaxConcurrentHeavy's per-run FFmpeg/TTS/HTTP cap);
// maxQueued bounds how many accepted-but-not-yet-finished executions
// (running + waiting in line for a worker) may exist at once before
// Start starts rejecting new work with ErrQueueFull (rule 19:
// "reject excess work safely if necessary, NEVER intentionally OOM").
//
// Manager shares r's execution semaphore (see Runner.WithConcurrencyLimit)
// rather than keeping a private one, so maxConcurrent is a real
// process-wide cap: scheduler/webhook runs going straight through
// r.RunFromNode compete for the exact same slots as executions
// started here. If the caller hasn't already called
// r.WithConcurrencyLimit (e.g. existing callers built before this
// fix), NewManager sets it up using maxConcurrent so behavior is
// unchanged for anyone who only uses the async path.
func NewManager(r *Runner, maxConcurrent, maxQueued int) *Manager {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	if maxQueued < maxConcurrent {
		maxQueued = maxConcurrent
	}
	if r.sem == nil {
		r.WithConcurrencyLimit(maxConcurrent)
	}
	return &Manager{
		r:         r,
		sem:       r.sem,
		maxQueued: int32(maxQueued),
		states:    map[string]*runState{},
		all:       newBroadcaster(),
	}
}

// Start validates the workflow/start node synchronously (so a bad
// request still gets an immediate 4xx, not a queued execution that's
// doomed to fail), persists a StatusQueued execution, and returns its
// id -- all before the actual run has started. The run itself
// proceeds in a background goroutine bounded by m.sem. This is the
// spec's async execute contract (section M): the HTTP request that
// calls Start MUST NOT wait for workflow completion.
func (m *Manager) Start(ctx context.Context, workflowID, startNode, mode string, seed model.NodeOutput) (string, error) {
	wf, err := m.r.Workflows.LoadWorkflow(ctx, workflowID)
	if err != nil {
		return "", fmt.Errorf("runner: load workflow %q: %w", workflowID, err)
	}
	if startNode == "" {
		startNode = FirstTriggerNode(wf)
	}
	if startNode == "" {
		return "", errors.New("runner: no trigger node found and no startNode given")
	}
	if _, ok := wf.Nodes[startNode]; !ok {
		return "", fmt.Errorf("runner: workflow %q has no node named %q", workflowID, startNode)
	}

	if n := atomic.AddInt32(&m.queued, 1); n > m.maxQueued {
		atomic.AddInt32(&m.queued, -1)
		return "", ErrQueueFull
	}

	execID := newID()
	// Deliberately context.Background(), not the inbound HTTP request's
	// ctx: that request context is cancelled the moment the handler
	// returns 202, which must NOT cancel a run that's only just been
	// queued (section M: "Client disconnect MUST NOT cancel the
	// workflow automatically"). r.Timeout is still the run's own outer
	// bound; explicit Cancel() is the only other way this fires early.
	runCtx, cancel := context.WithTimeout(context.Background(), m.r.Timeout)

	rs := &runState{
		ex: &model.Execution{
			ID:         execID,
			WorkflowID: wf.ID,
			Mode:       mode,
			Status:     model.StatusQueued,
			StartedAt:  time.Now(),
		},
		cancel: cancel,
		bcast:  newBroadcaster(),
	}

	m.mu.Lock()
	m.states[execID] = rs
	m.mu.Unlock()

	if saveErr := m.r.Execs.SaveExecution(context.Background(), rs.snapshot()); saveErr != nil {
		log.Printf("runner: manager: failed to persist queued execution %s: %v", execID, saveErr)
	}
	m.publish(rs, Event{Type: EventExecutionCreated, ExecutionID: execID, Time: time.Now(), Status: model.StatusQueued})

	go m.runJob(runCtx, wf, execID, startNode, mode, seed, rs)

	return execID, nil
}

func (m *Manager) runJob(ctx context.Context, wf *model.Workflow, execID, startNode, mode string, seed model.NodeOutput, rs *runState) {
	defer rs.cancel()
	defer atomic.AddInt32(&m.queued, -1)

	select {
	case m.sem <- struct{}{}:
	case <-ctx.Done():
		m.finishCancelled(execID, rs, "cancelled while queued")
		return
	}
	defer func() { <-m.sem }()

	// Re-check: Cancel() may have fired while this job was waiting for
	// a worker slot.
	select {
	case <-ctx.Done():
		m.finishCancelled(execID, rs, "cancelled while queued")
		return
	default:
	}

	rs.mu.Lock()
	rs.ex.Status = model.StatusRunning
	rs.mu.Unlock()
	m.publish(rs, Event{Type: EventExecutionStarted, ExecutionID: execID, Time: time.Now(), Status: model.StatusRunning})

	onNodeRun := func(result model.NodeRunResult) {
		rs.mu.Lock()
		rs.ex.NodeRuns = append(rs.ex.NodeRuns, result)
		if extra := len(rs.ex.NodeRuns) - 500; extra > 0 {
			rs.ex.NodeRuns = rs.ex.NodeRuns[extra:]
		}
		rs.mu.Unlock()

		evType := EventNodeCompleted
		if result.Status == model.StatusError {
			evType = EventNodeFailed
		}
		// result is this closure's own parameter (a fresh copy per
		// call, not a shared loop variable), so &result is safe to hand
		// to publish/replay without a capture bug.
		m.publish(rs, Event{Type: evType, ExecutionID: execID, Time: time.Now(), Node: &result, Status: result.Status})
		if result.Status == model.StatusWaiting {
			m.publish(rs, Event{Type: EventExecutionWaiting, ExecutionID: execID, Time: time.Now(), Status: model.StatusWaiting})
		}
	}

	final, runErr := m.r.runOnce(ctx, wf, execID, startNode, mode, seed, onNodeRun)

	rs.mu.Lock()
	if final != nil {
		rs.ex = final
	} else {
		rs.ex.Status = model.StatusError
		if runErr != nil {
			rs.ex.Error = runErr.Error()
		}
		now := time.Now()
		rs.ex.FinishedAt = &now
	}
	rs.done = true
	status := rs.ex.Status
	errMsg := rs.ex.Error
	rs.mu.Unlock()

	m.publish(rs, Event{Type: terminalEventFor(status), ExecutionID: execID, Time: time.Now(), Status: status, Error: errMsg})
	rs.bcast.close()
	m.scheduleEviction(execID)
}

func (m *Manager) finishCancelled(execID string, rs *runState, reason string) {
	rs.mu.Lock()
	rs.ex.Status = model.StatusCancelled
	rs.ex.Error = reason
	now := time.Now()
	rs.ex.FinishedAt = &now
	rs.done = true
	ex := rs.snapshotLocked()
	rs.mu.Unlock()

	if err := m.r.Execs.SaveExecution(context.Background(), ex); err != nil {
		log.Printf("runner: manager: failed to persist cancelled execution %s: %v", execID, err)
	}
	m.publish(rs, Event{Type: EventExecutionCancelled, ExecutionID: execID, Time: time.Now(), Status: model.StatusCancelled, Error: reason})
	rs.bcast.close()
	m.scheduleEviction(execID)
}

func (m *Manager) scheduleEviction(execID string) {
	time.AfterFunc(finishedRetention, func() {
		m.mu.Lock()
		delete(m.states, execID)
		m.mu.Unlock()
	})
}

func (m *Manager) publish(rs *runState, ev Event) {
	rs.bcast.publish(ev)
	m.all.publish(ev)
}

// List returns a bounded snapshot of all executions still held in the
// manager. Finished records remain here briefly for fast live/history
// views; durable history is supplied by the Store through the API.
func (m *Manager) List(limit int) []*model.Execution {
	if limit < 1 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	m.mu.Lock()
	states := make([]*runState, 0, len(m.states))
	for _, rs := range m.states {
		states = append(states, rs)
	}
	m.mu.Unlock()

	out := make([]*model.Execution, 0, len(states))
	for _, rs := range states {
		out = append(out, rs.snapshot())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Stats exposes only queue capacity/occupancy metadata for the live
// monitor; it never includes workflow payloads or binary data.
func (m *Manager) Stats() map[string]int {
	queued := int(atomic.LoadInt32(&m.queued))
	running := len(m.sem)
	waiting := queued - running
	if waiting < 0 {
		waiting = 0
	}
	return map[string]int{
		"accepted":      queued,
		"running":       running,
		"waiting":       waiting,
		"maxConcurrent": cap(m.sem),
		"maxQueued":     int(m.maxQueued),
	}
}

// SubscribeAll streams metadata-only events for the global live monitor.
func (m *Manager) SubscribeAll() (<-chan Event, []Event, func()) {
	id, ch, replay := m.all.subscribe()
	return ch, replay, func() { m.all.unsubscribe(id) }
}

func terminalEventFor(status model.ExecutionStatus) EventType {
	switch status {
	case model.StatusSuccess:
		return EventExecutionCompleted
	case model.StatusCancelled:
		return EventExecutionCancelled
	default:
		return EventExecutionFailed
	}
}

// Get returns a defensive copy of the in-memory execution record, or
// (nil, false) if the Manager has no record of it (never started
// through this Manager, or evicted after finishedRetention -- the
// caller should fall back to the durable store for the latter case).
func (m *Manager) Get(execID string) (*model.Execution, bool) {
	m.mu.Lock()
	rs, ok := m.states[execID]
	m.mu.Unlock()
	if !ok {
		return nil, false
	}
	return rs.snapshot(), true
}

// Cancel requests cancellation of a queued-or-running execution.
// Idempotent-ish: cancelling an already-finished execution's context
// is a harmless no-op (its context is already done).
func (m *Manager) Cancel(execID string) error {
	m.mu.Lock()
	rs, ok := m.states[execID]
	m.mu.Unlock()
	if !ok {
		return ErrExecutionNotFound
	}
	rs.cancel()
	return nil
}

// Subscribe registers an SSE client for execID's live events, returning
// a small replay of recently-published events (so a client that
// connects a moment after execution.created still sees it) followed by
// new events as they're published. unsubscribe MUST be called (e.g.
// via defer) when the client disconnects, or the subscriber channel
// leaks for the remaining lifetime of the broadcaster.
func (m *Manager) Subscribe(execID string) (ch <-chan Event, replay []Event, unsubscribe func(), ok bool) {
	m.mu.Lock()
	rs, exists := m.states[execID]
	m.mu.Unlock()
	if !exists {
		return nil, nil, nil, false
	}
	id, c, rep := rs.bcast.subscribe()
	return c, rep, func() { rs.bcast.unsubscribe(id) }, true
}
