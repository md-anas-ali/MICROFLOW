package scheduler

// queue.go turns the Scheduler into the ONE global execution gate.
//
// Every path that wants to run a workflow -- a due Schedule Trigger, a manual
// Run, a webhook, a Run Service / Run All step, crash recovery -- submits a job
// here. A single worker goroutine takes jobs in FIFO order and runs exactly
// one at a time (max concurrency 1):
//
//	submit -> queue -> [cooldown gap] -> run -> cleanup hook -> next job
//
// A job is identified by a key (the workflow ID; each Service has one
// workflow). A key that is queued, reserved or running is refused with
// ErrDuplicate, so the same Service/Workflow can never be queued or run twice.
// A job that fails or panics never stops the queue.

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// ErrDuplicate is returned when the key is already queued, reserved or running.
var ErrDuplicate = errors.New("this workflow is already queued or running")

// cleanupTimeout bounds one cleanup hook call so it can never wedge the queue.
const cleanupTimeout = 30 * time.Second

type job struct {
	key      string
	kind     string // schedule | manual | recovery | webhook | run-all (log only)
	run      func(ctx context.Context) bool
	popped   bool // taken by the worker (guarded by gq.qmu)
	canceled bool // withdrawn while still queued (guarded by gq.qmu)
}

// gq is the queue state, embedded by pointer in Scheduler.
type gq struct {
	qmu  sync.Mutex
	jobs []*job
	keys map[string]struct{} // queued + reserved + running keys
	wake chan struct{}       // capacity 1: "the queue changed"

	workerOnce sync.Once
	baseCtx    context.Context // set by Start; Background until then

	cooldown   time.Duration
	cleanup    func(ctx context.Context)
	lastFinish time.Time // when the last job that really ran (and its cleanup) ended
}

func newGQ() *gq {
	return &gq{keys: map[string]struct{}{}, wake: make(chan struct{}, 1), baseCtx: context.Background()}
}

func (s *Scheduler) setContext(ctx context.Context) {
	s.qmu.Lock()
	s.baseCtx = ctx
	s.qmu.Unlock()
}

func (s *Scheduler) getContext() context.Context {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	return s.baseCtx
}

// SetCooldown sets the minimum settling gap between the end of one job and the
// start of the next (SERVICE_COOLDOWN_SECONDS). Zero or negative disables it.
func (s *Scheduler) SetCooldown(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.qmu.Lock()
	s.cooldown = d
	s.qmu.Unlock()
}

// SetCleanup registers the hook run after every job that actually ran (success,
// error, cancelled or panic), before the cooldown and the next job.
func (s *Scheduler) SetCleanup(fn func(ctx context.Context)) {
	s.qmu.Lock()
	s.cleanup = fn
	s.qmu.Unlock()
}

// Ticket is a claimed queue position for one key: Submit it or Release it,
// exactly once. Lets a caller claim the key (and learn of a duplicate) BEFORE
// it creates any durable state, then hand over the job.
type Ticket struct {
	s    *Scheduler
	key  string
	used bool
}

// Reserve claims key or returns ErrDuplicate. Release must follow if the caller
// then decides not to submit.
func (s *Scheduler) Reserve(key string) (*Ticket, error) {
	if key == "" {
		return nil, errors.New("scheduler: empty job key")
	}
	s.qmu.Lock()
	defer s.qmu.Unlock()
	if _, busy := s.keys[key]; busy {
		return nil, ErrDuplicate
	}
	s.keys[key] = struct{}{}
	return &Ticket{s: s, key: key}, nil
}

// Release gives the key back without running anything. Safe on nil.
func (t *Ticket) Release() {
	if t == nil {
		return
	}
	t.s.qmu.Lock()
	defer t.s.qmu.Unlock()
	if t.used {
		return
	}
	t.used = true
	delete(t.s.keys, t.key)
}

// Submit appends the job to the back of the queue. run reports whether it
// really executed a workflow (false = nothing ran, so no cleanup/cooldown).
// The returned func withdraws the job if it is still waiting (true = removed,
// it will never run and its key is free again; false = already picked up).
func (t *Ticket) Submit(kind string, run func(ctx context.Context) bool) (withdraw func() bool) {
	j := &job{key: t.key, kind: kind, run: run}
	t.s.submit(t, j)
	return func() bool { return t.s.withdraw(j) }
}

func (s *Scheduler) submit(t *Ticket, j *job) {
	s.qmu.Lock()
	if t.used {
		s.qmu.Unlock()
		return
	}
	t.used = true
	s.jobs = append(s.jobs, j)
	s.qmu.Unlock()
	s.workerOnce.Do(func() { go s.worker() })
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Enqueue reserves key and submits in one step.
func (s *Scheduler) Enqueue(key, kind string, run func(ctx context.Context) bool) error {
	t, err := s.Reserve(key)
	if err != nil {
		return err
	}
	t.Submit(kind, run)
	return nil
}

// RunSync queues fn under key and blocks until it has finished. fn runs on the
// queue worker (so it is serialized with everything else) with the caller's
// ctx. If ctx ends while the job is still waiting it is withdrawn and ctx.Err()
// returned; once it has started, RunSync waits for it to end (fn is expected to
// honour ctx), so the caller never believes a run is over while it still runs.
func (s *Scheduler) RunSync(ctx context.Context, key, kind string, fn func(ctx context.Context)) error {
	done := make(chan struct{})
	j := &job{key: key, kind: kind}
	j.run = func(context.Context) bool {
		defer close(done)
		if ctx.Err() != nil {
			return false
		}
		fn(ctx)
		return true
	}
	t, err := s.Reserve(key)
	if err != nil {
		return err
	}
	s.submit(t, j)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		if s.withdraw(j) {
			return ctx.Err()
		}
		<-done
		return nil
	}
}

// withdraw removes a not-yet-started job. Reports false if the worker already
// took it.
func (s *Scheduler) withdraw(j *job) bool {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	if j.popped || j.canceled {
		return false
	}
	j.canceled = true
	delete(s.keys, j.key)
	return true
}

// next blocks for the next runnable job, in FIFO order.
func (s *Scheduler) next() *job {
	for {
		s.qmu.Lock()
		for len(s.jobs) > 0 {
			j := s.jobs[0]
			s.jobs[0] = nil
			s.jobs = s.jobs[1:]
			if j.canceled {
				continue
			}
			j.popped = true
			s.qmu.Unlock()
			return j
		}
		s.jobs = nil // drop the drained backing array
		s.qmu.Unlock()
		<-s.wake
	}
}

// worker is the single queue consumer: max concurrency 1.
func (s *Scheduler) worker() {
	for {
		j := s.next()
		s.settle()
		ran := s.execute(j)

		s.qmu.Lock()
		delete(s.keys, j.key)
		s.qmu.Unlock()

		if ran {
			s.runCleanup(j)
			s.qmu.Lock()
			s.lastFinish = time.Now() // the cooldown runs after cleanup
			s.qmu.Unlock()
		}
	}
}

// settle waits out the remainder of the cooldown since the last finished job.
// Interrupted by shutdown so a stopping server is not held up.
func (s *Scheduler) settle() {
	s.qmu.Lock()
	wait := s.cooldown - time.Since(s.lastFinish)
	ctx := s.baseCtx
	s.qmu.Unlock()
	if wait <= 0 {
		return
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// execute runs one job, converting a panic into "it ran and failed" so the
// queue always carries on with the next job.
func (s *Scheduler) execute(j *job) (ran bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("scheduler: %s job for %s panicked: %v (continuing with the next queued job)", j.kind, j.key, r)
			ran = true
		}
	}()
	log.Printf("scheduler: starting %s job for %s", j.kind, j.key)
	ran = j.run(s.getContext())
	log.Printf("scheduler: finished %s job for %s", j.kind, j.key)
	return ran
}

func (s *Scheduler) runCleanup(j *job) {
	s.qmu.Lock()
	fn := s.cleanup
	s.qmu.Unlock()
	if fn == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("scheduler: cleanup after %s panicked: %v", j.key, r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	fn(ctx)
}
