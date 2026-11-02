package runall

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"microflow/internal/model"
	"microflow/internal/runner"
)

type fakeRunner struct{ calls int32 }

func (f *fakeRunner) RunFromNode(ctx context.Context, wf, start, mode string, seed model.NodeOutput) (*model.Execution, error) {
	if atomic.AddInt32(&f.calls, 1) < 2 {
		return nil, runner.ErrAlreadyQueued
	}
	return &model.Execution{ID: "x"}, nil
}

func TestWaitsForActiveRunInsteadOfSkipping(t *testing.T) {
	dupPollInterval = 5 * time.Millisecond
	defer func() { dupPollInterval = 5 * time.Second }()
	f := &fakeRunner{}
	m := &Manager{run: f}
	ex, err := m.runWaitingForActive(context.Background(), "wf", "start", nil)
	if err != nil || ex == nil {
		t.Fatalf("expected success after waiting, got ex=%v err=%v", ex, err)
	}
	if f.calls != 2 {
		t.Fatalf("calls=%d want 2", f.calls)
	}
}

func TestGivesUpAfterMaxWait(t *testing.T) {
	dupPollInterval = 5 * time.Millisecond
	t.Setenv("MICROFLOW_RUNALL_DUP_WAIT_SECONDS", "0")
	defer func() { dupPollInterval = 5 * time.Second }()
	f := &fakeRunner{calls: -1000}
	m := &Manager{run: f}
	if _, err := m.runWaitingForActive(context.Background(), "wf", "start", nil); err == nil {
		t.Fatal("expected ErrAlreadyQueued when wait is 0")
	}
}
