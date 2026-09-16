package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"microflow/internal/engine"

	"microflow/internal/model"
)

func tinyWorkflow() *model.Workflow {
	return &model.Workflow{ID: "wf", UpdatedAt: time.Unix(1, 0), Nodes: map[string]*model.Node{
		"Start": {Name: "Start", Type: model.TypeNoOp},
	}}
}

func validCP(t *testing.T) *model.ExecutionCheckpoint {
	wf := tinyWorkflow()
	h, err := workflowHash(wf)
	if err != nil {
		t.Fatal(err)
	}
	return &model.ExecutionCheckpoint{
		ExecutionID: "ex", WorkflowID: wf.ID, WorkflowHash: h, Mode: "manual", StartedAt: time.Now(),
		State: model.ExecutionCheckpointState{Version: checkpointVersion, Pending: []model.CheckpointQueueItem{{NodeName: "Start", Input: model.NodeOutput{{{JSON: map[string]any{"x": 1}}}}}}, Steps: 2, Status: model.StatusRunning},
	}
}

func TestValidateCheckpointRejectsVersion(t *testing.T) {
	cp := validCP(t)
	cp.State.Version++
	if !errors.Is(validateCheckpoint(cp, tinyWorkflow(), t.TempDir()), ErrInvalidCheckpoint) {
		t.Fatal("expected invalid version")
	}
}

func TestValidateCheckpointRejectsWorkflowChange(t *testing.T) {
	cp := validCP(t)
	wf := tinyWorkflow()
	wf.Nodes["Start"].Parameters = map[string]any{"changed": true}
	if !errors.Is(validateCheckpoint(cp, wf, t.TempDir()), ErrInvalidCheckpoint) {
		t.Fatal("expected workflow mismatch")
	}
}

func TestValidateCheckpointRejectsUnknownNode(t *testing.T) {
	cp := validCP(t)
	cp.State.Pending[0].NodeName = "Missing"
	if !errors.Is(validateCheckpoint(cp, tinyWorkflow(), t.TempDir()), ErrInvalidCheckpoint) {
		t.Fatal("expected missing node rejection")
	}
}

func TestValidateCheckpointAcceptsExistingBinaryUnderScratch(t *testing.T) {
	root := t.TempDir()
	exID := "ex"
	dir := filepath.Join(root, exID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	cp := validCP(t)
	cp.State.Pending[0].Input[0][0].Binary = map[string]model.BinaryRef{"data": {FileName: file, SizeBytes: 1}}
	cp.ExecutionID = exID
	if err := validateCheckpoint(cp, tinyWorkflow(), root); err != nil {
		t.Fatal(err)
	}
}

func TestValidateCheckpointRejectsMissingBinary(t *testing.T) {
	root := t.TempDir()
	cp := validCP(t)
	cp.State.Pending[0].Input[0][0].Binary = map[string]model.BinaryRef{"data": {FileName: filepath.Join(root, "ex", "missing")}}
	if !errors.Is(validateCheckpoint(cp, tinyWorkflow(), root), ErrInvalidCheckpoint) {
		t.Fatal("expected missing binary rejection")
	}
}

func TestValidateCheckpointRejectsOversizedStateCounts(t *testing.T) {
	cp := validCP(t)
	cp.State.Steps = 5001
	if !errors.Is(validateCheckpoint(cp, tinyWorkflow(), t.TempDir()), ErrInvalidCheckpoint) {
		t.Fatal("expected step bound")
	}
}

func TestCheckpointMaxBytesBound(t *testing.T) {
	old := os.Getenv("MICROFLOW_MAX_CHECKPOINT_BYTES")
	defer os.Setenv("MICROFLOW_MAX_CHECKPOINT_BYTES", old)
	os.Setenv("MICROFLOW_MAX_CHECKPOINT_BYTES", "65536")
	if got := checkpointMaxBytes(); got != 65536 {
		t.Fatalf("got %d", got)
	}
	os.Setenv("MICROFLOW_MAX_CHECKPOINT_BYTES", "1")
	if got := checkpointMaxBytes(); got != 512*1024 {
		t.Fatalf("unsafe value should fall back, got %d", got)
	}
}

func TestRecoveryLeaseBound(t *testing.T) {
	old := os.Getenv("MICROFLOW_RECOVERY_LEASE_SECONDS")
	defer os.Setenv("MICROFLOW_RECOVERY_LEASE_SECONDS", old)
	os.Setenv("MICROFLOW_RECOVERY_LEASE_SECONDS", "3600")
	if recoveryLease() != time.Hour {
		t.Fatal("expected one hour lease")
	}
	os.Setenv("MICROFLOW_RECOVERY_LEASE_SECONDS", "10")
	if recoveryLease() != time.Hour {
		t.Fatal("unsafe short lease should fall back")
	}
}

func TestRecoveryBatchIsBounded(t *testing.T) {
	for _, tc := range []struct{ in, want int }{{0, 1}, {1, 1}, {16, 16}, {100, 16}} {
		if got := recoveryBatchSize(tc.in); got != tc.want {
			t.Fatalf("%d => %d, want %d", tc.in, got, tc.want)
		}
	}
}

// fakeRecoveryStore is enough to test the manager's in-process duplicate guard
// without requiring a live PostgreSQL server in the unit-test environment.
type fakeRecoveryStore struct {
	claims  []*model.ExecutionCheckpoint
	saved   []string
	deleted []string
}

func (f *fakeRecoveryStore) SaveExecutionCheckpoint(_ context.Context, cp *model.ExecutionCheckpoint) error {
	f.saved = append(f.saved, cp.ExecutionID)
	return nil
}
func (f *fakeRecoveryStore) DeleteExecutionCheckpoint(_ context.Context, id string) error {
	f.deleted = append(f.deleted, id)
	return nil
}
func (f *fakeRecoveryStore) ClaimRecoverableExecutionCheckpoints(_ context.Context, _ string, limit int, _ time.Duration) ([]*model.ExecutionCheckpoint, error) {
	if len(f.claims) == 0 {
		return nil, nil
	}
	if len(f.claims) > limit {
		out := f.claims[:limit]
		f.claims = f.claims[limit:]
		return out, nil
	}
	out := f.claims
	f.claims = nil
	return out, nil
}
func (f *fakeRecoveryStore) ClearExecutionCheckpointLease(_ context.Context, id, _ string) error {
	_ = id
	return nil
}
func (f *fakeRecoveryStore) CleanupOrphanedExecutionCheckpoints(_ context.Context, _ int, _ time.Time) (int64, error) {
	return 0, nil
}

func TestRecoverBatchDoesNotDuplicateExistingState(t *testing.T) {
	wf := tinyWorkflow()
	cp := validCP(t)
	cp.WorkflowID = wf.ID
	fr := &fakeRecoveryStore{}
	r := New(funcLoader{wf}, noopSaver{}, nil, nil, nil, t.TempDir()).WithRecovery(fr)
	m := NewManager(r, 1, 2)
	rs := &runState{ex: &model.Execution{ID: cp.ExecutionID, WorkflowID: wf.ID}, cancel: func() {}, bcast: newBroadcaster()}
	m.states[cp.ExecutionID] = rs
	fr.claims = []*model.ExecutionCheckpoint{cp}
	if err := m.recoverBatch(context.Background(), "owner"); err != nil {
		t.Fatal(err)
	}
	if len(m.states) != 1 {
		t.Fatal("duplicate recovery state created")
	}
}

type funcLoader struct{ wf *model.Workflow }

func (f funcLoader) LoadWorkflow(context.Context, string) (*model.Workflow, error) { return f.wf, nil }

type noopSaver struct{}

func (noopSaver) SaveExecution(context.Context, *model.Execution) error { return nil }

func TestRunOnceDeletesCheckpointAfterSuccess(t *testing.T) {
	wf := tinyWorkflow()
	rec := &fakeRecoveryStore{}
	saver := &recordingSaver{}
	eng := engineNoOp()
	r := New(funcLoader{wf}, saver, eng, nil, nil, t.TempDir()).WithRecovery(rec)
	ex, err := r.runOnce(context.Background(), wf, "success-ex", "Start", "manual", model.NodeOutput{{{JSON: map[string]any{"x": 1}}}}, nil, nil)
	if err != nil || ex.Status != model.StatusSuccess {
		t.Fatalf("run failed: ex=%#v err=%v", ex, err)
	}
	if len(rec.deleted) != 1 || rec.deleted[0] != "success-ex" {
		t.Fatalf("checkpoint not deleted: %#v", rec.deleted)
	}
}

func TestRunOnceDeletesCheckpointAfterPermanentFailure(t *testing.T) {
	n := &model.Node{Name: "Fail", Type: model.TypeNoOp}
	wf := &model.Workflow{ID: "wf-fail", Nodes: map[string]*model.Node{"Fail": n}}
	rec := &fakeRecoveryStore{}
	saver := &recordingSaver{}
	failExec := &failureExecutor{}
	eng := engine.New(map[model.NodeType]engine.NodeExecutor{model.TypeNoOp: failExec})
	r := New(funcLoader{wf}, saver, eng, nil, nil, t.TempDir()).WithRecovery(rec)
	ex, err := r.runOnce(context.Background(), wf, "fail-ex", "Fail", "manual", model.NodeOutput{{{JSON: map[string]any{}}}}, nil, nil)
	if err == nil || ex.Status != model.StatusError {
		t.Fatalf("expected permanent failure: ex=%#v err=%v", ex, err)
	}
	if len(rec.deleted) != 1 || rec.deleted[0] != "fail-ex" {
		t.Fatalf("checkpoint not deleted after failure: %#v", rec.deleted)
	}
}

func TestRunOnceDeletesCheckpointAfterCancellation(t *testing.T) {
	wf := tinyWorkflow()
	rec := &fakeRecoveryStore{}
	saver := &recordingSaver{}
	eng := engineNoOp()
	r := New(funcLoader{wf}, saver, eng, nil, nil, t.TempDir()).WithRecovery(rec)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ex, err := r.runOnce(ctx, wf, "cancel-ex", "Start", "manual", nil, nil, nil)
	if err == nil || ex.Status != model.StatusCancelled {
		t.Fatalf("expected cancellation: ex=%#v err=%v", ex, err)
	}
	if len(rec.deleted) != 1 || rec.deleted[0] != "cancel-ex" {
		t.Fatalf("checkpoint not deleted after cancellation: %#v", rec.deleted)
	}
}

func TestValidateCheckpointRejectsInvalidWaitState(t *testing.T) {
	cp := validCP(t)
	cp.State.Wait = &model.CheckpointWait{Until: time.Now().Add(time.Minute)}
	if !errors.Is(validateCheckpoint(cp, tinyWorkflow(), t.TempDir()), ErrInvalidCheckpoint) {
		t.Fatal("expected invalid wait state")
	}
}

func TestValidateCheckpointRejectsUnknownStatus(t *testing.T) {
	cp := validCP(t)
	cp.State.Status = model.ExecutionStatus("bogus")
	if !errors.Is(validateCheckpoint(cp, tinyWorkflow(), t.TempDir()), ErrInvalidCheckpoint) {
		t.Fatal("expected unknown status rejection")
	}
}

func TestValidScratchRefDoesNotEscapeExecutionDirectory(t *testing.T) {
	root := t.TempDir()
	exID := "ex"
	inside := filepath.Join(root, exID, "file")
	if err := os.MkdirAll(filepath.Dir(inside), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inside, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !validScratchRef(inside, root, exID) {
		t.Fatal("expected in-scratch file to validate")
	}
	outside := filepath.Join(root, "outside")
	if err := os.WriteFile(outside, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if validScratchRef(outside, root, exID) {
		t.Fatal("outside scratch ref must be rejected")
	}
}

func engineNoOp() *engine.Engine {
	return engine.New(map[model.NodeType]engine.NodeExecutor{model.TypeNoOp: noOpExecutor{}})
}

type noOpExecutor struct{}

func (noOpExecutor) Execute(_ context.Context, _ *engine.RunContext, _ *model.Node, in model.NodeOutput) (model.NodeOutput, error) {
	return in, nil
}

type failureExecutor struct{}

func (*failureExecutor) Execute(_ context.Context, _ *engine.RunContext, _ *model.Node, _ model.NodeOutput) (model.NodeOutput, error) {
	return nil, engine.Permanent(errors.New("permanent test failure"))
}

type recordingSaver struct {
	executions []*model.Execution
	deleted    []string
}

func (s *recordingSaver) SaveExecution(_ context.Context, ex *model.Execution) error {
	cp := *ex
	s.executions = append(s.executions, &cp)
	return nil
}
