package nodes

import (
	"context"
	"testing"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
)

func newTestRunContext() *engine.RunContext {
	return &engine.RunContext{
		Execution: &model.Execution{ID: "test-exec", Mode: "manual"},
	}
}

// TestWaitExecutor_LiteralAmount confirms the original numeric-literal
// behavior (a Wait node with a plain float64 "amount", no expression)
// is unchanged by the fix below.
func TestWaitExecutor_LiteralAmount(t *testing.T) {
	rc := newTestRunContext()
	node := &model.Node{Name: "Cooldown", Parameters: map[string]any{"amount": float64(0)}}
	start := time.Now()
	out, err := WaitExecutor{}.Execute(context.Background(), rc, node, model.NodeOutput{{{JSON: map[string]any{}}}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("expected near-instant wait for amount=0, took %v", time.Since(start))
	}
	if len(out) != 1 || len(out[0]) != 1 {
		t.Fatalf("expected input passed through unchanged, got %#v", out)
	}
}

// TestWaitExecutor_ExpressionAmount is the regression test for the core
// bug: a Wait node whose "amount" is an expression string (as the
// workflow's retry-loop fix now sets on every "Loop Wait" node, e.g.
// "{{ $json.waitSeconds }}") must actually evaluate it against the
// current item, not silently fall back to the 1-second default.
func TestWaitExecutor_ExpressionAmount(t *testing.T) {
	rc := newTestRunContext()
	node := &model.Node{Name: "Loop Wait", Parameters: map[string]any{"amount": "{{ $json.waitSeconds }}"}}
	input := model.NodeOutput{{{JSON: map[string]any{"waitSeconds": float64(0)}}}}

	start := time.Now()
	_, err := WaitExecutor{}.Execute(context.Background(), rc, node, input)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// waitSeconds=0 in $json must resolve to a ~0s sleep. If the
	// expression were silently ignored (the pre-fix bug), this would
	// take ~1s (the old hardcoded default) instead.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("expression amount was not evaluated: waited %v for waitSeconds=0 (expected near-instant)", elapsed)
	}
}

// TestWaitExecutor_ExpressionAmount_MissingField confirms a bad/missing
// expression target falls back to the safe 1-second default rather than
// blocking forever or erroring the run.
func TestWaitExecutor_ExpressionAmount_MissingField(t *testing.T) {
	rc := newTestRunContext()
	node := &model.Node{Name: "Loop Wait", Parameters: map[string]any{"amount": "{{ $json.waitSeconds }}"}}
	input := model.NodeOutput{{{JSON: map[string]any{}}}} // no waitSeconds field

	start := time.Now()
	_, err := WaitExecutor{}.Execute(context.Background(), rc, node, input)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("expected ~1s default fallback, got %v", elapsed)
	}
}

// TestWaitExecutor_CancelledContext confirms cancellation still returns
// promptly and with ctx.Err(), unaffected by the expression-eval path.
func TestWaitExecutor_CancelledContext(t *testing.T) {
	rc := newTestRunContext()
	node := &model.Node{Name: "Loop Wait", Parameters: map[string]any{"amount": "{{ $json.waitSeconds }}"}}
	input := model.NodeOutput{{{JSON: map[string]any{"waitSeconds": float64(30)}}}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, err := WaitExecutor{}.Execute(ctx, rc, node, input)
	if err == nil {
		t.Fatal("expected context.Canceled error")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("cancellation should be near-instant, took %v", time.Since(start))
	}
}
