package nodes

import (
	"context"
	"encoding/json"
	"testing"

	"microflow/internal/engine"
	"microflow/internal/model"
)

// jsonRoundTripStaticData is an in-memory StaticDataStore that JSON
// round-trips every value on every WithLock call, the same way the real
// Postgres-backed store does (see internal/store/postgres.go). Using a
// plain Go map here would hide bugs where code assumes a stored value
// keeps its original Go type (e.g. []model.Item) across calls, when in
// production it always comes back as generic []interface{}/
// map[string]interface{} JSON soup.
type jsonRoundTripStaticData struct {
	raw []byte // nil until first write; jsonb starts as '{}' in Postgres
}

func (s *jsonRoundTripStaticData) WithLock(_ context.Context, _ string, fn func(map[string]any) (map[string]any, error)) error {
	data := map[string]any{}
	if s.raw != nil {
		if err := json.Unmarshal(s.raw, &data); err != nil {
			return err
		}
	}
	newData, err := fn(data)
	if err != nil {
		return err
	}
	b, err := json.Marshal(newData)
	if err != nil {
		return err
	}
	s.raw = b
	return nil
}

// TestIfNodeStatusCodeEqualsStringLiteral is a regression test for a real
// bug found while running the reference "My workflow 14" through
// cmd/e2echeck: an httpRequest node's $json.statusCode decodes from JSON
// as a Go float64, but an IF node's condition literal ("200") is always a
// Go string. fmtEq's numeric fast path required BOTH sides to already be
// float64/int, so it silently fell through to toStr(a)==toStr(b), and the
// old toStr returned "" for any non-string value -- meaning
// `{{ $json.statusCode }} equals "200"` could NEVER be true, no matter
// what the actual status code was. This is an extremely common n8n IF
// condition shape (any "did this HTTP call succeed?" check), so the bug
// silently broke every such condition across every imported workflow, not
// just this one node ("Image Generated?" in the reference workflow).
func TestIfNodeStatusCodeEqualsStringLiteral(t *testing.T) {
	node := &model.Node{
		Name: "Image Generated?",
		Type: "n8n-nodes-base.if",
		Parameters: map[string]any{
			"combinator": "and",
			"conditions": []any{
				map[string]any{
					"left":     "{{ $json.statusCode }}",
					"operator": "equals",
					"right":    "200",
				},
			},
		},
	}
	rc := testRunContext()

	cases := []struct {
		name       string
		statusCode any // as it would actually decode from JSON: float64
		wantTrue   bool
	}{
		{"200 float64 matches string literal 200", float64(200), true},
		{"404 float64 does not match", float64(404), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := model.NodeOutput{{
				{JSON: map[string]any{"statusCode": tc.statusCode}},
			}}
			out, err := (IfExecutor{}).Execute(context.Background(), rc, node, input)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			gotTrue := len(out[0]) == 1
			gotFalse := len(out[1]) == 1
			if gotTrue == gotFalse {
				t.Fatalf("expected exactly one branch populated, got true=%d false=%d", len(out[0]), len(out[1]))
			}
			if gotTrue != tc.wantTrue {
				t.Fatalf("statusCode=%v: got routed-true=%v, want %v", tc.statusCode, gotTrue, tc.wantTrue)
			}
		})
	}
}

// TestSplitInBatchesLoopsOverAllItems is a regression test for a real bug
// found while running "My workflow 14" end-to-end through cmd/e2echeck:
// SplitInBatchesExecutor used to recompute its item list from the
// CURRENT call's `input` on every invocation. On the very first call
// that's correct (input = the full fanned-out list), but every later
// call happens via the loop body's own output looping back into this
// same node (e.g. "Cooldown Between Scenes" -> "Loop One By One" in the
// reference workflow), so `input` on those calls is only the single
// batch that just went around -- not the original N items. With
// batchSize=1 that meant the loop always reported "finished" after
// exactly ONE iteration, no matter how many items it started with, and
// even then emitted an empty "done" output instead of the accumulated
// set -- so a 5-scene video workflow silently only ever processed scene
// 1 and never reached final assembly/upload, with no error anywhere.
//
// This test drives the executor through a full 5-item loop using
// jsonRoundTripStaticData (which forces every stored value through a
// JSON marshal/unmarshal cycle, exactly like the real Postgres-backed
// store) to also guard against the fix relying on a Go type surviving
// in-memory across calls.
func TestSplitInBatchesLoopsOverAllItems(t *testing.T) {
	node := &model.Node{
		Name:       "Loop One By One",
		Type:       "n8n-nodes-base.splitInBatches",
		Parameters: map[string]any{},
	}
	rc := &engine.RunContext{
		Workflow:  &model.Workflow{ID: "wf-test"},
		Execution: &model.Execution{ID: "exec-test"},
	}
	rc.StaticData = &jsonRoundTripStaticData{}

	exec := &SplitInBatchesExecutor{}

	// First call: 5 items fanned out upstream (e.g. by Split Scenes).
	first := model.NodeOutput{{
		{JSON: map[string]any{"scene": float64(1)}},
		{JSON: map[string]any{"scene": float64(2)}},
		{JSON: map[string]any{"scene": float64(3)}},
		{JSON: map[string]any{"scene": float64(4)}},
		{JSON: map[string]any{"scene": float64(5)}},
	}}

	var seenScenes []float64
	out, err := exec.Execute(context.Background(), rc, node, first)
	if err != nil {
		t.Fatalf("call 1: %v", err)
	}
	if len(out[0]) != 0 {
		t.Fatalf("call 1: expected empty done-output, got %d items", len(out[0]))
	}
	if len(out[1]) != 1 {
		t.Fatalf("call 1: expected exactly 1 loop item, got %d", len(out[1]))
	}
	seenScenes = append(seenScenes, out[1][0].JSON["scene"].(float64))

	// Every subsequent call happens via the loop body looping back with
	// ONLY the single item that just went around -- not the original 5.
	// This is the exact shape that triggered the bug.
	for i := 2; i <= 5; i++ {
		loopBack := model.NodeOutput{out[1]}
		out, err = exec.Execute(context.Background(), rc, node, loopBack)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if len(out[0]) != 0 {
			t.Fatalf("call %d: expected empty done-output mid-loop, got %d items", i, len(out[0]))
		}
		if len(out[1]) != 1 {
			t.Fatalf("call %d: expected exactly 1 loop item, got %d -- loop terminated early (the bug)", i, len(out[1]))
		}
		seenScenes = append(seenScenes, out[1][0].JSON["scene"].(float64))
	}

	// One more call (loop body's output from the 5th item looping back):
	// all 5 items are now exhausted, so this should emit the FULL
	// accumulated set on "done" and nothing on "loop".
	loopBack := model.NodeOutput{out[1]}
	out, err = exec.Execute(context.Background(), rc, node, loopBack)
	if err != nil {
		t.Fatalf("final call: %v", err)
	}
	if len(out[1]) != 0 {
		t.Fatalf("final call: expected empty loop-output, got %d items", len(out[1]))
	}
	if len(out[0]) != 5 {
		t.Fatalf("final call: expected all 5 items on done-output, got %d -- accumulated set was lost", len(out[0]))
	}

	if len(seenScenes) != 5 {
		t.Fatalf("expected to have looped over 5 scenes, got %d: %v", len(seenScenes), seenScenes)
	}
	for i, want := range []float64{1, 2, 3, 4, 5} {
		if seenScenes[i] != want {
			t.Fatalf("scene order mismatch at index %d: got %v, want %v (full: %v)", i, seenScenes[i], want, seenScenes)
		}
	}
}
