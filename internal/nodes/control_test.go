package nodes

import (
	"context"
	"testing"

	"microflow/internal/model"
)

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
