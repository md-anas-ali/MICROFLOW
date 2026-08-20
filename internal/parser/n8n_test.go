// UNVERIFIED IN SANDBOX: written but never run here (no Go toolchain
// available in this environment). Run with `go test ./...` on your
// machine -- see STATUS.md.
package parser

import (
	"os"
	"testing"
)

func TestParseActualWorkflow(t *testing.T) {
	raw, err := os.ReadFile("../../test/testdata/sample_workflow.json")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	result, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(result.Workflow.Nodes) == 0 {
		t.Fatal("expected nodes, got none")
	}
	// This is the caller's actual workflow: assert the exact node count
	// so a future n8n export format change or parser regression is
	// caught immediately rather than silently under-importing nodes.
	const wantNodes = 120
	if len(result.Workflow.Nodes) != wantNodes {
		t.Errorf("got %d nodes, want %d", len(result.Workflow.Nodes), wantNodes)
	}

	unsupported := result.Unsupported()
	if len(unsupported) > 0 {
		t.Logf("compatibility checklist: %d unsupported node type(s):", len(unsupported))
		for _, u := range unsupported {
			t.Logf("  - %s (%s)", u.NodeName, u.OriginalType)
		}
	}

	// Spot-check a few specific nodes the workflow depends on end-to-end.
	mustHave := []string{"Schedule Trigger", "Model Controller", "Concat + BGM + Subtitle", "YouTube Upload (Scheduled)", "Gmail Error"}
	for _, name := range mustHave {
		if _, ok := result.Workflow.Nodes[name]; !ok {
			t.Errorf("expected node %q to be present after parse", name)
		}
	}
}

func TestParseRejectsEmptyWorkflow(t *testing.T) {
	_, err := Parse([]byte(`{"nodes": [], "connections": {}}`))
	if err == nil {
		t.Fatal("expected error for zero-node workflow, got nil")
	}
}

func TestParseRejectsInvalidJSON(t *testing.T) {
	_, err := Parse([]byte(`{not json`))
	if err == nil {
		t.Fatal("expected error for invalid json, got nil")
	}
}
