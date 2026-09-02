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
	// This is the caller's actual workflow (the real exported
	// "My workflow 14" JSON, verbatim -- see test/testdata/README or
	// STATUS.md): assert the exact node count so a future n8n export
	// format change or parser regression is caught immediately rather
	// than silently under-importing nodes.
	const wantNodes = 123
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

// TestParseReadUsedTopicsRetryConfig locks in the "Read Used Topics"
// node's retry/failure config exactly as authored in the real
// "My workflow 14" export. NOTE: an earlier pass's testdata fixture had
// this node edited to RetryOnFail=true/MaxTries=3/ContinueOnFail=false
// ("production-readiness fix") -- that was a change to the *workflow*,
// which the mission for this pass explicitly forbids ("do not change
// ... node logic ... of My workflow 14"). The real uploaded workflow
// has waitBetweenTries=1000 with retryOnFail unset and
// continueOnFail=true (a failed read is swallowed and the run
// continues), so that is what parsing must produce; this test was
// updated to match the actual file instead of the other way around.
func TestParseReadUsedTopicsRetryConfig(t *testing.T) {
	raw, err := os.ReadFile("../../test/testdata/sample_workflow.json")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	result, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	node, ok := result.Workflow.Nodes["Read Used Topics"]
	if !ok {
		t.Fatal("expected node \"Read Used Topics\" to be present after parse")
	}
	if node.RetryOnFail {
		t.Error("Read Used Topics: RetryOnFail = true, want false (matches the real workflow export)")
	}
	if node.WaitBetweenTriesMs != 1000 {
		t.Errorf("Read Used Topics: WaitBetweenTriesMs = %d, want 1000", node.WaitBetweenTriesMs)
	}
	if !node.ContinueOnFail {
		t.Error("Read Used Topics: ContinueOnFail = false, want true (matches the real workflow export)")
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
