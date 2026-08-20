// UNVERIFIED IN SANDBOX: written but never run here. See STATUS.md.
package parser

import (
	"os"
	"testing"
)

func TestExportRoundTrip(t *testing.T) {
	raw, err := os.ReadFile("../../test/testdata/sample_workflow.json")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	first, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	exported, err := Export(first.Workflow)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	second, err := Parse(exported)
	if err != nil {
		t.Fatalf("Parse(Export(...)) failed -- exported JSON is not valid n8n input: %v", err)
	}

	if len(second.Workflow.Nodes) != len(first.Workflow.Nodes) {
		t.Errorf("round-trip node count = %d, want %d", len(second.Workflow.Nodes), len(first.Workflow.Nodes))
	}

	firstConnCount, secondConnCount := 0, 0
	for _, c := range first.Workflow.Connections {
		firstConnCount += len(c)
	}
	for _, c := range second.Workflow.Connections {
		secondConnCount += len(c)
	}
	if firstConnCount != secondConnCount {
		t.Errorf("round-trip connection count = %d, want %d", secondConnCount, firstConnCount)
	}

	// Every node's original n8n type string must survive the round trip
	// exactly -- this is the field a real n8n import would validate most
	// strictly.
	for name, n1 := range first.Workflow.Nodes {
		n2, ok := second.Workflow.Nodes[name]
		if !ok {
			t.Errorf("node %q missing after round-trip", name)
			continue
		}
		if n1.OriginalType != n2.OriginalType {
			t.Errorf("node %q type changed: %q -> %q", name, n1.OriginalType, n2.OriginalType)
		}
	}
}
