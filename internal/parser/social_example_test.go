package parser

import (
	"os"
	"testing"
)

func TestExampleSocialWorkflowParses(t *testing.T) {
	raw, err := os.ReadFile("../../examples/multi-platform-publish.workflow.json")
	if err != nil {
		t.Fatal(err)
	}
	r, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range r.Checklist {
		if !c.Supported {
			t.Errorf("unsupported: %+v", c)
		}
	}
	if len(r.Workflow.Nodes) != 9 {
		t.Fatalf("nodes=%d", len(r.Workflow.Nodes))
	}
}
