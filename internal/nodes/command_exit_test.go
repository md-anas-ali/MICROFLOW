package nodes

import (
	"context"
	"strings"
	"testing"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
)

func runShForExitTest(t *testing.T, script string) (model.NodeOutput, error) {
	t.Helper()
	e := &ExecuteCommandExecutor{AllowedBinaries: map[string]string{"sh": "/bin/sh"}, Timeout: 20 * time.Second}
	rc := &engine.RunContext{Execution: &model.Execution{}, ScratchDir: t.TempDir(), HeavyWorkGate: make(chan struct{}, 1)}
	node := &model.Node{Name: "T", Parameters: map[string]any{"binary": "sh", "args": []any{"-c", script}}}
	return e.Execute(context.Background(), rc, node, model.NodeOutput{{model.Item{JSON: map[string]any{}}}})
}

func TestExitZeroIsSuccess(t *testing.T) {
	out, err := runShForExitTest(t, "echo VOICE_OK_6.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out[0][0].JSON["stdout"].(string); !strings.Contains(got, "VOICE_OK_6.0") {
		t.Fatalf("stdout lost: %q", got)
	}
}

func TestNonZeroExitFailsNodeWithMarkers(t *testing.T) {
	_, err := runShForExitTest(t, "echo EDGE_FAILED_ALL_PATHS; echo boom >&2; exit 1")
	if err == nil {
		t.Fatal("exit 1 must fail the node")
	}
	for _, want := range []string{"exited with code 1", "EDGE_FAILED_ALL_PATHS", "boom"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err.Error(), want)
		}
	}
}

func TestKilledBySignalFailsNode(t *testing.T) {
	if _, err := runShForExitTest(t, "kill -9 $$"); err == nil {
		t.Fatal("a process killed by a signal must fail the node")
	}
}
