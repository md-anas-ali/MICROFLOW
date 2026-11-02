package engine

import (
	"context"
	"testing"

	"microflow/internal/model"
)

type panicExec struct{}

func (panicExec) Execute(ctx context.Context, rc *RunContext, node *model.Node, in model.NodeOutput) (model.NodeOutput, error) {
	var m map[string]int
	m["x"] = 1 // nil-map write -> panic
	return nil, nil
}

func TestSafeExecuteConvertsPanicToPermanentError(t *testing.T) {
	_, err := safeExecute(context.Background(), panicExec{}, nil, &model.Node{Name: "boom"}, nil)
	if err == nil {
		t.Fatal("expected an error from a panicking node")
	}
	if !IsPermanent(err) {
		t.Fatal("panic error must be permanent so it is not retried")
	}
}
