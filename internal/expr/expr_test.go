// UNVERIFIED IN SANDBOX: written but never run here. See STATUS.md.
package expr

import (
	"os"
	"testing"
)

func TestEvalJSON(t *testing.T) {
	ctx := Context{JSON: map[string]any{"name": "trend-1", "score": 42.0}}
	got, err := Eval("hello {{ $json.name }}", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello trend-1" {
		t.Errorf("got %q", got)
	}
}

func TestEvalValuePreservesType(t *testing.T) {
	ctx := Context{JSON: map[string]any{"score": 42.0}}
	v, err := EvalValue("{{ $json.score }}", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if f, ok := v.(float64); !ok || f != 42.0 {
		t.Errorf("got %#v, want float64(42)", v)
	}
}

func TestEvalNodeReference(t *testing.T) {
	ctx := Context{
		NodeOutputs: map[string]map[string]any{
			"HTTP Request": {"id": "abc123"},
		},
	}
	got, err := Eval(`{{ $node["HTTP Request"].json.id }}`, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != "abc123" {
		t.Errorf("got %q", got)
	}
}

func TestEvalEnv(t *testing.T) {
	os.Setenv("MICROFLOW_TEST_VAR", "hi")
	defer os.Unsetenv("MICROFLOW_TEST_VAR")
	got, err := Eval("{{ $env.MICROFLOW_TEST_VAR }}", Context{})
	if err != nil {
		t.Fatal(err)
	}
	if got != "hi" {
		t.Errorf("got %q", got)
	}
}

func TestEvalMissingFieldErrors(t *testing.T) {
	ctx := Context{JSON: map[string]any{}}
	_, err := evalExpression("$json.nested.deep", ctx)
	if err == nil {
		t.Fatal("expected error walking into a nil field")
	}
}
