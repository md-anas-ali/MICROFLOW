package expr

import "testing"

func TestEvalJSONAndEnv(t *testing.T) {
	ctx := Context{
		JSON: map[string]any{"name": "flow", "n": 3},
		Env:  func(k string) string { return map[string]string{"X": "envval"}[k] },
	}
	got, err := Eval("hi {{ $json.name }} / {{ $env.X }}", ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "hi flow / envval" {
		t.Fatalf("got %q", got)
	}
}

func TestEvalValueKeepsNativeType(t *testing.T) {
	ctx := Context{JSON: map[string]any{"n": 3}}
	v, err := EvalValue("{{ $json.n }}", ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 3 {
		t.Fatalf("got %#v want 3", v)
	}
}

func TestEvalNoExpressionIsUnchanged(t *testing.T) {
	got, err := Eval("plain text", Context{})
	if err != nil || got != "plain text" {
		t.Fatalf("got %q err %v", got, err)
	}
}
