package engine

import (
	"os"
	"testing"
)

func TestRunContextEnvPrecedence(t *testing.T) {
	t.Setenv("MF_PREC_KEY", "process")
	rc := &RunContext{}
	if v := rc.Env("MF_PREC_KEY"); v != "process" {
		t.Fatalf("want process, got %q", v)
	}
	rc.GlobalEnv = map[string]string{"MF_PREC_KEY": "global"}
	if v := rc.Env("MF_PREC_KEY"); v != "global" {
		t.Fatalf("global must beat process, got %q", v)
	}
	rc.ServiceEnv = map[string]string{"MF_PREC_KEY": "service"}
	if v, ok := rc.LookupEnv("MF_PREC_KEY"); !ok || v != "service" {
		t.Fatalf("service must beat global, got %q", v)
	}
	os.Unsetenv("MF_PREC_KEY")
	rc.ServiceEnv, rc.GlobalEnv = nil, nil
	if _, ok := rc.LookupEnv("MF_PREC_KEY"); ok {
		t.Fatal("unset everywhere must report not-found")
	}
}
