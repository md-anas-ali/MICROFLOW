package engine

import (
	"errors"
	"fmt"
	"testing"
)

func TestPermanentErrors(t *testing.T) {
	base := errors.New("401 unauthorized")
	p := Permanent(base)
	if !IsPermanent(p) {
		t.Fatal("Permanent error not detected")
	}
	if !errors.Is(p, base) {
		t.Fatal("Permanent must unwrap to the original error")
	}
	if !IsPermanent(fmt.Errorf("wrapped: %w", p)) {
		t.Fatal("wrapped permanent error not detected")
	}
	if IsPermanent(base) {
		t.Fatal("plain error must not be permanent")
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
}
