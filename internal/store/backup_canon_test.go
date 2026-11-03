package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"testing"
)

func TestBackupCanonical(t *testing.T) {
	orig := []byte(`{"b":[{"z":"a<b && c>d \u0001 \\u003c  ","a":1.50}],"a":null}`)
	// what Go's old handler wrote
	esc, _ := json.Marshal(json.RawMessage(orig))
	if bytes.Equal(esc, orig) {
		t.Fatal("expected escaping")
	}
	l1, _ := canonicalize(orig, canonLegacy)
	l2, _ := canonicalize(esc, canonLegacy)
	l3, _ := canonicalize([]byte(" {\n \"b\" : "+string(orig[5:])), canonLegacy)
	if !bytes.Equal(l1, l2) || !bytes.Equal(l1, l3) {
		t.Fatalf("legacy differs\n%s\n%s", l1, l2)
	}
	if !bytes.Equal(l1, orig) {
		t.Fatalf("legacy != original bytes\n%s", l1)
	}
	// c2
	reorder := []byte(`{ "a":null, "b":[{"a":1.50,"z":"a<b && c>d \u0001 \\u003c  "}] }`)
	c1, err := canonicalize(orig, canonV2)
	if err != nil {
		t.Fatal(err)
	}
	c2, _ := canonicalize(reorder, canonV2)
	c3, _ := canonicalize(esc, canonV2)
	if !bytes.Equal(c1, c2) || !bytes.Equal(c1, c3) {
		t.Fatalf("c2 differs\n%s\n%s", c1, c2)
	}
	_ = sha256.New
	if _, err := canonicalize([]byte(`{"a":1} x`), canonV2); err == nil {
		t.Fatal("trailing")
	}
	// tamper must change digest
	tam, _ := canonicalize([]byte(`{"b":[{"z":"X","a":1.50}],"a":null}`), canonV2)
	if bytes.Equal(c1, tam) {
		t.Fatal("tamper undetected")
	}
}
