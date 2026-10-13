package engine

import (
	"crypto/sha256"
	"encoding/json"

	"microflow/internal/model"
)

// loopGuard detects a stuck cycle: one node receiving the byte-identical
// input more than `limit` times in a single pass. It is opt-in (limit 0
// disables it) because legitimate loops such as "Loop One By One" can
// replay identical inputs; MaxSteps stays the unconditional backstop.
//
// NOTE: this file was missing from the uploaded archive (engine.go called
// newLoopGuard but nothing defined it, so the baseline did not compile).
// It was reconstructed from the call site in RunWithCheckpoint.
type loopGuard struct {
	limit int
	seen  map[string]map[[32]byte]int
}

func newLoopGuard(limit int) *loopGuard {
	return &loopGuard{limit: limit, seen: map[string]map[[32]byte]int{}}
}

// Observe records one delivery of input to node and reports how many times
// this exact input has been seen and whether the limit was exceeded.
func (g *loopGuard) Observe(node string, input model.NodeOutput) (int, bool) {
	if g == nil || g.limit <= 0 {
		return 0, false
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return 0, false
	}
	sum := sha256.Sum256(raw)
	m := g.seen[node]
	if m == nil {
		m = map[[32]byte]int{}
		g.seen[node] = m
	}
	m[sum]++
	n := m[sum]
	return n, n > g.limit
}
