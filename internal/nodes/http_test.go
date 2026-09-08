package nodes

import (
	"testing"
	"time"
)

func TestIsCacheableGETURL(t *testing.T) {
	cases := []struct {
		url  string
		want bool
	}{
		{"https://openrouter.ai/api/v1/models", true},
		{"https://openrouter.ai/api/v1/chat/completions", false}, // different path -- must NOT be cached
		{"https://example.com/api/v1/models", false},             // different host
		{"not a url", false},
	}
	for _, c := range cases {
		if got := isCacheableGETURL(c.url); got != c.want {
			t.Errorf("isCacheableGETURL(%q) = %v, want %v", c.url, got, c.want)
		}
	}
}

func TestGetResponseCache_HitAndMiss(t *testing.T) {
	c := newGetResponseCache(time.Minute)
	url := "https://openrouter.ai/api/v1/models"

	if _, ok := c.get(url); ok {
		t.Fatal("expected miss on empty cache")
	}

	value := map[string]any{"data": []any{map[string]any{"id": "a/model:free"}}}
	c.set(url, value)

	got, ok := c.get(url)
	if !ok {
		t.Fatal("expected hit after set")
	}
	if len(got) != len(value) {
		t.Fatalf("cached value mismatch: got %#v want %#v", got, value)
	}
}

func TestGetResponseCache_Expiry(t *testing.T) {
	c := newGetResponseCache(10 * time.Millisecond)
	url := "https://openrouter.ai/api/v1/models"
	c.set(url, map[string]any{"data": []any{}})

	if _, ok := c.get(url); !ok {
		t.Fatal("expected hit immediately after set")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := c.get(url); ok {
		t.Fatal("expected miss after TTL expiry -- a stale free-model list must never be served past its TTL")
	}
}

// TestGetResponseCache_BoundedCapacity confirms the cache can never grow
// without bound (rule: nothing unbounded) -- once full, new distinct
// keys are simply not admitted rather than evicting/growing forever.
func TestGetResponseCache_BoundedCapacity(t *testing.T) {
	c := newGetResponseCache(time.Minute)
	for i := 0; i < maxCachedGETEntries+5; i++ {
		c.set(urlFor(i), map[string]any{"n": i})
	}
	if len(c.entries) > maxCachedGETEntries {
		t.Fatalf("cache grew past cap: %d entries, want <= %d", len(c.entries), maxCachedGETEntries)
	}
}

func urlFor(i int) string {
	return "https://openrouter.ai/api/v1/models?fake=" + string(rune('a'+i))
}

// TestGetResponseCache_NilSafe confirms a nil *getResponseCache (the
// zero value HTTPRequestExecutor.modelListCache has if never wired, as
// in older tests/other callers) behaves as "always miss / never store"
// rather than panicking.
func TestGetResponseCache_NilSafe(t *testing.T) {
	var c *getResponseCache
	if _, ok := c.get("https://openrouter.ai/api/v1/models"); ok {
		t.Fatal("nil cache must always report a miss")
	}
	c.set("https://openrouter.ai/api/v1/models", map[string]any{}) // must not panic
}
