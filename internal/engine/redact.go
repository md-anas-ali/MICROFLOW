package engine

import (
	"os"
	"regexp"
	"strings"
	"sync"

	"microflow/internal/model"
)

// RedactedPlaceholder replaces any secret value the redactor catches,
// wherever it's about to be persisted, logged, or shown in the API/UI.
const RedactedPlaceholder = "***REDACTED***"

// secretKeyPattern matches JSON object keys / map keys that name a
// secret by convention (geminiApiKey, openrouterApiKey, youtubeDataApiKey,
// clientSecret, accessToken, refreshToken, Authorization, password, ...).
// Case-insensitive, deliberately broad: false positives (redacting a
// harmless field that happens to match) are cheap; false negatives (a
// real secret slipping through) are not.
var secretKeyPattern = regexp.MustCompile(`(?i)(api[_-]?key|apikey|client[_-]?secret|access[_-]?token|refresh[_-]?token|auth[_-]?token|bearer[_-]?token|private[_-]?key|secret[_-]?key|\bsecret\b|\btoken\b|password|passwd|\bcredential)`)

// envSecretPattern is the same idea applied to *environment variable
// names*, used only to decide which env values to pre-seed into the
// redactor at run start (so a leak is caught even before any node
// explicitly reads the value into JSON, e.g. a stray console.log of
// $env.GEMINI_API_KEY before Config Center ever runs).
var envSecretPattern = regexp.MustCompile(`(?i)(API[_-]?KEY|SECRET|TOKEN|PASSWORD|PASSWD|PRIVATE[_-]?KEY|CREDENTIAL)`)

// SecretRedactor scrubs secret values out of anything about to be
// persisted (execution history), served over the API, or written to the
// server log -- while leaving the *live* execution data (rc.nodeOutputs,
// used for $('Node').item.json expressions) completely untouched, so
// nodes downstream of Config Center can still use real key values to
// make authenticated calls. It works two ways at once:
//
//  1. Key-name based: any map key matching secretKeyPattern has its
//     value replaced outright (covers Config Center's geminiApiKey,
//     openrouterApiKey, youtubeDataApiKey, and any credential-shaped
//     field in any other node's output).
//  2. Value based: every secret value it has ever seen (from #1, or
//     pre-seeded from matching environment variables at run start) is
//     also scrubbed out of every OTHER string it touches -- so a key
//     echoed inside an Authorization header, an error message, or a
//     console.log call is caught too, not just the field it was
//     originally read from.
type SecretRedactor struct {
	mu     sync.RWMutex
	values map[string]struct{}
}

// NewSecretRedactor returns an empty redactor (key-name based redaction
// still applies; value-propagation redaction only kicks in once a
// secret value has actually been seen).
func NewSecretRedactor() *SecretRedactor {
	return &SecretRedactor{values: map[string]struct{}{}}
}

// NewSecretRedactorFromEnv seeds a redactor with the value of every
// environment variable whose *name* looks secret-shaped (per
// envSecretPattern), so leaks are caught from the very first node of a
// run, not only after a Config Center-style node has read the value
// into JSON. Called once per run (see internal/runner).
func NewSecretRedactorFromEnv() *SecretRedactor {
	r := NewSecretRedactor()
	for _, kv := range os.Environ() {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if envSecretPattern.MatchString(name) {
			r.noteValue(value)
		}
	}
	return r
}

// minSecretLen avoids blacklisting trivial/common short strings (empty,
// "0", "true", ...) that could appear incidentally all over normal
// output and would make redaction useless-noisy if treated as secrets.
const minSecretLen = 6

func (r *SecretRedactor) noteValue(v string) {
	v = strings.TrimSpace(v)
	if len(v) < minSecretLen {
		return
	}
	r.mu.Lock()
	r.values[v] = struct{}{}
	r.mu.Unlock()
}

// RedactString scrubs every previously-noted secret value out of s.
func (r *SecretRedactor) RedactString(s string) string {
	if r == nil || s == "" {
		return s
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for v := range r.values {
		if strings.Contains(s, v) {
			s = strings.ReplaceAll(s, v, RedactedPlaceholder)
		}
	}
	return s
}

// redactAny walks an arbitrary JSON-shaped value (map[string]any,
// []any, or a scalar) and returns a redacted deep copy: keys matching
// secretKeyPattern have their (string) value replaced outright (and
// noted for value-propagation), everything else is recursed into /
// value-scrubbed. The input is never mutated.
func (r *SecretRedactor) redactAny(v any) any {
	if r == nil {
		return v
	}
	switch val := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, sub := range val {
			if secretKeyPattern.MatchString(k) {
				if s, ok := sub.(string); ok && s != "" {
					r.noteValue(s)
					out[k] = RedactedPlaceholder
					continue
				}
			}
			out[k] = r.redactAny(sub)
		}
		return out
	case []any:
		out := make([]any, len(val))
		for i, sub := range val {
			out[i] = r.redactAny(sub)
		}
		return out
	case string:
		return r.RedactString(val)
	default:
		return v
	}
}

// RedactJSON returns a redacted deep copy of a node item's json map.
func (r *SecretRedactor) RedactJSON(m map[string]any) map[string]any {
	if r == nil || m == nil {
		return m
	}
	// Two passes: the first pass's noteValue calls (from key-name
	// matches anywhere in the map) must be visible to string-scrubbing
	// of sibling fields regardless of map iteration order, so we redact
	// twice -- the second pass catches a same-node leak whose secret-
	// value-bearing key happened to be processed after the field that
	// echoes it.
	first := r.redactAny(m)
	second := r.redactAny(first)
	out, _ := second.(map[string]any)
	if out == nil {
		return map[string]any{}
	}
	return out
}

// RedactItems returns a redacted deep copy of a slice of items (binary
// refs are left as-is -- they're file paths/metadata on disk, not
// secret values, and are never logged as raw bytes).
func (r *SecretRedactor) RedactItems(items []model.Item) []model.Item {
	if r == nil || items == nil {
		return items
	}
	out := make([]model.Item, len(items))
	for i, it := range items {
		out[i] = model.Item{JSON: r.RedactJSON(it.JSON), Binary: it.Binary}
	}
	return out
}

// RedactOutput returns a redacted deep copy of a full NodeOutput (all
// output branches), for safe storage in Execution.NodeRuns / the API
// response / the frontend inspector.
func (r *SecretRedactor) RedactOutput(out model.NodeOutput) model.NodeOutput {
	if r == nil || out == nil {
		return out
	}
	redacted := make(model.NodeOutput, len(out))
	for i, branch := range out {
		redacted[i] = r.RedactItems(branch)
	}
	return redacted
}
