package engine

import (
	"testing"

	"microflow/internal/model"
)

// TestRedactJSONKeyNameMatch verifies that fields named like secrets
// (Config Center's geminiApiKey/openrouterApiKey/youtubeDataApiKey
// pattern) are replaced outright, while ordinary fields pass through
// unchanged.
func TestRedactJSONKeyNameMatch(t *testing.T) {
	r := NewSecretRedactor()
	in := map[string]any{
		"geminiApiKey":      "sk-real-gemini-secret-value",
		"openrouterApiKey":  "sk-real-openrouter-secret-value",
		"youtubeDataApiKey": "AIzaReallySecretYouTubeKeyValue",
		"notifyWebhookUrl":  "https://example.com/hook",
		"topic":             "obscure history facts",
	}
	out := r.RedactJSON(in)

	for _, k := range []string{"geminiApiKey", "openrouterApiKey", "youtubeDataApiKey"} {
		if out[k] != RedactedPlaceholder {
			t.Errorf("field %q = %v, want redacted placeholder", k, out[k])
		}
	}
	if out["notifyWebhookUrl"] != "https://example.com/hook" {
		t.Errorf("unrelated field notifyWebhookUrl was altered: %v", out["notifyWebhookUrl"])
	}
	if out["topic"] != "obscure history facts" {
		t.Errorf("unrelated field topic was altered: %v", out["topic"])
	}
	// The original map must be untouched -- callers (recordNodeOutput)
	// need the real values for expression evaluation.
	if in["geminiApiKey"] != "sk-real-gemini-secret-value" {
		t.Fatal("RedactJSON mutated the input map; must return a copy")
	}
}

// TestRedactJSONValuePropagation verifies that once a secret value has
// been seen under a secret-shaped key, that exact value is also
// scrubbed out of any OTHER field it appears in -- e.g. an
// Authorization header string built as "Bearer <key>", or an error
// message that happens to echo the key back.
func TestRedactJSONValuePropagation(t *testing.T) {
	r := NewSecretRedactor()
	in := map[string]any{
		"geminiApiKey": "sk-leak-me-1234567890",
		"headers": map[string]any{
			"Authorization": "Bearer sk-leak-me-1234567890",
		},
		"debugNote": "used key sk-leak-me-1234567890 for this call",
	}
	out := r.RedactJSON(in)

	headers, _ := out["headers"].(map[string]any)
	if headers == nil {
		t.Fatal("headers field missing/wrong type after redaction")
	}
	if got := headers["Authorization"]; got != "Bearer "+RedactedPlaceholder {
		t.Errorf("Authorization header = %q, want secret value scrubbed", got)
	}
	if got := out["debugNote"]; got != "used key "+RedactedPlaceholder+" for this call" {
		t.Errorf("debugNote = %q, want secret value scrubbed", got)
	}
}

// TestRedactOutputPreservesShapeAndBinary verifies RedactOutput deep-
// copies every branch/item, redacts JSON, and leaves binary refs
// (file paths/metadata, never raw secret bytes) untouched.
func TestRedactOutputPreservesShapeAndBinary(t *testing.T) {
	r := NewSecretRedactor()
	out := model.NodeOutput{
		{
			{JSON: map[string]any{"apiKey": "super-secret-value-here"}, Binary: map[string]model.BinaryRef{
				"data": {FileName: "/tmp/x.jpg", MimeType: "image/jpeg"},
			}},
		},
		{
			{JSON: map[string]any{"ok": true}},
		},
	}
	redacted := r.RedactOutput(out)

	if len(redacted) != 2 || len(redacted[0]) != 1 || len(redacted[1]) != 1 {
		t.Fatalf("RedactOutput changed shape: %+v", redacted)
	}
	if redacted[0][0].JSON["apiKey"] != RedactedPlaceholder {
		t.Errorf("apiKey not redacted: %v", redacted[0][0].JSON["apiKey"])
	}
	if redacted[0][0].Binary["data"].FileName != "/tmp/x.jpg" {
		t.Errorf("binary ref altered: %+v", redacted[0][0].Binary)
	}
	// Original untouched.
	if out[0][0].JSON["apiKey"] != "super-secret-value-here" {
		t.Fatal("RedactOutput mutated the original NodeOutput")
	}
}

// TestSecretRedactorNilSafe verifies every method is a safe no-op on a
// nil *SecretRedactor, matching MemGuard's documented nil-is-safe
// pattern elsewhere in this package -- a Runner/RunContext that
// doesn't set one must not panic.
func TestSecretRedactorNilSafe(t *testing.T) {
	var r *SecretRedactor
	if got := r.RedactString("hello secret"); got != "hello secret" {
		t.Errorf("RedactString on nil = %q, want unchanged", got)
	}
	m := map[string]any{"apiKey": "x"}
	if got := r.RedactJSON(m); got["apiKey"] != "x" {
		t.Errorf("RedactJSON on nil = %v, want unchanged", got)
	}
	out := model.NodeOutput{{{JSON: map[string]any{"a": 1}}}}
	if got := r.RedactOutput(out); got[0][0].JSON["a"] != 1 {
		t.Errorf("RedactOutput on nil = %v, want unchanged", got)
	}
}

// TestNewSecretRedactorFromEnvSeedsMatchingNames verifies env-based
// seeding only picks up secret-shaped variable *names* (API_KEY,
// SECRET, TOKEN, PASSWORD, ...), not arbitrary env vars, and that a
// seeded value is then caught wherever it appears.
func TestNewSecretRedactorFromEnvSeedsMatchingNames(t *testing.T) {
	t.Setenv("GEMINI_API_KEY", "env-seeded-secret-abcdef")
	t.Setenv("APP_REFERER_URL", "https://example.com") // should NOT be seeded

	r := NewSecretRedactorFromEnv()
	msg := r.RedactString("using key env-seeded-secret-abcdef now")
	if msg != "using key "+RedactedPlaceholder+" now" {
		t.Errorf("env-seeded secret not redacted: %q", msg)
	}
	msg2 := r.RedactString("referer is https://example.com")
	if msg2 != "referer is https://example.com" {
		t.Errorf("non-secret env value was incorrectly redacted: %q", msg2)
	}
}
