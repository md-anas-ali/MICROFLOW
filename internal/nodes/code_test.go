package nodes

import "testing"

// TestCodeExecutorEnvAllowlist verifies that $env only ever exposes the
// operator-configured allowlist (security rule 11/22): allowlisted vars
// that are set come through, an allowlisted var that is unset is simply
// absent (not an empty string), and a real env var that is NOT on the
// allowlist never leaks in, even though it's set in the process
// environment right next to the allowlisted ones.
func TestCodeExecutorEnvAllowlist(t *testing.T) {
	t.Setenv("OPENROUTER_API_KEY", "sk-test-openrouter")
	t.Setenv("GEMINI_API_KEY", "sk-test-gemini")
	t.Setenv("YOUTUBE_DATA_API_KEY", "yt-test-key")
	t.Setenv("APP_REFERER_URL", "https://example.com")
	t.Setenv("NOTIFY_WEBHOOK_URL", "https://hooks.example.com/notify")
	// Deliberately NOT on the allowlist, but set in the process env --
	// must never appear in the result.
	t.Setenv("MICROFLOW_MASTER_KEY", "super-secret-should-not-leak")
	t.Setenv("DATABASE_URL", "postgres://should/not/leak")

	e := CodeExecutor{EnvAllowlist: []string{
		"OPENROUTER_API_KEY",
		"GEMINI_API_KEY",
		"YOUTUBE_DATA_API_KEY",
		"APP_REFERER_URL",
		"NOTIFY_WEBHOOK_URL",
		"SOME_ALLOWLISTED_BUT_UNSET_VAR",
	}}

	got := e.envAllowlist()

	want := map[string]string{
		"OPENROUTER_API_KEY":   "sk-test-openrouter",
		"GEMINI_API_KEY":       "sk-test-gemini",
		"YOUTUBE_DATA_API_KEY": "yt-test-key",
		"APP_REFERER_URL":      "https://example.com",
		"NOTIFY_WEBHOOK_URL":   "https://hooks.example.com/notify",
	}
	if len(got) != len(want) {
		t.Fatalf("envAllowlist() = %v (len %d), want %v (len %d)", got, len(got), want, len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("envAllowlist()[%q] = %q, want %q", k, got[k], v)
		}
	}
	if _, present := got["SOME_ALLOWLISTED_BUT_UNSET_VAR"]; present {
		t.Errorf("envAllowlist() exposed unset allowlisted var as a key: %v", got)
	}
	if _, present := got["MICROFLOW_MASTER_KEY"]; present {
		t.Fatalf("envAllowlist() leaked a non-allowlisted env var: %v", got)
	}
	if _, present := got["DATABASE_URL"]; present {
		t.Fatalf("envAllowlist() leaked a non-allowlisted env var: %v", got)
	}
}

// TestCodeExecutorEnvAllowlistEmptyByDefault verifies the fail-closed
// default: a CodeExecutor with no EnvAllowlist configured exposes
// nothing, even if the process environment has plenty of variables set.
func TestCodeExecutorEnvAllowlistEmptyByDefault(t *testing.T) {
	t.Setenv("SOME_RANDOM_VAR", "value")

	e := CodeExecutor{}
	got := e.envAllowlist()
	if len(got) != 0 {
		t.Fatalf("envAllowlist() with no configured allowlist = %v, want empty", got)
	}
}
