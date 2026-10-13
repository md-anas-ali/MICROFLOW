package vault

import (
	"net/url"
	"strings"
	"testing"
)

func scopesFor(t *testing.T, env map[string]string, service string) string {
	t.Helper()
	get := func(k string) string { return env[k] }
	app, ok := GoogleOAuthAppFromEnv(get)
	if !ok {
		t.Fatal("app not built")
	}
	raw, err := app.AuthURL("svc", service)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(raw)
	return u.Query().Get("scope")
}

func TestAnalyticsScopeIsOptIn(t *testing.T) {
	base := map[string]string{"GOOGLE_OAUTH_CLIENT_ID": "id", "GOOGLE_OAUTH_CLIENT_SECRET": "secret", "GOOGLE_OAUTH_REDIRECT_URL": "https://x.example/cb"}
	if s := scopesFor(t, base, "youtube"); strings.Contains(s, "yt-analytics") {
		t.Fatalf("default consent must be unchanged: %s", s)
	}
	on := map[string]string{YouTubeAnalyticsScopeEnvKey: "true"}
	for k, v := range base {
		on[k] = v
	}
	if s := scopesFor(t, on, "youtube"); !strings.Contains(s, YouTubeAnalyticsScope) || !strings.Contains(s, "auth/youtube.upload") {
		t.Fatalf("opt-in must add the read-only scope and keep existing ones: %s", s)
	}
	if s := scopesFor(t, on, "gmail"); strings.Contains(s, "yt-analytics") || strings.Contains(s, "youtube") {
		t.Fatalf("other services must never receive it: %s", s)
	}
}
