// UNVERIFIED IN SANDBOX note does not apply here: signState/verifyState
// and AuthURL are pure logic with no network calls, so these run for
// real (unlike Exchange, which needs a real Google endpoint and is a
// local-machine test per the user's own plan).
package vault

import (
	"strings"
	"testing"
	"time"
)

func testApp() *GoogleOAuthApp {
	return NewGoogleOAuthApp("test-client-id", "test-client-secret", "https://example.com/api/oauth/google/callback")
}

func TestSignVerifyStateRoundTrip(t *testing.T) {
	app := testApp()
	for _, svc := range GoogleServices {
		state, err := app.signState(svc)
		if err != nil {
			t.Fatalf("signState(%q): %v", svc, err)
		}
		got, err := app.verifyState(state)
		if err != nil {
			t.Fatalf("verifyState round trip for %q failed: %v", svc, err)
		}
		if got != svc {
			t.Fatalf("verifyState returned service %q, want %q", got, svc)
		}
	}
}

func TestVerifyStateRejectsTamperedService(t *testing.T) {
	app := testApp()
	state, err := app.signState("gmail")
	if err != nil {
		t.Fatalf("signState: %v", err)
	}
	// Flip a character in the payload half (before the signature dot) --
	// simulates someone trying to swap which service a stolen/replayed
	// state token grants a connection for.
	dot := strings.LastIndexByte(state, '.')
	tampered := state[:dot-1] + flipChar(state[dot-1]) + state[dot:]
	if _, err := app.verifyState(tampered); err == nil {
		t.Fatal("expected tampered state to be rejected")
	}
}

func TestVerifyStateRejectsWrongSigningSecret(t *testing.T) {
	issuer := NewGoogleOAuthApp("cid", "secret-A", "https://example.com/callback")
	verifier := NewGoogleOAuthApp("cid", "secret-B", "https://example.com/callback")
	state, err := issuer.signState("youtube")
	if err != nil {
		t.Fatalf("signState: %v", err)
	}
	if _, err := verifier.verifyState(state); err == nil {
		t.Fatal("expected state signed with a different client secret to be rejected")
	}
}

func TestVerifyStateRejectsExpired(t *testing.T) {
	app := testApp()
	// Hand-build a state whose timestamp is well outside googleOAuthStateTTL,
	// signed with the same HMAC path signState uses, to test the TTL
	// check specifically (signState always uses "now").
	payload := "sheets|" + itoa(time.Now().Add(-2*googleOAuthStateTTL).Unix()) + "|deadbeef"
	state := signPayloadForTest(app, payload)
	if _, err := app.verifyState(state); err == nil {
		t.Fatal("expected expired state to be rejected")
	}
}

func TestVerifyStateRejectsUnknownService(t *testing.T) {
	app := testApp()
	payload := "not-a-real-service|" + itoa(time.Now().Unix()) + "|deadbeef"
	state := signPayloadForTest(app, payload)
	if _, err := app.verifyState(state); err == nil {
		t.Fatal("expected unknown service in state to be rejected")
	}
}

func TestAuthURLIncludesServiceScopesAndOfflineConsent(t *testing.T) {
	app := testApp()
	url, err := app.AuthURL("gmail")
	if err != nil {
		t.Fatalf("AuthURL: %v", err)
	}
	for _, want := range []string{
		"access_type=offline",
		"prompt=consent",
		"gmail.send",
	} {
		if !strings.Contains(url, want) {
			t.Errorf("AuthURL missing %q: %s", want, url)
		}
	}
}

func TestAuthURLUnknownService(t *testing.T) {
	app := testApp()
	if _, err := app.AuthURL("drive"); err == nil {
		t.Fatal("expected an error for an unsupported service")
	}
}

// --- small test-only helpers ---

func flipChar(b byte) string {
	if b == 'a' {
		return "b"
	}
	return "a"
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// signPayloadForTest signs an arbitrary payload the same way signState
// does, so tests can construct states with specific (e.g. expired)
// timestamps that signState itself (always "now") can't produce.
func signPayloadForTest(app *GoogleOAuthApp, payload string) string {
	return app.signRaw(payload)
}
