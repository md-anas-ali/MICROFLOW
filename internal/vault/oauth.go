package vault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrGoogleReauthRequired is returned (wrapped, so errors.Is still
// matches) when Google's token endpoint rejects a refresh_token with
// "invalid_grant" -- i.e. the person revoked access, changed their
// Google password, or the refresh token otherwise died. There is no
// way to recover an accessToken in this state; the only fix is for the
// person to reconnect (see the Google Connections UI / handleGoogle*
// endpoints in internal/api), which is why callers translate this into
// "Google connection expired. Please reconnect." instead of the raw
// Google error.
var ErrGoogleReauthRequired = errors.New("google: refresh token invalid or revoked -- reconnect required")

// OAuthResolver wraps a Vault and transparently refreshes Google OAuth2
// access tokens before handing them to node executors, so
// internal/nodes/google.go never has to know about expiry/refresh --
// it just calls Resolve and gets a usable accessToken (implements
// engine.CredentialResolver, same interface as *Vault alone).
//
// Stored secret shape per logical credential (set once via
// `Vault.Put`, e.g. from a one-time OAuth consent flow run by an
// operator CLI/setup step -- that initial consent flow itself is out
// of scope here, same as it is for n8n's own credential setup UI):
//
//	{
//	  "accessToken":  "...",
//	  "refreshToken": "...",
//	  "expiresAt":    "1737498234"  // unix seconds, string (secrets map is map[string]string)
//	  "clientId":     "...",
//	  "clientSecret": "...",
//	  "tokenType":    "Bearer"
//	}
type OAuthResolver struct {
	vault *Vault
	eng   *refreshEngine
}

func NewOAuthResolver(v *Vault) *OAuthResolver {
	return &OAuthResolver{vault: v, eng: newRefreshEngine()}
}

const tokenExpiryLeeway = 2 * time.Minute // refresh a bit before actual expiry to avoid a request landing right on the boundary

func (r *OAuthResolver) Resolve(ctx context.Context, workflowID, logicalName string) (map[string]string, error) {
	lockKey := workflowID + "/" + logicalName
	return r.eng.resolve(ctx, lockKey,
		func(ctx context.Context) (map[string]string, error) {
			return r.vault.Resolve(ctx, workflowID, logicalName)
		},
		func(ctx context.Context, secrets map[string]string) error {
			return r.vault.Put(ctx, workflowID, logicalName, secrets)
		},
	)
}

// refreshEngine is the storage-agnostic "read secrets, refresh the
// access token if it's near/at expiry, write the refreshed secrets
// back" algorithm. It knows nothing about *how* secrets are persisted
// (that's the get/put closures its caller supplies) so both
// OAuthResolver (per-workflow/node credentials, keyed by
// workflowID+"/"+logicalName) and AccountResolver (the single central
// Google account credential, see central.go) share exactly one
// implementation of the refresh logic and its concurrency guard,
// instead of two copies that could drift apart.
type refreshEngine struct {
	httpClient *http.Client
	// refreshMu prevents two concurrent node executions for the same
	// credential from both refreshing at once and racing to write the
	// vault (the underlying Postgres write is last-writer-wins; this
	// keeps it to one in-process writer at a time per lock key).
	refreshMu sync.Map // map[string]*sync.Mutex
}

func newRefreshEngine() *refreshEngine {
	return &refreshEngine{httpClient: &http.Client{Timeout: 15 * time.Second}}
}

func (e *refreshEngine) resolve(
	ctx context.Context,
	lockKey string,
	get func(ctx context.Context) (map[string]string, error),
	put func(ctx context.Context, secrets map[string]string) error,
) (map[string]string, error) {
	secrets, err := get(ctx)
	if err != nil {
		return nil, err
	}

	// Non-OAuth credentials (plain API keys) have no refreshToken; pass
	// through unchanged.
	refreshToken := secrets["refreshToken"]
	if refreshToken == "" {
		return secrets, nil
	}

	if !needsRefresh(secrets["expiresAt"]) {
		return secrets, nil
	}

	muAny, _ := e.refreshMu.LoadOrStore(lockKey, &sync.Mutex{})
	mu := muAny.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	// Re-check after acquiring the lock: another goroutine may have
	// already refreshed while we were waiting.
	secrets, err = get(ctx)
	if err != nil {
		return nil, err
	}
	if !needsRefresh(secrets["expiresAt"]) {
		return secrets, nil
	}

	newAccess, newExpiresAt, err := e.refresh(ctx, secrets["clientId"], secrets["clientSecret"], refreshToken)
	if err != nil {
		return nil, fmt.Errorf("oauth: refresh failed for %q: %w", lockKey, err)
	}

	secrets["accessToken"] = newAccess
	secrets["expiresAt"] = strconv.FormatInt(newExpiresAt.Unix(), 10)
	if err := put(ctx, secrets); err != nil {
		// Non-fatal: we still have a valid in-memory token for this call,
		// even though persisting the refreshed token failed. Log-worthy
		// but the caller should proceed with the token it has.
		return secrets, fmt.Errorf("oauth: refreshed token but failed to persist it (will re-refresh next call): %w", err)
	}
	return secrets, nil
}

func needsRefresh(expiresAtStr string) bool {
	if expiresAtStr == "" {
		return true // unknown expiry: be conservative and refresh
	}
	sec, err := strconv.ParseInt(expiresAtStr, 10, 64)
	if err != nil {
		return true
	}
	return time.Now().Add(tokenExpiryLeeway).After(time.Unix(sec, 0))
}

// refresh calls Google's token endpoint directly (stdlib net/http +
// net/url only -- no golang.org/x/oauth2 dependency, since this was
// written where fetching an extra module wasn't possible; feel free to
// swap in x/oauth2 locally if you prefer it).
func (e *refreshEngine) refresh(ctx context.Context, clientID, clientSecret, refreshToken string) (accessToken string, expiresAt time.Time, err error) {
	if clientID == "" || clientSecret == "" {
		return "", time.Time{}, fmt.Errorf("missing clientId/clientSecret on stored credential")
	}
	form := url.Values{
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"refresh_token": {refreshToken},
		"grant_type":    {"refresh_token"},
	}
	req, err := http.NewRequestWithContext(ctx, "POST", "https://oauth2.googleapis.com/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()

	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		Error       string `json:"error"`
		ErrorDesc   string `json:"error_description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", time.Time{}, fmt.Errorf("decode token response: %w", err)
	}
	if body.Error == "invalid_grant" {
		return "", time.Time{}, fmt.Errorf("token endpoint rejected refresh token: %w", ErrGoogleReauthRequired)
	}
	if resp.StatusCode >= 300 || body.Error != "" {
		return "", time.Time{}, fmt.Errorf("token endpoint returned %d: %s: %s", resp.StatusCode, body.Error, body.ErrorDesc)
	}
	return body.AccessToken, time.Now().Add(time.Duration(body.ExpiresIn) * time.Second), nil
}
