package vault

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
	vault      *Vault
	httpClient *http.Client
	// refreshMu prevents two concurrent node executions for the same
	// credential from both refreshing at once and racing to write the
	// vault (the underlying Postgres write is last-writer-wins; this
	// keeps it to one in-process writer at a time per logical name).
	refreshMu sync.Map // map[string]*sync.Mutex, key = workflowID+"/"+logicalName
}

func NewOAuthResolver(v *Vault) *OAuthResolver {
	return &OAuthResolver{vault: v, httpClient: &http.Client{Timeout: 15 * time.Second}}
}

const tokenExpiryLeeway = 2 * time.Minute // refresh a bit before actual expiry to avoid a request landing right on the boundary

func (r *OAuthResolver) Resolve(ctx context.Context, workflowID, logicalName string) (map[string]string, error) {
	secrets, err := r.vault.Resolve(ctx, workflowID, logicalName)
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

	lockKey := workflowID + "/" + logicalName
	muAny, _ := r.refreshMu.LoadOrStore(lockKey, &sync.Mutex{})
	mu := muAny.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	// Re-check after acquiring the lock: another goroutine may have
	// already refreshed while we were waiting.
	secrets, err = r.vault.Resolve(ctx, workflowID, logicalName)
	if err != nil {
		return nil, err
	}
	if !needsRefresh(secrets["expiresAt"]) {
		return secrets, nil
	}

	newAccess, newExpiresAt, err := r.refresh(ctx, secrets["clientId"], secrets["clientSecret"], refreshToken)
	if err != nil {
		return nil, fmt.Errorf("oauth: refresh failed for %q/%q: %w", workflowID, logicalName, err)
	}

	secrets["accessToken"] = newAccess
	secrets["expiresAt"] = strconv.FormatInt(newExpiresAt.Unix(), 10)
	if err := r.vault.Put(ctx, workflowID, logicalName, secrets); err != nil {
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
func (r *OAuthResolver) refresh(ctx context.Context, clientID, clientSecret, refreshToken string) (accessToken string, expiresAt time.Time, err error) {
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

	resp, err := r.httpClient.Do(req)
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
	if resp.StatusCode >= 300 || body.Error != "" {
		return "", time.Time{}, fmt.Errorf("token endpoint returned %d: %s: %s", resp.StatusCode, body.Error, body.ErrorDesc)
	}
	return body.AccessToken, time.Now().Add(time.Duration(body.ExpiresIn) * time.Second), nil
}
