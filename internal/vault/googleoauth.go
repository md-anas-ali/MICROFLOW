package vault

// n8n-style "Connect with Google" -- the Authorization Code flow that
// produces exactly the secrets shape oauth.go/central.go already know
// how to store/refresh (clientId/clientSecret/refreshToken/...). Before
// this file, the only way to populate that shape was to paste a
// clientId/clientSecret/refreshToken by hand (cmd/setcred or the old
// manual-paste "Google Credentials" page) -- this is the piece that was
// previously out of scope. Nothing about HOW a credential is stored,
// refreshed, or handed to node executors changes: this only automates
// producing the refreshToken/accessToken/email a person would otherwise
// have had to obtain themselves via Google's OAuth Playground.

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	googleAuthEndpoint     = "https://accounts.google.com/o/oauth2/v2/auth"
	googleTokenEndpoint    = "https://oauth2.googleapis.com/token"
	googleUserinfoEndpoint = "https://www.googleapis.com/oauth2/v2/userinfo"

	// googleOAuthStateTTL bounds how long a "Connect" link is valid for
	// before the person must click Connect again -- long enough to get
	// through Google's account picker + consent screen, short enough
	// that an old, leaked callback URL isn't replayable indefinitely.
	googleOAuthStateTTL = 10 * time.Minute
)

// GoogleServiceScopes are the minimum OAuth scopes each connectable
// service needs, kept separate per service (rather than one combined
// scope list) so connecting Gmail never asks Google for YouTube/Sheets
// access and vice versa -- required by the "connect each service
// independently" design. userinfo.email is added to every request (see
// AuthURL) purely so the "Connected as: user@gmail.com" UI has an email
// to show; it grants no access to mail content.
var GoogleServiceScopes = map[string][]string{
	"gmail":   {"https://www.googleapis.com/auth/gmail.send"},
	"youtube": {"https://www.googleapis.com/auth/youtube.upload", "https://www.googleapis.com/auth/youtube"},
	"sheets":  {"https://www.googleapis.com/auth/spreadsheets"},
}

// GoogleOAuthApp holds one MicroFlow-wide Google OAuth 2.0 client
// (Client ID/Secret/redirect URI registered in Google Cloud Console --
// see the server's setup instructions). The SAME client is used for
// all three services; only the requested scopes differ per connect
// request, which is normal OAuth "incremental authorization".
type GoogleOAuthApp struct {
	ClientID     string
	ClientSecret string
	// RedirectURL must be the exact callback URL registered in Google
	// Cloud Console (e.g. "https://your-app.example.com/api/oauth/google/callback").
	RedirectURL string
	httpClient  *http.Client
}

// GoogleOAuthAppFromEnv builds a GoogleOAuthApp from
// GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_CLIENT_SECRET /
// GOOGLE_OAUTH_REDIRECT_URL. ok is false (nil app) if any are unset, so
// callers can run with the "Connect with Google" buttons disabled
// instead of failing to start -- the manual paste-a-refresh-token path
// (cmd/setcred / the legacy central credentials endpoint) still works
// without these env vars.
func GoogleOAuthAppFromEnv(getenv func(string) string) (app *GoogleOAuthApp, ok bool) {
	id := strings.TrimSpace(getenv("GOOGLE_OAUTH_CLIENT_ID"))
	secret := strings.TrimSpace(getenv("GOOGLE_OAUTH_CLIENT_SECRET"))
	redirect := strings.TrimSpace(getenv("GOOGLE_OAUTH_REDIRECT_URL"))
	if id == "" || secret == "" || redirect == "" {
		return nil, false
	}
	return NewGoogleOAuthApp(id, secret, redirect), true
}

func NewGoogleOAuthApp(clientID, clientSecret, redirectURL string) *GoogleOAuthApp {
	return &GoogleOAuthApp{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURL,
		httpClient:   &http.Client{Timeout: 15 * time.Second},
	}
}

// AuthURL builds the Google consent-screen URL for the given service.
// access_type=offline + prompt=consent guarantee a refresh_token comes
// back on EVERY connect (not just the first time an account has ever
// granted access) -- required both for the initial Connect and for
// Reconnect after a revoked/expired token to actually produce a fresh,
// usable refresh_token. select_account additionally forces Google's
// account chooser even if the browser is only signed into one account,
// matching n8n's "choose which Google account" step.
func (g *GoogleOAuthApp) AuthURL(service string) (string, error) {
	scopes, ok := GoogleServiceScopes[service]
	if !ok {
		return "", fmt.Errorf("vault: unknown google service %q", service)
	}
	allScopes := append([]string{"openid", "https://www.googleapis.com/auth/userinfo.email"}, scopes...)
	state, err := g.signState(service)
	if err != nil {
		return "", err
	}
	q := url.Values{
		"client_id":              {g.ClientID},
		"redirect_uri":           {g.RedirectURL},
		"response_type":          {"code"},
		"scope":                  {strings.Join(allScopes, " ")},
		"access_type":            {"offline"},
		"prompt":                 {"consent select_account"},
		"include_granted_scopes": {"true"},
		"state":                  {state},
	}
	return googleAuthEndpoint + "?" + q.Encode(), nil
}

// signState produces a stateless, tamper-evident state parameter
// ("service|unixTimestamp|nonce" + HMAC-SHA256 signature, base64url
// throughout) instead of a server-side session/table: MicroFlow's API
// is otherwise stateless, and a signed token avoids adding storage just
// for a short-lived CSRF value. Signed with the OAuth client secret,
// which only this server knows.
func (g *GoogleOAuthApp) signState(service string) (string, error) {
	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	payload := service + "|" + strconv.FormatInt(time.Now().Unix(), 10) + "|" + base64.RawURLEncoding.EncodeToString(nonce)
	return g.signRaw(payload), nil
}

// signRaw HMAC-signs an arbitrary "service|timestamp|nonce" payload and
// returns the full base64url(payload) + "." + base64url(signature)
// state token. Split out from signState so tests can construct states
// with specific (e.g. deliberately expired) timestamps.
func (g *GoogleOAuthApp) signRaw(payload string) string {
	mac := hmac.New(sha256.New, []byte(g.ClientSecret))
	mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + sig
}

// verifyState checks the HMAC signature and TTL and returns the
// service the state was originally issued for.
func (g *GoogleOAuthApp) verifyState(state string) (service string, err error) {
	dot := strings.LastIndexByte(state, '.')
	if dot < 0 {
		return "", errors.New("malformed state")
	}
	payloadPart, sigPart := state[:dot], state[dot+1:]
	payload, err := base64.RawURLEncoding.DecodeString(payloadPart)
	if err != nil {
		return "", errors.New("malformed state")
	}
	mac := hmac.New(sha256.New, []byte(g.ClientSecret))
	mac.Write(payload)
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(sigPart)) {
		return "", errors.New("state signature mismatch")
	}
	fields := strings.SplitN(string(payload), "|", 3)
	if len(fields) != 3 {
		return "", errors.New("malformed state payload")
	}
	if !IsGoogleService(fields[0]) {
		return "", fmt.Errorf("unknown service %q in state", fields[0])
	}
	ts, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return "", errors.New("malformed state timestamp")
	}
	if time.Since(time.Unix(ts, 0)) > googleOAuthStateTTL {
		return "", errors.New("connect link expired -- please click Connect again")
	}
	return fields[0], nil
}

// googleTokenResponse is Google's token-endpoint response shape
// (POST with grant_type=authorization_code).
type googleTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Error        string `json:"error"`
	ErrorDesc    string `json:"error_description"`
}

// Exchange completes the Authorization Code flow for a callback GET
// request's `code` and `state` query params: verifies state, swaps the
// code for tokens, fetches the connected account's email, and returns
// the secrets map ready for GoogleServiceAccounts.Put -- the exact
// shape OAuthResolver/AccountResolver already know how to refresh.
func (g *GoogleOAuthApp) Exchange(ctx context.Context, code, state string) (service string, secrets map[string]string, err error) {
	service, err = g.verifyState(state)
	if err != nil {
		return "", nil, err
	}

	form := url.Values{
		"client_id":     {g.ClientID},
		"client_secret": {g.ClientSecret},
		"code":          {code},
		"redirect_uri":  {g.RedirectURL},
		"grant_type":    {"authorization_code"},
	}
	req, err := http.NewRequestWithContext(ctx, "POST", googleTokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("contacting google: %w", err)
	}
	defer resp.Body.Close()

	var body googleTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", nil, fmt.Errorf("decoding google token response: %w", err)
	}
	if resp.StatusCode >= 300 || body.Error != "" {
		return "", nil, fmt.Errorf("google rejected the connect request: %s: %s", body.Error, body.ErrorDesc)
	}
	if body.RefreshToken == "" {
		// Shouldn't happen given access_type=offline+prompt=consent, but
		// fail loudly with a clear cause rather than silently storing an
		// access-token-only credential that would stop working the
		// moment it first expires.
		return "", nil, errors.New("google did not return a refresh token -- please try connecting again")
	}

	email, err := g.fetchEmail(ctx, body.AccessToken)
	if err != nil {
		// Non-fatal: the connection itself succeeded. The UI just won't
		// have an email to display; execution is unaffected.
		email = ""
	}

	tokenType := body.TokenType
	if tokenType == "" {
		tokenType = "Bearer"
	}
	secrets = map[string]string{
		"clientId":     g.ClientID,
		"clientSecret": g.ClientSecret,
		"refreshToken": body.RefreshToken,
		"accessToken":  body.AccessToken,
		"expiresAt":    strconv.FormatInt(time.Now().Add(time.Duration(body.ExpiresIn)*time.Second).Unix(), 10),
		"tokenType":    tokenType,
		"email":        email,
	}
	return service, secrets, nil
}

func (g *GoogleOAuthApp) fetchEmail(ctx context.Context, accessToken string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", googleUserinfoEndpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("userinfo endpoint returned %d", resp.StatusCode)
	}
	var body struct {
		Email string `json:"email"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	return body.Email, nil
}
