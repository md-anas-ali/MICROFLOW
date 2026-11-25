package vault

// "Connect Facebook" -- log in ONCE (global), then every socialPublish node
// only needs the Page ID(s) in the Environment.
//
// How "once, never again" works with Facebook:
//   1. Login -> short-lived user token -> exchanged for a long-lived user token.
//   2. GET /me/accounts with a long-lived user token returns one PAGE access
//      token per Page. Page tokens obtained this way do not expire (they only
//      die if the person changes their Facebook password, removes the app, or
//      loses their role on the Page -- then one "Reconnect" fixes it).
//   3. We store every Page token (and its linked Instagram business account id)
//      encrypted in the vault, so runs never depend on the 60-day user token.
//      A Page added later is picked up on demand while the user token is valid.
//
// The login is instance-wide (one row, FacebookAccountKey); the App ID/Secret
// come from the Environment like the Google OAuth client does.
//
// UNVERIFIED against live Facebook: needs a real Meta app and a real login.

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
	"sync"
	"time"
)

// FacebookAccountKey is the single (global) vault row holding the connected login.
const FacebookAccountKey = "facebook"

// FacebookOAuthCallbackPath is the fixed callback route (register
// HOST_URL + this path in the Meta app's "Valid OAuth Redirect URIs").
const FacebookOAuthCallbackPath = "/api/oauth/facebook/callback"

// Default scopes: publish to Pages + Instagram Reels. Override with the
// FB_OAUTH_SCOPES env var (comma separated) if your Meta app uses other ones.
const DefaultFacebookScopes = "pages_show_list,pages_manage_posts,pages_read_engagement,business_management,instagram_basic,instagram_content_publish"

const facebookOAuthStateTTL = 10 * time.Minute

// Vars only so tests can point them at httptest servers.
var (
	facebookGraph  = "https://graph.facebook.com/v21.0"
	facebookDialog = "https://www.facebook.com/v21.0/dialog/oauth"
)

// ErrFacebookReauthRequired means the stored login no longer works.
var ErrFacebookReauthRequired = errors.New("facebook: connection expired or revoked -- reconnect required")

// FacebookPage is one Page the login can post to.
type FacebookPage struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Token  string `json:"token"`
	IGID   string `json:"igId,omitempty"`
	IGUser string `json:"igUser,omitempty"`
}

// FacebookApp is the Meta app (App ID / Secret / redirect) used for login.
type FacebookApp struct {
	AppID       string
	AppSecret   string
	RedirectURL string
	Scopes      string
	httpClient  *http.Client
}

func NewFacebookApp(id, secret, redirect, scopes string) *FacebookApp {
	if strings.TrimSpace(scopes) == "" {
		scopes = DefaultFacebookScopes
	}
	return &FacebookApp{AppID: id, AppSecret: secret, RedirectURL: redirect, Scopes: scopes,
		httpClient: &http.Client{Timeout: 20 * time.Second}}
}

// FacebookOAuthAppFromEnv builds the app from FB_APP_ID / FB_APP_SECRET and
// HOST_URL (or FB_OAUTH_REDIRECT_URL). ok=false when something is missing.
func FacebookOAuthAppFromEnv(getenv func(string) string) (app *FacebookApp, missing []string) {
	id := strings.TrimSpace(getenv("FB_APP_ID"))
	secret := strings.TrimSpace(getenv("FB_APP_SECRET"))
	redirect := strings.TrimSpace(getenv("FB_OAUTH_REDIRECT_URL"))
	if host := NormalizeHostURL(getenv(HostURLEnvKey)); host != "" {
		redirect = host + FacebookOAuthCallbackPath
	}
	if id == "" {
		missing = append(missing, "FB_APP_ID")
	}
	if secret == "" {
		missing = append(missing, "FB_APP_SECRET")
	}
	if redirect == "" {
		missing = append(missing, HostURLEnvKey+" (or FB_OAUTH_REDIRECT_URL)")
	}
	if len(missing) > 0 {
		return nil, missing
	}
	return NewFacebookApp(id, secret, redirect, getenv("FB_OAUTH_SCOPES")), nil
}

// AuthURL is the Facebook login dialog URL. msvcID only rides in the signed
// state so the callback can send the browser back to the right page.
func (a *FacebookApp) AuthURL(msvcID string) (string, error) {
	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	payload := msvcID + "|facebook|" + strconv.FormatInt(time.Now().Unix(), 10) + "|" + base64.RawURLEncoding.EncodeToString(nonce)
	q := url.Values{
		"client_id":     {a.AppID},
		"redirect_uri":  {a.RedirectURL},
		"response_type": {"code"},
		"scope":         {a.Scopes},
		"state":         {a.signRaw(payload)},
	}
	return facebookDialog + "?" + q.Encode(), nil
}

func (a *FacebookApp) signRaw(payload string) string {
	mac := hmac.New(sha256.New, []byte(a.AppSecret))
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (a *FacebookApp) verifyState(state string) (msvcID string, err error) {
	dot := strings.LastIndexByte(state, '.')
	if dot < 0 {
		return "", errors.New("malformed state")
	}
	payload, err := base64.RawURLEncoding.DecodeString(state[:dot])
	if err != nil {
		return "", errors.New("malformed state")
	}
	mac := hmac.New(sha256.New, []byte(a.AppSecret))
	mac.Write(payload)
	if !hmac.Equal([]byte(base64.RawURLEncoding.EncodeToString(mac.Sum(nil))), []byte(state[dot+1:])) {
		return "", errors.New("state signature mismatch")
	}
	f := strings.SplitN(string(payload), "|", 4)
	if len(f) != 4 || f[0] == "" || f[1] != "facebook" {
		return "", errors.New("malformed state payload")
	}
	ts, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil {
		return "", errors.New("malformed state timestamp")
	}
	if time.Since(time.Unix(ts, 0)) > facebookOAuthStateTTL {
		return "", errors.New("connect link expired -- please click Connect again")
	}
	return f[0], nil
}

type fbTokenResp struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	Error       *struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	} `json:"error"`
}

// graph runs one Graph call (POST form or GET) and decodes JSON into out.
// Tokens travel in the Authorization header or POST body, never in a URL.
func (a *FacebookApp) graph(ctx context.Context, method, path string, bearer string, form url.Values, out any) error {
	var body io.Reader
	u := facebookGraph + path
	if method == "POST" {
		body = strings.NewReader(form.Encode())
	} else if len(form) > 0 {
		u += "?" + form.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	if method == "POST" {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("contacting facebook: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct {
				Message string `json:"message"`
				Code    int    `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(b, &e)
		if e.Error.Code == 190 {
			return fmt.Errorf("%w: %s", ErrFacebookReauthRequired, e.Error.Message)
		}
		return fmt.Errorf("facebook HTTP %d: %s", resp.StatusCode, e.Error.Message)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			return fmt.Errorf("bad JSON from facebook: %w", err)
		}
	}
	return nil
}

// listPages returns every Page (with its Page token and linked Instagram
// business account) the user token can reach.
func (a *FacebookApp) listPages(ctx context.Context, userToken string) ([]FacebookPage, error) {
	var pages []FacebookPage
	after := ""
	for i := 0; i < 20; i++ { // hard cap: 20 pages of 100
		q := url.Values{"fields": {"id,name,access_token,instagram_business_account{id,username}"}, "limit": {"100"}}
		if after != "" {
			q.Set("after", after)
		}
		var r struct {
			Data []struct {
				ID, Name    string
				AccessToken string `json:"access_token"`
				IG          *struct {
					ID       string `json:"id"`
					Username string `json:"username"`
				} `json:"instagram_business_account"`
			} `json:"data"`
			Paging struct {
				Cursors struct {
					After string `json:"after"`
				} `json:"cursors"`
				Next string `json:"next"`
			} `json:"paging"`
		}
		if err := a.graph(ctx, "GET", "/me/accounts", userToken, q, &r); err != nil {
			return nil, err
		}
		for _, d := range r.Data {
			if d.ID == "" || d.AccessToken == "" {
				continue
			}
			p := FacebookPage{ID: d.ID, Name: d.Name, Token: d.AccessToken}
			if d.IG != nil {
				p.IGID, p.IGUser = d.IG.ID, d.IG.Username
			}
			pages = append(pages, p)
		}
		if r.Paging.Next == "" || r.Paging.Cursors.After == "" {
			break
		}
		after = r.Paging.Cursors.After
	}
	return pages, nil
}

// Exchange completes the login: verifies state, code -> long-lived user token
// -> all Page tokens. Returns the secrets map for FacebookAccount.Put.
func (a *FacebookApp) Exchange(ctx context.Context, code, state string) (msvcID string, secrets map[string]string, err error) {
	if msvcID, err = a.verifyState(state); err != nil {
		return "", nil, err
	}
	var short fbTokenResp
	if err := a.graph(ctx, "POST", "/oauth/access_token", "", url.Values{
		"client_id": {a.AppID}, "client_secret": {a.AppSecret}, "redirect_uri": {a.RedirectURL}, "code": {code},
	}, &short); err != nil || short.AccessToken == "" {
		if err == nil {
			err = errors.New("no access token returned")
		}
		return "", nil, fmt.Errorf("facebook login failed: %w", err)
	}
	var long fbTokenResp
	if err := a.graph(ctx, "POST", "/oauth/access_token", "", url.Values{
		"grant_type": {"fb_exchange_token"}, "client_id": {a.AppID}, "client_secret": {a.AppSecret},
		"fb_exchange_token": {short.AccessToken},
	}, &long); err != nil || long.AccessToken == "" {
		if err == nil {
			err = errors.New("no long-lived token returned")
		}
		return "", nil, fmt.Errorf("facebook long-lived token exchange failed: %w", err)
	}
	var me struct{ ID, Name string }
	if err := a.graph(ctx, "GET", "/me", long.AccessToken, url.Values{"fields": {"id,name"}}, &me); err != nil {
		return "", nil, err
	}
	pages, err := a.listPages(ctx, long.AccessToken)
	if err != nil {
		return "", nil, err
	}
	if len(pages) == 0 {
		return "", nil, errors.New("no Facebook Pages were shared -- reconnect and tick the Pages you want to post to")
	}
	pj, _ := json.Marshal(pages)
	exp := ""
	if long.ExpiresIn > 0 {
		exp = strconv.FormatInt(time.Now().Add(time.Duration(long.ExpiresIn)*time.Second).Unix(), 10)
	}
	return msvcID, map[string]string{
		"userToken": long.AccessToken, "userExpiresAt": exp, "userId": me.ID, "name": me.Name, "pages": string(pj),
	}, nil
}

// CheckToken asks Facebook (debug_token) whether a token is still valid.
func (a *FacebookApp) CheckToken(ctx context.Context, token string) (bool, error) {
	var r struct {
		Data struct {
			IsValid bool `json:"is_valid"`
		} `json:"data"`
	}
	if err := a.graph(ctx, "GET", "/debug_token", a.AppID+"|"+a.AppSecret, url.Values{"input_token": {token}}, &r); err != nil {
		return false, err
	}
	return r.Data.IsValid, nil
}

// ---------------------------------------------------------------- stored account

// FacebookAccount reads/writes the global Facebook login row. It implements
// nodes.FacebookPageResolver.
type FacebookAccount struct {
	accounts *AccountVault
	mu       sync.Mutex
	// refresher is used to pick up a Page added after connecting (needs the
	// user token to still be valid). May be nil.
	refresher func(ctx context.Context) *FacebookApp
}

func NewFacebookAccount(av *AccountVault) *FacebookAccount { return &FacebookAccount{accounts: av} }

// SetRefresher lets PageAccess find newly added Pages using the app resolved
// from the Environment.
func (f *FacebookAccount) SetRefresher(fn func(ctx context.Context) *FacebookApp) { f.refresher = fn }

func (f *FacebookAccount) Put(ctx context.Context, secrets map[string]string) error {
	return f.accounts.Put(ctx, FacebookAccountKey, secrets)
}
func (f *FacebookAccount) Disconnect(ctx context.Context) error {
	return f.accounts.Delete(ctx, FacebookAccountKey)
}

// Connected reports whether a login is stored (no network, no secrets).
func (f *FacebookAccount) Connected(ctx context.Context) bool {
	_, ok, err := f.accounts.Status(ctx, FacebookAccountKey)
	return err == nil && ok
}

func pagesFrom(secrets map[string]string) []FacebookPage {
	var pages []FacebookPage
	_ = json.Unmarshal([]byte(secrets["pages"]), &pages)
	return pages
}

// PageAccess returns the Page token (and linked Instagram business account id,
// "" if none) for pageID.
func (f *FacebookAccount) PageAccess(ctx context.Context, pageID string) (token, igUserID string, err error) {
	pageID = strings.TrimSpace(pageID)
	secrets, err := f.accounts.Resolve(ctx, FacebookAccountKey)
	if err != nil {
		return "", "", errors.New("Facebook is not connected -- open Credentials and click \"Connect Facebook\" (or set accessToken on the node)")
	}
	for _, p := range pagesFrom(secrets) {
		if p.ID == pageID {
			return p.Token, p.IGID, nil
		}
	}
	// Unknown Page: it may have been added after connecting. Try the user token.
	if f.refresher != nil && secrets["userToken"] != "" {
		if app := f.refresher(ctx); app != nil {
			f.mu.Lock()
			defer f.mu.Unlock()
			if pages, lerr := app.listPages(ctx, secrets["userToken"]); lerr == nil && len(pages) > 0 {
				pj, _ := json.Marshal(pages)
				secrets["pages"] = string(pj)
				_ = f.accounts.Put(ctx, FacebookAccountKey, secrets)
				for _, p := range pages {
					if p.ID == pageID {
						return p.Token, p.IGID, nil
					}
				}
			} else if lerr != nil && errors.Is(lerr, ErrFacebookReauthRequired) {
				return "", "", fmt.Errorf("Page %s is not saved and the Facebook login expired -- reconnect Facebook", pageID)
			}
		}
	}
	return "", "", fmt.Errorf("Page %s is not available to the connected Facebook account (%s) -- check the ID, or reconnect Facebook and tick this Page", pageID, secrets["name"])
}

// FacebookStatus is what the Credentials page shows. Never any token.
type FacebookStatus struct {
	Connected      bool               `json:"connected"`
	NeedsReconnect bool               `json:"needsReconnect,omitempty"`
	Name           string             `json:"name,omitempty"`
	UpdatedAt      time.Time          `json:"updatedAt,omitempty"`
	Pages          []FacebookPageInfo `json:"pages,omitempty"`
}

type FacebookPageInfo struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Instagram string `json:"instagram,omitempty"`
}

// Status builds the UI view; app (may be nil) is used for a debug_token check.
func (f *FacebookAccount) Status(ctx context.Context, app *FacebookApp) (FacebookStatus, error) {
	updatedAt, ok, err := f.accounts.Status(ctx, FacebookAccountKey)
	if err != nil || !ok {
		return FacebookStatus{}, err
	}
	st := FacebookStatus{Connected: true, UpdatedAt: updatedAt}
	secrets, err := f.accounts.Resolve(ctx, FacebookAccountKey)
	if err != nil {
		return st, nil
	}
	st.Name = secrets["name"]
	pages := pagesFrom(secrets)
	for _, p := range pages {
		st.Pages = append(st.Pages, FacebookPageInfo{ID: p.ID, Name: p.Name, Instagram: p.IGUser})
	}
	if app != nil && len(pages) > 0 {
		if valid, cerr := app.CheckToken(ctx, pages[0].Token); cerr == nil && !valid {
			st.NeedsReconnect = true
		} else if cerr != nil && errors.Is(cerr, ErrFacebookReauthRequired) {
			st.NeedsReconnect = true
		} // other errors are transient: keep showing "connected"
	}
	return st, nil
}
