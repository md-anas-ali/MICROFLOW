// "Connect Facebook": one global login whose Page tokens every socialPublish
// node uses (see internal/vault/facebook.go). Mirrors google_connect.go.
package api

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/url"
	"strings"

	"microflow/internal/vault"
)

// FacebookOAuthResolver builds the Meta app from the merged Environment of one
// Service on every request (never cached), like GoogleOAuthResolver.
// baseURL is the public address the browser used (derived from the request), used
// as the redirect when HOST_URL / FB_OAUTH_REDIRECT_URL are not set.
type FacebookOAuthResolver func(ctx context.Context, serviceID, baseURL string) (*vault.FacebookApp, error)

// requestBaseURL is the scheme+host the browser used to reach this server,
// honoring the reverse-proxy headers hosts like Render/Heroku add. A wrong or
// spoofed value cannot be abused: the redirect must match one registered in
// the Meta app, and the login state is HMAC-signed.
func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if v := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]); v == "http" || v == "https" {
		scheme = v
	}
	host := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Host"), ",")[0])
	if host == "" {
		host = r.Host
	}
	return scheme + "://" + host
}

// EnableFacebookOAuthResolver registers the Facebook connect routes.
func (s *Server) EnableFacebookOAuthResolver(resolve FacebookOAuthResolver, account *vault.FacebookAccount) {
	s.fbOAuthResolver = resolve
	s.fbAccount = account
	s.mux.HandleFunc("GET /api/services/{msvc}/facebook/connection", s.handleFacebookConnection)
	s.mux.HandleFunc("GET /api/services/{msvc}/facebook/connect", s.handleFacebookConnectStart)
	s.mux.HandleFunc("GET /api/oauth/facebook/callback", s.handleFacebookOAuthCallback)
	s.mux.HandleFunc("POST /api/services/{msvc}/facebook/disconnect", s.handleFacebookDisconnect)
}

// handleFacebookConnection: status for the Credentials page. If the app
// isn't configured the connection (if any) is still listed, plus a hint.
func (s *Server) handleFacebookConnection(w http.ResponseWriter, r *http.Request) {
	msvc := r.PathValue("msvc")
	if !s.requireService(w, r, msvc) {
		return
	}
	app, appErr := s.fbOAuthResolver(r.Context(), msvc, requestBaseURL(r))
	st, err := s.fbAccount.Status(r.Context(), app)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to check Facebook connection"))
		return
	}
	out := map[string]any{"status": st, "configured": appErr == nil}
	if appErr != nil {
		out["configHint"] = appErr.Error()
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleFacebookConnectStart(w http.ResponseWriter, r *http.Request) {
	msvc := r.PathValue("msvc")
	if !s.requireService(w, r, msvc) {
		return
	}
	app, err := s.fbOAuthResolver(r.Context(), msvc, requestBaseURL(r))
	if err != nil {
		http.Redirect(w, r, "/#/services/"+url.PathEscape(msvc)+"/credentials?fbError="+urlMsg(err.Error()), http.StatusFound)
		return
	}
	authURL, err := app.AuthURL(msvc)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to build Facebook connect link"))
		return
	}
	http.Redirect(w, r, authURL, http.StatusFound)
}

func (s *Server) handleFacebookOAuthCallback(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	stateMsvc, _ := vault.PeekStateServiceID(state)
	backTo := func(msvc string) string {
		if msvc == "" {
			return "/#/credentials"
		}
		return "/#/services/" + url.PathEscape(msvc) + "/credentials"
	}
	fail := func(msg string) {
		http.Redirect(w, r, backTo(stateMsvc)+"?fbError="+urlMsg(msg), http.StatusFound)
	}
	if r.URL.Query().Get("error") != "" {
		fail("Facebook login was cancelled")
		return
	}
	code := r.URL.Query().Get("code")
	if code == "" || state == "" || stateMsvc == "" {
		fail("Invalid Facebook callback")
		return
	}
	app, err := s.fbOAuthResolver(r.Context(), stateMsvc, requestBaseURL(r))
	if err != nil {
		fail(err.Error())
		return
	}
	msvc, secrets, err := app.Exchange(r.Context(), code, state)
	if err != nil {
		log.Printf("facebook oauth exchange failed: %v", err)
		fail("Could not connect Facebook: " + err.Error())
		return
	}
	if err := s.fbAccount.Put(r.Context(), secrets); err != nil {
		log.Printf("facebook oauth: failed to save connection: %v", err)
		fail("Connected to Facebook but failed to save the connection. Please try again.")
		return
	}
	http.Redirect(w, r, backTo(msvc)+"?fbConnected=1", http.StatusFound)
}

func (s *Server) handleFacebookDisconnect(w http.ResponseWriter, r *http.Request) {
	if !s.requireService(w, r, r.PathValue("msvc")) {
		return
	}
	if err := s.fbAccount.Disconnect(r.Context()); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to disconnect"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
