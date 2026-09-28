// Handlers backing the "Google Connections" page: one Connect button
// per service (Gmail/YouTube/Sheets) PER MicroFlow Service (tenant),
// each independently connect/reconnect/disconnect-able to its own
// Google account. This is the n8n-style replacement for pasting a
// clientId/clientSecret/refreshToken by hand -- the legacy manual-paste
// endpoints in server.go (GET/POST/DELETE /api/credentials/google and
// the per-node override endpoints) are untouched and still work
// (scoped to the "default" Service, matching pre-existing behavior),
// so nothing that previously depended on them breaks.
package api

import (
	"errors"
	"log"
	"net/http"
	"net/url"
	"time"

	"microflow/internal/vault"
)

// EnableGoogleOAuth registers the /api/services/{msvc}/google/* routes
// and wires the Google OAuth app used to build consent-screen URLs and
// exchange codes. Deliberately a post-construction step (not a New()
// parameter) so existing callers of New(...) -- including internal/api's
// own tests -- are unaffected; a server this is never called on simply
// doesn't expose these routes, while the legacy manual-paste credential
// endpoints keep working regardless.
func (s *Server) EnableGoogleOAuth(app *vault.GoogleOAuthApp, accounts *vault.GoogleServiceAccounts) {
	s.googleOAuth = app
	s.googleAccounts = accounts

	s.mux.HandleFunc("GET /api/services/{msvc}/google/connections", s.handleGoogleConnections)
	s.mux.HandleFunc("GET /api/services/{msvc}/google/connect/{service}", s.handleGoogleConnectStart)
	s.mux.HandleFunc("GET /api/oauth/google/callback", s.handleGoogleOAuthCallback)
	s.mux.HandleFunc("POST /api/services/{msvc}/google/disconnect/{service}", s.handleGoogleDisconnect)
}

// googleConnectionView is one row of the "Google Connections" page --
// never secret material (rule 11/12), matching the shape of every other
// credential-status response in this package.
type googleConnectionView struct {
	Service        string `json:"service"`
	Connected      bool   `json:"connected"`
	Email          string `json:"email,omitempty"`
	UpdatedAt      string `json:"updatedAt,omitempty"`
	NeedsReconnect bool   `json:"needsReconnect,omitempty"`
}

// handleGoogleConnections lists the connect status of every service,
// for one MicroFlow Service -- what that Service's dashboard renders as
// three cards (Gmail, YouTube, Sheets), each either "Connect Google" or
// "Connected as: user@example.com" + Reconnect/Disconnect.
func (s *Server) handleGoogleConnections(w http.ResponseWriter, r *http.Request) {
	if s.googleAccounts == nil {
		writeErr(w, http.StatusInternalServerError, errGoogleOAuthNotConfigured)
		return
	}
	msvc := r.PathValue("msvc")
	if !s.requireService(w, r, msvc) {
		return
	}
	out := make([]googleConnectionView, 0, len(vault.GoogleServices))
	for _, svc := range vault.GoogleServices {
		email, updatedAt, connected, needsReconnect, err := s.googleAccounts.Status(r.Context(), msvc, svc)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, errors.New("failed to check connection status"))
			return
		}
		view := googleConnectionView{Service: svc, Connected: connected, NeedsReconnect: needsReconnect}
		if connected {
			view.Email = email
			view.UpdatedAt = updatedAt.Format(time.RFC3339)
		}
		out = append(out, view)
	}
	writeJSON(w, http.StatusOK, out)
}

// handleGoogleConnectStart is what the UI's "Connect Google" /
// "Reconnect" button links to: redirects straight to Google's own
// consent screen (account picker -> permissions -> Allow). There is
// nothing for the frontend to POST or parse here -- the whole point of
// the Authorization Code flow is that the browser navigates to Google
// and back, never touching a token directly.
func (s *Server) handleGoogleConnectStart(w http.ResponseWriter, r *http.Request) {
	if s.googleOAuth == nil {
		writeErr(w, http.StatusInternalServerError, errGoogleOAuthNotConfigured)
		return
	}
	msvc := r.PathValue("msvc")
	if !s.requireService(w, r, msvc) {
		return
	}
	service := r.PathValue("service")
	if !vault.IsGoogleService(service) {
		writeErr(w, http.StatusBadRequest, errors.New("unknown google service -- must be gmail, youtube, or sheets"))
		return
	}
	authURL, err := s.googleOAuth.AuthURL(msvc, service)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to build google connect link"))
		return
	}
	http.Redirect(w, r, authURL, http.StatusFound)
}

// handleGoogleOAuthCallback is the redirect_uri registered in Google
// Cloud Console -- necessarily one single fixed URL shared by every
// MicroFlow Service (Google does not support a per-Service redirect
// path), so which Service a connection belongs to travels inside the
// HMAC-signed `state` parameter instead (see GoogleOAuthApp.signState)
// rather than the URL. Google lands the browser here with either
// `code`+`state` (person clicked Allow) or `error` (person clicked
// Cancel, or something else went wrong on Google's side). Either way
// this always finishes by redirecting the browser back into the SPA's
// Google Connections page for the right Service -- never a raw JSON
// error page -- with a query param the frontend uses to show a toast.
func (s *Server) handleGoogleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	backTo := func(msvc string) string {
		if msvc == "" {
			return "/#/credentials"
		}
		return "/#/services/" + url.PathEscape(msvc) + "/credentials"
	}

	if s.googleOAuth == nil || s.googleAccounts == nil {
		http.Redirect(w, r, backTo("")+"?googleError="+urlMsg("Google OAuth is not configured on this server"), http.StatusFound)
		return
	}

	if googleErr := r.URL.Query().Get("error"); googleErr != "" {
		// Person clicked Cancel on Google's consent screen, or Google
		// rejected the request -- never our own bug, so no server log.
		http.Redirect(w, r, backTo("")+"?googleError="+urlMsg("Google sign-in was cancelled"), http.StatusFound)
		return
	}

	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		http.Redirect(w, r, backTo("")+"?googleError="+urlMsg("Invalid Google callback"), http.StatusFound)
		return
	}

	msvc, service, secrets, err := s.googleOAuth.Exchange(r.Context(), code, state)
	if err != nil {
		log.Printf("google oauth exchange failed: %v", err)
		http.Redirect(w, r, backTo("")+"?googleError="+urlMsg("Could not connect to Google. Please try again."), http.StatusFound)
		return
	}

	if err := s.googleAccounts.Put(r.Context(), msvc, service, secrets); err != nil {
		log.Printf("google oauth: failed to save connected account for service=%q google=%q: %v", msvc, service, err)
		http.Redirect(w, r, backTo(msvc)+"?googleError="+urlMsg("Connected to Google but failed to save the connection. Please try again."), http.StatusFound)
		return
	}

	http.Redirect(w, r, backTo(msvc)+"?googleConnected="+service, http.StatusFound)
}

// handleGoogleDisconnect removes one service's connected account only
// -- Gmail/YouTube/Sheets are disconnected completely independently, so
// disconnecting one never touches the others or any other MicroFlow
// Service (see vault.GoogleServiceAccounts.Disconnect).
func (s *Server) handleGoogleDisconnect(w http.ResponseWriter, r *http.Request) {
	if s.googleAccounts == nil {
		writeErr(w, http.StatusInternalServerError, errGoogleOAuthNotConfigured)
		return
	}
	msvc := r.PathValue("msvc")
	if !s.requireService(w, r, msvc) {
		return
	}
	service := r.PathValue("service")
	if !vault.IsGoogleService(service) {
		writeErr(w, http.StatusBadRequest, errors.New("unknown google service -- must be gmail, youtube, or sheets"))
		return
	}
	if err := s.googleAccounts.Disconnect(r.Context(), msvc, service); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to disconnect"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": service})
}

var errGoogleOAuthNotConfigured = errors.New("google oauth is not configured on this server -- set GOOGLE_OAUTH_CLIENT_ID/GOOGLE_OAUTH_CLIENT_SECRET/GOOGLE_OAUTH_REDIRECT_URL")

// requireService writes a 404 and returns false if id doesn't name an
// existing Service, or if tenancy support isn't wired on this Server
// (s.services == nil -- an older test helper). Every Service-scoped
// handler calls this first so a request for a nonexistent/deleted
// Service fails the same clear way a nonexistent workflow does.
func (s *Server) requireService(w http.ResponseWriter, r *http.Request, id string) bool {
	if s.services == nil {
		writeErr(w, http.StatusInternalServerError, errors.New("service management is not configured on this server"))
		return false
	}
	if id == "" {
		writeErr(w, http.StatusBadRequest, errors.New("service id is required"))
		return false
	}
	if _, err := s.services.GetService(r.Context(), id); err != nil {
		writeErr(w, http.StatusNotFound, errors.New("service not found"))
		return false
	}
	return true
}

// urlMsg URL-query-escapes a human-readable message for use as a query
// param value.
func urlMsg(s string) string {
	return url.QueryEscape(s)
}
