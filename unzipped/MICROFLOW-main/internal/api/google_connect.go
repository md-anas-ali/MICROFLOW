// Handlers backing the "Google Connections" page: one Connect button
// per service (Gmail/YouTube/Sheets), each independently
// connect/reconnect/disconnect-able to its own Google account. This is
// the n8n-style replacement for pasting a clientId/clientSecret/
// refreshToken by hand -- the legacy manual-paste endpoints in
// server.go (GET/POST/DELETE /api/credentials/google and the per-node
// override endpoints) are untouched and still work, so nothing that
// previously depended on them breaks.
package api

import (
	"errors"
	"log"
	"net/http"
	"net/url"
	"time"

	"microflow/internal/vault"
)

// EnableGoogleOAuth registers the /api/google/* routes and wires the
// Google OAuth app used to build consent-screen URLs and exchange
// codes. Deliberately a post-construction step (not a New() parameter)
// so existing callers of New(...) -- including internal/api's own
// tests -- are unaffected; a server this is never called on simply
// doesn't expose these routes (handleGoogleConnect and friends aren't
// reachable), while the legacy manual-paste credential endpoints keep
// working regardless.
func (s *Server) EnableGoogleOAuth(app *vault.GoogleOAuthApp, accounts *vault.GoogleServiceAccounts) {
	s.googleOAuth = app
	s.googleAccounts = accounts

	s.mux.HandleFunc("GET /api/google/connections", s.handleGoogleConnections)
	s.mux.HandleFunc("GET /api/google/connect/{service}", s.handleGoogleConnectStart)
	s.mux.HandleFunc("GET /api/oauth/google/callback", s.handleGoogleOAuthCallback)
	s.mux.HandleFunc("POST /api/google/disconnect/{service}", s.handleGoogleDisconnect)
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

// handleGoogleConnections lists the connect status of every service --
// what the "Google Connections" page renders as three cards (Gmail,
// YouTube, Sheets), each either "Connect Google" or "Connected as:
// user@example.com" + Reconnect/Disconnect.
func (s *Server) handleGoogleConnections(w http.ResponseWriter, r *http.Request) {
	if s.googleAccounts == nil {
		writeErr(w, http.StatusInternalServerError, errGoogleOAuthNotConfigured)
		return
	}
	out := make([]googleConnectionView, 0, len(vault.GoogleServices))
	for _, svc := range vault.GoogleServices {
		email, updatedAt, connected, needsReconnect, err := s.googleAccounts.Status(r.Context(), svc)
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
	service := r.PathValue("service")
	if !vault.IsGoogleService(service) {
		writeErr(w, http.StatusBadRequest, errors.New("unknown google service -- must be gmail, youtube, or sheets"))
		return
	}
	authURL, err := s.googleOAuth.AuthURL(service)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to build google connect link"))
		return
	}
	http.Redirect(w, r, authURL, http.StatusFound)
}

// handleGoogleOAuthCallback is the redirect_uri registered in Google
// Cloud Console. Google lands the browser here with either `code`+
// `state` (person clicked Allow) or `error` (person clicked Cancel, or
// something else went wrong on Google's side). Either way this always
// finishes by redirecting the browser back into the SPA's Google
// Connections page -- never a raw JSON error page -- with a query
// param the frontend uses to show a toast.
func (s *Server) handleGoogleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	const backToConnectionsPage = "/#/credentials"

	if s.googleOAuth == nil || s.googleAccounts == nil {
		http.Redirect(w, r, backToConnectionsPage+"?googleError="+urlMsg("Google OAuth is not configured on this server"), http.StatusFound)
		return
	}

	if googleErr := r.URL.Query().Get("error"); googleErr != "" {
		// Person clicked Cancel on Google's consent screen, or Google
		// rejected the request -- never our own bug, so no server log.
		http.Redirect(w, r, backToConnectionsPage+"?googleError="+urlMsg("Google sign-in was cancelled"), http.StatusFound)
		return
	}

	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		http.Redirect(w, r, backToConnectionsPage+"?googleError="+urlMsg("Invalid Google callback"), http.StatusFound)
		return
	}

	service, secrets, err := s.googleOAuth.Exchange(r.Context(), code, state)
	if err != nil {
		log.Printf("google oauth exchange failed: %v", err)
		http.Redirect(w, r, backToConnectionsPage+"?googleError="+urlMsg("Could not connect to Google. Please try again."), http.StatusFound)
		return
	}

	if err := s.googleAccounts.Put(r.Context(), service, secrets); err != nil {
		log.Printf("google oauth: failed to save connected account for %q: %v", service, err)
		http.Redirect(w, r, backToConnectionsPage+"?googleError="+urlMsg("Connected to Google but failed to save the connection. Please try again."), http.StatusFound)
		return
	}

	http.Redirect(w, r, backToConnectionsPage+"?googleConnected="+service, http.StatusFound)
}

// handleGoogleDisconnect removes one service's connected account only
// -- Gmail/YouTube/Sheets are disconnected completely independently, so
// disconnecting one never touches the others (see
// vault.GoogleServiceAccounts.Disconnect).
func (s *Server) handleGoogleDisconnect(w http.ResponseWriter, r *http.Request) {
	if s.googleAccounts == nil {
		writeErr(w, http.StatusInternalServerError, errGoogleOAuthNotConfigured)
		return
	}
	service := r.PathValue("service")
	if !vault.IsGoogleService(service) {
		writeErr(w, http.StatusBadRequest, errors.New("unknown google service -- must be gmail, youtube, or sheets"))
		return
	}
	if err := s.googleAccounts.Disconnect(r.Context(), service); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to disconnect"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": service})
}

var errGoogleOAuthNotConfigured = errors.New("google oauth is not configured on this server -- set GOOGLE_OAUTH_CLIENT_ID/GOOGLE_OAUTH_CLIENT_SECRET/GOOGLE_OAUTH_REDIRECT_URL")

// urlMsg URL-query-escapes a human-readable message for use as a query
// param value.
func urlMsg(s string) string {
	return url.QueryEscape(s)
}
