// Login gate for the MicroFlow web UI + API.
//
// Credentials come ONLY from environment variables (never stored in the
// database, never hardcoded):
//
//	MICROFLOW_LOGIN_USER       login user name
//	MICROFLOW_LOGIN_PASSWORD   login password
//	MICROFLOW_SESSION_HOURS    optional  -- session lifetime, default 168 (7 days)
//
// Login is OPTIONAL: if BOTH MICROFLOW_LOGIN_USER and
// MICROFLOW_LOGIN_PASSWORD are unset/empty the login gate is disabled
// (no login page, everything is open). If both are set the gate is on.
// If only ONE of them is set the server refuses to start (fail closed --
// that is almost certainly a typo, and must not silently expose the app).
//
// How it works:
//   - Every request except /login, /logout, /healthz and /webhook/* needs a
//     valid signed session cookie. /webhook/* keeps its own token check
//     (MICROFLOW_WEBHOOK_TOKEN) because external services call it.
//   - Unauthenticated /api/* calls get 401 JSON; browser pages redirect to /login.
//   - The session is a stateless HMAC-signed cookie (no DB table needed). The
//     signing key is derived from user+password+MICROFLOW_MASTER_KEY, so
//     changing the password in the environment logs everybody out.
//   - Failed logins are rate limited per client IP (5 failures / 10 min ->
//     10 min lockout) and slowed down by ~1s each.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"html"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	sessionCookieName = "mf_session"
	loginMaxFailures  = 5
	loginFailWindow   = 10 * time.Minute
	loginLockout      = 10 * time.Minute
)

type failRecord struct {
	count       int
	first       time.Time
	lockedUntil time.Time
}

type authGate struct {
	// enabled is false when no login credentials are configured: the gate
	// then passes every request straight through.
	enabled bool

	user string
	pass string
	key  []byte
	ttl  time.Duration

	mu    sync.Mutex
	fails map[string]*failRecord
}

// newAuthGateFromEnv: both login vars unset -> login disabled; both set
// -> login enabled; exactly one set -> error (fail closed).
func newAuthGateFromEnv() (*authGate, error) {
	user := strings.TrimSpace(os.Getenv("MICROFLOW_LOGIN_USER"))
	pass := os.Getenv("MICROFLOW_LOGIN_PASSWORD")
	if user == "" && pass == "" {
		return &authGate{enabled: false, fails: map[string]*failRecord{}}, nil
	}
	if user == "" || pass == "" {
		return nil, errors.New("MICROFLOW_LOGIN_USER and MICROFLOW_LOGIN_PASSWORD must be set together -- set both to enable the login page, or leave both unset to disable it")
	}
	hours := envInt("MICROFLOW_SESSION_HOURS", 168)
	if hours < 1 {
		hours = 1
	}
	sum := sha256.Sum256([]byte("microflow-session-v1\x00" + user + "\x00" + pass + "\x00" + os.Getenv("MICROFLOW_MASTER_KEY")))
	return &authGate{
		enabled: true,
		user:    user,
		pass:    pass,
		key:     sum[:],
		ttl:     time.Duration(hours) * time.Hour,
		fails:   map[string]*failRecord{},
	}, nil
}

// Wrap puts the login gate in front of the whole app.
func (g *authGate) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if !g.enabled {
			// Login disabled (no credentials configured): no login page.
			switch {
			case p == "/login" || p == "/logout":
				http.Redirect(w, r, "/", http.StatusFound)
			case p == "/healthz":
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				_, _ = w.Write([]byte("ok"))
			case p == "/api/auth-status":
				writeAuthStatus(w, false)
			default:
				next.ServeHTTP(w, r)
			}
			return
		}
		if p == "/api/auth-status" {
			if !g.validSession(r) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"login required"}`))
				return
			}
			writeAuthStatus(w, true)
			return
		}
		switch {
		case p == "/login":
			g.handleLogin(w, r)
			return
		case p == "/logout":
			g.handleLogout(w, r)
			return
		case p == "/healthz":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte("ok"))
			return
		case strings.HasPrefix(p, "/webhook/"):
			next.ServeHTTP(w, r) // has its own MICROFLOW_WEBHOOK_TOKEN check
			return
		}
		if g.validSession(r) {
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(p, "/api/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"login required"}`))
			return
		}
		http.Redirect(w, r, "/login", http.StatusFound)
	})
}

// writeAuthStatus tells the dashboard whether a login password exists
// (so the "delete Environment set" dialog knows whether to ask for it).
func writeAuthStatus(w http.ResponseWriter, required bool) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if required {
		_, _ = w.Write([]byte(`{"loginRequired":true}`))
		return
	}
	_, _ = w.Write([]byte(`{"loginRequired":false}`))
}

func (g *authGate) handleLogin(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if g.validSession(r) {
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		g.renderLogin(w, http.StatusOK, "")
	case http.MethodPost:
		ip := clientIP(r)
		if wait := g.lockedFor(ip); wait > 0 {
			g.renderLogin(w, http.StatusTooManyRequests,
				"Too many failed attempts. Try again in "+strconv.Itoa(int(wait.Minutes())+1)+" minute(s).")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
		if err := r.ParseForm(); err != nil {
			g.renderLogin(w, http.StatusBadRequest, "Bad request.")
			return
		}
		okUser := secureEq(strings.TrimSpace(r.PostFormValue("username")), g.user)
		okPass := secureEq(r.PostFormValue("password"), g.pass)
		if !(okUser && okPass) {
			g.recordFail(ip)
			time.Sleep(time.Second) // slow down guessing
			g.renderLogin(w, http.StatusUnauthorized, "Wrong user name or password.")
			return
		}
		g.clearFails(ip)
		g.setSession(w, r)
		http.Redirect(w, r, "/", http.StatusSeeOther)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// verifyPassword re-checks the login password for a sensitive action
// (used by the API's "delete Environment set" second step). It reuses
// the login gate's secureEq, per-IP failure limiter and 1s slow-down --
// no separate auth system. The request must also carry a valid session.
// retryAfter > 0 means the IP is currently locked out.
func (g *authGate) verifyPassword(r *http.Request, password string) (ok bool, retryAfter time.Duration) {
	if !g.enabled {
		// No login password is configured, so there is nothing to
		// re-check; the typed-name confirmation is the only step.
		return true, 0
	}
	if !g.validSession(r) {
		return false, 0
	}
	ip := clientIP(r)
	if wait := g.lockedFor(ip); wait > 0 {
		return false, wait
	}
	if secureEq(password, g.pass) {
		g.clearFails(ip)
		return true, 0
	}
	g.recordFail(ip)
	time.Sleep(time.Second) // slow down guessing, same as login
	return false, 0
}

func (g *authGate) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   isHTTPS(r),
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// ---------------- session cookie ----------------

func (g *authGate) sign(exp int64) string {
	payload := strconv.FormatInt(exp, 10)
	mac := hmac.New(sha256.New, g.key)
	mac.Write([]byte(payload))
	return payload + "." + hex.EncodeToString(mac.Sum(nil))
}

func (g *authGate) setSession(w http.ResponseWriter, r *http.Request) {
	exp := time.Now().Add(g.ttl).Unix()
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    g.sign(exp),
		Path:     "/",
		MaxAge:   int(g.ttl.Seconds()),
		HttpOnly: true,
		Secure:   isHTTPS(r),
		SameSite: http.SameSiteLaxMode, // blocks cross-site POSTs (CSRF), still allows the Google OAuth redirect back
	})
}

func (g *authGate) validSession(r *http.Request) bool {
	c, err := r.Cookie(sessionCookieName)
	if err != nil {
		return false
	}
	parts := strings.SplitN(c.Value, ".", 2)
	if len(parts) != 2 {
		return false
	}
	exp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || time.Now().Unix() > exp {
		return false
	}
	return hmac.Equal([]byte(g.sign(exp)), []byte(c.Value))
}

// ---------------- brute-force limiter ----------------

func (g *authGate) lockedFor(ip string) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	if rec, ok := g.fails[ip]; ok && time.Now().Before(rec.lockedUntil) {
		return time.Until(rec.lockedUntil)
	}
	return 0
}

func (g *authGate) recordFail(ip string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	if len(g.fails) > 1000 { // keep memory bounded
		for k, v := range g.fails {
			if now.After(v.lockedUntil) && now.Sub(v.first) > loginFailWindow {
				delete(g.fails, k)
			}
		}
	}
	rec, ok := g.fails[ip]
	if !ok || now.Sub(rec.first) > loginFailWindow {
		rec = &failRecord{first: now}
		g.fails[ip] = rec
	}
	rec.count++
	if rec.count >= loginMaxFailures {
		rec.lockedUntil = now.Add(loginLockout)
		rec.count = 0
		rec.first = now
	}
}

func (g *authGate) clearFails(ip string) {
	g.mu.Lock()
	delete(g.fails, ip)
	g.mu.Unlock()
}

// ---------------- helpers ----------------

func secureEq(a, b string) bool {
	ha := sha256.Sum256([]byte(a))
	hb := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(ha[:], hb[:]) == 1
}

// clientIP uses the LAST X-Forwarded-For entry (the one added by the
// nearest proxy, e.g. Render) because earlier entries can be spoofed by
// the client.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if ip := strings.TrimSpace(parts[len(parts)-1]); ip != "" {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func isHTTPS(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func (g *authGate) renderLogin(w http.ResponseWriter, status int, errMsg string) {
	errHTML := ""
	if errMsg != "" {
		errHTML = `<div class="err">` + html.EscapeString(errMsg) + `</div>`
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(strings.Replace(loginPageHTML, "{{ERROR}}", errHTML, 1)))
}

const loginPageHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MicroFlow &mdash; Login</title>
<link rel="icon" href="data:,">
<style>
  :root { --bg:#f4f5f7; --surface:#fff; --border:#e2e4e9; --text:#1c1f26; --dim:#6b7280; --accent:#4f46e5; --error:#dc2626; --error-bg:#fef2f2; }
  * { box-sizing: border-box; }
  html, body { margin:0; height:100%; background:var(--bg); color:var(--text);
    font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }
  body { display:flex; align-items:center; justify-content:center; padding:16px; }
  .card { width:100%; max-width:360px; background:var(--surface); border:1px solid var(--border);
    border-radius:12px; padding:28px 24px; box-shadow:0 4px 18px rgba(0,0,0,.06); }
  .brand { display:flex; align-items:center; gap:10px; margin-bottom:20px; }
  .mark { width:30px; height:30px; background:var(--accent); color:#fff; border-radius:8px;
    display:flex; align-items:center; justify-content:center; font-weight:700; }
  .name { font-size:18px; font-weight:600; }
  label { display:block; font-size:12px; color:var(--dim); margin:14px 0 4px; }
  input { width:100%; padding:10px 12px; font:inherit; color:inherit; background:#fff;
    border:1px solid var(--border); border-radius:8px; outline:none; }
  input:focus { border-color:var(--accent); box-shadow:0 0 0 3px #eef0fe; }
  button { width:100%; margin-top:20px; padding:11px; font:inherit; font-weight:600; color:#fff;
    background:var(--accent); border:0; border-radius:8px; cursor:pointer; }
  button:hover { filter:brightness(1.08); }
  .err { margin-bottom:6px; padding:9px 12px; background:var(--error-bg); color:var(--error);
    border-radius:8px; font-size:13px; }
</style>
</head>
<body>
  <form class="card" method="post" action="/login" autocomplete="on">
    <div class="brand"><span class="mark">M</span><span class="name">MicroFlow</span></div>
    {{ERROR}}
    <label for="u">User name</label>
    <input id="u" name="username" type="text" autocomplete="username" autocapitalize="none" spellcheck="false" required autofocus>
    <label for="p">Password</label>
    <input id="p" name="password" type="password" autocomplete="current-password" required>
    <button type="submit">Sign in</button>
  </form>
</body>
</html>`
