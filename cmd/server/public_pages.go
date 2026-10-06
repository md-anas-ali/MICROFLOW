// Public (no-login) pages required for Google OAuth verification:
//
//	GET /          public homepage (only shown to visitors who are NOT
//	               signed in; a signed-in user, or any visitor when the
//	               login gate is disabled, still gets the dashboard)
//	GET /privacy   Privacy Policy
//	GET /terms     Terms of Service
//
// These pages are self-contained (inline CSS, no JS, no external assets) and
// never touch the database, the vault, or the OAuth flow. The OAuth callback
// (/api/oauth/google/callback), GOOGLE_OAUTH_REDIRECT_URL handling and the
// scopes in internal/vault are NOT modified by this file.
//
// Optional environment variables (both are plain, non-secret text):
//
//	MICROFLOW_PUBLIC_APP_NAME  product name shown on the pages (default "MicroFlow").
//	                           Keep it identical to the App name on your Google
//	                           OAuth consent screen.
//	MICROFLOW_CONTACT_EMAIL    contact address shown on the pages. Use the same
//	                           address as the support/developer email on the
//	                           consent screen.
package main

import (
	"html"
	"net/http"
	"os"
	"strings"
)

func publicAppName() string {
	if v := strings.TrimSpace(os.Getenv("MICROFLOW_PUBLIC_APP_NAME")); v != "" {
		return v
	}
	return "MicroFlow"
}

func publicContactHTML() string {
	if v := strings.TrimSpace(os.Getenv("MICROFLOW_CONTACT_EMAIL")); v != "" {
		e := html.EscapeString(v)
		return `<a href="mailto:` + e + `">` + e + `</a>`
	}
	return "the support email address listed on our Google OAuth consent screen"
}

// servePublicPage handles the public routes. It returns true when it wrote a
// response, false when the request should continue through the normal gate.
func (g *authGate) servePublicPage(w http.ResponseWriter, r *http.Request) bool {
	var title, body string
	switch strings.TrimSuffix(r.URL.Path, "/") {
	case "":
		// Exactly "/": only the signed-out visitor sees the homepage. When
		// login is disabled, or the visitor has a valid session, fall
		// through so the existing dashboard is served exactly as before.
		if r.URL.Path != "/" || !g.enabled || g.validSession(r) {
			return false
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			return false
		}
		title, body = "", publicHomeBody()
	case "/privacy":
		title, body = "Privacy Policy", publicPrivacyBody()
	case "/terms":
		title, body = "Terms of Service", publicTermsBody()
	default:
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write([]byte(publicPageShell(title, body)))
	}
	return true
}

func publicPageShell(title, body string) string {
	app := html.EscapeString(publicAppName())
	full := app
	if title != "" {
		full = title + " - " + app
	}
	var b strings.Builder
	b.WriteString(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>` + full + `</title>
<meta name="description" content="` + app + ` is a self-hosted workflow automation tool that can connect to your own Google account (Gmail, YouTube, Google Sheets) to run the workflows you build.">
<link rel="icon" href="data:,">
<style>
  :root { --bg:#f4f5f7; --surface:#fff; --border:#e2e4e9; --text:#1c1f26; --dim:#6b7280; --accent:#4f46e5; }
  * { box-sizing: border-box; }
  html, body { margin:0; background:var(--bg); color:var(--text);
    font:16px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }
  header, main, footer { max-width:820px; margin:0 auto; padding:0 20px; }
  header { display:flex; align-items:center; gap:10px; padding-top:22px; padding-bottom:10px; }
  header a.brand { display:flex; align-items:center; gap:10px; color:inherit; text-decoration:none; }
  .mark { width:32px; height:32px; background:var(--accent); color:#fff; border-radius:8px;
    display:flex; align-items:center; justify-content:center; font-weight:700; }
  .name { font-size:19px; font-weight:600; }
  nav { margin-left:auto; display:flex; gap:16px; font-size:14px; }
  a { color:var(--accent); }
  .card { background:var(--surface); border:1px solid var(--border); border-radius:12px;
    padding:28px 26px; margin:18px 0; box-shadow:0 4px 18px rgba(0,0,0,.05); }
  h1 { font-size:30px; line-height:1.25; margin:0 0 12px; }
  h2 { font-size:19px; margin:26px 0 6px; }
  p, li { color:#2b303a; }
  ul { padding-left:22px; }
  .dim { color:var(--dim); font-size:14px; }
  .btn { display:inline-block; margin-top:8px; padding:11px 22px; background:var(--accent); color:#fff;
    border-radius:8px; font-weight:600; text-decoration:none; }
  .btn:hover { filter:brightness(1.08); }
  footer { padding-top:8px; padding-bottom:32px; font-size:14px; color:var(--dim); }
  footer a { margin-right:14px; }
</style>
</head>
<body>
<header>
  <a class="brand" href="/"><span class="mark">M</span><span class="name">` + app + `</span></a>
  <nav><a href="/privacy">Privacy</a><a href="/terms">Terms</a><a href="/login">Sign in</a></nav>
</header>
<main>
` + body + `
</main>
<footer>
  <a href="/">Home</a><a href="/privacy">Privacy Policy</a><a href="/terms">Terms of Service</a>
</footer>
</body>
</html>`)
	return b.String()
}

func publicHomeBody() string {
	app := html.EscapeString(publicAppName())
	return `<div class="card">
  <h1>` + app + `</h1>
  <p>` + app + ` is a self-hosted workflow automation tool. You build workflows out of nodes
  (schedules, webhooks, HTTP requests, code, files and more) and ` + app + ` runs them on your
  own server, on a schedule or on demand.</p>
  <p>Optionally, a signed-in user can connect their <strong>own</strong> Google account so that their
  workflows can use these Google services on their behalf:</p>
  <ul>
    <li><strong>Gmail</strong> &mdash; send emails that the user's own workflow composes.</li>
    <li><strong>YouTube</strong> &mdash; upload and manage videos on the user's own channel.</li>
    <li><strong>Google Sheets</strong> &mdash; read and write the user's own spreadsheets.</li>
  </ul>
  <p>Each Google service is connected separately, only when the user clicks <em>Connect</em>, and can be
  disconnected at any time. Google data is used only to run the workflows the user has created.
  See our <a href="/privacy">Privacy Policy</a> and <a href="/terms">Terms of Service</a>.</p>
  <a class="btn" href="/login">Sign in</a>
</div>`
}

func publicPrivacyBody() string {
	app := html.EscapeString(publicAppName())
	contact := publicContactHTML()
	return `<div class="card">
  <h1>Privacy Policy</h1>
  <p class="dim">Last updated: October 2026</p>
  <p>This policy explains what information ` + app + ` handles when you use it, and in particular what it
  does with data from your Google account if you choose to connect one.</p>

  <h2>1. Information we handle</h2>
  <ul>
    <li><strong>Account sign-in.</strong> Access to ` + app + ` is protected by a user name and password
    configured by the server's operator. A session cookie keeps you signed in.</li>
    <li><strong>Workflows and settings.</strong> The workflows, environment values and credentials you create
    are stored in the operator's database. Secrets and OAuth tokens are encrypted before they are stored.</li>
    <li><strong>Google account data (only if you click Connect).</strong> See section 2.</li>
    <li><strong>Execution logs.</strong> Run history is kept only for a short time (about 12 hours) and is then
    deleted automatically.</li>
  </ul>

  <h2>2. Google user data</h2>
  <p>When you click <em>Connect</em> for a Google service, ` + app + ` asks Google for the minimum access
  that service needs:</p>
  <ul>
    <li><strong>Your email address</strong> (<code>userinfo.email</code>, with <code>openid</code>) &mdash; shown in the
    dashboard as &ldquo;Connected as&rdquo; so you know which account is linked. It does not give access to your mail.</li>
    <li><strong>Gmail</strong> (<code>gmail.send</code>) &mdash; only to send emails that your own workflow composes.
    ` + app + ` does not read, list or delete your email.</li>
    <li><strong>YouTube</strong> (<code>youtube.upload</code>, <code>youtube</code>) &mdash; only to upload and manage videos
    on your own channel as directed by your workflow.</li>
    <li><strong>Google Sheets</strong> (<code>spreadsheets</code>) &mdash; only to read and write the spreadsheets your
    workflow points to.</li>
  </ul>
  <p>Each service is requested separately and independently of the others. Google data is accessed only when a workflow
  you created runs, and only to perform the action you configured.</p>

  <h2>3. How we use Google data</h2>
  <p>We use Google user data solely to provide the features described above to you. We do not sell it, do not use it
  for advertising, do not use it to build or train generalised AI/ML models, and do not transfer it to third parties
  except as needed to carry out the action you asked for, to comply with law, or with your consent. No person reads
  your Google data unless you ask for support and explicitly allow it, it is necessary for security or abuse
  investigation, or the law requires it.</p>
  <p>` + app + `'s use and transfer to any other app of information received from Google APIs will adhere to the
  <a href="https://developers.google.com/terms/api-services-user-data-policy" rel="noopener">Google API Services User Data Policy</a>,
  including the Limited Use requirements.</p>

  <h2>4. Storage, security and retention</h2>
  <p>The OAuth refresh/access tokens issued by Google are stored encrypted (AES-GCM, using the server's master key) and
  are used only to call the Google APIs you connected. Tokens are kept until you disconnect the service or revoke access.
  Data returned by Google while a workflow runs is processed in memory and may appear in the short-lived execution log
  described in section 1.</p>

  <h2>5. Your choices and deleting your data</h2>
  <ul>
    <li>Click <em>Disconnect</em> next to a Google service in the dashboard to delete the stored tokens for it.</li>
    <li>You can also revoke access at any time at
    <a href="https://myaccount.google.com/permissions" rel="noopener">myaccount.google.com/permissions</a>.</li>
    <li>To have other stored data removed, contact us (section 7) or the operator of this installation.</li>
  </ul>

  <h2>6. Changes</h2>
  <p>We may update this policy; the date above shows the latest revision. Material changes affecting Google user data
  will be reflected here before they take effect.</p>

  <h2>7. Contact</h2>
  <p>Questions or requests about this policy: ` + contact + `.</p>
</div>`
}

func publicTermsBody() string {
	app := html.EscapeString(publicAppName())
	contact := publicContactHTML()
	return `<div class="card">
  <h1>Terms of Service</h1>
  <p class="dim">Last updated: October 2026</p>
  <p>By signing in to or using ` + app + ` you agree to these terms. If you do not agree, please do not use the service.</p>

  <h2>1. The service</h2>
  <p>` + app + ` is a workflow automation tool. It lets authorised users build and run workflows and, optionally,
  connect their own Google account (Gmail, YouTube, Google Sheets) so that those workflows can act on their behalf.</p>

  <h2>2. Accounts and access</h2>
  <p>Access is limited to users given credentials by the operator. You are responsible for keeping your credentials
  confidential and for all activity under your account.</p>

  <h2>3. Acceptable use</h2>
  <ul>
    <li>Use the service only for lawful purposes and in line with the terms of every third-party service you connect,
    including the <a href="https://policies.google.com/terms" rel="noopener">Google Terms of Service</a> and the
    <a href="https://www.youtube.com/t/terms" rel="noopener">YouTube Terms of Service</a>.</li>
    <li>Do not send spam or unsolicited bulk email, upload content you have no right to publish, or attempt to disrupt,
    probe or gain unauthorised access to the service or other people's data.</li>
    <li>You are solely responsible for the workflows you create and the content they send, upload or modify.</li>
  </ul>

  <h2>4. Google account connection</h2>
  <p>Connecting a Google account is optional and under your control. You can disconnect it at any time from the
  dashboard or from your Google Account permissions page. How Google data is handled is described in our
  <a href="/privacy">Privacy Policy</a>.</p>

  <h2>5. Availability and changes</h2>
  <p>The service is provided &ldquo;as is&rdquo; and &ldquo;as available&rdquo;, without warranties of any kind. We may
  change, suspend or discontinue features at any time.</p>

  <h2>6. Limitation of liability</h2>
  <p>To the maximum extent permitted by law, ` + app + ` and its operator are not liable for indirect, incidental or
  consequential damages, or for loss of data, profits or revenue, arising from your use of the service.</p>

  <h2>7. Termination</h2>
  <p>We may suspend or end access for anyone who breaches these terms. You may stop using the service at any time.</p>

  <h2>8. Changes to these terms</h2>
  <p>We may update these terms; continued use after an update means you accept the revised terms.</p>

  <h2>9. Contact</h2>
  <p>Questions about these terms: ` + contact + `.</p>
</div>`
}
