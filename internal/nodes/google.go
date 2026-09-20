package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"microflow/internal/engine"
	"microflow/internal/expr"
	"microflow/internal/model"
	"microflow/internal/vault"
)

// --- Google Sheets: automatic spreadsheet ID from GOOGLE_SHEETS_URL ---
//
// The spreadsheet ID is no longer read from the node's "documentId"
// parameter (which required editing the workflow/hardcoding an ID
// per-deployment). Instead it is derived once per call from the single
// GOOGLE_SHEETS_URL environment variable, so switching spreadsheets is
// just changing that one env var -- no code or workflow JSON edits.
//
// Accepts any normal Sheets URL, including one with a trailing
// "/edit", extra path segments, or query params like "?usp=sharing" /
// "?usp=drivesdk" / "#gid=0". Also accepts a bare spreadsheet ID
// directly, for flexibility.

// validSpreadsheetID matches the character set Google uses for
// spreadsheet IDs. Real IDs are ~44 chars; 20 is a conservative floor
// that rejects obvious typos/empty values without being brittle to
// Google changing the exact length.
var validSpreadsheetID = regexp.MustCompile(`^[a-zA-Z0-9_-]{20,}$`)

// extractSpreadsheetIDFromURL pulls the ID out of a
// ".../spreadsheets/d/<ID>/..." URL. Returns "" if the URL doesn't
// contain that marker.
func extractSpreadsheetIDFromURL(rawURL string) string {
	const marker = "/d/"
	i := strings.Index(rawURL, marker)
	if i == -1 {
		return ""
	}
	rest := rawURL[i+len(marker):]
	end := len(rest)
	for _, sep := range []string{"/", "?", "#"} {
		if j := strings.Index(rest, sep); j != -1 && j < end {
			end = j
		}
	}
	return rest[:end]
}

// resolveSpreadsheetID reads GOOGLE_SHEETS_URL and returns the
// spreadsheet ID to use for every Sheets operation. Returns a clear,
// actionable error if the variable is missing or doesn't contain a
// recognizable/valid ID.
func resolveSpreadsheetID() (string, error) {
	raw := strings.TrimSpace(os.Getenv("GOOGLE_SHEETS_URL"))
	if raw == "" {
		return "", errors.New(`GOOGLE_SHEETS_URL is not set -- set it to your Google Sheets URL, e.g. "https://docs.google.com/spreadsheets/d/<ID>/edit?usp=sharing"`)
	}
	id := extractSpreadsheetIDFromURL(raw)
	if id == "" {
		// Allow a bare spreadsheet ID (no URL wrapper) too.
		id = raw
	}
	if !validSpreadsheetID.MatchString(id) {
		return "", fmt.Errorf("GOOGLE_SHEETS_URL does not contain a valid spreadsheet ID (got %q) -- expected a URL like \"https://docs.google.com/spreadsheets/d/<ID>/edit\"", raw)
	}
	return id, nil
}

// --- Google Sheets: target tab from the gid in GOOGLE_SHEETS_URL ---
//
// GOOGLE_SHEETS_URL is the single configuration source. Besides the spreadsheet
// ID it must carry the target tab's gid (".../edit#gid=123456789"). The Sheets
// API addresses tabs by NAME, so the gid is resolved to the exact tab title with
// one authenticated spreadsheets.get metadata call using the same connected
// Google account as the Sheets node itself (no extra credential, no dependency).
// There is deliberately NO fallback: no gid, an unresolvable gid or a failed
// lookup is a fatal GOOGLE_SHEETS_CONFIG_ERROR (never swallowed by
// continueOnFail), so nothing is ever read from or written to a wrong tab.

// defaultSheetColumns is only the column span; the tab always comes from the gid.
const defaultSheetColumns = "A:Z"

var (
	sheetGIDPattern    = regexp.MustCompile(`[#&?]gid=(\d+)`)
	sheetColumnsFormat = regexp.MustCompile(`^[A-Za-z]{1,3}[0-9]*(:[A-Za-z]{1,3}[0-9]*)?$`)
)

func sheetsConfigError(format string, args ...any) error {
	return engine.Fatal(fmt.Errorf("GOOGLE_SHEETS_CONFIG_ERROR: "+format, args...))
}

// resolveSheetsGID returns the required gid from GOOGLE_SHEETS_URL.
func resolveSheetsGID() (string, error) {
	raw := strings.TrimSpace(os.Getenv("GOOGLE_SHEETS_URL"))
	m := sheetGIDPattern.FindStringSubmatch(raw)
	if m == nil {
		return "", sheetsConfigError("GOOGLE_SHEETS_URL must include the target sheet gid (e.g. https://docs.google.com/spreadsheets/d/<ID>/edit#gid=123456789); no default or first-tab fallback is used")
	}
	return m[1], nil
}

// Tiny bounded cache (max 4 entries, 5 min TTL) so the metadata call is not
// repeated for every Sheets node/row; a renamed tab is picked up within the TTL.
type sheetTitleEntry struct {
	title string
	at    time.Time
}

const (
	sheetTitleTTL      = 5 * time.Minute
	sheetTitleCacheMax = 4
)

var (
	sheetTitleMu    sync.Mutex
	sheetTitleCache = map[string]sheetTitleEntry{}
)

// resolveSheetTitle maps gid -> exact tab title via the authenticated Sheets metadata API.
func resolveSheetTitle(ctx context.Context, token, spreadsheetID, gid, operationID string) (string, error) {
	want, err := strconv.ParseInt(gid, 10, 64)
	if err != nil {
		return "", sheetsConfigError("gid %q in GOOGLE_SHEETS_URL is not a number", gid)
	}
	key := spreadsheetID + "#" + gid
	sheetTitleMu.Lock()
	if e, ok := sheetTitleCache[key]; ok && time.Since(e.at) < sheetTitleTTL {
		sheetTitleMu.Unlock()
		return e.title, nil
	}
	sheetTitleMu.Unlock()

	var resp struct {
		Sheets []struct {
			Properties struct {
				SheetID int64  `json:"sheetId"`
				Title   string `json:"title"`
			} `json:"properties"`
		} `json:"sheets"`
	}
	metaURL := fmt.Sprintf("https://sheets.googleapis.com/v4/spreadsheets/%s?fields=sheets.properties(sheetId,title)", spreadsheetID)
	if err := googleAPICall(ctx, "GET", metaURL, token, nil, &resp, operationID); err != nil {
		return "", sheetsConfigError("unable to resolve sheet gid %s (authenticated Sheets metadata lookup failed): %v", gid, err)
	}
	for _, sh := range resp.Sheets {
		if sh.Properties.SheetID == want && sh.Properties.Title != "" {
			sheetTitleMu.Lock()
			if len(sheetTitleCache) >= sheetTitleCacheMax {
				for k := range sheetTitleCache {
					delete(sheetTitleCache, k)
					break
				}
			}
			sheetTitleCache[key] = sheetTitleEntry{title: sh.Properties.Title, at: time.Now()}
			sheetTitleMu.Unlock()
			return sh.Properties.Title, nil
		}
	}
	return "", sheetsConfigError("unable to resolve sheet gid %s: no tab with that gid exists in the spreadsheet", gid)
}

// buildSheetRange returns the URL-escaped A1 range "'<tab>'!<columns>". The node's
// optional "range" parameter may only hold columns (default A:Z); a tab name in it
// is rejected so the tab can only ever come from the gid in GOOGLE_SHEETS_URL.
func buildSheetRange(node *model.Node, title string) (string, error) {
	cols := strings.TrimSpace(node.ParamString("range", ""))
	if cols == "" {
		cols = defaultSheetColumns
	}
	if !sheetColumnsFormat.MatchString(cols) {
		return "", sheetsConfigError("node %q: range %q must be columns only (e.g. A:Z); the tab is taken from the gid in GOOGLE_SHEETS_URL", node.Name, cols)
	}
	return url.PathEscape("'" + strings.ReplaceAll(title, "'", "''") + "'!" + cols), nil
}

// All three executors below assume the credential vault (internal/vault)
// hands back a valid, already-refreshed OAuth2 access token under the
// "accessToken" key for the node's logical credential name. Token
// refresh itself (using a stored refresh_token + client_id/secret) is
// vault responsibility, not the node executor's -- see vault/oauth.go.
//
// Credential resolution order, per node, is:
//  1. Creds: an explicit per-node/per-workflow override (unchanged from
//     before the "Connect with Google" feature -- still set via
//     cmd/setcred or a node's side-panel override section).
//  2. Accounts: the connected Google account for this node's SERVICE
//     (gmail/youtube/sheets), set up once via the "Google Connections"
//     page's Connect button and shared by every node of that type
//     across every workflow -- see vault.GoogleServiceAccounts.
//
// UNVERIFIED: these call real Google endpoints and can only be
// exercised with real OAuth credentials, which is explicitly a
// local-machine test step in the user's own plan (rule 25).

// GoogleAccountResolver resolves the connected-account credential for
// one Google service ("gmail" | "youtube" | "sheets"). Implemented by
// *vault.GoogleServiceAccounts; a distinct (smaller) interface than
// engine.CredentialResolver so executors can accept it without vault
// leaking into the engine package, and so a nil Accounts (no
// GOOGLE_OAUTH_CLIENT_ID/SECRET configured on this server) is trivially
// checked by callers instead of requiring a fake implementation in tests.
type GoogleAccountResolver interface {
	Resolve(ctx context.Context, service string) (map[string]string, error)
}

// resolveGoogleCreds tries the per-node override first (Creds), then
// falls back to the service's connected account (Accounts, may be nil
// if Google OAuth isn't configured on this server). Any error is
// translated so a revoked/expired Google connection never surfaces
// Google's raw "invalid_grant" to a workflow's execution log.
func resolveGoogleCreds(ctx context.Context, creds engine.CredentialResolver, accounts GoogleAccountResolver, service, workflowID, nodeName string) (map[string]string, error) {
	secrets, err := creds.Resolve(ctx, workflowID, nodeName)
	if err == nil {
		return secrets, nil
	}
	if accounts != nil {
		secrets, acctErr := accounts.Resolve(ctx, service)
		if acctErr == nil {
			return secrets, nil
		}
		err = acctErr
	}
	if errors.Is(err, vault.ErrGoogleReauthRequired) {
		return nil, errors.New("Google connection expired. Please reconnect.")
	}
	return nil, err
}

type GoogleSheetsExecutor struct {
	Creds    engine.CredentialResolver
	Accounts GoogleAccountResolver
	Service  string // "sheets"
}

func (e *GoogleSheetsExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	creds, err := resolveGoogleCreds(ctx, e.Creds, e.Accounts, e.Service, rc.Workflow.ID, node.Name)
	if err != nil {
		return nil, fmt.Errorf("googleSheets %q: credential error: %w", node.Name, err)
	}
	token := creds["accessToken"]
	spreadsheetID, err := resolveSpreadsheetID()
	if err != nil {
		return nil, fmt.Errorf("googleSheets %q: %w", node.Name, sheetsConfigError("%v", err))
	}
	gid, err := resolveSheetsGID()
	if err != nil {
		return nil, fmt.Errorf("googleSheets %q: %w", node.Name, err)
	}
	tabTitle, err := resolveSheetTitle(ctx, token, spreadsheetID, gid, rc.CurrentOperationID)
	if err != nil {
		return nil, fmt.Errorf("googleSheets %q: %w", node.Name, err)
	}
	sheetRange, err := buildSheetRange(node, tabTitle)
	if err != nil {
		return nil, fmt.Errorf("googleSheets %q: %w", node.Name, err)
	}
	operation, _ := node.Parameters["operation"].(string) // "read" | "append" | "update"

	var out []model.Item
	switch operation {
	case "append":
		for _, it := range flatten(input) {
			row := jsonToRow(it.JSON)
			body, _ := json.Marshal(map[string]any{"values": [][]any{row}})
			url := fmt.Sprintf("https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s:append?valueInputOption=USER_ENTERED", spreadsheetID, sheetRange)
			if err := googleAPICall(ctx, "POST", url, token, body, nil, rc.CurrentOperationID); err != nil {
				return nil, fmt.Errorf("googleSheets %q append: %w", node.Name, err)
			}
			out = append(out, it)
		}
	case "update":
		for _, it := range flatten(input) {
			row := jsonToRow(it.JSON)
			body, _ := json.Marshal(map[string]any{"values": [][]any{row}})
			url := fmt.Sprintf("https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s?valueInputOption=USER_ENTERED", spreadsheetID, sheetRange)
			if err := googleAPICall(ctx, "PUT", url, token, body, nil, rc.CurrentOperationID); err != nil {
				return nil, fmt.Errorf("googleSheets %q update: %w", node.Name, err)
			}
			out = append(out, it)
		}
	default: // read
		var resp struct {
			Values [][]any `json:"values"`
		}
		url := fmt.Sprintf("https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s", spreadsheetID, sheetRange)
		if err := googleAPICall(ctx, "GET", url, token, nil, &resp, rc.CurrentOperationID); err != nil {
			return nil, fmt.Errorf("googleSheets %q read: %w", node.Name, err)
		}
		if len(resp.Values) > 0 {
			header := resp.Values[0]
			for _, row := range resp.Values[1:] {
				m := map[string]any{}
				for i, h := range header {
					key := fmt.Sprintf("%v", h)
					if i < len(row) {
						m[key] = row[i]
					}
				}
				out = append(out, model.Item{JSON: m})
			}
		}
	}
	return model.NodeOutput{out}, nil
}

type YouTubeExecutor struct {
	Creds    engine.CredentialResolver
	Accounts GoogleAccountResolver
	Service  string // "youtube"
}

func (e *YouTubeExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	creds, err := resolveGoogleCreds(ctx, e.Creds, e.Accounts, e.Service, rc.Workflow.ID, node.Name)
	if err != nil {
		return nil, fmt.Errorf("youTube %q: credential error: %w", node.Name, err)
	}
	token := creds["accessToken"]
	resource, _ := node.Parameters["resource"].(string) // "video" | "thumbnail"
	operation, _ := node.Parameters["operation"].(string)

	var out []model.Item
	for _, it := range flatten(input) {
		exprCtx := rc.ExprContext(it.JSON)

		if resource == "thumbnail" || operation == "setThumbnail" {
			videoID, _ := expr.EvalValue(node.ParamString("videoId", ""), exprCtx)
			ref, ok := it.Binary["data"]
			if !ok {
				return nil, fmt.Errorf("youTube %q: setThumbnail needs binary image data", node.Name)
			}
			url := fmt.Sprintf("https://www.googleapis.com/upload/youtube/v3/thumbnails/set?videoId=%v", videoID)
			if err := uploadBinaryFile(ctx, url, token, ref.FileName, ref.MimeType, rc.CurrentOperationID); err != nil {
				return nil, fmt.Errorf("youTube %q: %w", node.Name, err)
			}
			out = append(out, model.Item{JSON: map[string]any{"videoId": videoID, "thumbnailSet": true}})
			continue
		}

		// video upload (resumable upload simplified to a single multipart
		// request here; production use should switch to YouTube's
		// resumable-upload protocol for files that can be multi-hundred-MB)
		ref, ok := it.Binary["data"]
		if !ok {
			return nil, fmt.Errorf("youTube %q: upload needs binary video data", node.Name)
		}
		title, _ := expr.EvalValue(node.ParamString("title", ""), exprCtx)
		description, _ := expr.EvalValue(node.ParamString("description", ""), exprCtx)
		snippet := map[string]any{
			"snippet": map[string]any{
				"title":       title,
				"description": description,
			},
			"status": map[string]any{"privacyStatus": node.ParamString("privacyStatus", "private")},
		}
		videoID, err := uploadVideoMultipart(ctx, token, ref.FileName, ref.MimeType, snippet, rc.CurrentOperationID)
		if err != nil {
			return nil, fmt.Errorf("youTube %q upload: %w", node.Name, err)
		}
		// Field name bug: this used to emit only "videoId", but every
		// real n8n YouTube-node-derived workflow (and the YouTube Data
		// API v3 videos.insert response itself, which literally returns
		// {"id": "...", "snippet": {...}, "status": {...}}) reads the
		// uploaded video's ID off "id", e.g. `$node("YouTube Upload
		// (Scheduled)").json.id`. That expression silently resolved to
		// undefined against the old shape -- not a thrown error, so it
		// surfaced downstream as a literal "<nil>"/"undefined" baked
		// into thumbnail-set URLs, comment bodies, and log messages
		// instead of the real video ID. Emit "id" (matching real n8n)
		// and keep "videoId" alongside it for any script written
		// against MicroFlow's previous shape.
		out = append(out, model.Item{JSON: map[string]any{"id": videoID, "videoId": videoID}})
	}
	return model.NodeOutput{out}, nil
}

type GmailExecutor struct {
	Creds    engine.CredentialResolver
	Accounts GoogleAccountResolver
	Service  string // "gmail"
}

func (e *GmailExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	creds, err := resolveGoogleCreds(ctx, e.Creds, e.Accounts, e.Service, rc.Workflow.ID, node.Name)
	if err != nil {
		return nil, fmt.Errorf("gmail %q: credential error: %w", node.Name, err)
	}
	token := creds["accessToken"]

	var out []model.Item
	for _, it := range flatten(input) {
		exprCtx := rc.ExprContext(it.JSON)
		to, _ := expr.EvalValue(node.ParamString("toRecipients", node.ParamString("to", "")), exprCtx)
		subject, _ := expr.EvalValue(node.ParamString("subject", ""), exprCtx)
		message, _ := expr.EvalValue(node.ParamString("message", node.ParamString("text", "")), exprCtx)

		raw := buildRFC2822(fmt.Sprintf("%v", to), fmt.Sprintf("%v", subject), fmt.Sprintf("%v", message))
		body, _ := json.Marshal(map[string]any{"raw": raw})
		url := "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
		if err := googleAPICall(ctx, "POST", url, token, body, nil, rc.CurrentOperationID); err != nil {
			return nil, fmt.Errorf("gmail %q: %w", node.Name, err)
		}
		out = append(out, model.Item{JSON: map[string]any{"sent": true, "to": to}})
	}
	return model.NodeOutput{out}, nil
}

// --- shared helpers ---

func googleAPICall(ctx context.Context, method, url, token string, body []byte, into any, operationID string) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if operationID != "" {
		req.Header.Set("X-MicroFlow-Operation-ID", operationID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Network-level failures (DNS, connection reset, timeout) are
		// transient by nature -- let the node's own RetryOnFail/MaxTries
		// budget (if configured) retry them.
		return fmt.Errorf("google api %s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode >= 300 {
		msg := fmt.Errorf("google api %s %s: status %d: %s", method, url, resp.StatusCode, truncate(string(respBody), 500))
		switch {
		case resp.StatusCode == 401 || resp.StatusCode == 403:
			// Invalid/expired/missing credential -- retrying the same
			// request with the same token can never succeed, so mark
			// this permanent: the engine stops retrying immediately and
			// reports a clear configuration/credential error instead of
			// burning the retry budget on guaranteed-identical failures.
			return engine.Permanent(fmt.Errorf("credential/configuration error (not retried): %w", msg))
		case resp.StatusCode == 429 || resp.StatusCode >= 500:
			// Rate limit or transient server error -- exactly what the
			// node's RetryOnFail/MaxTries + backoff exists for.
			return fmt.Errorf("transient error (will retry if configured): %w", msg)
		default:
			return msg
		}
	}
	if into != nil {
		return json.Unmarshal(respBody, into)
	}
	return nil
}

func uploadBinaryFile(ctx context.Context, url, token, filePath, mimeType, operationID string) error {
	f, err := openForUpload(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	req, err := http.NewRequestWithContext(ctx, "POST", url, f)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", mimeType)
	if operationID != "" {
		req.Header.Set("X-MicroFlow-Operation-ID", operationID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2000))
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func uploadVideoMultipart(ctx context.Context, token, filePath, mimeType string, metadata map[string]any, operationID string) (string, error) {
	f, err := openForUpload(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		defer pw.Close()
		defer mw.Close()
		metaPart, _ := mw.CreatePart(map[string][]string{"Content-Type": {"application/json; charset=UTF-8"}})
		metaBytes, _ := json.Marshal(metadata)
		metaPart.Write(metaBytes)

		videoPart, _ := mw.CreatePart(map[string][]string{"Content-Type": {mimeType}})
		buf := make([]byte, 256*1024)
		for {
			n, rerr := f.Read(buf)
			if n > 0 {
				videoPart.Write(buf[:n])
			}
			if rerr != nil {
				break
			}
		}
	}()

	req, err := http.NewRequestWithContext(ctx, "POST",
		"https://www.googleapis.com/upload/youtube/v3/videos?uploadType=multipart&part=snippet,status", pr)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
	if operationID != "" {
		req.Header.Set("X-MicroFlow-Operation-ID", operationID)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(respBody), 500))
	}
	var parsed struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", err
	}
	return parsed.ID, nil
}

func jsonToRow(m map[string]any) []any {
	row := make([]any, 0, len(m))
	for _, v := range m {
		row = append(row, v)
	}
	return row
}

func buildRFC2822(to, subject, body string) string {
	msg := fmt.Sprintf("To: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s", to, subject, body)
	return base64URLEncode([]byte(msg))
}
