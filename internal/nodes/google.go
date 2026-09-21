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
	"sort"
	"strconv"
	"strings"

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
		return nil, fmt.Errorf("googleSheets %q: %w", node.Name, err)
	}
	sheetRange := node.ParamString("range", "A1:Z1000")
	operation, _ := node.Parameters["operation"].(string) // "read" | "append" | "update"

	var out []model.Item
	tab := sheetTabFromRange(sheetRange)
	switch operation {
	case "append":
		// Header-aware append: every item's fields are written into the column
		// whose row-1 header has the same name (new field names are added as new
		// header columns). The old code wrote map values in random Go-map order
		// with no header mapping, which scrambled the sheet.
		items := flatten(input)
		if err := sheetsAppendItems(ctx, rc, token, spreadsheetID, tab, sheetRange, items); err != nil {
			return nil, fmt.Errorf("googleSheets %q append: %w", node.Name, err)
		}
		out = append(out, items...)
	case "update":
		// Row-targeted update: finds the LAST row whose <matchingColumn> equals
		// the item's value and rewrites only the item's own fields in that row.
		// If no row matches, the item is appended (upsert). The old code PUT
		// the item over A1 of the range, overwriting the header row.
		matchCol := strings.TrimSpace(node.ParamString("matchingColumn", ""))
		if matchCol == "" {
			return nil, fmt.Errorf("googleSheets %q update: set the node parameter \"matchingColumn\" to the header name that identifies the row (e.g. runId)", node.Name)
		}
		for _, it := range flatten(input) {
			if err := sheetsUpdateItem(ctx, rc, token, spreadsheetID, tab, sheetRange, matchCol, it); err != nil {
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
					key := strings.TrimSpace(fmt.Sprintf("%v", h))
					if key == "" {
						continue
					}
					if i < len(row) {
						m[key] = row[i]
					}
				}
				out = append(out, model.Item{JSON: m})
			}
		}
		// An empty sheet (header only) must still let the workflow continue:
		// downstream nodes never run when a node outputs zero items.
		if len(out) == 0 {
			out = append(out, model.Item{JSON: map[string]any{}})
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
		snip := map[string]any{
			"title":       title,
			"description": description,
		}
		status := map[string]any{"privacyStatus": node.ParamString("privacyStatus", "private")}
		if cat, _ := expr.EvalValue(node.ParamString("categoryId", ""), exprCtx); cat != nil && fmt.Sprint(cat) != "" {
			snip["categoryId"] = fmt.Sprint(cat)
		}
		if opts, ok := node.Parameters["options"].(map[string]any); ok {
			if raw, ok := opts["publishAt"].(string); ok && raw != "" {
				if v, _ := expr.EvalValue(raw, exprCtx); v != nil && fmt.Sprint(v) != "" {
					// Scheduled publish: YouTube requires private + publishAt (RFC3339).
					status["publishAt"] = fmt.Sprint(v)
					status["privacyStatus"] = "private"
				}
			}
		}
		snippet := map[string]any{"snippet": snip, "status": status}
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

// ---------------------------------------------------------------------------
// Google Sheets helpers: header-aware append / row-targeted update.
// Row 1 of the tab is the header; item fields are matched to it BY NAME.
// ---------------------------------------------------------------------------

// sheetTabFromRange returns the tab name of a range like "UsedTopics!A:Z".
func sheetTabFromRange(r string) string {
	if i := strings.Index(r, "!"); i > 0 {
		return strings.Trim(r[:i], "'")
	}
	return ""
}

// a1 builds a quoted A1 reference for a tab and a cell/range part.
func a1(tab, part string) string {
	if tab == "" {
		return part
	}
	return "'" + strings.ReplaceAll(tab, "'", "''") + "'!" + part
}

// colName converts a zero-based column index to A, B, ... Z, AA, AB, ...
func colName(i int) string {
	s := ""
	for i >= 0 {
		s = string(rune('A'+i%26)) + s
		i = i/26 - 1
	}
	return s
}

// sheetCellValue turns any JSON value into something a Sheets cell can hold.
func sheetCellValue(v any) any {
	switch t := v.(type) {
	case nil:
		return ""
	case string, bool, float64, float32, int, int32, int64, json.Number:
		return t
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprint(t)
		}
		return string(b)
	}
}

func sheetCellString(v any) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func sheetValuesURL(spreadsheetID, ref, query string) string {
	u := fmt.Sprintf("https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s", spreadsheetID, url.PathEscape(ref))
	if query != "" {
		u += "?" + query
	}
	return u
}

func sheetsReadValues(ctx context.Context, rc *engine.RunContext, token, spreadsheetID, ref string) ([][]any, error) {
	var resp struct {
		Values [][]any `json:"values"`
	}
	if err := googleAPICall(ctx, "GET", sheetValuesURL(spreadsheetID, ref, ""), token, nil, &resp, rc.CurrentOperationID); err != nil {
		return nil, err
	}
	return resp.Values, nil
}

func sheetsWriteHeader(ctx context.Context, rc *engine.RunContext, token, spreadsheetID, tab string, header []string) error {
	row := make([]any, len(header))
	for i, h := range header {
		row[i] = h
	}
	body, _ := json.Marshal(map[string]any{"values": [][]any{row}})
	ref := a1(tab, "A1:"+colName(len(header)-1)+"1")
	return googleAPICall(ctx, "PUT", sheetValuesURL(spreadsheetID, ref, "valueInputOption=RAW"), token, body, nil, rc.CurrentOperationID)
}

// sheetsEnsureHeader makes sure every key of the items has a header column,
// appending missing names to the right of the existing header (never
// reordering or overwriting existing header cells).
func sheetsEnsureHeader(ctx context.Context, rc *engine.RunContext, token, spreadsheetID, tab string, header []string, items []model.Item) ([]string, error) {
	idx := map[string]int{}
	for i, h := range header {
		if h != "" {
			idx[h] = i
		}
	}
	changed := false
	for _, it := range items {
		keys := make([]string, 0, len(it.JSON))
		for k := range it.JSON {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if strings.TrimSpace(k) == "" {
				continue
			}
			if _, ok := idx[k]; !ok {
				header = append(header, k)
				idx[k] = len(header) - 1
				changed = true
			}
		}
	}
	if changed {
		if err := sheetsWriteHeader(ctx, rc, token, spreadsheetID, tab, header); err != nil {
			return nil, err
		}
	}
	return header, nil
}

func sheetHeaderFromRow(row []any) []string {
	header := make([]string, len(row))
	for i, h := range row {
		header[i] = strings.TrimSpace(fmt.Sprint(h))
	}
	return header
}

func sheetsAppendItems(ctx context.Context, rc *engine.RunContext, token, spreadsheetID, tab, sheetRange string, items []model.Item) error {
	if len(items) == 0 {
		return nil
	}
	rows, err := sheetsReadValues(ctx, rc, token, spreadsheetID, a1(tab, "1:1"))
	if err != nil {
		return err
	}
	var header []string
	if len(rows) > 0 {
		header = sheetHeaderFromRow(rows[0])
	}
	header, err = sheetsEnsureHeader(ctx, rc, token, spreadsheetID, tab, header, items)
	if err != nil {
		return err
	}
	if len(header) == 0 {
		return errors.New("nothing to append: item has no fields")
	}
	idx := map[string]int{}
	for i, h := range header {
		if h != "" {
			idx[h] = i
		}
	}
	values := make([][]any, 0, len(items))
	for _, it := range items {
		row := make([]any, len(header))
		for i := range row {
			row[i] = ""
		}
		for k, v := range it.JSON {
			if i, ok := idx[k]; ok {
				row[i] = sheetCellValue(v)
			}
		}
		values = append(values, row)
	}
	body, _ := json.Marshal(map[string]any{"values": values})
	ref := a1(tab, "A:"+colName(len(header)-1))
	if tab == "" {
		ref = sheetRange
	}
	return googleAPICall(ctx, "POST", sheetValuesURL(spreadsheetID, ref, "valueInputOption=RAW&insertDataOption=INSERT_ROWS"), token, body, nil, rc.CurrentOperationID)
}

func sheetsUpdateItem(ctx context.Context, rc *engine.RunContext, token, spreadsheetID, tab, sheetRange, matchCol string, it model.Item) error {
	want := sheetCellString(it.JSON[matchCol])
	if want == "" {
		return fmt.Errorf("item has no value for matchingColumn %q", matchCol)
	}
	values, err := sheetsReadValues(ctx, rc, token, spreadsheetID, sheetRange)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return sheetsAppendItems(ctx, rc, token, spreadsheetID, tab, sheetRange, []model.Item{it})
	}
	header := sheetHeaderFromRow(values[0])
	mi := -1
	for i, h := range header {
		if h == matchCol {
			mi = i
			break
		}
	}
	if mi < 0 {
		return fmt.Errorf("column %q not found in header row", matchCol)
	}
	rowNum := -1
	for r := len(values) - 1; r >= 1; r-- {
		if mi < len(values[r]) && sheetCellString(values[r][mi]) == want {
			rowNum = r + 1 // 1-based sheet row
			break
		}
	}
	if rowNum < 0 {
		return sheetsAppendItems(ctx, rc, token, spreadsheetID, tab, sheetRange, []model.Item{it})
	}
	header, err = sheetsEnsureHeader(ctx, rc, token, spreadsheetID, tab, header, []model.Item{it})
	if err != nil {
		return err
	}
	idx := map[string]int{}
	for i, h := range header {
		if h != "" {
			idx[h] = i
		}
	}
	keys := make([]string, 0, len(it.JSON))
	for k := range it.JSON {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var data []map[string]any
	for _, k := range keys {
		if k == matchCol {
			continue
		}
		i, ok := idx[k]
		if !ok {
			continue
		}
		data = append(data, map[string]any{
			"range":  a1(tab, colName(i)+strconv.Itoa(rowNum)),
			"values": [][]any{{sheetCellValue(it.JSON[k])}},
		})
	}
	if len(data) == 0 {
		return nil
	}
	body, _ := json.Marshal(map[string]any{"valueInputOption": "RAW", "data": data})
	u := fmt.Sprintf("https://sheets.googleapis.com/v4/spreadsheets/%s/values:batchUpdate", spreadsheetID)
	return googleAPICall(ctx, "POST", u, token, body, nil, rc.CurrentOperationID)
}

func buildRFC2822(to, subject, body string) string {
	msg := fmt.Sprintf("To: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s", to, subject, body)
	return base64URLEncode([]byte(msg))
}
