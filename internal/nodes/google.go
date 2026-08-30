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

	"microflow/internal/engine"
	"microflow/internal/expr"
	"microflow/internal/model"
	"microflow/internal/vault"
)

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
	spreadsheetID := node.ParamString("documentId", "")
	sheetRange := node.ParamString("range", "A1:Z1000")
	operation, _ := node.Parameters["operation"].(string) // "read" | "append" | "update"

	var out []model.Item
	switch operation {
	case "append":
		for _, it := range flatten(input) {
			row := jsonToRow(it.JSON)
			body, _ := json.Marshal(map[string]any{"values": [][]any{row}})
			url := fmt.Sprintf("https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s:append?valueInputOption=USER_ENTERED", spreadsheetID, sheetRange)
			if err := googleAPICall(ctx, "POST", url, token, body, nil); err != nil {
				return nil, fmt.Errorf("googleSheets %q append: %w", node.Name, err)
			}
			out = append(out, it)
		}
	case "update":
		for _, it := range flatten(input) {
			row := jsonToRow(it.JSON)
			body, _ := json.Marshal(map[string]any{"values": [][]any{row}})
			url := fmt.Sprintf("https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s?valueInputOption=USER_ENTERED", spreadsheetID, sheetRange)
			if err := googleAPICall(ctx, "PUT", url, token, body, nil); err != nil {
				return nil, fmt.Errorf("googleSheets %q update: %w", node.Name, err)
			}
			out = append(out, it)
		}
	default: // read
		var resp struct {
			Values [][]any `json:"values"`
		}
		url := fmt.Sprintf("https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s", spreadsheetID, sheetRange)
		if err := googleAPICall(ctx, "GET", url, token, nil, &resp); err != nil {
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
			if err := uploadBinaryFile(ctx, url, token, ref.FileName, ref.MimeType); err != nil {
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
		videoID, err := uploadVideoMultipart(ctx, token, ref.FileName, ref.MimeType, snippet)
		if err != nil {
			return nil, fmt.Errorf("youTube %q upload: %w", node.Name, err)
		}
		out = append(out, model.Item{JSON: map[string]any{"videoId": videoID}})
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
		if err := googleAPICall(ctx, "POST", url, token, body, nil); err != nil {
			return nil, fmt.Errorf("gmail %q: %w", node.Name, err)
		}
		out = append(out, model.Item{JSON: map[string]any{"sent": true, "to": to}})
	}
	return model.NodeOutput{out}, nil
}

// --- shared helpers ---

func googleAPICall(ctx context.Context, method, url, token string, body []byte, into any) error {
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
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("google api %s %s: status %d: %s", method, url, resp.StatusCode, truncate(string(respBody), 500))
	}
	if into != nil {
		return json.Unmarshal(respBody, into)
	}
	return nil
}

func uploadBinaryFile(ctx context.Context, url, token, filePath, mimeType string) error {
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

func uploadVideoMultipart(ctx context.Context, token, filePath, mimeType string, metadata map[string]any) (string, error) {
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
