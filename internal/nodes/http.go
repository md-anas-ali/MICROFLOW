package nodes

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"microflow/internal/engine"
	"microflow/internal/expr"
	"microflow/internal/model"
)

// HTTPRequestExecutor covers the workflow's many httpRequest nodes:
// image generation calls, the "Unified AI Request" family, LanguageTool
// grammar check, and outbound notification webhooks.
type HTTPRequestExecutor struct {
	Client *http.Client
}

const maxResponseBytes = 25 * 1024 * 1024 // 25MB cap so one response can't blow the 512MB budget

// defaultUserAgent is sent on every outbound httpRequest call that
// doesn't set its own User-Agent header. Several third-party APIs (most
// notably the Wikimedia/Wikipedia REST and Action APIs) require a
// descriptive User-Agent identifying the application and a contact
// point per their robot/API etiquette policy, and will otherwise
// respond with 403s -- a generic Go http.Client default ("Go-http-
// client/1.1") does not satisfy that. Operators can override this via
// MICROFLOW_USER_AGENT (e.g. to include their own contact email/URL, as
// Wikimedia's policy specifically asks for) without a code change.
func defaultUserAgent() string {
	if v := os.Getenv("MICROFLOW_USER_AGENT"); v != "" {
		return v
	}
	return "MicroFlow-Workflow-Automation/1.0 (+https://github.com/microflow/microflow; contact: set MICROFLOW_USER_AGENT to your own contact info)"
}

func (e *HTTPRequestExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	client := e.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	var out []model.Item
	for _, it := range flatten(input) {
		exprCtx := rc.ExprContext(it.JSON)

		rawURL := node.ParamString("url", "")
		resolvedURL, err := expr.Eval(rawURL, exprCtx)
		if err != nil {
			return nil, fmt.Errorf("http request %q: %w", node.Name, err)
		}
		if err := guardSSRF(resolvedURL); err != nil {
			return nil, fmt.Errorf("http request %q: %w", node.Name, err)
		}

		method, _ := node.Parameters["method"].(string)
		if method == "" {
			method = "GET"
		}

		var body io.Reader
		if bodyTmpl, ok := node.Parameters["body"].(string); ok && bodyTmpl != "" {
			resolvedBody, err := expr.Eval(bodyTmpl, exprCtx)
			if err != nil {
				return nil, err
			}
			body = strings.NewReader(resolvedBody)
		}

		// rate-limit heavy outbound calls to MaxConcurrentHeavy (rule 19/20),
		// backing off first if we're already over the RAM ceiling (rule 19).
		rc.MemGuard.WaitIfThrottled(ctx)
		rc.HeavyWorkGate <- struct{}{}
		req, err := http.NewRequestWithContext(ctx, method, resolvedURL, body)
		if err != nil {
			<-rc.HeavyWorkGate
			return nil, err
		}
		req.Header.Set("User-Agent", defaultUserAgent())
		if headers, ok := node.Parameters["headers"].(map[string]any); ok {
			for k, v := range headers {
				req.Header.Set(k, fmt.Sprintf("%v", v))
			}
		}

		resp, err := client.Do(req)
		<-rc.HeavyWorkGate
		if err != nil {
			return nil, fmt.Errorf("http request %q: %w", node.Name, err)
		}

		// Opt-in error classification: a node only fails the run over its
		// HTTP status code if it has explicitly asked to (RetryOnFail
		// set), matching n8n's own per-node retry configuration model.
		// This is deliberately NOT applied unconditionally -- several
		// existing nodes in this workflow family (the multi-model AI
		// request loop) intentionally treat a non-2xx response as regular
		// data (the provider's JSON error body is inspected downstream by
		// a Validate/Router code node to decide whether to loop), and
		// forcing every non-2xx into a hard Go error here would break
		// that already-working retry-loop design. A node that DOES set
		// RetryOnFail is asking MicroFlow's own retry/backoff to handle
		// failures instead, so for that node (only) we turn a 429/5xx
		// into a real (retryable) error and a 401/403 into a real
		// (permanent, clearly-labeled configuration/credential) error --
		// otherwise RetryOnFail/MaxTries on an httpRequest node would be
		// silently inert (the exact "silent failure" class of bug this
		// pass is meant to eliminate).
		if node.RetryOnFail {
			switch {
			case resp.StatusCode == 401 || resp.StatusCode == 403:
				snippet := readSnippetAndClose(resp, 500)
				return nil, engine.Permanent(fmt.Errorf(
					"http request %q: configuration error (HTTP %d) -- check the credential/API key configured for this node: %s",
					node.Name, resp.StatusCode, snippet))
			case resp.StatusCode == 429 || resp.StatusCode >= 500:
				snippet := readSnippetAndClose(resp, 500)
				return nil, fmt.Errorf(
					"http request %q: transient error (HTTP %d), will retry if attempts remain: %s",
					node.Name, resp.StatusCode, snippet)
			}
		}

		result := map[string]any{
			"statusCode": resp.StatusCode,
			"headers":    flattenHeader(resp.Header),
		}
		ct := resp.Header.Get("Content-Type")

		if strings.HasPrefix(ct, "image/") || strings.HasPrefix(ct, "audio/") || strings.HasPrefix(ct, "video/") {
			// Binary responses (generated images/audio/video) are streamed
			// straight to a spooled file on disk as they're read off the
			// wire -- never buffered whole in a []byte first (rule 7/18:
			// don't hold large media fully in RAM; media is
			// streamed/processed via /tmp per the LOW-RAM spec). Previously
			// this path did io.ReadAll into memory *then* spooled to disk,
			// so a 20MB generated video briefly cost 20MB of heap for
			// nothing.
			ref, err := spoolBinaryStream(rc.ScratchDir, node.Name, resp.Body, ct, maxResponseBytes)
			resp.Body.Close()
			if err != nil {
				return nil, fmt.Errorf("http request %q: %w", node.Name, err)
			}
			out = append(out, model.Item{JSON: result, Binary: map[string]model.BinaryRef{"data": ref}})
			continue
		}

		limited := io.LimitReader(resp.Body, maxResponseBytes+1)
		respBody, readErr := io.ReadAll(limited)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if len(respBody) > maxResponseBytes {
			return nil, fmt.Errorf("http request %q: response exceeded %d byte cap", node.Name, maxResponseBytes)
		}

		if strings.Contains(ct, "application/json") {
			// best-effort JSON decode; fall back to raw body on failure
			if m, err := decodeJSONLoose(respBody); err == nil {
				result["json"] = m
			} else {
				result["body"] = string(respBody)
			}
		} else {
			result["body"] = string(respBody)
		}
		out = append(out, model.Item{JSON: result})
	}
	return model.NodeOutput{out}, nil
}

// readSnippetAndClose reads a bounded preview of a non-2xx response
// body (for a clear error message) and closes it. Never includes
// request headers (Authorization, API keys) -- only what the *server*
// sent back -- so a credential can't leak into an error message via
// this path; RunContext.Redactor scrubs the stored/served copy of the
// message as defense in depth regardless.
func readSnippetAndClose(resp *http.Response, max int) string {
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, int64(max)))
	return truncate(string(b), max)
}

// allowLoopbackForTests disables the loopback part of guardSSRF's check
// -- ONLY set (to true, via t.Cleanup to restore it) by this package's
// own white-box tests, which need to point HTTPRequestExecutor at a
// local httptest.Server. It is never touched by production code paths
// and defaults to false, so the SSRF guard is fully enforced any time
// this package is imported/run outside `go test ./internal/nodes/...`.
var allowLoopbackForTests = false

// guardSSRF rejects requests to loopback/link-local/private ranges
// unless the target host was explicitly allowlisted -- prevents a
// workflow (or an injected expression) from pivoting the server into
// hitting internal infrastructure. AI/image APIs are public hosts and
// are unaffected.
func guardSSRF(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme %q not allowed", u.Scheme)
	}
	if allowLoopbackForTests {
		return nil
	}
	host := u.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
			return fmt.Errorf("requests to private/loopback addresses are blocked")
		}
	}
	if host == "localhost" {
		return fmt.Errorf("requests to localhost are blocked")
	}
	return nil
}

func flattenHeader(h http.Header) map[string]any {
	m := map[string]any{}
	for k, v := range h {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}
	return m
}

func decodeJSONLoose(b []byte) (any, error) {
	var v any
	err := jsonUnmarshal(b, &v)
	return v, err
}

// spoolBinary / spoolBinaryStream are implemented in binary.go
