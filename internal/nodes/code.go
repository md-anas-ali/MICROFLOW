package nodes

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dop251/goja"

	"microflow/internal/engine"
	"microflow/internal/model"
)

// CodeExecutor runs an n8n Code node's JavaScript body per item (or once
// over all items, matching the node's "Run Once for All Items" /
// "Run Once for Each Item" mode param) using goja, a pure-Go JS engine.
//
// Security posture (rule 5 + rule 22): goja has NO access to the host
// filesystem, network, or process by default -- we only expose the data
// bindings the workflow actually needs ($json, $node, $env allowlist,
// $execution, items/item, console.log/warn/error/info/debug (forwarded
// to the server log, length-capped), plus JS builtins like
// JSON/Date/Array/String which goja provides natively). We do not
// expose `require`, so npm-style module loading is impossible from
// inside a Code node. A wall-clock timeout aborts runaway scripts
// (rule 22: infinite loops).
type CodeExecutor struct {
	// EnvAllowlist holds the env var *names* an operator has opted in to
	// exposing to Code node scripts via $env (populated from server
	// config, see cmd/server/main.go and Deps.EnvAllowlist). Nil/empty
	// means no env vars are exposed -- same fail-closed default as
	// ExecuteCommandExecutor.AllowedBinaries.
	EnvAllowlist []string

	// Client backs this.helpers.httpRequest (see newHTTPRequestHelper). It
	// is the same *http.Client the httpRequest node uses (Deps.HTTPClient,
	// whose transport dials through SafeDialContext), so scripts get the
	// identical SSRF boundary and client-level timeout. Nil falls back to
	// the same 30s default client HTTPRequestExecutor uses.
	Client *http.Client
}

// codeNodeTimeout is the wall-clock limit for one Code-node script run. It
// also caps every this.helpers.httpRequest call the script makes, so no
// outbound request can outlive the script that issued it.
const codeNodeTimeout = 10 * time.Second

func (e CodeExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	src := node.ParamString("jsCode", node.ParamString("code", ""))
	mode := node.ParamString("mode", "runOnceForAllItems")
	items := flatten(input)

	// Rule 13: if the script calls $getWorkflowStaticData(...), the whole
	// node execution (which may run the script once per item under
	// runOnceForEachItem) happens under a single Postgres row lock on
	// this workflow's static data, so counters/dedup-state/retry-queues
	// the script reads and mutates can't race against a concurrent
	// execution of the same workflow (this is exactly the mechanism the
	// workflow's "Fallback 1/2 Guard" and dedup-check Code nodes need).
	usesStaticData := strings.Contains(src, "getWorkflowStaticData")

	runAll := func(staticData map[string]any) ([]model.Item, error) {
		run := func(itemsIn []model.Item) ([]model.Item, error) {
			// Back off before allocating a new JS VM if we're already
			// over the RAM ceiling (rule 19) -- each goja.New() has a
			// real allocation cost, and a runOnceForEachItem Code node
			// can otherwise spin up one VM per item back-to-back with no
			// pressure relief in between.
			rc.MemGuard.WaitIfThrottled(ctx)

			vm := goja.New()
			vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
			// Bound recursion depth (rule 22: nothing unbounded) -- goja
			// has no direct heap-byte limit API, so call-stack depth is
			// the practical lever against a runaway/malicious recursive
			// script blowing up memory via stack frames.
			vm.SetMaxCallStackSize(256)

			inputItems := make([]map[string]any, len(itemsIn))
			for i, it := range itemsIn {
				inputItems[i] = map[string]any{"json": it.JSON}
			}
			mustSet(vm, "$input", map[string]any{"all": func() []map[string]any { return inputItems }})
			if len(itemsIn) > 0 {
				mustSet(vm, "$json", itemsIn[0].JSON)
			} else {
				mustSet(vm, "$json", map[string]any{})
			}
			// n8n's Code node also exposes bare (non-$-prefixed) globals as
			// a convenience, and workflows imported from n8n commonly use
			// them directly instead of $input.all(): `items` (the full
			// input array) in "Run Once for All Items" mode, `item` (the
			// single current item) in "Run Once for Each Item" mode.
			// Exposing both here regardless of mode is harmless and
			// maximizes compatibility with imported scripts either way.
			mustSet(vm, "items", inputItems)
			if len(inputItems) > 0 {
				mustSet(vm, "item", inputItems[0])
			} else {
				mustSet(vm, "item", map[string]any{"json": map[string]any{}})
			}
			// n8n Code node scripts commonly use console.log/warn/error/info
			// for debugging; goja has no built-in console. Forward to the
			// server log (prefixed with the node name so multiple nodes'
			// output isn't ambiguous), capped in length per call so a
			// runaway/malicious script can't flood the log (rule 22: no
			// unbounded resource use). This never returns anything to the
			// script -- console methods are fire-and-forget, matching
			// real console semantics.
			mustSet(vm, "console", map[string]any{
				"log":   consoleLogger(rc.Redactor, node.Name, "LOG"),
				"info":  consoleLogger(rc.Redactor, node.Name, "INFO"),
				"warn":  consoleLogger(rc.Redactor, node.Name, "WARN"),
				"error": consoleLogger(rc.Redactor, node.Name, "ERROR"),
				"debug": consoleLogger(rc.Redactor, node.Name, "DEBUG"),
			})

			nodeGetter := func(name string) map[string]any {
				if out, ok := rc2ExprNodeOutputs(rc)[name]; ok {
					return map[string]any{"json": out}
				}
				return map[string]any{"json": map[string]any{}}
			}
			mustSet(vm, "$node", nodeGetterProxy(nodeGetter))

			mustSet(vm, "$env", e.envAllowlist())
			mustSet(vm, "$execution", map[string]any{"id": rc.Execution.ID, "mode": rc.Execution.Mode})
			mustSet(vm, "$workflow", map[string]any{"id": rc.Workflow.ID, "name": rc.Workflow.Name})

			// $getWorkflowStaticData('global') -- goja wraps the returned
			// Go map so property reads/writes from JS act directly on
			// staticData; whatever the script mutates is visible to our
			// caller once the script finishes, no export step needed.
			mustSet(vm, "$getWorkflowStaticData", func(scope string) map[string]any {
				if staticData == nil {
					return map[string]any{}
				}
				sub, ok := staticData[scope].(map[string]any)
				if !ok {
					sub = map[string]any{}
					staticData[scope] = sub
				}
				return sub
			})

			// $readFileBase64(path) -- a narrow, allowlisted substitute for
			// the Node.js `fs`/`require` module (which we deliberately do
			// NOT expose; see the security note atop this file). Several
			// imported n8n Code node scripts (e.g. this workflow's "Build
			// QC Vision Request", which base64-encodes locally-extracted
			// video frames for a vision-model API call) use
			// `require('fs').readFileSync(path).toString('base64')` --
			// that pattern is common enough in real n8n exports that
			// giving it a safe equivalent is worth it, while still never
			// handing a sandboxed script the general filesystem or
			// module loader. Same path allowlist as ReadWriteFileExecutor
			// (scratch dir + OS temp dir, no traversal escapes) and a
			// hard size cap so a script can't pull a multi-hundred-MB
			// file into the JS heap and blow the 512MB RAM budget (rule
			// 19). Returns null (not a thrown error) on any failure --
			// missing file, path rejected, too large -- matching the
			// try/catch-wrapped call sites this replaces, which already
			// treat a null/failed read as "skip this frame".
			mustSet(vm, "$readFileBase64", func(path string) any {
				return readFileBase64ForCode(rc, path)
			})
			// $readFileText(path) -- same allowlist/size-cap contract as
			// $readFileBase64, for scripts reading a small local text
			// file (e.g. the generated .srt subtitle file) directly as a
			// UTF-8 string. Kept separate rather than having scripts
			// base64-decode text themselves, since goja has no Node
			// `Buffer` global to do that decoding with.
			mustSet(vm, "$readFileText", func(path string) any {
				return readFileTextForCode(rc, path)
			})

			done := make(chan struct{})
			var resultVal goja.Value
			var runErr error
			// n8n exposes `this.helpers.httpRequest(...)` to Code nodes;
			// scripts (e.g. Model Controller's provider discovery) rely on
			// it. Requests are bounded by helperCtx so none outlives the
			// script's own wall-clock limit below.
			helperCtx, cancelHelpers := context.WithTimeout(ctx, codeNodeTimeout)
			defer cancelHelpers()
			thisVal := vm.ToValue(map[string]any{
				"helpers": map[string]any{
					"httpRequest": e.newHTTPRequestHelper(helperCtx, rc, vm),
				},
			})

			go func() {
				defer close(done)
				v, err := runCode(vm, src, thisVal)
				resultVal, runErr = v, err
			}()

			select {
			case <-done:
			case <-time.After(codeNodeTimeout):
				vm.Interrupt("code node timeout (10s)")
				<-done
				return nil, fmt.Errorf("code node %q: execution timed out", node.Name)
			case <-ctx.Done():
				vm.Interrupt("cancelled")
				<-done
				return nil, ctx.Err()
			}
			if runErr != nil {
				return nil, fmt.Errorf("code node %q: %w", node.Name, runErr)
			}

			exported := resultVal.Export()
			return exportToItems(exported)
		}

		if mode == "runOnceForEachItem" {
			var out []model.Item
			for _, it := range items {
				res, err := run([]model.Item{it})
				if err != nil {
					return nil, err
				}
				out = append(out, res...)
			}
			return out, nil
		}
		return run(items)
	}

	if !usesStaticData {
		out, err := runAll(nil)
		if err != nil {
			return nil, err
		}
		return model.NodeOutput{out}, nil
	}

	var out []model.Item
	err := rc.StaticData.WithLock(ctx, rc.Workflow.ID, func(data map[string]any) (map[string]any, error) {
		if data == nil {
			data = map[string]any{}
		}
		res, err := runAll(data)
		if err != nil {
			return data, err
		}
		out = res
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	return model.NodeOutput{out}, nil
}

// wrapCode makes the user's n8n-style code body (which ends with
// `return [...]` or `return {...}`) into a function expression; runCode
// calls it with `this` bound to the n8n-style context (this.helpers).
func wrapCode(body string) string {
	return "(function(){\n" + body + "\n})"
}

// wrapAsyncCode is wrapCode for bodies that use top-level `await`, which n8n
// Code nodes allow but a plain function body rejects as a syntax error.
func wrapAsyncCode(body string) string {
	return "(async function(){\n" + body + "\n})"
}

// runCode compiles and runs a Code-node body with the given `this`. Bodies
// that compile as a plain function behave exactly as before. Only a body
// that fails to compile that way is retried as an async function (top-level
// await); goja drains its promise job queue before the call returns, so the
// returned promise is already settled and is unwrapped here.
func runCode(vm *goja.Runtime, src string, thisVal goja.Value) (goja.Value, error) {
	async := false
	prog, err := goja.Compile("", wrapCode(src), false)
	if err != nil {
		asyncProg, asyncErr := goja.Compile("", wrapAsyncCode(src), false)
		if asyncErr != nil {
			return nil, err // report the original syntax error
		}
		prog, async = asyncProg, true
	}
	fnVal, err := vm.RunProgram(prog)
	if err != nil {
		return nil, err
	}
	fn, ok := goja.AssertFunction(fnVal)
	if !ok {
		return nil, errors.New("code node: wrapped script is not callable")
	}
	res, err := fn(thisVal)
	if err != nil || !async {
		return res, err
	}
	p, ok := res.Export().(*goja.Promise)
	if !ok {
		return res, nil
	}
	switch p.State() {
	case goja.PromiseStateFulfilled:
		return p.Result(), nil
	case goja.PromiseStateRejected:
		return nil, fmt.Errorf("code node: unhandled rejection: %s", p.Result().String())
	default:
		return nil, errors.New("code node: async script did not settle (awaiting a promise that never resolves)")
	}
}

// newHTTPRequestHelper implements this.helpers.httpRequest(options) for Code
// nodes, mirroring the subset of n8n's helper that scripts here use:
// { method, url, headers, body, timeout (ms), json }. It reuses the httpRequest
// node's building blocks -- guardSSRF, the shared *http.Client (SafeDialContext),
// the default User-Agent, the response size cap, MemGuard throttling and the
// HeavyWorkGate -- rather than introducing a second HTTP stack.
//
// Like n8n's helper it returns the parsed body itself (an object OR a bare
// array when json is true, else the body string) and throws on transport
// errors and non-2xx statuses. Thrown messages never include the URL,
// headers or response body, so a credential cannot leak through an error.
// The call is synchronous; awaiting its result is fine.
func (e CodeExecutor) newHTTPRequestHelper(ctx context.Context, rc *engine.RunContext, vm *goja.Runtime) func(goja.FunctionCall) goja.Value {
	fail := func(format string, args ...any) {
		panic(vm.NewGoError(fmt.Errorf("httpRequest: "+format, args...)))
	}
	return func(call goja.FunctionCall) goja.Value {
		opts, _ := call.Argument(0).Export().(map[string]any)
		if opts == nil {
			fail("options object required")
		}
		rawURL, _ := opts["url"].(string)
		if rawURL == "" {
			fail("url required")
		}
		if err := guardSSRF(rawURL); err != nil {
			fail("%v", err)
		}
		method, _ := opts["method"].(string)
		if method == "" {
			method = "GET"
		}
		method = strings.ToUpper(method)

		// goja exports JS integers as int64 (toFloat doesn't cover that), so
		// read the numeric timeout explicitly. It can only shorten the limit.
		timeout := codeNodeTimeout
		var ms float64
		switch n := opts["timeout"].(type) {
		case int64:
			ms = float64(n)
		case float64:
			ms = n
		}
		if d := time.Duration(ms * float64(time.Millisecond)); d > 0 && d < timeout {
			timeout = d
		}
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		var body io.Reader
		switch b := opts["body"].(type) {
		case nil:
		case string:
			body = strings.NewReader(b)
		default:
			enc, err := json.Marshal(b)
			if err != nil {
				fail("body is not JSON-serializable")
			}
			body = strings.NewReader(string(enc))
		}
		req, err := http.NewRequestWithContext(reqCtx, method, rawURL, body)
		if err != nil {
			fail("invalid request")
		}
		req.Header.Set("User-Agent", defaultUserAgent())
		if headers, ok := opts["headers"].(map[string]any); ok {
			for k, v := range headers {
				req.Header.Set(k, fmt.Sprintf("%v", v))
			}
		}
		wantJSON, _ := opts["json"].(bool)
		if wantJSON && req.Header.Get("Accept") == "" {
			req.Header.Set("Accept", "application/json")
		}
		if _, isObj := opts["body"].(map[string]any); isObj && req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/json")
		}

		client := e.Client
		if client == nil {
			client = &http.Client{Timeout: 30 * time.Second}
		}
		rc.MemGuard.WaitIfThrottled(reqCtx)
		if rc.HeavyWorkGate != nil {
			select {
			case rc.HeavyWorkGate <- struct{}{}:
			case <-reqCtx.Done():
				fail("timed out waiting for a free request slot")
			}
		}
		resp, err := client.Do(req)
		if rc.HeavyWorkGate != nil {
			<-rc.HeavyWorkGate
		}
		if err != nil {
			var ue *url.Error
			if errors.As(err, &ue) {
				err = ue.Err // drop the URL from the message
			}
			fail("request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fail("HTTP %d", resp.StatusCode)
		}
		respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
		if err != nil {
			fail("reading response failed")
		}
		if len(respBody) > maxResponseBytes {
			fail("response exceeded %d byte cap", maxResponseBytes)
		}
		if !wantJSON {
			return vm.ToValue(string(respBody))
		}
		// Parse with the VM's own JSON so the script gets native objects and
		// arrays (Array.isArray, filter/map, ...), not wrapped Go values.
		parse, ok := goja.AssertFunction(vm.Get("JSON").ToObject(vm).Get("parse"))
		if !ok {
			fail("JSON.parse unavailable")
		}
		parsed, err := parse(goja.Undefined(), vm.ToValue(string(respBody)))
		if err != nil {
			return vm.ToValue(string(respBody)) // non-JSON body: hand back the text, as n8n does
		}
		return parsed
	}
}

func mustSet(vm *goja.Runtime, name string, v any) {
	if err := vm.Set(name, v); err != nil {
		panic(err) // programmer error (bad binding), not user input
	}
}

// maxReadFileBase64Bytes caps what $readFileBase64 will load into RAM/JS
// heap in one call. Generous for a single QC preview JPEG frame,
// nowhere near enough to swallow a full rendered video (rule 19: stay
// well inside the 512MB budget).
const maxReadFileBase64Bytes = 5 * 1024 * 1024 // 5MB

// readFileBase64ForCode implements $readFileBase64 for Code node scripts.
// Enforces the same path allowlist as ReadWriteFileExecutor (scratch dir
// + OS temp dir, no traversal escapes) since this is the one deliberate,
// narrow filesystem door into an otherwise sandboxed JS VM -- everything
// reachable through it must stay inside directories the run already
// owns.
func readFileBase64ForCode(rc *engine.RunContext, path string) any {
	if path == "" {
		return nil
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(rc.ScratchDir, path)
	}
	allowedRoots := []string{rc.ScratchDir, os.TempDir()}
	if err := guardPathTraversal(allowedRoots, path); err != nil {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return nil
	}
	if info.Size() > maxReadFileBase64Bytes {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return base64.StdEncoding.EncodeToString(b)
}

// readFileTextForCode implements $readFileText for Code node scripts --
// same allowlist/size-cap contract as readFileBase64ForCode, returning
// the file's content decoded as a UTF-8 string instead of base64.
func readFileTextForCode(rc *engine.RunContext, path string) any {
	if path == "" {
		return nil
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(rc.ScratchDir, path)
	}
	allowedRoots := []string{rc.ScratchDir, os.TempDir()}
	if err := guardPathTraversal(allowedRoots, path); err != nil {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return nil
	}
	if info.Size() > maxReadFileBase64Bytes {
		return nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return string(b)
}

// maxConsoleLogBytes bounds a single console.log/warn/etc call's
// formatted output so a runaway or malicious script can't flood the
// server log (rule 22: no unbounded resource use).
const maxConsoleLogBytes = 2000

// consoleLogger returns a variadic function suitable for vm.Set as
// console.log/info/warn/error/debug: formats args the way JS console
// methods do (space-joined) and writes one line to the server log,
// prefixed with the node name and level so concurrent nodes' output
// isn't ambiguous. Never returns anything to the script. redactor (if
// set) scrubs any secret-shaped value the script logs -- e.g. a Config
// Center-style node's `console.log(cfg.geminiApiKey)` during debugging
// -- before it ever reaches the server's own log output, matching the
// same redaction applied to stored/served execution data.
func consoleLogger(redactor *engine.SecretRedactor, nodeName, level string) func(args ...any) {
	return func(args ...any) {
		msg := fmt.Sprintln(args...)
		msg = strings.TrimRight(msg, "\n")
		if len(msg) > maxConsoleLogBytes {
			msg = msg[:maxConsoleLogBytes] + "...(truncated)"
		}
		msg = redactor.RedactString(msg)
		log.Printf("code node %q [%s]: %s", nodeName, level, msg)
	}
}

// envAllowlist exposes only explicitly-approved env vars to Code nodes,
// never the whole process environment (credential leakage prevention,
// rule 11/22). The set of *names* an operator opted in to comes from
// e.EnvAllowlist (wired at server startup, see cmd/server/main.go); this
// reads the actual values from the process environment fresh on every
// call so a value change (e.g. a rotated API key) takes effect without a
// restart. A name in the allowlist with no matching process env var is
// simply omitted, not exposed as an empty string, so scripts can use
// `if ($env.X)` to detect it's unset.
func (e CodeExecutor) envAllowlist() map[string]string {
	out := make(map[string]string, len(e.EnvAllowlist))
	for _, name := range e.EnvAllowlist {
		if v, ok := os.LookupEnv(name); ok {
			out[name] = v
		}
	}
	return out
}

func nodeGetterProxy(get func(string) map[string]any) func(string) map[string]any {
	return get
}

func rc2ExprNodeOutputs(rc *engine.RunContext) map[string]map[string]any {
	// RunContext keeps this unexported; engine exposes it via ExprContext.
	// For $node[...] inside Code nodes we reuse the same recorded outputs.
	ctx := rc.ExprContext(nil)
	return ctx.NodeOutputs
}

// exportToItems normalizes a goja-exported return value (array of
// {json:...} objects, a single object, or a bare array of objects) into
// MicroFlow items, matching the shapes n8n Code nodes commonly return.
//
// goja's Export() doesn't always produce []any for a JS array: when
// every element happens to export to the same concrete Go map type, it
// can produce a more specific slice type instead (observed:
// []map[string]interface{} for an array of plain JS objects). Both
// shapes are handled explicitly rather than relying on a single
// []any case, so a Code node returning a plain array of objects works
// regardless of which slice type goja happened to pick.
func exportToItems(v any) ([]model.Item, error) {
	switch val := v.(type) {
	case []any:
		var out []model.Item
		for _, e := range val {
			m, ok := e.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("code node: array element is not an object")
			}
			out = append(out, itemFromMap(m))
		}
		return out, nil
	case []map[string]any:
		var out []model.Item
		for _, m := range val {
			out = append(out, itemFromMap(m))
		}
		return out, nil
	case map[string]any:
		return []model.Item{itemFromMap(val)}, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("code node: unsupported return type %T", v)
	}
}

// itemFromMap unwraps n8n's {json: {...}} item shape if present,
// otherwise treats the whole map as the item's json.
func itemFromMap(m map[string]any) model.Item {
	if j, ok := m["json"].(map[string]any); ok {
		return model.Item{JSON: j}
	}
	return model.Item{JSON: m}
}
