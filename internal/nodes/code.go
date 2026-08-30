package nodes

import (
	"context"
	"fmt"
	"os"
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
// $execution, items/item, plus JS builtins like JSON/Date/Array/String
// which goja provides natively). We do not expose `require`, so
// npm-style module loading is impossible from inside a Code node. A
// wall-clock timeout aborts runaway scripts (rule 22: infinite loops).
type CodeExecutor struct {
	// EnvAllowlist holds the env var *names* an operator has opted in to
	// exposing to Code node scripts via $env (populated from server
	// config, see cmd/server/main.go and Deps.EnvAllowlist). Nil/empty
	// means no env vars are exposed -- same fail-closed default as
	// ExecuteCommandExecutor.AllowedBinaries.
	EnvAllowlist []string
}

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

			done := make(chan struct{})
			var resultVal goja.Value
			var runErr error
			go func() {
				defer close(done)
				v, err := vm.RunString(wrapCode(src))
				resultVal, runErr = v, err
			}()

			select {
			case <-done:
			case <-time.After(10 * time.Second):
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
// `return [...]` or `return {...}`) into a callable IIFE.
func wrapCode(body string) string {
	return "(function(){\n" + body + "\n})()"
}

func mustSet(vm *goja.Runtime, name string, v any) {
	if err := vm.Set(name, v); err != nil {
		panic(err) // programmer error (bad binding), not user input
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
