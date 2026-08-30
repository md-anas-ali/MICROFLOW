package nodes

import (
	"context"
	"net/http"
	"time"

	"microflow/internal/engine"
	"microflow/internal/expr"
	"microflow/internal/model"
)

// Deps bundles what executors need at construction time.
type Deps struct {
	HTTPClient         *http.Client
	AllowedBinaries    map[string]string // logical name -> absolute path, allowlist for ExecuteCommand (security rule 22)
	EnvAllowlist       []string          // env var names exposed to Code node's $env (security rule 11/22) -- see CodeExecutor.envAllowlist
	ScratchRoot        string
	CredentialResolver engine.CredentialResolver
	// GoogleAccounts is the per-service (gmail/youtube/sheets) connected
	// Google account fallback for googleSheets/youTube/gmail nodes -- see
	// internal/nodes/google.go's GoogleAccountResolver and
	// internal/vault.GoogleServiceAccounts. Nil-safe: nil means Google
	// OAuth isn't configured on this server, so those nodes only work via
	// an explicit per-node CredentialResolver override.
	GoogleAccounts GoogleAccountResolver
}

// PassThroughExecutor is used for trigger nodes (the scheduler/webhook/
// manual-run caller already supplies the seed input) and NoOp (used in
// the workflow purely as a retry-loop-back junction).
type PassThroughExecutor struct{}

func (PassThroughExecutor) Execute(_ context.Context, _ *engine.RunContext, _ *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	if len(input) == 0 {
		return model.NodeOutput{{{JSON: map[string]any{}}}}, nil
	}
	return input, nil
}

// IfExecutor evaluates the node's condition(s) against each item and
// routes to output 0 (true) or output 1 (false), matching n8n's IF node.
// Conditions are read from Parameters["conditions"], a slice of
// {left, operator, right} maps -- the shape n8n's IF v2 node writes.
type IfExecutor struct{}

func (IfExecutor) Execute(_ context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	trueOut := []model.Item{}
	falseOut := []model.Item{}

	items := flatten(input)
	for _, it := range items {
		ok, err := evalConditions(node, it, rc)
		if err != nil {
			return nil, err
		}
		if ok {
			trueOut = append(trueOut, it)
		} else {
			falseOut = append(falseOut, it)
		}
	}
	return model.NodeOutput{trueOut, falseOut}, nil
}

func evalConditions(node *model.Node, it model.Item, rc *engine.RunContext) (bool, error) {
	raw, _ := node.Parameters["conditions"].([]any)
	combinator, _ := node.Parameters["combinator"].(string) // "and" | "or"
	if combinator == "" {
		combinator = "and"
	}
	if len(raw) == 0 {
		return true, nil
	}
	ctx := rc.ExprContext(it.JSON)
	result := combinator == "and"
	for _, rc0 := range raw {
		cond, _ := rc0.(map[string]any)
		leftTmpl, _ := cond["left"].(string)
		rightTmpl, _ := cond["right"].(string)
		op, _ := cond["operator"].(string)

		left, err := expr.EvalValue(leftTmpl, ctx)
		if err != nil {
			return false, err
		}
		right, err := expr.EvalValue(rightTmpl, ctx)
		if err != nil {
			return false, err
		}
		this := compare(left, op, right)
		if combinator == "and" {
			result = result && this
		} else {
			result = result || this
		}
	}
	return result, nil
}

func compare(left any, op string, right any) bool {
	ls, lok := left.(string)
	rs, rok := right.(string)
	switch op {
	case "equals", "==":
		return fmtEq(left, right)
	case "notEquals", "!=":
		return !fmtEq(left, right)
	case "contains":
		return lok && rok && contains(ls, rs)
	case "isEmpty":
		return left == nil || left == ""
	case "isNotEmpty":
		return left != nil && left != ""
	case "gt", ">":
		lf, lok2 := toFloat(left)
		rf, rok2 := toFloat(right)
		return lok2 && rok2 && lf > rf
	case "lt", "<":
		lf, lok2 := toFloat(left)
		rf, rok2 := toFloat(right)
		return lok2 && rok2 && lf < rf
	default:
		return fmtEq(left, right)
	}
}

// WaitExecutor sleeps for a fixed duration or until a resume time, as
// configured (n8n Wait node supports both). This DOES block the running
// goroutine, which is intentional and cheap: the engine caps concurrent
// heavy work separately via HeavyWorkGate, and a blocked goroutine
// waiting on time.Sleep uses effectively no RAM/CPU.
type WaitExecutor struct{}

func (WaitExecutor) Execute(ctx context.Context, _ *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	seconds := 1.0
	if v, ok := node.Parameters["amount"].(float64); ok {
		seconds = v
	}
	unit, _ := node.Parameters["unit"].(string)
	mult := map[string]float64{"seconds": 1, "minutes": 60, "hours": 3600, "": 1}[unit]
	if mult == 0 {
		mult = 1
	}
	select {
	case <-time.After(time.Duration(seconds*mult) * time.Second):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return input, nil
}

// SplitOutExecutor turns one item's array field into N items, e.g.
// splitting a scenes[] array into one item per scene.
type SplitOutExecutor struct{}

func (SplitOutExecutor) Execute(_ context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	field, _ := node.Parameters["fieldToSplitOut"].(string)
	var out []model.Item
	for _, it := range flatten(input) {
		ctx := rc.ExprContext(it.JSON)
		v, err := expr.EvalValue("{{ $json."+field+" }}", ctx)
		if err != nil {
			return nil, err
		}
		arr, ok := v.([]any)
		if !ok {
			continue
		}
		for _, elem := range arr {
			m, _ := elem.(map[string]any)
			if m == nil {
				m = map[string]any{"value": elem}
			}
			out = append(out, model.Item{JSON: m})
		}
	}
	return model.NodeOutput{out}, nil
}

// SplitInBatchesExecutor implements n8n's classic "Loop Over Items"
// pattern: output 0 is "done" (empty until all batches processed),
// output 1 is "loop" (the next batch). State (current index) is kept in
// workflow static data keyed by node name so the loop survives across
// separate engine.Run work-queue steps, matching how n8n's own
// splitInBatches persists position across executions of the node.
type SplitInBatchesExecutor struct{}

func (e *SplitInBatchesExecutor) Execute(ctx context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	batchSize := 1
	if v, ok := node.Parameters["batchSize"].(float64); ok && v > 0 {
		batchSize = int(v)
	}
	items := flatten(input)

	var doneOut, loopOut []model.Item
	stateKey := "splitInBatches:" + node.Name
	err := rc.StaticData.WithLock(ctx, rc.Workflow.ID, func(data map[string]any) (map[string]any, error) {
		idxF, _ := data[stateKey].(float64)
		idx := int(idxF)
		if idx >= len(items) {
			// finished: reset for next run, emit nothing further on loop
			delete(data, stateKey)
			return data, nil
		}
		end := idx + batchSize
		if end > len(items) {
			end = len(items)
		}
		loopOut = items[idx:end]
		if end >= len(items) {
			delete(data, stateKey)
			doneOut = items // n8n emits full accumulated set on "done"
		} else {
			data[stateKey] = float64(end)
		}
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	return model.NodeOutput{doneOut, loopOut}, nil
}

func flatten(out model.NodeOutput) []model.Item {
	var items []model.Item
	for _, branch := range out {
		items = append(items, branch...)
	}
	return items
}

func fmtEq(a, b any) bool {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok {
		return af == bf
	}
	return toStr(a) == toStr(b)
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	}
	return 0, false
}

func toStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
