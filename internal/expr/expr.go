// Package expr evaluates n8n-style {{ ... }} expressions against a
// node's runtime context. Only the context objects the sample workflow
// actually references are implemented: $json, $node["Name"].json/.binary,
// $env, $execution. Extending this to more of n8n's expression surface
// means adding a case in resolvePath, not touching the engine.
package expr

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Context is everything an expression may read.
type Context struct {
	JSON        map[string]any            // current item's $json
	NodeOutputs map[string]map[string]any // nodeName -> that node's first output item's $json, for $node["X"]
	Execution   map[string]any            // $execution.id, $execution.mode, etc.
}

var exprPattern = regexp.MustCompile(`\{\{(.*?)\}\}`)

// Eval replaces every {{ ... }} in tmpl with its evaluated value. If the
// entire string is a single expression, the native type (not just string)
// is preserved by EvalValue -- callers that need a parameter's real type
// (e.g. a number or boolean) should call EvalValue directly instead.
func Eval(tmpl string, ctx Context) (string, error) {
	var firstErr error
	out := exprPattern.ReplaceAllStringFunc(tmpl, func(m string) string {
		inner := strings.TrimSpace(m[2 : len(m)-2])
		v, err := evalExpression(inner, ctx)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			return ""
		}
		return fmt.Sprintf("%v", v)
	})
	return out, firstErr
}

// EvalValue evaluates a string that may be a single {{ expr }} (returning
// its native Go value: string/number/bool/map/slice) or, if it contains
// surrounding text / multiple expressions, falls back to string
// interpolation via Eval.
func EvalValue(tmpl string, ctx Context) (any, error) {
	trimmed := strings.TrimSpace(tmpl)
	if strings.HasPrefix(trimmed, "{{") && strings.HasSuffix(trimmed, "}}") &&
		strings.Count(trimmed, "{{") == 1 {
		inner := strings.TrimSpace(trimmed[2 : len(trimmed)-2])
		return evalExpression(inner, ctx)
	}
	return Eval(tmpl, ctx)
}

// evalExpression handles ONE expression body, e.g.:
//
//	$json.name
//	$json["field"]
//	$env.API_KEY
//	$node["HTTP Request"].json.id
//	$execution.id
//
// This is a small hand-rolled path evaluator, not a general JS
// interpreter -- the Code node (internal/nodes/code.go) is what runs
// actual JavaScript, via goja. This only covers n8n's expression
// mini-language.
func evalExpression(src string, ctx Context) (any, error) {
	src = strings.TrimSpace(src)

	switch {
	case strings.HasPrefix(src, "$json"):
		return resolvePath(ctx.JSON, src[len("$json"):])

	case strings.HasPrefix(src, "$env."):
		key := strings.TrimPrefix(src, "$env.")
		return os.Getenv(key), nil

	case strings.HasPrefix(src, "$execution"):
		return resolvePath(ctx.Execution, src[len("$execution"):])

	case strings.HasPrefix(src, `$node[`):
		// $node["Node Name"].json.field  or  $node['Node Name'].json["field"]
		rest := src[len("$node"):]
		name, remainder, err := readBracketString(rest)
		if err != nil {
			return nil, err
		}
		out, ok := ctx.NodeOutputs[name]
		if !ok {
			return nil, fmt.Errorf("expr: $node[%q] has no recorded output", name)
		}
		remainder = strings.TrimPrefix(remainder, ".json")
		return resolvePath(out, remainder)

	default:
		return nil, fmt.Errorf("expr: unsupported expression: %s", src)
	}
}

// readBracketString parses a leading ["literal"] or ['literal'] and
// returns the literal plus whatever follows.
func readBracketString(s string) (string, string, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "[") {
		return "", "", fmt.Errorf("expr: expected '[' in %q", s)
	}
	end := strings.Index(s, "]")
	if end < 0 {
		return "", "", fmt.Errorf("expr: unterminated bracket in %q", s)
	}
	lit := strings.Trim(s[1:end], `"'`)
	return lit, s[end+1:], nil
}

// resolvePath walks a chain of .field / ["field"] / [0] accessors
// against a root map/slice value.
func resolvePath(root any, path string) (any, error) {
	cur := any(root)
	path = strings.TrimSpace(path)
	for len(path) > 0 {
		switch path[0] {
		case '.':
			path = path[1:]
			end := strings.IndexAny(path, ".[")
			var field string
			if end == -1 {
				field, path = path, ""
			} else {
				field, path = path[:end], path[end:]
			}
			if field == "" {
				continue
			}
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expr: cannot access field %q on non-object", field)
			}
			cur = m[field]
		case '[':
			end := strings.Index(path, "]")
			if end == -1 {
				return nil, fmt.Errorf("expr: unterminated '[' in path")
			}
			key := strings.Trim(path[1:end], `"'`)
			path = path[end+1:]
			if idx, err := strconv.Atoi(key); err == nil {
				s, ok := cur.([]any)
				if !ok || idx < 0 || idx >= len(s) {
					return nil, fmt.Errorf("expr: index %d out of range", idx)
				}
				cur = s[idx]
			} else {
				m, ok := cur.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("expr: cannot access key %q on non-object", key)
				}
				cur = m[key]
			}
		default:
			return nil, fmt.Errorf("expr: unexpected token at %q", path)
		}
	}
	return cur, nil
}
