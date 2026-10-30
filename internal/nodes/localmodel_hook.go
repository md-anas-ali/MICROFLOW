package nodes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"microflow/internal/engine"
	"microflow/internal/localmodel"
	"microflow/internal/model"
)

// LocalModel is the slice of internal/localmodel.Manager the HTTP node needs.
// It is an interface so the node package stays testable and so a nil value
// means "feature off" with zero behaviour change.
type LocalModel interface {
	Available() bool
	MaxContext() int
	Chat(ctx context.Context, req map[string]any) (map[string]any, error)
}

// localTracker implements "each distinct prompt gets the local model at most N
// times per execution". A workflow's own retry loop re-sends the same prompt
// after rejecting an answer; sending that retry to Qwen again would loop on the
// same bad answer, so the retry is allowed to reach the existing free providers.
type localTracker struct {
	mu   sync.Mutex
	exec string
	seen map[string]int
}

func (t *localTracker) take(exec, key string, max int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.seen == nil || t.exec != exec || len(t.seen) > 4096 {
		t.exec, t.seen = exec, map[string]int{}
	}
	if t.seen[key] >= max {
		return false
	}
	t.seen[key]++
	return true
}

func (t *localTracker) reset() {
	t.mu.Lock()
	t.seen = nil
	t.mu.Unlock()
}

type localKind int

const (
	kindOpenAI localKind = iota + 1 // OpenRouter and every OpenAI-compatible /chat/completions
	kindGemini                      // generativelanguage.googleapis.com ...:generateContent
)

func localKindFor(rawURL string) (localKind, bool) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return 0, false
	}
	switch {
	case strings.HasSuffix(u.Path, "/chat/completions"):
		return kindOpenAI, true
	case u.Hostname() == "generativelanguage.googleapis.com" && strings.HasSuffix(u.Path, ":generateContent"):
		return kindGemini, true
	}
	return 0, false
}

// tryLocal runs one AI request on the local Qwen3 model when that is enabled
// and available. ok=false means "use the normal provider request unchanged";
// that is the case for every non-AI request, every request shape the local
// model cannot honour exactly (tools, images, schemas, streaming ...), a
// retried prompt, a failure, a timeout, or an answer cut off by the token limit.
func (e *HTTPRequestExecutor) tryLocal(ctx context.Context, rc *engine.RunContext, node *model.Node, rawURL, bodyStr string) (map[string]any, bool) {
	lm := e.Local
	if lm == nil || bodyStr == "" || !lm.Available() {
		return nil, false
	}
	kind, ok := localKindFor(rawURL)
	if !ok {
		return nil, false
	}
	var body map[string]any
	if json.Unmarshal([]byte(bodyStr), &body) != nil {
		return nil, false
	}
	var req map[string]any
	var key string
	if kind == kindOpenAI {
		req, key, ok = buildFromOpenAI(body, lm.MaxContext())
	} else {
		req, key, ok = buildFromGemini(body, lm.MaxContext())
	}
	if !ok {
		return nil, false
	}
	execID := ""
	if rc != nil && rc.Execution != nil {
		execID = rc.Execution.ID
	}
	max := 1
	if v := envOf(rc, "MICROFLOW_QWEN_ATTEMPTS_PER_PROMPT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 5 {
			max = n
		}
	}
	if !e.localTrack.take(execID, key, max) {
		return nil, false // a retry of the same prompt: let the fallback providers answer
	}
	if rc != nil && rc.HeavyWorkGate != nil {
		rc.MemGuard.WaitIfThrottled(ctx)
		select {
		case rc.HeavyWorkGate <- struct{}{}:
			defer func() { <-rc.HeavyWorkGate }()
		case <-ctx.Done():
			return nil, false
		}
	}
	resp, err := lm.Chat(ctx, req)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("[local-model] node %q: local answer failed, using fallback providers: %v", node.Name, err)
		}
		return nil, false
	}
	if fr := finishReason(resp); fr == "length" {
		log.Printf("[local-model] node %q: local answer hit the token limit, using fallback providers", node.Name)
		return nil, false
	}
	text, err := localmodel.ExtractText(resp)
	if err != nil {
		return nil, false
	}
	var out map[string]any
	if kind == kindOpenAI {
		out = openAIResult(text, resp)
	} else {
		out = geminiResult(text, resp)
	}
	// Normalise numbers/types exactly like a decoded provider response.
	raw, _ := json.Marshal(out)
	dec, err := decodeJSONLoose(raw)
	m, isMap := dec.(map[string]any)
	if err != nil || !isMap {
		return nil, false
	}
	if wantsFullResponse(node) {
		return map[string]any{"statusCode": 200, "headers": map[string]any{}, "body": m}, true
	}
	return m, true
}

func envOf(rc *engine.RunContext, k string) string {
	if rc != nil {
		return rc.Env(k)
	}
	return ""
}

func finishReason(resp map[string]any) string {
	if ch, _ := resp["choices"].([]any); len(ch) > 0 {
		if c0, _ := ch[0].(map[string]any); c0 != nil {
			s, _ := c0["finish_reason"].(string)
			return s
		}
	}
	return ""
}

func usageOf(resp map[string]any) (p, c int) {
	if u, _ := resp["usage"].(map[string]any); u != nil {
		pf, _ := u["prompt_tokens"].(float64)
		cf, _ := u["completion_tokens"].(float64)
		return int(pf), int(cf)
	}
	return 0, 0
}

func openAIResult(text string, resp map[string]any) map[string]any {
	p, c := usageOf(resp)
	return map[string]any{
		"id": "chatcmpl-local-" + strconv.FormatInt(time.Now().UnixNano(), 36), "object": "chat.completion",
		"created": time.Now().Unix(), "model": localmodel.ModelAlias,
		"choices": []any{map[string]any{"index": 0, "finish_reason": "stop",
			"message": map[string]any{"role": "assistant", "content": text}}},
		"usage":                 map[string]any{"prompt_tokens": p, "completion_tokens": c, "total_tokens": p + c},
		"microflow_local_model": true,
	}
}

func geminiResult(text string, resp map[string]any) map[string]any {
	p, c := usageOf(resp)
	return map[string]any{
		"candidates": []any{map[string]any{"index": 0, "finishReason": "STOP",
			"content": map[string]any{"role": "model", "parts": []any{map[string]any{"text": text}}}}},
		"usageMetadata":         map[string]any{"promptTokenCount": p, "candidatesTokenCount": c, "totalTokenCount": p + c},
		"modelVersion":          localmodel.ModelAlias,
		"microflow_local_model": true,
	}
}

// ---- request translation ----

func hasAny(m map[string]any, keys ...string) bool {
	for _, k := range keys {
		if _, ok := m[k]; ok {
			return true
		}
	}
	return false
}

type chatMsg struct{ role, text string }

func finishRequest(msgs []chatMsg, body map[string]any, ctxSize int, req map[string]any, requestedMax int) (map[string]any, string, bool) {
	if len(msgs) == 0 {
		return nil, "", false
	}
	chars := 0
	out := make([]any, 0, len(msgs))
	for _, m := range msgs {
		chars += len(m.text) + 32
		out = append(out, map[string]any{"role": m.role, "content": m.text})
	}
	// Deliberately pessimistic (non-Latin scripts tokenise badly): ~2 chars/token.
	remaining := ctxSize - chars/2 - 64
	if remaining < 256 {
		return nil, "", false // prompt too big for the local context: use the providers
	}
	maxTok := requestedMax
	if maxTok <= 0 || maxTok > remaining {
		maxTok = remaining
		if requestedMax <= 0 && maxTok > 2048 {
			maxTok = 2048
		}
	}
	req["messages"] = out
	req["max_tokens"] = maxTok
	if _, ok := req["temperature"]; !ok {
		req["temperature"] = 0.7
	}
	if _, ok := req["top_p"]; !ok {
		req["top_p"] = 0.8
	}
	req["top_k"] = 20
	h := sha256.New()
	j, _ := json.Marshal(out)
	h.Write(j)
	return req, hex.EncodeToString(h.Sum(nil)), true
}

func localNum(v any) (float64, bool) { f, ok := v.(float64); return f, ok }

func buildFromOpenAI(body map[string]any, ctxSize int) (map[string]any, string, bool) {
	if hasAny(body, "tools", "tool_choice", "functions", "function_call", "modalities", "audio", "plugins", "web_search_options") {
		return nil, "", false
	}
	if s, _ := body["stream"].(bool); s {
		return nil, "", false
	}
	if n, ok := localNum(body["n"]); ok && n > 1 {
		return nil, "", false
	}
	raw, _ := body["messages"].([]any)
	var msgs []chatMsg
	for _, r := range raw {
		m, _ := r.(map[string]any)
		role, _ := m["role"].(string)
		if role != "system" && role != "user" && role != "assistant" {
			return nil, "", false
		}
		var text string
		switch c := m["content"].(type) {
		case string:
			text = c
		case []any:
			var parts []string
			for _, p := range c {
				pm, _ := p.(map[string]any)
				if t, _ := pm["type"].(string); t != "text" {
					return nil, "", false // image/audio/file parts need a multimodal model
				}
				s, _ := pm["text"].(string)
				parts = append(parts, s)
			}
			text = strings.Join(parts, "\n")
		default:
			return nil, "", false
		}
		msgs = append(msgs, chatMsg{role, text})
	}
	req := map[string]any{}
	for _, k := range []string{"temperature", "top_p", "seed", "stop", "presence_penalty", "frequency_penalty"} {
		if v, ok := body[k]; ok {
			req[k] = v
		}
	}
	if rf, ok := body["response_format"].(map[string]any); ok {
		switch rf["type"] {
		case "json_object", "json_schema", "text":
			req["response_format"] = rf
		default:
			return nil, "", false
		}
	}
	reqMax := 0
	if f, ok := localNum(body["max_tokens"]); ok {
		reqMax = int(f)
	} else if f, ok := localNum(body["max_completion_tokens"]); ok {
		reqMax = int(f)
	}
	return finishRequest(msgs, body, ctxSize, req, reqMax)
}

func geminiText(parts any) (string, bool) {
	arr, _ := parts.([]any)
	var out []string
	for _, p := range arr {
		pm, _ := p.(map[string]any)
		for k := range pm {
			if k != "text" {
				return "", false // inlineData / fileData / functionCall ...
			}
		}
		s, _ := pm["text"].(string)
		out = append(out, s)
	}
	return strings.Join(out, "\n"), true
}

func buildFromGemini(body map[string]any, ctxSize int) (map[string]any, string, bool) {
	if hasAny(body, "tools", "toolConfig", "cachedContent") {
		return nil, "", false
	}
	var msgs []chatMsg
	for _, k := range []string{"systemInstruction", "system_instruction"} {
		if si, ok := body[k].(map[string]any); ok {
			t, ok := geminiText(si["parts"])
			if !ok {
				return nil, "", false
			}
			msgs = append(msgs, chatMsg{"system", t})
			break
		}
	}
	contents, _ := body["contents"].([]any)
	for _, c := range contents {
		cm, _ := c.(map[string]any)
		role := "user"
		if r, _ := cm["role"].(string); r == "model" {
			role = "assistant"
		}
		t, ok := geminiText(cm["parts"])
		if !ok {
			return nil, "", false
		}
		msgs = append(msgs, chatMsg{role, t})
	}
	req := map[string]any{}
	reqMax := 0
	if gc, ok := body["generationConfig"].(map[string]any); ok {
		if hasAny(gc, "responseSchema", "responseJsonSchema", "responseModalities", "thinkingConfig", "speechConfig") {
			if _, only := gc["thinkingConfig"]; !only || hasAny(gc, "responseSchema", "responseJsonSchema", "responseModalities", "speechConfig") {
				return nil, "", false
			}
		}
		if n, ok := localNum(gc["candidateCount"]); ok && n > 1 {
			return nil, "", false
		}
		if v, ok := gc["temperature"]; ok {
			req["temperature"] = v
		}
		if v, ok := gc["topP"]; ok {
			req["top_p"] = v
		}
		if v, ok := gc["seed"]; ok {
			req["seed"] = v
		}
		if v, ok := gc["stopSequences"]; ok {
			req["stop"] = v
		}
		if f, ok := localNum(gc["maxOutputTokens"]); ok {
			reqMax = int(f)
		}
		if mt, _ := gc["responseMimeType"].(string); mt == "application/json" {
			req["response_format"] = map[string]any{"type": "json_object"}
		}
	}
	return finishRequest(msgs, body, ctxSize, req, reqMax)
}
