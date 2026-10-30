package localmodel

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// tailBuf keeps the last few KB of the child's output for error messages.
type tailBuf struct {
	mu  sync.Mutex
	buf []byte
}

func (t *tailBuf) Write(p []byte) (int, error) {
	t.mu.Lock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > 4096 {
		t.buf = t.buf[len(t.buf)-4096:]
	}
	t.mu.Unlock()
	return len(p), nil
}
func (t *tailBuf) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimSpace(string(t.buf))
}

type procHandle struct {
	cmd      *exec.Cmd
	pid      int
	port     int
	apiKey   string
	ready    bool
	stopping bool
	exited   chan struct{}
	out      *tailBuf
}

func randomKey() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func (m *Manager) pidPath() string { return filepath.Join(m.dir, "llama.pid") }

// Start launches llama-server on 127.0.0.1 (random port, random API key) and
// returns once the process is spawned; readiness is tracked in the background.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startLocked(true)
}

func (m *Manager) startLocked(persist bool) error {
	if m.blocked != "" {
		return errors.New(m.blocked)
	}
	if m.inst != nil {
		return errors.New("wait for the install to finish")
	}
	if !m.installedLocked() {
		return errors.New("install Qwen3 1.7B first")
	}
	if m.proc != nil {
		if m.proc.stopping {
			return errors.New("the model is still stopping; try again in a moment")
		}
		return nil // already running/starting: idempotent
	}
	bin := filepath.Join(m.dir, m.st.RuntimeBin)
	if _, err := os.Stat(bin); err != nil {
		return errors.New("runtime files are missing; uninstall and install again")
	}
	if _, err := os.Stat(m.modelPath()); err != nil {
		return errors.New("model file is missing; uninstall and install again")
	}
	port, err := freePort()
	if err != nil {
		return fmt.Errorf("no free local port: %w", err)
	}
	h := &procHandle{port: port, apiKey: randomKey(), exited: make(chan struct{}), out: &tailBuf{}}
	args := []string{
		"-m", m.modelPath(),
		"--host", "127.0.0.1", "--port", strconv.Itoa(port),
		"--api-key", h.apiKey,
		"--alias", ModelAlias,
		"-c", strconv.Itoa(ContextSize()),
		"-t", strconv.Itoa(Threads()), "-tb", strconv.Itoa(Threads()),
		"-np", "1", "-b", "256", "-ub", "256",
		"--no-webui", "--no-warmup", "--cache-ram", "0",
		"--chat-template-kwargs", `{"enable_thinking":false}`,
	}
	cmd := exec.Command(bin, args...)
	cmd.Dir = filepath.Dir(bin)
	cmd.Stdout, cmd.Stderr = h.out, h.out
	configureChild(cmd)
	if err := cmd.Start(); err != nil {
		m.lastErr = "could not start runtime: " + err.Error()
		return errors.New(m.lastErr)
	}
	h.cmd, h.pid = cmd, cmd.Process.Pid
	lowerPriority(h.pid)
	_ = os.WriteFile(m.pidPath(), []byte(strconv.Itoa(h.pid)), 0o644)
	m.proc = h
	m.lastErr = ""
	if persist {
		m.st.WantRunning = true
		_ = m.saveLocked()
	}
	go m.watch(h)
	go m.waitReady(h)
	m.logf("[local-model] starting llama-server pid=%d port=%d ctx=%d threads=%d", h.pid, port, ContextSize(), Threads())
	return nil
}

// watch reaps the child, records why it ended, and (only if it died on its own
// while it was supposed to run) restarts it at most twice per hour.
func (m *Manager) watch(h *procHandle) {
	err := h.cmd.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()
	defer close(h.exited) // runs before Unlock, after m.proc is cleared below
	if m.proc == h {
		m.proc = nil
	}
	os.Remove(m.pidPath())
	if h.stopping {
		return
	}
	reason := "exited"
	if err != nil {
		reason = err.Error()
	}
	m.lastErr = fmt.Sprintf("model process stopped unexpectedly (%s): %s", reason, tail(h.out.String(), 400))
	m.logf("[local-model] %s", m.lastErr)
	if !m.st.WantRunning {
		return
	}
	cut := time.Now().Add(-time.Hour)
	var recent []time.Time
	for _, t := range m.restarts {
		if t.After(cut) {
			recent = append(recent, t)
		}
	}
	m.restarts = recent
	if len(recent) >= 2 {
		m.st.WantRunning = false
		_ = m.saveLocked()
		m.lastErr += " (not restarting: too many crashes; press Start to retry)"
		return
	}
	m.restarts = append(m.restarts, time.Now())
	go func() {
		time.Sleep(3 * time.Second)
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.st.WantRunning && m.proc == nil {
			if err := m.startLocked(false); err != nil {
				m.lastErr = "automatic restart failed: " + err.Error()
			}
		}
	}()
}

func (m *Manager) waitReady(h *procHandle) {
	deadline := time.Now().Add(time.Duration(envInt("MICROFLOW_QWEN_START_TIMEOUT_SECONDS", 180, 10, 1800)) * time.Second)
	client := &http.Client{Timeout: 3 * time.Second}
	url := "http://127.0.0.1:" + strconv.Itoa(h.port) + "/health"
	for time.Now().Before(deadline) {
		select {
		case <-h.exited:
			return
		case <-time.After(500 * time.Millisecond):
		}
		resp, err := client.Get(url)
		if err != nil {
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			m.mu.Lock()
			if m.proc == h && !h.stopping {
				h.ready = true
				m.logf("[local-model] ready on 127.0.0.1:%d", h.port)
			}
			m.mu.Unlock()
			return
		}
	}
	// never became ready: kill it so it does not sit on RAM
	m.mu.Lock()
	if m.proc == h && !h.ready {
		m.lastErr = "model did not become ready in time: " + tail(h.out.String(), 400)
	}
	m.mu.Unlock()
	m.stopHandle(h)
}

// Stop terminates the process (graceful, then forced) and remembers that it
// should stay stopped across MicroFlow restarts.
func (m *Manager) Stop() error {
	m.mu.Lock()
	h := m.proc
	m.st.WantRunning = false
	_ = m.saveLocked()
	m.mu.Unlock()
	if h == nil {
		return nil
	}
	return m.stopHandle(h)
}

func (m *Manager) stopHandle(h *procHandle) error {
	m.mu.Lock()
	h.stopping = true
	h.ready = false
	m.mu.Unlock()
	terminateProcess(h.cmd, h.pid, 5*time.Second)
	select {
	case <-h.exited:
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("model process did not exit")
	}
}

// Shutdown is called when MicroFlow exits: stop the child but keep the
// "wanted running" flag so Resume restarts it next boot.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	h := m.proc
	m.mu.Unlock()
	if h != nil {
		_ = m.stopHandle(h)
	}
}

// Resume restarts the model at boot if it was running when MicroFlow last
// exited. Failure is only logged: AI requests simply use the fallback.
func (m *Manager) Resume() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blocked == "" && m.st.WantRunning && m.installedLocked() {
		if err := m.startLocked(false); err != nil {
			m.lastErr = err.Error()
			m.logf("[local-model] resume failed: %v", err)
		}
	}
}

// reapStale kills a llama-server left behind by a previous MicroFlow crash.
func (m *Manager) reapStale() {
	b, err := os.ReadFile(m.pidPath())
	if err != nil {
		return
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	if pid > 1 && processLooksLike(pid, modelFile) {
		m.logf("[local-model] terminating leftover model process pid=%d", pid)
		killPID(pid, 5*time.Second)
	}
	os.Remove(m.pidPath())
}

// ---- chat client ----

// Chat sends an OpenAI-style chat completion to the local server and returns
// the decoded response. Any failure (not running, timeout, bad/empty output)
// is returned as an error AND counted toward a short circuit-breaker so a
// broken model stops adding latency to every request.
func (m *Manager) Chat(ctx context.Context, req map[string]any) (map[string]any, error) {
	m.mu.Lock()
	h := m.proc
	ok := m.availableLocked()
	m.mu.Unlock()
	if !ok || h == nil {
		return nil, errors.New("local model not available")
	}
	select {
	case m.chatSem <- struct{}{}:
		defer func() { <-m.chatSem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	timeout := time.Duration(envInt("MICROFLOW_QWEN_TIMEOUT_SECONDS", 180, 5, 3600)) * time.Second
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req["model"] = ModelAlias
	req["stream"] = false
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	hr, err := http.NewRequestWithContext(cctx, http.MethodPost, "http://127.0.0.1:"+strconv.Itoa(h.port)+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("Authorization", "Bearer "+h.apiKey)
	resp, err := (&http.Client{}).Do(hr)
	if err != nil {
		if ctx.Err() == nil {
			m.recordFailure("request failed: " + err.Error())
		}
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		m.recordFailure("read failed: " + err.Error())
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		// 400 = request too large for the context etc.: the request, not the
		// model, is at fault, so it does not trip the circuit breaker.
		if resp.StatusCode >= 500 {
			m.recordFailure(fmt.Sprintf("HTTP %d", resp.StatusCode))
		}
		return nil, fmt.Errorf("local model HTTP %d: %s", resp.StatusCode, tail(string(raw), 200))
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		m.recordFailure("invalid JSON from model")
		return nil, err
	}
	if _, err := ExtractText(out); err != nil {
		m.recordFailure(err.Error())
		return nil, err
	}
	m.recordSuccess()
	return out, nil
}

// ExtractText returns choices[0].message.content (with any <think> block
// removed) or an error if the response carries no usable text.
func ExtractText(resp map[string]any) (string, error) {
	choices, _ := resp["choices"].([]any)
	if len(choices) == 0 {
		return "", errors.New("local model returned no choices")
	}
	c0, _ := choices[0].(map[string]any)
	msg, _ := c0["message"].(map[string]any)
	text, _ := msg["content"].(string)
	text = StripThink(text)
	if strings.TrimSpace(text) == "" {
		return "", errors.New("local model returned empty text")
	}
	return text, nil
}

// StripThink removes Qwen3 <think>...</think> blocks (also an unterminated one).
func StripThink(s string) string {
	for {
		i := strings.Index(s, "<think>")
		if i < 0 {
			break
		}
		j := strings.Index(s[i:], "</think>")
		if j < 0 {
			s = s[:i]
			break
		}
		s = s[:i] + s[i+j+len("</think>"):]
	}
	if j := strings.Index(s, "</think>"); j >= 0 {
		s = s[j+len("</think>"):]
	}
	return strings.TrimSpace(s)
}

func (m *Manager) recordFailure(why string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures++
	m.lastErr = "local model request failed: " + why
	if m.failures >= 3 {
		m.cooldownUntil = time.Now().Add(2 * time.Minute)
		m.failures = 0
		m.logf("[local-model] 3 consecutive failures; using fallback providers for 2 minutes")
	}
}

func (m *Manager) recordSuccess() {
	m.mu.Lock()
	m.failures = 0
	m.mu.Unlock()
}
