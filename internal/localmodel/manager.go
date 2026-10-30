package localmodel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// persisted host-local state (a small JSON file next to the downloaded files;
// deliberately NOT in the shared database: the install lives on this machine).
type state struct {
	Enabled     bool      `json:"enabled"`
	WantRunning bool      `json:"wantRunning"`
	RuntimeTag  string    `json:"runtimeTag,omitempty"`
	RuntimeBin  string    `json:"runtimeBin,omitempty"` // relative to Manager.dir
	RuntimeVer  string    `json:"runtimeVersion,omitempty"`
	ModelFile   string    `json:"modelFile,omitempty"`
	ModelBytes  int64     `json:"modelBytes,omitempty"`
	InstalledAt time.Time `json:"installedAt,omitempty"`
}

// Status is the JSON shown in Settings → Model.
type Status struct {
	Supported        bool   `json:"supported"`
	BlockedReason    string `json:"blockedReason,omitempty"`
	Platform         string `json:"platform"`
	Installed        bool   `json:"installed"`
	RuntimeInstalled bool   `json:"runtimeInstalled"`
	ModelInstalled   bool   `json:"modelInstalled"`
	RuntimeVersion   string `json:"runtimeVersion,omitempty"`
	ModelFile        string `json:"modelFile,omitempty"`
	ModelBytes       int64  `json:"modelBytes,omitempty"`
	Installing       bool   `json:"installing"`
	InstallStage     string `json:"installStage,omitempty"`
	InstallDone      int64  `json:"installDoneBytes,omitempty"`
	InstallTotal     int64  `json:"installTotalBytes,omitempty"`
	InstallError     string `json:"installError,omitempty"`
	Enabled          bool   `json:"enabled"`
	Process          string `json:"process"` // stopped | starting | running | stopping
	Ready            bool   `json:"ready"`
	PID              int    `json:"pid,omitempty"`
	Available        bool   `json:"available"` // enabled && ready && not cooling down
	CooldownSeconds  int    `json:"cooldownSeconds,omitempty"`
	LastError        string `json:"lastError,omitempty"`
	ContextSize      int    `json:"contextSize"`
	Threads          int    `json:"threads"`
	Alias            string `json:"modelId"`
}

// Manager owns the install, the child process and the on-disk state.
type Manager struct {
	dir  string
	sp   spec
	spOK bool
	logf func(format string, args ...any)

	mu       sync.Mutex
	st       state
	blocked  string
	inst     *installJob
	instErr  string
	proc     *procHandle
	lastErr  string
	restarts []time.Time

	failures      int
	cooldownUntil time.Time

	chatSem chan struct{}
}

type installJob struct {
	cancel context.CancelFunc
	done   chan struct{}
	stage  string
	bytes  int64
	total  int64
}

// New creates a Manager storing everything under dir ("" = default per-user
// location). It never starts anything by itself; call Resume once at boot.
func New(dir string, logf func(string, ...any)) *Manager {
	if logf == nil {
		logf = log.Printf
	}
	if dir == "" {
		dir = defaultDir()
	}
	m := &Manager{dir: dir, logf: logf, chatSem: make(chan struct{}, 1)}
	m.sp, m.spOK = defaultSpec()
	if os.Getenv("RENDER") != "" && os.Getenv("MICROFLOW_LOCAL_MODEL_ALLOW") != "1" {
		m.blocked = "Local model is disabled on Render/hosted deployments. Run MicroFlow on your own computer to use Qwen3 1.7B."
	} else if !m.spOK {
		m.blocked = "No llama.cpp runtime is available for " + runtime.GOOS + "/" + runtime.GOARCH + "."
	}
	m.loadState()
	m.reapStale()
	return m
}

func defaultDir() string {
	if v := strings.TrimSpace(os.Getenv("MICROFLOW_LOCAL_MODEL_DIR")); v != "" {
		return v
	}
	if base, err := os.UserCacheDir(); err == nil && base != "" {
		return filepath.Join(base, "microflow", "localmodel")
	}
	return filepath.Join(os.TempDir(), "microflow-localmodel")
}

func (m *Manager) statePath() string { return filepath.Join(m.dir, "state.json") }
func (m *Manager) modelPath() string { return filepath.Join(m.dir, "models", modelFile) }

func (m *Manager) loadState() {
	b, err := os.ReadFile(m.statePath())
	if err != nil {
		return
	}
	var s state
	if json.Unmarshal(b, &s) == nil {
		m.st = s
	}
	// Self-heal: state says installed but files are gone.
	if m.st.RuntimeBin != "" {
		if _, err := os.Stat(filepath.Join(m.dir, m.st.RuntimeBin)); err != nil {
			m.st.RuntimeBin, m.st.RuntimeVer, m.st.RuntimeTag = "", "", ""
		}
	}
	if m.st.ModelFile != "" {
		if _, err := os.Stat(m.modelPath()); err != nil {
			m.st.ModelFile, m.st.ModelBytes = "", 0
		}
	}
}

// saveLocked writes state atomically. Caller holds m.mu.
func (m *Manager) saveLocked() error {
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(m.st, "", "  ")
	tmp := m.statePath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.statePath())
}

func (m *Manager) installedLocked() bool { return m.st.RuntimeBin != "" && m.st.ModelFile != "" }

// ---- tunables (env overrides, safe low-resource defaults) ----

func envInt(name string, def, min, max int) int {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= min && n <= max {
			return n
		}
	}
	return def
}

// ContextSize is the model context window (tokens). The default 16384 lets the
// Script Writer prompt (about 6.4k tokens) plus its 5000-token answer fit; the KV
// cache then needs roughly 1.9 GB. Lower it with MICROFLOW_QWEN_CTX on small
// machines (4096 needs about 0.45 GB but is too small for the Script Writer).
func ContextSize() int { return envInt("MICROFLOW_QWEN_CTX", 16384, 1024, 32768) }

// Threads defaults to half the cores (max 4) so the rest of MicroFlow keeps CPU.
func Threads() int {
	d := runtime.NumCPU() / 2
	if d < 1 {
		d = 1
	}
	if d > 4 {
		d = 4
	}
	return envInt("MICROFLOW_QWEN_THREADS", d, 1, 64)
}

// MaxContext exposes the context size to the request translator.
func (m *Manager) MaxContext() int { return ContextSize() }

// ---- status ----

func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := Status{
		Supported:        m.blocked == "",
		BlockedReason:    m.blocked,
		Platform:         runtime.GOOS + "/" + runtime.GOARCH,
		RuntimeInstalled: m.st.RuntimeBin != "",
		ModelInstalled:   m.st.ModelFile != "",
		RuntimeVersion:   m.st.RuntimeVer,
		ModelFile:        m.st.ModelFile,
		ModelBytes:       m.st.ModelBytes,
		Enabled:          m.st.Enabled,
		LastError:        m.lastErr,
		ContextSize:      ContextSize(),
		Threads:          Threads(),
		Alias:            ModelAlias,
		Process:          "stopped",
	}
	s.Installed = m.installedLocked()
	if m.inst != nil {
		s.Installing = true
		s.InstallStage, s.InstallDone, s.InstallTotal = m.inst.stage, m.inst.bytes, m.inst.total
	}
	s.InstallError = m.instErr
	if m.proc != nil {
		s.PID = m.proc.pid
		switch {
		case m.proc.stopping:
			s.Process = "stopping"
		case m.proc.ready:
			s.Process, s.Ready = "running", true
		default:
			s.Process = "starting"
		}
	}
	if d := time.Until(m.cooldownUntil); d > 0 {
		s.CooldownSeconds = int(d.Seconds()) + 1
	}
	s.Available = m.availableLocked()
	return s
}

func (m *Manager) availableLocked() bool {
	return m.st.Enabled && m.proc != nil && m.proc.ready && !m.proc.stopping && time.Now().After(m.cooldownUntil)
}

// Available reports whether an AI request may be sent to the local model now:
// enabled, running, ready and not in a failure cool-down.
func (m *Manager) Available() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.availableLocked()
}

// ---- install / uninstall ----

var errBusy = errors.New("an install is already in progress")

// Install starts the background install (runtime + model). It returns at once;
// progress is visible through Status.
func (m *Manager) Install() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.blocked != "" {
		return errors.New(m.blocked)
	}
	if m.inst != nil {
		return errBusy
	}
	if m.installedLocked() {
		return errors.New("already installed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	job := &installJob{cancel: cancel, done: make(chan struct{}), stage: "starting"}
	m.inst = job
	m.instErr = ""
	go m.runInstall(ctx, job)
	return nil
}

func (m *Manager) setStage(job *installJob, stage string, done, total int64) {
	m.mu.Lock()
	job.stage, job.bytes, job.total = stage, done, total
	m.mu.Unlock()
}

func (m *Manager) runInstall(ctx context.Context, job *installJob) {
	err := m.doInstall(ctx, job)
	m.mu.Lock()
	if err != nil {
		if errors.Is(err, context.Canceled) {
			m.instErr = "install cancelled"
		} else {
			m.instErr = err.Error()
		}
		m.logf("[local-model] install failed: %v", err)
	}
	m.inst = nil
	close(job.done)
	m.mu.Unlock()
}

func (m *Manager) doInstall(ctx context.Context, job *installJob) error {
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return fmt.Errorf("cannot create %s: %w", m.dir, err)
	}
	// 1) runtime
	m.mu.Lock()
	haveRuntime := m.st.RuntimeBin != ""
	m.mu.Unlock()
	if !haveRuntime {
		archive := filepath.Join(m.dir, "downloads", m.sp.RuntimeName)
		m.setStage(job, "downloading runtime", 0, 0)
		if err := m.download(ctx, []string{m.sp.RuntimeURL}, archive, m.sp.RuntimeSHA256, m.sp.RuntimeMinBytes, 512<<20, false, func(d, t int64) { m.setStage(job, "downloading runtime", d, t) }); err != nil {
			return fmt.Errorf("runtime download: %w", err)
		}
		m.setStage(job, "unpacking runtime", 0, 0)
		rtDir := filepath.Join(m.dir, "runtime", llamaTag)
		os.RemoveAll(rtDir)
		if err := extractArchive(archive, rtDir); err != nil {
			os.RemoveAll(rtDir)
			os.Remove(archive)
			return fmt.Errorf("runtime unpack: %w", err)
		}
		bin, err := findServerBinary(rtDir)
		if err != nil {
			os.RemoveAll(rtDir)
			return err
		}
		_ = os.Chmod(bin, 0o755)
		m.setStage(job, "verifying runtime", 0, 0)
		ver, err := verifyRuntime(bin)
		if err != nil {
			os.RemoveAll(rtDir)
			return err
		}
		os.Remove(archive) // keep disk small; the verified tree is what we run
		rel, _ := filepath.Rel(m.dir, bin)
		m.mu.Lock()
		m.st.RuntimeBin, m.st.RuntimeVer, m.st.RuntimeTag = rel, ver, llamaTag
		err = m.saveLocked()
		m.mu.Unlock()
		if err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// 2) model
	m.setStage(job, "downloading model", 0, 0)
	dest := m.modelPath()
	if err := m.download(ctx, m.sp.ModelURLs, dest, m.sp.ModelSHA256, m.sp.MinModelBytes, m.sp.MaxModelBytes, m.sp.CheckLinkedETag, func(d, t int64) { m.setStage(job, "downloading model", d, t) }); err != nil {
		return fmt.Errorf("model download: %w", err)
	}
	if err := checkGGUF(dest); err != nil {
		os.Remove(dest)
		return err
	}
	st, err := os.Stat(dest)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.st.ModelFile, m.st.ModelBytes, m.st.InstalledAt = modelFile, st.Size(), time.Now().UTC()
	err = m.saveLocked()
	m.mu.Unlock()
	if err == nil {
		m.logf("[local-model] installed runtime %s + %s", llamaTag, modelFile)
	}
	return err
}

// CancelInstall aborts a running install (partial model file is kept so a
// later Install can resume).
func (m *Manager) CancelInstall() {
	m.mu.Lock()
	job := m.inst
	m.mu.Unlock()
	if job != nil {
		job.cancel()
		<-job.done
	}
}

// Uninstall stops the process, cancels any install and deletes runtime, model,
// downloads and state. Settings return to "not installed, disabled".
func (m *Manager) Uninstall() error {
	m.CancelInstall()
	if err := m.Stop(); err != nil {
		m.logf("[local-model] stop during uninstall: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.proc != nil {
		return errors.New("the model process is still shutting down; try again in a moment")
	}
	for _, sub := range []string{"runtime", "models", "downloads"} {
		if err := os.RemoveAll(filepath.Join(m.dir, sub)); err != nil {
			return fmt.Errorf("could not remove %s: %w", sub, err)
		}
	}
	os.Remove(filepath.Join(m.dir, "llama.pid"))
	m.st = state{}
	m.lastErr, m.instErr = "", ""
	m.failures, m.cooldownUntil = 0, time.Time{}
	if err := m.saveLocked(); err != nil {
		return err
	}
	return nil
}

// SetEnabled turns use of the model for AI requests on/off (independent of
// whether the process is running).
func (m *Manager) SetEnabled(on bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if on && !m.installedLocked() {
		return errors.New("install Qwen3 1.7B first")
	}
	m.st.Enabled = on
	if on {
		m.failures, m.cooldownUntil = 0, time.Time{}
	}
	return m.saveLocked()
}
