package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/nodes"
	"microflow/internal/parser"
)

func lookPath(name string) (string, error) {
	return exec.LookPath(name)
}

func envIntOr(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func runEngine(res *parser.ParseResult, startNode string) {
	mt := &mockTransport{}
	// google.go's Sheets/YouTube/Gmail executors call http.DefaultClient
	// directly (not the injectable client below) -- see verification
	// report. Overriding the process-wide default transport is the only
	// way to intercept those calls from a harness.
	http.DefaultTransport = mt

	envVars := map[string]string{
		"OPENROUTER_API_KEY":   "synthetic-test-openrouter-key",
		"GEMINI_API_KEY":       "synthetic-test-gemini-key",
		"YOUTUBE_DATA_API_KEY": "synthetic-test-youtube-key",
		"NOTIFY_WEBHOOK_URL":   "https://example.invalid/notify",
		"APP_REFERER_URL":      "https://example.invalid",
	}
	var envAllowlist []string
	for k, v := range envVars {
		os.Setenv(k, v)
		envAllowlist = append(envAllowlist, k)
	}

	scratch, err := os.MkdirTemp("", "e2echeck-scratch-*")
	if err != nil {
		fmt.Println("mkdir scratch:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(scratch)

	pythonPath, err := lookPath("python3")
	if err != nil {
		fmt.Println("python3 not found on PATH:", err)
		os.Exit(1)
	}

	deps := nodes.Deps{
		HTTPClient:         &http.Client{Transport: mt, Timeout: 20 * time.Second},
		AllowedBinaries:    map[string]string{"python3": pythonPath},
		EnvAllowlist:       envAllowlist,
		ScratchRoot:        scratch,
		CredentialResolver: fakeCredentialResolver{},
		GoogleAccounts:     fakeGoogleAccounts{},
	}
	registry := nodes.DefaultRegistry(deps)

	eng := engine.New(registry)
	// MaxSteps ceiling explained below; MaxConcurrentHeavy left at
	// engine.New's own default for this harness (irrelevant here since
	// e2echeck's http/command stand-ins are effectively instant).

	var peakSampler *heapPeakSampler
	if profPath := os.Getenv("MICROFLOW_DEBUG_HEAP_PROFILE"); profPath != "" {
		peakSampler = startHeapPeakSampler(profPath)
		defer peakSampler.stopAndReport()
	}
	// Keep this bounded well below the engine's own 5000-step ceiling so
	// a genuine infinite retry loop in THIS run (e.g. every AI call
	// looking "invalid" to the workflow's own validator, forever
	// cycling the retry-router) fails fast and diagnosably instead of
	// silently eating the whole timeout.
	eng.MaxSteps = 400

	wf := res.Workflow
	rc := &engine.RunContext{
		Workflow:   wf,
		Execution:  &model.Execution{ID: "e2echeck-1", WorkflowID: wf.ID, Mode: "manual"},
		StaticData: newFakeStaticData(),
		ScratchDir: scratch,
		// Mirrors cmd/server/main.go's MICROFLOW_NODE_RUN_CAP so this
		// harness measures the same real-world memory profile a
		// production run would have (see LOWRAM.md).
		NodeRunCap: envIntOr("MICROFLOW_NODE_RUN_CAP", 12),
	}

	// 90s was a reasonable cap for a smoke test that only exercises one
	// or two nodes, but a real end-to-end run of a workflow like this
	// one deliberately paces itself with production rate-limit waits
	// (Wait Before Fact Check, Wait Before Semantic Check, Wait Before
	// Image x5, Cooldown Between Scenes x5 -- ~85s of real sleep alone
	// across a 5-scene run) plus genuine FFmpeg render time for the
	// concat/subtitle/QC-frame steps. A 90s cap cut the run off mid-way
	// through "Check Final Video" and mis-reported it as a per-command
	// FFmpeg timeout, when the actual cause was this harness's own
	// outer deadline elapsing first. 10 minutes comfortably covers one
	// full multi-scene run with room to spare, while still catching a
	// genuine infinite-retry-loop bug (which MaxSteps=400 bounds
	// independently of wall-clock time).
	const wallClockCap = 10 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), wallClockCap)
	defer cancel()

	fmt.Printf("=== Engine run: starting at %q (MaxSteps=%d, %s wall clock cap) ===\n", startNode, eng.MaxSteps, wallClockCap)
	start := time.Now()
	_, runErr := eng.Run(ctx, rc, startNode, model.NodeOutput{{{JSON: map[string]any{}}}})
	elapsed := time.Since(start)

	fmt.Printf("\nRun finished in %s. Final status: %s\n", elapsed, rc.Execution.Status)
	if runErr != nil {
		fmt.Println("Run-level error:", runErr)
	}

	printNodeRunSummary(rc)
	printHTTPLog(mt)
}

// heapPeakSampler polls runtime.MemStats in the background and, whenever
// it observes a new peak HeapAlloc, overwrites a heap profile at path
// with a fresh pprof.WriteHeapProfile snapshot -- so by the time the run
// finishes, the on-disk profile reflects allocations live at (very close
// to) the actual peak, not just whatever's left resident at exit after
// GC has already reclaimed the interesting objects. Temporary profiling
// aid, gated behind MICROFLOW_DEBUG_HEAP_PROFILE -- not part of the
// normal build/runtime path.
type heapPeakSampler struct {
	path string
	stop chan struct{}
	done chan struct{}
	peak uint64
}

func startHeapPeakSampler(path string) *heapPeakSampler {
	s := &heapPeakSampler{path: path, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > s.peak {
					s.peak = ms.HeapAlloc
					if f, err := os.Create(s.path); err == nil {
						pprof.WriteHeapProfile(f)
						f.Close()
					}
				}
			}
		}
	}()
	return s
}

func (s *heapPeakSampler) stopAndReport() {
	close(s.stop)
	<-s.done
	fmt.Fprintf(os.Stderr, "Peak sampled HeapAlloc: %d bytes (%.1f MB) -- profile written to %s\n",
		s.peak, float64(s.peak)/1024/1024, s.path)
}

func printNodeRunSummary(rc *engine.RunContext) {
	fmt.Printf("\n=== Node run trail (%d entries) ===\n", len(rc.Execution.NodeRuns))
	statusCounts := map[model.ExecutionStatus]int{}
	for i, nr := range rc.Execution.NodeRuns {
		statusCounts[nr.Status]++
		line := fmt.Sprintf("%3d. [%-8s] %-45s attempt=%d dur=%s", i+1, nr.Status, nr.NodeName, nr.Attempt, nr.Duration.Round(time.Millisecond))
		if nr.Error != "" {
			line += "  ERROR: " + truncate(nr.Error, 160)
		}
		fmt.Println(line)
	}
	fmt.Println("\n--- status totals ---")
	var keys []string
	for k := range statusCounts {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-10s %d\n", k, statusCounts[model.ExecutionStatus(k)])
	}
}

func printHTTPLog(mt *mockTransport) {
	fmt.Printf("\n=== Outbound HTTP calls intercepted (%d) ===\n", len(mt.log))
	for i, l := range mt.log {
		fmt.Printf("%3d. %s\n", i+1, l)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...[truncated]"
}
