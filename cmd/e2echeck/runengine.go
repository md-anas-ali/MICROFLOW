package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/nodes"
	"microflow/internal/parser"
)

func lookPath(name string) (string, error) {
	return exec.LookPath(name)
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
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	fmt.Printf("=== Engine run: starting at %q (MaxSteps=%d, 90s wall clock cap) ===\n", startNode, eng.MaxSteps)
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
