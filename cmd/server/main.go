// MicroFlow server entrypoint.
//
// Verified in-sandbox (go build/vet/test/-race, gofmt, frontend smoketest --
// see STATUS.md). To run it:
//
//	go mod tidy   # only needed if go.sum is out of date for your toolchain
//	export DATABASE_URL="postgres://user:pass@host/db?sslmode=require"
//	export MICROFLOW_MASTER_KEY="$(go run ./cmd/server genkey)"
//	go run ./cmd/server
package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata" // embed the IANA zoneinfo database in the binary so
	// time.LoadLocation (see schedulerLocation below) works even on a
	// minimal image with no OS-level tzdata package installed (e.g. this
	// repo's Alpine runtime stage, which only apk-adds ffmpeg/python3/etc,
	// not tzdata) -- a few hundred KB of extra binary size, no added
	// runtime heap, in exchange for never silently falling back to UTC
	// on a host that happens to lack /usr/share/zoneinfo.

	"microflow/internal/api"
	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/nodes"
	"microflow/internal/runall"
	"microflow/internal/runner"
	"microflow/internal/scheduler"
	"microflow/internal/store"
	"microflow/internal/vault"
	"microflow/internal/webhook"
)

//go:embed all:static
var embeddedFrontend embed.FS // populated by `npm run build` output copied into backend/cmd/server/static -- see frontend/README

func main() {
	configureGoRuntimeForLowRAM()

	if len(os.Args) > 1 && os.Args[1] == "genkey" {
		k, err := vault.GenerateMasterKey()
		if err != nil {
			log.Fatal(err)
		}
		println(k)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := requireEnv("DATABASE_URL")
	masterKey, err := vault.MasterKeyFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	// Login page credentials come from MICROFLOW_LOGIN_USER /
	// MICROFLOW_LOGIN_PASSWORD (see auth.go). Fails closed if missing.
	gate, err := newAuthGateFromEnv()
	if err != nil {
		log.Fatal(err)
	}

	st, err := store.Open(ctx, dbURL)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer st.Close()

	schemaBytes, err := os.ReadFile("internal/store/schema.sql")
	if err == nil {
		if err := st.ApplySchema(ctx, string(schemaBytes)); err != nil {
			log.Fatalf("apply schema: %v", err)
		}
	} else {
		log.Printf("warning: could not read schema.sql locally (%v) -- apply internal/store/schema.sql manually if this is a fresh database", err)
	}

	// Execution history is intentionally short-lived: keep completed runs
	// for 12 hours, clean them once at startup, then repeat every 12 hours.
	// Running/queued executions are never deleted by this job. This keeps
	// Postgres history bounded without requiring a separate cron service.
	cleanupExecutionHistory := func() {
		cutoff := time.Now().Add(-12 * time.Hour)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		deleted, cleanupErr := st.DeleteExecutionsBefore(cleanupCtx, cutoff)
		if cleanupErr != nil {
			log.Printf("execution history cleanup failed: %v", cleanupErr)
			return
		}
		if deleted > 0 {
			log.Printf("execution history cleanup: deleted %d execution(s) older than 12h", deleted)
		}
	}
	cleanupExecutionHistory()
	recoveryTTLHours := envInt("MICROFLOW_RECOVERY_TTL_HOURS", 168)
	if recoveryTTLHours < 24 {
		recoveryTTLHours = 24
	}
	if recoveryTTLHours > 24*30 {
		recoveryTTLHours = 24 * 30
	}
	cleanupRecoveryState := func() {
		cutoff := time.Now().Add(-time.Duration(recoveryTTLHours) * time.Hour)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		deleted, cleanupErr := st.CleanupOrphanedExecutionCheckpoints(cleanupCtx, 20, cutoff)
		if cleanupErr != nil {
			log.Printf("recovery orphan cleanup failed: %v", cleanupErr)
			return
		}
		if deleted > 0 {
			log.Printf("recovery orphan cleanup: deleted %d checkpoint(s)", deleted)
		}
	}
	cleanupRecoveryState()
	go func() {
		ticker := time.NewTicker(12 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cleanupExecutionHistory()
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cleanupRecoveryState()
			}
		}
	}()

	baseVault, err := vault.New(st, masterKey)
	if err != nil {
		log.Fatal(err)
	}
	// accountVault stores the single central Google account credential
	// (shared AEAD/master key with baseVault, separate table -- see
	// internal/vault/central.go); st satisfies vault.AccountStore the
	// same way it already satisfies vault.Store.
	accountVault := baseVault.NewAccountVault(st)
	// envVault backs Global Environment / Service Environment: AEAD-encrypted
	// (same master key) app-level configuration that replaces setting every
	// common variable in the hosting dashboard, with precedence Service >
	// Global > process environment (see engine.RunContext.Env).
	envVault := baseVault.NewEnvVault(st)

	// creds is what every node executor actually calls: it tries a
	// per-node/per-workflow override first (OAuthResolver, unchanged
	// behavior/storage from before this feature), then falls back to
	// the central Google account (AccountResolver) so a single saved
	// account "just works" for every googleSheets/youTube/gmail node
	// across every workflow without per-node setup. Both legs
	// transparently refresh an expiring accessToken before handing it
	// to the node; plain API-key credentials (no refreshToken) pass
	// through unchanged.
	perNodeCreds := vault.NewOAuthResolver(baseVault)
	// Per-node overrides are keyed by workflow id, so they are already
	// isolated per Service. The legacy single central Google account is
	// deliberately NOT chained in here any more (it used to be, via
	// CentralFallbackResolver): that fallback had no notion of which
	// Service a workflow belongs to and would have handed one shared
	// account to every Service. Google nodes now get their account only
	// via googleAccounts below, which is Service-scoped and keeps the
	// legacy account as a fallback for the Default Service alone, so an
	// existing single-Service deployment behaves exactly as before.
	creds := perNodeCreds
	// googleAccounts backs the new n8n-style "Connect with Google" flow:
	// one connected account PER SERVICE (Gmail/YouTube/Sheets), each
	// independently connect/reconnect/disconnect-able, falling back to
	// the legacy single `creds` account above only for a service that
	// was never individually (re)connected -- see
	// vault.GoogleServiceAccounts's doc comment.
	googleAccounts := vault.NewGoogleServiceAccounts(accountVault)

	scratchRoot := envOr("MICROFLOW_SCRATCH_DIR", "/tmp/microflow")
	_ = os.MkdirAll(scratchRoot, 0o700)

	allowedBinaries := map[string]string{
		"ffmpeg":   envOr("MICROFLOW_FFMPEG_PATH", "/usr/bin/ffmpeg"),
		"edge-tts": envOr("MICROFLOW_EDGE_TTS_PATH", "/usr/local/bin/edge-tts"),
		"python3":  envOr("MICROFLOW_PYTHON_PATH", "/usr/bin/python3"),
	}

	// Env vars the workflow's Code nodes are allowed to read via $env
	// (security rule 11/22: never expose the whole process environment,
	// only names an operator has explicitly opted in). The original six
	// are what the sample workflow's Code nodes actually reference (AI
	// provider keys for the multi-model fallback chain, the YouTube
	// Data API key, the app's referer URL for API calls that require
	// one, the failure-notification webhook, and the Google Sheets
	// URL used by Code nodes instead of a hardcoded Spreadsheet ID).
	// The rest are additional AI-provider/API credentials an operator
	// may opt a workflow into (Together, Fireworks, SambaNova,
	// DeepInfra, Hugging Face, Groq, Cerebras, Cloudflare) -- none of
	// these are referenced by the sample workflow itself.
	codeEnvAllowlist := []string{
		"OPENROUTER_API_KEY",
		"GEMINI_API_KEY",
		"YOUTUBE_DATA_API_KEY",
		"APP_REFERER_URL",
		"NOTIFY_WEBHOOK_URL",
		"GOOGLE_SHEETS_URL",
		"GOOGLE_SHEET_ID",
		"YOUTUBE_CATEGORY_ID",
		"TOGETHER_API_KEY",
		"FIREWORKS_API_KEY",
		"SAMBANOVA_API_KEY",
		"DEEPINFRA_API_KEY",
		"HF_TOKEN",
		"GROQ_API_KEY",
		"CEREBRAS_API_KEY",
		"CLOUDFLARE_API_TOKEN",
		"CLOUDFLARE_ACCOUNT_ID",
	}

	// MaxIdleConnsPerHost/MaxIdleConns kept small on purpose: this
	// process only ever runs one workflow with one heavy call in
	// flight at a time (MaxConcurrentHeavy below), so a large idle
	// keep-alive pool just holds buffers for connections that will
	// never be reused concurrently. IdleConnTimeout releases them
	// quickly instead of holding sockets/buffers open between the
	// workflow's ~85s of paced Wait/cooldown gaps.
	nodeHTTPClient := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        4,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     20 * time.Second,
			// DialContext enforces the actual SSRF boundary
			// (resolve-once-then-dial-that-IP) for every
			// httpRequest node call -- see
			// nodes.SafeDialContext's doc comment for why this,
			// not a second hostname check in guardSSRF, is where
			// hostname-based private-address blocking has to
			// live to avoid a DNS-rebinding gap.
			DialContext: nodes.SafeDialContext,
		},
	}

	registry := nodes.DefaultRegistry(nodes.Deps{
		HTTPClient: nodeHTTPClient,
		AllowedBinaries: allowedBinaries,
		EnvAllowlist:    codeEnvAllowlist,
		ScratchRoot:     scratchRoot,
		// CredentialResolver: per-node/per-workflow override only (an
		// explicit credential saved for one specific node) -- Google
		// executors fall back to GoogleAccounts, not this resolver's own
		// central fallback, so each service's connected account can be
		// chosen independently. See internal/nodes/google.go.
		CredentialResolver: perNodeCreds,
		GoogleAccounts:     googleAccounts,
	})
	eng := engine.New(registry)

	// MaxConcurrentHeavy bounds how many FFmpeg/TTS/HTTP calls this run
	// can have in flight at once (engine.go). Defaults to 1 (down from
	// the 512MB-target default of 2): on a ~170MB total budget there is
	// no headroom for two concurrent FFmpeg/python3 child processes
	// alongside the Go heap, so heavy work is fully serialized unless an
	// operator explicitly opts back into 2+ via
	// MICROFLOW_MAX_CONCURRENT_HEAVY.
	eng.MaxConcurrentHeavy = envInt("MICROFLOW_MAX_CONCURRENT_HEAVY", 1)

	// RAM guard: soft ceiling well under the total container budget,
	// leaving room for the Go runtime/OS/FFmpeg+python3+edge-tts child
	// processes, which are separate OS processes and are NOT part of
	// this Go heap (rule 17/19). Default lowered again, 90MB -> 40MB:
	// LOWRAM.md's own measurement put this exact workflow's Go-side
	// live heap at ~17-18MB peak, so 40MB is already ~2x that measured
	// number, not a bare-bones cutoff picked without evidence. FFmpeg's
	// own measured peak (~81-97MB) is the actual dominant cost and it
	// lives outside this Go heap entirely, so shrinking this ceiling
	// further doesn't touch that number -- it only tightens the margin
	// on the piece that was already small. Tune via
	// MICROFLOW_HEAP_CEILING_MB once you've measured real usage on your
	// machine (rule 21: don't claim numbers without measurement) --
	// this is a starting point, not a guarantee, since FFmpeg/python3
	// RSS depends on the workflow's actual resolution/bitrate/scripts.
	memGuard := engine.NewMemGuard(uint64(envInt("MICROFLOW_HEAP_CEILING_MB", 40)) * 1024 * 1024)
	stopGuard := make(chan struct{})
	go memGuard.Start(stopGuard)
	defer close(stopGuard)

	// See engine.RunContext.NodeRunCap's doc comment / LOWRAM.md: this is
	// the single largest measured live-memory reduction available for a
	// large-graph workflow like this one, and it's a real memory freed
	// on every node run past the cap, not just a GC-pacing knob. Default
	// lowered hard from the engine's own 500 to 12 -- this deployment
	// only ever runs one always-on workflow with no need to keep a long
	// in-memory step-by-step history; the terminal status/error and the
	// last few steps (what actually matters for diagnosing a failure)
	// are still always kept.
	// MICROFLOW_EXECUTION_TIMEOUT_MINUTES bounds one full workflow run
	// (rule 22: nothing unbounded). Default raised from the package's
	// old 30-minute default to 180: a large real workflow (AI script
	// generation with retries, TTS, FFmpeg rendering across 100+
	// nodes) can legitimately run well past 30 minutes end to end, and
	// 30 minutes was observed cutting off healthy runs. Note this
	// clock only starts once a run actually acquires a worker slot
	// (see internal/runner.RunFromNode / Manager.runJob) -- time spent
	// queued behind another run under MICROFLOW_MAX_CONCURRENT_EXECUTIONS
	// no longer counts against it.
	run := runner.New(st, st, eng, st, creds, scratchRoot).
		WithRecovery(st).
		WithMemGuard(memGuard).
		WithNodeRunCap(envInt("MICROFLOW_NODE_RUN_CAP", 12)).
		WithEnv(envVault).
		WithTimeout(time.Duration(envInt("MICROFLOW_EXECUTION_TIMEOUT_MINUTES", 180)) * time.Minute)

	// Global Scheduler: the ONLY thing that starts scheduled executions, and
	// the one sequential queue (max concurrency 1) every run path goes
	// through -- due Schedule Triggers, manual Run, webhooks, Run Service /
	// Run All steps and crash recovery. A Schedule Trigger node never starts
	// a run on its own; it only tells this scheduler when its workflow is due.
	sch := scheduler.New(func(ctx context.Context, workflowID, nodeName string) {
		seed := model.NodeOutput{{{JSON: map[string]any{"triggeredAt": time.Now().Format(time.RFC3339)}}}}
		// Scheduler jobs are server-owned work, not HTTP-request work. Do not
		// cancel an already-running scheduled execution merely because the
		// scheduler loop's shutdown context was cancelled; its durable
		// checkpoint must survive a graceful process exit for automatic restart
		// recovery. The process itself is still allowed to exit normally.
		runCtx := context.WithoutCancel(ctx)
		ex, runErr := run.RunFromNode(runCtx, workflowID, nodeName, "schedule", seed)
		if runErr != nil {
			log.Printf("schedule run %s/%s failed: %v", workflowID, nodeName, runErr)
			return
		}
		log.Printf("schedule run %s/%s finished: %s (execution %s)", workflowID, nodeName, ex.Status, ex.ID)
	})
	sch.SetLocation(schedulerLocation())
	// Settling gap after every finished workflow, before the next queued
	// Service starts. SERVICE_COOLDOWN_SECONDS=0 disables it.
	serviceCooldown := time.Duration(envInt("SERVICE_COOLDOWN_SECONDS", 10)) * time.Second
	sch.SetCooldown(serviceCooldown)
	// Runs after EVERY workflow (success, error, cancelled or panic) and before
	// the cooldown: wait for finished runs' scratch directories to be removed,
	// drop idle keep-alive connections, and hand freed heap back to the OS.
	sch.SetCleanup(func(ctx context.Context) {
		run.WaitScratchCleanup(ctx)
		nodeHTTPClient.CloseIdleConnections()
		http.DefaultClient.CloseIdleConnections()
		debug.FreeOSMemory()
	})
	log.Printf("global scheduler: sequential queue, max concurrency 1, service cooldown %s", serviceCooldown)

	// Async execution (spec sections M/N): bounded worker pool on top
	// of the same Runner -- MaxConcurrentExecutions caps simultaneous
	// full workflow runs (independent of, and in addition to,
	// Engine.MaxConcurrentHeavy's per-run FFmpeg/TTS/HTTP cap);
	// MaxQueuedExecutions bounds accepted-but-not-yet-finished runs
	// before POST .../execute starts replying 429 instead of queuing
	// unboundedly (rule 19). Defaults are deliberately conservative --
	// this workflow's heavy nodes (FFmpeg/TTS/image) are memory-hungry
	// per run, so "bounded worker pool" here means small numbers, not a
	// typical web-request worker count. Lowered 5 -> 2: this deployment
	// runs one always-on scheduled workflow, so there is little value
	// in holding more than a couple of extra trigger requests (each
	// holding its own seed-input JSON in memory) while a run that can
	// take up to MICROFLOW_EXECUTION_TIMEOUT_MINUTES is in flight --
	// past that, 429 and let the trigger's own retry/schedule handle it
	// rather than accumulating queued memory.
	execManager := runner.NewManager(run,
		envInt("MICROFLOW_MAX_CONCURRENT_EXECUTIONS", 1),
		envInt("MICROFLOW_MAX_QUEUED_EXECUTIONS", 2),
	).WithDispatcher(sch)
	execManager.StartRecoveryLoop(ctx)

	// Run Service / Run All Services: durable, strictly sequential sweeps
	// through the same Runner. Resume picks up a sweep interrupted by a
	// restart from its last persisted step (no duplicate re-runs).
	runAllManager := runall.NewManager(st, queuedRunner{sch: sch, run: run})
	runAllManager.Resume(ctx)

	// st also satisfies api.CredentialStore (ListCredentials); baseVault
	// (not the OAuthResolver) is passed so per-node credential writes go
	// through the same Put path cmd/setcred uses -- no token refresh
	// needed just to save a credential. accountVault is the central
	// Google account store backing /api/credentials/google. st also
	// satisfies api.ExecutionLoader (GetExecution) -- the durable
	// fallback for GET /api/executions/{id} once execManager evicts a
	// finished execution from memory.
	apiServer := api.New(st, run, st, baseVault, accountVault).WithAsync(execManager, st).WithTenancy(st, envVault, runAllManager).WithReauth(gate.verifyPassword)

	// "Connect with Google" (n8n-style OAuth Authorization Code flow) is
	// always available and fully automatic: GOOGLE_OAUTH_CLIENT_ID /
	// GOOGLE_OAUTH_CLIENT_SECRET / GOOGLE_OAUTH_REDIRECT_URL are resolved
	// on every request, each key independently, from the merged
	// Environment of the Service involved -- Service Environment first,
	// then Global Environment, then the host/Render environment (the same
	// precedence every node uses). So the three values may be split across
	// those places in any combination, and adding or changing one takes
	// effect immediately with no restart. The legacy manual
	// clientId/clientSecret/refreshToken paste (cmd/setcred or the central
	// credentials endpoint) still works as before.
	oauthEnvKeys := []string{"GOOGLE_OAUTH_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_SECRET", "GOOGLE_OAUTH_REDIRECT_URL"}
	apiServer.EnableGoogleOAuthResolver(func(rctx context.Context, serviceID string) (*vault.GoogleOAuthApp, error) {
		serviceEnv, globalEnv, err := envVault.ResolveAll(rctx, serviceID)
		if err != nil {
			// Never fail the connect flow just because the dashboard
			// Environment could not be read: fall back to the host env.
			log.Printf("warning: could not read Environment for google oauth (service=%q): %v", serviceID, err)
			serviceEnv, globalEnv = nil, nil
		}
		// Per key: first non-empty of Service > Global > host.
		get := func(k string) string {
			if v := strings.TrimSpace(serviceEnv[k]); v != "" {
				return v
			}
			if v := strings.TrimSpace(globalEnv[k]); v != "" {
				return v
			}
			return os.Getenv(k)
		}
		if app, ok := vault.GoogleOAuthAppFromEnv(get); ok {
			return app, nil
		}
		var missing []string
		for _, k := range oauthEnvKeys {
			if strings.TrimSpace(get(k)) == "" {
				missing = append(missing, k)
			}
		}
		return nil, fmt.Errorf("google oauth is not configured -- missing %s (set in Service Environment, Global Environment, or host environment)", strings.Join(missing, ", "))
	}, googleAccounts)

	whServer := webhook.NewServer()
	webhookToken := envOr("MICROFLOW_WEBHOOK_TOKEN", "")

	mux := http.NewServeMux()
	mux.Handle("/api/", apiServer.Handler())
	if sub, err := fs.Sub(embeddedFrontend, "static"); err == nil {
		mux.Handle("/", http.FileServer(http.FS(sub)))
	}

	// --- Startup registration pass: scan every saved workflow for
	// Schedule Trigger and Webhook Trigger nodes and wire them to the
	// shared runner. This closes the two integration points that were
	// previously left as logging-only TODOs.
	workflows, err := st.ListWorkflows(ctx)
	if err != nil {
		log.Printf("warning: could not list workflows at startup (%v) -- schedules/webhooks won't be registered until the next restart after this is fixed", err)
		workflows = nil
	}

	var schedules []scheduler.Schedule
	for _, wf := range workflows {
		for name, n := range wf.Nodes {
			switch n.Type {
			case model.TypeScheduleTrigger:
				schedules = append(schedules, schedulesFromNode(wf.ID, name, n, wf.Active)...)
			case model.TypeWebhookTrigger:
				registerWebhookRoute(whServer, webhookToken, run, sch, wf.ID, name, n)
			}
		}
	}
	log.Printf("startup: registered %d schedule(s) across %d workflow(s)", len(schedules), len(workflows))

	sch.Load(schedules)
	go sch.Start(ctx)

	// After every save/import/delete, re-register that workflow's Schedule
	// Trigger nodes from the just-persisted definition (same
	// schedulesFromNode the startup pass uses, so Enabled == wf.Active &&
	// !node.Disabled either way). wf == nil means the workflow was deleted.
	apiServer.WithScheduleSync(func(workflowID string, wf *model.Workflow) {
		var updated []scheduler.Schedule
		if wf != nil {
			for name, n := range wf.Nodes {
				if n != nil && n.Type == model.TypeScheduleTrigger {
					updated = append(updated, schedulesFromNode(workflowID, name, n, wf.Active)...)
				}
			}
		}
		sch.ReplaceWorkflow(workflowID, updated)
		log.Printf("schedule sync: workflow %s now has %d schedule(s) registered", workflowID, len(updated))
	})

	mux.Handle("/webhook/", whServer.Handler())

	addr := envOr("MICROFLOW_ADDR", ":8080")
	srv := &http.Server{Addr: addr, Handler: gate.Wrap(mux), ReadTimeout: 30 * time.Second, WriteTimeout: 35 * time.Minute}

	go func() {
		log.Printf("MicroFlow listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	shutdownManagerCtx, cancelManager := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelManager()
	if err := execManager.Shutdown(shutdownManagerCtx); err != nil {
		log.Printf("execution manager shutdown: %v", err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// schedulerLocation reads MICROFLOW_SCHEDULER_TIMEZONE (an IANA zone name,
// e.g. "Asia/Dhaka") and resolves it via time/tzdata's embedded database, so
// a Schedule Trigger's cron expression is read as that zone's wall-clock
// time (matching what a person typing "0 19 * * *" actually means) rather
// than the server host's zone, which is UTC on most container platforms
// (this repo's own Alpine runtime image included) regardless of where the
// workflow's audience or owner is. Unset or invalid falls back to UTC --
// the scheduler's behavior before this existed -- with a log line so a typo
// doesn't silently mis-schedule every run.
func schedulerLocation() *time.Location {
	name := strings.TrimSpace(os.Getenv("MICROFLOW_SCHEDULER_TIMEZONE"))
	if name == "" {
		return time.UTC
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		log.Printf("warning: MICROFLOW_SCHEDULER_TIMEZONE=%q is not a valid IANA zone name (%v) -- Schedule Trigger cron expressions will be evaluated in UTC instead", name, err)
		return time.UTC
	}
	log.Printf("startup: Schedule Trigger cron expressions will be evaluated in %s", name)
	return loc
}

// schedulesFromNode reads a Schedule Trigger node's n8n-shaped parameters
// (rule interval config under parameters.rule.interval[], each item
// either {field:"cronExpression", expression:"..."} or
// {field:"seconds"/"minutes"/"hours", secondsInterval/...}) and produces
// one scheduler.Schedule per recognised rule -- a node with several
// rules (e.g. a weekday cron plus a separate Friday cron) fires for all
// of them, not just the first. Falls back to a direct "cronExpression"/
// "intervalSeconds" param for simplicity if the workflow used a flatter
// shape. The first schedule keeps the plain "workflow/node" ID; further
// rules get a "#N" suffix so each has its own last-run bookkeeping.
// wfActive mirrors n8n's own semantics: a Schedule Trigger only actually
// fires when its *workflow* is switched Active, regardless of the node's
// own Enabled flag -- both must be true, matching the "Active" toggle in
// the editor toolbar (see cmd/server/static/app.js's #wfActive checkbox).
func schedulesFromNode(workflowID, nodeName string, n *model.Node, wfActive bool) []scheduler.Schedule {
	baseID := workflowID + "/" + nodeName
	var out []scheduler.Schedule
	add := func(sc scheduler.Schedule) {
		sc.WorkflowID = workflowID
		sc.NodeName = nodeName
		sc.Enabled = wfActive && !n.Disabled
		sc.ID = baseID
		if len(out) > 0 {
			sc.ID = fmt.Sprintf("%s#%d", baseID, len(out)+1)
		}
		if sc.CronExpr == "" && sc.IntervalSeconds > 0 && sc.IntervalSeconds < 60 {
			log.Printf("warning: Schedule Trigger node %q in workflow %q has an interval under 60s -- the scheduler has one-minute resolution, so it will run at most once a minute", nodeName, workflowID)
		}
		out = append(out, sc)
	}
	addCron := func(expr string) {
		if !scheduler.CronValid(expr) {
			log.Printf("warning: Schedule Trigger node %q in workflow %q has an invalid cron expression %q (need 5 fields: minute hour day month weekday) -- that rule will never fire", nodeName, workflowID, expr)
			return
		}
		add(scheduler.Schedule{CronExpr: strings.TrimSpace(expr)})
	}

	if cronExpr, ok := n.Parameters["cronExpression"].(string); ok && cronExpr != "" {
		addCron(cronExpr)
		return out
	}
	if secs, ok := n.Parameters["intervalSeconds"].(float64); ok && secs > 0 {
		add(scheduler.Schedule{IntervalSeconds: int(secs)})
		return out
	}
	// n8n's actual Schedule Trigger shape: parameters.rule.interval is an
	// array of interval objects; every one MicroFlow understands is
	// registered.
	if rule, ok := n.Parameters["rule"].(map[string]any); ok {
		if intervals, ok := rule["interval"].([]any); ok {
			for _, raw := range intervals {
				item, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				field, _ := item["field"].(string)
				switch field {
				case "cronExpression":
					if expr, ok := item["expression"].(string); ok {
						addCron(expr)
					}
				case "seconds":
					if v := intOr(item["secondsInterval"], 1); v > 0 {
						add(scheduler.Schedule{IntervalSeconds: v})
					}
				case "minutes":
					if v := intOr(item["minutesInterval"], 1); v > 0 {
						add(scheduler.Schedule{IntervalSeconds: v * 60})
					}
				case "hours":
					if v := intOr(item["hoursInterval"], 1); v > 0 {
						add(scheduler.Schedule{IntervalSeconds: v * 3600})
					}
				}
			}
		}
	}
	if len(out) == 0 {
		log.Printf("warning: Schedule Trigger node %q in workflow %q has no recognized interval config -- it will never fire; check its parameters", nodeName, workflowID)
	}
	return out
}

func intOr(v any, def int) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return def
}

// queuedRunner adapts *runner.Runner to runall.WorkflowRunner so every Run
// Service / Run All step is submitted to the global scheduler's queue and runs
// only when it reaches the front (max concurrency 1, cleanup + cooldown between
// steps). runall itself is unchanged: it still calls RunFromNode per workflow.
type queuedRunner struct {
	sch *scheduler.Scheduler
	run *runner.Runner
}

func (q queuedRunner) RunFromNode(ctx context.Context, workflowID, startNode, mode string, seed model.NodeOutput) (*model.Execution, error) {
	var ex *model.Execution
	var runErr error
	if err := q.sch.RunSync(ctx, workflowID, mode, func(jobCtx context.Context) {
		ex, runErr = q.run.RunFromNode(jobCtx, workflowID, startNode, mode, seed)
	}); err != nil {
		return nil, fmt.Errorf("workflow %q was not run: %w", workflowID, err)
	}
	if ex == nil && runErr == nil {
		runErr = fmt.Errorf("workflow %q run ended without a result", workflowID)
	}
	return ex, runErr
}

// registerWebhookRoute wires a Webhook Trigger node's configured path to
// the runner. Path defaults to /webhook/<workflowId>/<nodeName> if the
// node didn't set an explicit "path" parameter.
func registerWebhookRoute(whServer *webhook.Server, token string, run *runner.Runner, sch *scheduler.Scheduler, workflowID, nodeName string, n *model.Node) {
	path := n.ParamString("path", "")
	if path == "" {
		path = fmt.Sprintf("/webhook/%s/%s", workflowID, nodeName)
	} else if !strings.HasPrefix(path, "/webhook/") {
		path = "/webhook/" + strings.TrimPrefix(path, "/")
	}

	whServer.Register(path, token, func(headers, query map[string]string, body map[string]any) (int, any) {
		seedJSON := map[string]any{
			"headers": headers,
			"query":   query,
			"body":    body,
		}
		seed := model.NodeOutput{{{JSON: seedJSON}}}
		// Webhook runs go through the same global queue as everything else
		// (max concurrency 1, cleanup + cooldown between runs); the request
		// waits for its turn and for the run to finish, as it did before.
		var ex *model.Execution
		var runErr error
		if qErr := sch.RunSync(context.Background(), workflowID, "webhook", func(jobCtx context.Context) {
			ex, runErr = run.RunFromNode(jobCtx, workflowID, nodeName, "webhook", seed)
		}); qErr != nil {
			if errors.Is(qErr, scheduler.ErrDuplicate) {
				return http.StatusConflict, map[string]string{"error": qErr.Error()}
			}
			return http.StatusInternalServerError, map[string]string{"error": qErr.Error()}
		}
		execID := ""
		if ex != nil {
			execID = ex.ID
		}
		if runErr != nil {
			return http.StatusInternalServerError, map[string]string{"error": runErr.Error(), "executionId": execID}
		}
		if ex == nil {
			return http.StatusInternalServerError, map[string]string{"error": "workflow run ended without a result"}
		}
		return http.StatusOK, map[string]string{"status": string(ex.Status), "executionId": execID}
	})
	log.Printf("registered webhook route %s -> %s/%s", path, workflowID, nodeName)
}

// configureGoRuntimeForLowRAM is the fix for the biggest gap in the
// previous draft: engine.MemGuard polled runtime.MemStats every 5s and
// computed ShouldThrottle() correctly, but nothing actually told the Go
// *garbage collector* about the 512MB budget -- Go's default GC policy
// (GOGC=100, no memory limit) lets the heap roughly double between
// collections, which is fine on a normal server and a bad idea in a
// 512MB container running FFmpeg/TTS child processes alongside it.
//
// debug.SetMemoryLimit (Go 1.19+) is the runtime's own answer to this:
// it makes the GC work harder as heap usage approaches the limit,
// instead of only reacting after the fact via ShouldThrottle()'s 5s
// poll. The two mechanisms are complementary: SetMemoryLimit keeps the
// Go heap itself smaller; MemGuard/WaitIfThrottled additionally delays
// *new* FFmpeg/TTS processes and JS VMs (which SetMemoryLimit alone
// cannot do, since child-process RSS and goja VMs aren't Go heap).
//
// Respects an operator-set GOMEMLIMIT/GOGC env var if present (Go's
// runtime already applies those before main() runs) rather than
// silently overriding an explicit choice.
func configureGoRuntimeForLowRAM() {
	if os.Getenv("GOMEMLIMIT") == "" {
		ceilingMB := envInt("MICROFLOW_HEAP_CEILING_MB", 40)
		// Smaller headroom than the previous pass (was +30MB): LOWRAM.md's
		// own measurement put this workflow's actual Go-side live heap at
		// ~17-18MB peak, so +15MB above the soft ceiling is already ~3x
		// that measured peak. Still enough that SetMemoryLimit (closer to
		// a hard cap for GC pacing) doesn't fight MemGuard's own throttle
		// point, without reserving RAM this process has never been shown
		// to need. Re-measure if a bigger/different workflow is imported.
		debug.SetMemoryLimit(int64(ceilingMB+15) * 1024 * 1024)
	}
	if os.Getenv("GOGC") == "" {
		// Lower than the previous pass's 30: trades more CPU for GC for a
		// smaller average heap. Worth it here because the tradeoff is
		// against a single always-on workflow's total RAM budget, not
		// against latency-sensitive request throughput -- this process
		// has no concurrent traffic to slow down. SetMemoryLimit above is
		// the real backstop; this just makes the GC work harder before
		// ever getting close to it.
		debug.SetGCPercent(15)
	}
	if os.Getenv("GOMAXPROCS") == "" {
		// This deployment runs one workflow at a time
		// (MaxConcurrentExecutions=1, MaxConcurrentHeavy=1): there is no
		// real parallel work for extra Ps to do, and each additional P
		// costs its own mcache/mspan bookkeeping. Pin to 1 by default so
		// Go doesn't size its scheduler for however many (possibly
		// fractional, cgroup-limited) CPUs the host reports. Set
		// GOMAXPROCS explicitly in the environment to override.
		runtime.GOMAXPROCS(1)
	}
	// Disable memory-profiling sample bookkeeping -- this process is not
	// profiled in production, and MemProfileRate>0 (the 512KB-interval
	// default) keeps a background sampling table alive for no benefit
	// here.
	runtime.MemProfileRate = 0
}

func requireEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("%s is required", k)
	}
	return v
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return def
	}
	return n
}
