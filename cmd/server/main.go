// MicroFlow server entrypoint.
//
// UNVERIFIED IN SANDBOX: this file has never been compiled or run --
// this environment has no Go toolchain and no network access to fetch
// dependencies (see STATUS.md). Build and run it on your own machine:
//
//	cd backend
//	go mod tidy
//	export DATABASE_URL="postgres://user:pass@host/db?sslmode=require"
//	export MICROFLOW_MASTER_KEY="$(go run ./cmd/server genkey)"
//	go run ./cmd/server
package main

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"microflow/internal/api"
	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/nodes"
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

	baseVault, err := vault.New(st, masterKey)
	if err != nil {
		log.Fatal(err)
	}
	// accountVault stores the single central Google account credential
	// (shared AEAD/master key with baseVault, separate table -- see
	// internal/vault/central.go); st satisfies vault.AccountStore the
	// same way it already satisfies vault.Store.
	accountVault := baseVault.NewAccountVault(st)

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
	accountCreds := vault.NewAccountResolver(accountVault, vault.CentralGoogleAccount)
	creds := vault.NewCentralFallbackResolver(perNodeCreds, accountCreds)
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
	// only names an operator has explicitly opted in). These five are
	// what the sample workflow's Code nodes actually reference (AI
	// provider keys for the multi-model fallback chain, the YouTube
	// Data API key, the app's referer URL for API calls that require
	// one, and the failure-notification webhook).
	codeEnvAllowlist := []string{
		"OPENROUTER_API_KEY",
		"GEMINI_API_KEY",
		"YOUTUBE_DATA_API_KEY",
		"APP_REFERER_URL",
		"NOTIFY_WEBHOOK_URL",
	}

	registry := nodes.DefaultRegistry(nodes.Deps{
		HTTPClient:      &http.Client{Timeout: 60 * time.Second},
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

	// RAM guard: soft ceiling well under the 512MB total, leaving room
	// for the Go runtime/frontend assets/OS/FFmpeg-TTS child processes
	// (rule 17/19). Default 220MB heap ceiling targets the "~200MB free"
	// ideal from the spec; tune via MICROFLOW_HEAP_CEILING_MB once you've
	// measured real usage on your machine (rule 21: don't claim numbers
	// without measurement).
	memGuard := engine.NewMemGuard(uint64(envInt("MICROFLOW_HEAP_CEILING_MB", 220)) * 1024 * 1024)
	stopGuard := make(chan struct{})
	go memGuard.Start(stopGuard)
	defer close(stopGuard)

	run := runner.New(st, st, eng, st, creds, scratchRoot).WithMemGuard(memGuard)

	// Async execution (spec sections M/N): bounded worker pool on top
	// of the same Runner -- MaxConcurrentExecutions caps simultaneous
	// full workflow runs (independent of, and in addition to,
	// Engine.MaxConcurrentHeavy's per-run FFmpeg/TTS/HTTP cap);
	// MaxQueuedExecutions bounds accepted-but-not-yet-finished runs
	// before POST .../execute starts replying 429 instead of queuing
	// unboundedly (rule 19). Defaults are deliberately conservative for
	// a 512MB target -- this workflow's heavy nodes (FFmpeg/TTS/image)
	// are memory-hungry per run, so "bounded worker pool" here means
	// small numbers, not a typical web-request worker count.
	execManager := runner.NewManager(run,
		envInt("MICROFLOW_MAX_CONCURRENT_EXECUTIONS", 1),
		envInt("MICROFLOW_MAX_QUEUED_EXECUTIONS", 5),
	)

	// st also satisfies api.CredentialStore (ListCredentials); baseVault
	// (not the OAuthResolver) is passed so per-node credential writes go
	// through the same Put path cmd/setcred uses -- no token refresh
	// needed just to save a credential. accountVault is the central
	// Google account store backing /api/credentials/google. st also
	// satisfies api.ExecutionLoader (GetExecution) -- the durable
	// fallback for GET /api/executions/{id} once execManager evicts a
	// finished execution from memory.
	apiServer := api.New(st, run, st, baseVault, accountVault).WithAsync(execManager, st)

	// "Connect with Google" (n8n-style OAuth Authorization Code flow)
	// only turns on if a Google Cloud OAuth client is configured -- see
	// the GOOGLE_OAUTH_CLIENT_ID/SECRET/REDIRECT_URL doc comment in
	// vault.GoogleOAuthAppFromEnv. Without it, the server still runs
	// fine; only the "Connect Google" buttons are unavailable, and the
	// legacy manual clientId/clientSecret/refreshToken paste (cmd/setcred
	// or the central credentials endpoint) still works as a fallback.
	if oauthApp, ok := vault.GoogleOAuthAppFromEnv(os.Getenv); ok {
		apiServer.EnableGoogleOAuth(oauthApp, googleAccounts)
		log.Printf("Google OAuth configured -- \"Connect with Google\" is enabled for Gmail/YouTube/Sheets")
	} else {
		log.Printf("warning: GOOGLE_OAUTH_CLIENT_ID/GOOGLE_OAUTH_CLIENT_SECRET/GOOGLE_OAUTH_REDIRECT_URL not fully set -- " +
			"\"Connect with Google\" buttons are disabled; existing manual credential paste still works")
	}

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
				schedules = append(schedules, scheduleFromNode(wf.ID, name, n))
			case model.TypeWebhookTrigger:
				registerWebhookRoute(whServer, webhookToken, run, wf.ID, name, n)
			}
		}
	}
	log.Printf("startup: registered %d schedule(s) across %d workflow(s)", len(schedules), len(workflows))

	sch := scheduler.New(func(ctx context.Context, workflowID, nodeName string) {
		seed := model.NodeOutput{{{JSON: map[string]any{"triggeredAt": time.Now().Format(time.RFC3339)}}}}
		ex, runErr := run.RunFromNode(ctx, workflowID, nodeName, "schedule", seed)
		if runErr != nil {
			log.Printf("schedule run %s/%s failed: %v", workflowID, nodeName, runErr)
			return
		}
		log.Printf("schedule run %s/%s finished: %s (execution %s)", workflowID, nodeName, ex.Status, ex.ID)
	})
	sch.Load(schedules)
	go sch.Start(ctx)

	mux.Handle("/webhook/", whServer.Handler())

	addr := envOr("MICROFLOW_ADDR", ":8080")
	srv := &http.Server{Addr: addr, Handler: mux, ReadTimeout: 30 * time.Second, WriteTimeout: 35 * time.Minute}

	go func() {
		log.Printf("MicroFlow listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// scheduleFromNode reads a Schedule Trigger node's n8n-shaped parameters
// (rule interval config under parameters.rule.interval[], each item
// either {field:"cronExpression", expression:"..."} or
// {field:"seconds"/"minutes"/"hours", secondsInterval/...}) and produces
// a scheduler.Schedule. Falls back to a direct "cronExpression"/
// "intervalSeconds" param for simplicity if the workflow used a flatter
// shape.
func scheduleFromNode(workflowID, nodeName string, n *model.Node) scheduler.Schedule {
	sc := scheduler.Schedule{
		ID:         workflowID + "/" + nodeName,
		WorkflowID: workflowID,
		NodeName:   nodeName,
		Enabled:    !n.Disabled,
	}
	if cronExpr, ok := n.Parameters["cronExpression"].(string); ok && cronExpr != "" {
		sc.CronExpr = cronExpr
		return sc
	}
	if secs, ok := n.Parameters["intervalSeconds"].(float64); ok && secs > 0 {
		sc.IntervalSeconds = int(secs)
		return sc
	}
	// n8n's actual Schedule Trigger shape: parameters.rule.interval is an
	// array of interval objects; we take the first one MicroFlow
	// understands.
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
						sc.CronExpr = expr
						return sc
					}
				case "seconds":
					sc.IntervalSeconds = intOr(item["secondsInterval"], 1)
					return sc
				case "minutes":
					sc.IntervalSeconds = intOr(item["minutesInterval"], 1) * 60
					return sc
				case "hours":
					sc.IntervalSeconds = intOr(item["hoursInterval"], 1) * 3600
					return sc
				}
			}
		}
	}
	log.Printf("warning: Schedule Trigger node %q in workflow %q has no recognized interval config -- it will never fire; check its parameters", nodeName, workflowID)
	return sc
}

func intOr(v any, def int) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return def
}

// registerWebhookRoute wires a Webhook Trigger node's configured path to
// the runner. Path defaults to /webhook/<workflowId>/<nodeName> if the
// node didn't set an explicit "path" parameter.
func registerWebhookRoute(whServer *webhook.Server, token string, run *runner.Runner, workflowID, nodeName string, n *model.Node) {
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
		ex, runErr := run.RunFromNode(context.Background(), workflowID, nodeName, "webhook", seed)
		if runErr != nil {
			return http.StatusInternalServerError, map[string]string{"error": runErr.Error(), "executionId": ex.ID}
		}
		return http.StatusOK, map[string]string{"status": string(ex.Status), "executionId": ex.ID}
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
		ceilingMB := envInt("MICROFLOW_HEAP_CEILING_MB", 220)
		// A little headroom above the soft ceiling MemGuard throttles at,
		// so SetMemoryLimit (which the runtime treats as closer to a hard
		// cap for GC pacing purposes) doesn't fight MemGuard's own 220MB
		// throttle point.
		debug.SetMemoryLimit(int64(ceilingMB+30) * 1024 * 1024)
	}
	if os.Getenv("GOGC") == "" {
		// Lower than the default 100: trades a bit of extra CPU for GC
		// for a smaller average heap, which matters more than CPU on a
		// 512MB box running FFmpeg/TTS alongside the Go process.
		debug.SetGCPercent(50)
	}
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
