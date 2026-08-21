// Command setcred is a one-shot operator CLI that provisions a single
// Google OAuth2 credential (clientId/clientSecret/refreshToken) into
// MicroFlow's vault for EVERY node in a saved workflow that actually
// needs Google credentials (googleSheets/youTube/gmail node types).
//
// This exists because the vault package (internal/vault) intentionally
// has no HTTP endpoint for writing credentials -- see vault.go's doc
// comment and STATUS.md: the initial OAuth consent flow is out of
// scope, and an operator is expected to `vault.Put` credentials once,
// out of band. This CLI is that one-off step, made safe (no secrets on
// the process list or in shell history) and workflow-aware (no need to
// enumerate node names by hand).
//
// Usage:
//
//	cd backend
//	export DATABASE_URL="postgres://...?sslmode=require"
//	export MICROFLOW_MASTER_KEY="..."          # same key the server uses
//	export MICROFLOW_CLIENT_SECRET="..."        # or omit for a hidden prompt
//	export MICROFLOW_REFRESH_TOKEN="..."        # or omit for a hidden prompt
//	go run ./cmd/setcred \
//	    --workflow=S53CVyHpCTGmCjy3 \
//	    --client-id=1234567890-abc.apps.googleusercontent.com
//
// Does NOT touch vault.go's encryption or storage format -- it only
// calls the existing *vault.Vault.Put method, the same one the server
// itself would call, once per detected node name.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"microflow/internal/model"
	"microflow/internal/store"
	"microflow/internal/vault"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "setcred: "+err.Error())
		os.Exit(1)
	}
}

func run() error {
	var (
		workflowID   = flag.String("workflow", "", "Workflow ID to provision credentials for (required)")
		clientID     = flag.String("client-id", "", "Google OAuth client ID (or set MICROFLOW_CLIENT_ID)")
		clientSecret = flag.String("client-secret", "", "Google OAuth client secret (or set MICROFLOW_CLIENT_SECRET; omit both for a hidden prompt)")
		refreshToken = flag.String("refresh-token", "", "Google OAuth refresh token (or set MICROFLOW_REFRESH_TOKEN; omit both for a hidden prompt)")
		dryRun       = flag.Bool("dry-run", false, "List which nodes WOULD be provisioned, without writing anything")
	)
	flag.Parse()

	if *workflowID == "" {
		flag.Usage()
		return fmt.Errorf("--workflow is required")
	}

	if *clientID == "" {
		*clientID = os.Getenv("MICROFLOW_CLIENT_ID")
	}
	if *clientSecret == "" {
		*clientSecret = os.Getenv("MICROFLOW_CLIENT_SECRET")
	}
	if *refreshToken == "" {
		*refreshToken = os.Getenv("MICROFLOW_REFRESH_TOKEN")
	}

	if *clientID == "" {
		return fmt.Errorf("--client-id (or MICROFLOW_CLIENT_ID) is required")
	}
	var err error
	if *clientSecret == "" {
		*clientSecret, err = promptHidden("Google client secret: ")
		if err != nil {
			return fmt.Errorf("reading client secret: %w", err)
		}
	}
	if *refreshToken == "" {
		*refreshToken, err = promptHidden("Google refresh token: ")
		if err != nil {
			return fmt.Errorf("reading refresh token: %w", err)
		}
	}
	if *clientSecret == "" || *refreshToken == "" {
		return fmt.Errorf("client secret and refresh token must not be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL is not set")
	}
	masterKey, err := vault.MasterKeyFromEnv()
	if err != nil {
		return err
	}

	st, err := store.Open(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("db connect: %w", err)
	}
	defer st.Close()

	wf, err := st.LoadWorkflow(ctx, *workflowID)
	if err != nil {
		return fmt.Errorf("loading workflow %q: %w", *workflowID, err)
	}

	nodeNames := googleNodeNames(wf)
	if len(nodeNames) == 0 {
		return fmt.Errorf("workflow %q has no googleSheets/youTube/gmail nodes -- nothing to provision", *workflowID)
	}

	fmt.Printf("Workflow %q (%s) -- found %d node(s) needing Google credentials:\n", wf.Name, wf.ID, len(nodeNames))
	for _, name := range nodeNames {
		fmt.Printf("  - %s\n", name)
	}

	if *dryRun {
		fmt.Println("\n--dry-run: nothing written.")
		return nil
	}

	v, err := vault.New(st, masterKey)
	if err != nil {
		return fmt.Errorf("opening vault: %w", err)
	}

	// expiresAt left unset (""): OAuthResolver treats a missing/unparseable
	// expiresAt as "needs refresh", so the very first Resolve() call will
	// immediately refresh and obtain a real accessToken/expiresAt -- no
	// need for this CLI to call Google itself just to seed one.
	secrets := map[string]string{
		"clientId":     *clientID,
		"clientSecret": *clientSecret,
		"refreshToken": *refreshToken,
		"tokenType":    "Bearer",
	}

	fmt.Println()
	for _, name := range nodeNames {
		if err := v.Put(ctx, wf.ID, name, secrets); err != nil {
			return fmt.Errorf("saving credential for node %q: %w", name, err)
		}
		fmt.Printf("saved: %s\n", name)
	}

	fmt.Printf("\nDone -- %d node(s) provisioned with one shared Google OAuth credential.\n", len(nodeNames))
	return nil
}

// googleNodeNames returns the sorted list of node names in wf whose type
// requires a Google OAuth credential (Sheets/YouTube/Gmail), i.e. every
// name internal/nodes/google.go will call Creds.Resolve(workflowID, name)
// for at runtime.
func googleNodeNames(wf *model.Workflow) []string {
	var names []string
	for _, n := range wf.Nodes {
		switch n.Type {
		case model.TypeGoogleSheets, model.TypeYouTube, model.TypeGmail:
			names = append(names, n.Name)
		}
	}
	sort.Strings(names)
	return names
}

// stdinReader is shared across every promptHidden call. A fresh
// bufio.Reader per call would each read-ahead and buffer whatever is
// currently available on stdin, so a second prompt could lose input
// the first call had already buffered but not consumed (this bit us
// when piping "secret\ntoken\n" in for testing -- the token line
// vanished). One shared reader avoids that.
var stdinReader = bufio.NewReader(os.Stdin)

// promptHidden reads a line from stdin without echoing it to the
// terminal, using only the standard library (raw termios flag toggling
// via syscall on Linux/macOS -- no golang.org/x/term dependency). If
// stdin isn't a real terminal (e.g. piped input, or an unsupported
// OS/ioctl failure), it falls back to a normal visible read so the CLI
// still works non-interactively (e.g. in scripts feeding a heredoc).
func promptHidden(label string) (string, error) {
	fmt.Fprint(os.Stderr, label)
	defer fmt.Fprintln(os.Stderr)

	if restore, ok := disableEcho(os.Stdin.Fd()); ok {
		defer restore()
	}

	line, err := stdinReader.ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}
