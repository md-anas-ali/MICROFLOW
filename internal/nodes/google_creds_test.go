package nodes

import (
	"context"
	"errors"
	"strings"
	"testing"

	"microflow/internal/vault"
)

// fakeFailingResolver always fails Resolve -- simulates "no per-node
// override saved for this node", which is the normal case: most nodes
// rely entirely on the connected service account, not a per-node
// override.
type fakeFailingResolver struct{ err error }

func (f fakeFailingResolver) Resolve(ctx context.Context, workflowID, logicalName string) (map[string]string, error) {
	return nil, f.err
}

type fakeGoogleAccountResolver struct {
	secrets map[string]string
	err     error
}

func (f fakeGoogleAccountResolver) Resolve(ctx context.Context, service string) (map[string]string, error) {
	return f.secrets, f.err
}

func TestResolveGoogleCredsPerNodeOverrideWins(t *testing.T) {
	override := map[string]string{"accessToken": "override-token"}
	creds := fakeSucceedingResolver{secrets: override}
	accounts := fakeGoogleAccountResolver{secrets: map[string]string{"accessToken": "account-token"}}

	got, err := resolveGoogleCreds(context.Background(), creds, accounts, "gmail", "wf1", "Send Email")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["accessToken"] != "override-token" {
		t.Fatalf("expected per-node override to take priority, got %v", got)
	}
}

func TestResolveGoogleCredsFallsBackToAccount(t *testing.T) {
	creds := fakeFailingResolver{err: errors.New("no per-node override")}
	accounts := fakeGoogleAccountResolver{secrets: map[string]string{"accessToken": "account-token"}}

	got, err := resolveGoogleCreds(context.Background(), creds, accounts, "gmail", "wf1", "Send Email")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["accessToken"] != "account-token" {
		t.Fatalf("expected fallback to the connected account, got %v", got)
	}
}

func TestResolveGoogleCredsNilAccountsPropagatesOriginalError(t *testing.T) {
	wantErr := errors.New("no per-node override")
	creds := fakeFailingResolver{err: wantErr}

	_, err := resolveGoogleCreds(context.Background(), creds, nil, "gmail", "wf1", "Send Email")
	if err != wantErr {
		t.Fatalf("expected the original error when Accounts is nil, got %v", err)
	}
}

func TestResolveGoogleCredsTranslatesInvalidGrant(t *testing.T) {
	creds := fakeFailingResolver{err: errors.New("no per-node override")}
	accounts := fakeGoogleAccountResolver{err: vault.ErrGoogleReauthRequired}

	_, err := resolveGoogleCreds(context.Background(), creds, accounts, "gmail", "wf1", "Send Email")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "reconnect") {
		t.Fatalf("expected a user-facing reconnect message, got raw error: %v", err)
	}
	if strings.Contains(err.Error(), "invalid_grant") {
		t.Fatalf("must not leak Google's raw invalid_grant error to the user: %v", err)
	}
}

type fakeSucceedingResolver struct{ secrets map[string]string }

func (f fakeSucceedingResolver) Resolve(ctx context.Context, workflowID, logicalName string) (map[string]string, error) {
	return f.secrets, nil
}
