package main

import (
	"context"
	"fmt"
	"sync"
)

// fakeStaticData is an in-memory StaticDataStore (mirrors engine's own
// test double), standing in for the Postgres row-lock implementation.
type fakeStaticData struct {
	mu   sync.Mutex
	data map[string]map[string]any
}

func newFakeStaticData() *fakeStaticData {
	return &fakeStaticData{data: map[string]map[string]any{}}
}

func (f *fakeStaticData) WithLock(_ context.Context, workflowID string, fn func(map[string]any) (map[string]any, error)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.data[workflowID]
	if d == nil {
		d = map[string]any{}
	}
	newD, err := fn(d)
	if err != nil {
		return err
	}
	f.data[workflowID] = newD
	return nil
}

// fakeCredentialResolver simulates "no per-node n8n credential attached"
// -- true for every node in this export (see verification report) -- so
// GoogleSheets/YouTube/Gmail executors fall through to GoogleAccounts.
type fakeCredentialResolver struct{}

func (fakeCredentialResolver) Resolve(_ context.Context, _, _ string) (map[string]string, error) {
	return nil, fmt.Errorf("no credential configured for this node")
}

// fakeGoogleAccounts simulates a central "log in once" Google connection
// being configured, so the Sheets/YouTube/Gmail executors proceed far
// enough to build and send a request (which mockTransport then answers).
type fakeGoogleAccounts struct{}

func (fakeGoogleAccounts) Resolve(_ context.Context, service string) (map[string]string, error) {
	return map[string]string{"accessToken": "synthetic-test-token-" + service}, nil
}
