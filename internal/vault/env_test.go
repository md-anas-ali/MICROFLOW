package vault

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"testing"
)

type fakeEnvStore struct {
	global  map[string][]byte
	service map[string]map[string][]byte
}

func (f *fakeEnvStore) PutGlobalEnv(_ context.Context, k string, ct []byte, _ bool) error {
	f.global[k] = ct
	return nil
}
func (f *fakeEnvStore) DeleteGlobalEnv(_ context.Context, k string) error {
	delete(f.global, k)
	return nil
}
func (f *fakeEnvStore) AllGlobalEnvCiphertext(context.Context) (map[string][]byte, error) {
	return f.global, nil
}
func (f *fakeEnvStore) PutServiceEnv(_ context.Context, sid, k string, ct []byte, _ bool) error {
	if f.service[sid] == nil {
		f.service[sid] = map[string][]byte{}
	}
	f.service[sid][k] = ct
	return nil
}
func (f *fakeEnvStore) DeleteServiceEnv(_ context.Context, sid, k string) error {
	delete(f.service[sid], k)
	return nil
}
func (f *fakeEnvStore) AllServiceEnvCiphertext(_ context.Context, sid string) (map[string][]byte, error) {
	return f.service[sid], nil
}

// Stub satisfying vault.Store is not needed: we only need the AEAD, so
// build a Vault via New with a nil store (New does not touch the store).
func newTestEnvVault(t *testing.T) (*EnvVault, *fakeEnvStore) {
	t.Helper()
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	v, err := New(nil, base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	fs := &fakeEnvStore{global: map[string][]byte{}, service: map[string]map[string][]byte{}}
	return v.NewEnvVault(fs), fs
}

func TestEnvVaultEncryptsAndIsolatesServices(t *testing.T) {
	ev, fs := newTestEnvVault(t)
	ctx := context.Background()
	if err := ev.PutGlobal(ctx, "API_KEY", "global-secret", true); err != nil {
		t.Fatal(err)
	}
	if err := ev.PutService(ctx, "svc-a", "SHEET", "sheet-a", true); err != nil {
		t.Fatal(err)
	}
	if string(fs.global["API_KEY"]) == "global-secret" {
		t.Fatal("global value stored in plaintext")
	}
	a, g, err := ev.ResolveAll(ctx, "svc-a")
	if err != nil || a["SHEET"] != "sheet-a" || g["API_KEY"] != "global-secret" {
		t.Fatalf("svc-a resolve wrong: %v %v %v", a, g, err)
	}
	b, _, err := ev.ResolveAll(ctx, "svc-b")
	if err != nil || len(b) != 0 {
		t.Fatalf("svc-b must not see svc-a's env: %v %v", b, err)
	}
}
