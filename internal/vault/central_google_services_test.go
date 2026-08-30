package vault

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// fakeAccountStoreForTest is a minimal in-memory vault.AccountStore,
// local to this test file so internal/vault's own tests don't need to
// depend on internal/api's fake of the same shape.
type fakeAccountStoreForTest struct {
	rows      map[string][]byte
	updatedAt map[string]time.Time
}

func newFakeAccountStoreForTest() *fakeAccountStoreForTest {
	return &fakeAccountStoreForTest{rows: map[string][]byte{}, updatedAt: map[string]time.Time{}}
}

func (f *fakeAccountStoreForTest) GetEncryptedAccount(ctx context.Context, account string) ([]byte, error) {
	ct, ok := f.rows[account]
	if !ok {
		return nil, errFakeNotFound
	}
	return ct, nil
}

func (f *fakeAccountStoreForTest) PutEncryptedAccount(ctx context.Context, account string, ciphertext []byte) error {
	f.rows[account] = ciphertext
	f.updatedAt[account] = time.Now()
	return nil
}

func (f *fakeAccountStoreForTest) DeleteEncryptedAccount(ctx context.Context, account string) error {
	delete(f.rows, account)
	delete(f.updatedAt, account)
	return nil
}

func (f *fakeAccountStoreForTest) AccountCredentialMeta(ctx context.Context, account string) (time.Time, bool, error) {
	t, ok := f.updatedAt[account]
	return t, ok, nil
}

type fakeNotFoundErr string

func (e fakeNotFoundErr) Error() string { return string(e) }

const errFakeNotFound = fakeNotFoundErr("not found")

const testGoogleServicesMasterKey = "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=" // 32 raw bytes, base64

func newTestGoogleServiceAccounts(t *testing.T) *GoogleServiceAccounts {
	t.Helper()
	v, err := New(nil, testGoogleServicesMasterKey) // Vault.store is unused by NewAccountVault's callers here
	if err != nil {
		t.Fatalf("vault.New: %v", err)
	}
	av := v.NewAccountVault(newFakeAccountStoreForTest())
	return NewGoogleServiceAccounts(av)
}

// farFutureExpiry avoids these pure-logic tests accidentally triggering
// a real refresh HTTP call to Google (which would fail/hang in a
// sandboxed or offline test environment) -- refreshEngine only refreshes
// when expiresAt is empty/near/past, so every test secret map here
// carries a comfortably-future expiresAt.
func farFutureExpiry() string {
	return strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 10)
}

func TestGoogleServiceAccountsIndependentPerService(t *testing.T) {
	g := newTestGoogleServiceAccounts(t)
	ctx := context.Background()

	if err := g.Put(ctx, "gmail", map[string]string{"email": "a@example.com", "refreshToken": "rt-a", "clientId": "cid", "clientSecret": "secret", "expiresAt": farFutureExpiry()}); err != nil {
		t.Fatalf("Put gmail: %v", err)
	}
	if err := g.Put(ctx, "youtube", map[string]string{"email": "b@example.com", "refreshToken": "rt-b", "clientId": "cid", "clientSecret": "secret", "expiresAt": farFutureExpiry()}); err != nil {
		t.Fatalf("Put youtube: %v", err)
	}

	// Sheets was never connected -- and there's no legacy row either --
	// so Status must report not connected, and Resolve must fail.
	_, _, connected, _, err := g.Status(ctx, "sheets")
	if err != nil {
		t.Fatalf("Status sheets: %v", err)
	}
	if connected {
		t.Fatal("sheets should not be connected")
	}
	if _, err := g.Resolve(ctx, "sheets"); err == nil {
		t.Fatal("expected Resolve(sheets) to fail with no row and no legacy fallback")
	}

	// Disconnecting gmail must not affect youtube.
	if err := g.Disconnect(ctx, "gmail"); err != nil {
		t.Fatalf("Disconnect gmail: %v", err)
	}
	_, _, gmailConnected, _, _ := g.Status(ctx, "gmail")
	if gmailConnected {
		t.Fatal("gmail should be disconnected")
	}
	ytSecrets, err := g.Resolve(ctx, "youtube")
	if err != nil {
		t.Fatalf("youtube should be unaffected by disconnecting gmail: %v", err)
	}
	if ytSecrets["refreshToken"] != "rt-b" {
		t.Fatalf("youtube refreshToken changed unexpectedly: %v", ytSecrets)
	}
}

func TestGoogleServiceAccountsLegacyFallback(t *testing.T) {
	g := newTestGoogleServiceAccounts(t)
	ctx := context.Background()

	// Simulate an existing install that only ever configured the old
	// single central "google" credential, and never used the new
	// per-service Connect flow.
	if err := g.accounts.Put(ctx, CentralGoogleAccount, map[string]string{"refreshToken": "legacy-rt", "clientId": "cid", "clientSecret": "secret", "expiresAt": farFutureExpiry()}); err != nil {
		t.Fatalf("seeding legacy account: %v", err)
	}

	secrets, err := g.Resolve(ctx, "gmail")
	if err != nil {
		t.Fatalf("Resolve(gmail) should fall back to the legacy account: %v", err)
	}
	if secrets["refreshToken"] != "legacy-rt" {
		t.Fatalf("expected legacy refreshToken, got %v", secrets)
	}

	// Now connect gmail directly -- it must take priority over the
	// legacy row from here on.
	if err := g.Put(ctx, "gmail", map[string]string{"refreshToken": "new-rt", "clientId": "cid", "clientSecret": "secret", "expiresAt": farFutureExpiry()}); err != nil {
		t.Fatalf("Put gmail: %v", err)
	}
	secrets, err = g.Resolve(ctx, "gmail")
	if err != nil {
		t.Fatalf("Resolve(gmail) after direct connect: %v", err)
	}
	if secrets["refreshToken"] != "new-rt" {
		t.Fatalf("expected the newly-connected refreshToken to take priority over legacy, got %v", secrets)
	}
}

func TestGoogleServiceAccountsUnknownService(t *testing.T) {
	g := newTestGoogleServiceAccounts(t)
	ctx := context.Background()
	if err := g.Put(ctx, "drive", map[string]string{}); err == nil {
		t.Fatal("expected Put to reject an unknown service")
	}
	if err := g.Disconnect(ctx, "drive"); err == nil {
		t.Fatal("expected Disconnect to reject an unknown service")
	}
	if _, _, _, _, err := g.Status(ctx, "drive"); err == nil {
		t.Fatal("expected Status to reject an unknown service")
	}
}
