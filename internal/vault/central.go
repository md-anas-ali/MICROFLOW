package vault

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"
)

// CentralGoogleAccount is the fixed key under which MicroFlow's one
// shared/central Google account credential is stored. There is exactly
// one central Google account per MicroFlow instance today (the whole
// point of this feature -- "log in once, every Google node uses it"),
// so this never varies at runtime; it exists as a named constant only
// so the account table's key isn't a magic string sprinkled across
// callers.
const CentralGoogleAccount = "google"

// AccountStore is the persistence interface for central (account-scoped,
// not per-workflow/per-node) credentials. Deliberately a separate
// interface and a separate table from Store/credentials -- see the
// package-level architecture note below -- so the central Google
// account credential can never accidentally collide with, or be
// confused for, a per-node credential row.
type AccountStore interface {
	GetEncryptedAccount(ctx context.Context, account string) ([]byte, error)
	PutEncryptedAccount(ctx context.Context, account string, ciphertext []byte) error
	DeleteEncryptedAccount(ctx context.Context, account string) error
	// AccountCredentialMeta reports whether a central credential is
	// saved for account and, if so, when it was last written -- no
	// ciphertext, so this is always safe to expose over HTTP as a
	// "configured?" status check (rule 11/12).
	AccountCredentialMeta(ctx context.Context, account string) (updatedAt time.Time, exists bool, err error)
}

// AccountVault stores central (account-scoped) credentials, encrypted
// with the SAME AEAD cipher/master key as the per-workflow Vault (see
// Vault.NewAccountVault) -- one MICROFLOW_MASTER_KEY secures both
// storage scopes; there is no second key to manage.
type AccountVault struct {
	store AccountStore
	aead  cipher.AEAD
}

// NewAccountVault builds an AccountVault that shares v's cipher, so
// callers never construct a second master key/AEAD by hand.
func (v *Vault) NewAccountVault(store AccountStore) *AccountVault {
	return &AccountVault{store: store, aead: v.aead}
}

// Put encrypts and stores secrets for the given central account name
// (in practice, always CentralGoogleAccount today).
func (av *AccountVault) Put(ctx context.Context, account string, secrets map[string]string) error {
	plaintext, err := marshalSecrets(secrets)
	if err != nil {
		return err
	}
	nonce := make([]byte, av.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}
	ciphertext := av.aead.Seal(nonce, nonce, plaintext, nil)
	return av.store.PutEncryptedAccount(ctx, account, ciphertext)
}

// Resolve implements engine.CredentialResolver-shaped lookup for a
// central account (note: unlike Vault.Resolve, there is no workflowID
// -- central credentials are shared across every workflow by design).
func (av *AccountVault) Resolve(ctx context.Context, account string) (map[string]string, error) {
	ciphertext, err := av.store.GetEncryptedAccount(ctx, account)
	if err != nil {
		return nil, fmt.Errorf("vault: no central credential saved for %q yet", account)
	}
	ns := av.aead.NonceSize()
	if len(ciphertext) < ns {
		return nil, errors.New("vault: corrupt ciphertext")
	}
	nonce, ct := ciphertext[:ns], ciphertext[ns:]
	plaintext, err := av.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		// deliberately generic: never echo cipher internals or key material
		return nil, errors.New("vault: decryption failed")
	}
	return unmarshalSecrets(plaintext)
}

// Delete removes the stored central credential, if any (the UI's
// "Clear" action).
func (av *AccountVault) Delete(ctx context.Context, account string) error {
	return av.store.DeleteEncryptedAccount(ctx, account)
}

// Status reports whether a central credential is configured, without
// ever touching secret bytes -- what the frontend's "Google Credentials"
// page polls to render its configured/not-configured badge.
func (av *AccountVault) Status(ctx context.Context, account string) (updatedAt time.Time, configured bool, err error) {
	return av.store.AccountCredentialMeta(ctx, account)
}

// AccountResolver refreshes and resolves ONE named central-account
// credential row, transparently keeping its accessToken fresh the same
// way OAuthResolver does for per-node credentials (see refreshEngine in
// oauth.go, which this and OAuthResolver both use). `account` used to
// be hardcoded to CentralGoogleAccount (one shared row for every Google
// node); it's now a constructor parameter so the same type can also
// back one row per connected Google *service* (gmail/youtube/sheets --
// see GoogleServiceAccounts below), without a second implementation of
// the refresh/lock logic.
type AccountResolver struct {
	accounts *AccountVault
	account  string
	eng      *refreshEngine
}

// NewAccountResolver builds a resolver for the given central-account row.
func NewAccountResolver(av *AccountVault, account string) *AccountResolver {
	return &AccountResolver{accounts: av, account: account, eng: newRefreshEngine()}
}

func (r *AccountResolver) Resolve(ctx context.Context) (map[string]string, error) {
	return r.eng.resolve(ctx, "account/"+r.account,
		func(ctx context.Context) (map[string]string, error) { return r.accounts.Resolve(ctx, r.account) },
		func(ctx context.Context, secrets map[string]string) error {
			return r.accounts.Put(ctx, r.account, secrets)
		},
	)
}

// CentralFallbackResolver implements engine.CredentialResolver and is
// the piece that makes "one saved Google account, every Google node
// uses it automatically" actually happen at execution time. It tries a
// node-specific credential first (an explicit per-node override --
// exactly the storage/behavior that existed before this feature, via
// cmd/setcred or the per-node "Google Credentials" section in a node's
// side panel), and only falls back to the central account credential
// when no per-node override was ever saved for that node.
//
// This is the whole "central credential injection" mechanism: node
// executors (internal/nodes/google.go) are completely unaware of it --
// they just call Creds.Resolve(ctx, workflowID, node.Name) exactly as
// before, and get back either their own override or the shared central
// account, whichever exists. Nothing about node configuration,
// execution, or the engine changes.
type CentralFallbackResolver struct {
	perNode *OAuthResolver
	account *AccountResolver
}

func NewCentralFallbackResolver(perNode *OAuthResolver, account *AccountResolver) *CentralFallbackResolver {
	return &CentralFallbackResolver{perNode: perNode, account: account}
}

func (r *CentralFallbackResolver) Resolve(ctx context.Context, workflowID, logicalName string) (map[string]string, error) {
	if secrets, err := r.perNode.Resolve(ctx, workflowID, logicalName); err == nil {
		return secrets, nil
	}
	return r.account.Resolve(ctx)
}

// --- per-service connected Google accounts (n8n-style "Connect with
// Google" per node type) ---
//
// GoogleServices are the connectable service keys, one per Google node
// type MicroFlow has an executor for. Keep this list, the scope map in
// googleoauth.go, and internal/nodes/google.go's node-type-to-service
// mapping in sync if a Google node type is ever added.
var GoogleServices = []string{"gmail", "youtube", "sheets"}

func IsGoogleService(s string) bool {
	for _, v := range GoogleServices {
		if v == s {
			return true
		}
	}
	return false
}

// serviceAccountKey namespaces each service's row distinctly from the
// legacy single CentralGoogleAccount row, so introducing per-service
// accounts can never collide with or silently overwrite an existing
// install's central credential.
func serviceAccountKey(service string) string { return "svc:" + service }

// GoogleServiceAccounts holds one AccountResolver per connectable
// Google service, each backed by its own row (so Gmail, YouTube, and
// Sheets can each be connected to the same Google account or to three
// different ones, independently of each other), plus the legacy single
// CentralGoogleAccount resolver as a migration fallback: an install
// that already had the old single "log in once" credential configured
// keeps working for every service, unmodified, until an operator
// connects a specific service through the new per-service flow (at
// which point that service's own row takes over and the legacy row is
// no longer consulted for it).
type GoogleServiceAccounts struct {
	accounts  *AccountVault
	resolvers map[string]*AccountResolver
	legacy    *AccountResolver
}

// NewGoogleServiceAccounts builds resolvers for every GoogleServices
// entry plus the legacy fallback, all sharing av's storage/cipher.
func NewGoogleServiceAccounts(av *AccountVault) *GoogleServiceAccounts {
	m := make(map[string]*AccountResolver, len(GoogleServices))
	for _, svc := range GoogleServices {
		m[svc] = NewAccountResolver(av, serviceAccountKey(svc))
	}
	return &GoogleServiceAccounts{
		accounts:  av,
		resolvers: m,
		legacy:    NewAccountResolver(av, CentralGoogleAccount),
	}
}

// Put stores/replaces the connected-account credential for one service
// (called by the OAuth callback once a code exchange succeeds).
func (g *GoogleServiceAccounts) Put(ctx context.Context, service string, secrets map[string]string) error {
	if !IsGoogleService(service) {
		return fmt.Errorf("vault: unknown google service %q", service)
	}
	return g.accounts.Put(ctx, serviceAccountKey(service), secrets)
}

// Disconnect removes only this service's own row -- it never touches
// the legacy central row or any other service's row, so disconnecting
// YouTube can't affect Gmail/Sheets.
func (g *GoogleServiceAccounts) Disconnect(ctx context.Context, service string) error {
	if !IsGoogleService(service) {
		return fmt.Errorf("vault: unknown google service %q", service)
	}
	return g.accounts.Delete(ctx, serviceAccountKey(service))
}

// Resolve is what node executors call at run time: this service's own
// connected account first, falling back to the legacy central account
// only if this service was never individually connected (see type doc).
func (g *GoogleServiceAccounts) Resolve(ctx context.Context, service string) (map[string]string, error) {
	r, ok := g.resolvers[service]
	if !ok {
		return nil, fmt.Errorf("vault: unknown google service %q", service)
	}
	secrets, err := r.Resolve(ctx)
	if err == nil {
		return secrets, nil
	}
	legacySecrets, legacyErr := g.legacy.Resolve(ctx)
	if legacyErr == nil {
		return legacySecrets, nil
	}
	return nil, err
}

// Status reports this service's OWN connection state for the "Google
// Connections" UI (never the legacy fallback -- the UI should show
// "Connect Google" for a service that hasn't been individually
// connected yet, even if the legacy account would still work at
// execution time). needsReconnect is true when a credential row exists
// but Google has revoked/expired the refresh token (invalid_grant) --
// the UI's cue to show "Reconnect" instead of "Connect".
func (g *GoogleServiceAccounts) Status(ctx context.Context, service string) (email string, updatedAt time.Time, connected bool, needsReconnect bool, err error) {
	if !IsGoogleService(service) {
		return "", time.Time{}, false, false, fmt.Errorf("vault: unknown google service %q", service)
	}
	updatedAt, exists, err := g.accounts.Status(ctx, serviceAccountKey(service))
	if err != nil || !exists {
		return "", updatedAt, false, false, err
	}
	secrets, rerr := g.resolvers[service].Resolve(ctx)
	if rerr != nil {
		if errors.Is(rerr, ErrGoogleReauthRequired) {
			return "", updatedAt, true, true, nil
		}
		return "", updatedAt, true, false, rerr
	}
	return secrets["email"], updatedAt, true, false, nil
}
