package vault

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"microflow/internal/tenant"
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

// tenantServiceAccountKey namespaces a connected Google account row by
// BOTH the MicroFlow Service (tenant.Service.ID -- an isolated
// workspace) and the Google service (gmail/youtube/sheets), e.g.
// "svc:acme-shorts:youtube". This is the actual isolation boundary for
// rule 4 ("YouTube account, Google account, ... কখনো mix হবে না"): two
// different MicroFlow Services connecting YouTube independently land on
// two entirely distinct rows in google_account_credentials, with no
// shared key in common. Reuses the exact same table/column/AEAD cipher
// as the pre-existing single-account design -- no new storage, no
// duplicate implementation (rule: reuse existing storage).
func tenantServiceAccountKey(msvcID, service string) string { return "svc:" + msvcID + ":" + service }

// GoogleServiceAccounts holds one AccountResolver per (MicroFlow
// Service, Google service) pair, created lazily and cached, so Gmail/
// YouTube/Sheets can each be connected to a different Google account
// per MicroFlow Service, fully isolated from every other Service.
//
// Backward compatibility (rule 14): a MicroFlow Service falls back to
// the pre-existing single "log in once" resolvers ONLY when its ID is
// tenant.DefaultID -- the Service every workflow that existed before
// this feature was introduced is automatically migrated into (see
// store schema migration). This means an existing single-tenant
// deployment's already-connected Google accounts keep working
// unmodified after upgrading, while every OTHER (newly created)
// Service starts with no fallback at all: if it hasn't connected its
// own account yet, execution fails clearly instead of silently
// borrowing another Service's or the legacy account's credential --
// nothing to accidentally leak across the isolation boundary.
type GoogleServiceAccounts struct {
	accounts *AccountVault

	mu        sync.Mutex
	resolvers map[string]*AccountResolver // key: tenantServiceAccountKey(msvcID, service)

	// legacyResolvers/legacyPerService back tenant.DefaultID's fallback
	// chain exactly as before this feature existed: per-service rows
	// first (serviceAccountKey), then the single original
	// CentralGoogleAccount row.
	legacyPerService map[string]*AccountResolver
	legacy           *AccountResolver
}

// NewGoogleServiceAccounts builds the Default-Service legacy resolvers
// eagerly (matching the pre-existing behavior exactly) and prepares an
// empty cache for every other Service's resolvers, built on first use.
func NewGoogleServiceAccounts(av *AccountVault) *GoogleServiceAccounts {
	legacy := make(map[string]*AccountResolver, len(GoogleServices))
	for _, svc := range GoogleServices {
		legacy[svc] = NewAccountResolver(av, serviceAccountKey(svc))
	}
	return &GoogleServiceAccounts{
		accounts:         av,
		resolvers:        make(map[string]*AccountResolver),
		legacyPerService: legacy,
		legacy:           NewAccountResolver(av, CentralGoogleAccount),
	}
}

// resolverFor returns (creating and caching if needed) the resolver for
// one (MicroFlow Service, Google service) pair.
func (g *GoogleServiceAccounts) resolverFor(msvcID, service string) *AccountResolver {
	key := tenantServiceAccountKey(msvcID, service)
	g.mu.Lock()
	defer g.mu.Unlock()
	if r, ok := g.resolvers[key]; ok {
		return r
	}
	r := NewAccountResolver(g.accounts, key)
	g.resolvers[key] = r
	return r
}

// Put stores/replaces the connected-account credential for one
// (MicroFlow Service, Google service) pair (called by the OAuth
// callback once a code exchange succeeds, or by the manual paste
// endpoint).
func (g *GoogleServiceAccounts) Put(ctx context.Context, msvcID, service string, secrets map[string]string) error {
	if !IsGoogleService(service) {
		return fmt.Errorf("vault: unknown google service %q", service)
	}
	return g.accounts.Put(ctx, tenantServiceAccountKey(msvcID, service), secrets)
}

// Disconnect removes only this (Service, Google service) pair's own
// row -- it never touches any other Service's row, any other Google
// service's row, or the legacy account (isolation rule 4/13).
func (g *GoogleServiceAccounts) Disconnect(ctx context.Context, msvcID, service string) error {
	if !IsGoogleService(service) {
		return fmt.Errorf("vault: unknown google service %q", service)
	}
	return g.accounts.Delete(ctx, tenantServiceAccountKey(msvcID, service))
}

// Resolve is what node executors call at run time: this MicroFlow
// Service's own connected account for `service` first; only for
// tenant.DefaultID, fall back to the pre-existing legacy per-service
// row and then the original single central account (see type doc).
// Every other Service gets no fallback at all -- a clear "not
// connected" error rather than ever silently reusing another Service's
// credential.
func (g *GoogleServiceAccounts) Resolve(ctx context.Context, msvcID, service string) (map[string]string, error) {
	if !IsGoogleService(service) {
		return nil, fmt.Errorf("vault: unknown google service %q", service)
	}
	secrets, err := g.resolverFor(msvcID, service).Resolve(ctx)
	if err == nil {
		return secrets, nil
	}
	if msvcID != tenant.DefaultID {
		return nil, err
	}
	if legacySecrets, legacyErr := g.legacyPerService[service].Resolve(ctx); legacyErr == nil {
		return legacySecrets, nil
	}
	if legacySecrets, legacyErr := g.legacy.Resolve(ctx); legacyErr == nil {
		return legacySecrets, nil
	}
	return nil, err
}

// Status reports this (Service, Google service) pair's OWN connection
// state for the "Google Connections" UI (never a fallback -- the UI
// should show "Connect Google" for a Service that hasn't individually
// connected yet, even if a fallback would still work at execution
// time). needsReconnect is true when a credential row exists but
// Google has revoked/expired the refresh token (invalid_grant) -- the
// UI's cue to show "Reconnect" instead of "Connect".
func (g *GoogleServiceAccounts) Status(ctx context.Context, msvcID, service string) (email string, updatedAt time.Time, connected bool, needsReconnect bool, err error) {
	if !IsGoogleService(service) {
		return "", time.Time{}, false, false, fmt.Errorf("vault: unknown google service %q", service)
	}
	key := tenantServiceAccountKey(msvcID, service)
	updatedAt, exists, err := g.accounts.Status(ctx, key)
	if err != nil || !exists {
		return "", updatedAt, false, false, err
	}
	secrets, rerr := g.resolverFor(msvcID, service).Resolve(ctx)
	if rerr != nil {
		if errors.Is(rerr, ErrGoogleReauthRequired) {
			return "", updatedAt, true, true, nil
		}
		return "", updatedAt, true, false, rerr
	}
	return secrets["email"], updatedAt, true, false, nil
}
