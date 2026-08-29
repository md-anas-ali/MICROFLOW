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

// AccountResolver refreshes and resolves the central Google account
// credential, transparently keeping its accessToken fresh the same way
// OAuthResolver does for per-node credentials (see refreshEngine in
// oauth.go, which this and OAuthResolver both use).
type AccountResolver struct {
	accounts *AccountVault
	account  string
	eng      *refreshEngine
}

// NewAccountResolver builds a resolver for the central Google account
// (CentralGoogleAccount).
func NewAccountResolver(av *AccountVault) *AccountResolver {
	return &AccountResolver{accounts: av, account: CentralGoogleAccount, eng: newRefreshEngine()}
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
