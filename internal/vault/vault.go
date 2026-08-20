// Package vault stores workflow credentials (API keys, OAuth tokens,
// Postgres connection info for downstream nodes, etc.) encrypted at
// rest. Plaintext secrets are only ever held in memory for the
// duration of a single node execution and are never written to logs,
// error messages, or exported workflow JSON (rule 11).
package vault

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
)

// Store is the persistence interface the vault needs; internal/store's
// Postgres implementation satisfies it. Kept minimal and swappable so
// tests can use an in-memory fake.
type Store interface {
	GetEncrypted(ctx context.Context, workflowID, logicalName string) ([]byte, error)
	PutEncrypted(ctx context.Context, workflowID, logicalName string, ciphertext []byte) error
}

type Vault struct {
	store Store
	aead  cipher.AEAD
}

// New builds a Vault from a base64-encoded 32-byte master key, read from
// the MICROFLOW_MASTER_KEY env var by the caller (not read directly here
// so tests can inject a key without touching the environment).
func New(store Store, masterKeyB64 string) (*Vault, error) {
	keyBytes, err := base64.StdEncoding.DecodeString(masterKeyB64)
	if err != nil {
		return nil, fmt.Errorf("vault: invalid master key encoding: %w", err)
	}
	if len(keyBytes) != 32 {
		return nil, errors.New("vault: master key must decode to exactly 32 bytes (AES-256)")
	}
	block, err := aes.NewCipher(keyBytes)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Vault{store: store, aead: aead}, nil
}

// GenerateMasterKey is a one-time setup helper (CLI command) to produce
// a fresh base64 key for MICROFLOW_MASTER_KEY.
func GenerateMasterKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// Put encrypts and stores a set of key/value secrets for one logical
// credential (e.g. {"accessToken": "...", "refreshToken": "..."}).
func (v *Vault) Put(ctx context.Context, workflowID, logicalName string, secrets map[string]string) error {
	plaintext, err := marshalSecrets(secrets)
	if err != nil {
		return err
	}
	nonce := make([]byte, v.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}
	ciphertext := v.aead.Seal(nonce, nonce, plaintext, nil)
	return v.store.PutEncrypted(ctx, workflowID, logicalName, ciphertext)
}

// Resolve implements engine.CredentialResolver.
func (v *Vault) Resolve(ctx context.Context, workflowID, logicalName string) (map[string]string, error) {
	ciphertext, err := v.store.GetEncrypted(ctx, workflowID, logicalName)
	if err != nil {
		return nil, fmt.Errorf("vault: credential %q not found for workflow %q", logicalName, workflowID)
	}
	ns := v.aead.NonceSize()
	if len(ciphertext) < ns {
		return nil, errors.New("vault: corrupt ciphertext")
	}
	nonce, ct := ciphertext[:ns], ciphertext[ns:]
	plaintext, err := v.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		// deliberately generic: never echo cipher internals or key material
		return nil, errors.New("vault: decryption failed")
	}
	return unmarshalSecrets(plaintext)
}

// masterKeyFromEnv is a convenience the API server calls at startup.
func MasterKeyFromEnv() (string, error) {
	k := os.Getenv("MICROFLOW_MASTER_KEY")
	if k == "" {
		return "", errors.New("MICROFLOW_MASTER_KEY is not set -- generate one with `microflow vault generate-key` and set it before starting the server")
	}
	return k, nil
}
