package vault

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

// EnvStore is the persistence interface Global/Service Environment
// needs; internal/store's Postgres implementation satisfies it. Every
// value is stored as ciphertext regardless of IsSecret (see schema.sql's
// global_env/service_env doc comment) -- simplest safe default.
type EnvStore interface {
	PutGlobalEnv(ctx context.Context, key string, ciphertext []byte, isSecret bool) error
	DeleteGlobalEnv(ctx context.Context, key string) error
	AllGlobalEnvCiphertext(ctx context.Context) (map[string][]byte, error)

	PutServiceEnv(ctx context.Context, serviceID, key string, ciphertext []byte, isSecret bool) error
	DeleteServiceEnv(ctx context.Context, serviceID, key string) error
	AllServiceEnvCiphertext(ctx context.Context, serviceID string) (map[string][]byte, error)
}

// EnvVault encrypts/decrypts Global and Service Environment values,
// sharing the same AEAD cipher/master key as every other secret in
// this codebase (Vault, AccountVault) -- one MICROFLOW_MASTER_KEY
// secures all of it; there is no separate key to manage or rotate.
type EnvVault struct {
	store EnvStore
	aead  cipher.AEAD
}

// NewEnvVault builds an EnvVault sharing v's cipher.
func (v *Vault) NewEnvVault(store EnvStore) *EnvVault {
	return &EnvVault{store: store, aead: v.aead}
}

func (ev *EnvVault) seal(plaintext string) ([]byte, error) {
	nonce := make([]byte, ev.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return ev.aead.Seal(nonce, nonce, []byte(plaintext), nil), nil
}

func (ev *EnvVault) open(ciphertext []byte) (string, error) {
	ns := ev.aead.NonceSize()
	if len(ciphertext) < ns {
		return "", errors.New("vault: corrupt ciphertext")
	}
	nonce, ct := ciphertext[:ns], ciphertext[ns:]
	plaintext, err := ev.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", errors.New("vault: decryption failed")
	}
	return string(plaintext), nil
}

// PutGlobal encrypts and stores one Global Environment value.
func (ev *EnvVault) PutGlobal(ctx context.Context, key, value string, isSecret bool) error {
	ct, err := ev.seal(value)
	if err != nil {
		return err
	}
	return ev.store.PutGlobalEnv(ctx, key, ct, isSecret)
}

func (ev *EnvVault) DeleteGlobal(ctx context.Context, key string) error {
	return ev.store.DeleteGlobalEnv(ctx, key)
}

// PutService encrypts and stores one Service Environment override.
func (ev *EnvVault) PutService(ctx context.Context, serviceID, key, value string, isSecret bool) error {
	ct, err := ev.seal(value)
	if err != nil {
		return err
	}
	return ev.store.PutServiceEnv(ctx, serviceID, key, ct, isSecret)
}

func (ev *EnvVault) DeleteService(ctx context.Context, serviceID, key string) error {
	return ev.store.DeleteServiceEnv(ctx, serviceID, key)
}

// ResolveAll decrypts and returns every Global Environment value and
// every Service Environment override for serviceID, as two plain maps
// -- one DB round trip and one decrypt pass per map, called once per
// workflow run (see runner.Runner), never once per $env lookup. Callers
// apply precedence themselves (Service Environment > Global Environment
// > process environment, rule 2) via engine.RunContext.Env.
func (ev *EnvVault) ResolveAll(ctx context.Context, serviceID string) (serviceEnv, globalEnv map[string]string, err error) {
	globalCT, err := ev.store.AllGlobalEnvCiphertext(ctx)
	if err != nil {
		return nil, nil, err
	}
	globalEnv = make(map[string]string, len(globalCT))
	for k, ct := range globalCT {
		v, err := ev.open(ct)
		if err != nil {
			return nil, nil, err
		}
		globalEnv[k] = v
	}

	serviceCT, err := ev.store.AllServiceEnvCiphertext(ctx, serviceID)
	if err != nil {
		return nil, nil, err
	}
	serviceEnv = make(map[string]string, len(serviceCT))
	for k, ct := range serviceCT {
		v, err := ev.open(ct)
		if err != nil {
			return nil, nil, err
		}
		serviceEnv[k] = v
	}
	return serviceEnv, globalEnv, nil
}

// GetService decrypts and returns ONE Service Environment value for
// serviceID (found=false if that Service has no such key). It only ever
// reads serviceID's own rows, and decrypts only the requested key. Used
// solely by the explicit, per-key "reveal" endpoint -- never by list
// responses.
func (ev *EnvVault) GetService(ctx context.Context, serviceID, key string) (value string, found bool, err error) {
	all, err := ev.store.AllServiceEnvCiphertext(ctx, serviceID)
	if err != nil {
		return "", false, err
	}
	ct, ok := all[key]
	if !ok {
		return "", false, nil
	}
	v, err := ev.open(ct)
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// DeleteAllService removes every Service Environment override that
// belongs to serviceID -- and nothing else (Global Environment, other
// Services, workflows and credentials are never touched). Returns how
// many variables were removed. Idempotent: safe to retry after a
// partial failure.
func (ev *EnvVault) DeleteAllService(ctx context.Context, serviceID string) (int, error) {
	if serviceID == "" {
		return 0, errors.New("vault: service id is required")
	}
	// Preferred path: the store deletes the whole set in ONE statement
	// (atomic). Optional interface so EnvStore and its other
	// implementations/fakes stay unchanged.
	if a, ok := ev.store.(interface {
		DeleteAllServiceEnv(ctx context.Context, serviceID string) (int, error)
	}); ok {
		return a.DeleteAllServiceEnv(ctx, serviceID)
	}
	all, err := ev.store.AllServiceEnvCiphertext(ctx, serviceID)
	if err != nil {
		return 0, err
	}
	n := 0
	for k := range all {
		if err := ev.store.DeleteServiceEnv(ctx, serviceID, k); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
