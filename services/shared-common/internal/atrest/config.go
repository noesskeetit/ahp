package atrest

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// Environment variables the local operator profile reads. They carry only
// references (a filename or a hex-encoded key that the operator exported into
// the process environment out-of-band). Values are never logged and never
// appear in a command line.
const (
	// EnvRootSecretFile names a file holding the operator root secret. When set,
	// per-tenant operator keys are derived from it (Tier 0). Absent means random
	// operator keys generated at startup (still per-tenant, still isolated).
	EnvRootSecretFile = "AHP_ATREST_ROOT_SECRET_FILE"
	// EnvByokSecretFile is a comma-separated list of "<tenant>=<hexkey>" BYOK
	// entries. Each hex key is 32 bytes of customer-supplied material (Tier 1);
	// it is provisioned verbatim as that tenant's first key instead of deriving
	// from the operator root.
	EnvByokSecretFile = "AHP_ATREST_BYOK_FILE"
)

// LoadAuthorityFromEnv builds a SyntheticKeyManager from the operator profile.
//
// Resolution order per tenant:
//  1. BYOK material from EnvByokSecretFile for that tenant, if present (Tier 1).
//  2. Otherwise a key derived from the operator root secret (EnvRootSecretFile)
//     bound to the tenant (Tier 0, deterministic across restarts).
//  3. Otherwise a freshly generated random key (isolated but non-deterministic).
//
// tenants lists which tenants to provision. It never returns key material and
// never logs it.
func LoadAuthorityFromEnv(ctx context.Context, tenants []string) (*SyntheticKeyManager, error) {
	auth := NewSyntheticKeyManager()
	byok, err := loadByok()
	if err != nil {
		return nil, err
	}
	root, err := loadRootSecret()
	if err != nil {
		return nil, err
	}
	for _, tenant := range tenants {
		if tenant == "" {
			return nil, ErrInvalidArgument
		}
		var material []byte
		switch {
		case byok[tenant] != nil:
			material = byok[tenant]
		case root != nil:
			derived, derr := deriveKEK(root, tenant, "root", 1)
			if derr != nil {
				return nil, derr
			}
			material = derived
		default:
			generated, gerr := randomBytes(keyBytes)
			if gerr != nil {
				return nil, gerr
			}
			material = generated
		}
		if _, err := auth.Provision(ctx, tenant, "root", material); err != nil {
			return nil, fmt.Errorf("provision %s: %w", tenant, err)
		}
		clearBytes(material)
	}
	return auth, nil
}

// loadRootSecret reads the operator root secret from a file, if configured. It
// enforces the 32-byte length so a truncated or wrong-sized secret fails closed
// rather than being silently padded.
func loadRootSecret() ([]byte, error) {
	path := strings.TrimSpace(os.Getenv(EnvRootSecretFile))
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read root secret: %w", err)
	}
	key, err := parseKeyBytes(raw)
	clearBytes(raw)
	if err != nil {
		return nil, fmt.Errorf("root secret: %w", err)
	}
	return key, nil
}

// loadByok parses "<tenant>=<hex>" entries from the BYOK file. Every entry must
// decode to exactly 32 bytes.
func loadByok() (map[string][]byte, error) {
	path := strings.TrimSpace(os.Getenv(EnvByokSecretFile))
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read byok file: %w", err)
	}
	defer clearBytes(raw)
	out := map[string][]byte{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		tenant, hexKey, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("byok: malformed entry (missing '=')")
		}
		key, err := parseKeyBytes([]byte(strings.TrimSpace(hexKey)))
		if err != nil {
			return nil, fmt.Errorf("byok %s: %w", strings.TrimSpace(tenant), err)
		}
		out[strings.TrimSpace(tenant)] = key
	}
	return out, nil
}

// parseKeyBytes accepts either 64 hex chars (32 bytes) or exactly 32 raw bytes.
//
// The two forms are distinguished by length so neither can be mistaken for the
// other: raw material is used verbatim (only a single trailing newline is
// stripped, so binary keys containing spaces or control bytes survive intact),
// while the hex form may carry surrounding whitespace. Any other length fails
// closed rather than being silently padded or truncated.
func parseKeyBytes(raw []byte) ([]byte, error) {
	// Raw form first: exact key material, tolerant only of a trailing newline so
	// a text editor does not corrupt it. We do NOT trim leading/trailing spaces
	// here because a raw key may legitimately contain them.
	trimmed := bytes.TrimRight(raw, "\r\n")
	if len(trimmed) == keyBytes {
		out := make([]byte, keyBytes)
		copy(out, trimmed)
		return out, nil
	}
	// Hex form: 64 hex characters, whitespace-tolerant.
	hexStr := strings.TrimSpace(string(raw))
	if len(hexStr) == hex.EncodedLen(keyBytes) {
		if dec, err := hex.DecodeString(hexStr); err == nil && len(dec) == keyBytes {
			return dec, nil
		}
	}
	return nil, ErrKeyLength
}

// randomTenantKeyMaterial is a test/dev helper that yields 32 fresh bytes.
func randomTenantKeyMaterial() ([]byte, error) {
	b := make([]byte, keyBytes)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// clearBytes zeroes a slice so key material does not linger in memory longer
// than needed. Go's GC may still copy, but this narrows the window.
func clearBytes(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
