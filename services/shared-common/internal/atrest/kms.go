package atrest

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Sentinel errors. Callers switch on these; none of them carry key material or
// plaintext in their messages.
var (
	// ErrUnknownTenant means the authority does not hold a key for a tenant.
	// Sealing or opening for an unprovisioned tenant fails closed: there is no
	// implicit default key.
	ErrUnknownTenant = errors.New("atrest: unknown tenant")
	// ErrUnknownKey means the (key id, version) named by an envelope is not
	// known to the authority for that tenant.
	ErrUnknownKey = errors.New("atrest: unknown key version")
	// ErrKeyDisabled means the key is administratively disabled; both new seals
	// and reads of prior ciphertext are refused until it is re-enabled.
	ErrKeyDisabled = errors.New("atrest: key disabled")
	// ErrKeyRevoked means the key is permanently revoked (compromise). Ciphertext
	// under it can never be read again; re-enabling is not possible.
	ErrKeyRevoked = errors.New("atrest: key revoked")
	// ErrTenantMismatch is returned when an envelope names a different tenant
	// than the authenticated scope that asked to open it. It is a hard isolation
	// failure, distinct from a plain authentication failure.
	ErrTenantMismatch = errors.New("atrest: tenant mismatch")
	// ErrAuthFailed means GCM authentication failed: wrong key, tampered bytes,
	// or a mismatched binding (version / purpose). No plaintext is returned.
	ErrAuthFailed = errors.New("atrest: authentication failed")
	// ErrMalformed means the stored bytes are not a well-formed envelope.
	ErrMalformed = errors.New("atrest: malformed envelope")
	// ErrKeyLength means supplied key material is not 32 bytes.
	ErrKeyLength = errors.New("atrest: key material must be 32 bytes")
	// ErrInvalidArgument marks a programming error (empty tenant/purpose).
	ErrInvalidArgument = errors.New("atrest: invalid argument")
)

// KeyStatus is the administrative state of one master-key version.
type KeyStatus uint8

const (
	// StatusActive keys seal new data and open existing data.
	StatusActive KeyStatus = iota
	// StatusRotated keys are no longer used for new seals but still open the
	// ciphertext written while they were current — this is what makes rotation
	// non-destructive.
	StatusRotated
	// StatusDisabled temporarily suspends a key: it neither seals nor opens.
	// It can be re-enabled.
	StatusDisabled
	// StatusRevoked permanently destroys a key's usability (assumed compromise).
	StatusRevoked
)

func (s KeyStatus) String() string {
	switch s {
	case StatusActive:
		return "active"
	case StatusRotated:
		return "rotated"
	case StatusDisabled:
		return "disabled"
	case StatusRevoked:
		return "revoked"
	default:
		return "unknown"
	}
}

// MasterKey describes a single key version held by the authority. Material is
// never exposed through the read interface; it is only reachable inside this
// package when deriving a KEK.
type MasterKey struct {
	Tenant  string
	KeyID   string
	Version uint32
	Status  KeyStatus
	Created time.Time

	material []byte
}

// KeyManager is the read-only view the data path (Codec) holds. It deliberately
// has no method that accepts key material or lets the caller pick a foreign
// tenant's key: it can only resolve keys the authority has already provisioned.
type KeyManager interface {
	// Current returns the active key a tenant should seal new data under. It
	// fails closed if the tenant has no active key.
	Current(ctx context.Context, tenant string) (MasterKey, error)
	// Resolve returns a specific key version named by an envelope. Rotated keys
	// resolve (they must still open old data); disabled and revoked keys resolve
	// to their respective errors.
	Resolve(ctx context.Context, tenant, keyID string, version uint32) (MasterKey, error)
}

// KeyAuthority is the mutating, operator-side view. Ordinary persistence owners
// must not be handed a KeyAuthority; only an operator or a control-plane service
// performs provisioning, rotation and status changes.
type KeyAuthority interface {
	KeyManager
	// Provision installs a tenant's key. If material is non-nil it is treated as
	// customer BYOK material (Tier 1); when nil the authority generates a random
	// key (operator-managed Tier 0). A new tenant's first key is Active.
	Provision(ctx context.Context, tenant, keyID string, material []byte) (MasterKey, error)
	// Rotate adds a new Active version under keyID and demotes the previous
	// Active version to Rotated (still readable, no longer sealing).
	Rotate(ctx context.Context, tenant, keyID string, material []byte) (MasterKey, error)
	// SetStatus moves a key version to a new administrative status, refusing an
	// illegal revocation of the key back to active.
	SetStatus(ctx context.Context, tenant, keyID string, version uint32, status KeyStatus) error
}

// SyntheticKeyManager is a self-contained KeyAuthority/KeyManager suitable for
// Tier 0 (operator-managed per-tenant keys) and Tier 1 (BYOK, when material is
// supplied). Keys live in-process; the operator loads master material from a
// private env/file at startup (see LoadAuthorityFromEnv) — never from a task
// bearer or the plaintext payload.
//
// It models the platform's own KMS: the tenant is the isolation partition and
// each tenant owns an ordered, versioned set of keys. The same envelope format
// and Codec work unchanged when this is backed by a real external KMS.
type SyntheticKeyManager struct {
	mu sync.RWMutex
	// byTenantKey: tenant -> keyID -> ascending versions.
	byTenantKey map[string]map[string][]*MasterKey
}

// NewSyntheticKeyManager returns an empty authority. Every tenant must be
// explicitly provisioned before any Seal succeeds for it.
func NewSyntheticKeyManager() *SyntheticKeyManager {
	return &SyntheticKeyManager{byTenantKey: map[string]map[string][]*MasterKey{}}
}

var (
	_ KeyManager   = (*SyntheticKeyManager)(nil)
	_ KeyAuthority = (*SyntheticKeyManager)(nil)
)

// Current returns the single Active key for a tenant. If a tenant has more than
// one key id, the most recently created active version wins (deterministic by
// creation order and version).
func (k *SyntheticKeyManager) Current(ctx context.Context, tenant string) (MasterKey, error) {
	if tenant == "" {
		return MasterKey{}, ErrInvalidArgument
	}
	k.mu.RLock()
	defer k.mu.RUnlock()
	keys := k.byTenantKey[tenant]
	if len(keys) == 0 {
		return MasterKey{}, ErrUnknownTenant
	}
	var best *MasterKey
	for _, versions := range keys {
		for i := len(versions) - 1; i >= 0; i-- {
			mk := versions[i]
			if mk.Status != StatusActive {
				continue
			}
			if best == nil || mk.Created.After(best.Created) ||
				(mk.Created.Equal(best.Created) && mk.Version > best.Version) {
				best = mk
			}
		}
	}
	if best == nil {
		return MasterKey{}, fmt.Errorf("%w: no active key for tenant", ErrUnknownTenant)
	}
	return cloneKey(*best), nil
}

// Resolve returns a named key version, refusing disabled and revoked keys.
func (k *SyntheticKeyManager) Resolve(ctx context.Context, tenant, keyID string, version uint32) (MasterKey, error) {
	if tenant == "" || keyID == "" {
		return MasterKey{}, ErrInvalidArgument
	}
	k.mu.RLock()
	defer k.mu.RUnlock()
	versions := k.byTenantKey[tenant][keyID]
	for _, mk := range versions {
		if mk.Version != version {
			continue
		}
		switch mk.Status {
		case StatusDisabled:
			return MasterKey{}, ErrKeyDisabled
		case StatusRevoked:
			return MasterKey{}, ErrKeyRevoked
		default:
			return cloneKey(*mk), nil
		}
	}
	return MasterKey{}, ErrUnknownKey
}

// Provision installs the first key for a (tenant, keyID) pair. Re-provisioning an
// existing version is refused; use Rotate to add versions.
func (k *SyntheticKeyManager) Provision(ctx context.Context, tenant, keyID string, material []byte) (MasterKey, error) {
	if tenant == "" || keyID == "" {
		return MasterKey{}, ErrInvalidArgument
	}
	if len(material) != 0 && len(material) != keyBytes {
		return MasterKey{}, ErrKeyLength
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.byTenantKey[tenant]; !ok {
		k.byTenantKey[tenant] = map[string][]*MasterKey{}
	}
	versions := k.byTenantKey[tenant][keyID]
	if len(versions) != 0 {
		return MasterKey{}, fmt.Errorf("atrest: %s/%s already provisioned; use Rotate", tenant, keyID)
	}
	mk, err := newMasterKey(tenant, keyID, 1, material)
	if err != nil {
		return MasterKey{}, err
	}
	k.byTenantKey[tenant][keyID] = []*MasterKey{mk}
	return *mk, nil
}

// Rotate adds the next Active version and demotes the current Active to Rotated.
func (k *SyntheticKeyManager) Rotate(ctx context.Context, tenant, keyID string, material []byte) (MasterKey, error) {
	if tenant == "" || keyID == "" {
		return MasterKey{}, ErrInvalidArgument
	}
	if len(material) != 0 && len(material) != keyBytes {
		return MasterKey{}, ErrKeyLength
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	versions := k.byTenantKey[tenant][keyID]
	if len(versions) == 0 {
		return MasterKey{}, ErrUnknownKey
	}
	next := versions[len(versions)-1].Version + 1
	// Demote any currently-active version of this key id to rotated.
	for _, mk := range versions {
		if mk.Status == StatusActive {
			mk.Status = StatusRotated
		}
	}
	mk, err := newMasterKey(tenant, keyID, next, material)
	if err != nil {
		return MasterKey{}, err
	}
	k.byTenantKey[tenant][keyID] = append(k.byTenantKey[tenant][keyID], mk)
	return *mk, nil
}

// SetStatus changes a version's administrative status. A revoked key can never
// return to active.
func (k *SyntheticKeyManager) SetStatus(ctx context.Context, tenant, keyID string, version uint32, status KeyStatus) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	versions := k.byTenantKey[tenant][keyID]
	for _, mk := range versions {
		if mk.Version != version {
			continue
		}
		if mk.Status == StatusRevoked && status != StatusRevoked {
			return ErrKeyRevoked
		}
		mk.Status = status
		return nil
	}
	return ErrUnknownKey
}

// KeyVersions lists the known versions of a key for an operator/admin surface.
// Order is ascending by version. It exposes status only, never material.
func (k *SyntheticKeyManager) KeyVersions(tenant, keyID string) []MasterKey {
	k.mu.RLock()
	defer k.mu.RUnlock()
	versions := k.byTenantKey[tenant][keyID]
	out := make([]MasterKey, 0, len(versions))
	for _, mk := range versions {
		cp := *mk
		cp.material = nil
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out
}

// cloneKey returns a MasterKey whose material is a fresh copy, so callers that
// zero their derived key never touch the authority's stored copy.
func cloneKey(in MasterKey) MasterKey {
	out := in
	if in.material != nil {
		out.material = append([]byte(nil), in.material...)
	}
	return out
}

func newMasterKey(tenant, keyID string, version uint32, material []byte) (*MasterKey, error) {
	if len(material) == 0 {
		var err error
		material, err = randomBytes(keyBytes)
		if err != nil {
			return nil, err
		}
	}
	cp := make([]byte, keyBytes)
	copy(cp, material)
	return &MasterKey{
		Tenant:   tenant,
		KeyID:    keyID,
		Version:  version,
		Status:   StatusActive,
		Created:  time.Now().UTC(),
		material: cp,
	}, nil
}

// constantTimeEq is a helper for comparing secret bytes without early exit.
func constantTimeEq(a, b []byte) bool { return subtle.ConstantTimeCompare(a, b) == 1 }
