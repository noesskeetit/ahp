package atrest

import (
	"context"
	"fmt"
	"sync"
)

// DynamicAuthority is the KeyManager for a long-running product binary whose
// tenant set is not known at start-up. It provisions tenant keys on first use
// with the exact same material-resolution rules as LoadAuthorityFromEnv
// (BYOK first, then deterministic derivation from the operator root secret),
// so keys survive restarts. It deliberately FAILS CLOSED when neither root
// secret nor tenant BYOK material is configured: a volatile in-memory random
// key would seal data that cannot be opened after a restart, which is data
// loss, not security.
//
// This remains the SYNTHETIC authority (marked, not a vendor KMS): the real
// KMS integration is an external prerequisite tracked in the residual matrix.
type DynamicAuthority struct {
	auth    *SyntheticKeyManager
	root    []byte
	byok    map[string][]byte
	mu      sync.Mutex
	ensured map[string]bool
}

// LoadDynamicAuthorityFromEnv builds a DynamicAuthority from the operator
// profile. Enabling it without any durable material (root secret file or at
// least a BYOK entry) fails closed here, at start-up, not at first write.
func LoadDynamicAuthorityFromEnv(ctx context.Context) (*DynamicAuthority, error) {
	byok, err := loadByok()
	if err != nil {
		return nil, err
	}
	root, err := loadRootSecret()
	if err != nil {
		return nil, err
	}
	if root == nil && len(byok) == 0 {
		return nil, fmt.Errorf("atrest: dynamic authority requires %s or %s when encryption is enabled", EnvRootSecretFile, EnvByokSecretFile)
	}
	return &DynamicAuthority{auth: NewSyntheticKeyManager(), root: root, byok: byok, ensured: map[string]bool{}}, nil
}

func (d *DynamicAuthority) ensure(ctx context.Context, tenant string) error {
	if tenant == "" {
		return ErrInvalidArgument
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ensured[tenant] {
		return nil
	}
	var material []byte
	switch {
	case d.byok[tenant] != nil:
		material = d.byok[tenant]
	case d.root != nil:
		derived, err := deriveKEK(d.root, tenant, "root", 1)
		if err != nil {
			return err
		}
		material = derived
	default:
		return ErrInvalidArgument
	}
	if _, err := d.auth.Provision(ctx, tenant, "root", material); err != nil {
		return err
	}
	clearBytes(material)
	d.ensured[tenant] = true
	return nil
}

// Current provisions the tenant on demand, then delegates.
func (d *DynamicAuthority) Current(ctx context.Context, tenant string) (MasterKey, error) {
	if err := d.ensure(ctx, tenant); err != nil {
		return MasterKey{}, err
	}
	return d.auth.Current(ctx, tenant)
}

// NewDynamicAuthority builds a dynamic SYNTHETIC authority from a durable root
// secret, for embedders and tests that hold the root out of band. Production
// binaries use LoadDynamicAuthorityFromEnv; this constructor exists so a caller
// can mount the seam with a known root without going through the environment.
// It keeps the SYNTHETIC marking: no vendor KMS is implied.
func NewDynamicAuthority(root []byte) *DynamicAuthority {
	cp := make([]byte, len(root))
	copy(cp, root)
	return &DynamicAuthority{auth: NewSyntheticKeyManager(), root: cp, byok: map[string][]byte{}, ensured: map[string]bool{}}
}

// Resolve needs no provisioning: an envelope names an existing key version.
func (d *DynamicAuthority) Resolve(ctx context.Context, tenant, keyID string, version uint32) (MasterKey, error) {
	return d.auth.Resolve(ctx, tenant, keyID, version)
}
