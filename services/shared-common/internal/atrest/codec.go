package atrest

import (
	"context"
	"crypto/sha256"
)

// Codec is the data-path handle a persistence owner uses to seal and open
// bytes at its durable boundary. It holds only the read-only KeyManager: it can
// never provision, rotate or read out a key, and it never accepts caller-chosen
// key material. The tenant and purpose arguments must come from the owner's
// authenticated scope, never from the payload being encrypted.
type Codec struct {
	kms KeyManager
}

// NewCodec wraps a KeyManager for sealing/opening. A nil manager is refused:
// there is no such thing as an unkeyed envelope.
func NewCodec(kms KeyManager) (*Codec, error) {
	if kms == nil {
		return nil, ErrInvalidArgument
	}
	return &Codec{kms: kms}, nil
}

// Seal encrypts plaintext for tenant under a purpose label and returns a
// self-describing envelope. purpose should be a stable, low-cardinality domain
// tag (for example "state.payload", "artifact.bytes", "memory.fact") that ties
// a ciphertext to its use so a blob cannot be replayed into another column.
//
// Seal fails closed if the tenant has no active key (unknown, disabled or
// revoked): it never writes a record it could not later read.
func (c *Codec) Seal(ctx context.Context, tenant, purpose string, plaintext []byte) ([]byte, error) {
	if tenant == "" || purpose == "" {
		return nil, ErrInvalidArgument
	}
	mk, err := c.kms.Current(ctx, tenant)
	if err != nil {
		return nil, err
	}
	dek, err := randomBytes(dekBytes)
	if err != nil {
		return nil, err
	}
	defer clearBytes(dek)
	kek, err := deriveKEK(mk.material, tenant, mk.KeyID, mk.Version)
	if err != nil {
		return nil, err
	}
	defer clearBytes(kek)
	nonceWrap, err := randomBytes(nonceBytes)
	if err != nil {
		return nil, err
	}
	nonceData, err := randomBytes(nonceBytes)
	if err != nil {
		return nil, err
	}
	dekWrap, err := gcmSeal(kek, dek, aad(roleKEK, tenant, mk.KeyID, purpose, mk.Version, nil), nonceWrap)
	if err != nil {
		return nil, err
	}
	dataAAD := dataAAD(tenant, mk.KeyID, purpose, mk.Version, dekWrap)
	ciphertext, err := gcmSeal(dek, plaintext, dataAAD, nonceData)
	if err != nil {
		return nil, err
	}
	e := &envelope{
		tenant:     tenant,
		keyID:      mk.KeyID,
		keyVersion: mk.Version,
		nonceWrap:  nonceWrap,
		dekWrap:    dekWrap,
		nonceData:  nonceData,
		ciphertext: ciphertext,
	}
	return e.sealBytes()
}

// Open decrypts an envelope produced by Seal. tenant and purpose must match the
// values used at Seal time; a mismatch fails closed. It returns ErrTenantMismatch
// if the envelope belongs to another tenant (a hard isolation violation, not a
// mere auth failure), and ErrKeyDisabled/ErrKeyRevoked if its key version is no
// longer usable. No plaintext is returned on any failure.
func (c *Codec) Open(ctx context.Context, tenant, purpose string, raw []byte) ([]byte, error) {
	if tenant == "" || purpose == "" {
		return nil, ErrInvalidArgument
	}
	e, err := openEnvelope(raw)
	if err != nil {
		return nil, err
	}
	if e.tenant != tenant {
		return nil, ErrTenantMismatch
	}
	mk, err := c.kms.Resolve(ctx, tenant, e.keyID, e.keyVersion)
	if err != nil {
		return nil, err
	}
	kek, err := deriveKEK(mk.material, tenant, e.keyID, e.keyVersion)
	if err != nil {
		return nil, err
	}
	defer clearBytes(kek)
	dek, err := gcmOpen(kek, e.dekWrap, aad(roleKEK, tenant, e.keyID, purpose, e.keyVersion, nil), e.nonceWrap)
	if err != nil {
		return nil, err
	}
	defer clearBytes(dek)
	dataAAD := dataAAD(tenant, e.keyID, purpose, e.keyVersion, e.dekWrap)
	return gcmOpen(dek, e.ciphertext, dataAAD, e.nonceData)
}

// dataAAD binds the payload layer to the wrapped DEK, so swapping dekWrap to a
// captured blob from another envelope breaks the payload tag even before the KEK
// layer is considered.
func dataAAD(tenant, keyID, purpose string, version uint32, dekWrap []byte) []byte {
	sum := sha256.Sum256(dekWrap)
	return aad(roleData, tenant, keyID, purpose, version, sum[:])
}
