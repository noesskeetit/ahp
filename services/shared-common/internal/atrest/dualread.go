package atrest

import "context"

// This file implements the dual-mode read primitive that lets a persistence
// owner turn encryption on without breaking readers of records written before
// it was enabled. An owner stores the envelope for new writes and, on read,
// transparently opens envelopes while passing legacy plaintext through unchanged.
// There is no silent enable: only bytes that carry the envelope magic are ever
// decrypted, and a genuine envelope always authenticates (fail-closed).

// IsEnvelope reports whether raw carries the atrest envelope magic, i.e. it was
// produced by Codec.Seal at the current format version. It is a cheap prefix
// test used to choose the dual-read branch and never touches keys.
func IsEnvelope(raw []byte) bool {
	if len(raw) < len(formatMagic)+1 {
		return false
	}
	for i, b := range formatMagic {
		if raw[i] != b {
			return false
		}
	}
	return raw[len(formatMagic)] == formatVersion
}

// OpenTransparent decrypts an envelope or passes through a plaintext record that
// predates encryption, returning whether the record was sealed. A non-envelope
// is returned unchanged with a nil error, so a reader that predates the feature
// keeps working while new writes are sealed. A real envelope that fails to
// authenticate still fails closed — passthrough never weakens a genuine record.
func (c *Codec) OpenTransparent(ctx context.Context, tenant, purpose string, raw []byte) (plain []byte, wasSealed bool, err error) {
	if !IsEnvelope(raw) {
		return raw, false, nil
	}
	pt, err := c.Open(ctx, tenant, purpose, raw)
	if err != nil {
		return nil, true, err
	}
	return pt, true, nil
}
