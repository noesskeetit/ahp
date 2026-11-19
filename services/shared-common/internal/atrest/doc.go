// Package atrest provides encryption-at-rest with per-tenant key binding for
// AHP persistence owners (State payloads, Artifact bytes, Memory facts,
// Workspace blobs, Tool results, runtime secrets). It is the mechanism the
// PDF rev3.2 §8.3 requires ("tenants ... own ... KMS keys") and §10 maps onto
// the delivery tiers.
//
// # What it is
//
// The package implements envelope encryption with reviewed upstream
// primitives only — AES-256-GCM (NIST SP 800-38D) for authenticated
// encryption and HKDF-SHA256 (RFC 5869) for key derivation, both from the Go
// standard library. There is no bespoke cipher, no custom MAC, and no
// hand-rolled KDF. Every cryptographic primitive here is a thin, audited
// wrapper around crypto/aes, crypto/cipher, crypto/hkdf and crypto/rand.
//
// The scheme is the standard KMS envelope pattern:
//
//   - A per-record Data Encryption Key (DEK) is drawn from crypto/rand and
//     encrypts the payload with AES-256-GCM. Data is authenticated; any byte
//     change is detected at Open.
//   - The DEK is itself encrypted under a Key-Encrypting Key (KEK) derived
//     with HKDF from the tenant's master key. The KEK is bound to
//     (tenant, key id, key version, purpose) so a ciphertext cannot be read
//     under a different tenant, key or version even by an attacker who holds
//     the master key: the binding lives both in the HKDF inputs and in the
//     GCM additional-authenticated-data of both layers.
//
// # Trust model — key authority, not task bearer
//
// Keys are operator/tenant authority. The KeyManager read interface carries no
// method that accepts caller-supplied key material or lets a data-path caller
// select another tenant's key. Mutating operations (Provision / Rotate /
// SetStatus) live on a separate KeyAuthority interface that ordinary owners
// never hold, and the tenant argument to Seal/Open must come from an
// authenticated server-side scope, never from the plaintext being encrypted.
//
// # Delivery tiers (PDF §10) and honest limits
//
// This local profile demonstrates Tier 0 (shared SaaS, per-tenant operator KMS)
// and a BYOK code path matching Tier 1 (tenant-supplied master key material).
// Tier 2A (customer PG/S3/KMS) reuses the same envelope with the KeyAuthority
// backed by a customer-held provider; Tier 2B (whole sovereign cell) is a
// deployment fact, not something a library can prove. Physical disk/volume
// encryption, backup-media confidentiality and customer residency are separate
// environment evidence and are NOT claimed here. See docs/implementation-pitfalls.md.
package atrest
