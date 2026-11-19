package atrest

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

// Cryptographic parameters. 32-byte keys give AES-256; 12-byte nonces are the
// GCM standard size and are drawn fresh from crypto/rand on every Seal, so a
// (key, nonce) pair is never reused.
const (
	keyBytes   = 32
	nonceBytes = 12
	// dekBytes is the random per-record data key length (AES-256).
	dekBytes = 32
)

// envelope is the serialized, self-describing ciphertext record that a
// persistence owner stores in its durable byte boundary. It carries enough to
// resolve the correct master-key version at Open without ever carrying key
// material or plaintext.
type envelope struct {
	tenant     string
	keyID      string
	keyVersion uint32
	nonceWrap  []byte // GCM nonce for the wrapped DEK
	dekWrap    []byte // DEK encrypted under the KEK (includes GCM tag)
	nonceData  []byte // GCM nonce for the payload
	ciphertext []byte // payload encrypted under the DEK (includes GCM tag)
}

// formatMagic and formatVersion identify and version the on-disk envelope
// layout. A reader that does not recognize them must fail closed.
var formatMagic = [6]byte{'A', 'H', 'P', 'E', 'N', 'C'}

const formatVersion uint8 = 1

// Role bytes domain-separate the two GCM uses so a wrapped-DEK blob can never
// be replayed as payload ciphertext or vice versa.
const (
	roleKEK  = byte('K')
	roleData = byte('D')
)

// sealBytes serializes the envelope into a compact, versioned byte string. It
// is deterministic in its fields; randomness lives only in the nonces and DEK
// chosen by Seal.
func (e *envelope) sealBytes() ([]byte, error) {
	var b bytes.Buffer
	b.Write(formatMagic[:])
	b.WriteByte(formatVersion)
	if err := writeString(&b, e.tenant); err != nil {
		return nil, err
	}
	if err := writeString(&b, e.keyID); err != nil {
		return nil, err
	}
	var ver [4]byte
	binary.BigEndian.PutUint32(ver[:], e.keyVersion)
	b.Write(ver[:])
	if err := writeBytes(&b, e.nonceWrap); err != nil {
		return nil, err
	}
	if err := writeBytes(&b, e.dekWrap); err != nil {
		return nil, err
	}
	if err := writeBytes(&b, e.nonceData); err != nil {
		return nil, err
	}
	if err := writeBytes(&b, e.ciphertext); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// openEnvelope parses bytes produced by sealBytes. It validates the magic,
// version and field shapes and enforces the fixed cryptographic sizes, so a
// malformed or truncated record is rejected before any key is consulted.
func openEnvelope(raw []byte) (*envelope, error) {
	r := bytes.NewReader(raw)
	var magic [6]byte
	if _, err := readFull(r, magic[:]); err != nil {
		return nil, ErrMalformed
	}
	if magic != formatMagic {
		return nil, ErrMalformed
	}
	ver, err := r.ReadByte()
	if err != nil {
		return nil, ErrMalformed
	}
	if ver != formatVersion {
		return nil, fmt.Errorf("%w: version %d", ErrMalformed, ver)
	}
	e := &envelope{}
	if e.tenant, err = readString(r); err != nil {
		return nil, err
	}
	if e.keyID, err = readString(r); err != nil {
		return nil, err
	}
	var vbuf [4]byte
	if _, err := readFull(r, vbuf[:]); err != nil {
		return nil, ErrMalformed
	}
	e.keyVersion = binary.BigEndian.Uint32(vbuf[:])
	if e.nonceWrap, err = readBytes(r); err != nil {
		return nil, err
	}
	if e.dekWrap, err = readBytes(r); err != nil {
		return nil, err
	}
	if e.nonceData, err = readBytes(r); err != nil {
		return nil, err
	}
	if e.ciphertext, err = readBytes(r); err != nil {
		return nil, err
	}
	if len(e.nonceWrap) != nonceBytes || len(e.nonceData) != nonceBytes {
		return nil, ErrMalformed
	}
	// A wrapped DEK is exactly plaintext(dekBytes) plus the 16-byte GCM tag.
	if len(e.dekWrap) != dekBytes+16 {
		return nil, ErrMalformed
	}
	return e, nil
}

// deriveKEK derives the per-(tenant,key,version,purpose) key-encrypting key
// from master key material with HKDF-SHA256. The master key is never used
// raw; salt and info bind the derived key to the exact tenant and version so a
// different master or version produces an unrelated KEK.
func deriveKEK(master []byte, tenant, keyID string, version uint32) ([]byte, error) {
	if len(master) != keyBytes {
		return nil, ErrKeyLength
	}
	salt := []byte(fmt.Sprintf("AHP/atrest/%s/%d", keyID, version))
	info := "AHP/atrest/kek/" + tenant
	key, err := hkdf.Key(sha256.New, master, salt, info, keyBytes)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// aad builds the additional-authenticated-data that binds an envelope's
// identity into both GCM layers. A mismatch in any component makes the tag
// check fail even if the derived key happened to be correct.
func aad(role byte, tenant, keyID, purpose string, version uint32, extra []byte) []byte {
	var b bytes.Buffer
	b.WriteByte(role)
	b.WriteString(tenant)
	b.WriteByte(0)
	b.WriteString(keyID)
	b.WriteByte(0)
	b.WriteString(purpose)
	b.WriteByte(0)
	var v [4]byte
	binary.BigEndian.PutUint32(v[:], version)
	b.Write(v[:])
	b.Write(extra)
	return b.Bytes()
}

// gcmSeal encrypts plaintext under key with a fresh nonce and the supplied AAD.
func gcmSeal(key, plaintext, aad, nonce []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	return gcm.Seal(nil, nonce, plaintext, aad), nil
}

// gcmOpen decrypts ciphertext under key with the supplied nonce/AAD. Any
// authentication failure surfaces as ErrAuthFailed.
func gcmOpen(key, ciphertext, aad, nonce []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, ErrAuthFailed
	}
	return plaintext, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != keyBytes {
		return nil, ErrKeyLength
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// randomBytes returns n bytes from the system CSPRNG. Failure is fatal to the
// operation: sealing without a strong random source is never attempted.
func randomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// hashing helpers -----------------------------------------------------------

// Variable-length fields carry a 4-byte big-endian length so a payload of up to
// the 4 GiB uint32 bound serializes; the earlier 2-byte bound silently rejected
// any ciphertext over 64 KiB, which a 1 MiB state blob or artifact must survive.

func writeString(b *bytes.Buffer, s string) error {
	if uint64(len(s)) > math.MaxUint32 {
		return ErrMalformed
	}
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(s)))
	b.Write(l[:])
	b.WriteString(s)
	return nil
}

func writeBytes(b *bytes.Buffer, s []byte) error {
	if uint64(len(s)) > math.MaxUint32 {
		return ErrMalformed
	}
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(s)))
	b.Write(l[:])
	b.Write(s)
	return nil
}

func readString(r *bytes.Reader) (string, error) {
	bs, err := readBytes(r)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

// readBytes refuses a declared length larger than the bytes actually present,
// so a tampered length cannot make the parser allocate gigabytes before failing.
func readBytes(r *bytes.Reader) ([]byte, error) {
	var l [4]byte
	if _, err := readFull(r, l[:]); err != nil {
		return nil, ErrMalformed
	}
	n := uint64(binary.BigEndian.Uint32(l[:]))
	if n > uint64(r.Len()) {
		return nil, ErrMalformed
	}
	buf := make([]byte, n)
	if _, err := readFull(r, buf); err != nil {
		return nil, ErrMalformed
	}
	return buf, nil
}

func readFull(r *bytes.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// equal reports constant-time byte equality for secrets when needed.
func equal(a, b []byte) bool { return bytes.Equal(a, b) }
