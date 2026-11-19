package registry_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
)

func trustRoots(t *testing.T, a *testAuthority) *registry.TrustRoots {
	t.Helper()
	roots, err := registry.NewTrustRootsFromPEM(a.caPEM)
	if err != nil {
		t.Fatalf("trust roots: %v", err)
	}
	return roots
}

// A good attestation from a publisher whose certificate chains to the installed
// trust root, bound to the exact image digest and SBOM, verifies and yields the
// facts the signature actually carried.
func TestVerifyGoodAttestation(t *testing.T) {
	auth := newAuthority(t, "operator-root")
	roots := trustRoots(t, auth)
	leaf, key, _ := auth.mintPublisher(t, "pub-acme", time.Hour)

	imageRef, digestHex, _ := makeImage(t, "good")
	sbom, sbomDigest := makeSBOM(t, imageRef)
	versionDigest := "sha256:" + hexOf("version-"+imageRef)
	evalDigest := versionDigest
	att := sign(t, leaf, key, attestationSpec{
		OCIDigestHex: digestHex, ImageRef: imageRef, VersionDigest: versionDigest,
		SBOMDigest: sbomDigest, EvalVerdictDgst: evalDigest, SecurityVerdict: "sec-1",
		ExpiresAt: time.Now().Add(time.Hour),
	})

	facts, err := roots.Verify(att, sbom, "sha256:"+digestHex)
	if err != nil {
		t.Fatalf("good attestation rejected: %v", err)
	}
	if facts.Publisher != "pub-acme" {
		t.Fatalf("publisher = %q, want pub-acme", facts.Publisher)
	}
	if facts.OCIDigest != "sha256:"+digestHex || facts.SBOMDigest != sbomDigest {
		t.Fatalf("bound digests wrong: %+v", facts)
	}
	if facts.VersionDigest != versionDigest || facts.EvalVerdictDigest != evalDigest {
		t.Fatalf("verdict/version digests wrong: %+v", facts)
	}
}

// A signature from a CA the operator did not install must not verify, no matter
// how well-formed it is: trust roots are the operator's, not the caller's.
func TestVerifyRejectsUntrustedRoot(t *testing.T) {
	operator := newAuthority(t, "operator-root")
	attacker := newAuthority(t, "attacker-root")
	roots := trustRoots(t, operator)
	leaf, key, _ := attacker.mintPublisher(t, "pub-acme", time.Hour)

	imageRef, digestHex, _ := makeImage(t, "untrusted")
	sbom, sbomDigest := makeSBOM(t, imageRef)
	att := sign(t, leaf, key, attestationSpec{
		OCIDigestHex: digestHex, ImageRef: imageRef, VersionDigest: hexPrefixed("v"),
		SBOMDigest: sbomDigest, EvalVerdictDgst: hexPrefixed("e"), SecurityVerdict: "s",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if _, err := roots.Verify(att, sbom, "sha256:"+digestHex); err == nil {
		t.Fatal("attestation from an untrusted CA verified")
	}
}

// The verifier binds the signature to the digest it is asked about. A signature
// made for one image can never admit a different image.
func TestVerifyRejectsWrongDigest(t *testing.T) {
	auth := newAuthority(t, "operator-root")
	roots := trustRoots(t, auth)
	leaf, key, _ := auth.mintPublisher(t, "pub-acme", time.Hour)

	imageRef, digestHex, _ := makeImage(t, "signed")
	otherRef, otherHex, _ := makeImage(t, "other")
	sbom, sbomDigest := makeSBOM(t, imageRef)
	att := sign(t, leaf, key, attestationSpec{
		OCIDigestHex: digestHex, ImageRef: imageRef, VersionDigest: hexPrefixed("v"),
		SBOMDigest: sbomDigest, EvalVerdictDgst: hexPrefixed("e"), SecurityVerdict: "s",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	// Ask to verify the signed attestation against a different image digest.
	if _, err := roots.Verify(att, sbom, "sha256:"+otherHex); err == nil {
		t.Fatalf("attestation for %s admitted image %s", shortHex(digestHex), shortHex(otherHex))
	}
	_ = otherRef
}

// A missing SBOM, or an SBOM whose bytes do not hash to the signed digest, is a
// supply-chain rejection ("отсутствующий/несовпадающий SBOM -> reject").
func TestVerifyRejectsMissingAndMismatchedSBOM(t *testing.T) {
	auth := newAuthority(t, "operator-root")
	roots := trustRoots(t, auth)
	leaf, key, _ := auth.mintPublisher(t, "pub-acme", time.Hour)

	imageRef, digestHex, _ := makeImage(t, "sbom")
	sbom, sbomDigest := makeSBOM(t, imageRef)
	att := sign(t, leaf, key, attestationSpec{
		OCIDigestHex: digestHex, ImageRef: imageRef, VersionDigest: hexPrefixed("v"),
		SBOMDigest: sbomDigest, EvalVerdictDgst: hexPrefixed("e"), SecurityVerdict: "s",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if _, err := roots.Verify(att, nil, "sha256:"+digestHex); err == nil {
		t.Fatal("missing SBOM accepted")
	}
	tampered := append(append([]byte{}, sbom...), ' ')
	if _, err := roots.Verify(att, tampered, "sha256:"+digestHex); err == nil {
		t.Fatal("mismatched SBOM accepted")
	}
}

// A signature flipped after the fact must not verify -- this is what makes the
// binding cryptographic rather than a claim.
func TestVerifyRejectsTamperedSignature(t *testing.T) {
	auth := newAuthority(t, "operator-root")
	roots := trustRoots(t, auth)
	leaf, key, _ := auth.mintPublisher(t, "pub-acme", time.Hour)

	imageRef, digestHex, _ := makeImage(t, "tamper")
	sbom, sbomDigest := makeSBOM(t, imageRef)
	att := sign(t, leaf, key, attestationSpec{
		OCIDigestHex: digestHex, ImageRef: imageRef, VersionDigest: hexPrefixed("v"),
		SBOMDigest: sbomDigest, EvalVerdictDgst: hexPrefixed("e"), SecurityVerdict: "s",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	raw, _ := base64.StdEncoding.DecodeString(att.Signatures[0].Sig)
	raw[len(raw)-1] ^= 0x01
	att.Signatures[0].Sig = base64.StdEncoding.EncodeToString(raw)
	if _, err := roots.Verify(att, sbom, "sha256:"+digestHex); err == nil {
		t.Fatal("tampered signature accepted")
	}
}

// An expired signing certificate or an expired attestation window both refuse
// admission; time-bounded trust is part of the guarantee.
func TestVerifyRejectsExpiredCertificateAndAttestation(t *testing.T) {
	auth := newAuthority(t, "operator-root")
	roots := trustRoots(t, auth)
	imageRef, digestHex, _ := makeImage(t, "expiry")
	sbom, sbomDigest := makeSBOM(t, imageRef)
	base := attestationSpec{
		OCIDigestHex: digestHex, ImageRef: imageRef, VersionDigest: hexPrefixed("v"),
		SBOMDigest: sbomDigest, EvalVerdictDgst: hexPrefixed("e"), SecurityVerdict: "s",
	}

	expiredCertLeaf, expiredKey, _ := auth.mintPublisher(t, "pub-acme", -time.Minute)
	att := sign(t, expiredCertLeaf, expiredKey, withExpiry(base, time.Now().Add(time.Hour)))
	if _, err := roots.Verify(att, sbom, "sha256:"+digestHex); err == nil {
		t.Fatal("expired certificate accepted")
	}

	freshLeaf, freshKey, _ := auth.mintPublisher(t, "pub-acme", time.Hour)
	att2 := sign(t, freshLeaf, freshKey, withExpiry(base, time.Now().Add(-time.Minute)))
	if _, err := roots.Verify(att2, sbom, "sha256:"+digestHex); err == nil {
		t.Fatal("expired attestation window accepted")
	}
}

// A certificate without the code-signing EKU cannot author an attestation even
// though it chains to the root: the usage is constrained, not just the chain.
func TestVerifyRejectsNonCodeSigningEKU(t *testing.T) {
	auth := newAuthority(t, "operator-root")
	roots := trustRoots(t, auth)
	leaf, key, pemLeaf := mintWithEKU(t, auth, "pub-acme", x509.ExtKeyUsageServerAuth)
	imageRef, digestHex, _ := makeImage(t, "eku")
	sbom, sbomDigest := makeSBOM(t, imageRef)
	att := signWithPEM(t, leaf, key, pemLeaf, attestationSpec{
		OCIDigestHex: digestHex, ImageRef: imageRef, VersionDigest: hexPrefixed("v"),
		SBOMDigest: sbomDigest, EvalVerdictDgst: hexPrefixed("e"), SecurityVerdict: "s",
		ExpiresAt: time.Now().Add(time.Hour),
	})
	if _, err := roots.Verify(att, sbom, "sha256:"+digestHex); err == nil {
		t.Fatal("server-auth certificate used to sign an attestation")
	}
}

// mintWithEKU mints a publisher leaf with an arbitrary EKU, for the negative.
func mintWithEKU(t *testing.T, a *testAuthority, publisher string, eku x509.ExtKeyUsage) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "publisher:" + publisher},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{eku},
		URIs:         []*url.URL{{Scheme: "spiffe", Host: "ahp", Path: "/publisher/" + publisher}},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	leaf, _ := x509.ParseCertificate(der)
	return leaf, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// signWithPEM is sign() but carries a caller-supplied PEM (the EKU leaf).
func signWithPEM(t *testing.T, leaf *x509.Certificate, key *ecdsa.PrivateKey, pemLeaf []byte, spec attestationSpec) registry.Attestation {
	t.Helper()
	statement := map[string]any{
		"_type":         "https://in-toto.io/Statement/v1",
		"subject":       []any{map[string]any{"name": spec.ImageRef, "digest": map[string]string{"sha256": spec.OCIDigestHex}}},
		"predicateType": "https://ahp-platform.github.io/attestations/supply-chain/v1",
		"predicate": map[string]any{
			"imageRef": spec.ImageRef, "versionDigest": spec.VersionDigest, "sbomDigest": spec.SBOMDigest,
			"evalVerdictDigest": spec.EvalVerdictDgst, "securityVerdictId": spec.SecurityVerdict, "expiresAt": spec.ExpiresAt,
		},
	}
	payload, _ := json.Marshal(statement)
	const payloadType = "application/vnd.in-toto+json"
	b64 := base64.StdEncoding.EncodeToString(payload)
	pae := []byte("DSSEv1 " + itoa(len(payloadType)) + " " + payloadType + " " + itoa(len(payload)) + " " + string(payload))
	sum := sha256.Sum256(pae)
	sigDER, err := ecdsa.SignASN1(rand.Reader, key, sum[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	csum := sha256.Sum256(leaf.Raw)
	return registry.Attestation{
		PayloadType: payloadType, Payload: b64,
		Signatures: []registry.AttestationSignature{{KeyID: hex.EncodeToString(csum[:]), Sig: base64.StdEncoding.EncodeToString(sigDER), Cert: string(pemLeaf)}},
	}
}

func withExpiry(s attestationSpec, exp time.Time) attestationSpec { s.ExpiresAt = exp; return s }
func hexOf(s string) string                                       { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) }
func hexPrefixed(s string) string                                 { return "sha256:" + hexOf(s) }
func shortHex(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}
func itoa(n int) string { return base64.StdEncoding.EncodeToString([]byte{}) + intToStr(n) }
func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
