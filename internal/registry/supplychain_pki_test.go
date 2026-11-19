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
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
)

// This file is the PUBLISHER and OPERATOR authority, and it exists only in the
// test binary: the product registry never signs anything. It mints a CA (the
// root an operator would install as trust material), publisher code-signing
// leaves, and DSSE/in-toto attestations -- assembling the published formats by
// hand from JSON so the verifier is checked against an independent encoder, not
// a shortcut that shares its internal types.

// testAuthority is an operator trust anchor: a CA plus the PEM the operator
// would hand to registry.NewTrustRootsFromPEM.
type testAuthority struct {
	cert  *x509.Certificate
	key   *ecdsa.PrivateKey
	caPEM []byte
}

func newAuthority(t *testing.T, cn string) *testAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ca parse: %v", err)
	}
	return &testAuthority{cert: cert, key: key, caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// mintPublisher mints a code-signing leaf whose SPIFFE URI SAN is the publisher
// identity the registry must see. ttl controls validity; a negative ttl yields
// an already-expired certificate for the negatives.
func (a *testAuthority) mintPublisher(t *testing.T, publisher string, ttl time.Duration) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "publisher:" + publisher},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(ttl),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning},
		URIs:         []*url.URL{spiffePublisher(publisher)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("leaf parse: %v", err)
	}
	pemLeaf := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return leaf, key, pemLeaf
}

// spiffePublisher builds the only URI SAN a publisher leaf may carry.
func spiffePublisher(p string) *url.URL {
	return &url.URL{Scheme: "spiffe", Host: "ahp", Path: "/publisher/" + p}
}

// attestationSpec names every fact a signed statement binds. The signer turns
// it into a real DSSE envelope; the verifier must re-derive exactly these.
type attestationSpec struct {
	OCIDigestHex    string // 64-hex, no prefix: the subject digest
	ImageRef        string // repo@sha256:<hex>
	VersionDigest   string // sha256:<hex>
	SBOMDigest      string // sha256:<hex>
	EvalVerdictDgst string // sha256:<hex>
	SecurityVerdict string
	ExpiresAt       time.Time
}

// sign builds and signs one DSSE/in-toto attestation for spec under the given
// publisher leaf. It implements the DSSE PAE independently of the product code.
func sign(t *testing.T, leaf *x509.Certificate, key *ecdsa.PrivateKey, spec attestationSpec) registry.Attestation {
	t.Helper()
	statement := map[string]any{
		"_type": "https://in-toto.io/Statement/v1",
		"subject": []any{
			map[string]any{"name": spec.ImageRef, "digest": map[string]string{"sha256": spec.OCIDigestHex}},
		},
		"predicateType": "https://ahp-platform.github.io/attestations/supply-chain/v1",
		"predicate": map[string]any{
			"imageRef":          spec.ImageRef,
			"versionDigest":     spec.VersionDigest,
			"sbomDigest":        spec.SBOMDigest,
			"evalVerdictDigest": spec.EvalVerdictDgst,
			"securityVerdictId": spec.SecurityVerdict,
			"expiresAt":         spec.ExpiresAt,
			"builder":           "tests/ahp-sdk",
		},
	}
	payload, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("statement json: %v", err)
	}
	const payloadType = "application/vnd.in-toto+json"
	b64 := base64.StdEncoding.EncodeToString(payload)
	pae := []byte(fmt.Sprintf("DSSEv1 %d %s %d %s", len(payloadType), payloadType, len(payload), payload))
	digest := sha256.Sum256(pae)
	sigDER, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	leafDER := leaf.Raw
	sum := sha256.Sum256(leafDER)
	pemLeaf := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	return registry.Attestation{
		PayloadType: payloadType,
		Payload:     b64,
		Signatures: []registry.AttestationSignature{{
			KeyID: hex.EncodeToString(sum[:]),
			Sig:   base64.StdEncoding.EncodeToString(sigDER),
			Cert:  string(pemLeaf),
		}},
	}
}

// makeImage returns a content-addressed image reference whose digest is the real
// sha256 of the returned bytes -- an honest immutable OCI-style digest, not an
// arbitrary hex string. (Pushing/pulling a live OCI registry is a separate proof
// limit; the digest here is computed the way OCI computes it: over content.)
func makeImage(t *testing.T, tag string) (imageRef string, digestHex string, content []byte) {
	t.Helper()
	content = []byte(fmt.Sprintf(`{"ociVersion":"1.1.0","mediaType":"application/vnd.oci.image.manifest.v1+json","ahpTestImage":%q,"nonce":%q}`, tag, randSuffix(t)))
	sum := sha256.Sum256(content)
	digestHex = hex.EncodeToString(sum[:])
	lower := strings.ToLower(tag)
	return "registry.local/ahp/" + "agent" + ":" + lower + "@sha256:" + digestHex, digestHex, content
}

// makeSBOM returns an SBOM document bound to the image and its content digest.
func makeSBOM(t *testing.T, imageRef string) (sbom []byte, sbomDigest string) {
	t.Helper()
	sbom = []byte(`{"bomFormat":"CycloneDX","specVersion":"1.5","components":[{"type":"container","purl":"` + imageRef + `"}]}`)
	sum := sha256.Sum256(sbom)
	return sbom, "sha256:" + hex.EncodeToString(sum[:])
}
