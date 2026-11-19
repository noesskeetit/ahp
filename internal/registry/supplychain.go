// Supply-chain verification for the Registry deploy admission path.
//
// The PDF pipeline (rev 3.2, c. 41 "Конвейер, установка, изоляция и каталог")
// fixes the order: "ahp-sdk check, сборка image@sha256, SBOM и подпись. Затем
// версия в Registry, dev, stage с оценками ... проверка ИБ и prod". CI therefore
// produces, for every immutable OCI image, an SBOM and a SIGNATURE; the registry
// must not trust a caller's word that either exists -- it verifies them against
// operator-controlled trust roots and binds the result to the image's real
// content digest.
//
// This file implements that verification using only the Go standard library's
// crypto and x509 packages and two public, versioned formats:
//
//   - DSSE, the "Dead Simple Signing Envelope" (secure-systems-lab
//     go-securesystemslib, DSSE_PAE pre-authentication encoding), and
//   - in-toto Statement v1 (https://in-toto.io/Statement/v1).
//
// Nothing here invents a signature scheme: ECDSA-P256/SHA-256 over the DSSE PAE
// is checked with crypto/ecdsa, and the signing certificate is validated as an
// X.509 chain to an operator root pool with crypto/x509. The registry only ever
// VERIFIES; keys belong to publishers and to the operator, never to this code,
// and no boolean "attested"/"verified" flag supplied by a caller is trusted.
package registry

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"strings"
	"time"
)

const (
	// inTotoPayloadType is the DSSE payload type for an in-toto Statement.
	inTotoPayloadType = "application/vnd.in-toto+json"
	// inTotoStatementType is the in-toto Statement v1 discriminator.
	inTotoStatementType = "https://in-toto.io/Statement/v1"
	// supplyChainPredicateType names the AHP predicate that pins one image to
	// its SBOM and to the eval/security verdict records that cleared it.
	supplyChainPredicateType = "https://ahp-platform.github.io/attestations/supply-chain/v1"
	// publisherSPIFFEPrefix is the only URI SAN a signing leaf may carry. The
	// SPIFFE scheme matches the workload identities the platform already uses.
	publisherSPIFFEPrefix = "spiffe://ahp/publisher/"

	maxAttestationPayload = 1 << 20 // 1 MiB signed statement
	maxAttestationSigs    = 8
	maxAttestationCertPEM = 1 << 16
	// Cached receipts from the former base64-text verifier are not valid proof
	// under the DSSE protocol. Only a fresh successful verification sets this.
	supplyChainVerificationContract = "dsse-v1-raw-payload"
)

// Attestation is a DSSE envelope wrapping one in-toto Statement that binds a
// publisher's signed claim to exactly one immutable OCI image.
type Attestation struct {
	PayloadType string                 `json:"payloadType"`
	Payload     string                 `json:"payload"` // base64 of the in-toto Statement JSON
	Signatures  []AttestationSignature `json:"signatures"`
}

// AttestationSignature is one DSSE signature. KeyID is hex(sha256(leaf DER));
// Sig is base64 of the ECDSA ASN.1/DER signature over the DSSE PAE; Cert is the
// PEM leaf that produced it, carried inline the way a cosign bundle does so the
// verifier does not have to fetch a key out of band.
type AttestationSignature struct {
	KeyID string `json:"keyid"`
	Sig   string `json:"sig"`
	Cert  string `json:"cert,omitempty"`
}

// inTotoStatement is the subset of in-toto Statement v1 this verifier decodes.
type inTotoStatement struct {
	Type          string          `json:"_type"`
	Subject       []inTotoSubject `json:"subject"`
	PredicateType string          `json:"predicateType"`
	Predicate     json.RawMessage `json:"predicate"`
}

type inTotoSubject struct {
	Name   string            `json:"name,omitempty"`
	Digest map[string]string `json:"digest"`
}

// supplyChainPredicate is the predicate carried by the statement. Its fields
// bind the signed image to the SBOM and to the eval/security verdicts; the
// verifier checks the SBOM digest against bytes it is given, and the eval and
// security references against the registry's own verdict planes.
type supplyChainPredicate struct {
	ImageRef          string    `json:"imageRef"`
	VersionDigest     string    `json:"versionDigest"`
	SBOMDigest        string    `json:"sbomDigest"`
	EvalVerdictDigest string    `json:"evalVerdictDigest"`
	SecurityVerdictID string    `json:"securityVerdictId"`
	ExpiresAt         time.Time `json:"expiresAt"`
	Builder           string    `json:"builder,omitempty"`
}

// AttestedFacts is what verification proves about an attestation. It is derived
// only from the signature, certificate and statement -- never from a caller
// assertion -- and is what the admission record persists.
type AttestedFacts struct {
	Publisher         string    `json:"publisher"`
	OCIDigest         string    `json:"oci_digest"`
	VersionDigest     string    `json:"version_digest"`
	SBOMDigest        string    `json:"sbom_digest"`
	EvalVerdictDigest string    `json:"eval_verdict_digest"`
	SecurityVerdictID string    `json:"security_verdict_id"`
	ExpiresAt         time.Time `json:"expires_at"`
}

// TrustRoots is the operator-controlled anchor set. A Registry enforces the
// supply chain only once an operator installs the trust roots here, so who to
// trust is an operator authority decision, not a caller or per-task choice.
type TrustRoots struct {
	pool *x509.CertPool
	now  func() time.Time
}

// NewTrustRootsFromPEM builds operator trust roots from one or more PEM-encoded
// CA certificates. An empty or unparsable bundle is a construction error: an
// operator that means to enforce the supply chain must supply real roots, and
// the Registry never falls back to "trust anything."
func NewTrustRootsFromPEM(pemBundle []byte) (*TrustRoots, error) {
	if len(pemBundle) == 0 {
		return nil, invalid("supply chain: trust roots require at least one PEM CA certificate")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBundle) {
		return nil, invalid("supply chain: trust roots contained no parsable PEM certificate")
	}
	return &TrustRoots{pool: pool, now: func() time.Time { return time.Now().UTC() }}, nil
}

// WithClock returns a copy of the trust roots whose time source is replaced.
// Every expiry the supply chain judges -- leaf-certificate validity, attestation
// lifetime, and (through Store.now) admission and verdict freshness -- is read
// from that one source, so replacing it moves the whole plane at once instead of
// leaving one check on wall time while another uses the harness clock.
//
// Production must not call it: the operator installs roots built from wall time,
// and a request can never reach this method. It exists so expiry boundaries can
// be exercised deterministically by the acceptance harness rather than by
// sleeping or by hoping the wall clock crosses a boundary mid-test.
//
// A nil receiver stays nil and a nil clock is left as it was, so a caller that
// conditionally injects a clock cannot accidentally disable the supply chain.
func (t *TrustRoots) WithClock(now func() time.Time) *TrustRoots {
	if t == nil {
		return nil
	}
	copied := *t
	if now != nil {
		copied.now = now
	}
	return &copied
}

// dssePAE builds the DSSE pre-authentication encoding exactly as the DSSE spec
// defines it: "DSSEv1" SP len(type) SP type SP len(body) SP body. The body is
// decoded SERIALIZED_BODY bytes, not its base64 transport representation.
func dssePAE(payloadType string, payload []byte) []byte {
	return []byte(fmt.Sprintf("DSSEv1 %d %s %d %s",
		len(payloadType), payloadType, len(payload), payload))
}

// DSSE v1.0.2 permits standard and URL-safe base64 transport encodings. Their
// spelling has no bearing on the signed bytes. Raw forms are also unambiguous.
func decodeDSSEBase64(value string) ([]byte, error) {
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.URLEncoding, base64.RawStdEncoding, base64.RawURLEncoding} {
		if decoded, err := encoding.DecodeString(value); err == nil {
			return decoded, nil
		}
	}
	return nil, fmt.Errorf("invalid DSSE base64")
}

// Verify checks one attestation against the operator trust roots and returns the
// facts it proves. It enforces, in order: a well-formed DSSE envelope; an
// ECDSA/ASN.1 signature over the DSSE PAE that validates under a certificate
// chaining to an operator root with the code-signing EKU inside its validity
// window; a publisher identity taken from the leaf's SPIFFE URI; the statement's
// subject digest equalling the exact OCI digest it is given; and the provided
// SBOM bytes hashing to the SBOM digest the statement signed. Any failure is a
// supply-chain rejection, not a soft warning.
func (t *TrustRoots) Verify(att Attestation, sbom []byte, ociDigest string) (*AttestedFacts, error) {
	if t == nil || t.pool == nil {
		return nil, invalid("supply chain: verifier has no trust roots")
	}
	if att.PayloadType != inTotoPayloadType {
		return nil, invalid("supply chain: unexpected DSSE payloadType")
	}
	if len(att.Signatures) < 1 || len(att.Signatures) > maxAttestationSigs {
		return nil, invalid("supply chain: attestation must carry between 1 and 8 signatures")
	}
	rawPayload, err := decodeDSSEBase64(att.Payload)
	if err != nil || len(rawPayload) == 0 || len(rawPayload) > maxAttestationPayload {
		return nil, invalid("supply chain: DSSE payload is not a usable base64 document")
	}
	// Every signature in the envelope must validate; a stray or wrong one is a
	// rejection rather than something to ignore.
	var publisher string
	pae := dssePAE(att.PayloadType, rawPayload)
	for i := range att.Signatures {
		got, err := t.verifySignature(att.Signatures[i], pae)
		if err != nil {
			return nil, err
		}
		if publisher == "" {
			publisher = got
		} else if publisher != got {
			return nil, invalid("supply chain: signatures are attributed to different publishers")
		}
	}

	var stmt inTotoStatement
	if err := json.Unmarshal(rawPayload, &stmt); err != nil {
		return nil, invalid("supply chain: signed payload is not an in-toto Statement")
	}
	if stmt.Type != inTotoStatementType {
		return nil, invalid("supply chain: signed payload is not an in-toto Statement v1")
	}
	if stmt.PredicateType != supplyChainPredicateType {
		return nil, invalid("supply chain: unexpected predicateType")
	}
	var pred supplyChainPredicate
	if err := json.Unmarshal(stmt.Predicate, &pred); err != nil {
		return nil, invalid("supply chain: supply-chain predicate is malformed")
	}

	// The signed subject must name the exact immutable OCI digest being admitted.
	wantHex := strings.ToLower(strings.TrimPrefix(ociDigest, "sha256:"))
	if _, err := hex.DecodeString(wantHex); err != nil || len(wantHex) != 64 {
		return nil, invalid("supply chain: requested OCI digest is malformed")
	}
	bound := false
	for _, subj := range stmt.Subject {
		if strings.EqualFold(subj.Digest["sha256"], wantHex) {
			bound = true
			break
		}
	}
	if !bound {
		return nil, invalid("supply chain: attestation is not bound to the requested image digest")
	}

	// The predicate's image reference must itself be pinned to the exact digest
	// being admitted, so the signed repo@sha256 reference and the subject digest
	// can never disagree.
	if ociDigestOfImage(pred.ImageRef) != "sha256:"+wantHex {
		return nil, invalid("supply chain: predicate imageRef is not pinned to the admitted digest")
	}

	// The SBOM must be present and must hash to the digest the statement signed.
	if len(sbom) == 0 {
		return nil, invalid("supply chain: a matching SBOM is required")
	}
	sbomDigest := sbomDigestOf(sbom)
	if !strings.EqualFold(sbomDigest, pred.SBOMDigest) {
		return nil, invalid("supply chain: SBOM does not match the signed attestation")
	}

	if err := ValidateVersionDigest(pred.VersionDigest); err != nil {
		return nil, invalid("supply chain: predicate version digest is malformed")
	}
	if err := ValidateVersionDigest(pred.EvalVerdictDigest); err != nil {
		return nil, invalid("supply chain: predicate eval verdict digest is malformed")
	}
	// A clearance reference is optional HERE and mandatory WHERE it counts. The
	// CI pipeline signs provenance before the security authority ("проверка ИБ")
	// has looked at the image, so requiring a reference here would put the dev
	// step behind a stage verdict -- a cycle the pipeline cannot break without
	// faking a clearance. An attestation without one therefore proves only the
	// supply chain (publisher, digest, SBOM, evaluation), and requireDeployAdmission
	// refuses stage/prod until an admission exists whose signature binds the
	// currently passing clearance.
	if t.now().After(pred.ExpiresAt) {
		return nil, invalid("supply chain: attestation has expired")
	}

	return &AttestedFacts{
		Publisher:         publisher,
		OCIDigest:         "sha256:" + wantHex,
		VersionDigest:     pred.VersionDigest,
		SBOMDigest:        sbomDigest,
		EvalVerdictDigest: pred.EvalVerdictDigest,
		SecurityVerdictID: pred.SecurityVerdictID,
		ExpiresAt:         pred.ExpiresAt.UTC(),
	}, nil
}

// verifySignature validates one DSSE signature against the operator roots and
// returns the publisher identity taken from the signing leaf.
func (t *TrustRoots) verifySignature(sig AttestationSignature, pae []byte) (string, error) {
	if strings.TrimSpace(sig.Cert) == "" || len(sig.Cert) > maxAttestationCertPEM {
		return "", invalid("supply chain: signature must carry an inline PEM certificate")
	}
	block, rest := pem.Decode([]byte(sig.Cert))
	if block == nil || block.Type != "CERTIFICATE" || len(strings.TrimSpace(string(rest))) != 0 {
		return "", invalid("supply chain: signature certificate is not a single PEM certificate")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", invalid("supply chain: signature certificate is unparsable")
	}
	sum := sha256.Sum256(leaf.Raw)
	if !strings.EqualFold(sig.KeyID, hex.EncodeToString(sum[:])) {
		return "", invalid("supply chain: signature keyid does not match the certificate")
	}

	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:       t.pool,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning},
		CurrentTime: t.now(),
	}); err != nil {
		return "", invalid("supply chain: signing certificate does not chain to an operator trust root")
	}

	pub, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return "", invalid("supply chain: signing certificate must carry an ECDSA public key")
	}
	sigBytes, err := decodeDSSEBase64(sig.Sig)
	if err != nil {
		return "", invalid("supply chain: signature is not base64")
	}
	digest := sha256.Sum256(pae)
	if !ecdsa.VerifyASN1(pub, digest[:], sigBytes) {
		return "", invalid("supply chain: signature does not verify over the image attestation")
	}

	return publisherFromLeaf(leaf)
}

// publisherFromLeaf reads the publisher identity from the leaf's SPIFFE URI SAN.
// A leaf without exactly one publisher URI cannot author an admission.
func publisherFromLeaf(leaf *x509.Certificate) (string, error) {
	var publisher string
	for _, uri := range leaf.URIs {
		trimmed := strings.TrimPrefix(uri.String(), publisherSPIFFEPrefix)
		if trimmed == uri.String() {
			// Not a publisher URI; ignore other SANs.
			continue
		}
		if trimmed == "" || strings.Contains(trimmed, "/") {
			return "", invalid("supply chain: certificate publisher URI is malformed")
		}
		if publisher != "" && publisher != trimmed {
			return "", invalid("supply chain: certificate carries multiple publishers")
		}
		publisher = trimmed
	}
	if publisher == "" {
		return "", invalid("supply chain: certificate carries no publisher identity")
	}
	if err := requireID("publisher", publisher); err != nil {
		return "", err
	}
	return publisher, nil
}

// sbomDigestOf is the content digest an SBOM is bound by: sha256 over its exact
// bytes, in the same "sha256:<hex>" form the platform uses for every digest.
func sbomDigestOf(sbom []byte) string {
	sum := sha256.Sum256(sbom)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// ociDigestOfImage extracts the immutable "sha256:<hex>" digest from an
// image@sha256:... reference. It returns "" for a reference that is not pinned.
func ociDigestOfImage(ref string) string {
	i := strings.Index(ref, "@sha256:")
	if i < 0 || !imageRefPattern.MatchString(ref) {
		return ""
	}
	return ref[i+1:]
}
