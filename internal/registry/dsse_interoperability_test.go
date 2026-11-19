package registry_test

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
)

// The independent signer below follows DSSE protocol v1.0.2: PAE authenticates
// the decoded SERIALIZED_BODY, not the envelope's base64 text. The reference
// vector is DSSEv1 29 http://example.com/HelloWorld 11 hello world.
// https://github.com/dsse-lf/dsse/blob/master/protocol.md
func TestVerifyDSSEProtocolRawPayloadInteroperability(t *testing.T) {
	authority := newAuthority(t, "interop-root")
	roots := trustRoots(t, authority)
	leaf, key, _ := authority.mintPublisher(t, "interop-publisher", time.Hour)
	image, digest, _ := makeImage(t, "interop")
	sbom, sbomDigest := makeSBOM(t, image)
	att := sign(t, leaf, key, attestationSpec{OCIDigestHex: digest, ImageRef: image,
		VersionDigest: hexPrefixed("version"), SBOMDigest: sbomDigest, EvalVerdictDgst: hexPrefixed("eval"),
		ExpiresAt: time.Now().Add(time.Hour)})
	decoded, err := base64.StdEncoding.DecodeString(att.Payload)
	if err != nil {
		t.Fatal(err)
	}
	var statement map[string]any
	if err := json.Unmarshal(decoded, &statement); err != nil {
		t.Fatal(err)
	}
	statement["predicate"].(map[string]any)["builder"] = "protocol-interop-🧠"
	decoded, err = json.Marshal(statement)
	if err != nil {
		t.Fatal(err)
	}
	pae := []byte(fmt.Sprintf("DSSEv1 %d %s %d %s", len(att.PayloadType), att.PayloadType, len(decoded), decoded))
	hash := sha256.Sum256(pae)
	signature, err := ecdsa.SignASN1(rand.Reader, key, hash[:])
	if err != nil {
		t.Fatal(err)
	}
	for name, encoding := range map[string]*base64.Encoding{
		"standard": base64.StdEncoding, "url-safe": base64.URLEncoding,
		"raw-standard": base64.RawStdEncoding, "raw-url-safe": base64.RawURLEncoding,
	} {
		t.Run(name, func(t *testing.T) {
			candidate := att
			candidate.Signatures = append([]registry.AttestationSignature(nil), att.Signatures...)
			candidate.Payload = encoding.EncodeToString(decoded)
			candidate.Signatures[0].Sig = encoding.EncodeToString(signature)
			if _, err := roots.Verify(candidate, sbom, "sha256:"+digest); err != nil {
				t.Fatalf("standard DSSE signature rejected: %v", err)
			}
		})
	}
	t.Run("base64-text-is-not-the-signed-body", func(t *testing.T) {
		att.Payload = base64.StdEncoding.EncodeToString(decoded)
		wrong := sha256.Sum256([]byte(fmt.Sprintf("DSSEv1 %d %s %d %s", len(att.PayloadType), att.PayloadType, len(att.Payload), att.Payload)))
		sig, err := ecdsa.SignASN1(rand.Reader, key, wrong[:])
		if err != nil {
			t.Fatal(err)
		}
		att.Signatures[0].Sig = base64.StdEncoding.EncodeToString(sig)
		if _, err := roots.Verify(att, sbom, "sha256:"+digest); err == nil {
			t.Fatal("nonstandard signature over base64 envelope text accepted")
		}
	})
}
