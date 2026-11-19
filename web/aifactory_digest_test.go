package web

import (
	"encoding/json"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// digestVectors are produced by the platform itself (go run
// ./web/testdata/digestvectors): every digest is contracts.Digest over a parsed
// registry.Manifest, i.e. exactly what Store.PutVersion recomputes before it
// rejects a caller-supplied digest. The browser side (web/aifactory/ahp-canonical.js,
// checked by web/aifactory_canonical.test.mjs) has to reproduce these bytes,
// otherwise the prototype cannot publish at all and the "publish → evaluation
// gate" leg stops being evidence.
type digestVectors struct {
	About   string `json:"_about"`
	Vectors []struct {
		Name     string          `json:"name"`
		Valid    bool            `json:"valid"`
		Digest   string          `json:"digest"`
		SameAs   string          `json:"same_digest_as,omitempty"`
		Manifest json.RawMessage `json:"manifest"`
	} `json:"manifests"`
	Values []struct {
		Name   string          `json:"name"`
		Digest string          `json:"digest"`
		Value  json.RawMessage `json:"value"`
	} `json:"canonical_values"`
}

func loadDigestVectors(t *testing.T) digestVectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/aifactory-digest-vectors.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var doc digestVectors
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	if len(doc.Vectors) == 0 || len(doc.Values) == 0 {
		t.Fatalf("vectors file is empty: %d manifests, %d values", len(doc.Vectors), len(doc.Values))
	}
	return doc
}

// TestPutVersionDigestVectorsAreThePlatformOnes pins the oracle itself: if the
// canonicalisation in pkg/contracts or the manifest schema in internal/registry
// ever changes, this test fails first and the JS side is then re-generated from
// the new vectors rather than silently diverging.
func TestPutVersionDigestVectorsAreThePlatformOnes(t *testing.T) {
	doc := loadDigestVectors(t)
	byName := make(map[string]string, len(doc.Vectors))
	for _, vector := range doc.Vectors {
		var manifest registry.Manifest
		if err := json.Unmarshal(vector.Manifest, &manifest); err != nil {
			t.Fatalf("%s: manifest does not parse: %v", vector.Name, err)
		}
		digest, err := contracts.Digest(manifest)
		if err != nil {
			t.Fatalf("%s: digest: %v", vector.Name, err)
		}
		if digest != vector.Digest {
			t.Errorf("%s: platform digest moved to %s, vector says %s — regenerate web/testdata/digestvectors",
				vector.Name, digest, vector.Digest)
		}
		byName[vector.Name] = digest

		// `valid` records whether the same form is publishable at all, so the
		// vector the UI copies from cannot rot into an invalid shape unnoticed.
		validationErr := registry.ValidateManifest(manifest)
		if vector.Valid && validationErr != nil {
			t.Errorf("%s: vector claims valid, ValidateManifest says %v", vector.Name, validationErr)
		}
		if !vector.Valid && validationErr == nil {
			t.Errorf("%s: vector claims invalid, but ValidateManifest accepted it", vector.Name)
		}

		// A digest hashes content, so unknown fields must not move it: the client
		// and the server would otherwise disagree about the same publication.
		if vector.SameAs != "" {
			base, ok := byName[vector.SameAs]
			if !ok {
				t.Fatalf("%s: references unknown vector %q", vector.Name, vector.SameAs)
			}
			if digest != base {
				t.Errorf("%s: digest %s must equal %s of %s", vector.Name, digest, base, vector.SameAs)
			}
		}
	}
}

// TestCanonicalValueVectors covers the generic canonical JSON rules (key order,
// UTF-8 comparison, encoding/json string escaping) on values a manifest cannot
// carry, so a future "simplification" of the client cannot pass by accident.
func TestCanonicalValueVectors(t *testing.T) {
	doc := loadDigestVectors(t)
	for _, vector := range doc.Values {
		digest, err := contracts.Digest(json.RawMessage(vector.Value))
		if err != nil {
			t.Fatalf("%s: digest: %v", vector.Name, err)
		}
		if digest != vector.Digest {
			t.Errorf("%s: platform digest %s, vector %s", vector.Name, digest, vector.Digest)
		}
	}
}

// TestAIFactoryCanonicalScriptIsEmbedded fails when the digest module stops
// shipping: the served UI would then be unable to compute a canonical digest,
// which is a silent loss of the publish path rather than a visible error.
func TestAIFactoryCanonicalScriptIsEmbedded(t *testing.T) {
	read := func(name string) string {
		t.Helper()
		body, err := fs.ReadFile(aifactoryFS, "aifactory/"+name)
		if err != nil {
			t.Fatalf("aifactory/%s: %v", name, err)
		}
		if len(body) == 0 {
			t.Fatalf("aifactory/%s: embedded copy is empty", name)
		}
		return string(body)
	}
	canonical := read("ahp-canonical.js")
	for _, marker := range []string{"AHP_CANONICAL", "manifestDigest", "publishManifest"} {
		if !strings.Contains(canonical, marker) {
			t.Errorf("ahp-canonical.js lost %q — the served UI can no longer compute a canonical digest", marker)
		}
	}
	index := read("index.html")
	canonicalAt := strings.Index(index, "ahp-canonical.js")
	liveAt := strings.Index(index, "ahp-live.js")
	if canonicalAt < 0 || liveAt < 0 {
		t.Fatalf("index.html must load both ahp-canonical.js and ahp-live.js")
	}
	if canonicalAt > liveAt {
		t.Errorf("ahp-canonical.js must load before ahp-live.js (canonicalAt=%d liveAt=%d)", canonicalAt, liveAt)
	}
}
