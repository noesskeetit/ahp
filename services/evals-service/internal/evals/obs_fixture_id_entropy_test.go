package evals_test

// obs_fixture_id_entropy_test.go -- insurance against the defect class that put
// the wide acceptance gate red on a RE-RUN: a fixture id built by truncating a
// prefixed digest. SetPolicy refusing to re-declare a used id with different
// content is correct ("one id, one content"), so the only way a suite survives a
// second run against a store that still holds the first run's rows is for its
// fixture ids to carry real entropy. The floor below is the 32 bits named by the
// requirement; the control case keeps the check from losing its teeth.

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// Eight hex characters is 32 bits, the stated floor. The collision loop is what
// turns that floor into a guarantee against the shape that actually failed: with
// more draws than the truncated shape has values, a duplicate is arithmetic,
// while a 32-bit space collides over this many draws with probability ~1e-5.
const obsMinFixtureIDHexChars = 8

func TestObsFixtureIDsCarryEnoughEntropyToSurviveARerun(t *testing.T) {
	const draws = 512
	seen := make(map[string]int, draws)
	for i := 0; i < draws; i++ {
		id := randID(t)
		if len(id) < obsMinFixtureIDHexChars {
			t.Fatalf("fixture id %q holds fewer than %d hex characters: a re-run against a persistent store reuses ids and SetPolicy answers CONFLICT", id, obsMinFixtureIDHexChars)
		}
		if _, err := hex.DecodeString(id); err != nil {
			t.Fatalf("fixture id %q is not raw hex, so its length does not describe its entropy: %v", id, err)
		}
		if strings.Contains(id, "sha256:") {
			t.Fatalf("fixture id %q embeds a digest prefix, which a truncation keeps while dropping the entropy behind it", id)
		}
		if first, dup := seen[id]; dup {
			t.Fatalf("fixture id %q drawn twice (draw %d and %d of %d): this id space cannot serve a suite that re-runs against a store holding its earlier rows", id, first, i, draws)
		}
		seen[id] = i
	}
}

// TestObsTruncatedPrefixedDigestIsNotAnID is the control case: the exact shape
// the gate died on. Eight characters of "sha256:" plus hex is the prefix and ONE
// hex digit, so at most sixteen draws exist and a later draw colliding is
// guaranteed. If this stops reproducing, the leg above stopped measuring.
func TestObsTruncatedPrefixedDigestIsNotAnID(t *testing.T) {
	const draws = 512
	seen := make(map[string]int, 16)
	collisions := 0
	for i := 0; i < draws; i++ {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			t.Fatalf("rand: %v", err)
		}
		sum := sha256.Sum256(b[:])
		truncated := ("sha256:" + hex.EncodeToString(sum[:]))[:8]
		if _, dup := seen[truncated]; dup {
			collisions++
			continue
		}
		seen[truncated] = i
	}
	if len(seen) > 16 {
		t.Fatalf("the truncated shape produced %d distinct ids, at most 16 exist: this control no longer models the defect", len(seen))
	}
	if collisions == 0 {
		t.Fatalf("the truncated shape produced no collision in %d draws: the leg above no longer has anything to measure", draws)
	}
}
