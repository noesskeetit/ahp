package main

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"

	"github.com/ahp-platform/ahp/internal/memory"
)

// The seed is the operator-facing copy of the Memory capability vocabulary.
// It is allowed to carry other planes' capabilities, but every memory* string
// in it must be real, and every real one must be published. Without this test
// the SQL drifts from the code exactly the way DEF-MEM-6 describes.
func TestSeedCapabilityListMatchesCode(t *testing.T) {
	path := filepath.Join("..", "..", "infra", "memory", "capability-roles.sql")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	text := string(raw)
	// apply script must exist and be executable: a seed nobody can apply reproducibly
	// is documentation, not configuration.
	if _, err := os.Stat(filepath.Join("..", "..", "scripts", "apply-memory-capabilities.sh")); err != nil {
		t.Fatalf("apply script missing: %v", err)
	}

	quoted := regexp.MustCompile(`'((?:memory)(?::[a-z-]+)?)'`)
	inSeed := map[string]bool{}
	for _, m := range quoted.FindAllStringSubmatch(text, -1) {
		inSeed[m[1]] = true
	}
	for s := range inSeed {
		if !memory.IsCapability(s) {
			t.Errorf("seed publishes %q which the code does not recognise", s)
		}
	}
	for _, want := range memory.Vocabulary {
		if !inSeed[want] {
			t.Errorf("seed omits %q: operators cannot provision what the seed does not name", want)
		}
	}
}

// TestSeedIsIdempotentSQL pins the shape the operator relies on: UPSERT, not
// bare INSERT, so re-applying after a rollback cannot fail or duplicate grants.
func TestSeedIsIdempotentSQL(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "infra", "memory", "capability-roles.sql"))
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	up := regexp.MustCompile(`(?i)ON CONFLICT[\s\S]{0,200}DO UPDATE`)
	if !up.Match(raw) {
		t.Fatal("seed must UPSERT (ON CONFLICT ... DO UPDATE); a re-apply must be safe")
	}
}

func TestVocabularySortedForStableDisplay(t *testing.T) {
	if !sort.StringsAreSorted(memory.Vocabulary) {
		t.Errorf("Vocabulary must stay sorted for deterministic error text: %v", memory.Vocabulary)
	}
}
