package dote2e_test

// Lane identity-carrier self-check (director directive 2026-11-10, after Russell's independent
// review of the quota-seed band).
//
// The product guard internal/model/routing/config_copies_test.go pins the EXACT set of files that
// may mention a provider/model identity: the routed model is declared once, in
// infra/model/providers.json, and every other mention is a copy that has to converge on that file.
// This lane is a CONSUMER of the identity and never a declarer: a leg that needs the model reads it
// back from the generation row (journey_test.go records generation.Model) or sends AHP_LLM_MODEL, so
// a route change has to move the leg instead of silently disagreeing with a literal.
//
// Two fixtures in this lane carried the routed identity as a literal, which made the lane an
// unaccounted copy and reddened the wide gate. This file is the regression, and it is deliberately
// stricter than the product guard for this directory: no file here may mention such an identity at
// all, ledgered or not. It re-implements only the DISCOVERY rule (family prefix + shape), not the
// product ledger, so it cannot drift into a second source of truth -- the product guard stays the
// authority, and if its rule changes this one may over- or under-match.
//
// The rule is duplicated instead of imported because the guard lives in another package's _test
// files (not importable), and depending on a file this lane must not edit is worse than a small,
// documented copy of eight lines of regex.
//
// Family names are stored bare on purpose: a family word without a dash is prose to the discovery
// rule, so this file stays invisible to the guard it emulates, and the only identity-shaped string in
// the tests below is assembled at runtime.

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var providerFamilies = []string{"qwen", "gpt", "deepseek", "claude", "llama"}

var (
	laneIdentityTokenRe = regexp.MustCompile(`(?:` + strings.Join(providerFamilies, `|`) + `)[A-Za-z0-9_.-]*`)
	laneModelShapeRe    = regexp.MustCompile(`^[a-z]+(-[a-z0-9]+)+$`)
	laneProfileShapeRe  = regexp.MustCompile(`^[a-z]+[0-9]+(-[a-z0-9]+)*$`)
	laneHostShapeRe     = regexp.MustCompile(`^[a-z]+(\.[a-z0-9-]+)+$`)
	laneSourceExtRe     = map[string]bool{
		".go": true, ".md": true, ".json": true, ".sh": true, ".mjs": true, ".js": true,
		".py": true, ".ts": true, ".sql": true, ".yaml": true, ".yml": true, ".txt": true,
		".log": true, ".sum": true, ".mod": true, ".toml": true, ".env": true, ".csv": true,
	}
)

// laneIdentityShaped mirrors identityShaped in the product guard: an identity is written in lower
// case with a dash (or a digit glued on, or dots for a host); a Go identifier glued to a family name
// or a file name is not one.
func laneIdentityShaped(token string) bool {
	if token != strings.ToLower(token) {
		return false
	}
	if laneSourceExtRe[filepath.Ext(token)] {
		return false
	}
	return laneModelShapeRe.MatchString(token) || laneProfileShapeRe.MatchString(token) || laneHostShapeRe.MatchString(token)
}

// laneIdentitiesIn returns every identity-shaped token in one piece of content.
func laneIdentitiesIn(content string) []string {
	var out []string
	for _, match := range laneIdentityTokenRe.FindAllString(content, -1) {
		if laneIdentityShaped(match) {
			out = append(out, strings.ToLower(match))
		}
	}
	return out
}

// scanLaneForIdentities walks this lane's own directory and reports "rel:line token" for every
// mention. Sub-directories (customagent/, harness/) are lane content too, so they are included.
func scanLaneForIdentities(root string) ([]string, error) {
	var hits []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			if info.Name() == ".git" || info.Name() == "__pycache__" || info.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil || len(raw) == 0 {
			return nil
		}
		probe := raw
		if len(probe) > 8192 {
			probe = probe[:8192]
		}
		if strings.ContainsRune(string(probe), 0) {
			return nil // binary: the product guard skips these too
		}
		rel, _ := filepath.Rel(root, path)
		for i, line := range strings.Split(string(raw), "\n") {
			for _, token := range laneIdentitiesIn(line) {
				hits = append(hits, rel+":"+strconv.Itoa(i+1)+" "+token)
			}
		}
		return nil
	})
	return hits, err
}

// TestThisLaneMentionsNoProviderIdentity is the lane's own half of the guard: a leg must read the
// identity back, never restate it. A failure here means this lane became a second declarer of a
// provider identity and the wide gate will (rightly) demand a ledger entry for it.
func TestThisLaneMentionsNoProviderIdentity(t *testing.T) {
	hits, err := scanLaneForIdentities(".")
	if err != nil {
		t.Fatalf("lane scan: %v", err)
	}
	if len(hits) > 0 {
		t.Fatalf("this lane must consume provider identities, not carry them: the routed model is declared once in "+
			"infra/model/providers.json and a leg reads it from the generation row or AHP_LLM_MODEL. carriers:\n  %s",
			strings.Join(hits, "\n  "))
	}
}

// TestLaneIdentityScanFiresOnASyntheticCarrier keeps the self-check from passing because it is broken:
// a synthetic carrier (assembled at runtime, so this file stays invisible to the rule) must be caught,
// while prose, file names and Go identifiers glued to a family name must not.
func TestLaneIdentityScanFiresOnASyntheticCarrier(t *testing.T) {
	family := providerFamilies[0]
	carrier := family + "-38-27b" + "-fp8"
	hits := laneIdentitiesIn(`const model = "` + carrier + `"`)
	if len(hits) != 1 || hits[0] != carrier {
		t.Fatalf("the scan must catch a carrier, got %v", hits)
	}
	for _, clean := range []string{
		"// the " + family + " route is declared once",
		"native-" + family + "-assert.sh",
		family + "Model",
		family + ".go",
		strings.ToUpper(family) + "-38-27b-fp8",
	} {
		if got := laneIdentitiesIn(clean); len(got) != 0 {
			t.Errorf("%q must not read as an identity (prose, file name, Go identifier or uppercase), got %v", clean, got)
		}
	}
}
