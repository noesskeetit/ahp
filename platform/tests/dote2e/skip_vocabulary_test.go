package dote2e_test

// The skip vocabulary belongs to the gate, not to this lane
// (DEF-GATE-SKIP-POLICY: scripts/skip-verdict.mjs, owner Russell, tip dd9e47a2).
//
// Why this test exists: the mechanism reads a `go test -json` stream and refuses to
// count an unmeasured leg as a pass. It distinguishes three kinds of skip - an
// ENVIRONMENT absence (`SKIP_ENV[tag=…] reason=… missing=…`), a DECLARED not-covered
// leg (no token, but listed with a justification in the allow-file it is run with),
// and everything else, which is a violation. Before this commit this lane emitted
// six unexplained skips: green in `go test`, invisible to the gate.
//
// The lane therefore CONSUMES the vocabulary: it does not restate the tag classes or
// invent tokens of its own. Tags that are not yet in the mechanism's registry are
// reported here so Russell/Volta can register canonical names; that is their naming
// decision, and this test only refuses unexplained silence and undeclared tokens.
//
// The registry moved during this band and the leg moved with it: the canonical
// dictionary is now the tracked data file tools/skip-tags.json (owner Volta), while the
// TAG_CLASS literal in scripts/skip-verdict.mjs is deliberately empty and gets the
// dictionary injected via --tags. A check that kept reading the literal would have gone
// on printing that a canonical tag is unregistered - a wrong signal is worse than none,
// which is why the leg reads the data file first and refuses a registry it cannot parse.
//
// Two rules from that registry are enforced here rather than trusted, because both were
// broken by this lane: a tag whose status is "deprecated" must not gain new emissions
// (real_deps was frozen as an umbrella: one token does not say what was not covered),
// and a tag whose class is opt_in_only must be emitted as SKIP_EXPLICIT, because a
// nobody-chose-it skip dressed as a nothing-was-supplied skip misreports the lane.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const skipAllowFile = "skip-not-covered.json"

// registryFileName names the file in the messages, so a reader opens the right one.
const registryFileName = "tools/skip-tags.json"

// skipCallRe finds a skip call; the token has to be inside the call's own text, which
// is why the check below reads the call and the two lines after it (the message can
// wrap).
var skipCallRe = regexp.MustCompile(`t\.Skip(f|f?)(\(|\s*\()`)

var (
	skipEnvTokenRe      = regexp.MustCompile(`SKIP_ENV\[tag=([A-Za-z0-9_.-]+)\]`)
	skipExplicitTokenRe = regexp.MustCompile(`SKIP_EXPLICIT\b(?:\[tag=([A-Za-z0-9_.-]+)\])?`)
	reasonRe            = regexp.MustCompile(`reason=\S+`)
	missingRe           = regexp.MustCompile(`missing=\S+`)
)

// tagMeta is the registry entry this leg needs: the class a tag maps to and whether the
// owner still allows new emissions under that name.
type tagMeta struct {
	Class  string `json:"class"`
	Status string `json:"status"`
}

// loadTagRegistry reads the dictionary the gate itself reads - the tracked data file - and
// with it the CLOSED CLASS ALPHABET the registry publishes. Nothing is copied: a copied list
// drifts, and a drifted check is worse than none.
//
// The old fallback to the literal in scripts/skip-verdict.mjs is retired for this leg: the
// literal carries tag names but no classes, and a leg that reads tags without their class
// alphabet cannot run its own rules. Judging form-against-class with an unknown alphabet
// produces green out of ignorance, so the leg refuses instead.
func loadTagRegistry(t *testing.T) (map[string]tagMeta, []string, string) {
	t.Helper()
	moduleRoot := filepath.Join("..", "..")
	if dataPath := filepath.Join(moduleRoot, "tools", "skip-tags.json"); fileExists(dataPath) {
		raw, err := os.ReadFile(dataPath)
		if err != nil {
			t.Fatalf("read the canonical tag registry: %v", err)
		}
		var doc struct {
			Tags          map[string]tagMeta `json:"tags"`
			ClosedClasses []string           `json:"closed_classes"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("the canonical tag registry does not parse (%v): it became data, and a leg that silently stops reading it would stop meaning anything", err)
		}
		if len(doc.Tags) == 0 {
			t.Fatal("the canonical tag registry holds no tags: a check against an empty dictionary passes by construction")
		}
		if len(doc.ClosedClasses) == 0 {
			t.Fatal("the canonical tag registry declares no closed_classes: without the class alphabet this leg cannot tell a registered class from a typo, and it will not answer green out of ignorance")
		}
		return doc.Tags, doc.ClosedClasses, "tools/skip-tags.json"
	}
	t.Fatalf("no canonical tag registry at %s: this leg judges a skip FORM against a class, so it needs the class alphabet and refuses to invent one",
		filepath.Join(moduleRoot, "tools", "skip-tags.json"))
	return nil, nil, ""
}

// registryClassProblem checks one registry entry against the closed alphabet before the
// leg is allowed to use it. An empty or unknown class used to be a hole: the two rules
// below compare meta.Class against "opt_in_only", so a drifted value made both branches
// false and the leg kept walking - it did not fail, it stopped judging that pair, which
// is the failure mode this band was refused for once already.
//
// Who owns which red: an EMITTED tag missing from the registry belongs to
// tests/hygiene/skip_tags_test.go (it reddens the repository-wide dictionary gap as
// UNKNOWN_SKIP_TAG), and so does a class outside closed_classes there. This leg refuses
// earlier and for its own reason: it is about to evaluate a rule on that class, and a
// drifted class must abort the leg rather than silently disable the rule.
func registryClassProblem(tag, class string, closed []string) string {
	value := strings.TrimSpace(class)
	if value == "" {
		return fmt.Sprintf("tag=%s carries an EMPTY class in %s: the form/class rules cannot be evaluated on a registry entry that names no class - register it with a class from closed_classes %v",
			tag, registryFileName, closed)
	}
	for _, known := range closed {
		if known == value {
			return ""
		}
	}
	return fmt.Sprintf("tag=%s declares class %q which is outside the closed alphabet %v in %s: a class the mechanism does not implement cannot colour a verdict, and here it would silently switch the rules off - fix the registry, do not widen the alphabet",
		tag, value, closed, registryFileName)
}

// declaredUncovered is the allow-file the gate is run with: entries it declares as
// not-covered rather than environment-absent.
type declaredUncovered struct {
	Package       string `json:"package"`
	Name          string `json:"name"`
	Justification string `json:"justification"`
}

func readDeclaredUncovered(t *testing.T) map[string]declaredUncovered {
	t.Helper()
	raw, err := os.ReadFile(skipAllowFile)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]declaredUncovered{}
		}
		t.Fatalf("read %s: %v", skipAllowFile, err)
	}
	var entries []declaredUncovered
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("parse %s: %v", skipAllowFile, err)
	}
	out := map[string]declaredUncovered{}
	for _, e := range entries {
		if strings.TrimSpace(e.Name) == "" || strings.TrimSpace(e.Justification) == "" ||
			strings.TrimSpace(e.Package) == "" {
			t.Fatalf("%s: every entry needs package, name and justification (the gate refuses the file otherwise): %+v", skipAllowFile, e)
		}
		out[e.Name] = e
	}
	return out
}

// enclosingTestName is a lexical helper, not a parser: the files are gofmt'ed and one
// Test function per skip site is what this lane writes.
func enclosingTestName(lines []string, idx int) string {
	for i := idx; i >= 0; i-- {
		if strings.HasPrefix(lines[i], "func Test") {
			name := strings.TrimPrefix(lines[i], "func ")
			if paren := strings.IndexByte(name, '('); paren > 0 {
				return name[:paren]
			}
		}
	}
	return ""
}

// TestRegistryClassMustComeFromTheClosedAlphabet pins the rule itself, so it cannot be
// lost by a refactor: an empty or unknown class must produce a reason, never the empty
// string that lets the leg walk past the site.
func TestRegistryClassMustComeFromTheClosedAlphabet(t *testing.T) {
	closed := []string{"real_store_env", "database_env", "harness_precondition_env", "opt_in_only"}
	cases := []struct {
		name     string
		class    string
		wantRed  bool
		contains string
	}{
		{"a registered class is usable", "harness_precondition_env", false, ""},
		{"empty class", "", true, "EMPTY class"},
		{"whitespace class", "   ", true, "EMPTY class"},
		{"class outside the alphabet", "not_a_class", true, "outside the closed alphabet"},
		{"near-miss spelling", "harness_precondition", true, "outside the closed alphabet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason := registryClassProblem("dot_e2e_layout", tc.class, closed)
			if tc.wantRed && reason == "" {
				t.Fatalf("class %q must be refused; an empty answer here is the leg disabling its own rule", tc.class)
			}
			if !tc.wantRed && reason != "" {
				t.Fatalf("class %q must be accepted, got %q", tc.class, reason)
			}
			if tc.wantRed && !strings.Contains(reason, tc.contains) {
				t.Fatalf("the refusal must name %q so a reader fixes the registry: %s", tc.contains, reason)
			}
		})
	}
}

func TestLaneSkipsSpeakTheGateVocabulary(t *testing.T) {
	registry, closedClasses, registrySource := loadTagRegistry(t)
	declared := readDeclaredUncovered(t)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob package: %v", err)
	}
	sort.Strings(files)
	var silence, unregistered, deprecated, nonOptIn []string
	usedTags := map[string]int{}
	for _, name := range files {
		if name == "skip_vocabulary_test.go" {
			continue // this file quotes the grammar; it is not a skip site
		}
		raw, readErr := os.ReadFile(name)
		if readErr != nil {
			t.Fatalf("read %s: %v", name, readErr)
		}
		lines := strings.Split(string(raw), "\n")
		for i, line := range lines {
			if !skipCallRe.MatchString(line) {
				continue
			}
			end := i + 3
			if end > len(lines) {
				end = len(lines)
			}
			call := strings.Join(lines[i:end], " ")
			testName := enclosingTestName(lines, i)
			if testName == "" {
				testName = "package helper (not inside a Test function)"
			}
			envToken := skipEnvTokenRe.FindStringSubmatch(call)
			explicitToken := skipExplicitTokenRe.FindStringSubmatch(call)
			tag, kind := "", ""
			switch {
			case envToken != nil:
				tag, kind = envToken[1], "SKIP_ENV"
			case explicitToken != nil && explicitToken[1] != "":
				tag, kind = explicitToken[1], "SKIP_EXPLICIT"
			}
			if tag == "" {
				if _, ok := declared[testName]; ok {
					continue // declared not-covered, with a justification, as the gate allows
				}
				silence = append(silence, name+":"+strconv.Itoa(i+1)+" in "+testName)
				continue
			}
			where := name + ":" + strconv.Itoa(i+1)
			usedTags[tag]++
			if !reasonRe.MatchString(call) || !missingRe.MatchString(call) {
				silence = append(silence, where+" token without reason=/missing=")
			}
			if registry == nil {
				continue
			}
			meta, known := registry[tag]
			if !known {
				unregistered = append(unregistered, tag)
				continue
			}
			// A drifted class aborts the leg. Falling through here would leave both
			// form/class branches false and the site unjudged - green from silence.
			if reason := registryClassProblem(tag, meta.Class, closedClasses); reason != "" {
				t.Fatalf("%s (site %s): %s", "registry drift", where, reason)
			}
			if strings.EqualFold(meta.Status, "deprecated") {
				deprecated = append(deprecated, where+" tag="+tag+" (status deprecated: "+meta.Class+")")
			}
			if meta.Class == "opt_in_only" && kind != "SKIP_EXPLICIT" {
				nonOptIn = append(nonOptIn, where+" tag="+tag+" emitted as "+kind+
					" although its class is opt_in_only: a nobody-chose-it skip must not be reported as a missing dependency")
			}
			if meta.Class != "opt_in_only" && kind == "SKIP_EXPLICIT" {
				nonOptIn = append(nonOptIn, where+" tag="+tag+" emitted as SKIP_EXPLICIT although its class is "+meta.Class+
					": a missing dependency must not be dressed up as a choice")
			}
		}
	}
	if len(silence) > 0 {
		t.Fatalf("%d skip site(s) do not speak the gate vocabulary: %s; a skip must carry SKIP_ENV[tag=…] reason=… missing=… or be listed in %s with a justification - otherwise the gate counts an unmeasured leg as a pass",
			len(silence), strings.Join(silence, ", "), skipAllowFile)
	}
	// A declared entry nobody skips any more is stale: it would hide a future silent
	// skip under an old name.
	for name := range declared {
		if !declaredNameStillUsed(name) {
			t.Fatalf("%s lists %q but that test no longer exists - delete the entry, or the file hides the next silent skip under an old name", skipAllowFile, name)
		}
	}
	if registry == nil {
		t.Fatal("no tag registry in this tree (neither tools/skip-tags.json nor scripts/skip-verdict.mjs): this leg cannot claim the lane speaks the gate vocabulary without the dictionary it speaks")
	}
	if len(deprecated) > 0 {
		t.Fatalf("%d skip site(s) emit a deprecated tag: %s; the registry froze that name because one umbrella token does not say which dependency was not supplied - name the dependency",
			len(deprecated), strings.Join(deprecated, ", "))
	}
	if len(nonOptIn) > 0 {
		t.Fatalf("%d skip site(s) use the wrong token form for their class: %s; SKIP_ENV means 'nothing was supplied', SKIP_EXPLICIT means 'nobody opted in' - mixing them lies to the gate about the colour a leg deserves",
			len(nonOptIn), strings.Join(nonOptIn, "; "))
	}
	if len(unregistered) > 0 {
		t.Logf("tags this lane uses but the registry (%s) does not classify yet - canonical naming is the mechanism owner's call: %s",
			registrySource, strings.Join(dedupe(unregistered), ", "))
	}
	t.Logf("registry read from %s (%d tags); tags this lane emits: %v", registrySource, len(registry), sortedIntKeys(usedTags))
}

// declaredNameStillUsed checks the allow-file against reality: the named test must
// still exist in this package.
func declaredNameStillUsed(name string) bool {
	files, err := filepath.Glob("*.go")
	if err != nil {
		return false
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		if strings.Contains(string(raw), "func "+name+"(t *testing.T)") {
			return true
		}
	}
	return false
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// sortedIntKeys keeps the log line stable across runs so two receipts diff cleanly.
func sortedIntKeys(counts map[string]int) []string {
	out := make([]string, 0, len(counts))
	for key := range counts {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
