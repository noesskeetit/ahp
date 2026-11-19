package dote2e_test

// No value harvested by the gate's harvest rule may live in this lane's sources
// (DEF-SECRET-HYGIENE-HARNESS-TEST, director 2026-11-10), and this file used to carry
// a second defect of its own (DEF-TEST-CHECKOUT-COUPLING, P2, director 06:49Z).
//
// What actually happened, because the honest version is the useful one. The merge
// gate refused the frame-free band: three files here carried a value the lane's
// operational stores also carry under the key name j1_task_grant_token. Reading the
// stores showed the value is a TERMINAL-STATE LABEL this harness publishes in
// terminal_frame_states, not a credential; it entered the guard's value set only
// because the guard harvests by KEY NAME and a map key named after leg j1 ends in
// _token. Both halves were real, so both were fixed: the harness stopped publishing a
// harvestable spelling at all (terminal_frame_vocabulary = "v2"), and this file got
// the check that would have caught the two occurrences already sitting in the
// integration branch.
//
// Then the checkout coupling surfaced: the policy and the stores this file read were
// UNTRACKED files outside the repository, found by walking UP past the module root.
// In a clean detached worktree of the exact SHA both legs failed ("no lane root found
// above /"); in the headquarters layout they passed only because the walk happened to
// hit private/pdf-closure-20261102/{tools,evidence}. A reviewer checking the accepted
// SHA elsewhere was guaranteed to see two red legs - a local proof sold as a
// reproducible one, which is exactly what this lane refuses to do.
//
// Fix, without weakening anything:
//   - the harvest rule is now a TRACKED file of this repository, tools/secret-guard.py
//	 (single source of truth; TestSecretGuardPolicyIsTrackedAndSingle refuses a second
//	 policy file appearing anywhere in the module);
//   - inputs are TRACKED fixtures under testdata/secret-stores whose values are
//	 synthetic, so the rule is exercised identically in every checkout;
//   - the LIVE operational stores are still swept, through an explicit operator
//	 override AHP_SECRET_STORE_DIR. When it is absent the dedicated leg skips with a
//	 SKIP_ENV token the gate reads as environment-absence - never as a pass. Nothing
//	 walks up out of the module, and no leg here silently vanishes.
//
// A failure never prints a value: file, line, and the KEY NAME that produced it.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	// guardPolicyEnv overrides where the harvest policy lives, for an operator who
	// keeps it elsewhere. An override that does not resolve is a failure, never a
	// silent fall-back.
	guardPolicyEnv = "AHP_SECRET_GUARD_PATH"
	// storeDirEnv points at real operational credential stores. Unset means the live
	// sweep did not run; TestLiveOperationalStoreSweepCoversTheLane says so loudly.
	storeDirEnv = "AHP_SECRET_STORE_DIR"
	// trackedPolicy is the policy's home in this repository, next to tests/.
	trackedPolicy = "tools/secret-guard.py"
	// fixtureStores are tracked inputs: the harvest rule applied to synthetic values.
	fixtureStores = "testdata/secret-stores"
)

// fileExists answers for both this file and the skip-vocabulary leg next door: a
// tracked input that is absent must fail a leg, never make one vacuously green.
func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// The fixture's controls by name: three marker-keyed values that MUST be harvested, and
// three that must not (a long value under a non-marker key; a marker key under the length
// floor). Named in the leg twice - present in the inputs, then correct in the outcome -
// so neither renaming a control nor widening the rule passes silently.
var (
	fixturePositives = []string{"ahp_test_db_password", "ahp_object_access_key", "AHP_AGENT_VAULT_DEV_TOKEN"}
	fixtureNegatives = []string{"notes_bundle_path", "build_note", "deployment_token_short"}
)

// moduleRoot walks up only as far as the module itself: go.mod is tracked, so the
// answer is a property of the checkout and of nothing else.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 12; i++ {
		if fileExists(filepath.Join(dir, "go.mod")) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("no go.mod above the package: this leg runs inside the module it checks - fix the checkout, this is not a skip")
	return ""
}

// guardPolicySource resolves the policy file: explicit override, else the tracked path
// at the module root. It reads the rule instead of copying it, so a change at the gate
// cannot drift away from the leg meant to pre-empt it.
func guardPolicySource(t *testing.T) ([]byte, string) {
	t.Helper()
	path := strings.TrimSpace(os.Getenv(guardPolicyEnv))
	source := "tracked " + trackedPolicy
	if path == "" {
		path = filepath.Join(moduleRoot(t), trackedPolicy)
	} else {
		source = "operator override " + guardPolicyEnv
	}
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the harvest policy (%s) at %s: %v - the policy is tracked in the repo; a checkout without it is broken and this is not a skip",
			source, path, err)
	}
	t.Logf("harvest policy read from %s", source)
	return src, path
}

// guardPolicy returns SECRET_MARKERS and MIN_LEN as the gate declares them.
func guardPolicy(t *testing.T) ([]string, int) {
	t.Helper()
	src, _ := guardPolicySource(t)
	block := regexp.MustCompile(`(?s)SECRET_MARKERS\s*=\s*\((.*?)\)`).FindStringSubmatch(string(src))
	if block == nil {
		t.Fatal("policy has no SECRET_MARKERS tuple: the harvest rule moved, update this test deliberately")
	}
	var markers []string
	for _, item := range strings.Split(block[1], ",") {
		item = strings.Trim(strings.TrimSpace(item), "\"'\"")
		if item != "" {
			markers = append(markers, item)
		}
	}
	minBlock := regexp.MustCompile(`MIN_LEN\s*=\s*([0-9]+)`).FindStringSubmatch(string(src))
	if minBlock == nil {
		t.Fatal("policy has no MIN_LEN: the harvest rule moved, update this test deliberately")
	}
	minLen, err := strconv.Atoi(minBlock[1])
	if err != nil || len(markers) == 0 || minLen < 1 {
		t.Fatalf("unreadable harvest policy: %d markers, min len %d", len(markers), minLen)
	}
	return markers, minLen
}

// storePairs returns the key/value pairs a store publishes, flattening JSON the way
// the gate does and parsing NAME=value lines for env stores.
func storePairs(path string, raw []byte) [][2]string {
	var pairs [][2]string
	if filepath.Ext(path) != ".json" {
		for _, line := range strings.Split(string(raw), "\n") {
			if k, v, ok := strings.Cut(line, "="); ok && !strings.HasPrefix(strings.TrimSpace(k), "#") {
				pairs = append(pairs, [2]string{strings.TrimSpace(k), strings.Trim(strings.TrimSpace(v), "\"")})
			}
		}
		return pairs
	}
	var doc any
	if json.Unmarshal(raw, &doc) != nil {
		return pairs
	}
	var walk func(any)
	walk = func(node any) {
		switch cur := node.(type) {
		case map[string]any:
			for k, v := range cur {
				if s, ok := v.(string); ok {
					pairs = append(pairs, [2]string{k, s})
					continue
				}
				walk(v)
			}
		case []any:
			for _, v := range cur {
				walk(v)
			}
		}
	}
	walk(doc)
	return pairs
}

// harvest builds the value set the gate builds: every string stored under a key whose
// NAME carries a credential marker, from every store file under dir.
// Maps value -> originating key name. A sweep that finds nothing is a failure: an
// empty set makes every downstream scan vacuously green.
func harvest(t *testing.T, dir string) map[string]string {
	t.Helper()
	markers, minLen := guardPolicy(t)
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("store directory %s is unreadable: %v - the fixtures are tracked in the repo (testdata/secret-stores); restore them, this is not a skip", dir, err)
	}
	out := map[string]string{}
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if info.IsDir() {
			switch info.Name() {
			case "node_modules", ".git", "runtime":
				return filepath.SkipDir
			}
			return nil
		}
		if info.Size() > 4_000_000 {
			return nil
		}
		if ext := filepath.Ext(path); ext != ".json" && ext != ".env" {
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		for _, kv := range storePairs(path, raw) {
			key, val := kv[0], kv[1]
			if len(val) < minLen {
				continue
			}
			upper := strings.ToUpper(key)
			for _, marker := range markers {
				if strings.Contains(upper, marker) {
					out[val] = key
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	if len(out) == 0 {
		t.Fatalf("no values harvested under %s from a marker-named key: a sweep with nothing to check against is a failure, not a pass", dir)
	}
	return out
}

// scanSources looks for any of the values in the files of root. The fixture stores are
// excluded by path: they ARE the inputs, and a value found there is the fixture doing
// its job, not a leak. Anything else under root that carries a value is a hit, named by
// file:line plus the key name (never the value).
func scanSources(t *testing.T, root string, values map[string]string, exclude ...string) ([]string, int) {
	t.Helper()
	var hits []string
	scanned := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() || info.Size() > 8_000_000 {
			return nil
		}
		clean := filepath.ToSlash(path)
		for _, skip := range exclude {
			if strings.HasPrefix(clean, filepath.ToSlash(skip)+"/") {
				return nil
			}
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		scanned++
		for i, line := range strings.Split(string(raw), "\n") {
			for val, key := range values {
				if strings.Contains(line, val) {
					hits = append(hits, clean+":"+strconv.Itoa(i+1)+" (key "+key+")")
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return hits, scanned
}

// TestSecretGuardPolicyIsTrackedAndSingle keeps the director's rule mechanical: the
// harvest policy is one tracked file of this repository, and a second copy of it
// anywhere in the module is a defect (two policy files always drift).
func TestSecretGuardPolicyIsTrackedAndSingle(t *testing.T) {
	root := moduleRoot(t)
	want := filepath.Join(root, trackedPolicy)
	var found []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(info.Name(), "secret-guard") && strings.HasSuffix(info.Name(), ".py") {
			found = append(found, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk module: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("%d policy files found in the module (%s): the rule is ONE tracked file at %s - a second copy drifts, and the second one is what a reviewer reads",
			len(found), strings.Join(found, ", "), trackedPolicy)
	}
	if found[0] != want {
		t.Fatalf("policy found at %s, want the tracked path %s", found[0], want)
	}
	src, path := guardPolicySource(t)
	if strings.TrimSpace(os.Getenv(guardPolicyEnv)) == "" && path != want {
		t.Fatalf("the leg read the policy from %s instead of the tracked %s", path, want)
	}
	if len(src) < 100 {
		t.Fatalf("policy at %s is %d bytes: too short to hold a rule - this is not a pass", path, len(src))
	}
}

// TestFixtureStoresHarvestUnderTheGateRule proves the tracked inputs actually feed the
// rule, in both directions: a marker-named key is harvested, and the two negative
// controls (long value under a non-marker key; marker key under the length floor) are
// not. If the rule is ever widened or narrowed casually, this names the control.
func TestFixtureStoresHarvestUnderTheGateRule(t *testing.T) {
	values := harvest(t, fixtureStores)
	keys := map[string]bool{}
	for _, key := range values {
		keys[key] = true
	}
	// Controls are pinned by NAME before their outcome is checked. A control that is
	// absent from the inputs cannot fail, so a renamed or dropped control would leave
	// this leg green: measured, renaming the non-marker control to a marker key kept it
	// green until this assertion existed. That is the difference between a control and a
	// wish, so the fixture's control keys are required to be present, then required to
	// behave, in that order.
	declared := map[string]bool{}
	walkErr := filepath.Walk(fixtureStores, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		for _, kv := range storePairs(path, raw) {
			declared[kv[0]] = true
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("read the fixture stores: %v", walkErr)
	}
	for _, control := range append(append([]string{}, fixturePositives...), fixtureNegatives...) {
		if !declared[control] {
			t.Fatalf("the fixture no longer declares the control key %q: absent from the inputs it cannot fail, and the leg would report a mutated fixture as clean", control)
		}
	}
	if len(values) < 3 {
		t.Fatalf("fixture stores yielded %d values, want at least 3: the fixture lost entries, so the sweep is thinner than it claims", len(values))
	}
	for _, must := range fixturePositives {
		if !keys[must] {
			t.Fatalf("fixture key %q was not harvested: the rule this leg pins no longer matches the fixture - %d values harvested from keys %v", must, len(values), sortedSetKeys(keys))
		}
	}
	for _, mustNot := range fixtureNegatives {
		if keys[mustNot] {
			t.Fatalf("fixture key %q was harvested although it is a negative control (non-marker name, or below MIN_LEN): the rule was widened, and every leak report from it becomes unreliable", mustNot)
		}
	}
}

// TestSecretScanIsNotVacuous is the machine mutation the lane demands of itself: a
// planted value must be found, a clean tree must not be. Without it, a scan whose walk
// silently matches nothing would stay green forever.
func TestSecretScanIsNotVacuous(t *testing.T) {
	dir := t.TempDir()
	planted := "planted-value-" + strings.Repeat("a", 24) + "-not-an-operational-credential"
	if err := os.WriteFile(filepath.Join(dir, "fixture_source.go.txt"), []byte("package x\n\nconst leaked = \""+planted+"\"\n"), 0o600); err != nil {
		t.Fatalf("plant: %v", err)
	}
	values := map[string]string{planted: "fixture_planted_token"}
	hits, scanned := scanSources(t, dir, values)
	if scanned != 1 {
		t.Fatalf("the scan read %d files from a directory holding 1: the walk is broken, so a clean result means nothing", scanned)
	}
	if len(hits) != 1 {
		t.Fatalf("a planted value was scanned as %d hits, want 1: the scan cannot both miss a planted value and be trusted on a clean tree", len(hits))
	}
	if !strings.Contains(hits[0], "fixture_source.go.txt:3") || !strings.Contains(hits[0], "key fixture_planted_token") {
		t.Fatalf("the hit does not name file:line and the key name (%s): a leak report that cannot be acted on is noise", hits[0])
	}
	clean := t.TempDir()
	if err := os.WriteFile(filepath.Join(clean, "clean.txt"), []byte("nothing harvested here\n"), 0o600); err != nil {
		t.Fatalf("write clean: %v", err)
	}
	if cleanHits, _ := scanSources(t, clean, values); len(cleanHits) != 0 {
		t.Fatalf("a directory without the value produced %d hits: the scan matches something other than the value set", len(cleanHits))
	}
}

// TestNoOperationalCredentialInLaneSources is the machine version of "no credential in
// a committed file". The fixture set makes the check reproducible in any checkout of
// the exact SHA; when an operator supplies the live stores, their values are checked in
// the same pass, and the log says which sets were used (counts and key names only).
func TestNoOperationalCredentialInLaneSources(t *testing.T) {
	values := harvest(t, fixtureStores)
	sets := "tracked fixtures"
	if live := strings.TrimSpace(os.Getenv(storeDirEnv)); live != "" {
		liveValues := harvest(t, live)
		for v, k := range liveValues {
			values[v] = k
		}
		sets = "tracked fixtures + live operational stores"
	}
	hits, scanned := scanSources(t, ".", values, fixtureStores)
	fixtureHits, fixtureFiles := scanSources(t, fixtureStores, values)
	if len(fixtureHits) == 0 {
		t.Fatalf("the fixture stores do not carry any of the values they produced (%d files scanned): the inputs are decorative and the sweep below proves nothing", fixtureFiles)
	}
	t.Logf("scanned %d lane file(s) against %d harvested value(s) from %s; %d fixture file(s) excluded as the inputs themselves",
		scanned, len(values), sets, fixtureFiles)
	if len(hits) > 0 {
		t.Fatalf("%d line(s) in tests/dote2e carry a value harvested under a credential key name: %s; "+
			"replace the value with a reference to its KEY NAME and read it from the lane env at runtime. "+
			"If the value is a state label rather than a credential, rename the label - do not weaken the guard",
			len(hits), strings.Join(hits, ", "))
	}
}

// TestLiveOperationalStoreSweepCoversTheLane keeps the live half of the control
// honest: it runs whenever the operator points at real stores, and says out loud - as a
// gate-visible skip token, never as a pass - when they were not supplied. In the
// headquarters layout it is run with AHP_SECRET_STORE_DIR=<lane evidence dir>.
func TestLiveOperationalStoreSweepCoversTheLane(t *testing.T) {
	live := strings.TrimSpace(os.Getenv(storeDirEnv))
	if live == "" {
		t.Skipf("SKIP_ENV[tag=secret_store_dir] reason=live_operational_stores_not_supplied missing=%s -- a skip here means not-covered, not passed: the sweep against real credential stores runs only when the operator supplies their directory", storeDirEnv)
	}
	values := harvest(t, live)
	hits, scanned := scanSources(t, ".", values)
	policy := filepath.Join(moduleRoot(t), trackedPolicy)
	policyHits, _ := scanSources(t, filepath.Dir(policy), values)
	t.Logf("live sweep: %d file(s) of the lane and the tracked policy directory scanned against %d harvested value(s)", scanned, len(values))
	if len(policyHits) > 0 {
		t.Fatalf("the tracked harvest policy directory carries a live credential value: %s - the policy file is read by the gate and must stay a rule, not a store", strings.Join(policyHits, ", "))
	}
	if len(hits) > 0 {
		t.Fatalf("%d line(s) in tests/dote2e carry a LIVE operational credential value: %s; a credential in a tracked file is disclosed by definition - rotate it (root's call) and reference the KEY NAME here",
			len(hits), strings.Join(hits, ", "))
	}
}

// TestTerminalFrameLabelsAreNotHarvestable pins the durable half of the original fix:
// no terminal-frame label may use a spelling the harvest rule reads as a credential.
// Red before the rename on both frame-free labels, green after; the live stores are
// included whenever they are supplied, because the original collision came from one.
func TestTerminalFrameLabelsAreNotHarvestable(t *testing.T) {
	if terminalFrameVocabulary != "v2" {
		t.Fatalf("terminal_frame_vocabulary = %q: bump it deliberately - it is how a verifier tells which label spelling an artifact uses", terminalFrameVocabulary)
	}
	values := harvest(t, fixtureStores)
	if live := strings.TrimSpace(os.Getenv(storeDirEnv)); live != "" {
		for v, k := range harvest(t, live) {
			values[v] = k
		}
	}
	for _, label := range []string{terminalSeenCompleted, terminalDeclaredNoFrame, terminalDeclaredUnproven, terminalContradicted, terminalAbsent} {
		if key, ok := values[label]; ok {
			t.Fatalf("terminal-frame label %q is harvested under key %q: a state label a credential scanner reads as a credential will be quoted back as a leak, which is exactly how this band's merge was blocked. Rename the vocabulary, do not weaken the guard", label, key)
		}
	}
	// The stamp must reach the artifact, not stay a constant. The report map needs a
	// live stand to build, so this is a source pin rather than a built artifact -
	// enough to refuse a rename that quietly stops being published.
	src, err := os.ReadFile("harness_test.go")
	if err != nil {
		t.Fatalf("read harness source: %v", err)
	}
	if !strings.Contains(string(src), "\"terminal_frame_vocabulary\":") || !strings.Contains(string(src), "terminalFrameVocabulary,") {
		t.Fatal("terminal_frame_vocabulary is not published in the artifact: a verifier cannot tell v1 from v2 without it")
	}
}

// sortedSetKeys keeps a failure message deterministic: the same run prints the same
// list, so two receipts can be compared line by line.
func sortedSetKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
