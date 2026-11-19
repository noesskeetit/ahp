package routing_test

// Divergence guard for the provider configuration copies (PDF rev3.2 :1620-1630
// gives the Model Gateway "копии файлов у провайдера"; the acceptance question is
// "which providers and models does THIS deployment route to", which cannot be
// answered while the answer is typed into several places).
//
// Measured on this tree before the guard existed:
//
//   - the routed model id qwen-38-27b-fp8 appears in 5 files besides the config
//     point (docs/coverage.md, scripts/native-qwen-assert.sh,
//     tests/e2e/live-journey.mjs, tests/e2e/model-to-artifact.mjs,
//     tests/space/native_qwen_test.go);
//   - the provider host qwen.internal.lan appears in 1 file
//     (scripts/native-qwen-assert.sh);
//   - other qwen-family identifiers are fixtures, gate negative controls or
//     script-name fragments, listed and reasoned below;
//   - outside this module the stand repeats the same values again under
//     private/composed-backend (11 further files, including .before-* backups),
//     which this module cannot see and therefore cannot guard -- the stand has
//     to point at this file instead of retyping it (AHP_MODEL_PROVIDERS_FILE).
//
// The guard does three things. It refuses an *unaccounted* provider/model
// identity anywhere in the tree, so a new hard-coded model id cannot land
// silently. It pins the file set of every identity whose divergence changes
// what a deployment does (routed model ids, provider hosts), so a new copy of
// those reddens the build until someone records it deliberately. And it refuses
// stale records, so the accounting cannot rot into fiction.
//
// What it does NOT do: it does not pretend to discover arbitrary model
// identifiers. Discovery follows provider-family prefixes that exist in this
// repository; adding a family is a deliberate, greppable edit to this file,
// which is a much better failure mode than a silent copy.

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// providerConfigPoint is the single source the copies are meant to converge on.
const providerConfigPoint = "infra/model/providers.json"

// Identity kinds.
const (
	// kindCanonical: a model id this deployment actually routes to, per the
	// config point. Copies are pinned.
	kindCanonical = "canonical"
	// kindHost: a provider endpoint host used by a gate or a stand inside this
	// module. Copies are pinned, because a divergent host is a different
	// deployment pretending to be the same one.
	kindHost = "host"
	// kindFixture: an identifier that exists only as test data or a negative
	// control. Accounted and reasoned, but not pinned -- test growth must not
	// redden a build that did not change any deployment.
	kindFixture = "fixture"
	// kindFileName: the token is a fragment of a file name (a script's own name),
	// not an identifier of anything. Accounted, not pinned.
	kindFileName = "file-name"
)

// accountedIdentity records one provider-identity token found in this tree.
type accountedIdentity struct {
	Token  string
	Kind   string
	Reason string
	// Copies lists every file allowed to mention the token, for pinned kinds.
	// The config point itself is never a copy of itself and is excluded.
	Copies []string
}

// accountedIdentities is the ledger. Every entry needs a reason; pinned entries
// additionally need the exact file set.
var accountedIdentities = []accountedIdentity{
	{
		Token:  "qwen-38-27b-fp8",
		Kind:   kindCanonical,
		Reason: "the routed model of provider modelrun, declared once in " + providerConfigPoint + "; every other mention is a copy that has to converge on that file",
		Copies: []string{
			"docs/coverage.md",
			"scripts/native-qwen-assert.sh",
			"tests/e2e/live-journey.mjs",
			"tests/e2e/model-to-artifact.mjs",
			"tests/space/native_qwen_test.go",
			// A leg that pins what the accepted journey must still route to has to
			// state the identity as a literal; reading it from the table under test
			// would make the leg tautological and blind to exactly the drift it
			// exists to catch.
			"internal/model/routing/acceptance_table_test.go",
			// The guard's own legs about where identities come from demonstrate the
			// rule on a real identity, so they name it; synthetic identities in that
			// file are built at runtime and stay invisible to discovery on purpose.
			"internal/model/routing/identity_source_test.go",
			// The profiler names the identity to attribute provenance, not to route:
			// the durable fixture records which model the 261 provider_empty_completion
			// rows were observed on, the harness leg states the same measurement, and
			// the run report prints that attribution so acceptance reads it instead of
			// inferring it. Saying "this observation was made on that model" does not
			// declare a route -- what this deployment routes is still declared once,
			// in infra/model/providers.json.
			"profiling/cmd/ahp-profile/durable_cause_mapping_test.go",
			"profiling/cmd/ahp-profile/main.go",
			"profiling/cmd/ahp-profile/max_tokens_default_test.go",
		},
	},
	{
		Token:  "qwen.internal.lan",
		Kind:   kindHost,
		Reason: "the native Qwen host as known to the G0 gate; the gate must be able to deny a foreign host, so the value is named there, while runtime hosts stay in deployment material (AHP_NATIVE_QWEN_HOSTS), never in the config point",
		Copies: []string{"scripts/native-qwen-assert.sh"},
	},
	{
		Token:  "gpt-4o",
		Kind:   kindFixture,
		Reason: "negative control of the G0 gate and the native-model test: a foreign model that must be DENIED, not a provider this deployment routes to",
	},
	{
		Token:  "qwen-max",
		Kind:   kindFixture,
		Reason: "model identifier in registry manifest/store tests only; not offered by any provider in the config point",
	},
	{
		Token:  "qwen-turbo",
		Kind:   kindFixture,
		Reason: "second model identifier in the registry store test, used to prove a route can name a model the gateway never serves",
	},
	{
		Token:  "qwen-38",
		Kind:   kindFixture,
		Reason: "payload fixture of the registry model-route test (internal/registry/gaps_test.go); a truncated look-alike of the routed id, which is exactly why ids must be compared exactly",
	},
	{
		Token:  "qwen-test",
		Kind:   kindFixture,
		Reason: "unit-test model name used across model/execbridge/state tests",
	},
	{
		Token:  "qwen-classifier",
		Kind:   kindFixture,
		Reason: "model class example in the generated acceptance pack (acceptance/pack, docs) and its generator; documentation, not routing",
	},
	{
		Token:  "qwen-ds-core",
		Kind:   kindFixture,
		Reason: "documentation example of a deployment-chosen provider label in internal/model/service.go; no provider of that name exists",
	},
	{
		Token:  "qwen38",
		Kind:   kindFixture,
		Reason: "profile name in the G0 gate script and one mount hand-off note; a deployment profile label, not a model id",
	},
	{
		Token:  "qwen.example",
		Kind:   kindFixture,
		Reason: "secret-store path fixture in internal/model/secretstore/secretstore_test.go",
	},
	{
		Token:  "qwen.invalid.internal",
		Kind:   kindFixture,
		Reason: "negative host in internal/core/credgw_test.go: an endpoint the credential gateway must refuse",
	},
	{
		Token:  "qwen-assert",
		Kind:   kindFileName,
		Reason: "fragment of the file name scripts/native-qwen-assert.sh, not an identifier",
	},
	{
		Token:  "qwen-gate",
		Kind:   kindFileName,
		Reason: "fragment of the file name scripts/native-qwen-gate.sh, not an identifier",
	},
	{
		Token:  "qwen-int",
		Kind:   kindFileName,
		Reason: "local contractor checkout directory named by tools/secret-guard.py; not a model identity or a runtime provider route",
	},
}

// providerFamilies are the provider-name prefixes present in this repository.
// Discovery walks tokens starting with one of these; a new family is added here
// on purpose.
var providerFamilies = []string{"qwen", "gpt", "deepseek", "claude", "llama"}

// sourceFileExtensions are dropped from host-shaped matches: qwen.txt is a file
// name, qwen.internal.lan is a host.
var sourceFileExtensions = map[string]bool{
	".go": true, ".md": true, ".json": true, ".sh": true, ".mjs": true, ".js": true,
	".py": true, ".ts": true, ".tsx": true, ".sql": true, ".yaml": true, ".yml": true,
	".txt": true, ".html": true, ".css": true, ".svg": true, ".png": true, ".log": true,
	".sum": true, ".mod": true, ".toml": true, ".env": true, ".pem": true, ".crt": true,
	".key": true, ".csv": true, ".tmpl": true,
}

var (
	identityTokenRe = regexp.MustCompile(`(qwen|gpt|deepseek|claude|llama)[A-Za-z0-9_.-]*`)
	// modelShapeRe wants a dash-separated identifier: qwen-max, qwen-38-27b-fp8,
	// gpt-4o. The dash is mandatory, otherwise the bare family name -- "qwen",
	// which is prose and a path segment dozens of times over -- would read as an
	// identity.
	modelShapeRe = regexp.MustCompile(`^[a-z]+(-[a-z0-9]+)+$`)
	// profileShapeRe covers deployment profile labels that glue a number
	// straight onto the family name, the way qwen38 is used in the G0 gate.
	profileShapeRe = regexp.MustCompile(`^[a-z]+[0-9]+(-[a-z0-9]+)*$`)
	hostShapeRe    = regexp.MustCompile(`^[a-z]+(\.[a-z0-9-]+)+$`)
)

// identityShaped reports whether a maximal token looks like a provider or model
// identifier rather than a Go identifier, a file name or prose. It sees the
// token as written, because case is the signal that separates qwenModel (a Go
// identifier glued to a family name) from an identifier written in caps.
//
// The rule is that an identity is written in lower case, as every provider id,
// model id and host in this repository is (checked against the config point,
// which lists its models in lower case). Prose that capitalises the family name
// -- "Qwen-backed gateway", "native-Qwen-only", "QwenSecret.String",
// "qwenText.slice" -- is therefore not mistaken for an identity. The accepted
// cost: an id written in caps would slip past discovery, which is why the routed
// models are additionally verified structurally from the config point itself in
// TestEveryRoutedModelActuallyAppearsInTheTree, where no text matching is
// involved.
func identityShaped(token string) bool {
	if strings.ToLower(token) != token {
		return false
	}
	lower := token
	if sourceFileExtensions[filepath.Ext(lower)] {
		return false
	}
	return modelShapeRe.MatchString(lower) || profileShapeRe.MatchString(lower) || hostShapeRe.MatchString(lower)
}

// skipDir names directories the walk never enters.
func skipDir(name string) bool {
	switch name {
	case ".git", "node_modules", "vendor", "dist", "build", ".next", "__pycache__":
		return true
	}
	return false
}

// discoverIdentities returns, for every identity-shaped provider token in the
// module, the set of files mentioning it (maximal-token matching, so
// qwen-38-27b-fp8 never counts as a mention of qwen-38).
func discoverIdentities(t *testing.T, root string) map[string]map[string]bool {
	t.Helper()
	found := make(map[string]map[string]bool)
	thisFile := "internal/model/routing/config_copies_test.go"
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			if skipDir(info.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if rel == thisFile || strings.HasSuffix(rel, ".sum") {
			return nil
		}
		// A linked git worktree keeps .git as a FILE whose body is
		// "gitdir: /somewhere/.git/worktrees/<checkout-name>". Reading it would
		// make an identity out of the directory the reviewer happens to have
		// checked this module into -- not a provider identity, and not something
		// the config point can ever route. Git metadata is not tree content
		// (skipDir above covers the .git DIRECTORY of a normal clone).
		if rel == ".git" || strings.HasPrefix(rel, ".git/") || strings.Contains(rel, "/.git/") {
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil || len(raw) == 0 || containsNUL(raw) {
			return nil // unreadable or binary
		}
		for _, m := range identityTokenRe.FindAllString(string(raw), -1) {
			if !identityShaped(m) {
				continue
			}
			token := strings.ToLower(m)
			if found[token] == nil {
				found[token] = make(map[string]bool)
			}
			found[token][rel] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return found
}

func containsNUL(raw []byte) bool {
	limit := len(raw)
	if limit > 8192 {
		limit = 8192
	}
	for _, b := range raw[:limit] {
		if b == 0 {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func difference(want, have []string) []string {
	set := make(map[string]bool, len(have))
	for _, h := range have {
		set[h] = true
	}
	var out []string
	for _, w := range want {
		if !set[w] {
			out = append(out, w)
		}
	}
	sort.Strings(out)
	return out
}

// declaredIdentityEnv names the explicit source an operator or CI uses to say
// "this identity belongs to the deployment". Identities come from here or from
// the config point -- never from the filesystem location of the checkout, which
// is what made this guard unrunnable in a worktree whose directory name happened
// to look provider-shaped (measured: the reviewer's checkout name was reported
// as an identity, discovered inside the .git file of a linked worktree).
const declaredIdentityEnv = "AHP_PROVIDER_IDENTITIES"

// splitIdentityList reads a comma/semicolon/space separated identity list.
func splitIdentityList(raw string) []string {
	set := map[string]bool{}
	for _, token := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\n' || r == '\t'
	}) {
		if token = strings.ToLower(strings.TrimSpace(token)); token != "" {
			set[token] = true
		}
	}
	return sortedKeys(set)
}

// routedIdentities are the identities the config point itself names: whatever it
// routes is accounted for BY the config point, which is the point of having one.
func routedIdentities(t *testing.T, root string) []string {
	t.Helper()
	table, err := loadProviderTable(filepath.Join(root, providerConfigPoint))
	if err != nil {
		t.Fatalf("config point must load: %v", err)
	}
	var raw []string
	for _, p := range table.Providers {
		raw = append(raw, p.Name)
		raw = append(raw, p.Models...)
	}
	return splitIdentityList(strings.Join(raw, ","))
}

// declaredIdentities are the identities the deployment states out loud through
// the environment, on top of the config point.
func declaredIdentities(t *testing.T) []string {
	t.Helper()
	return splitIdentityList(os.Getenv(declaredIdentityEnv))
}

// identityProblem is one divergence between what the tree and the declared
// configuration say and what the ledger can explain.
type identityProblem struct {
	Token string
	Why   string
}

// reconcileIdentities is the whole rule, kept free of filesystem access so the
// negative cases can be shown without editing the repository. It answers three
// questions: is every identity the tree mentions explained, is every explained
// identity still real, and is every identity the deployment DECLARES known here.
func reconcileIdentities(found map[string]map[string]bool, ledgerEntries []accountedIdentity, routed, declared []string) []identityProblem {
	var problems []identityProblem
	ledger := make(map[string]accountedIdentity, len(ledgerEntries))
	for _, a := range ledgerEntries {
		if strings.TrimSpace(a.Reason) == "" {
			problems = append(problems, identityProblem{a.Token, "recorded without a reason; an unexplained copy is how the next divergence gets accepted"})
		}
		switch a.Kind {
		case kindCanonical, kindHost:
			if a.Copies == nil {
				problems = append(problems, identityProblem{a.Token, "of kind " + a.Kind + " must pin its copies"})
			}
		case kindFixture, kindFileName:
		default:
			problems = append(problems, identityProblem{a.Token, "unknown kind " + strconv.Quote(a.Kind)})
		}
		if _, dup := ledger[a.Token]; dup {
			problems = append(problems, identityProblem{a.Token, "recorded twice"})
		}
		ledger[a.Token] = a
	}

	for _, token := range sortedKeys(toBoolMap(found)) {
		entry, ok := ledger[token]
		if !ok {
			problems = append(problems, identityProblem{token, fmt.Sprintf("appears in %v and the provider config point does not know it; either route it in %s or record it as an accounted copy with a reason in this test -- an unaccounted model id or provider host is exactly the divergence this guard exists to catch", sortedKeys(found[token]), providerConfigPoint)})
			continue
		}
		if entry.Copies == nil {
			continue
		}
		var actual []string
		for f := range found[token] {
			if f != providerConfigPoint {
				actual = append(actual, f)
			}
		}
		sort.Strings(actual)
		want := append([]string{}, entry.Copies...)
		sort.Strings(want)
		if extra := difference(want, actual); len(extra) > 0 {
			problems = append(problems, identityProblem{token, fmt.Sprintf("lost a recorded copy %v: the ledger is stale, update it deliberately", extra)})
		}
		if extra := difference(actual, want); len(extra) > 0 {
			problems = append(problems, identityProblem{token, fmt.Sprintf("has %d copy/copies beyond the ledger %v: point them at %s or record the copy with a reason", len(extra), extra, providerConfigPoint)})
		}
	}

	for _, entry := range ledgerEntries {
		if _, ok := found[entry.Token]; !ok {
			problems = append(problems, identityProblem{entry.Token, "recorded in this test but no longer appears in the tree; remove the stale record"})
		}
	}

	// An identity the deployment DECLARES that is neither routed by the config
	// point nor explained by the ledger is a claim nobody can account for. This is
	// the negative the guard exists for, and it arrives through configuration --
	// never through the name of the directory someone checked this module into.
	routedSet := splitToSet(routed)
	for _, d := range sortedKeys(splitToSet(declared)) {
		if _, ok := ledger[d]; ok {
			continue
		}
		if routedSet[d] {
			continue
		}
		if _, ok := found[d]; ok {
			continue // the tree rules above already judged it
		}
		problems = append(problems, identityProblem{d, "declared through " + declaredIdentityEnv + " but routed nowhere in " + providerConfigPoint + " and accounted by no reason"})
	}
	return problems
}

// toBoolMap / splitToSet are set helpers for the rules above.

func splitToSet(tokens []string) map[string]bool {
	out := make(map[string]bool, len(tokens))
	for _, token := range tokens {
		out[token] = true
	}
	return out
}

// TestProviderIdentitiesAreAccountedFor fails when a provider/model identity
// appears in the tree that the ledger above does not know, when a pinned
// identity gains or loses a copy, or when a declared identity is unaccounted.
func TestProviderIdentitiesAreAccountedFor(t *testing.T) {
	root := moduleRoot(t)
	found := discoverIdentities(t, root)
	for _, problem := range reconcileIdentities(found, accountedIdentities, routedIdentities(t, root), declaredIdentities(t)) {
		t.Errorf("provider identity %q: %s", problem.Token, problem.Why)
	}
}

// TestEveryRoutedModelActuallyAppearsInTheTree: the config point must not
// advertise a model nothing uses, and every routed model must be ledgered as
// canonical -- otherwise the pinning above silently covers nothing.
func TestEveryRoutedModelActuallyAppearsInTheTree(t *testing.T) {
	root := moduleRoot(t)
	table, err := loadProviderTable(filepath.Join(root, providerConfigPoint))
	if err != nil {
		t.Fatalf("config point must load: %v", err)
	}
	found := discoverIdentities(t, root)
	ledger := make(map[string]accountedIdentity, len(accountedIdentities))
	for _, a := range accountedIdentities {
		ledger[a.Token] = a
	}
	seen := make(map[string]bool)
	for _, p := range table.Providers {
		for _, m := range p.Models {
			key := p.Name + "/" + m
			if seen[key] {
				continue
			}
			seen[key] = true
			if _, ok := found[m]; !ok {
				t.Errorf("routed model %s of provider %s appears nowhere in the tree outside the config point: it is either dead configuration or an id nobody verifies", m, p.Name)
			}
			entry, ok := ledger[m]
			if !ok {
				t.Errorf("routed model %s is not ledgered at all", m)
				continue
			}
			if entry.Kind != kindCanonical {
				t.Errorf("routed model %s is ledgered as %s, must be %s", m, entry.Kind, kindCanonical)
			}
		}
	}
}

func toBoolMap(in map[string]map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for k := range in {
		out[k] = true
	}
	return out
}
