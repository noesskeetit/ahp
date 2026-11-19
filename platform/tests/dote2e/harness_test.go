package dote2e_test

// Shared harness for the personal-Dot E2E lane. Every scenario drives the REAL
// platform over HTTPS/mTLS against the lane stand (own tenant, own database,
// own MinIO bucket, own certificates) and records an evidence JSON file.
//
// Nothing here is a fixture substitute for a platform owner: PostgreSQL, MinIO,
// Redis, Vault, Hindsight and the Qwen endpoint are the real services named in
// the instance scope. Where a physical substrate is mocked (microVM Compute)
// the scenario says so in its own evidence.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type instance struct {
	Dir      string
	Scope    map[string]any
	Revision string
	Binaries map[string]string
	// VCS carries, per binary, the revision and modified flag the BINARY itself
	// reports (docs/acceptance-run.md §0: an ldflags stamp is not provenance).
	VCS       map[string]binaryVCS
	BinSource string
	BinDir    string
	Origin    string
	HTTP      *http.Client
	Foreign   *http.Client
	Pool      *pgxpool.Pool
	Evidence  string
}

// binaryVCS mirrors the build.json "vcs" block written by harness/build.py.
type binaryVCS struct {
	Revision string `json:"revision"`
	Modified string `json:"modified"`
}

func cookieJarTransport(cert tls.Certificate, ca *x509.CertPool) *http.Transport {
	return &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert},
			RootCAs: ca, ServerName: "localhost"},
	}
}

// newLaneClient keeps one browser session per principal: the browser login
// answers with a session cookie and the user-token exchange consumes it.
func newLaneClient(cert tls.Certificate, pool *x509.CertPool) *http.Client {
	jar, err := cookiejar.New(nil)
	if err != nil {
		panic("lane cookie jar: " + err.Error())
	}
	return &http.Client{Jar: jar, Transport: cookieJarTransport(cert, pool), Timeout: 240 * time.Second}
}

// mustEnv answers for the lane's live legs: without the value the leg cannot be
// attempted, and the skip has to name WHICH dependency is absent. The tag dictionary
// (tools/skip-tags.json, owner Volta) froze the umbrella tag this helper used to emit,
// with the note "no new emissions under this name: one token does not say what was not
// covered" - so each variable states its own tag, and an unlisted variable fails loudly
// instead of guessing one. The tokens stay literal on purpose: the registry measures its
// own alphabet by grepping diffs for SKIP_*[tag=...], and a tag hidden behind a format
// verb would be invisible to the mechanism that is supposed to register it.
func mustEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value != "" {
		return value
	}
	const note = " -- a skip here means not-covered, not passed: the personal-Dot lane leg needs this value (see tests/dote2e/harness)"
	switch name {
	case "AHP_DOT_E2E_INSTANCE":
		t.Skipf("SKIP_ENV[tag=dot_e2e_layout] reason=lane_stand_not_supplied missing=%s"+note, name)
	case "AHP_DOT_E2E_EVIDENCE":
		t.Skipf("SKIP_ENV[tag=dot_e2e_layout] reason=lane_evidence_dir_not_supplied missing=%s"+note, name)
	case "AHP_LLM_MODEL":
		t.Skipf("SKIP_ENV[tag=dot_e2e_llm] reason=lane_model_name_not_supplied missing=%s"+note, name)
	}
	t.Fatalf("%s: no dependency is registered for this variable in mustEnv - naming one is the only honest way to skip, an umbrella tag cannot be counted", name)
	return ""
}
func loadStand(t *testing.T) *instance {
	t.Helper()
	dir := mustEnv(t, "AHP_DOT_E2E_INSTANCE")
	evidence := mustEnv(t, "AHP_DOT_E2E_EVIDENCE")
	scopeBytes, err := os.ReadFile(filepath.Join(dir, "scope.json"))
	if err != nil {
		t.Fatalf("read instance scope: %v", err)
	}
	var scope map[string]any
	if err := json.Unmarshal(scopeBytes, &scope); err != nil {
		t.Fatalf("parse instance scope: %v", err)
	}
	buildBytes, err := os.ReadFile(filepath.Join(dir, "build.json"))
	if err != nil {
		t.Fatalf("read instance build manifest: %v", err)
	}
	var build struct {
		Revision       string               `json:"revision"`
		Binaries       map[string]string    `json:"binaries"`
		VCS            map[string]binaryVCS `json:"vcs"`
		SourceWorktree string               `json:"source_worktree"`
		BinaryDir      string               `json:"binary_dir"`
	}
	if err := json.Unmarshal(buildBytes, &build); err != nil {
		t.Fatalf("parse instance build manifest: %v", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	caPem, err := os.ReadFile(filepath.Join(dir, "certs/ca.crt"))
	if err != nil {
		t.Fatalf("read lane CA: %v", err)
	}
	if !pool.AppendCertsFromPEM(caPem) {
		t.Fatal("lane CA could not be added to the trust pool")
	}
	operator, err := tls.LoadX509KeyPair(filepath.Join(dir, "certs/operator.crt"), filepath.Join(dir, "certs/operator.key"))
	if err != nil {
		t.Fatalf("load operator workload certificate: %v", err)
	}
	foreign, err := tls.LoadX509KeyPair(filepath.Join(dir, "certs/foreign.crt"), filepath.Join(dir, "certs/foreign.key"))
	if err != nil {
		t.Fatalf("load foreign workload certificate: %v", err)
	}
	stand := &instance{Dir: dir, Scope: scope, Revision: build.Revision, Binaries: build.Binaries,
		VCS: build.VCS, BinSource: build.SourceWorktree, BinDir: build.BinaryDir,
		Origin: scope["origin"].(string), Evidence: evidence,
		HTTP:    newLaneClient(operator, pool),
		Foreign: newLaneClient(foreign, pool)}
	// Revision discipline (director mandate 2026-11-09): when AHP_EXPECTED_REV is
	// set, the harness REFUSES to validate a run whose stand binaries - or whose
	// lane worktree HEAD - are not that exact integration revision. The artifact
	// then says BLOCKED with the mismatch spelled out, instead of silently
	// certifying evidence recorded against a different tree.
	if expected := strings.TrimSpace(os.Getenv("AHP_EXPECTED_REV")); expected != "" {
		worktree := gitWorktreeRev()
		// Binaries must report the expected revision THEMSELVES (§0): the vcs
		// block build.py read back with `go version -m`, vcs.modified=false.
		// The lane worktree may either equal the expected revision or be a
		// descendant whose delta touches only tests/dote2e (harness bytes).
		deltaHarnessOnly := false
		if worktree != "" && !shaMatch(expected, worktree) {
			deltaHarnessOnly = productRevReason(expected, worktree) == ""
		}
		reason := vcsProvenanceReason(expected, build.VCS)
		if reason == "" {
			reason = revGuardReason(expected, stand.Revision, worktree, deltaHarnessOnly)
		}
		if reason != "" {
			stand.record(t, "revision-guard", "BLOCKED", map[string]any{"revision_guard": map[string]any{
				"status":            "BLOCKED",
				"reason":            reason,
				"expected_revision": expected,
				"build_revision":    stand.Revision,
				"worktree_revision": worktree,
				"worktree_delta":    worktreeDeltaLabel(worktree, expected, deltaHarnessOnly),
				"vcs_provenance":    build.VCS,
				"binaries_source":   build.SourceWorktree,
			}})
			t.Fatalf("revision guard: %s", reason)
		}
	}
	// Product/harness separation (director 2026-11-09): when AHP_PRODUCT_REV names the
	// integration candidate the stand was merged onto, prove mechanically that (a) it is
	// an ancestor of the built revision and (b) the delta between candidate and build is
	// ONLY tests/dote2e - harness bytes may differ, product bytes may not. The artifact
	// then carries both SHAs separately.
	if product := strings.TrimSpace(os.Getenv("AHP_PRODUCT_REV")); product != "" {
		reason := productRevReason(product, stand.Revision)
		if reason == "" {
			// The lane worktree is what actually executed the scenarios: the
			// candidate must also be its ancestor with a tests/dote2e-only delta.
			if worktree := gitWorktreeRev(); worktree != "" && !shaMatch(product, worktree) {
				reason = productRevReason(product, worktree)
			}
		}
		if reason == "" {
			// Acceptance-manifest binding (director 2026-11-10): when the run is
			// tied to an ACCEPTANCE-MANIFEST, the manifest must name THIS product
			// revision and its tree - evidence then references a manifest it was
			// mechanically checked against, never a decorative path.
			reason = acceptanceManifestReason(os.Getenv("AHP_ACCEPTANCE_MANIFEST"), product)
		}
		if reason != "" {
			stand.record(t, "revision-guard", "BLOCKED", map[string]any{"revision_guard": map[string]any{
				"status":           "BLOCKED",
				"reason":           reason,
				"product_revision": product,
				"build_revision":   stand.Revision,
			}})
			t.Fatalf("product rev guard: %s", reason)
		}
	}
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL is required: PG assertions are part of every scenario")
	}
	poolCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	poolPG, err := pgxpool.New(poolCtx, dsn)
	if err != nil {
		t.Fatalf("connect lane PostgreSQL: %v", err)
	}
	if err := poolPG.Ping(poolCtx); err != nil {
		t.Fatalf("ping lane PostgreSQL: %v", err)
	}
	stand.Pool = poolPG
	t.Cleanup(poolPG.Close)
	if status, _ := scope["status"].(string); status == "" {
		t.Fatal("instance scope has no status")
	}
	return stand
}

// apiError carries the platform error envelope so scenarios can assert on the
// refusal instead of only on the status code.
type apiError struct {
	Status  int
	Code    string
	Message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("http %d code=%s message=%s", e.Status, e.Code, e.Message)
}

// call posts one platform operation with a bearer token and decodes the reply.
func (s *instance) call(client *http.Client, path string, in any, token string, out any) error {
	_, err := s.callWithStatus(client, path, in, token, out)
	return err
}

// callWithStatus is call plus the observed HTTP status code. Some acceptance
// checks must gate on the RESPONSE CODE itself (a public read-back leg that
// records "http=0 because the helper only knew errors" hides whether the
// platform ever answered 200); errors are returned exactly as call returns them.
func (s *instance) callWithStatus(client *http.Client, path string, in any, token string, out any) (int, error) {
	body, err := json.Marshal(in)
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequest(http.MethodPost, s.Origin+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-AHP-Session", "1")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 32<<20))
	if err != nil {
		return response.StatusCode, err
	}
	if response.StatusCode/100 != 2 {
		parsed := apiError{Status: response.StatusCode}
		var envelope struct {
			Code    string `json:"code"`
			Message string `json:"message"`
			Error   struct {
				Code    string `json:"code"`
				Message string `json:"message"`
				Reason  string `json:"reason"`
			} `json:"error"`
		}
		switch err := json.Unmarshal(payload, &envelope); {
		case err != nil:
			parsed.Message = strings.TrimSpace(string(payload))
		case envelope.Code != "" || envelope.Message != "":
			parsed.Code, parsed.Message = envelope.Code, envelope.Message
		default:
			// The Memory authority answers with the nested {"error":{...}} shape.
			parsed.Code = firstNonEmpty(envelope.Error.Code, envelope.Error.Reason)
			parsed.Message = envelope.Error.Message
		}
		if len(parsed.Message) > 300 {
			parsed.Message = parsed.Message[:300]
		}
		return response.StatusCode, &parsed
	}
	if out != nil && len(payload) > 0 {
		return response.StatusCode, json.Unmarshal(payload, out)
	}
	return response.StatusCode, nil
}

// rawGet performs an authenticated GET (used by artifact download checks).
func (s *instance) rawGet(client *http.Client, path, token string, headers map[string]string) (int, []byte, error) {
	request, err := http.NewRequest(http.MethodGet, s.Origin+path, nil)
	if err != nil {
		return 0, nil, err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	request.Header.Set("X-AHP-Session", "1")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 64<<20))
	return response.StatusCode, payload, err
}

// machinePost posts exactly like the SDKs do: task token and JSON only, no
// browser session marker. It isolates owner gates that key on the marker.
func (s *instance) machinePost(client *http.Client, path string, body any, token string) (int, []byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequest(http.MethodPost, s.Origin+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 64<<20))
	return response.StatusCode, payload, err
}

// humanToken performs the browser login and exchanges it for a user token.
func (s *instance) humanToken(t *testing.T, client *http.Client, email, password string) string {
	t.Helper()
	var login struct {
		Access_token string `json:"access_token"`
	}
	var in = map[string]string{"username": email, "password": password}
	if err := s.call(client, "/u-ahp/v1/auth/login", in, "", nil); err != nil {
		t.Fatalf("browser login refused: %v", err)
	}
	if err := s.call(client, "/u-ahp/v1/auth/token", map[string]any{}, "", &login); err != nil {
		t.Fatalf("user token exchange refused: %v", err)
	}
	if login.Access_token == "" {
		t.Fatal("user token exchange returned an empty token")
	}
	return login.Access_token
}

// taskGrant exchanges a human token for a task grant + task token: PDF rev3.2
// L2050 ties a chat turn to "задача с грантом и токеном" at the AHP boundary.
func (s *instance) taskGrant(t *testing.T, client *http.Client, human, task string, capabilities []string) (string, string) {
	t.Helper()
	return s.taskGrantContext(t, client, human, task, task, capabilities)
}

// taskGrantContext is the same exchange with an explicitly named conversation
// context. Identity mints the task id itself, so a scenario that binds memory or
// artifacts to a conversation must know the context it asked for, not the task
// the server answered with.
func (s *instance) taskGrantContext(t *testing.T, client *http.Client, human, task, contextID string, capabilities []string) (string, string) {
	t.Helper()
	var out struct {
		Token string `json:"token"`
		Task  string `json:"task"`
	}
	err := s.call(client, "/v1/identity/control/exchange", map[string]any{
		"human_token": human, "task": task, "context": contextID, "capabilities": capabilities}, human, &out)
	if err != nil {
		t.Fatalf("identity control exchange refused: %v", err)
	}
	if out.Token == "" || out.Task == "" {
		t.Fatal("identity control exchange returned no token/task")
	}
	return out.Token, out.Task
}

// record writes the scenario evidence JSON and returns its path.
func (s *instance) record(t *testing.T, scenario, verdict string, checks map[string]any) string {
	t.Helper()
	// Evidence must be OPENABLE by the acceptor: an artifact nobody can read has no
	// evidentiary force (found 2026-11-09: 0700 evidence dir hid four artifacts from
	// director/root). Artifacts carry identifiers/digests only, never credentials.
	if err := os.MkdirAll(s.Evidence, 0o755); err != nil {
		t.Fatalf("create evidence directory: %v", err)
	}
	scopeHash := sha256.Sum256([]byte(fmt.Sprintf("%v", s.Scope)))
	legs := classifyLegs(scenario, checks)
	completedLegs, blockedLegs, failedLegs := legs.completed, legs.blocked, legs.failed
	// Verdict authority (DEF-VERDICT-1 at the record boundary): a scenario may ask
	// for a verdict, but the artifact can never be MORE optimistic than the legs it
	// carries. Before this, record() wrote the caller's string verbatim, so e.g.
	// delegation-perimeter wrote PASS next to a BLOCKED leg.
	computedVerdict := overallVerdict(completedLegs, blockedLegs, failedLegs)
	finalVerdict := moreSevereVerdict(verdict, computedVerdict)
	verdictConflict := ""
	if finalVerdict != verdict {
		verdictConflict = fmt.Sprintf("the scenario asked for %s but its legs prove %s (completed=%d blocked=%d failed=%d); the artifact carries the worse verdict - a verdict is derived from evidence, never declared",
			verdict, computedVerdict, len(completedLegs), len(blockedLegs), len(failedLegs))
	}
	// Top-level dirty (director acceptance): the artifact must STATE whether the lane
	// worktree was clean at record time. build.py already refuses a dirty tree at build
	// time; this closes the second half - the JSON itself carries the proof, and a git
	// that cannot answer is recorded as unknown rather than silently omitted.
	dirty, dirtyErr := gitWorktreeDirty()
	evidence := map[string]any{
		"lane": "dot-e2e", "scenario": scenario, "verdict": finalVerdict,
		// What the scenario asked for stays visible: an acceptor must see the
		// disagreement, not just the correction.
		"requested_verdict": verdict,
		"revision":          s.Revision, "binary_digests": s.Binaries,
		"dirty": dirty,
		// Separate product and harness SHAs (director): revision is the built
		// binaries' source (= product candidate); product_revision is the same
		// integration candidate, validated at loadStand to differ from the
		// executing worktree ONLY under tests/dote2e. harness_revision is the
		// EXECUTING lane HEAD - the harness code that produced this artifact,
		// deliberately ahead of the product by test-only commits.
		"harness_revision": gitWorktreeRev(),
		// §0 provenance: what the stand binaries themselves report (go version -m),
		// the source worktree they were built from, and the executing worktree HEAD.
		"vcs_provenance": s.VCS, "binaries_source": s.BinSource, "worktree_revision": gitWorktreeRev(),
		"instance": filepath.Base(s.Dir), "tenant": s.Scope["tenant"], "database": s.Scope["database"],
		"bucket": s.Scope["bucket"], "instance_scope_sha256": hex.EncodeToString(scopeHash[:]),
		"identity_profile": s.Scope["iam_profile"], "compute_profile": s.Scope["compute_profile"],
		// Row constraint (root via director 2026-11-10): the A2A agent replica is
		// isolated in its own deployment so the Dot queue has exactly one consumer
		// (as-runner). Recorded, never silent - and never quoted as A2A absence.
		"a2a_deployment":       s.Scope["a2a_deployment"],
		"queue_isolation_note": s.Scope["queue_isolation_note"],
		// MOCK is accepted for the prototype (customer decision) but must never be
		// read as proven isolation: the artifact states the claim explicitly.
		"compute_isolation_claimed": false,
		"compute_note":              "mock-microvm simulates the compute protocol in-process; it proves the platform contract path, NOT VM isolation - no leg may be quoted as an isolation proof",
		"hindsight":                 s.Scope["hindsight_url"], "checks": checks,
		// Separated provenance axes (director 2026-11-10): built_from_revision is
		// the source the STAND BINARIES were compiled from (their build.json,
		// cross-checked by vcs_provenance below); product_revision is the
		// acceptance candidate under test. One field never substitutes the other:
		// when binaries come from a canonical catalog built elsewhere, both are
		// visible and any divergence is explicit.
		"built_from_revision": s.Revision, "built_from_source": s.BinSource,
		// Machine-checkable honest leg status at the TOP level (DEF-VERDICT-1):
		// completed/blocked/failed are derived from every leg that carries a terminal
		// "status", so an acceptance tool reads them without parsing checks.
		"completed_legs": completedLegs, "blocked_legs": blockedLegs, "failed_legs": failedLegs,
		// Per-leg reasons and terminal evidence (DEF-VERDICT-1 completion, Heisenberg
		// finding on f14585d2): a name without a reason is no longer admissible, and
		// terminal_frame_states says explicitly which legs OBSERVED a terminal frame,
		// which declared themselves frame-free, and which are unknown.
		"blocked_reasons": legs.blockedReasons, "failed_reasons": legs.failedReasons,
		"terminal_frame_states": legs.terminalStates,
		// Which label spelling the states above use. v1 artifacts predate the field
		// and are never rewritten (guard: TestTerminalFrameLabelsAreNotHarvestable).
		"terminal_frame_vocabulary":   terminalFrameVocabulary,
		"unknown_terminal_frame_legs": legs.unknown,
		// Declared frame-free legs publish their DURABLE LOCATOR and whether the leg
		// actually presented it (DEF-DOTSDK-TERMINAL-1): a verifier re-reads the
		// table/query or the access span named here instead of trusting a flag, and
		// frame_free_unproven_legs names the ones that offered no evidence at all.
		"frame_free_legs":            legs.frameFree,
		"frame_free_unproven_legs":   legs.frameFreeUnproven,
		"incomplete_reasons_present": len(legs.blocked) == len(legs.blockedReasons) && len(legs.failed) == len(legs.failedReasons),
		"terminal_frame_enforcement": map[string]any{
			"strict":                        legs.strictEnforcement,
			"rule":                          "strict: unknown terminal evidence and a frame-free declaration without its durable stand-in are never completed; lenient: both gaps stay recorded here, never silently dropped",
			"frame_free_declaration_source": "tests/dote2e/harness_test.go:frameFreeDeclarations (guard: TestFrameFreeDeclarationCountsAreExact)",
		},
		"recorded_at":     time.Now().UTC().Format(time.RFC3339),
		"load_at_record":  loadCurrent(),
		"load_before_run": strings.TrimSpace(os.Getenv("AHP_DOT_E2E_LOAD_BEFORE")),
	}
	if env := strings.TrimSpace(os.Getenv("AHP_DOT_E2E_ENV_START")); env != "" {
		// Machine-readable environment snapshot (loadavg, memory, timestamp) taken
		// BEFORE the build, so a shared-host run can be attributed to its conditions.
		evidence["environment_at_start"] = env
	}
	if dry := strings.TrimSpace(os.Getenv("AHP_DOT_E2E_DRY_RUN")); dry != "" {
		// Preparation runs (stand bring-up, smoke) are NOT acceptance evidence:
		// the artifact must shout this so nobody quotes a dry run as a result.
		evidence["dry_run"] = true
		evidence["dry_run_note"] = dry
	}
	// The rev guard ran BEFORE this artifact existed (loadStand fatals otherwise),
	// so reaching record() with an expectation set means it MATCHED. State that
	// positively: an acceptance reader must see "guard matched", not infer the
	// guard was absent (director 2026-11-10).
	if expected := strings.TrimSpace(os.Getenv("AHP_EXPECTED_REV")); expected != "" {
		evidence["rev_guard"] = "matched"
		evidence["rev_guard_expected"] = expected
	}
	if verdictConflict != "" {
		evidence["verdict_conflict"] = verdictConflict
	}
	if coverage, ok := s.Scope["coverage"]; ok && coverage != nil {
		// Self-declared coverage: what this run proved, proved HOW, and what it does
		// NOT prove. A green verdict must never be read as full requirement coverage.
		evidence["coverage"] = coverage
	}
	if dirtyErr != "" {
		evidence["dirty_check_error"] = dirtyErr
	}
	if product := strings.TrimSpace(os.Getenv("AHP_PRODUCT_REV")); product != "" {
		evidence["product_revision"] = product
	}
	if manifest := strings.TrimSpace(os.Getenv("AHP_ACCEPTANCE_MANIFEST")); manifest != "" {
		// loadStand already refused the run unless this manifest names exactly
		// the product revision and its real git tree.
		evidence["acceptance_manifest"] = manifest
	}
	name := filepath.Join(s.Evidence, fmt.Sprintf("dot-e2e-scenario-%s-%s.json", scenario,
		time.Now().UTC().Format("20060102T150405Z")))
	payload, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("marshal evidence: %v", err)
	}
	if err := os.WriteFile(name, append(payload, '\n'), 0o644); err != nil {
		t.Fatalf("write evidence: %v", err)
	}
	t.Logf("EVIDENCE %s -> %s", finalVerdict, name)
	// rc=0 is NOT acceptance (director 2026-11-10, DEF-DOTSDK-TERMINAL-1 p.3):
	// `go test` exits 0 while the artifact says BLOCKED, and the lane already burned
	// a series on exactly that confusion - the same class as "HTTP 200 = success".
	// Under AHP_DOT_E2E_ACCEPT=1 a non-PASS verdict reddens the test, so the exit
	// code and the verdict agree. Outside acceptance mode the reader is
	// tests/dote2e/cmd/dote2e-verdict, which exits non-zero on a non-PASS artifact.
	// Either way the verdict is the field, never the return code.
	if acceptanceEnforced() && finalVerdict != "PASS" {
		t.Errorf("acceptance mode (AHP_DOT_E2E_ACCEPT=1): scenario %s recorded verdict=%s with blocked=%v failed=%v - a non-PASS artifact may not exit 0. Artifact: %s",
			scenario, finalVerdict, blockedLegs, failedLegs, name)
	}
	return name
}

// acceptanceEnforced reports whether this run is an acceptance run: the lane's
// protocol sets AHP_DOT_E2E_ACCEPT=1 together with AHP_EXPECTED_REV and
// AHP_DOT_E2E_STRICT_TERMINAL=1, and under it a non-PASS verdict fails the test
// instead of exiting 0.
func acceptanceEnforced() bool {
	return strings.TrimSpace(os.Getenv("AHP_DOT_E2E_ACCEPT")) == "1"
}

// causeAttribution proves that a NON-completed terminal leg carries a named
// cause, so the artifact can never show a bare "failed" without an
// explanation (director mandate 2026-11-10: any terminal failed must ship a
// non-empty cause). It returns "" when the attribution is present, and a
// one-line gap description when it is not: a FAIL/BLOCKED leg without a
// failure_reason, without a durable failure_cause and without a named defect
// is itself recorded as an attribution gap - the absence of a cause is
// stated, never silently omitted.
func causeAttribution(entry map[string]any) string {
	status, _ := entry["status"].(string)
	if status == "PASS" || status == "PENDING" || status == "" {
		return ""
	}
	if s, _ := entry["failure_reason"].(string); strings.TrimSpace(s) != "" {
		return ""
	}
	if f, ok := entry["durable_forensics"].(map[string]any); ok {
		if s, _ := f["failure_cause"].(string); strings.TrimSpace(s) != "" {
			return ""
		}
	}
	if s, _ := entry["defect"].(string); strings.TrimSpace(s) != "" {
		return ""
	}
	return "cause_attribution_gap: terminal leg " + status +
		" carries no failure_reason, no durable failure_cause and no named defect - " +
		"attribution owner Space/State (Jason); see durable_forensics for the raw fingerprint"
}

// legStatusLists derives machine-checkable leg status lists from a scenario's
// checks. Only a checks value that is a map carrying a string "status" counts
// as a leg; anything else (nested data, plain info maps, the leg lists
// themselves) is skipped, so this is safe for every scenario in the lane. A
// status that is not PASS/FAIL is reported as blocked, never as completed.
type legClassification struct {
	completed, blocked, failed []string
	// blockedReasons/failedReasons carry WHY per leg, so an unknown or failed leg
	// can never appear as a bare name - and an empty list is provably "no such leg",
	// not "reason lost".
	blockedReasons map[string]string
	failedReasons  map[string]string
	// terminalStates records the terminal evidence of every leg: completed /
	// frame_free_proven / frame_free_unproven / non_completed / unknown.
	terminalStates map[string]string
	// unknown lists the legs whose terminal evidence is unknown, so an empty
	// blocked/failed list is provably "no such leg", never "reason lost".
	unknown           []string
	strictEnforcement bool
	// frameFree indexes, per declared frame-free leg, the durable locator that stands
	// in place of a terminal frame and whether the leg actually presented it
	// (DEF-DOTSDK-TERMINAL-1: a declaration alone is never a pass).
	frameFree map[string]map[string]any
	// frameFreeUnproven lists declared frame-free legs that presented no durable
	// terminal equivalent. Strict: the leg is BLOCKED. Lenient: recorded, never hidden.
	frameFreeUnproven []string
}

// classifyLegs applies the DEF-VERDICT-1 guard plus the terminal-frame rule.
// strict: unknown terminal evidence can never be completed (journey, and every
// scenario under AHP_DOT_E2E_STRICT_TERMINAL=1).
// lenient (other scenarios today): an absent terminal_status stays "not
// contradicted" - the residual gap is then LOUD in the artifact through
// terminal_frame_enforcement and terminal_frame_states, never invisible.
func classifyLegs(scenario string, checks map[string]any) legClassification {
	strict := strictTerminalEnforcement(scenario)
	out := legClassification{
		completed: []string{}, blocked: []string{}, failed: []string{},
		blockedReasons: map[string]string{}, failedReasons: map[string]string{},
		terminalStates: map[string]string{}, unknown: []string{}, strictEnforcement: strict,
		frameFree: map[string]map[string]any{}, frameFreeUnproven: []string{},
	}
	failedTags, _ := checks["failed_legs"].([]string)
	seen := map[string]bool{}
	for name, value := range checks {
		leg, ok := value.(map[string]any)
		if !ok {
			continue
		}
		st, isStr := leg["status"].(string)
		if !isStr {
			continue
		}
		state := terminalFrameState(leg["terminal_status"])
		// A declared frame-free leg is one that presents NO terminal frame at all.
		// If a frame is present it always wins: a declared leg whose frame says
		// "failed" is a failed leg, never a frame-free one, or a declaration would
		// become a way to launder a contradiction.
		if state == terminalAbsent {
			// Frame-free status comes ONLY from frameFreeDeclarations, and the
			// declaration counts only while the leg presents the durable terminal
			// equivalent it names (DEF-DOTSDK-TERMINAL-1, director variant (a)).
			if decl, declared := declaredFrameFree(scenario, name); declared {
				proven, evidence := frameFreeProof(decl, leg)
				out.frameFree[name] = evidence
				if proven {
					state = terminalDeclaredNoFrame
				} else {
					state = terminalDeclaredUnproven
					out.frameFreeUnproven = append(out.frameFreeUnproven, name)
				}
			}
		}
		out.terminalStates[name] = state
		if state == terminalAbsent {
			out.unknown = append(out.unknown, name)
		}
		switch state {
		case terminalContradicted:
			// A non-completed terminal contradicts a PASS outright (the ...165158Z bug).
			if st == "PASS" {
				leg["status"] = "BLOCKED"
				leg["note"] = "status PASS contradicts a non-completed terminal_status; downgraded to BLOCKED (DEF-VERDICT-1)"
				st = "BLOCKED"
			}
		case terminalAbsent:
			if strict && st == "PASS" {
				// No terminal frame, not declared frame-free: unknown, never completed.
				leg["status"] = "BLOCKED"
				leg["note"] = "missing_terminal_frame: the leg presents no terminal_status and is not declared frame-free; unknown is not completed (DEF-VERDICT-1, Heisenberg finding on f14585d2)"
				st = "BLOCKED"
			}
		case terminalDeclaredUnproven:
			if strict && st == "PASS" {
				// Declared frame-free but the durable stand-in for a terminal frame was
				// never presented: the declaration is a promise, not evidence.
				leg["status"] = "BLOCKED"
				leg["note"] = "frame_free_without_durable_evidence: the leg is declared frame-free but presents none of the durable facts its declaration names (see frame_free_legs in this artifact for the table/query it owes)"
				st = "BLOCKED"
			}
		}
		if st == "PASS" {
			if tag := contradictingFailedTag(name, failedTags); tag != "" {
				leg["status"] = "BLOCKED"
				leg["note"] = fmt.Sprintf("contradictory evidence: failed_legs records %q for this leg while the leg writes PASS; downgraded (DEF-VERDICT-1)", tag)
				st = "BLOCKED"
			}
		}
		switch st {
		case "PASS":
			out.completed = append(out.completed, name)
		case "FAIL":
			out.failed = append(out.failed, name)
			out.failedReasons[name] = firstNonEmpty(legString(leg, "failure_reason"), legString(leg, "reason"),
				legString(leg, "error"), legString(leg, "note"), "no reason recorded by the leg")
		default:
			out.blocked = append(out.blocked, name)
			reason := firstNonEmpty(legString(leg, "note"), legString(leg, "failure_reason"),
				legString(leg, "reason"), legString(leg, "error"))
			if state == terminalAbsent {
				reason = firstNonEmpty(reason, "missing_terminal_frame")
				if !strings.Contains(reason, "missing_terminal_frame") {
					reason += "; missing_terminal_frame"
				}
			}
			if state == terminalDeclaredUnproven {
				reason = firstNonEmpty(reason, "frame_free_without_durable_evidence")
				if !strings.Contains(reason, "frame_free_without_durable_evidence") {
					reason += "; frame_free_without_durable_evidence"
				}
			}
			out.blockedReasons[name] = firstNonEmpty(reason, "no reason recorded by the leg")
		}
		seen[name] = true
	}
	// Fold in the hard-fail tags a scenario's fail() path recorded, so a FAIL
	// verdict never reports empty failed_legs even when the failing leg's own
	// status map still says PASS (the leg aborted mid-check).
	if len(failedTags) > 0 {
		for _, tag := range failedTags {
			if !seen[tag] {
				out.failed = append(out.failed, tag)
				out.failedReasons[tag] = "recorded by fail() before the leg status map could be written"
				seen[tag] = true
			}
		}
	}
	sort.Strings(out.unknown)
	sort.Strings(out.frameFreeUnproven)
	sort.Strings(out.completed)
	sort.Strings(out.blocked)
	sort.Strings(out.failed)
	return out
}

// legString reads a string field of a leg map, "" when absent or not a string.
func legString(leg map[string]any, key string) string {
	v, _ := leg[key].(string)
	return strings.TrimSpace(v)
}

func legStatusLists(checks map[string]any) (completed, blocked, failed []string) {
	c := classifyLegs("", checks)
	return c.completed, c.blocked, c.failed
}

// Terminal-frame states. Never two, never three: an ABSENT terminal frame is
// distinguishable from a completed one, and a DECLARED frame-free leg is
// distinguishable from one that only claims to be. The two-state classifier this
// replaces treated a leg with no terminal_status as "not contradicted" and let it
// count as completed - exactly the false-PASS class root rejected twice
// (Heisenberg finding on f14585d2, reports/mac-director/HEISENBERG-DOT-VERIFY-f14585d2.md).
const (
	// terminalSeenCompleted: the leg presented a real completed terminal frame.
	terminalSeenCompleted = "completed"
	// terminalDeclaredNoFrame: the leg has no terminal frame BY CONSTRUCTION, is
	// declared in frameFreeDeclarations, AND presented the durable stand-in that
	// declaration names. Declaration without evidence is the next state down.
	// Spelling is v2 (see terminalFrameVocabulary): the v1 spelling was a state
	// label the lane's credential scanner harvested out of an artifact key named
	// after leg j1, so a green artifact could be quoted back as a leak.
	terminalDeclaredNoFrame = "frame_free_proven"
	// terminalDeclaredUnproven: declared frame-free, but the durable terminal
	// equivalent the declaration names was never presented. Under strict
	// enforcement this is BLOCKED, never completed (DEF-DOTSDK-TERMINAL-1).
	// v2 spelling: the v1 spelling held the v1 proven label as a prefix, so
	// renaming one of the two and not the other would have left the collision in
	// place (secret-guard matches substrings).
	terminalDeclaredUnproven = "frame_free_unproven"
	// terminalContradicted: a terminal frame exists and it is not a completed one
	// (failed, canceled, pending, ...).
	terminalContradicted = "non_completed"
	// terminalAbsent: no terminal frame was presented and the leg is not declared
	// as frame-free. Unknown is NEVER completed.
	terminalAbsent = "unknown"
)

// terminalFrameVocabulary stamps which label spelling an artifact uses, so a
// verifier reading an older artifact never mistakes a renamed label for an absent
// state, and so nobody has to guess whether a label missing from the source was
// deleted or renamed. v1 -> v2 renamed the two frame-free labels only: completed /
// non_completed / unknown are unchanged, so v1 "completed" legs stay comparable.
// Historical artifacts are never rewritten - read the v1 spelling from the source
// revision named in reports/mac-director/dot-frame-free-durable-evidence-20261110.md,
// do not quote into a file a value a credential scanner harvests by key name.
const terminalFrameVocabulary = "v2"

// terminalFrameState classifies the terminal evidence a leg presents.
// Absent, nil or empty string all mean the same thing: nothing was observed.
func terminalFrameState(v any) string {
	ts, ok := v.(string)
	if !ok || strings.TrimSpace(ts) == "" {
		return terminalAbsent
	}
	// "no_frame_expected" used to be honoured here, which let ANY leg waive its
	// own terminal frame by writing a magic string. Frame-free status is now only
	// ever granted by the declaration table below, never claimed by the leg; a leg
	// that writes an unrecognised terminal_status lands in terminalContradicted.
	switch ts {
	case "completed", "done":
		return terminalSeenCompleted
	default:
		return terminalContradicted
	}
}

// ---- declared frame-free legs (DEF-DOTSDK-TERMINAL-1, director variant (a)) ----
//
// Scenario names this table keys on are the acceptance scenario names; every
// personal-Dot journey variant (including personal-dot-journey-defect-recorded)
// shares the journey declaration through acceptanceScenarioKey.
const (
	scenarioPersonalDotJourney  = "personal-dot-journey"
	scenarioStandaloneGoSDK     = "custom-agent-standalone-go"
	scenarioStandalonePythonSDK = "custom-agent-standalone-python"
)

// Locator kinds. Each names a place a verifier can re-read WITHOUT the stand:
//
//	durable_row         - a PostgreSQL table plus the exact query.
//	public_face_read    - a route the leg called; the durable record of that call
//	                      is the OTLP access span the backend writes into
//	                      runtime/<instance>/logs/backend.log, named by predicate.
//	durable_store_file  - the connector's own store file, named by path+predicate.
const (
	locatorDurableRow       = "durable_row"
	locatorPublicFaceRead   = "public_face_read"
	locatorDurableStoreFile = "durable_store_file"
)

// frameFreeLocator is the durable stand-in for a terminal frame. ObservedKeys are
// the artifact keys that must carry the leg's observed value for the declaration
// to count; they are checked by frameFreeProof, never taken on trust.
type frameFreeLocator struct {
	Kind              string                 `json:"kind"`
	Table             string                 `json:"table,omitempty"`
	Query             string                 `json:"query,omitempty"`
	Face              string                 `json:"face,omitempty"`
	ExpectedHTTP      int                    `json:"expected_http,omitempty"`
	ExpectedHTTPAnyOf []int                  `json:"expected_http_any_of,omitempty"`
	MustBeEmpty       []string               `json:"must_be_empty,omitempty"`
	MustEqual         []frameFreeExpectation `json:"must_equal,omitempty"`
	Note              string                 `json:"note,omitempty"`
	ObservedKeys      []string               `json:"observed_keys"`
}

// frameFreeExpectation pins one observed key to one value. Presence of a key is not
// proof when the requirement is about WHICH value the platform produced: a stranger
// holding a grant row inside the owner's tenant is present evidence of a breach, and
// an empty breach list is present evidence of isolation. Without this the strongest
// available check would be "the leg mentioned the thing".
type frameFreeExpectation struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
	Why   string `json:"why"`
}

// frameFreeDeclaration is one declared frame-free leg: why no terminal frame
// exists by construction, and what durable fact stands in its place.
type frameFreeDeclaration struct {
	Reason  string           `json:"frame_free_reason"`
	Locator frameFreeLocator `json:"locator"`
}

// frameFreeDeclarations is the ONLY way a leg is allowed to be completed without
// a terminal frame, and a declaration is a promise, not a pass: the leg must
// present the named facts. It is written by hand, one leg at a time, for exactly
// the three acceptance scenarios; TestFrameFreeDeclarationCountsAreExact pins the
// counts, so a new frame-free leg cannot appear without a conscious entry here
// and a matching count change in the same commit.
var frameFreeDeclarations = map[string]map[string]frameFreeDeclaration{
	scenarioPersonalDotJourney: {
		"j1_task_grant_token": {
			Reason: "the leg observes a durable grant row, not a turn: no stream frame exists for a task-token grant",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_identity.grants_child",
				Query:        "SELECT count(*) FROM ahp_identity.grants_child WHERE tenant=$1 AND task=$2 AND context_id=$3",
				ObservedKeys: []string{"grant_rows"}},
		},
		"j2_config_profile_pin": {
			Reason: "the leg pins a configuration revision: the Agent Space never ran, so no terminal frame exists",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_registry.dot_pins",
				Query:        "SELECT snapshot->>'revision' FROM ahp_registry.dot_pins WHERE tenant=$1 AND operation_key=$2",
				Note:         "the pin itself is the durable row; the leg reads the revision the pin returned",
				ObservedKeys: []string{"config_revision"}},
		},
		"j3_model_real_qwen": {
			Reason: "a Model Gateway submit is answered by a generations row, not by a stream frame",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_model.generations",
				Query:        "SELECT count(*) FROM ahp_model.generations WHERE tenant=$1 AND task=$2 AND status='COMPLETED'",
				ObservedKeys: []string{"durable_rows", "generation"}},
		},
		"j4_tool_hitl_result": {
			Reason: "the proof is the PUBLIC GetResult read-back the director required; a tool call has no terminal frame",
			Locator: frameFreeLocator{Kind: locatorPublicFaceRead,
				Table: "runtime/<instance>/logs/backend.log",
				Face:  "/v1/tools/GetResult", ExpectedHTTP: 200,
				Query:        `attributes."http.target"=="/v1/tools/GetResult" && attributes."http.status_code"==200`,
				Note:         "the approval's own durable ledger is ahp_tools_all.approvals (consumed_at); the read-back is the gate",
				ObservedKeys: []string{"get_result_http", "result_carries_note"}},
		},
		"j5_memory": {
			Reason: "personal memory is answered by Search; the durable fact is the memory write, not a frame",
			Locator: frameFreeLocator{Kind: locatorPublicFaceRead,
				Table: "runtime/<instance>/logs/backend.log",
				Face:  "/v1/memory/Search", ExpectedHTTP: 200,
				Query:        `attributes."http.target"=="/v1/memory/Search" && attributes."http.status_code"==200`,
				Note:         "durable memory rows: ahp_longterm.owner_directory / ahp_longterm.memory_records",
				ObservedKeys: []string{"code_word_found", "operation_states"}},
		},
		"j6_file_sha": {
			Reason: "an artifact round trip is answered by a sealed artifact row, not by a stream frame",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_artifacts.artifacts",
				Query:        "SELECT sha256, size, sealed_at IS NOT NULL FROM ahp_artifacts.artifacts WHERE tenant=$1 AND id=$2",
				ObservedKeys: []string{"artifact", "sha256", "sealed"}},
		},
		"j7_history_reload": {
			Reason: "history reload reads the SAME durable run row on a new session; there is no new turn to frame",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_state.runs",
				Query:        "SELECT id, status FROM ahp_state.runs WHERE id=$1",
				ObservedKeys: []string{"same_run"}},
		},
		"j8_hitl_cancel": {
			Reason: "the proof is that NOTHING was written: a refusal has no terminal frame",
			Locator: frameFreeLocator{Kind: locatorDurableStoreFile,
				Table:        "runtime/<instance>/notes/notes.jsonl",
				Query:        `grep -c "never written <stamp>" runtime/<instance>/notes/notes.jsonl` + " must be 0",
				Note:         "second unsigned call must still answer 409; the store must carry no effect",
				ObservedKeys: []string{"held_again", "no_effect_in_store"}},
		},
		"j9_two_user_isolation": {
			Reason: "isolation is proven by refusals on a foreign subject; a refusal has no terminal frame",
			Locator: frameFreeLocator{Kind: locatorPublicFaceRead,
				Table: "runtime/<instance>/logs/backend.log",
				Face:  "/v1/artifacts/Get",
				// The canonical NOT_FOUND merge means a foreign read is refused as
				// NOT_FOUND, not as FORBIDDEN, on most kinds. Naming one code here would
				// make an honest refusal unprovable, so the declaration names the contract
				// SET and the leg's probes[] carry the per-resource code it actually got.
				ExpectedHTTPAnyOf: []int{403, 404},
				Query:             `attributes."http.status_code" IN (403,404) && attributes."http.target" IN ("/v1/artifacts/Get","/v1/state/GetRun","/v1/memory/Search")`,
				Note: "leg owned by Laplace (isolation-scope). Declared, not harness-reread: no span reader " +
					"exists in this harness yet, so provenance stays leg-declared and the verifier sees that. " +
					"Presence of the three refusal booleans is NOT enough - see must_be_empty/must_equal.",
				MustBeEmpty: []string{"leaked_resources"},
				MustEqual: []frameFreeExpectation{
					{Key: "foreign_subject_grant_rows", Value: 0,
						Why: "a grant row for the stranger inside the owner's tenant is a durable breach whatever the endpoints answered"},
					{Key: "owner_control_sentinel_ran", Value: true,
						Why: "if the owner-control trap did not run, the refusals prove nothing about the owner's own access"},
				},
				ObservedKeys: []string{"resources_attempted", "denied_resource_count", "contract_codes",
					"probes", "artifact_denied", "run_denied", "memory_empty_or_denied",
					"foreign_subject_grant_rows", "owner_control_sentinel_ran"}},
		},
		"j11_human_artifact_read": {
			Reason: "the human face answers with bytes; the durable fact is the sealed artifact row",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_artifacts.artifacts",
				Query:        "SELECT sha256, size FROM ahp_artifacts.artifacts WHERE tenant=$1 AND id=$2",
				Note:         "the browser face is /u-ahp/v1/artifacts/download; the sealed row is the durable side",
				ObservedKeys: []string{"sealed_sha256", "received_sha256"}},
		},
	},
	scenarioStandaloneGoSDK: {
		"state_work_created": {
			Reason: "the agent created State work over the SDK: no turn, therefore no terminal frame",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_state.runs",
				Query:        "SELECT id, status, thread_id FROM ahp_state.runs WHERE id=$1",
				ObservedKeys: []string{"durable_run_row", "run", "thread"}},
		},
		"state_run_terminal_cycle": {
			Reason: "the durable stand-in for a terminal frame IS this leg: claim, fence, checkpoint, KV, release",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_state.runs",
				Query:        "SELECT status, payload ? 'close_reason' FROM ahp_state.runs WHERE id=$1",
				Note:         "the release must land: runs.status is terminal (done), the checkpoint row and the KV row exist",
				ObservedKeys: []string{"durable_run_status", "durable_checkpoint_row", "durable_kv_row"}},
		},
		"model_gateway_on_real_qwen": {
			Reason: "the generation row is the durable answer of a submit; the SDK call has no frame",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_model.generations",
				Query:        "SELECT status, coalesce(usage_completion,0), model FROM ahp_model.generations WHERE id=$1",
				ObservedKeys: []string{"durable_generation_status"}},
		},
		"sdk_get_metadata": {
			Reason: "the SDK's Get reads metadata: the artifact row is the durable answer, no frame exists",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_artifacts.artifacts",
				Query:        "SELECT version, sha256, size FROM ahp_artifacts.artifacts WHERE tenant=$1 AND id=$2",
				ObservedKeys: []string{"durable_artifact_version"}},
		},
		"artifact_round_trip": {
			Reason: "a download returns bytes; the sealed row is the durable counterpart of the digest",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_artifacts.artifacts",
				Query:        "SELECT sha256, size FROM ahp_artifacts.artifacts WHERE tenant=$1 AND id=$2",
				ObservedKeys: []string{"sha256", "digest_matches", "artifact"}},
		},
		"text_extraction_gate": {
			Reason: "the sandboxed extraction has no task frame; its cache row and SDK readiness are the durable evidence",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_artifacts.extraction_cache",
				Query:        "SELECT count(*) FROM ahp_artifacts.extraction_cache WHERE tenant=$1 AND sha256=$2 AND mime='text/plain'",
				Note:         "the live scenario requires a positive row count, ready extraction/index, exact-version text and matching downloaded bytes",
				ObservedKeys: []string{"durable_extraction_rows", "http"}},
		},
	},
	scenarioStandalonePythonSDK: {
		"python_agent": {
			Reason: "the child program reports its own verdict; the durable stand-in is its State run/checkpoint/KV rows",
			Locator: frameFreeLocator{Kind: locatorDurableRow,
				Table:        "ahp_state.runs",
				Query:        "SELECT status FROM ahp_state.runs WHERE id=$1",
				Note:         "read by the HARNESS after the child exits, from the ids the child published - not by the child about itself",
				ObservedKeys: []string{"durable_run_status", "durable_checkpoint_row", "durable_kv_row"}},
		},
	},
}

// journeyFrameFreeLegs stays the flat view of the journey declaration: the
// journey guard test counts it (exactly ten) and the journey must never grow an
// undeclared leg.
var journeyFrameFreeLegs = declaredLegNames(scenarioPersonalDotJourney)

func declaredLegNames(scenario string) map[string]bool {
	set := map[string]bool{}
	for name := range frameFreeDeclarations[acceptanceScenarioKey(scenario)] {
		set[name] = true
	}
	return set
}

// acceptanceScenarioKey folds journey variants onto one declaration entry, so a
// defect-recorded replay of the journey is held to exactly the same frame-free
// rule as the journey itself.
func acceptanceScenarioKey(scenario string) string {
	if strings.HasPrefix(scenario, scenarioPersonalDotJourney) {
		return scenarioPersonalDotJourney
	}
	return scenario
}

// declaredFrameFree answers the only question that grants frame-free status.
func declaredFrameFree(scenario, leg string) (frameFreeDeclaration, bool) {
	decls, ok := frameFreeDeclarations[acceptanceScenarioKey(scenario)]
	if !ok {
		return frameFreeDeclaration{}, false
	}
	decl, ok := decls[leg]
	return decl, ok
}

// durableFactPresent decides whether a leg's fact can stand as a terminal
// equivalent. A missing key, a blank string, a false boolean or an empty
// collection is not a fact. Zero IS a fact: an absence query reporting zero rows
// says something, and that answer is the point of the query.
func durableFactPresent(v any) bool {
	switch value := v.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(value) != ""
	case bool:
		return value
	case float64, int, int64, int32, float32, uint64:
		return true
	case map[string]any:
		return len(value) > 0
	case []string:
		return len(value) > 0
	case []any:
		return len(value) > 0
	case []map[string]any:
		return len(value) > 0
	default:
		return true
	}
}

// factIsEmptyCollection answers the requirement-shaped question "the platform
// produced nothing here": an empty list/map/string is the pass condition, and a
// missing value is answered by the caller as "not recorded", never as "empty".
func factIsEmptyCollection(v any) bool {
	switch value := v.(type) {
	case nil:
		return false
	case []string:
		return len(value) == 0
	case []any:
		return len(value) == 0
	case []map[string]any:
		return len(value) == 0
	case map[string]any:
		return len(value) == 0
	case string:
		return strings.TrimSpace(value) == ""
	default:
		return false
	}
}

// factEquals compares an observed value with the declared one across the numeric
// types a value can arrive as: a Go int from the leg and a float64 from a re-read
// artifact mean the same number, and must not fail the gate for that reason.
func factEquals(observed, want any) bool {
	if observed == nil {
		return false
	}
	if a, ok := numericValue(observed); ok {
		if b, ok := numericValue(want); ok {
			return a == b
		}
		return false
	}
	return reflect.DeepEqual(observed, want)
}

func numericValue(v any) (float64, bool) {
	switch value := v.(type) {
	case int:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

// frameFreeProof checks a declared frame-free leg against the durable facts its
// declaration names, and returns the index entry the artifact publishes so a
// verifier reads the locator instead of a flag.
//
// Two provenances exist and are never conflated:
//
//	leg-declared   - the leg wrote the durable fact itself (a PG read or a public
//	                 read the leg performed). The artifact names where to re-check.
//	harness-reread - the leg attached terminal_evidence produced by a query the
//	                 HARNESS executed. The table must agree with the declaration,
//	                 so evidence cannot be pointed at an unrelated row.
func frameFreeProof(decl frameFreeDeclaration, leg map[string]any) (bool, map[string]any) {
	evidence := map[string]any{
		"frame_free_reason": decl.Reason,
		"kind":              decl.Locator.Kind,
		"locator_table":     decl.Locator.Table,
		"locator_query":     decl.Locator.Query,
		"required_facts":    decl.Locator.ObservedKeys,
	}
	if decl.Locator.Face != "" {
		evidence["locator_face"] = decl.Locator.Face
	}
	if decl.Locator.ExpectedHTTP != 0 {
		evidence["locator_expected_http"] = decl.Locator.ExpectedHTTP
	}
	if len(decl.Locator.ExpectedHTTPAnyOf) > 0 {
		evidence["locator_expected_http_any_of"] = decl.Locator.ExpectedHTTPAnyOf
	}
	if len(decl.Locator.MustBeEmpty) > 0 {
		evidence["must_be_empty"] = decl.Locator.MustBeEmpty
	}
	if len(decl.Locator.MustEqual) > 0 {
		evidence["must_equal"] = decl.Locator.MustEqual
	}
	if decl.Locator.Note != "" {
		evidence["locator_note"] = decl.Locator.Note
	}
	missing := []string{}
	for _, key := range decl.Locator.ObservedKeys {
		if !durableFactPresent(leg[key]) {
			missing = append(missing, key)
		}
	}
	// A present key can still contradict the requirement: an empty breach list is the
	// fact, a non-empty one is a breach. Both are present; only one proves isolation.
	violations := []string{}
	for _, key := range decl.Locator.MustBeEmpty {
		value, ok := leg[key]
		if !ok {
			violations = append(violations, key+": not recorded at all")
			continue
		}
		if !factIsEmptyCollection(value) {
			violations = append(violations, fmt.Sprintf("%s: must be empty, observed %v", key, value))
		}
	}
	for _, want := range decl.Locator.MustEqual {
		value, ok := leg[want.Key]
		if !ok {
			violations = append(violations, want.Key+": not recorded at all")
			continue
		}
		if !factEquals(value, want.Value) {
			violations = append(violations, fmt.Sprintf("%s: required %v, observed %v", want.Key, want.Value, value))
		}
	}
	if presented, ok := leg["terminal_evidence"].(map[string]any); ok {
		observed, _ := presented["observed"].(map[string]any)
		evidence["provenance"] = firstNonEmpty(legString(presented, "provenance"), "leg-declared")
		evidence["evidence_table"] = legString(presented, "table")
		evidence["evidence_query"] = legString(presented, "query")
		evidence["table_agrees_with_declaration"] = evidence["evidence_table"] == decl.Locator.Table
		// The read itself must have answered: a row that could not be read is not
		// evidence, whatever the leg claims around it.
		_, readFailed := observed["read_error"]
		proven := len(missing) == 0 && len(violations) == 0 && len(observed) > 0 && !readFailed &&
			evidence["evidence_table"] == decl.Locator.Table &&
			strings.TrimSpace(legString(presented, "query")) != ""
		evidence["observed"] = observed
		evidence["proven"] = proven
		if !proven {
			if readFailed {
				evidence["read_error"] = observed["read_error"]
			}
			evidence["missing_facts"] = missing
			evidence["contradicted_facts"] = violations
		}
		return proven, evidence
	}
	evidence["provenance"] = "leg-declared"
	observed := map[string]any{}
	for _, key := range decl.Locator.ObservedKeys {
		if durableFactPresent(leg[key]) {
			observed[key] = leg[key]
		}
	}
	for _, want := range decl.Locator.MustEqual {
		if factEquals(leg[want.Key], want.Value) {
			observed[want.Key] = leg[want.Key]
		}
	}
	if len(observed) > 0 {
		evidence["observed"] = observed
	}
	evidence["proven"] = len(missing) == 0 && len(violations) == 0
	if len(missing) > 0 {
		evidence["missing_facts"] = missing
	}
	if len(violations) > 0 {
		evidence["contradicted_facts"] = violations
	}
	return len(missing) == 0 && len(violations) == 0, evidence
}

// strictTerminalEnforcement reports whether a scenario classifies under the
// three-state rule. The personal Dot journey (the scenario root adjudicates) is
// always strict; AHP_DOT_E2E_STRICT_TERMINAL=1 extends it to every scenario in
// the lane, which is the state the lane converges to once each scenario declares
// its frame-free legs.
func strictTerminalEnforcement(scenario string) bool {
	if strings.HasPrefix(scenario, "personal-dot-journey") {
		return true
	}
	return strings.TrimSpace(os.Getenv("AHP_DOT_E2E_STRICT_TERMINAL")) == "1"
}

// moreSevereVerdict returns the worse of two verdicts (FAIL > BLOCKED > PASS).
// Used at the record boundary so a declared verdict can only be made worse by
// the evidence, never better.
func moreSevereVerdict(declared, computed string) string {
	rank := func(v string) int {
		switch v {
		case "FAIL":
			return 0
		case "BLOCKED":
			return 1
		case "PASS":
			return 2
		default:
			return 0
		}
	}
	if rank(declared) <= rank(computed) {
		return declared
	}
	return computed
}

// contradictingFailedTag reports a failed_legs tag that belongs to this leg
// (tag "j1" covers leg "j1_task_grant_token"). A leg that writes PASS while the
// scenario's fail() recorded a tag for it is contradictory evidence - the
// optimistic-PASS class where a later PASS line clobbers an honest FAIL.
func contradictingFailedTag(name string, tags []string) string {
	legNumber := journeyLegNumber(name)
	for _, tag := range tags {
		if tag == "" {
			continue
		}
		if name == tag || strings.HasPrefix(name, tag+"_") {
			return tag
		}
		if legNumber != "" && journeyLegNumber(tag) == legNumber {
			return tag
		}
	}
	return ""
}

// journeyLegNumber extracts the journey leg index from a leg name
// ("j1_task_grant_token", "j5-code-word") so a fail() tag names the same leg
// even when the author spelled it differently.
func journeyLegNumber(name string) string {
	if !strings.HasPrefix(name, "j") {
		return ""
	}
	end := 1
	for end < len(name) && name[end] >= '0' && name[end] <= '9' {
		end++
	}
	if end == 1 {
		return ""
	}
	return name[:end]
}

// setLeg records a leg's result and refuses to let a later PASS overwrite an
// honest FAIL written earlier in the same leg.
func setLeg(checks map[string]any, name string, leg map[string]any) {
	if previous, ok := checks[name].(map[string]any); ok {
		if was, _ := previous["status"].(string); was == "FAIL" {
			if now, _ := leg["status"].(string); now == "PASS" {
				return
			}
		}
	}
	checks[name] = leg
}

// overallVerdict is the single source of truth for a scenario verdict:
//   - FAIL when nothing was proven (no legs) or any leg failed;
//   - BLOCKED when any leg is blocked (known recorded external defect);
//   - PASS only when every leg is completed (none blocked, none failed, at least one).
//
// PASS is therefore unreachable while any leg is not completed.
func overallVerdict(completed, blocked, failed []string) string {
	if len(completed) == 0 && len(blocked) == 0 && len(failed) == 0 {
		return "FAIL"
	}
	if len(failed) > 0 {
		return "FAIL"
	}
	if len(blocked) > 0 {
		return "BLOCKED"
	}
	return "PASS"
}

// journeyVerdict applies the guarded leg classification and the verdict rule to a
// scenario checks map. Both the report generator and TestVerdictSemantics* use it,
// so the invariant proven by the test is the invariant used in production.
func journeyVerdict(checks map[string]any) (verdict string, completed, blocked, failed []string) {
	c := classifyLegs("personal-dot-journey", checks)
	return overallVerdict(c.completed, c.blocked, c.failed), c.completed, c.blocked, c.failed
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func status(err error) int {
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return apiErr.Status
	}
	return 0
}

func errorCode(err error) string {
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return ""
}

func digestHex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// shaMatch compares two SHAs allowing an unambiguous short form (>=7 hex).
func shaMatch(a, b string) bool {
	if len(a) < 7 || len(b) < 7 {
		return false
	}
	return strings.HasPrefix(a, b) || strings.HasPrefix(b, a)
}

// worktreeDeltaLabel names the lane worktree's relation to the expected
// revision for the artifact: identical, a harness-only delta (tests/dote2e
// files differ; product bytes are the candidate's), foreign, or unknown.
func worktreeDeltaLabel(worktreeRev, expected string, harnessOnly bool) string {
	if worktreeRev == "" {
		return "git-unavailable"
	}
	if shaMatch(expected, worktreeRev) {
		return "identical"
	}
	if harnessOnly {
		return "harness-only-delta(tests/dote2e)"
	}
	return "foreign"
}

// vcsProvenanceReason enforces docs/acceptance-run.md §0 from the lane side:
// every stand binary must ITSELF report vcs.revision == expected and
// vcs.modified == false (the block build.py read back with `go version -m`).
// A missing block means the binaries were built without stamped VCS info or
// the manifest is forged/stale: §0 says an ldflags revision stamp alone is
// not provenance, so such a run is refused, not waved through.
func vcsProvenanceReason(expected string, vcs map[string]binaryVCS) string {
	if expected == "" {
		return ""
	}
	if len(vcs) == 0 {
		return "build manifest carries no vcs provenance; docs/acceptance-run.md §0 requires vcs.revision/vcs.modified read from the binaries themselves (go version -m) - an ldflags stamp is not provenance"
	}
	names := make([]string, 0, len(vcs))
	for name := range vcs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entry := vcs[name]
		if entry.Modified != "false" {
			return fmt.Sprintf("%s reports vcs.modified=%q; §0 accepts clean-tree binaries only", name, entry.Modified)
		}
		if !shaMatch(expected, entry.Revision) {
			return fmt.Sprintf("%s reports vcs.revision %q, not the expected integration revision %q", name, entry.Revision, expected)
		}
	}
	return ""
}

// revGuardReason returns "" when expected (AHP_EXPECTED_REV: a full SHA or an
// unambiguous short SHA of at least 7 hex characters) names the revision the
// stand was built from, and the lane worktree either IS that revision or is a
// descendant whose delta is harness-only (worktreeDeltaHarnessOnly, decided by
// the caller from productRevReason: ancestor + only tests/dote2e touched). An
// empty worktree rev means git was unavailable: the build rev is still checked.
// Any mismatch yields an explicit reason - evidence from the wrong tree must
// fail loudly as BLOCKED, never quietly certify itself under another rev.
func revGuardReason(expected, buildRev, worktreeRev string, worktreeDeltaHarnessOnly bool) string {
	if expected == "" {
		return ""
	}
	if !shaMatch(expected, buildRev) {
		return fmt.Sprintf("stand binaries are built from revision %q, not the expected integration revision %q", buildRev, expected)
	}
	if worktreeRev != "" && !shaMatch(expected, worktreeRev) && !worktreeDeltaHarnessOnly {
		return fmt.Sprintf("lane worktree HEAD is %q, not the expected integration revision %q and not a tests/dote2e-only delta over it", worktreeRev, expected)
	}
	return ""
}

// gitWorktreeRev best-effort reports the lane worktree HEAD ("" on failure).
func gitWorktreeRev() string {
	dir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return ""
	}
	out, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// gitWorktreeDirty reports whether the lane worktree carries uncommitted changes at
// record time. On git failure it returns (false, reason) so the artifact records the
// uncertainty instead of pretending cleanliness.
func gitWorktreeDirty() (bool, string) {
	dir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return false, err.Error()
	}
	out, err := exec.Command("git", "-C", dir, "status", "--porcelain").Output()
	if err != nil {
		return false, err.Error()
	}
	return strings.TrimSpace(string(out)) != "", ""
}

// productRevReason validates the candidate/harness separation: the product revision
// must be an ancestor of the built revision, and every file differing between them
// must live under tests/dote2e (harness-only delta). "" means the separation holds.
func productRevReason(product, buildRev string) string {
	if len(product) < 7 || len(buildRev) < 7 {
		return "AHP_PRODUCT_REV or build revision is not a usable SHA"
	}
	dir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return err.Error()
	}
	if err := exec.Command("git", "-C", dir, "merge-base", "--is-ancestor", product, buildRev).Run(); err != nil {
		return fmt.Sprintf("product revision %s is not an ancestor of the built revision %s", product, buildRev)
	}
	out, err := exec.Command("git", "-C", dir, "diff", "--name-only", product+".."+buildRev).Output()
	if err != nil {
		return "cannot diff product revision against build revision: " + err.Error()
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "tests/dote2e/") {
			return fmt.Sprintf("build delta from the product candidate touches %q - only tests/dote2e is allowed", line)
		}
	}
	return ""
}

// acceptanceManifestReason binds a run to the director's ACCEPTANCE-MANIFEST:
// the file must be readable, name AHP_PRODUCT_REV as its source.revision, and
// carry the git tree that really belongs to that revision. Evidence then
// references a manifest it was mechanically checked against, never a decorative
// path. An unset AHP_ACCEPTANCE_MANIFEST requests no binding ("" = pass).
func acceptanceManifestReason(path, product string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "acceptance manifest unreadable: " + err.Error()
	}
	var m struct {
		Source struct {
			Revision string `json:"revision"`
			Tree     string `json:"tree"`
		} `json:"source"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return "acceptance manifest is not valid JSON: " + err.Error()
	}
	if !shaMatch(product, m.Source.Revision) {
		return fmt.Sprintf("acceptance manifest names source.revision %q, not the product revision %q under test", m.Source.Revision, product)
	}
	dir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return err.Error()
	}
	out, gerr := exec.Command("git", "-C", dir, "rev-parse", m.Source.Revision+"^{tree}").Output()
	if gerr != nil {
		return "cannot resolve manifest tree: " + gerr.Error()
	}
	if tree := strings.TrimSpace(string(out)); tree != m.Source.Tree {
		return fmt.Sprintf("manifest source.tree %q != git tree %q of %s", m.Source.Tree, tree, m.Source.Revision)
	}
	return ""
}

// loadCurrent best-effort reports the 1/5/15-minute load average for evidence
// honesty: a capture taken under competing heavy work must say so (director rule
// 2026-11-09: report load like Goodall does, never silently).
func loadCurrent() string {
	out, err := exec.Command("sysctl", "-n", "vm.loadavg").Output()
	if err != nil {
		return "unavailable: " + err.Error()
	}
	return strings.TrimSpace(string(out))
}

// ---- durable reads that stand in place of a terminal frame ----
//
// A frame-free leg may be completed only on a durable fact (see frameFreeProof).
// Where the lane can read that fact itself, it does: these helpers run the query
// the declaration names against the lane's own database and hand the leg an
// observation whose provenance is "harness-reread". A read that fails leaves the
// required fact absent, which makes the leg UNPROVEN - the harness never fills a
// gap with a description of a query it did not run.

// durableRow is one row-level durable observation: where it came from and what it
// answered.
type durableRow struct {
	Table    string
	Query    string
	Observed map[string]any
}

// evidence renders the observation in the shape frameFreeProof validates.
func (d durableRow) evidence() map[string]any {
	return map[string]any{"kind": locatorDurableRow, "table": d.Table, "query": d.Query,
		"observed": d.Observed, "provenance": "harness-reread"}
}

// attachDurableEvidence publishes the harness's own durable read on a leg: the
// evidence object plus each observed fact at the top level, so the declaration
// check and a human reader see the same values. A leg's own value for a key is
// never overwritten; a disagreement is recorded instead, because silently
// preferring either side is how a report starts lying.
func attachDurableEvidence(leg map[string]any, d durableRow) {
	evidence := d.evidence()
	if previous, ok := leg["terminal_evidence"].(map[string]any); ok {
		// A leg can stand on more than one row (run + checkpoint + KV). The FIRST
		// read stays the primary table the declaration is checked against, the rest
		// are recorded as also-read so nothing disappears, and every observed value
		// accumulates into one object instead of overwriting the last one.
		evidence["table"] = previous["table"]
		evidence["query"] = previous["query"]
		also, _ := previous["also_read"].([]map[string]any)
		also = append(also, map[string]any{"table": d.Table, "query": d.Query})
		if prior, ok := previous["observed"].(map[string]any); ok {
			merged := map[string]any{}
			for key, value := range prior {
				merged[key] = value
			}
			for key, value := range d.Observed {
				merged[key] = value
			}
			evidence["observed"] = merged
		}
		evidence["also_read"] = also
	}
	leg["terminal_evidence"] = evidence
	for key, value := range d.Observed {
		existing, taken := leg[key]
		if !taken {
			leg[key] = value
			continue
		}
		if fmt.Sprint(existing) != fmt.Sprint(value) {
			conflicts, _ := leg["durable_value_conflict"].(map[string]any)
			if conflicts == nil {
				conflicts = map[string]any{}
			}
			conflicts[key] = map[string]any{"leg": existing, "durable": value}
			leg["durable_value_conflict"] = conflicts
		}
	}
}

// durableReadFailed is the observation a failed read produces: no required fact
// is present, so the leg cannot be counted as completed.
func durableReadFailed(table, query string, err error) durableRow {
	return durableRow{Table: table, Query: query,
		Observed: map[string]any{"read_error": truncate(errText(err), 200)}}
}

const durableRunQuery = "SELECT id, status, thread_id, coalesce(payload->>'close_reason','') FROM ahp_state.runs WHERE id=$1"

// durableRunRowObservation reads the run row: the durable terminal state of the
// stand-alone agent's work (status done/failed/canceled is the stand-in for the
// terminal frame a turn would have emitted).
func durableRunRowObservation(stand *instance, runID string) durableRow {
	if stand == nil || stand.Pool == nil {
		return durableReadFailed("ahp_state.runs", durableRunQuery,
			errors.New("the lane stand has no database pool"))
	}
	var id, status, threadID, closeReason string
	err := stand.Pool.QueryRow(context.Background(), durableRunQuery, runID).
		Scan(&id, &status, &threadID, &closeReason)
	if err != nil {
		return durableReadFailed("ahp_state.runs", durableRunQuery, err)
	}
	return durableRow{Table: "ahp_state.runs", Query: durableRunQuery,
		Observed: map[string]any{"durable_run_row": "present", "durable_run_status": status,
			"durable_run_thread": threadID, "durable_run_close_reason": closeReason}}
}

const durableCheckpointQuery = "SELECT id, namespace FROM ahp_state.checkpoints WHERE thread_id=$1 AND namespace=$2 AND key=$3"

// durableCheckpointObservation proves the checkpoint row survived the fenced write.
func durableCheckpointObservation(stand *instance, threadID, namespace, key string) durableRow {
	if stand == nil || stand.Pool == nil {
		return durableReadFailed("ahp_state.checkpoints", durableCheckpointQuery,
			errors.New("the lane stand has no database pool"))
	}
	var id, ns string
	err := stand.Pool.QueryRow(context.Background(), durableCheckpointQuery, threadID, namespace, key).
		Scan(&id, &ns)
	if err != nil {
		return durableReadFailed("ahp_state.checkpoints", durableCheckpointQuery, err)
	}
	return durableRow{Table: "ahp_state.checkpoints", Query: durableCheckpointQuery,
		Observed: map[string]any{"durable_checkpoint_row": "present", "durable_checkpoint_id": id}}
}

const durableKVQuery = "SELECT version, deleted FROM ahp_state.kv WHERE thread_id=$1 AND key=$2 AND $3 = ANY(namespace)"

// durableKVObservation proves the KV row the agent wrote is the durable answer.
func durableKVObservation(stand *instance, threadID string, namespace []string, key string) durableRow {
	if stand == nil || stand.Pool == nil {
		return durableReadFailed("ahp_state.kv", durableKVQuery,
			errors.New("the lane stand has no database pool"))
	}
	var version int64
	var deleted bool
	err := stand.Pool.QueryRow(context.Background(), durableKVQuery, threadID, key, namespace[0]).
		Scan(&version, &deleted)
	if err != nil {
		return durableReadFailed("ahp_state.kv", durableKVQuery, err)
	}
	state := "present"
	if deleted {
		state = "tombstoned"
	}
	return durableRow{Table: "ahp_state.kv", Query: durableKVQuery,
		Observed: map[string]any{"durable_kv_row": state, "durable_kv_version": version}}
}

const durableGenerationQuery = "SELECT status, usage_completion, usage_prompt, model FROM ahp_model.generations WHERE id=$1"

// durableGenerationObservation reads the generation row: the durable answer of a
// Model Gateway submit.
func durableGenerationObservation(stand *instance, generationID string) durableRow {
	if stand == nil || stand.Pool == nil {
		return durableReadFailed("ahp_model.generations", durableGenerationQuery,
			errors.New("the lane stand has no database pool"))
	}
	var status, model string
	var completion, prompt int
	err := stand.Pool.QueryRow(context.Background(), durableGenerationQuery, generationID).
		Scan(&status, &completion, &prompt, &model)
	if err != nil {
		return durableReadFailed("ahp_model.generations", durableGenerationQuery, err)
	}
	return durableRow{Table: "ahp_model.generations", Query: durableGenerationQuery,
		Observed: map[string]any{"durable_generation_status": status,
			"durable_generation_completion_tokens": completion,
			"durable_generation_prompt_tokens":     prompt,
			"durable_generation_model":             model}}
}

const durableArtifactQuery = "SELECT coalesce(version,'none'), coalesce(sha256,'none'), coalesce(size,-1) FROM ahp_artifacts.artifacts WHERE tenant=$1 AND id=$2"

// durableArtifactObservation reads the sealed artifact row behind an SDK Get or a
// download.
func durableArtifactObservation(stand *instance, artifactID string) durableRow {
	if stand == nil || stand.Pool == nil {
		return durableReadFailed("ahp_artifacts.artifacts", durableArtifactQuery,
			errors.New("the lane stand has no database pool"))
	}
	var version, sha string
	var size int64
	err := stand.Pool.QueryRow(context.Background(), durableArtifactQuery, stand.Scope["tenant"], artifactID).
		Scan(&version, &sha, &size)
	if err != nil {
		return durableReadFailed("ahp_artifacts.artifacts", durableArtifactQuery, err)
	}
	return durableRow{Table: "ahp_artifacts.artifacts", Query: durableArtifactQuery,
		Observed: map[string]any{"durable_artifact_version": version, "durable_artifact_sha256": sha,
			"durable_artifact_bytes": size, "sha256": sha}}
}

const durableExtractionQuery = "SELECT count(*) FROM ahp_artifacts.extraction_cache WHERE tenant=$1 AND sha256=$2 AND mime='text/plain'"

// durableExtractionObservation counts extraction rows for these exact bytes. Zero
// is the answer the text gate claims: extraction never reached ready, which is why
// the download answers FORBIDDEN.
func durableExtractionObservation(stand *instance, sha256hex string) durableRow {
	if stand == nil || stand.Pool == nil {
		return durableReadFailed("ahp_artifacts.extraction_cache", durableExtractionQuery,
			errors.New("the lane stand has no database pool"))
	}
	var rows int
	err := stand.Pool.QueryRow(context.Background(), durableExtractionQuery, stand.Scope["tenant"], sha256hex).
		Scan(&rows)
	if err != nil {
		return durableReadFailed("ahp_artifacts.extraction_cache", durableExtractionQuery, err)
	}
	return durableRow{Table: "ahp_artifacts.extraction_cache", Query: durableExtractionQuery,
		Observed: map[string]any{"durable_extraction_rows": rows}}
}
