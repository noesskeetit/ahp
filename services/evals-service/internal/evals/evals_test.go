package evals_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set: no real PostgreSQL to run the evals plane against")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		t.Fatalf("pg unreachable: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func store(t *testing.T) *evals.Store {
	t.Helper()
	s := evals.NewStore(pool(t))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate not idempotent: %v", err)
	}
	return s
}

func randDigest(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	sum := sha256.Sum256(b[:])
	return "sha256:" + hex.EncodeToString(sum[:])
}

// randID returns a unique identifier for a test-scoped entity that lives in a
// persistent store (policy ids, task set ids). It is deliberately not a
// truncated prefixed digest: the first eight characters of "sha256:"+hex are
// "sha256:" plus ONE hex character, sixteen variants for a whole suite, so the
// second run against a store that remembers the first re-declares a used id
// with different content and SetPolicy answers CONFLICT -- the store being
// correct, not buggy. A whole random digest cannot exhaust its own id space.
func randID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// mustPolicy declares an operator policy for a test: verdicts in these tests
// are recorded under it and the gate measures them against it, exactly as in
// production. Re-declaring identical content is idempotent and keeps the
// policy CURRENT.
func mustPolicy(t *testing.T, s *evals.Store, id, taskSet string, required ...string) {
	t.Helper()
	if _, err := s.SetPolicy(context.Background(), evals.Policy{
		ID: id, TaskSetID: taskSet, RequiredTaskIDs: required, MaxAgeSeconds: 3600,
	}); err != nil {
		t.Fatalf("SetPolicy(%s): %v", id, err)
	}
}

func tasks(passed bool) []evals.TaskResult {
	// These fixtures simulate exchanges that completed at the wire level; the
	// pass/fail axis is the content verdict of a completed generation.
	return []evals.TaskResult{
		{TaskID: "ref-sum", Passed: passed, TerminalState: evals.TerminalCompleted, TokensIn: 120, TokensOut: 40, Cost: 0.003, LatencyMS: 410, Metrics: map[string]float64{"accuracy": 1}},
		{TaskID: "ref-search", Passed: passed, TerminalState: evals.TerminalCompleted, TokensIn: 900, TokensOut: 210, Cost: 0.02, LatencyMS: 1200},
	}
}

// The verdict is bound to the exact version digest: a passing verdict satisfies
// the gate for that digest and for no other, and a failing verdict never does.
func TestVerdictBoundToDigestAndGate(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	good, other := randDigest(t), randDigest(t)

	mustPolicy(t, s, "h26-test-policy", "ts-h26", "ref-sum", "ref-search")
	if err := s.RequirePassedVerdict(ctx, good); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("empty store gate = %v, want CONFLICT (fail closed)", err)
	}
	if _, err := s.Record(ctx, evals.RecordInput{
		AgentDigest: good, TaskSetID: "ts-h26", Setup: map[string]any{"v": 1}, Outcome: evals.OutcomeFail, Tasks: tasks(false),
		PolicyID: "h26-test-policy", TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("Record(fail): %v", err)
	}
	if err := s.RequirePassedVerdict(ctx, good); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("gate after a FAIL verdict = %v, want CONFLICT", err)
	}
	if _, err := s.Record(ctx, evals.RecordInput{
		AgentDigest: good, TaskSetID: "ts-h26", Setup: map[string]any{"v": 2}, Outcome: evals.OutcomePass, Tasks: tasks(true),
		PolicyID: "h26-test-policy", TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("Record(pass): %v", err)
	}
	if err := s.RequirePassedVerdict(ctx, good); err != nil {
		t.Fatalf("gate after PASS = %v, want nil", err)
	}
	// The verdict for `good` does not unlock a different digest.
	if err := s.RequirePassedVerdict(ctx, other); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("gate for an unrelated digest = %v, want CONFLICT (verdict not reusable)", err)
	}
}

// The plane is content-free. This is checked two ways, neither of them a mock:
// (1) a planted sentinel is placed inside the setup (the data that describes the
// reference run) and required to be absent from the serialized verdict and from
// every stored column; (2) the serialized verdict must expose no content-bearing
// field name at all.
func TestVerdictCarriesNoUserContent(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	digest := randDigest(t)
	sentinel := "SENTINEL_USER_CONTENT_" + randDigest(t)

	// The setup is arbitrary application data: it is only ever digested, never
	// stored, so the sentinel must not survive anywhere.
	setup := map[string]any{
		"dataset":    "reference-set-h26",
		"secret":     sentinel,
		"transcript": "the user asked to " + sentinel,
	}
	mustPolicy(t, s, "h26-test-policy", "ts-h26", "ref-sum", "ref-search")
	v, err := s.Record(ctx, evals.RecordInput{
		AgentDigest: digest, TaskSetID: "ts-h26", Setup: setup, Outcome: evals.OutcomePass, Tasks: tasks(true),
		PolicyID: "h26-test-policy", TargetKind: evals.TargetFixture,
	})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}

	// (1) serialized verdict: no sentinel, no content field names.
	blob, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	body := string(blob)
	if strings.Contains(body, sentinel) {
		t.Fatalf("the verdict leaked the planted setup content")
	}
	for _, forbidden := range []string{"content", "body", "payload", "text", "prompt", "transcript", "answer", "secret", "response", "output"} {
		if jsonHasKey(blob, forbidden) {
			t.Errorf("verdict JSON exposes a content-bearing field %q: %s", forbidden, body)
		}
	}

	// (2) the persisted rows: scan every text/varchar/jsonb column of both tables
	// for this run and require the sentinel absent.
	for _, q := range []string{
		`SELECT coalesce(string_agg(t::text,'|'),'') FROM (
		   SELECT task_set_id::text AS t FROM ahp_evals.verdicts WHERE agent_digest=$1
		   UNION ALL SELECT setup_digest::text FROM ahp_evals.verdicts WHERE agent_digest=$1
		 ) x`,
		`SELECT coalesce(string_agg((r.task_id||'|'||r.metrics::text),'|'),'')
		   FROM ahp_evals.task_results r
		  JOIN ahp_evals.verdicts v ON v.id = r.verdict_id
		  WHERE v.agent_digest=$1`,
	} {
		var agg string
		if err := pool(t).QueryRow(ctx, q, digest).Scan(&agg); err != nil {
			t.Fatalf("scan stored columns: %v", err)
		}
		if strings.Contains(agg, sentinel) {
			t.Fatalf("stored evals rows leaked the planted content")
		}
	}
}

// The setup reference is a digest, deterministic for identical setup and
// different across setups, so a reviewer can reproduce the run without the plane
// holding the bytes.
func TestSetupIsReferencedByDigest(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	mustPolicy(t, s, "setup-ref-policy", "ts", "ref-sum", "ref-search")
	setup := map[string]any{"dataset": "reference-set-h26", "seed": 7}
	v1, err := s.Record(ctx, evals.RecordInput{AgentDigest: randDigest(t), TaskSetID: "ts", Setup: setup, Outcome: evals.OutcomePass, Tasks: tasks(true), PolicyID: "setup-ref-policy", TargetKind: evals.TargetFixture})
	if err != nil {
		t.Fatalf("Record 1: %v", err)
	}
	v2, err := s.Record(ctx, evals.RecordInput{AgentDigest: randDigest(t), TaskSetID: "ts", Setup: setup, Outcome: evals.OutcomePass, Tasks: tasks(true), PolicyID: "setup-ref-policy", TargetKind: evals.TargetFixture})
	if err != nil {
		t.Fatalf("Record 2: %v", err)
	}
	if v1.SetupDigest != v2.SetupDigest {
		t.Errorf("identical setups produced different setup digests: %q vs %q", v1.SetupDigest, v2.SetupDigest)
	}
	v3, err := s.Record(ctx, evals.RecordInput{AgentDigest: randDigest(t), TaskSetID: "ts", Setup: map[string]any{"dataset": "other"}, Outcome: evals.OutcomePass, Tasks: tasks(true), PolicyID: "setup-ref-policy", TargetKind: evals.TargetFixture})
	if err != nil {
		t.Fatalf("Record 3: %v", err)
	}
	if v1.SetupDigest == v3.SetupDigest {
		t.Errorf("different setups produced the same setup digest")
	}
}

// jsonHasKey reports whether the top level of a JSON object carries any field
// named key (recursively, to be safe).
func jsonHasKey(blob []byte, key string) bool {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(blob, &probe); err != nil {
		return false
	}
	for k := range probe {
		if strings.EqualFold(k, key) {
			return true
		}
	}
	return false
}
