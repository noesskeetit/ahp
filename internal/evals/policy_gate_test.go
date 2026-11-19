package evals_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// isolatedStore stands up the evals plane in a freshly created database so the
// policy lifecycle test sees a plane with NO policy at all -- a state that
// append-only policy history makes impossible to reach inside a shared database
// (history cannot be deleted, by design). The database is dropped on cleanup.
func isolatedStore(t *testing.T) (*evals.Store, *pgxpool.Pool) {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("AHP_TEST_DATABASE_URL"))
	if raw == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	base, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	name := "evals_iso_" + hex.EncodeToString(b[:])
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		admin.Close()
		t.Fatalf("create isolated database: %v", err)
	}
	admin.Close()
	iso := *base
	iso.Path = "/" + name
	pool, err := pgxpool.New(ctx, iso.String())
	if err != nil {
		t.Fatalf("iso pool: %v", err)
	}
	s := evals.NewStore(pool)
	if err := s.Migrate(ctx); err != nil {
		pool.Close()
		t.Fatalf("iso migrate: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		a, err := pgxpool.New(ctx, raw)
		if err != nil {
			t.Errorf("drop admin pool: %v", err)
			return
		}
		defer a.Close()
		if _, err := a.Exec(ctx, `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`); err != nil {
			t.Errorf("drop isolated database: %v", err)
		}
	})
	return s, pool
}

// The row-24 gate in full: publication/promotion is denied when no policy is
// declared (fail closed), when no verdict exists, when the verdict was earned
// under a superseded policy, when it aged out of the policy window, when the
// policy demands a real target and the run hit a fixture, and when a PASS is
// partial against the policy's required tasks. Every denial is CONFLICT with an
// explicit reason, and every fix re-opens the gate only through a fresh honest
// run -- no request mutation can substitute for one.
func TestPolicyGateLifecycle(t *testing.T) {
	s, db := isolatedStore(t)
	ctx := context.Background()
	d := randDigest(t)
	passTasks := func(ids ...string) []evals.TaskResult {
		out := make([]evals.TaskResult, 0, len(ids))
		for _, id := range ids {
			out = append(out, evals.TaskResult{TaskID: id, Passed: true, TerminalState: evals.TerminalCompleted, Cost: 0.01})
		}
		return out
	}

	// (1) No policy at all: the plane stays closed.
	if err := s.RequirePassedVerdict(ctx, d); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("gate with no policy = %v, want CONFLICT", err)
	}
	if err := s.RequirePassedVerdict(ctx, d); !strings.Contains(err.Error(), "no evaluation policy") {
		t.Fatalf("gate reason = %v, want the no-policy denial", err)
	}

	// (2) Policy declared, no verdict.
	p1, err := s.SetPolicy(ctx, evals.Policy{ID: "iso-p1", TaskSetID: "refset", RequiredTaskIDs: []string{"t1", "t2"}, MaxAgeSeconds: 3600})
	if err != nil {
		t.Fatalf("SetPolicy p1: %v", err)
	}
	if p1.Digest == "" {
		t.Fatal("policy digest not computed")
	}
	if err := s.RequirePassedVerdict(ctx, d); !strings.Contains(err.Error(), "no evaluation verdict") {
		t.Fatalf("gate with no verdict = %v, want the missing-verdict denial", err)
	}

	// (3) Partial PASS refuses to record: missing policy-required tasks.
	if _, err := s.Record(ctx, evals.RecordInput{AgentDigest: d, TaskSetID: "refset", Outcome: evals.OutcomePass,
		Tasks: passTasks("t1"), PolicyID: "iso-p1", TargetKind: evals.TargetFixture}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("partial PASS = %v, want INVALID", err)
	}
	// Wrong reference set, unknown policy and missing provenance all refuse too.
	if _, err := s.Record(ctx, evals.RecordInput{AgentDigest: d, TaskSetID: "other-set", Outcome: evals.OutcomePass,
		Tasks: passTasks("t1", "t2"), PolicyID: "iso-p1", TargetKind: evals.TargetFixture}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("PASS on foreign set = %v, want INVALID", err)
	}
	if _, err := s.Record(ctx, evals.RecordInput{AgentDigest: d, TaskSetID: "refset", Outcome: evals.OutcomePass,
		Tasks: passTasks("t1", "t2"), PolicyID: "no-such-policy", TargetKind: evals.TargetFixture}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("unknown policy = %v, want INVALID", err)
	}
	if _, err := s.Record(ctx, evals.RecordInput{AgentDigest: d, TaskSetID: "refset", Outcome: evals.OutcomePass,
		Tasks: passTasks("t1", "t2"), PolicyID: "iso-p1"}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("missing target kind = %v, want INVALID", err)
	}

	// (4) An honest full PASS under the current policy opens the gate.
	if _, err := s.Record(ctx, evals.RecordInput{AgentDigest: d, TaskSetID: "refset", Outcome: evals.OutcomePass,
		Tasks: passTasks("t1", "t2"), PolicyID: "iso-p1", TargetKind: evals.TargetFixture}); err != nil {
		t.Fatalf("full PASS: %v", err)
	}
	if err := s.RequirePassedVerdict(ctx, d); err != nil {
		t.Fatalf("gate after honest PASS = %v, want nil", err)
	}

	// (5) The operator renews the policy: the same verdict is now stale by
	// policy identity -- re-declaring a mutated policy under a NEW id denies.
	if _, err := s.SetPolicy(ctx, evals.Policy{ID: "iso-p2", TaskSetID: "refset", RequiredTaskIDs: []string{"t1", "t2", "t3"}, MaxAgeSeconds: 3600}); err != nil {
		t.Fatalf("SetPolicy p2: %v", err)
	}
	if err := s.RequirePassedVerdict(ctx, d); !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("gate under new policy = %v, want the superseded denial", err)
	}

	// (6) Re-declaring identical content is idempotent and keeps it current.
	again, err := s.SetPolicy(ctx, evals.Policy{ID: "iso-p2", TaskSetID: "refset", RequiredTaskIDs: []string{"t1", "t2", "t3"}, MaxAgeSeconds: 3600})
	if err != nil || again.Digest == "" {
		t.Fatalf("idempotent SetPolicy: %v", err)
	}
	// Mutating the content under a used id is refused -- verdicts reference
	// policy digests that must never be rewritten.
	if _, err := s.SetPolicy(ctx, evals.Policy{ID: "iso-p2", TaskSetID: "refset", RequiredTaskIDs: []string{"t1"}, MaxAgeSeconds: 3600}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("policy content mutation = %v, want CONFLICT", err)
	}

	// (7) Freshness window: an aged-out verdict denies even under its own policy.
	p3, err := s.SetPolicy(ctx, evals.Policy{ID: "iso-p3", TaskSetID: "refset", RequiredTaskIDs: []string{"t1", "t2"}, MaxAgeSeconds: 60})
	if err != nil {
		t.Fatalf("SetPolicy p3: %v", err)
	}
	if _, err := s.Record(ctx, evals.RecordInput{AgentDigest: d, TaskSetID: "refset", Outcome: evals.OutcomePass,
		Tasks: passTasks("t1", "t2"), PolicyID: "iso-p3", TargetKind: evals.TargetFixture}); err != nil {
		t.Fatalf("PASS under p3: %v", err)
	}
	if err := s.RequirePassedVerdict(ctx, d); err != nil {
		t.Fatalf("gate right after PASS under p3 = %v, want nil", err)
	}
	if p3.ID != "iso-p3" {
		t.Fatalf("policy identity echo: %+v", p3)
	}
	// Age the honest PASS past the policy window the way clock drift would:
	// the stored timestamp, not the verdict row's existence, is what expires.
	if _, err := db.Exec(ctx, `UPDATE ahp_evals.verdicts SET created_at = created_at - interval '10 minutes' WHERE agent_digest = $1`, d); err != nil {
		t.Fatalf("age verdict: %v", err)
	}
	if err := s.RequirePassedVerdict(ctx, d); !strings.Contains(err.Error(), "older than the policy") {
		t.Fatalf("gate after aging = %v, want the expired denial", err)
	}

	// (8) Real-target requirement: a fixture verdict cannot satisfy a policy
	// that demands real-target runs (row 24: fixtures never close the row).
	if _, err := s.SetPolicy(ctx, evals.Policy{ID: "iso-p4", TaskSetID: "refset", RequiredTaskIDs: []string{"t1", "t2"}, MaxAgeSeconds: 3600, RequireRealTarget: true}); err != nil {
		t.Fatalf("SetPolicy p4: %v", err)
	}
	d2 := randDigest(t)
	if _, err := s.Record(ctx, evals.RecordInput{AgentDigest: d2, TaskSetID: "refset", Outcome: evals.OutcomePass,
		Tasks: passTasks("t1", "t2"), PolicyID: "iso-p4", TargetKind: evals.TargetFixture}); err != nil {
		t.Fatalf("fixture PASS under p4: %v", err)
	}
	if err := s.RequirePassedVerdict(ctx, d2); !strings.Contains(err.Error(), "fixture target") {
		t.Fatalf("gate on fixture verdict = %v, want the fixture denial", err)
	}
	d3 := randDigest(t)
	if _, err := s.Record(ctx, evals.RecordInput{AgentDigest: d3, TaskSetID: "refset", Outcome: evals.OutcomePass,
		Tasks: passTasks("t1", "t2"), PolicyID: "iso-p4", TargetKind: evals.TargetReal}); err != nil {
		t.Fatalf("real PASS under p4: %v", err)
	}
	if err := s.RequirePassedVerdict(ctx, d3); err != nil {
		t.Fatalf("gate on real-target verdict = %v, want nil", err)
	}

	// (9) Target revision binding: a verdict for one digest never clears another.
	if err := s.RequirePassedVerdict(ctx, randDigest(t)); !strings.Contains(err.Error(), "no evaluation verdict") {
		t.Fatalf("gate for an un-evaluated digest = %v, want the missing-verdict denial", err)
	}

	// (10) Freshness reports the same verdict as the gate, with a reason.
	f, err := s.Freshness(ctx, d2)
	if err != nil || f.Fresh || f.Reason == "" {
		t.Fatalf("Freshness on fixture verdict = %+v %v, want stale with reason", f, err)
	}
}

func TestSetPolicyValidation(t *testing.T) {
	s := store(t)
	ctx := context.Background()
	if _, err := s.SetPolicy(ctx, evals.Policy{ID: "", TaskSetID: "x", MaxAgeSeconds: 60}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("empty id = %v, want INVALID", err)
	}
	if _, err := s.SetPolicy(ctx, evals.Policy{ID: "v", TaskSetID: "", MaxAgeSeconds: 60}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("empty task set = %v, want INVALID", err)
	}
	if _, err := s.SetPolicy(ctx, evals.Policy{ID: "v", TaskSetID: "x", MaxAgeSeconds: 0}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("zero window = %v, want INVALID", err)
	}
	if _, err := s.SetPolicy(ctx, evals.Policy{ID: "v", TaskSetID: "x", RequiredTaskIDs: []string{"a", "a"}, MaxAgeSeconds: 60}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("duplicate required task = %v, want INVALID", err)
	}
}
