// Package evals: the "оценки перед публикацией" plane (PDF rev 3.2, с. 42/48;
// the pipeline row pairs Registry's "ревизии · публикация" with Evals'
// "оценки · трассы без данных", and the build/verify column names "evals на
// эталонных задачах").
//
// A Verdict is the record that a published agent version -- identified by its
// content digest, the same sha256 digest the Registry pins a version to -- was
// run on a named reference task set, together with the outcome and the per-task
// cost. It is the precondition the Registry's publication path requires: an
// agent version cannot be published for which no passing verdict exists.
//
// The plane is content-free by construction, exactly as the PDF demands
// ("трассы и метрики без содержимого", "только идентификаторы ... только ссылки").
// Nothing in a Verdict, a TaskResult or their stored rows can carry user data:
// every field is an identifier, an enumerated outcome, or a number. The reference
// task set and its setup are referenced by a DIGEST, never by their bytes -- the
// input that produced the verdict is named, not copied. This is enforced by the
// shape of the types (there is no string field that could hold a prompt, a
// transcript or a tool output) and asserted by tests that search the whole
// persisted verdict for a planted sentinel and require it absent.
package evals

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Schema is the exclusive schema of this plane.
const Schema = "ahp_evals"

// Outcome is the verdict of one evaluation run: pass means every reference task
// met its acceptance; fail means at least one did not. Only a pass unblocks
// publication.
type Outcome string

const (
	OutcomePass Outcome = "pass"
	OutcomeFail Outcome = "fail"
)

func (o Outcome) valid() bool { return o == OutcomePass || o == OutcomeFail }

// TargetKind records how the evaluated target was reached, so a verdict states
// its own provenance instead of leaving it to the reader's trust. "real" means
// the run drove an actual deployment over HTTPS; "fixture" means a local test
// endpoint. An operator policy can require real-target verdicts -- a fixture
// verdict then proves the machinery works but does not certify the artifact
// (matrix row 24: fixture targets NEVER close "реальный target").
type TargetKind string

const (
	TargetReal    TargetKind = "real"
	TargetFixture TargetKind = "fixture"
)

func (k TargetKind) valid() bool { return k == TargetReal || k == TargetFixture }

// Policy is the operator-declared definition of what a fresh evaluation means:
// which reference set and tasks a run must cover, how old a verdict may
// stay, and whether fixture-target verdicts count. Declaring a policy is
// append-only: a new policy id (or a re-declaration with the same id and
// content) never rewrites history, and any content change under a used id is
// refused. The CURRENT policy is the most recently declared one; the gate
// measures freshness against it, so "актуальный policy" is a stored fact, not
// a caller assertion. Digest is computed by the plane over the policy content,
// so a verdict can be bound to the exact policy bytes it was earned under.
type Policy struct {
	ID                string    `json:"id"`
	TaskSetID         string    `json:"task_set_id"`
	RequiredTaskIDs   []string  `json:"required_task_ids"`
	MaxAgeSeconds     int64     `json:"max_age_seconds"`
	RequireRealTarget bool      `json:"require_real_target"`
	Digest            string    `json:"digest"`
	CreatedAt         time.Time `json:"created_at"`
	DeclaredAt        time.Time `json:"declared_at"`
}

func (p Policy) content() map[string]any {
	return map[string]any{
		"id": p.ID, "task_set_id": p.TaskSetID, "required_task_ids": p.RequiredTaskIDs,
		"max_age_seconds": p.MaxAgeSeconds, "require_real_target": p.RequireRealTarget,
	}
}

// versionDigestPattern is the canonical immutable version digest the Registry
// pins a version to. The verdict is bound to exactly this string, so a verdict
// can never be reused for a different digest.
var versionDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// TaskResult is the content-free outcome of one reference task inside a run. It
// carries identifiers and numbers only: which task, whether it passed, and what
// it cost. There is no field for the task's input, the agent's answer, a trace
// body or a tool output -- those are user content and the plane forbids them.
// Terminal states are the durable outcome of one task as far as the plane can
// evidence it. They are tracked apart from transport errors on purpose:
// DEF-PROF-1 showed that "0 transport errors" must never be readable as
// "0 failed generations", and neither count may be a silent placeholder.
const (
	TerminalCompleted = "completed"
	TerminalFailed    = "failed"
	TerminalBlocked   = "blocked"
	TerminalUnknown   = "unknown"
)

type TaskResult struct {
	TaskID string `json:"task_id"`
	Passed bool   `json:"passed"`
	// TransportError states the exchange with the target broke at the wire
	// level (credential, HTTP, protocol). It is separate from TerminalState:
	// a broken exchange proves nothing about the terminal outcome, and a
	// clean exchange can still end in a failed task.
	TransportError bool `json:"transport_error"`
	// TerminalState is completed|failed|blocked|unknown -- never empty in
	// stored evidence; absence of evidence is the explicit "unknown".
	TerminalState string             `json:"terminal_state"`
	TokensIn      int64              `json:"tokens_in"`
	TokensOut     int64              `json:"tokens_out"`
	Cost          float64            `json:"cost"`
	LatencyMS     int64              `json:"latency_ms"`
	Metrics       map[string]float64 `json:"metrics,omitempty"`
}

// Verdict is one evaluation run bound to one version digest. TaskSetID names the
// reference set and SetupDigest is a content digest of that set and its setup --
// a pointer that lets a reviewer reproduce the run, never a copy of it.
type Verdict struct {
	ID          string       `json:"id"`
	AgentDigest string       `json:"agent_digest"`
	TaskSetID   string       `json:"task_set_id"`
	SetupDigest string       `json:"setup_digest"`
	Outcome     Outcome      `json:"outcome"`
	Tasks       []TaskResult `json:"tasks"`
	TotalCost   float64      `json:"total_cost"`
	CreatedAt   time.Time    `json:"created_at"`
	// PolicyID/PolicyDigest bind the run to the policy it was earned under; the
	// gate compares them with the CURRENT policy, so a verdict earned under a
	// superseded policy can never satisfy publication or promotion (row 24:
	// "устаревший policy" denies). TargetKind states the run's provenance.
	PolicyID     string     `json:"policy_id"`
	PolicyDigest string     `json:"policy_digest"`
	TargetKind   TargetKind `json:"target_kind"`
}

// RecordInput is what a caller submits after running the reference tasks. The
// setup is provided as any value so the plane can digest it; its bytes are never
// stored.
type RecordInput struct {
	AgentDigest string
	TaskSetID   string
	Setup       any
	Outcome     Outcome
	Tasks       []TaskResult
	// PolicyID names the policy the run was performed under. It is mandatory:
	// a verdict that cannot say which policy it satisfies certifies nothing,
	// and silently policy-less verdicts would let staleness pass unseen.
	PolicyID string
	// TargetKind is mandatory provenance, never inferred from trust: "real"
	// for an actual deployment, "fixture" for a local test endpoint.
	TargetKind TargetKind
}

// Store is the verdict owner. Safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

// New builds an evals store on an existing pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const evalsSchemaDDL = `
CREATE SCHEMA IF NOT EXISTS ahp_evals;

CREATE TABLE IF NOT EXISTS ahp_evals.verdicts (
    id            text PRIMARY KEY,
    agent_digest  text NOT NULL,
    task_set_id   text NOT NULL,
    setup_digest  text NOT NULL,
    outcome       text NOT NULL,
    total_cost    double precision NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT verdicts_digest_format CHECK (agent_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT verdicts_outcome_check CHECK (outcome IN ('pass', 'fail')),
    CONSTRAINT verdicts_task_set_nonempty CHECK (task_set_id <> '')
);
CREATE INDEX IF NOT EXISTS verdicts_digest_outcome_idx
    ON ahp_evals.verdicts (agent_digest, outcome, created_at DESC);

CREATE TABLE IF NOT EXISTS ahp_evals.task_results (
    id          text PRIMARY KEY,
    verdict_id  text NOT NULL REFERENCES ahp_evals.verdicts (id) ON DELETE CASCADE,
    task_id     text NOT NULL,
    passed      boolean NOT NULL,
    tokens_in   bigint  NOT NULL DEFAULT 0,
    tokens_out  bigint  NOT NULL DEFAULT 0,
    cost        double precision NOT NULL DEFAULT 0,
    latency_ms  bigint  NOT NULL DEFAULT 0,
    metrics     jsonb   NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS task_results_verdict_idx
    ON ahp_evals.task_results (verdict_id);

CREATE TABLE IF NOT EXISTS ahp_evals.policies (
    id                  text PRIMARY KEY,
    digest              text NOT NULL,
    task_set_id         text NOT NULL,
    required_tasks      text[] NOT NULL DEFAULT '{}',
    max_age_seconds     bigint NOT NULL CHECK (max_age_seconds > 0),
    require_real_target boolean NOT NULL DEFAULT false,
    created_at          timestamptz NOT NULL DEFAULT now(),
    declared_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT policies_task_set_nonempty CHECK (task_set_id <> '')
);

ALTER TABLE ahp_evals.verdicts ADD COLUMN IF NOT EXISTS policy_id     text NOT NULL DEFAULT '';
ALTER TABLE ahp_evals.verdicts ADD COLUMN IF NOT EXISTS policy_digest text NOT NULL DEFAULT '';
ALTER TABLE ahp_evals.verdicts ADD COLUMN IF NOT EXISTS target_kind   text NOT NULL DEFAULT '';
ALTER TABLE ahp_evals.task_results ADD COLUMN IF NOT EXISTS transport_error boolean NOT NULL DEFAULT false;
ALTER TABLE ahp_evals.task_results ADD COLUMN IF NOT EXISTS terminal_state  text NOT NULL DEFAULT 'unknown';
`

// Migrate creates the schema idempotently. The plane's DDL is serialized by a
// transaction-scoped advisory lock: several processes may mount the plane at
// once (cmd/ahp, test fixtures), and concurrent CREATE/ALTER interleaving in
// PostgreSQL can deadlock -- a mount race must never decide whether the
// verdict plane exists.
func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return contracts.NewError(contracts.ErrInvalid, "evals store has no connection pool")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return storageError("begin evals migrate", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('ahp_evals_migrate'))`); err != nil {
		return storageError("evals migrate lock", err)
	}
	if _, err := tx.Exec(ctx, evalsSchemaDDL); err != nil {
		return storageError("create evals schema", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return storageError("commit evals migrate", err)
	}
	return nil
}

// Record validates and persists one verdict. It is the only writer of verdict
// rows. A setup is reduced to a digest before it touches the database, so no
// bytes of the reference set are ever stored.
func (s *Store) Record(ctx context.Context, in RecordInput) (Verdict, error) {
	if !versionDigestPattern.MatchString(in.AgentDigest) {
		return Verdict{}, contracts.NewError(contracts.ErrInvalid, "evals: agent digest must be sha256:<64 lowercase hex>")
	}
	if trim(in.TaskSetID) == "" {
		return Verdict{}, contracts.NewError(contracts.ErrInvalid, "evals: task set id must not be empty")
	}
	if !in.Outcome.valid() {
		return Verdict{}, contracts.NewError(contracts.ErrInvalid, "evals: outcome must be pass or fail")
	}
	if !in.TargetKind.valid() {
		return Verdict{}, contracts.NewError(contracts.ErrInvalid,
			"evals: target kind must be real or fixture -- verdict provenance is never implicit")
	}
	if trim(in.PolicyID) == "" {
		return Verdict{}, contracts.NewError(contracts.ErrInvalid,
			"evals: a verdict must name the policy it was earned under")
	}
	policy, perr := s.getPolicy(ctx, trim(in.PolicyID))
	if perr != nil {
		return Verdict{}, perr
	}
	if in.Outcome == OutcomePass && trim(in.TaskSetID) != policy.TaskSetID {
		// A PASS must be earned on the reference set the policy mandates. A FAIL
		// records from any set: late failure evidence from a different probe is
		// exactly what a "superseded verdict" looks like, and the plane must be
		// able to hear it.
		return Verdict{}, contracts.NewError(contracts.ErrInvalid,
			"evals: passing run targets a reference set other than the policy's")
	}
	if len(in.Tasks) == 0 {
		return Verdict{}, contracts.NewError(contracts.ErrInvalid, "evals: at least one task result required")
	}
	// Own the slice before canonicalizing: an empty terminal state is stored
	// as the explicit "unknown", never as a blank that reads as "not measured".
	in.Tasks = append([]TaskResult(nil), in.Tasks...)
	for i := range in.Tasks {
		if in.Tasks[i].TerminalState == "" {
			in.Tasks[i].TerminalState = TerminalUnknown
		}
	}
	allSettled := true
	seen := map[string]bool{}
	var total float64
	for _, tr := range in.Tasks {
		if trim(tr.TaskID) == "" {
			return Verdict{}, contracts.NewError(contracts.ErrInvalid, "evals: a task result must name its task")
		}
		if seen[tr.TaskID] {
			return Verdict{}, contracts.NewError(contracts.ErrConflict, "evals: duplicate task id in one verdict: "+tr.TaskID)
		}
		seen[tr.TaskID] = true
		if tr.TokensIn < 0 || tr.TokensOut < 0 || tr.LatencyMS < 0 {
			return Verdict{}, contracts.NewError(contracts.ErrInvalid, "evals: cost counters must not be negative")
		}
		if tr.Cost < 0 || math.IsNaN(tr.Cost) || math.IsInf(tr.Cost, 0) {
			return Verdict{}, contracts.NewError(contracts.ErrInvalid, "evals: invalid cost")
		}
		switch tr.TerminalState {
		case TerminalCompleted, TerminalFailed, TerminalBlocked, TerminalUnknown:
		default:
			return Verdict{}, contracts.NewError(contracts.ErrInvalid,
				"evals: terminal state must be completed, failed, blocked or unknown")
		}
		if tr.TransportError && tr.TerminalState == TerminalCompleted {
			return Verdict{}, contracts.NewError(contracts.ErrInvalid,
				"evals: a task cannot be completed through a broken transport")
		}
		if tr.Passed && (tr.TransportError || tr.TerminalState != TerminalCompleted) {
			return Verdict{}, contracts.NewError(contracts.ErrInvalid,
				"evals: a passed task requires a completed terminal state and an intact exchange")
		}
		allSettled = allSettled && tr.Passed && !tr.TransportError && tr.TerminalState == TerminalCompleted
		total += tr.Cost
	}
	if (in.Outcome == OutcomePass) != allSettled {
		// A pass requires the full honest shape -- matching content AND a
		// completed terminal state AND an intact exchange. Transport evidence
		// is part of the outcome, not an annotation (row 24 / DEF-PROF-1).
		return Verdict{}, contracts.NewError(contracts.ErrInvalid, "evals: outcome disagrees with task results")
	}
	if in.Outcome == OutcomePass {
		// A pass must cover every task the policy demands: a partial run cannot
		// record itself into a pass ("отсутствующий/частичный прогон" denies).
		// Failing runs always record -- partial failure is honest evidence.
		for _, want := range policy.RequiredTaskIDs {
			if !seen[want] {
				return Verdict{}, contracts.NewError(contracts.ErrInvalid,
					"evals: passing run is missing a policy-required task: "+want)
			}
		}
	}
	setupDigest, err := contracts.Digest(in.Setup)
	if err != nil {
		return Verdict{}, contracts.NewError(contracts.ErrInvalid, "evals: setup cannot be digested: "+err.Error())
	}

	v := Verdict{
		ID:           contracts.NewID(),
		AgentDigest:  in.AgentDigest,
		TaskSetID:    in.TaskSetID,
		SetupDigest:  setupDigest,
		Outcome:      in.Outcome,
		Tasks:        append([]TaskResult(nil), in.Tasks...),
		TotalCost:    total,
		PolicyID:     policy.ID,
		PolicyDigest: policy.Digest,
		TargetKind:   in.TargetKind,
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Verdict{}, storageError("begin evals record", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var createdAt time.Time
	if err := tx.QueryRow(ctx, `
INSERT INTO ahp_evals.verdicts (id, agent_digest, task_set_id, setup_digest, outcome, total_cost, policy_id, policy_digest, target_kind)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING created_at`,
		v.ID, v.AgentDigest, v.TaskSetID, v.SetupDigest, string(v.Outcome), v.TotalCost,
		v.PolicyID, v.PolicyDigest, string(v.TargetKind)).Scan(&createdAt); err != nil {
		return Verdict{}, storageError("record verdict", err)
	}
	v.CreatedAt = createdAt.UTC()

	for _, tr := range v.Tasks {
		metrics, err := marshalMetrics(tr.Metrics)
		if err != nil {
			return Verdict{}, err
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO ahp_evals.task_results (id, verdict_id, task_id, passed, tokens_in, tokens_out, cost, latency_ms, metrics, transport_error, terminal_state)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)`,
			contracts.NewID(), v.ID, tr.TaskID, tr.Passed, tr.TokensIn, tr.TokensOut, tr.Cost, tr.LatencyMS, metrics, tr.TransportError, tr.TerminalState); err != nil {
			return Verdict{}, storageError("record task result", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Verdict{}, storageError("commit evals record", err)
	}
	return v, nil
}

// LatestVerdict returns the most recent verdict recorded for a version digest,
// regardless of outcome. It reports contracts.ErrNotFound when none exists.
func (s *Store) LatestVerdict(ctx context.Context, digest string) (Verdict, error) {
	if !versionDigestPattern.MatchString(digest) {
		return Verdict{}, contracts.NewError(contracts.ErrInvalid, "evals: agent digest must be sha256:<64 lowercase hex>")
	}
	var (
		v          Verdict
		outcome    string
		created    time.Time
		targetKind string
	)
	err := s.pool.QueryRow(ctx, `
SELECT id, agent_digest, task_set_id, setup_digest, outcome, total_cost, created_at, policy_id, policy_digest, target_kind
FROM ahp_evals.verdicts WHERE agent_digest = $1
ORDER BY created_at DESC, id DESC LIMIT 1`, digest).
		Scan(&v.ID, &v.AgentDigest, &v.TaskSetID, &v.SetupDigest, &outcome, &v.TotalCost, &created,
			&v.PolicyID, &v.PolicyDigest, &targetKind)
	if errors.Is(err, pgx.ErrNoRows) {
		return Verdict{}, contracts.NewError(contracts.ErrNotFound, "evals: no verdict recorded for this version digest")
	}
	if err != nil {
		return Verdict{}, storageError("load verdict", err)
	}
	v.Outcome = Outcome(outcome)
	v.TargetKind = TargetKind(targetKind)
	v.CreatedAt = created.UTC()
	tasks, err := s.loadTasks(ctx, v.ID)
	if err != nil {
		return Verdict{}, err
	}
	v.Tasks = tasks
	return v, nil
}

// HasPassingVerdict reports whether a passing verdict exists for exactly this
// version digest. It never errors on "not found"; it returns false.
func (s *Store) HasPassingVerdict(ctx context.Context, digest string) (bool, error) {
	if !versionDigestPattern.MatchString(digest) {
		return false, contracts.NewError(contracts.ErrInvalid, "evals: agent digest must be sha256:<64 lowercase hex>")
	}
	var found bool
	err := s.pool.QueryRow(ctx, `
SELECT COALESCE((SELECT outcome = 'pass' FROM ahp_evals.verdicts WHERE agent_digest = $1 ORDER BY created_at DESC, id DESC LIMIT 1), false)`, digest).Scan(&found)
	if err != nil {
		return false, storageError("check verdict", err)
	}
	return found, nil
}

// Freshness is the outcome of measuring the latest verdict for one digest
// against the CURRENT policy: not just "a pass existed", but "this artifact was
// freshly evaluated on the reference set the operator demands, under the policy
// that is in force, on a target kind the policy accepts".
type Freshness struct {
	Fresh  bool   `json:"fresh"`
	Reason string `json:"reason,omitempty"`
}

// Freshness evaluates the publication/promotion precondition for one version
// digest against the current policy. Every denial reason is explicit so an
// operator can tell "never evaluated" from "evaluated under a superseded
// policy" from "ran on a fixture when the policy demands a real target".
func (s *Store) Freshness(ctx context.Context, digest string) (Freshness, error) {
	if !versionDigestPattern.MatchString(digest) {
		return Freshness{}, contracts.NewError(contracts.ErrInvalid, "evals: agent digest must be sha256:<64 lowercase hex>")
	}
	cur, err := s.CurrentPolicy(ctx)
	if contracts.Code(err) == contracts.ErrNotFound {
		return Freshness{Reason: "no evaluation policy is declared; the plane stays closed (fail closed)"}, nil
	}
	if err != nil {
		return Freshness{}, err
	}
	v, err := s.LatestVerdict(ctx, digest)
	if contracts.Code(err) == contracts.ErrNotFound {
		return Freshness{Reason: "no evaluation verdict exists for this version digest"}, nil
	}
	if err != nil {
		return Freshness{}, err
	}
	if v.Outcome != OutcomePass {
		return Freshness{Reason: "the newest verdict for this version digest fails"}, nil
	}
	if v.PolicyID != cur.ID || v.PolicyDigest != cur.Digest {
		return Freshness{Reason: "the verdict was earned under a superseded evaluation policy; re-run the evaluation under the current policy"}, nil
	}
	if age := time.Since(v.CreatedAt); age > time.Duration(cur.MaxAgeSeconds)*time.Second {
		return Freshness{Reason: "the verdict is older than the policy's freshness window; re-run the evaluation"}, nil
	}
	if cur.RequireRealTarget && v.TargetKind != TargetReal {
		return Freshness{Reason: "the policy requires verdicts from a real target; this verdict was earned against a fixture target"}, nil
	}
	return Freshness{Fresh: true}, nil
}

// RequirePassedVerdict is the publication and promotion gate. It returns nil
// only when the CURRENT policy is satisfied by a fresh, passing, correctly
// targeted verdict for exactly this digest; otherwise it refuses with CONFLICT.
// Because the lookup is keyed on the digest, a verdict recorded for one version
// can never satisfy another ("target revision"); because it is measured against
// the stored current policy, mutating any request field cannot substitute for a
// fresh run: the gate re-derives everything from the digest at decision time.
func (s *Store) RequirePassedVerdict(ctx context.Context, digest string) error {
	f, err := s.Freshness(ctx, digest)
	if err != nil {
		return err
	}
	if !f.Fresh {
		return contracts.NewError(contracts.ErrConflict, "evals: "+f.Reason)
	}
	return nil
}

// SetPolicy declares one evaluation policy, or re-declares an identical one
// (idempotent, refreshing its "current" position). Changing the content under a
// used id is refused: history that verdicts were earned under is immutable.
func (s *Store) SetPolicy(ctx context.Context, p Policy) (Policy, error) {
	p.ID = trim(p.ID)
	p.TaskSetID = trim(p.TaskSetID)
	if p.ID == "" {
		return Policy{}, contracts.NewError(contracts.ErrInvalid, "evals: policy id must not be empty")
	}
	if p.TaskSetID == "" {
		return Policy{}, contracts.NewError(contracts.ErrInvalid, "evals: policy must name its reference task set")
	}
	if p.MaxAgeSeconds <= 0 {
		return Policy{}, contracts.NewError(contracts.ErrInvalid, "evals: policy freshness window must be positive")
	}
	seen := map[string]bool{}
	required := make([]string, 0, len(p.RequiredTaskIDs))
	for _, t := range p.RequiredTaskIDs {
		t = trim(t)
		if t == "" {
			return Policy{}, contracts.NewError(contracts.ErrInvalid, "evals: a policy required task id must not be empty")
		}
		if seen[t] {
			return Policy{}, contracts.NewError(contracts.ErrInvalid, "evals: duplicate policy required task id: "+t)
		}
		seen[t] = true
		required = append(required, t)
	}
	p.RequiredTaskIDs = required
	digest, err := contracts.Digest(p.content())
	if err != nil {
		return Policy{}, contracts.NewError(contracts.ErrInvalid, "evals: policy content cannot be canonicalised: "+err.Error())
	}
	tag, err := s.pool.Exec(ctx, `
INSERT INTO ahp_evals.policies (id, digest, task_set_id, required_tasks, max_age_seconds, require_real_target)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO UPDATE SET declared_at = now() WHERE policies.digest = $2`,
		p.ID, digest, p.TaskSetID, required, p.MaxAgeSeconds, p.RequireRealTarget)
	if err != nil {
		return Policy{}, storageError("declare policy", err)
	}
	if tag.RowsAffected() == 0 {
		return Policy{}, contracts.NewError(contracts.ErrConflict,
			"evals: policy id "+p.ID+" is already registered with different content; declare a new policy id")
	}
	stored, err := s.getPolicy(ctx, p.ID)
	if err != nil {
		return Policy{}, err
	}
	return stored, nil
}

// CurrentPolicy returns the most recently declared policy. It reports
// contracts.ErrNotFound when the plane has none -- which the gate reads as
// "stay closed", never as "no requirement".
func (s *Store) CurrentPolicy(ctx context.Context) (Policy, error) {
	return s.scanPolicy(s.pool.QueryRow(ctx, `
SELECT id, digest, task_set_id, required_tasks, max_age_seconds, require_real_target, created_at, declared_at
FROM ahp_evals.policies ORDER BY declared_at DESC, id DESC LIMIT 1`))
}

func (s *Store) getPolicy(ctx context.Context, id string) (Policy, error) {
	p, err := s.scanPolicy(s.pool.QueryRow(ctx, `
SELECT id, digest, task_set_id, required_tasks, max_age_seconds, require_real_target, created_at, declared_at
FROM ahp_evals.policies WHERE id = $1`, id))
	if contracts.Code(err) == contracts.ErrNotFound {
		return Policy{}, contracts.NewError(contracts.ErrInvalid, "evals: unknown policy: verdicts are recorded only under a declared policy ("+id+")")
	}
	return p, err
}

func (s *Store) scanPolicy(row pgx.Row) (Policy, error) {
	var p Policy
	err := row.Scan(&p.ID, &p.Digest, &p.TaskSetID, &p.RequiredTaskIDs,
		&p.MaxAgeSeconds, &p.RequireRealTarget, &p.CreatedAt, &p.DeclaredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Policy{}, contracts.NewError(contracts.ErrNotFound, "evals: policy not found")
	}
	if err != nil {
		return Policy{}, storageError("load policy", err)
	}
	p.CreatedAt = p.CreatedAt.UTC()
	p.DeclaredAt = p.DeclaredAt.UTC()
	return p, nil
}

func (s *Store) loadTasks(ctx context.Context, verdictID string) ([]TaskResult, error) {
	rows, err := s.pool.Query(ctx, `
SELECT task_id, passed, tokens_in, tokens_out, cost, latency_ms, metrics::text, transport_error, terminal_state
FROM ahp_evals.task_results WHERE verdict_id = $1 ORDER BY task_id`, verdictID)
	if err != nil {
		return nil, storageError("load task results", err)
	}
	defer rows.Close()
	var out []TaskResult
	for rows.Next() {
		var (
			tr      TaskResult
			metrics string
		)
		if err := rows.Scan(&tr.TaskID, &tr.Passed, &tr.TokensIn, &tr.TokensOut, &tr.Cost, &tr.LatencyMS, &metrics, &tr.TransportError, &tr.TerminalState); err != nil {
			return nil, storageError("scan task result", err)
		}
		if metrics != "" && metrics != "{}" {
			if err := json.Unmarshal([]byte(metrics), &tr.Metrics); err != nil {
				return nil, storageError("decode task metrics", err)
			}
		}
		out = append(out, tr)
	}
	return out, rows.Err()
}

func marshalMetrics(m map[string]float64) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	// Metrics are numeric-only by the map's type: no string value can enter, so
	// no user content can ride in through them.
	b, err := json.Marshal(m)
	if err != nil {
		return "", contracts.NewError(contracts.ErrInvalid, "evals: metrics cannot be encoded: "+err.Error())
	}
	return string(b), nil
}

func trim(s string) string { return strings.TrimSpace(s) }

// storageError maps driver errors onto the shared contract codes.
func storageError(op string, err error) error {
	if err == nil {
		return nil
	}
	if code := contracts.Code(err); code != "" {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.NewError(contracts.ErrNotFound, op+": not found")
	}
	return contracts.NewError(contracts.ErrConflict, op+": "+err.Error())
}
