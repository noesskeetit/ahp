package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// ErrApprovalRequired is returned by Call when a tool requires a human
// approval for the exact canonical digest of {tool, arguments} and none exists.
// It is a sentinel so callers can map it to an interactive approval prompt.
var ErrApprovalRequired = errors.New("tools: human approval required for this canonical action digest")

// ErrApprovalSpent is returned when a call that passed the (non-consuming)
// approval gate loses its approval at the atomic consume step: the approval was
// already consumed by an earlier effect, expired, or was un-bound concurrently.
// A side-effecting gated tool refuses at the consume step and NEVER runs the
// effect: a human signature authorises exactly one effect, at-most-once.
var ErrApprovalSpent = errors.New("tools: approval already consumed or no longer valid")

// ErrApprovalDeclined means a human refused THIS operation: the request bound
// to the exact (tenant, deployment, subject, operation key, digest, tool) was
// declined. It is distinct from ErrApprovalRequired on purpose -- "no signature
// yet" invites a human decision, "declined" reports one that was already made.
// The operation cannot be re-raised under the same key; a new intent needs a
// new operation key.
var ErrApprovalDeclined = errors.New("tools: approval was declined by the employee")

// defaultApprovalTTL bounds the lifetime of a pending human approval when the
// Service is constructed without Config.ApprovalTTL. Expiry is enforced at the
// gate and inside the atomic consume, never by a janitor.
const defaultApprovalTTL = 60 * time.Minute

// ApprovalDigest computes the canonical digest a human approval binds to:
// sha256 over {"tool": name, "arguments": decoded-args}. Argument key order and
// whitespace do not affect the digest because the payload is re-canonicalised
// through contracts.Digest.
func ApprovalDigest(tool string, arguments json.RawMessage) (string, error) {
	var decoded any
	if len(arguments) > 0 {
		if err := json.Unmarshal(arguments, &decoded); err != nil {
			return "", contracts.NewError(contracts.ErrInvalid, "tools: arguments must be valid JSON")
		}
	}
	return contracts.Digest(map[string]any{"tool": tool, "arguments": decoded})
}

// CreateApprovalRequest records (or refreshes) a pending approval request for
// the canonical digest of the proposed call and returns its request id and the
// digest a human must sign off on. It never mutates the effect ledger.
//
// The request is BOUND to the exact operation it was raised for: tenant,
// deployment, subject and the idempotency/operation key join the digest in the
// row's identity (root audit 2026-11-02, P1). A pending row created under one
// (deployment, subject, operation key) authorises no other operation: scope and
// key are match columns at the gate and inside the consume, never advisory.
// opKey may be empty only for out-of-band requests (direct service callers);
// such a row is never matched by a keyed Call's gate.
func (s *Service) CreateApprovalRequest(ctx context.Context, scope contracts.Scope, opKey, tool string, arguments json.RawMessage) (string, string, error) {
	if err := scope.Validate(); err != nil {
		return "", "", err
	}
	digest, err := ApprovalDigest(tool, arguments)
	if err != nil {
		return "", "", err
	}
	requestID := "apr_" + contracts.NewID()
	tx, err := s.tenantWrite(ctx, scope)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback(ctx)
	var effectiveID string
	q := fmt.Sprintf(`
		INSERT INTO %s.approvals
			(tenant, digest, request_id, tool, deployment, subject, op_key, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now() + $8::interval)
		ON CONFLICT (tenant, digest, tool, deployment, subject, op_key) DO UPDATE
		SET request_id = %s.approvals.request_id
		RETURNING request_id`, s.schema, s.schema)
	ttl := s.approvalTTL().String()
	if err := tx.QueryRow(ctx, q, scope.Tenant, digest, requestID, tool,
		scope.Deployment, scope.Subject, opKey, ttl).Scan(&effectiveID); err != nil {
		return "", "", fmt.Errorf("tools: create approval request: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", err
	}
	return effectiveID, digest, nil
}

// approvalTTL resolves the configured approval lifetime.
func (s *Service) approvalTTL() time.Duration {
	if s.cfg.ApprovalTTL > 0 {
		return s.cfg.ApprovalTTL
	}
	return defaultApprovalTTL
}

// HasApproval reports whether an ELIGIBLE approval exists for this exact
// operation: same tenant, deployment, subject, operation key, digest and tool,
// signed by a human (approved_by set), not yet consumed and not expired. It is
// the NON-consuming gate read; the durable authorisation is the atomic consume
// in TryConsumeApproval, which runs at the fresh-dispatch point so a signature
// can never fund a second effect (root audit 2026-11-02, P1).
func (s *Service) HasApproval(ctx context.Context, scope contracts.Scope, opKey, digest, tool string) (bool, error) {
	ok, _, err := s.approvalGate(ctx, scope, opKey, digest, tool)
	return ok, err
}

// approvalGate is HasApproval with a refusal reason: eligible reports a signed,
// unspent, unexpired row bound to this exact operation; reason explains why an
// approval is not eligible ("expired", "consumed", "unapproved", "missing").
func (s *Service) approvalGate(ctx context.Context, scope contracts.Scope, opKey, digest, tool string) (bool, string, error) {
	var state string
	q := fmt.Sprintf(`
		SELECT CASE
			WHEN declined_by IS NOT NULL          THEN 'declined'
			WHEN approved_by IS NULL             THEN 'unapproved'
			WHEN consumed_at IS NOT NULL          THEN 'consumed'
			WHEN expires_at IS NULL OR expires_at <= now()              THEN 'expired'
			ELSE 'eligible'
		END
		FROM %s.approvals
		WHERE tenant = $1 AND digest = $2 AND tool = $3
		  AND deployment = $4 AND subject = $5 AND op_key = $6`, s.schema)
	err := s.pool.QueryRow(ctx, q, scope.Tenant, digest, tool,
		scope.Deployment, scope.Subject, opKey).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "missing", nil
	}
	if err != nil {
		return false, "", fmt.Errorf("tools: approval gate: %w", err)
	}
	return state == "eligible", state, nil
}

// TryConsumeApproval atomically spends the approval bound to this exact
// operation (tenant, deployment, subject, operation key, digest, tool). The
// conditional UPDATE is the durable authorisation: exactly one concurrent
// winner sees a row, every other attempt (replay, race, expiry race) gets
// ErrApprovalSpent with a reason naming what happened. A crash between consume
// and dispatch loses the approval by design: a human signature funds at most
// ONE effect, never a retry.
func (s *Service) TryConsumeApproval(ctx context.Context, scope contracts.Scope, opKey, digest, tool string) error {
	tx, err := s.tenantWrite(ctx, scope)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var requestID string
	q := fmt.Sprintf(`
		UPDATE %s.approvals
		SET consumed_at = now()
		WHERE tenant = $1 AND digest = $2 AND tool = $3
		  AND deployment = $4 AND subject = $5 AND op_key = $6
		  AND approved_by IS NOT NULL
		  AND declined_by IS NULL
		  AND consumed_at IS NULL
		  AND expires_at > now()
		RETURNING request_id`, s.schema)
	err = tx.QueryRow(ctx, q, scope.Tenant, digest, tool,
		scope.Deployment, scope.Subject, opKey).Scan(&requestID)
	if errors.Is(err, pgx.ErrNoRows) {
		_, reason, gerr := s.approvalGate(ctx, scope, opKey, digest, tool)
		if gerr != nil {
			return gerr
		}
		return fmt.Errorf("%w (reason: %s)", ErrApprovalSpent, reason)
	}
	if err != nil {
		return fmt.Errorf("tools: consume approval: %w", err)
	}
	return tx.Commit(ctx)
}

// Approve is the legacy digest selector. It may authorize exactly one pending
// request belonging to the human, and refuses ambiguous worklists.
func (s *Service) Approve(ctx context.Context, scope contracts.Scope, digest, tool, humanToken string) (string, error) {
	return s.ApproveRequest(ctx, scope, "", digest, tool, humanToken)
}

// ApproveRequest signs one immutable request, bound to its owner and operation.
// A different human requires an explicit assignment checked by ApprovalAuthorize.
func (s *Service) ApproveRequest(ctx context.Context, scope contracts.Scope, requestID, digest, tool, humanToken string) (string, error) {
	if s.verifier == nil {
		return "", contracts.NewError(contracts.ErrInvalid, "tools: approval verifier not configured")
	}
	if !isDigest(digest) {
		return "", contracts.NewError(contracts.ErrInvalid, "tools: approval digest malformed")
	}
	claims, err := s.verifyHumanBearer(humanToken)
	if err != nil {
		return "", err
	}
	if claims.Tenant != scope.Tenant || claims.Subject == "" {
		return "", contracts.NewError(contracts.ErrForbidden, "tools: approver scope mismatch")
	}
	tx, err := s.tenantWrite(ctx, scope)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	// The assigned human can differ from the request owner. Drain both kinds
	// of writes before erasure so a late approval cannot recreate attribution.
	if err = s.ledger.CheckSubjectWritable(ctx, tx, scope.Tenant, claims.Subject); err != nil {
		return "", err
	}
	q := fmt.Sprintf(`SELECT request_id, deployment, subject FROM %s.approvals
 WHERE tenant=$1 AND digest=$2 AND tool=$3 AND approved_by IS NULL AND declined_by IS NULL
 AND consumed_at IS NULL AND expires_at > now()
 AND (($4='' AND subject=$5) OR ($4<>'' AND request_id=$4))
 ORDER BY request_id`, s.schema)
	rows, err := tx.Query(ctx, q, scope.Tenant, digest, tool, requestID, claims.Subject)
	if err != nil {
		return "", err
	}
	type pending struct{ id, deployment, subject string }
	var matches []pending
	for rows.Next() {
		var p pending
		if err = rows.Scan(&p.id, &p.deployment, &p.subject); err != nil {
			rows.Close()
			return "", err
		}
		matches = append(matches, p)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", s.noPendingApprovalError(ctx, tx, scope, requestID, digest, tool, claims.Subject, false)
	}
	if len(matches) != 1 {
		return "", contracts.NewError(contracts.ErrConflict, "tools: request_id required for ambiguous approvals")
	}
	chosen := matches[0]
	owner := contracts.Scope{Tenant: scope.Tenant, Deployment: chosen.deployment, Subject: chosen.subject}
	// Acquire the owner barrier before a row lock: deletion takes the same
	// barrier before purging rows. The guarded UPDATE below serializes signing.
	if err = s.ledger.CheckSubjectWritable(ctx, tx, owner.Tenant, owner.Subject); err != nil {
		return "", err
	}
	if chosen.subject != claims.Subject {
		if s.cfg.ApprovalAuthorize == nil {
			return "", contracts.NewError(contracts.ErrForbidden, "tools: human is not the request owner")
		}
		if err = s.cfg.ApprovalAuthorize(ctx, owner, claims.Subject); err != nil {
			return "", err
		}
	}
	q = fmt.Sprintf(`UPDATE %s.approvals SET approved_by=$1, approved_at=now()
 WHERE tenant=$2 AND request_id=$3 AND approved_by IS NULL AND consumed_at IS NULL AND expires_at>now()`, s.schema)
	tag, err := tx.Exec(ctx, q, claims.Subject, scope.Tenant, chosen.id)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() != 1 {
		return "", contracts.NewError(contracts.ErrConflict, "tools: approval expired while signing")
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return claims.Subject, nil
}

// noPendingApprovalError explains an empty pending selection for one intent.
//
// The two answers are deliberately different and both are contracts:
//   - A request the CALLER already DECLINED is a CONFLICT for either intent. An
//     employee refusal is an answer: signing something they refused (or refusing
//     it twice) contradicts a decision on record, and GAP-DOTE2E-7 asks for that
//     to be visible rather than looking like a missing approval.
//   - A row that is already SIGNED or SPENT stays NOT_FOUND for an approve
//     ("nothing of mine is pending here"), which is the accepted exactly-once
//     contract asserted by TestApprovalLegitSingleFlowStaysGreen and
//     TestHumanBrowserApprove; only a DECLINE against it reports CONFLICT, since
//     refusing a signature that was already given is a contradiction. The way to
//     stop an approved-but-not-yet-run effect is Fence, not Decline.
//
// The legacy digest selector (empty requestID) always keeps the neutral answer,
// and foreign or expired rows never change shape, so nobody can probe other
// subjects' approvals.
func (s *Service) noPendingApprovalError(ctx context.Context, tx pgx.Tx, scope contracts.Scope, requestID, digest, tool, caller string, declining bool) error {
	const neutral = "tools: no pending approval for that request"
	if requestID == "" {
		return contracts.NewError(contracts.ErrNotFound, neutral)
	}
	var (
		declinedBy, approvedBy string
		consumed               bool
	)
	q := fmt.Sprintf(`SELECT COALESCE(declined_by,''), COALESCE(approved_by,''), consumed_at IS NOT NULL
 FROM %s.approvals
 WHERE tenant=$1 AND digest=$2 AND tool=$3 AND subject=$4 AND request_id=$5`, s.schema)
	err := tx.QueryRow(ctx, q, scope.Tenant, digest, tool, caller, requestID).Scan(&declinedBy, &approvedBy, &consumed)
	switch {
	case err != nil:
		return contracts.NewError(contracts.ErrNotFound, neutral)
	case declinedBy != "":
		return contracts.NewError(contracts.ErrConflict,
			"tools: this approval request was already declined by "+declinedBy)
	case (approvedBy != "" || consumed) && declining:
		return contracts.NewError(contracts.ErrConflict,
			"tools: this approval request was already answered"+
				" by "+approvedBy+" -- a signed effect is stopped with Fence, not Decline")
	default:
		return contracts.NewError(contracts.ErrNotFound, neutral)
	}
}

// DeclineRequest records a HUMAN refusal of one pending approval request. It is
// the mirror of ApproveRequest and keeps the same invariants: the bearer must be
// a verified human credential of this tenant; the request must be pending,
// unspent and unexpired; only the request owner may decline it, or a human the
// owner assigned through ApprovalAuthorize; and the write is a single guarded
// UPDATE, so decline and approve cannot both land on one row.
//
// A decline is terminal for that operation key. The gate reports 'declined'
// before 'unapproved', the consume refuses the row, the worklist stops listing
// it, and a later Call under the same key is refused with ErrApprovalDeclined
// instead of silently raising a fresh request. The refusal is journalled twice:
// on the approvals row (declined_by/declined_at) and as a Guardian audit row
// with class 'human-decline' -- identifiers and a digest only, never arguments.
func (s *Service) DeclineRequest(ctx context.Context, scope contracts.Scope, requestID, digest, tool, humanToken string) (string, error) {
	if s.verifier == nil {
		return "", contracts.NewError(contracts.ErrInvalid, "tools: approval verifier not configured")
	}
	if !isDigest(digest) {
		return "", contracts.NewError(contracts.ErrInvalid, "tools: approval digest malformed")
	}
	claims, err := s.verifyHumanBearer(humanToken)
	if err != nil {
		return "", err
	}
	if claims.Tenant != scope.Tenant || claims.Subject == "" {
		return "", contracts.NewError(contracts.ErrForbidden, "tools: approver scope mismatch")
	}
	tx, err := s.tenantWrite(ctx, scope)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if err = s.ledger.CheckSubjectWritable(ctx, tx, scope.Tenant, claims.Subject); err != nil {
		return "", err
	}
	q := fmt.Sprintf(`SELECT request_id, deployment, subject FROM %s.approvals
 WHERE tenant=$1 AND digest=$2 AND tool=$3 AND approved_by IS NULL AND declined_by IS NULL
 AND consumed_at IS NULL AND expires_at > now()
 AND (($4='' AND subject=$5) OR ($4<>'' AND request_id=$4))
 ORDER BY request_id`, s.schema)
	rows, err := tx.Query(ctx, q, scope.Tenant, digest, tool, requestID, claims.Subject)
	if err != nil {
		return "", err
	}
	type pending struct{ id, deployment, subject string }
	var matches []pending
	for rows.Next() {
		var p pending
		if err = rows.Scan(&p.id, &p.deployment, &p.subject); err != nil {
			rows.Close()
			return "", err
		}
		matches = append(matches, p)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return "", s.noPendingApprovalError(ctx, tx, scope, requestID, digest, tool, claims.Subject, true)
	}
	if len(matches) != 1 {
		return "", contracts.NewError(contracts.ErrConflict, "tools: request_id required for ambiguous approvals")
	}
	chosen := matches[0]
	owner := contracts.Scope{Tenant: scope.Tenant, Deployment: chosen.deployment, Subject: chosen.subject}
	if err = s.ledger.CheckSubjectWritable(ctx, tx, owner.Tenant, owner.Subject); err != nil {
		return "", err
	}
	if chosen.subject != claims.Subject {
		if s.cfg.ApprovalAuthorize == nil {
			return "", contracts.NewError(contracts.ErrForbidden, "tools: human is not the request owner")
		}
		if err = s.cfg.ApprovalAuthorize(ctx, owner, claims.Subject); err != nil {
			return "", err
		}
	}
	q = fmt.Sprintf(`UPDATE %s.approvals SET declined_by=$1, declined_at=now()
 WHERE tenant=$2 AND request_id=$3 AND approved_by IS NULL AND declined_by IS NULL
 AND consumed_at IS NULL AND expires_at>now()`, s.schema)
	tag, err := tx.Exec(ctx, q, claims.Subject, scope.Tenant, chosen.id)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() != 1 {
		return "", contracts.NewError(contracts.ErrConflict, "tools: approval stopped being declinable concurrently")
	}
	// Audit row for the human refusal (content-free: identifiers + digest).
	if _, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.guardian_log
	 (tenant, deployment, subject, tool, side_effecting, digest, decision, reason, class)
	 VALUES ($1,$2,$3,$4,$5,$6,'deny','declined by the employee','human-decline')`, s.schema),
		scope.Tenant, chosen.deployment, claims.Subject, tool, false, digest); err != nil {
		return "", fmt.Errorf("tools: journal decline: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return claims.Subject, nil
}

// verifyHumanBearer re-verifies a presented bearer as a human credential, for
// the plane audience first and (when configured) the browser user audience
// second. Never returns a non-human claim: KindHuman is required on each try.
func (s *Service) verifyHumanBearer(token string) (auth.Claims, error) {
	claims, err := s.verifier.Verify(token, s.audience, auth.KindHuman, "")
	if err == nil {
		return claims, nil
	}
	if s.cfg.UserAudience == "" || s.cfg.UserAudience == s.audience {
		return auth.Claims{}, err
	}
	return s.verifier.Verify(token, s.cfg.UserAudience, auth.KindHuman, "")
}

// PendingApproval is the content-free projection of an approval request row:
// identifiers, the tool alias and a timestamp. It never carries tool arguments
// or any user content.
type PendingApproval struct {
	RequestID    string    `json:"request_id"`
	Digest       string    `json:"digest"`
	Tool         string    `json:"tool"`
	CreatedAt    time.Time `json:"created_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	OperationKey string    `json:"operation_key"`
	Deployment   string    `json:"deployment"`
}

// ListPendingApprovals returns the unbound approval requests recorded in this
// tenant -- the worklist a human approver signs. Tenant comes from the verified
// principal only; there is no cross-tenant read.
func (s *Service) ListPendingApprovals(ctx context.Context, scope contracts.Scope) ([]PendingApproval, error) {
	// The worklist is a TENANT-level human view: it keys on tenant and records
	// the human subject, so exactly those identifiers must be verified. The
	// task-style Scope.Validate also demands a deployment, which a browser
	// principal does not carry (Approve follows the same convention); demanding
	// it here would make the human worklist depend on an identifier the query
	// never uses. Values reach SQL only as bind parameters.
	if scope.Tenant == "" || scope.Subject == "" ||
		len(scope.Tenant) > 128 || len(scope.Subject) > 128 {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"tools: approvals worklist needs a verified tenant and subject")
	}
	q := fmt.Sprintf(`
		SELECT request_id, digest, tool, created_at, expires_at, op_key, deployment
		FROM %s.approvals
		WHERE tenant = $1 AND subject = $2 AND approved_by IS NULL AND declined_by IS NULL AND consumed_at IS NULL AND expires_at > now()
		ORDER BY created_at, digest`, s.schema)
	rows, err := s.pool.Query(ctx, q, scope.Tenant, scope.Subject)
	if err != nil {
		return nil, fmt.Errorf("tools: list pending approvals: %w", err)
	}
	defer rows.Close()
	out := []PendingApproval{}
	for rows.Next() {
		var p PendingApproval
		if err := rows.Scan(&p.RequestID, &p.Digest, &p.Tool, &p.CreatedAt, &p.ExpiresAt, &p.OperationKey, &p.Deployment); err != nil {
			return nil, fmt.Errorf("tools: scan pending approvals: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
