package emil

// coordinator_bridge.go — T1b (director-endorsed plan, gap-doc 3200fd2 Table A).
//
// StateRunStore (T1) covers the per-run durability primitives the Emil reference
// Core Agent kept in its private PostgreSQL run-store: lease/fence, stream events,
// inbox, checkpoints. This bridge covers the turn-LIFECYCLE operations that the
// Emil workflow.py performs through that same run-store but that AHP owns in its
// RunCoordinator: HITL approval gates, at-least-once outbox emission, steering,
// crash recovery and turn history.
//
// It forwards to the authoritative *space.Coordinator and hides the internal
// request-struct construction so the (stateless) engine calls the Table A surface
// in plain primitives. State transport stays on the single wire owned by
// internal/space.StateClient (fence in body, OBJECT read envelopes, scope only from
// the task-token principal); this bridge adds no new wire and no new persistence.
//
// Emil's container name/tag/port and upstream immutability are NOT a gate
// (decisions/2026-10-30-emil-image-scope-correction.md): this is AHP-owned Go code
// that adapts the Emil workflow's orchestration needs onto AHP's real State API +
// Coordinator per PDF rev3.2.

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

// CoordinatorOps is the workflow.py-facing turn-lifecycle surface expressed in
// plain primitives. *space.Coordinator satisfies it through CoordinatorBridge;
// tests can substitute a fake to assert the engine drives the right ops.
type CoordinatorOps interface {
	SubmitHITL(ctx context.Context, token, turnID, kind string, prompt json.RawMessage, deadline *time.Time, runID, worker string, epoch int64) (*space.HITLRequest, error)
	RespondHITL(ctx context.Context, token, hitlID string, approved bool, response json.RawMessage, threadID string) error
	EmitOutbox(ctx context.Context, token, turnID, key, kind string, payload json.RawMessage, threadID, worker string, epoch int64) (*space.OutboxEntry, error)
	Steer(ctx context.Context, token, turnID, instruction string) error
	CancelTurn(ctx context.Context, callerToken, turnID string) error
	Recover(ctx context.Context, token string) (*space.RecoveryResult, error)
	RehydrateTurn(t space.Turn)
	TurnSnapshot(turnID string) (space.Turn, bool)
}

// CoordinatorBridge implements CoordinatorOps over the authoritative Coordinator.
type CoordinatorBridge struct{ coord *space.Coordinator }

// NewCoordinatorBridge wires the turn-lifecycle seam to an existing Coordinator.
func NewCoordinatorBridge(c *space.Coordinator) *CoordinatorBridge {
	return &CoordinatorBridge{coord: c}
}

var _ CoordinatorOps = (*CoordinatorBridge)(nil)

// SubmitHITL opens an approval gate for a running turn and suspends it
// (workflow.py human-in-the-loop -> Coordinator.SubmitHITL -> State inbox).
func (b *CoordinatorBridge) SubmitHITL(ctx context.Context, token, turnID, kind string, prompt json.RawMessage, deadline *time.Time, runID, worker string, epoch int64) (*space.HITLRequest, error) {
	return b.coord.SubmitHITL(ctx, token, space.HITLSubmitRequest{
		TurnID:   turnID,
		Kind:     kind,
		Prompt:   prompt,
		Deadline: deadline,
	}, runID, worker, epoch)
}

// RespondHITL records a human decision for an open HITL gate.
func (b *CoordinatorBridge) RespondHITL(ctx context.Context, token, hitlID string, approved bool, response json.RawMessage, threadID string) error {
	return b.coord.RespondHITL(ctx, token, space.HITLRespondRequest{
		HITLID:   hitlID,
		Approved: approved,
		Response: response,
	}, threadID)
}

// EmitOutbox records an at-least-once delivery item for a turn.
func (b *CoordinatorBridge) EmitOutbox(ctx context.Context, token, turnID, key, kind string, payload json.RawMessage, threadID, worker string, epoch int64) (*space.OutboxEntry, error) {
	return b.coord.EmitOutbox(ctx, token, space.OutboxEmitRequest{
		TurnID:  turnID,
		Key:     key,
		Kind:    kind,
		Payload: payload,
	}, threadID, worker, epoch)
}

// Steer injects a steering instruction into a running turn.
func (b *CoordinatorBridge) Steer(ctx context.Context, token, turnID, instruction string) error {
	return b.coord.Steer(ctx, token, space.SteerRequest{TurnID: turnID, Instruction: instruction})
}

// CancelTurn cancels an active turn by id: it revokes the run's OWN grant with the run's child
// token (never the ingress authority) and then closes the run durably with callerToken, the
// caller's still-live State token - the run's own credential is dead by then by design. A revoke
// failure is surfaced, never a false ok.
func (b *CoordinatorBridge) CancelTurn(ctx context.Context, callerToken, turnID string) error {
	return b.coord.CancelTurn(ctx, turnID, callerToken)
}

// Recover reclassifies in-memory turns after a restart (pending -> recovered,
// running -> unknown) so the engine never double-executes an interrupted run.
func (b *CoordinatorBridge) Recover(ctx context.Context, token string) (*space.RecoveryResult, error) {
	return b.coord.Recover(ctx, token)
}

// RehydrateTurn merges a turn restored from State back into the Coordinator
// before Recover classification (restart recovery).
func (b *CoordinatorBridge) RehydrateTurn(t space.Turn) { b.coord.RehydrateTurn(t) }

// TurnSnapshot returns a read-only copy of a turn and whether it is known.
func (b *CoordinatorBridge) TurnSnapshot(turnID string) (space.Turn, bool) {
	return b.coord.TurnSnapshot(turnID)
}
