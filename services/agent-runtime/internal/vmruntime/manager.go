package vmruntime

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type Execution struct {
	Scope         contracts.Scope `json:"scope"`
	SessionID     string          `json:"session_id"`
	Harness       string          `json:"harness_type,omitempty"`
	LaunchTurnSeq int64           `json:"launch_turn_seq,omitempty"`
	Generation    int64           `json:"generation"`
	ID            string          `json:"execution_id"`
}

type ExecutionHost interface {
	StartOrAdopt(context.Context, Execution, int64) error
	Running(context.Context, Execution, int64) (bool, error)
	FenceAndWait(context.Context, Execution) error
}

type GrantStore interface {
	CreateRoot(context.Context, contracts.Scope, identity.RootRequest) (identity.Grant, error)
	FenceExecution(context.Context, contracts.Scope, string) error
}

type Manager struct {
	Pool         *pgxpool.Pool
	Grants       GrantStore
	PlaceHost    func(Execution) string
	Host         func(string) (ExecutionHost, error)
	ToolsAllowed func(contracts.Scope) bool
}

func (m Manager) Migrate(ctx context.Context) error {
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(2026112301)"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (m Manager) EnsureRunning(ctx context.Context, exec Execution) error {
	if err := exec.validate(); err != nil {
		return err
	}
	if exec.LaunchTurnSeq < 1 || exec.Harness == "" ||
		exec.ID != journal.DeterministicExecutionID(exec.Scope, exec.SessionID, exec.LaunchTurnSeq, exec.Generation) {
		return contracts.NewError(contracts.ErrInvalid, "invalid execution launch")
	}

	var hostID, status string
	var attempt int64
	err := m.Pool.QueryRow(ctx, `
SELECT host_id, status, attempt_no FROM ahp_runtime.executions
WHERE (tenant_id, deployment_id, execution_id, subject_id, session_id, generation) = ($1,$2,$3,$4,$5,$6)`,
		exec.key()...).Scan(&hostID, &status, &attempt)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return unavailable("cannot read execution")
	}
	switch status {
	case "FENCED", "STOPPING":
		return executionConflict()
	case "RUNNING":
		host, err := m.Host(hostID)
		if err != nil {
			return unavailable("execution host unavailable")
		}
		alive, err := host.Running(ctx, exec, attempt)
		if err != nil {
			return unavailable("cannot check execution on its host")
		}
		if alive {
			return nil
		}
	}

	candidateHost := m.PlaceHost(exec)
	if candidateHost == "" {
		return unavailable("no execution host configured")
	}
	args := append(exec.key(), exec.Harness, exec.LaunchTurnSeq, candidateHost)
	err = m.Pool.QueryRow(ctx, `
INSERT INTO ahp_runtime.executions AS e
    (tenant_id, deployment_id, execution_id, subject_id, session_id, generation, harness_type, launch_turn_seq, host_id, attempt_no, status)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,1,'STARTING')
ON CONFLICT (tenant_id, deployment_id, execution_id) DO UPDATE
SET attempt_no=e.attempt_no+1, status='STARTING', updated_at=clock_timestamp()
WHERE (e.subject_id, e.session_id, e.generation, e.harness_type, e.launch_turn_seq) =
      (EXCLUDED.subject_id, EXCLUDED.session_id, EXCLUDED.generation, EXCLUDED.harness_type, EXCLUDED.launch_turn_seq)
  AND (e.status='FAILED'
    OR (e.status='STARTING' AND e.updated_at <= statement_timestamp()-interval '10 seconds')
    OR (e.status='RUNNING' AND e.attempt_no=$10 AND $11='RUNNING'))
RETURNING host_id, attempt_no`, append(args, attempt, status)...).Scan(&hostID, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return executionConflict()
	}
	if err != nil {
		return unavailable("cannot reserve execution")
	}

	err = m.start(ctx, exec, hostID, attempt)
	if err != nil {
		// A later attempt can recover a partial start from the durable slot.
		recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		_, dbErr := m.Pool.Exec(recovery, `
UPDATE ahp_runtime.executions SET status='FAILED', updated_at=clock_timestamp()
WHERE (tenant_id, deployment_id, execution_id, subject_id, session_id, generation) = ($1,$2,$3,$4,$5,$6)
  AND status='STARTING' AND attempt_no=$7`, append(exec.key(), attempt)...)
		if dbErr != nil {
			return fmt.Errorf("execution start failed (%v) and recording failure: %w", err, dbErr)
		}
		return err
	}
	result, err := m.Pool.Exec(ctx, `
UPDATE ahp_runtime.executions SET status='RUNNING', updated_at=clock_timestamp()
WHERE (tenant_id, deployment_id, execution_id, subject_id, session_id, generation) = ($1,$2,$3,$4,$5,$6)
  AND status='STARTING' AND attempt_no=$7`, append(exec.key(), attempt)...)
	if err != nil {
		return unavailable("execution started but publication was not confirmed")
	}
	if result.RowsAffected() == 0 {
		// The host slot owns the guest; a stale manager must never stop it.
		return executionConflict()
	}
	return nil
}

func (m Manager) start(ctx context.Context, exec Execution, hostID string, attempt int64) error {
	host, err := m.Host(hostID)
	if err != nil {
		return unavailable("execution host unavailable")
	}
	capabilities := []string{"state", "state.vm_claim"}
	if m.ToolsAllowed != nil && m.ToolsAllowed(exec.Scope) {
		capabilities = append(capabilities, "tools", "tools.leased")
	}
	grant, err := m.Grants.CreateRoot(ctx, exec.Scope, identity.RootRequest{
		Deployment: exec.Scope.Deployment, Task: exec.ID,
		Context: journal.VMSessionContextID(exec.Scope, exec.SessionID), Capabilities: capabilities, TTL: time.Hour,
	})
	if err != nil {
		return err
	}
	if grant.Status != identity.StatusActive {
		return executionConflict()
	}
	return host.StartOrAdopt(ctx, exec, attempt)
}

func (m Manager) Fence(ctx context.Context, exec Execution) error {
	if err := exec.validate(); err != nil {
		return err
	}
	candidateHost := m.PlaceHost(exec)
	if candidateHost == "" {
		return unavailable("no execution host configured")
	}
	var hostID string
	err := m.Pool.QueryRow(ctx, `
INSERT INTO ahp_runtime.executions AS e
    (tenant_id, deployment_id, execution_id, subject_id, session_id, generation, harness_type, host_id, status)
VALUES ($1,$2,$3,$4,$5,$6,'',$7,'STOPPING')
ON CONFLICT (tenant_id, deployment_id, execution_id) DO UPDATE
SET status=CASE WHEN e.status='FENCED' THEN e.status ELSE 'STOPPING' END, updated_at=clock_timestamp()
WHERE (e.subject_id, e.session_id, e.generation)=(EXCLUDED.subject_id, EXCLUDED.session_id, EXCLUDED.generation)
RETURNING host_id`, append(exec.key(), candidateHost)...).Scan(&hostID)
	if errors.Is(err, pgx.ErrNoRows) {
		return executionConflict()
	}
	if err != nil {
		return unavailable("cannot reserve execution fence")
	}

	// Fence the identity even when no grant exists yet. This also invalidates
	// previously issued tokens while the host is stopping the guest.
	if err := m.Grants.FenceExecution(ctx, exec.Scope, exec.ID); err != nil {
		return unavailable("cannot confirm execution grant was fenced")
	}
	host, err := m.Host(hostID)
	if err != nil {
		return unavailable("execution host unavailable")
	}
	if err := host.FenceAndWait(ctx, exec); err != nil {
		return unavailable("execution stop not confirmed: " + err.Error())
	}
	result, err := m.Pool.Exec(ctx, `
UPDATE ahp_runtime.executions SET status='FENCED', fenced_at=coalesce(fenced_at,clock_timestamp()), updated_at=clock_timestamp()
WHERE (tenant_id, deployment_id, execution_id, subject_id, session_id, generation)=($1,$2,$3,$4,$5,$6)
  AND status IN ('FENCED','STOPPING')`, exec.key()...)
	if err != nil || result.RowsAffected() != 1 {
		return unavailable("cannot confirm durable execution fence")
	}
	return nil
}

func (e Execution) key() []any {
	return []any{e.Scope.Tenant, e.Scope.Deployment, e.ID, e.Scope.Subject, e.SessionID, e.Generation}
}

func (e Execution) validate() error {
	if err := e.Scope.Validate(); err != nil {
		return err
	}
	if !journal.ValidExecutionID(e.ID) || e.SessionID == "" || len(e.SessionID) > 256 ||
		strings.ContainsFunc(e.SessionID, func(r rune) bool { return r <= ' ' || r == 127 }) || e.Generation < 1 {
		return contracts.NewError(contracts.ErrInvalid, "invalid execution address")
	}
	return nil
}

func unavailable(msg string) error { return contracts.NewError(contracts.ErrUnavailable, msg) }

func executionConflict() error {
	return contracts.NewError(contracts.ErrConflict, "execution attempt is busy, fenced or superseded")
}
