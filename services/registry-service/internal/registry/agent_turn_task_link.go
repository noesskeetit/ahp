package registry

import (
	"context"
	"errors"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// A remote A2A task ID is opaque and need not equal its Identity grant ID. Keep
// one small link, not a second copy of the config snapshot. Removing the owning
// pin (including subject/tenant erasure) removes the link in the same transaction.
const agentTurnDDL = `CREATE TABLE IF NOT EXISTS ahp_registry.agent_turn_tasks (
 tenant text NOT NULL CHECK (tenant<>''),
 origin_pin_id text PRIMARY KEY REFERENCES ahp_registry.task_pins(task_id) ON DELETE CASCADE,
 grant_task_id text NOT NULL UNIQUE,
 runtime_task_id text NOT NULL
);`

type agentTaskLink struct {
	GrantTask   string
	RuntimeTask string
}

func (s *Store) bindAgentTask(ctx context.Context, scope contracts.Scope, pin, grant, remote string) error {
	if remote == "" || len(remote) > 512 || strings.ContainsAny(remote, "\x00\r\n") {
		return contracts.NewError(contracts.ErrConflict, "agent runtime returned an invalid task identity")
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err := checkSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		var owner string
		if err := tx.QueryRow(ctx, `SELECT subject FROM ahp_registry.task_pins WHERE tenant=$1 AND task_id=$2 FOR UPDATE`, scope.Tenant, pin).Scan(&owner); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return contracts.NewError(contracts.ErrNotFound, "agent turn pin not found")
			}
			return storageError("lock agent turn pin", err)
		}
		if owner != scope.Subject {
			return contracts.NewError(contracts.ErrNotFound, "agent turn pin not found")
		}
		if _, err := tx.Exec(ctx, `INSERT INTO ahp_registry.agent_turn_tasks(tenant,origin_pin_id,grant_task_id,runtime_task_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, scope.Tenant, pin, grant, remote); err != nil {
			return storageError("bind agent task", err)
		}
		var linked agentTaskLink
		if err := tx.QueryRow(ctx, `SELECT grant_task_id,runtime_task_id FROM ahp_registry.agent_turn_tasks WHERE tenant=$1 AND origin_pin_id=$2`, scope.Tenant, pin).Scan(&linked.GrantTask, &linked.RuntimeTask); err != nil {
			return storageError("read agent task binding", err)
		}
		if linked.GrantTask != grant || linked.RuntimeTask != remote {
			return contracts.NewError(contracts.ErrConflict, "agent runtime attempted to substitute the turn's task")
		}
		return nil
	})
}

func (s *Store) agentTask(ctx context.Context, scope contracts.Scope, pin string) (agentTaskLink, error) {
	var out agentTaskLink
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err := checkSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		err := tx.QueryRow(ctx, `SELECT l.grant_task_id,l.runtime_task_id FROM ahp_registry.agent_turn_tasks l JOIN ahp_registry.task_pins p ON p.task_id=l.origin_pin_id AND p.tenant=l.tenant WHERE l.tenant=$1 AND l.origin_pin_id=$2 AND p.subject=$3`, scope.Tenant, pin, scope.Subject).Scan(&out.GrantTask, &out.RuntimeTask)
		if errors.Is(err, pgx.ErrNoRows) {
			// Only this owner-side condition permits replay of the original start
			// message. A 404 returned by an already linked runtime task must remain
			// distinct: it does not prove the external work never happened.
			return contracts.NewReasoned(contracts.ErrNotFound, "AGENT_TASK_UNCONFIRMED", "agent turn has no confirmed remote task")
		}
		if err != nil {
			return storageError("read agent task link", err)
		}
		return nil
	})
	return out, err
}
