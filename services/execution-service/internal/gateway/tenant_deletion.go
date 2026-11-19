package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

func (s *Store) tenantGuards(ctx context.Context) error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s.erased_tenants(tenant text PRIMARY KEY);
 CREATE OR REPLACE FUNCTION %[1]s.guard_tenant_insert() RETURNS trigger LANGUAGE plpgsql AS $$
 DECLARE target text;
 BEGIN
 target:=to_jsonb(NEW)->>'tenant';
 IF target IS NULL THEN SELECT tenant INTO target FROM %[1]s.tasks WHERE task_id=to_jsonb(NEW)->>'task_id'; END IF;
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('%[1]s/tenant-erasure/'||target,0));
 IF EXISTS(SELECT 1 FROM %[1]s.erased_tenants WHERE tenant=target) THEN
 -- Cancellation's status journal is allowed while its task still exists.
 -- The same lock serializes that write with final purge.
 IF TG_TABLE_NAME NOT IN ('task_events','outbox') OR NOT EXISTS(SELECT 1 FROM %[1]s.tasks WHERE task_id=to_jsonb(NEW)->>'task_id' AND tenant=target) THEN
 RAISE EXCEPTION 'gateway: tenant erased' USING ERRCODE='42501'; END IF;
 END IF;
 RETURN NEW; END $$;`, s.schema)
	for _, table := range []string{"contexts", "tasks", "messages", "task_messages", "task_events", "outbox", "push_configs", "child_status_inbox"} {
		q += fmt.Sprintf(`CREATE OR REPLACE TRIGGER tenant_insert_guard BEFORE INSERT ON %[1]s.%[2]s FOR EACH ROW EXECUTE FUNCTION %[1]s.guard_tenant_insert();`, s.schema, table)
	}
	return s.migrateSubjectGuards(ctx, q)
}

// FenceTenant requests cooperative cancellation and fences every external effect.
func (s *Store) FenceTenant(ctx context.Context, tenant string) error {
	ledger, err := operations.New(s.pool, s.schema)
	if err != nil {
		return err
	}
	if err = ledger.FenceTenant(ctx, tenant); err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `UPDATE `+s.t("tasks")+` SET cancel_requested=true,updated_at=now() WHERE tenant=$1 AND terminal_at IS NULL`, tenant)
	if err != nil {
		return err
	}
	// Same rule as the subject fence: the row stops because this plane erased its tenant, and
	// that fact has to be in the journal at the moment the row stops, in the same statement
	// (DEF-J4-CHILD-TERMINAL-REASON).
	_, err = s.pool.Exec(ctx, `UPDATE `+s.t("tasks")+` SET state='TASK_STATE_CANCELED',status_version=status_version+1,metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object('`+MetadataCloseReason+`',$2::text),terminal_at=now() WHERE tenant=$1 AND terminal_at IS NULL AND state IN ('TASK_STATE_SUBMITTED','TASK_STATE_INPUT_REQUIRED','TASK_STATE_AUTH_REQUIRED') AND lease_owner=''`, tenant, CloseReasonTenantDeleted)
	return err
}

// DeleteTenant waits for real task termination and delivery reconciliation.
func (s *Store) DeleteTenant(ctx context.Context, tenant string) error {
	ledger, err := operations.New(s.pool, s.schema)
	if err != nil {
		return err
	}
	if err = ledger.DeleteTenant(ctx, tenant); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.schema+"/tenant-erasure/"+tenant); err != nil {
			return err
		}
		var pending bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+s.t("tasks")+` WHERE tenant=$1 AND terminal_at IS NULL) OR EXISTS(SELECT 1 FROM `+s.t("outbox")+` WHERE tenant=$1 AND state NOT IN ('DONE','FAILED') AND attempts>0)`, tenant).Scan(&pending); err != nil {
			return err
		}
		if pending {
			return contracts.NewReasoned(contracts.ErrUnavailable, "tenant_gateway_pending", "gateway: tasks or delivery outcomes pending")
		}
		for _, table := range []string{"child_status_inbox", "push_configs", "outbox", "task_events", "messages", "tasks", "contexts"} {
			if _, err := tx.Exec(ctx, `DELETE FROM `+s.t(table)+` WHERE tenant=$1`, tenant); err != nil {
				return err
			}
		}
		return nil
	})
}

// watchTenant interrupts the actual executor call; the effect ledger keeps its
// unknown outcome until reconciliation, even when task cancellation is terminal.
func (s *Service) watchOwner(ctx context.Context, scope contracts.Scope) (context.Context, context.CancelFunc) {
	watched, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-watched.Done():
				return
			case <-ticker.C:
				var erased bool
				err := s.store.pool.QueryRow(watched, `SELECT EXISTS(SELECT 1 FROM `+s.store.t("erased_tenants")+` WHERE tenant=$1) OR EXISTS(SELECT 1 FROM `+s.store.t("erased_subjects")+` WHERE tenant=$1 AND subject=$2)`, scope.Tenant, scope.Subject).Scan(&erased)
				if err != nil || erased {
					cancel()
					return
				}
			}
		}
	}()
	return watched, cancel
}
