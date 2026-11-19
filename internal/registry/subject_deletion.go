package registry

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

type TaskSubjectResolver func(context.Context, string, string) (string, error)

// WithTaskSubjectResolver binds legacy pins only from an owning authority's API.
// No user-supplied ownership or cross-service SQL is used by Registry.
func WithTaskSubjectResolver(resolve TaskSubjectResolver) Option {
	return func(s *Store) { s.taskSubject = resolve }
}

const subjectDeletionSchema = `CREATE TABLE IF NOT EXISTS ahp_registry.erased_subjects(tenant text NOT NULL,subject text NOT NULL,PRIMARY KEY(tenant,subject));`

func checkSubjectWritable(ctx context.Context, tx pgx.Tx, tenant, subject string) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))", "ahp_registry/subject-erasure/"+tenant+"/"+subject); err != nil {
		return err
	}
	var fenced bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ahp_registry.erased_subjects WHERE tenant=$1 AND (subject=$2 OR $2=''))", tenant, subject).Scan(&fenced); err != nil {
		return err
	}
	if fenced {
		return contracts.NewError(contracts.ErrForbidden, "registry: subject erased or legacy pin lacks owner")
	}
	return nil
}

func (s *Store) FenceSubject(ctx context.Context, tenant, subject string) error {
	if err := (contracts.Scope{Tenant: tenant, Subject: subject, Deployment: "control"}).Validate(); err != nil {
		return err
	}
	if err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", tenantFenceKey(tenant)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "ahp_registry/subject-erasure/"+tenant+"/"+subject); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO ahp_registry.erased_subjects VALUES($1,$2) ON CONFLICT DO NOTHING", tenant, subject)
		return err
	}); err != nil {
		return err
	}
	// Resolve before any owner loses its task inventory. Do not retain a pool
	// lease across the authority call: a composed MaxConns=1 pool must progress.
	rows, err := s.pool.Query(ctx, "SELECT task_id FROM ahp_registry.task_pins WHERE tenant=$1 AND subject=''", tenant)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if s.taskSubject == nil {
			return contracts.NewReasoned(contracts.ErrUnavailable, "registry_legacy_pin_owner_unknown", "registry: legacy pin needs an authoritative task owner")
		}
		owner, err := s.taskSubject(ctx, tenant, id)
		if err != nil || owner == "" {
			return contracts.NewReasoned(contracts.ErrUnavailable, "registry_legacy_pin_owner_unknown", "registry: legacy pin owner unavailable; inventory retained")
		}
		if _, err = s.pool.Exec(ctx, "UPDATE ahp_registry.task_pins SET subject=$3 WHERE tenant=$1 AND task_id=$2 AND subject=''", tenant, id, owner); err != nil {
			return err
		}
	}
	return nil
}

// DeleteSubject removes task snapshots only. Agents, releases, deployments and
// tenant configuration are shared catalog objects and are deliberately retained.
func (s *Store) DeleteSubject(ctx context.Context, tenant, subject string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "ahp_registry/subject-erasure/"+tenant+"/"+subject); err != nil {
			return err
		}
		var fenced, unknown bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ahp_registry.erased_subjects WHERE tenant=$1 AND subject=$2),EXISTS(SELECT 1 FROM ahp_registry.task_pins WHERE tenant=$1 AND subject='')", tenant, subject).Scan(&fenced, &unknown); err != nil {
			return err
		}
		if !fenced || unknown {
			return contracts.NewReasoned(contracts.ErrUnavailable, "registry_subject_inventory_pending", "registry: fence or legacy ownership unresolved")
		}
		for _, table := range []string{"profiles", "dot_pins", "skill_owners"} {
			if _, err := tx.Exec(ctx, "DELETE FROM ahp_registry."+table+" WHERE tenant=$1 AND subject=$2", tenant, subject); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, "DELETE FROM ahp_registry.skill_revisions WHERE publisher_id=$1", skillOwner(tenant, subject)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "DELETE FROM ahp_registry.task_pins WHERE tenant=$1 AND subject=$2", tenant, subject)
		return err
	})
}

func (s *Store) subjectGuards(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `ALTER TABLE ahp_registry.task_pins ADD COLUMN IF NOT EXISTS subject text NOT NULL DEFAULT '';
 CREATE OR REPLACE FUNCTION ahp_registry.guard_subject_pin() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('ahp_registry/subject-erasure/'||NEW.tenant||'/'||NEW.subject,0));
 IF EXISTS(SELECT 1 FROM ahp_registry.erased_subjects WHERE tenant=NEW.tenant AND (subject=NEW.subject OR NEW.subject='')) THEN RAISE EXCEPTION 'registry: subject erased or pin lacks owner' USING ERRCODE='42501'; END IF;
 RETURN NEW; END $$;
 CREATE OR REPLACE TRIGGER subject_pin_guard BEFORE INSERT ON ahp_registry.task_pins FOR EACH ROW EXECUTE FUNCTION ahp_registry.guard_subject_pin();`)
	return err
}
