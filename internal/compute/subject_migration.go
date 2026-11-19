package compute

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
)

// migrateSubjectGuards applies guard DDL once under a transaction lock. Repeated
// boots do not replace pg_proc tuples while admitted writers are running.
func (s *Store) migrateSubjectGuards(ctx context.Context, tenantDDL string) error {
	schema := "ahp_compute"
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", schema+"/owner-guard-migration"); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+schema+".owner_guard_migrations(name text PRIMARY KEY)"); err != nil {
			return err
		}
		var applied bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+schema+".owner_guard_migrations WHERE name='subject-v1')").Scan(&applied); err != nil {
			return err
		}
		if applied {
			return nil
		}
		q := tenantDDL + fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s.erased_subjects(tenant text NOT NULL, subject text NOT NULL, PRIMARY KEY(tenant,subject));
CREATE OR REPLACE FUNCTION %[1]s.guard_subject_insert() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE target text; subj text; BEGIN
target:=to_jsonb(NEW)->>'tenant'; subj:=to_jsonb(NEW)->>'subject'; IF subj IS NULL THEN SELECT tenant,subject INTO target,subj FROM ahp_compute.computers WHERE id=to_jsonb(NEW)->>'computer_id'; END IF;
IF subj IS NOT NULL THEN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('%[1]s/subject-erasure/'||target||'/'||subj,0));
 IF EXISTS(SELECT 1 FROM %[1]s.erased_subjects WHERE tenant=target AND subject=subj) THEN RAISE EXCEPTION 'compute: subject erased' USING ERRCODE='42501'; END IF;
END IF; RETURN NEW; END $$;`, schema)
		for _, table := range []string{"computers", "incarnations", "volume_fences", "activity_leases", "commands", "snapshots", "env_revisions", "workspace_revisions", "state_revisions", "cost_ledger"} {
			q += "CREATE OR REPLACE TRIGGER subject_insert_guard BEFORE INSERT ON " + schema + "." + table + " FOR EACH ROW EXECUTE FUNCTION " + schema + ".guard_subject_insert();"
		}
		if _, err := tx.Exec(ctx, q); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO "+schema+".owner_guard_migrations VALUES('subject-v1')")
		return err
	})
}
