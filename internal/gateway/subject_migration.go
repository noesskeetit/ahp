package gateway

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
)

// migrateSubjectGuards applies guard DDL once under a transaction lock. Repeated
// boots do not replace pg_proc tuples while admitted writers are running.
func (s *Store) migrateSubjectGuards(ctx context.Context, tenantDDL string) error {
	schema := s.schema
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
target:=to_jsonb(NEW)->>'tenant'; subj:=to_jsonb(NEW)->>'subject'; IF subj IS NULL THEN SELECT tenant,subject INTO target,subj FROM %[1]s.tasks WHERE task_id=to_jsonb(NEW)->>'task_id'; END IF;
IF subj IS NOT NULL THEN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('%[1]s/subject-erasure/'||target||'/'||subj,0));
 IF EXISTS(SELECT 1 FROM %[1]s.erased_subjects WHERE tenant=target AND subject=subj) AND (TG_TABLE_NAME NOT IN ('task_events','outbox') OR NOT EXISTS(SELECT 1 FROM %[1]s.tasks WHERE task_id=to_jsonb(NEW)->>'task_id' AND tenant=target AND subject=subj)) THEN RAISE EXCEPTION 'gateway: subject erased' USING ERRCODE='42501'; END IF;
END IF; RETURN NEW; END $$;`, schema)
		for _, table := range []string{"contexts", "tasks", "messages", "task_messages", "task_events", "outbox", "push_configs", "child_status_inbox"} {
			q += "CREATE OR REPLACE TRIGGER subject_insert_guard BEFORE INSERT ON " + schema + "." + table + " FOR EACH ROW EXECUTE FUNCTION " + schema + ".guard_subject_insert();"
		}
		if _, err := tx.Exec(ctx, q); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO "+schema+".owner_guard_migrations VALUES('subject-v1')")
		return err
	})
}
