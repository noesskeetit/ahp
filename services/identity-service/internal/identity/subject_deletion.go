package identity

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// FenceSubject implements the service-owner erasure contract with the same
// permanent tombstone consulted by every task and browser authentication.
func (s *Store) FenceSubject(ctx context.Context, tenant, subject string) error {
	return s.RevokeSubject(ctx, tenant, subject)
}

// DeleteSubject removes credentials, sessions and grants only after revocation.
// The composition calls this last, after owners resolve their task bindings.
func (s *Store) DeleteSubject(ctx context.Context, tenant, subject string) error {
	return s.runTxDetached(func(ctx context.Context, tx pgx.Tx) error {
		// The context label journal is append-only; an erasure is the one
		// operation allowed to remove events, and it says so inside its own
		// transaction so the rule stays a database rule for everyone else.
		if _, err := tx.Exec(ctx, `SET LOCAL ahp_identity.erasing = 'on'`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "identity-subject-erasure/"+tenant+"/"+subject); err != nil {
			return err
		}
		var fenced bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ahp_identity.subjects WHERE tenant=$1 AND subject=$2)", tenant, subject).Scan(&fenced); err != nil {
			return err
		}
		if !fenced {
			return contracts.NewError(contracts.ErrForbidden, "identity: subject revocation required")
		}
		for _, q := range []string{
			`DELETE FROM ahp_identity.execution_fences WHERE tenant=$1 AND subject=$2`,
			`DELETE FROM ahp_identity.resource_rules WHERE tenant=$1 AND subject=$2`,
			`DELETE FROM ahp_identity.browser_token_bindings WHERE session_id IN (SELECT session_id FROM ahp_identity.browser_sessions WHERE tenant=$1 AND subject=$2)`,
			`DELETE FROM ahp_identity.browser_sessions WHERE tenant=$1 AND subject=$2`,
			`DELETE FROM ahp_identity.browser_credentials WHERE user_id IN (SELECT user_id FROM ahp_identity.browser_users WHERE tenant=$1 AND subject=$2)`,
			`DELETE FROM ahp_identity.browser_grants WHERE user_id IN (SELECT user_id FROM ahp_identity.browser_users WHERE tenant=$1 AND subject=$2)`,
			`DELETE FROM ahp_identity.browser_users WHERE tenant=$1 AND subject=$2`,
			`DELETE FROM ahp_identity.issued_tokens WHERE grant_id IN (SELECT id FROM ahp_identity.grants_child WHERE tenant=$1 AND subject=$2 UNION SELECT id FROM ahp_identity.grants_root WHERE tenant=$1 AND subject=$2)`,
			`DELETE FROM ahp_identity.grants_child WHERE tenant=$1 AND subject=$2`,
			`DELETE FROM ahp_identity.grants_root WHERE tenant=$1 AND subject=$2`,
			`DELETE FROM ahp_identity.contexts WHERE tenant=$1 AND subject=$2`,
		} {
			if _, err := tx.Exec(ctx, q, tenant, subject); err != nil {
				return err
			}
		}
		return nil
	})
}

// TaskSubject resolves a legacy task binding from this authority's own rows.
// Ambiguous task IDs are refused rather than guessed across deployments.
func (s *Store) TaskSubject(ctx context.Context, tenant, task string) (string, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT subject FROM (SELECT subject FROM ahp_identity.grants_root WHERE tenant=$1 AND task=$2 UNION SELECT subject FROM ahp_identity.grants_child WHERE tenant=$1 AND task=$2) owners`, tenant, task)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	owner := ""
	n := 0
	for rows.Next() {
		if err = rows.Scan(&owner); err != nil {
			return "", err
		}
		n++
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	if n == 0 {
		return "", contracts.NewError(contracts.ErrNotFound, "identity: task owner unavailable")
	}
	if n > 1 {
		return "", contracts.NewError(contracts.ErrConflict, "identity: task owner ambiguous")
	}
	return owner, nil
}

func (s *Store) subjectGuards(ctx context.Context, conn *pgxpool.Conn) error {
	q := `CREATE OR REPLACE FUNCTION ahp_identity.guard_subject_insert() RETURNS trigger LANGUAGE plpgsql AS $$
 DECLARE target text; subj text; BEGIN
 target:=to_jsonb(NEW)->>'tenant'; subj:=to_jsonb(NEW)->>'subject';
 IF subj IS NULL AND to_jsonb(NEW)?'user_id' THEN SELECT tenant,subject INTO target,subj FROM ahp_identity.browser_users WHERE user_id=to_jsonb(NEW)->>'user_id'; END IF;
 IF subj IS NULL AND to_jsonb(NEW)?'session_id' THEN SELECT tenant,subject INTO target,subj FROM ahp_identity.browser_sessions WHERE session_id=to_jsonb(NEW)->>'session_id'; END IF;
 IF subj IS NULL AND to_jsonb(NEW)?'grant_id' THEN SELECT tenant,subject INTO target,subj FROM ahp_identity.grants_root WHERE id=to_jsonb(NEW)->>'grant_id'; IF subj IS NULL THEN SELECT tenant,subject INTO target,subj FROM ahp_identity.grants_child WHERE id=to_jsonb(NEW)->>'grant_id'; END IF; END IF;
 IF subj IS NOT NULL THEN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('identity-subject-erasure/'||target||'/'||subj,0));
 IF EXISTS(SELECT 1 FROM ahp_identity.subjects WHERE tenant=target AND subject=subj) THEN RAISE EXCEPTION 'identity: subject erased' USING ERRCODE='42501'; END IF;
 END IF; RETURN NEW; END $$;`
	for _, table := range []string{"resource_rules", "contexts", "context_label_events", "grants_root", "grants_child", "issued_tokens", "browser_users", "browser_credentials", "browser_grants", "browser_sessions", "browser_token_bindings"} {
		q += "CREATE OR REPLACE TRIGGER subject_insert_guard BEFORE INSERT ON ahp_identity." + table + " FOR EACH ROW EXECUTE FUNCTION ahp_identity.guard_subject_insert();"
	}
	_, err := conn.Exec(ctx, q)
	return err
}
