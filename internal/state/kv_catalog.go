package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const kvPolicyLockSeed = 7321

func kvCatalogLockID(id string) string {
	h := sha256.Sum256([]byte(id))
	return hex.EncodeToString(h[:])
}

const kvOwnerCatalogDDL = `CREATE TABLE IF NOT EXISTS ahp_state.kv_namespace_owners (
 tenant text NOT NULL, deployment text NOT NULL, namespace text[] NOT NULL,
 owner text NOT NULL CHECK(owner IN ('subject','deployment')),
 PRIMARY KEY(tenant,deployment,namespace));`

type kvPolicyConnection interface {
	ddlExec
	kvPolicyQuerier
	Begin(context.Context) (pgx.Tx, error)
}

// Register the reviewed policy durably. A namespace owner is immutable without
// an explicit offline migration; omission in a replica's config never resets it.
func (s *Store) registerKVPolicies(ctx context.Context, conn kvPolicyConnection) error {
	ids := make([]string, 0, len(s.kvOwnerPolicies))
	for id := range s.kvOwnerPolicies {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	locked := []string{}
	defer func() {
		c, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		for i := len(locked) - 1; i >= 0; i-- {
			_, _ = conn.Exec(c, `SELECT pg_advisory_unlock(hashtextextended($1,$2))`, kvCatalogLockID(locked[i]), int64(kvPolicyLockSeed))
		}
	}()
	for _, id := range ids {
		if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1,$2))`, kvCatalogLockID(id), int64(kvPolicyLockSeed)); err != nil {
			return err
		}
		locked = append(locked, id)
	}
	return pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		// All checks precede all inserts, and the complete declaration commits
		// atomically while namespace writers remain excluded.
		if err := s.validateKVPolicyRows(ctx, tx); err != nil {
			return err
		}
		for _, id := range ids {
			p := s.kvOwnerPolicies[id]
			var owner string
			err := tx.QueryRow(ctx, `SELECT owner FROM ahp_state.kv_namespace_owners WHERE tenant=$1 AND deployment=$2 AND namespace=$3`, p.Tenant, p.Deployment, p.Namespace).Scan(&owner)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			if err == nil && owner != p.Owner {
				return invalid("namespace ownership requires an explicit policy migration")
			}
		}
		for _, id := range ids {
			p := s.kvOwnerPolicies[id]
			if _, err := tx.Exec(ctx, `INSERT INTO ahp_state.kv_namespace_owners(tenant,deployment,namespace,owner) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, p.Tenant, p.Deployment, p.Namespace, p.Owner); err != nil {
				return err
			}
		}
		return nil
	})
}

// One ordered shared lock per namespace serializes admission against the
// registration/migration owner. Check after the lock wait in Read Committed.
func (s *Store) checkKVPolicies(ctx context.Context, tx pgx.Tx, actor contracts.Scope, namespaces [][]string) error {
	byID := map[string][]string{}
	for _, ns := range namespaces {
		ns = normalizeKVNamespace(ns)
		byID[kvPolicyID(actor, ns)] = ns
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,$2))`, kvCatalogLockID(id), int64(kvPolicyLockSeed)); err != nil {
			return err
		}
		var owner string
		err := tx.QueryRow(ctx, `SELECT owner FROM ahp_state.kv_namespace_owners WHERE tenant=$1 AND deployment=$2 AND namespace=$3`, actor.Tenant, actor.Deployment, byID[id]).Scan(&owner)
		expected := s.kvOwner(actor, byID[id]).Kind
		if errors.Is(err, pgx.ErrNoRows) {
			if expected == "thread" {
				continue
			}
			return contracts.NewError(contracts.ErrUnavailable, "reviewed namespace policy has not been registered")
		}
		if err != nil {
			return err
		}
		if owner != expected {
			return contracts.NewError(contracts.ErrUnavailable, "replica namespace policy differs from the durable declaration")
		}
	}
	return nil
}

func (s *Store) checkKVSearchPolicies(ctx context.Context, tx pgx.Tx, actor contracts.Scope) error {
	rows, err := tx.Query(ctx, `SELECT namespace FROM ahp_state.kv_namespace_owners WHERE tenant=$1 AND deployment=$2`, actor.Tenant, actor.Deployment)
	if err != nil {
		return err
	}
	namespaces := [][]string{}
	for rows.Next() {
		var ns []string
		if err = rows.Scan(&ns); err != nil {
			rows.Close()
			return err
		}
		namespaces = append(namespaces, ns)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, p := range s.kvOwnerPolicies {
		if p.Tenant == actor.Tenant && p.Deployment == actor.Deployment {
			namespaces = append(namespaces, p.Namespace)
		}
	}
	return s.checkKVPolicies(ctx, tx, actor, namespaces)
}

func kvScopeHasOwners(ctx context.Context, tx pgx.Tx, sc contracts.Scope) (bool, error) {
	var found bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.kv_namespace_owners WHERE tenant=$1 AND deployment=$2)`, sc.Tenant, sc.Deployment).Scan(&found)
	return found, err
}
