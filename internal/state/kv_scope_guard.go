package state

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// Format2 has no reviewed declaration/custody for deployment-owned KV.
// Refuse this profile instead of silently omitting shared rows from a subject export.
func (s *Store) checkOwnerKVScopeExport(ctx context.Context, tx pgx.Tx, sc contracts.Scope) error {
	if s.hasKVPolicy(sc) {
		return invalid("scope-v2 does not support owner-scoped KV")
	}
	if found, err := kvScopeHasOwners(ctx, tx, sc); err != nil {
		return err
	} else if found {
		return invalid("scope-v2 does not support declared owner-scoped KV")
	}
	var found bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.kv_ops
        WHERE tenant=$1 AND deployment=$2 AND subject=$3
        AND (result->>'kind'='batch' OR COALESCE(result->>'actor_thread_id','')<>''))`,
		sc.Tenant, sc.Deployment, sc.Subject).Scan(&found); err != nil {
		return err
	}
	if found {
		return invalid("scope-v2 does not support owner-scoped KV/batch receipts")
	}
	return nil
}
func (s *Store) checkOwnerKVScopeImport(sc contracts.Scope, e ScopeV2) error {
	if s.hasKVPolicy(sc) {
		return invalid("scope-v2 does not support owner-scoped KV")
	}
	for _, raw := range e.Entities["kv_ops"] {
		var row struct {
			Result struct {
				Kind          string `json:"kind"`
				ActorThreadID string `json:"actor_thread_id"`
			} `json:"result"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			return invalid("invalid KV receipt row")
		}
		if row.Result.Kind == "batch" || row.Result.ActorThreadID != "" {
			return invalid("scope-v2 does not support owner-scoped KV/batch receipts")
		}
	}
	return nil
}
