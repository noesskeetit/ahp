package statejournal

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

type KVEntry = journal.KVEntry

func (s *Store) GetUserState(ctx context.Context, scope contracts.Scope, namespace string) ([]KVEntry, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	entries, err := readKV(ctx, tx, scope, "", namespace, "user")
	if err != nil {
		return nil, err
	}
	return entries, tx.Commit(ctx)
}

func readKV(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session, namespace, scopeType string) ([]KVEntry, error) {
	if !validID(namespace) {
		return nil, contracts.NewError(contracts.ErrInvalid, "invalid state namespace")
	}
	rows, err := tx.Query(ctx, `
		SELECT scope_type, key, version, inline_payload, blob_ref, deleted
		FROM ahp_journal.scoped_kv
		WHERE tenant_id=$1 AND deployment_id=$2 AND namespace=$3
		  AND ($6='' OR scope_type=$6)
		  AND ((scope_type='session' AND scope_owner=$4)
		    OR (scope_type='user' AND scope_owner=$5)
		    OR (scope_type='app' AND scope_owner=$2))
		ORDER BY scope_type, key`, scope.Tenant, scope.Deployment, namespace,
		session, scope.Subject, scopeType)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (KVEntry, error) {
		var entry KVEntry
		err := row.Scan(&entry.ScopeType, &entry.Key, &entry.Version, &entry.Payload.Inline, &entry.Payload.Blob, &entry.Deleted)
		return entry, err
	})
}

type KVQuery struct {
	Session   string
	ScopeType string
	Namespace string
	Key       string
	Prefix    bool
	Limit     int
	Offset    int
}

func (s *Store) ListKV(ctx context.Context, scope contracts.Scope, query KVQuery) ([]KVEntry, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if !validID(query.Namespace) || (query.Key != "" && !validID(query.Key)) || query.Limit < 1 || query.Limit > 1000 ||
		query.Offset < 0 || query.Offset > 10000 || (query.Prefix && query.ScopeType != "store") {
		return nil, contracts.NewError(contracts.ErrInvalid, "invalid scoped KV query")
	}
	var owner string
	switch query.ScopeType {
	case "session":
		if _, err := s.GetTurnStatus(ctx, scope, query.Session); err != nil {
			return nil, err
		}
		owner = query.Session
	case "user", "store":
		owner = scope.Subject
	case "app":
		owner = scope.Deployment
	default:
		return nil, contracts.NewError(contracts.ErrInvalid, "unknown KV scope")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT namespace,key,version,inline_payload,blob_ref,deleted,updated_at
		FROM ahp_journal.scoped_kv
		WHERE tenant_id=$1 AND deployment_id=$2 AND scope_type=$3 AND scope_owner=$4
		  AND ((NOT $6 AND namespace=$5) OR ($6 AND starts_with(namespace,$5)))
		  AND ($9='' OR key=$9)
		ORDER BY namespace,key LIMIT $7 OFFSET $8`,
		scope.Tenant, scope.Deployment, query.ScopeType, owner, query.Namespace,
		query.Prefix, query.Limit, query.Offset, query.Key)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (KVEntry, error) {
		entry := KVEntry{ScopeType: query.ScopeType}
		err := row.Scan(&entry.Namespace, &entry.Key, &entry.Version, &entry.Payload.Inline,
			&entry.Payload.Blob, &entry.Deleted, &entry.UpdatedAt)
		return entry, err
	})
}
