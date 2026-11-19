package statejournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"

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
	if !validID(namespace) || namespace == kvReceiptNamespace {
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
	Keys      []string
	Prefix    bool
	Limit     int
	Offset    int
}

func (s *Store) ListKV(ctx context.Context, scope contracts.Scope, query KVQuery) ([]KVEntry, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if !validID(query.Namespace) || query.Namespace == kvReceiptNamespace || (query.Key != "" && !validID(query.Key)) || query.Limit < 1 || query.Limit > 1000 ||
		query.Offset < 0 || query.Offset > 10000 || (query.Prefix && query.ScopeType != "store") {
		return nil, contracts.NewError(contracts.ErrInvalid, "invalid scoped KV query")
	}
	keys := slices.Clone(query.Keys)
	if query.Key != "" {
		keys = append(keys, query.Key)
	}
	if len(keys) > 256 {
		return nil, contracts.NewError(contracts.ErrInvalid, "cannot request more than 256 KV keys")
	}
	for _, key := range keys {
		if !validID(key) {
			return nil, contracts.NewError(contracts.ErrInvalid, "invalid KV key filter")
		}
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
		  AND ($9::text[] IS NULL OR key=ANY($9))
		ORDER BY namespace,key LIMIT $7 OFFSET $8`,
		scope.Tenant, scope.Deployment, query.ScopeType, owner, query.Namespace,
		query.Prefix, query.Limit, query.Offset, keys)
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

// This namespace is private to the session and purged by ordinary retention.
const kvReceiptNamespace = "ahp:kv-receipts"

type kvReceipt struct {
	Digest [sha256.Size]byte
	Result AppendReceipt
}

type KVBatch struct {
	Key         string
	ExpectedSeq int64
	KV          []KVMutation
	Catalog     *OpenClawCatalogUpdate
}

// MutateKV applies a fenced CAS batch without allocating a journal sequence.
// A receipt in reserved session KV makes retries durable without another table.
func (s *Store) MutateKV(ctx context.Context, scope contracts.Scope, session string, lease Lease, shared bool, mutation KVBatch) (AppendReceipt, error) {
	key, expectedSeq, changes := mutation.Key, mutation.ExpectedSeq, mutation.KV
	request := AppendRequest{Key: key, ExpectedSeq: expectedSeq, KV: changes, Catalog: mutation.Catalog}
	if err := scope.Validate(); err != nil {
		return AppendReceipt{}, err
	}
	if err := validateAppend(session, lease, shared, request); err != nil {
		return AppendReceipt{}, err
	}
	wire, err := json.Marshal(request)
	if err != nil {
		return AppendReceipt{}, err
	}
	digest := sha256.Sum256(wire)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AppendReceipt{}, err
	}
	defer tx.Rollback(ctx)
	head, err := lockSession(ctx, tx, scope, session, lease)
	if err != nil {
		return AppendReceipt{}, err
	}
	var stored []byte
	err = tx.QueryRow(ctx, `
        SELECT inline_payload FROM ahp_journal.scoped_kv
        WHERE tenant_id=$1 AND deployment_id=$2 AND scope_type='session'
          AND scope_owner=$3 AND namespace=$4 AND key=$5`,
		scope.Tenant, scope.Deployment, session, kvReceiptNamespace, key).Scan(&stored)
	if err == nil {
		var previous kvReceipt
		if err := json.Unmarshal(stored, &previous); err != nil {
			return AppendReceipt{}, err
		}
		if previous.Digest != digest {
			return AppendReceipt{}, contracts.NewError(contracts.ErrKeyReuse, "KV mutation key has a different digest")
		}
		return previous.Result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return AppendReceipt{}, err
	}
	// Accept matching receipts produced by the previous kv:mutate route.
	var legacy AppendReceipt
	err = tx.QueryRow(ctx, `
        SELECT seq,result_head_seq,request_digest FROM ahp_journal.session_events
        WHERE tenant_id=$1 AND session_id=$2 AND operation_key=$3`,
		scope.Tenant, session, key).Scan(&legacy.FirstSeq, &legacy.HeadSeq, &stored)
	if err == nil {
		if !bytes.Equal(stored, digest[:]) {
			return AppendReceipt{}, contracts.NewError(contracts.ErrKeyReuse, "operation key has a different digest")
		}
		return legacy, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return AppendReceipt{}, err
	}
	if expectedSeq > 0 && head != expectedSeq {
		return AppendReceipt{}, contracts.NewError(contracts.ErrConflict, "session head has changed")
	}
	if err := s.pinBlobs(ctx, tx, scope, session, appendBlobs(request)); err != nil {
		return AppendReceipt{}, err
	}
	if err := applyKV(ctx, tx, scope, session, changes); err != nil {
		return AppendReceipt{}, err
	}
	if request.Catalog != nil {
		if err := updateCatalog(ctx, tx, scope, session, *request.Catalog); err != nil {
			return AppendReceipt{}, err
		}
	}
	receipt := AppendReceipt{HeadSeq: head}
	encoded, err := json.Marshal(kvReceipt{Digest: digest, Result: receipt})
	if err != nil {
		return AppendReceipt{}, err
	}
	if err := putKV(ctx, tx, scope, session, KVMutation{
		ScopeType: "session", Namespace: kvReceiptNamespace, Key: key, Payload: Payload{Inline: encoded},
	}); err != nil {
		return AppendReceipt{}, err
	}
	return receipt, tx.Commit(ctx)
}
