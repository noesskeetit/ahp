package state

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// ExpectedOwner checks trusted namespace configuration; it cannot select a scope.
type KVPut struct {
	Namespace       []string        `json:"namespace"`
	Key             string          `json:"key"`
	ExpectedOwner   string          `json:"expected_owner"`
	ExpectedVersion int64           `json:"expected_version"`
	Value           json.RawMessage `json:"value"`
}
type KVBatchRequest struct {
	OperationKey string  `json:"operation_key"`
	Items        []KVPut `json:"items"`
}
type KVBatchResult struct {
	Items []KVItem `json:"items"`
}
type KVRead struct {
	Namespace     []string `json:"namespace"`
	Key           string   `json:"key"`
	ExpectedOwner string   `json:"expected_owner"`
}
type kvPrepared struct {
	put    KVPut
	owner  kvOwnerTarget
	stored string
	digest string
	lock   string
}

func (s *Store) kvExpectedOwner(actor contracts.Scope, ns []string, expected string) (kvOwnerTarget, error) {
	owner := s.kvOwner(actor, ns)
	if expected == "" {
		expected = "thread"
	}
	if expected != owner.Kind {
		return owner, contracts.NewError(contracts.ErrForbidden, "namespace owner policy does not match the requested precondition")
	}
	return owner, nil
}

// prepareKVPuts is shared by KV batches and atomic common-cut publication.
func (s *Store) prepareKVPuts(actor contracts.Scope, puts []KVPut) ([]kvPrepared, []any, error) {
	if len(puts) < 1 || len(puts) > 8 {
		return nil, nil, invalid("KV batch must contain 1..8 items")
	}
	raw, err := json.Marshal(puts)
	if err != nil || len(raw) > 4<<20 {
		return nil, nil, invalid("KV updates exceed JSON bound")
	}
	items := make([]kvPrepared, 0, len(puts))
	seen := map[string]bool{}
	digests := []any{}
	for _, put := range puts {
		if err = validKVNamespace(put.Namespace); err != nil {
			return nil, nil, err
		}
		if err = validKVKey(put.Key); err != nil {
			return nil, nil, err
		}
		if put.ExpectedVersion < 0 {
			return nil, nil, invalid("expected version must not be negative")
		}
		ns := normalizeKVNamespace(put.Namespace)
		put.Namespace = ns
		owner, e := s.kvExpectedOwner(actor, ns, put.ExpectedOwner)
		if e != nil {
			return nil, nil, e
		}
		stored, d, e := canonicalKVValue(put.Value)
		if e != nil {
			return nil, nil, e
		}
		lock, e := kvLockDigest(owner.Scope, "item\x00"+put.Key+"\x00"+strings.Join(ns, "\x00"))
		if e != nil {
			return nil, nil, e
		}
		if seen[lock] {
			return nil, nil, invalid("duplicate KV batch target")
		}
		seen[lock] = true
		items = append(items, kvPrepared{put: put, owner: owner, stored: stored, digest: d, lock: lock})
		digests = append(digests, map[string]any{"namespace": ns, "key": put.Key, "expected_version": put.ExpectedVersion, "owner": owner.Kind, "target": owner.Scope, "value_digest": d})
	}
	return items, digests, nil
}

// applyPreparedKV must run inside the caller's existing fenced transaction.
func (s *Store) applyPreparedKV(ctx context.Context, tx pgx.Tx, l *Lease, items []kvPrepared) ([]KVItem, error) {
	locks := make([]string, 0, len(items))
	for _, item := range items {
		locks = append(locks, item.lock)
	}
	sort.Strings(locks)
	for _, lock := range locks {
		if err := kvAdvisoryXactLock(ctx, tx, kvLockItemSeed, lock); err != nil {
			return nil, err
		}
	}
	values := make([]KVItem, 0, len(items))
	for _, item := range items {
		value, err := s.putKVValue(ctx, tx, l, item.owner, item.put.Namespace, item.put.Key, item.put.ExpectedVersion, item.stored)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (s *Store) PutKVBatch(ctx context.Context, actor contracts.Scope, fence Fence, req KVBatchRequest) (KVBatchResult, error) {
	var out KVBatchResult
	if err := actor.Validate(); err != nil {
		return out, asInvalid(err)
	}
	if err := validIdentifier(req.OperationKey, "operation key"); err != nil {
		return out, err
	}
	if len(req.Items) < 1 || len(req.Items) > 8 {
		return out, invalid("KV batch must contain 1..8 items")
	}
	encoded, err := json.Marshal(req)
	if err != nil || len(encoded) > 4<<20 {
		return out, invalid("KV batch exceeds its JSON bound")
	}
	items, digests, err := s.prepareKVPuts(actor, req.Items)
	if err != nil {
		return out, err
	}

	err = s.withLease(ctx, actor, fence.RunID, fence.Worker, fence.Epoch, func(tx pgx.Tx, l *Lease) error {
		namespaces := make([][]string, 0, len(items))
		for _, item := range items {
			namespaces = append(namespaces, item.put.Namespace)
		}
		if e := s.checkKVPolicies(ctx, tx, actor, namespaces); e != nil {
			return e
		}
		requestDigest, e := contracts.Digest(map[string]any{"operation": "put_batch", "thread": l.Thread.ID, "items": digests})
		if e != nil {
			return asInvalid(e)
		}
		op, e := kvLockDigest(actor, "op\x00"+req.OperationKey)
		if e != nil {
			return e
		}
		if e = kvAdvisoryXactLock(ctx, tx, kvLockOpSeed, op); e != nil {
			return e
		}
		previous, found, e := kvOpReplay(ctx, tx, actor, req.OperationKey, requestDigest)
		if e != nil {
			return e
		}
		if found {
			if previous.receipt.Kind != "batch" || previous.receipt.ActorThreadID != l.Thread.ID || len(previous.receipt.Items) != len(items) {
				return contracts.NewError(contracts.ErrKeyReuse, "receipt is not this KV batch")
			}
			out.Items = previous.receipt.Items
			return nil
		}
		values, e := s.applyPreparedKV(ctx, tx, l, items)
		if e != nil {
			return e
		}

		if e = kvStoreReceipt(ctx, tx, actor, req.OperationKey, requestDigest, kvOpReceipt{Kind: "batch", Items: values, ActorThreadID: l.Thread.ID}); e != nil {
			return e
		}
		out.Items = values
		return nil
	})
	if err != nil {
		return KVBatchResult{}, err
	}
	return out, nil
}

// GetKVBatch returns one statement snapshot. Subject-control locks are acquired
// under Read Committed first: an erasure committed during lock wait must be seen.
func (s *Store) prepareKVRead(actor contracts.Scope, keys []KVRead) (string, []any, error) {
	if err := actor.Validate(); err != nil {
		return "", nil, asInvalid(err)
	}
	if len(keys) < 1 || len(keys) > 8 {
		return "", nil, invalid("KV read batch must contain 1..8 keys")
	}
	args := []any{actor.Tenant, actor.Deployment}
	wanted := []string{}
	arg := 3
	for i, key := range keys {
		if err := validKVNamespace(key.Namespace); err != nil {
			return "", nil, err
		}
		if err := validKVKey(key.Key); err != nil {
			return "", nil, err
		}
		owner, err := s.kvExpectedOwner(actor, normalizeKVNamespace(key.Namespace), key.ExpectedOwner)
		if err != nil {
			return "", nil, err
		}
		wanted = append(wanted, "("+itoa(i)+",$"+itoa(arg)+"::text,$"+itoa(arg+1)+"::text[],$"+itoa(arg+2)+"::text)")
		args = append(args, owner.Scope.Subject, normalizeKVNamespace(key.Namespace), key.Key)
		arg += 3
	}
	query := `WITH wanted(ord,subject,namespace,key) AS (VALUES ` + strings.Join(wanted, ",") + `)
        SELECT w.ord,k.namespace,k.key,k.value,k.version,k.thread_id,k.updated_at
        FROM wanted w LEFT JOIN ahp_state.kv k ON
        k.tenant=$1 AND k.deployment=$2 AND k.subject=w.subject
        AND k.namespace=w.namespace AND k.key=w.key AND k.deleted=false ORDER BY w.ord`
	return query, args, nil
}
func (s *Store) readPreparedKV(ctx context.Context, tx pgx.Tx, keys []KVRead, query string, args []any) ([]*KVItem, error) {
	out := make([]*KVItem, len(keys))
	err := func() error {
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var ord int
			var ns []string
			var key, thread *string
			var raw []byte
			var version *int64
			var updated *time.Time
			if err = rows.Scan(&ord, &ns, &key, &raw, &version, &thread, &updated); err != nil {
				return err
			}
			if version == nil {
				continue
			}
			if ord < 0 || ord >= len(out) || key == nil || updated == nil {
				return invalid("invalid KV batch read row")
			}
			item := &KVItem{Namespace: ns, Key: *key, Value: canonicalJSON(json.RawMessage(raw), nil), Version: *version, UpdatedAt: updated.UTC()}
			if thread != nil {
				item.ThreadID = *thread
			}
			out[ord] = item
		}
		return rows.Err()
	}()
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) GetKVBatch(ctx context.Context, actor contracts.Scope, keys []KVRead) ([]*KVItem, error) {
	query, args, err := s.prepareKVRead(actor, keys)
	if err != nil {
		return nil, err
	}
	var out []*KVItem
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, actor); err != nil {
			return err
		}
		namespaces := make([][]string, 0, len(keys))
		for _, key := range keys {
			namespaces = append(namespaces, key.Namespace)
		}
		if err := s.checkKVPolicies(ctx, tx, actor, namespaces); err != nil {
			return err
		}
		var err error
		out, err = s.readPreparedKV(ctx, tx, keys, query, args)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
