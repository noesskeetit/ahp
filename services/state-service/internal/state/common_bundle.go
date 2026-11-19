package state

// Research-only coherent latest common cut + explicitly requested shared KV.
// Session advisory barriers are acquired BEFORE starting the RR snapshot; a
// tombstone committed during lock wait must not be hidden by a stale snapshot.
import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

type CommonBundle struct {
	Unchanged bool           `json:"common_unchanged,omitempty"`
	Receipt   *CommonReceipt `json:"receipt,omitempty"`
	Common    *CommonRead    `json:"common"`
	KV        []*KVItem      `json:"kv"`
}
type commonReadLock struct {
	key  string
	seed int64
}

func (s *Store) ReadRecordsBundle(ctx context.Context, actor contracts.Scope, thread, namespace string, keys []KVRead) (CommonBundle, error) {
	return s.ReadRecordsBundleSince(ctx, actor, thread, namespace, keys, 0)
}

// Since is an immutable-cut cache hint, never authority. Fresh shared owners
// are still read under the same pre-snapshot barriers and RR transaction.
func (s *Store) ReadRecordsBundleSince(ctx context.Context, actor contracts.Scope, thread, namespace string, keys []KVRead, known int64) (CommonBundle, error) {

	var out CommonBundle
	if !s.commonRecords {
		return out, invalid("common records option is disabled")
	}
	if err := actor.Validate(); err != nil {
		return out, err
	}
	if err := validIdentifier(thread, "thread id"); err != nil {
		return out, err
	}
	if err := commonNamespace(namespace); err != nil {
		return out, err
	}
	if known < 0 {
		return out, invalid("common known revision must be nonnegative")
	}
	query, args, err := s.prepareKVRead(actor, keys)
	if err != nil {
		return out, err
	}
	locks := []commonReadLock{{tenantFenceKey(actor.Tenant), 0}, {subjectFenceKey(actor), 0}}
	policies := map[string][]string{}
	for _, key := range keys {
		ns := normalizeKVNamespace(key.Namespace)
		policies[kvPolicyID(actor, ns)] = ns
	}
	ids := []string{}
	for id := range policies {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	namespaces := [][]string{}
	for _, id := range ids {
		locks = append(locks, commonReadLock{kvCatalogLockID(id), int64(kvPolicyLockSeed)})
		namespaces = append(namespaces, policies[id])
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return out, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		safe := true
		// Even a cancelled acquire may have succeeded just before the response was
		// lost. Unlock every requested key; false means it was never held.
		for i := len(locks) - 1; i >= 0; i-- {
			var held bool
			if e := conn.QueryRow(cleanup, `SELECT pg_advisory_unlock_shared(hashtextextended($1,$2))`, locks[i].key, locks[i].seed).Scan(&held); e != nil {
				safe = false
				break
			}
		}
		if !safe {
			_ = conn.Hijack().Close(cleanup)
		} else {
			conn.Release()
		}
	}()
	for _, lock := range locks {
		if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock_shared(hashtextextended($1,$2))`, lock.key, lock.seed); err != nil {
			return CommonBundle{}, classifyStoreError(err)
		}
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, err
	}
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		_ = tx.Rollback(c)
	}()
	if err = denyErasedSubject(ctx, tx, actor); err != nil {
		return out, err
	}
	if _, err = s.getThread(ctx, tx, actor, thread, ""); err != nil {
		return out, err
	}
	if err = s.checkKVPolicies(ctx, tx, actor, namespaces); err != nil {
		return out, err
	}
	var revision int64
	err = tx.QueryRow(ctx, `SELECT revision FROM ahp_state.common_heads WHERE thread_id=$1 AND namespace=$2`, thread, namespace).Scan(&revision)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	if err == nil {
		if known > 0 && known == revision {
			var raw []byte
			var format string
			if e := tx.QueryRow(ctx, `SELECT c.format,r.result FROM ahp_state.common_cuts c JOIN ahp_state.common_receipts r USING(thread_id,namespace,revision) WHERE c.thread_id=$1 AND c.namespace=$2 AND c.revision=$3`, thread, namespace, revision).Scan(&format, &raw); e != nil {
				return CommonBundle{}, e
			}
			if format == commonVersionLayout && !s.commonRecordVersions {
				return CommonBundle{}, contracts.NewError(contracts.ErrUnavailable, "versioned common reader is not configured")
			}
			var receipt CommonReceipt
			if e := json.Unmarshal(raw, &receipt); e != nil {
				return CommonBundle{}, e
			}
			if receipt.Revision != revision || receipt.Namespace != namespace {
				return CommonBundle{}, contracts.NewError(contracts.ErrUnavailable, "common receipt identity mismatch")
			}
			out.Unchanged = true
			out.Receipt = &receipt
		} else {
			read, e := s.readCommonCutTx(ctx, tx, actor, thread, namespace, revision)
			if e != nil {
				return CommonBundle{}, e
			}
			out.Common = &read
		}
	}
	out.KV, err = s.readPreparedKV(ctx, tx, keys, query, args)
	if err != nil {
		return CommonBundle{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return CommonBundle{}, classifyStoreError(err)
	}
	return out, nil
}
