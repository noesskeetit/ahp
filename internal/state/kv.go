package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// KV bounds, part of the wire contract.
const (
	kvMaxValueBytes  = 1 << 20
	kvMaxNamespaces  = 8
	kvMaxSegmentLen  = 128
	kvMaxKeyLen      = 256
	kvMaxSearchLimit = 1000
)

// Transaction advisory-lock namespaces for KV serialisation. A lock identity
// is hashtextextended(canonical digest, seed) where the seed encodes the lock
// kind, so operation locks and item locks can never collide.
const (
	kvLockOpSeed   = int64(47001) // (scope, operation key): idempotency serialisation
	kvLockItemSeed = int64(47002) // (scope, namespace, key): CAS serialisation
)

// KVItem is one versioned JSON document inside a scoped namespace.
type KVItem struct {
	Namespace []string        `json:"namespace"`
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Version   int64           `json:"version"`
	ThreadID  string          `json:"thread_id,omitempty"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// kvOpReceipt is the replayable result of one fenced KV mutation.
type kvOpReceipt struct {
	Kind          string   `json:"kind"`
	Deleted       bool     `json:"deleted"`
	Item          *KVItem  `json:"item,omitempty"`
	Items         []KVItem `json:"items,omitempty"`
	ActorThreadID string   `json:"actor_thread_id,omitempty"`
}

// normalizeKVNamespace maps nil to the empty namespace so a nil prefix always
// means "every namespace" instead of "no namespace".
func normalizeKVNamespace(ns []string) []string {
	if ns == nil {
		return []string{}
	}
	return ns
}

func validKVNamespace(ns []string) error {
	if len(ns) > kvMaxNamespaces {
		return invalid("namespace exceeds 8 segments")
	}
	for _, seg := range ns {
		if seg == "" || len(seg) > kvMaxSegmentLen || strings.IndexByte(seg, 0) >= 0 {
			return invalid("namespace segment must be 1..128 bytes without NUL")
		}
	}
	return nil
}

func validKVKey(key string) error {
	if key == "" || len(key) > kvMaxKeyLen || strings.IndexByte(key, 0) >= 0 {
		return invalid("key must be 1..256 bytes without NUL")
	}
	return nil
}

// canonicalKVValue validates, compacts and fingerprints a KV value. Duplicate
// JSON object keys are rejected via contracts.Digest.
func canonicalKVValue(value json.RawMessage) (stored string, digest string, err error) {
	if len(value) == 0 {
		return "", "", invalid("value must not be empty")
	}
	if len(value) > kvMaxValueBytes {
		return "", "", invalid("value exceeds 1 MiB")
	}
	digest, derr := contracts.Digest(value)
	if derr != nil {
		return "", "", asInvalid(derr)
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, value); err != nil {
		return "", "", invalid("value is not valid JSON")
	}
	if buf.Len() > kvMaxValueBytes {
		return "", "", invalid("value exceeds 1 MiB")
	}
	return buf.String(), digest, nil
}

func kvRequestDigest(op, key string, ns []string, expectedVersion int64, valueDigest string) (string, error) {
	return contracts.Digest(map[string]any{
		"op": op, "key": key, "namespace": ns,
		"expected_version": expectedVersion, "value": valueDigest,
	})
}

// kvLockDigest fingerprints the full scope plus one discriminator (an
// operation key or a namespace/key pair) into a stable lock identity.
func kvLockDigest(scope contracts.Scope, discriminator string) (string, error) {
	return contracts.Digest(map[string]any{
		"tenant":     scope.Tenant,
		"deployment": scope.Deployment,
		"subject":    scope.Subject,
		"target":     discriminator,
	})
}

// kvAdvisoryXactLock takes a transaction-scoped advisory lock whose key is
// hashtextextended over a canonical digest with the given kind seed.
func kvAdvisoryXactLock(ctx context.Context, tx pgx.Tx, seed int64, digest string) error {
	_, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, $2))`, digest, seed)
	return err
}

// kvTakeLocks takes the two KV advisory locks in a fixed order (operation key
// first, then item) so concurrent mutations of one scope serialise without
// ever deadlocking on each other.
func kvTakeLocks(ctx context.Context, tx pgx.Tx, scope contracts.Scope, operationKey, nsKeyDiscriminator string) error {
	opDigest, err := kvLockDigest(scope, "op\x00"+operationKey)
	if err != nil {
		return err
	}
	if err := kvAdvisoryXactLock(ctx, tx, kvLockOpSeed, opDigest); err != nil {
		return err
	}
	itemDigest, err := kvLockDigest(scope, "item\x00"+nsKeyDiscriminator)
	if err != nil {
		return err
	}
	return kvAdvisoryXactLock(ctx, tx, kvLockItemSeed, itemDigest)
}

const kvItemColumns = `namespace, key, value, version, thread_id, updated_at`

func scanKVItem(row scanner) (KVItem, error) {
	var (
		ns       []string
		key      string
		value    []byte
		version  int64
		threadID *string
		updated  time.Time
	)
	if err := row.Scan(&ns, &key, &value, &version, &threadID, &updated); err != nil {
		return KVItem{}, err
	}
	item := KVItem{
		Namespace: ns,
		Key:       key,
		// The stored row is the single source of truth for the bytes a client
		// sees: reads return the column without PostgreSQL's insignificant
		// whitespace, so GetKV, SearchKV and a PutKV replay cannot disagree.
		Value:     canonicalJSON(json.RawMessage(value), nil),
		Version:   version,
		UpdatedAt: updated.UTC(),
	}
	if threadID != nil {
		item.ThreadID = *threadID
	}
	return item, nil
}

// canonicalJSON compacts raw jsonb bytes for the wire. PostgreSQL renders
// jsonb with its own insignificant whitespace (and sorted object keys), so
// every read path funnels through here to keep responses byte-stable.
func canonicalJSON(raw, fallback json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		if fallback != nil {
			return fallback
		}
		return json.RawMessage{}
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		if fallback != nil {
			return fallback
		}
		return json.RawMessage(raw)
	}
	return buf.Bytes()
}

// getKVRow loads the live (non-tombstoned) row for (scope, namespace, key).
// tx may be nil, in which case the pool answers.
func (s *Store) getKVRow(ctx context.Context, tx pgx.Tx, scope contracts.Scope, ns []string, key string) (KVItem, error) {
	const q = `SELECT ` + kvItemColumns + ` FROM ahp_state.kv
		WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND namespace = $4 AND key = $5
		  AND deleted = false`
	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, q, scope.Tenant, scope.Deployment, scope.Subject, ns, key)
	} else {
		row = s.pool.QueryRow(ctx, q, scope.Tenant, scope.Deployment, scope.Subject, ns, key)
	}
	item, err := scanKVItem(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return KVItem{}, notFound("kv item")
	}
	if err != nil {
		return KVItem{}, err
	}
	return item, nil
}

// kvStoredVersion returns the stored version of a key including tombstones:
// the CAS baseline that survives a delete/recreate cycle. Absent keys have
// version 0.
func kvStoredVersion(ctx context.Context, tx pgx.Tx, scope contracts.Scope, ns []string, key string) (int64, error) {
	var version int64
	err := tx.QueryRow(ctx, `
		SELECT version FROM ahp_state.kv
		WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND namespace = $4 AND key = $5`,
		scope.Tenant, scope.Deployment, scope.Subject, ns, key).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return version, nil
}

// GetKV returns the current item for (scope, namespace, key). Foreign,
// missing or deleted items are reported NOT_FOUND so the API cannot probe
// other scopes.
func (s *Store) GetKV(ctx context.Context, scope contracts.Scope, namespace []string, key string) (KVItem, error) {
	if err := scope.Validate(); err != nil {
		return KVItem{}, asInvalid(err)
	}
	if err := validKVNamespace(namespace); err != nil {
		return KVItem{}, err
	}
	if err := validKVKey(key); err != nil {
		return KVItem{}, err
	}
	ns := normalizeKVNamespace(namespace)
	owner := s.kvOwner(scope, ns)
	var out KVItem
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := s.checkKVPolicies(ctx, tx, scope, [][]string{ns}); err != nil {
			return err
		}
		if owner.Kind != "thread" {
			if err := checkSubjectWritable(ctx, tx, scope); err != nil {
				return err
			}
		}
		var err error
		out, err = s.getKVRow(ctx, tx, owner.Scope, ns, key)
		return err
	})
	return out, err
}

// kvOpResult carries one replayable mutation outcome.
type kvOpResult struct {
	receipt kvOpReceipt
	digest  string
}

// kvOpReplay looks up the full idempotency receipt for (scope, operationKey).
// found=false means no receipt yet; err != nil means the caller must abort
// (KEY_REUSE_MISMATCH). The request digest pins op, key, namespace, expected
// version and value digest, so a matching digest cannot describe a different
// write; the receipt is still shape-checked so a corrupt or foreign-kind
// receipt never replays as a silent success.
func kvOpReplay(ctx context.Context, tx pgx.Tx, scope contracts.Scope, operationKey, requestDigest string) (kvOpResult, bool, error) {
	var (
		result []byte
		stored string
	)
	err := tx.QueryRow(ctx, `
		SELECT result, request_digest FROM ahp_state.kv_ops
		WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND operation_key = $4`,
		scope.Tenant, scope.Deployment, scope.Subject, operationKey).Scan(&result, &stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return kvOpResult{}, false, nil
	}
	if err != nil {
		return kvOpResult{}, false, err
	}
	if stored != requestDigest {
		return kvOpResult{}, false, contracts.NewError(contracts.ErrKeyReuse,
			"operation key was already used with a different kv request")
	}
	var rec kvOpReceipt
	if err := json.Unmarshal(result, &rec); err != nil {
		return kvOpResult{}, false, err
	}
	for i := range rec.Items {
		rec.Items[i].Value = canonicalJSON(rec.Items[i].Value, nil)
	}
	if rec.Item != nil && len(rec.Item.Value) > 0 {
		var buf bytes.Buffer
		if json.Compact(&buf, rec.Item.Value) == nil {
			rec.Item.Value = json.RawMessage(bytes.Clone(buf.Bytes()))
		}
	}
	switch {
	case rec.Kind == "put" && rec.Item != nil:
	case rec.Kind == "delete" && rec.Deleted:
	case rec.Kind == "batch" && len(rec.Items) > 0 && rec.ActorThreadID != "":
	default:
		return kvOpResult{}, false, contracts.NewError(contracts.ErrKeyReuse,
			"stored kv receipt does not match its request")
	}
	return kvOpResult{receipt: rec, digest: stored}, true, nil
}

// kvStoreReceipt writes the idempotency receipt. The operation advisory lock
// serialises concurrent attempts, so a DO NOTHING skip can only be a replay
// of the identical request; anything else is verified, never ignored.
func kvStoreReceipt(ctx context.Context, tx pgx.Tx, scope contracts.Scope, operationKey, requestDigest string, rec kvOpReceipt) error {
	encoded, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO ahp_state.kv_ops
			(tenant, deployment, subject, operation_key, request_digest, result)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		ON CONFLICT (tenant, deployment, subject, operation_key) DO NOTHING`,
		scope.Tenant, scope.Deployment, scope.Subject, operationKey, requestDigest, encoded)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var stored string
		if err := tx.QueryRow(ctx, `
			SELECT request_digest FROM ahp_state.kv_ops
			WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND operation_key = $4`,
			scope.Tenant, scope.Deployment, scope.Subject, operationKey).Scan(&stored); err != nil {
			return err
		}
		if stored != requestDigest {
			return contracts.NewError(contracts.ErrKeyReuse,
				"operation key was already used with a different kv request")
		}
	}
	return nil
}

// PutKV creates or CAS-updates one KV item under the current lease.
//
// expectedVersion 0 is create-only; any other value must equal the current
// stored version (tombstones included, so a delete/recreate cycle never
// re-hands-out a version) or the call fails CONFLICT without writing. Every
// successful version bump allocates old+1. The mutation and its full
// idempotency receipt commit atomically per (scope, operationKey): a replay
// returns the original result even after the item changed again, and a
// differing request under a reused key fails KEY_REUSE_MISMATCH. Concurrent
// writers of the same scope serialise on transaction advisory locks taken in
// a fixed order after the lease check.
func (s *Store) PutKV(ctx context.Context, scope contracts.Scope, fence Fence,
	namespace []string, key, operationKey string, expectedVersion int64, value json.RawMessage) (KVItem, error) {
	if err := validKVNamespace(namespace); err != nil {
		return KVItem{}, err
	}
	if err := validKVKey(key); err != nil {
		return KVItem{}, err
	}
	if err := validIdentifier(operationKey, "operation key"); err != nil {
		return KVItem{}, err
	}
	if expectedVersion < 0 {
		return KVItem{}, invalid("expected version must not be negative")
	}
	ns := normalizeKVNamespace(namespace)
	stored, valueDigest, err := canonicalKVValue(value)
	if err != nil {
		return KVItem{}, err
	}
	requestDigest, err := kvRequestDigest("put", key, ns, expectedVersion, valueDigest)
	if err != nil {
		return KVItem{}, asInvalid(err)
	}
	owner := s.kvOwner(scope, ns)
	requestDigest, err = kvOwnerRequestDigest(requestDigest, owner)
	if err != nil {
		return KVItem{}, asInvalid(err)
	}
	itemTarget := key + "\x00" + strings.Join(ns, "\x00")

	var out KVItem
	err = s.withLease(ctx, scope, fence.RunID, fence.Worker, fence.Epoch, func(tx pgx.Tx, l *Lease) error {
		if err := s.checkKVPolicies(ctx, tx, scope, [][]string{ns}); err != nil {
			return err
		}
		if err := kvTakeOwnerLocks(ctx, tx, scope, owner.Scope, operationKey, itemTarget); err != nil {
			return err
		}
		res, found, err := kvOpReplay(ctx, tx, scope, operationKey, requestDigest)
		if err != nil {
			return err
		}
		if found {
			if res.receipt.Kind != "put" || res.receipt.Item == nil {
				return contracts.NewError(contracts.ErrKeyReuse,
					"operation key was already used with a different kv request")
			}
			out = *res.receipt.Item
			return nil
		}

		item, err := s.putKVValue(ctx, tx, l, owner, ns, key, expectedVersion, stored)
		if err != nil {
			return err
		}
		rec := kvOpReceipt{Kind: "put", Item: &item}
		if owner.Kind != "thread" {
			rec.ActorThreadID = l.Thread.ID
		}

		if err := kvStoreReceipt(ctx, tx, scope, operationKey, requestDigest,
			rec); err != nil {
			return err
		}
		out = item
		return nil
	})
	if err != nil {
		return KVItem{}, err
	}
	return out, nil
}

// DeleteKV tombstones one KV item under the current lease with the same CAS
// and full idempotency semantics as PutKV. expectedVersion must equal the
// current version; deleting an absent item is NOT_FOUND and a replay of the
// original delete returns success from its receipt. The tombstone keeps the
// version so a later create-only write continues from old+1.
func (s *Store) DeleteKV(ctx context.Context, scope contracts.Scope, fence Fence,
	namespace []string, key, operationKey string, expectedVersion int64) error {
	if err := validKVNamespace(namespace); err != nil {
		return err
	}
	if err := validKVKey(key); err != nil {
		return err
	}
	if err := validIdentifier(operationKey, "operation key"); err != nil {
		return err
	}
	if expectedVersion < 1 {
		return invalid("expected version must be positive for delete")
	}
	ns := normalizeKVNamespace(namespace)
	requestDigest, err := kvRequestDigest("delete", key, ns, expectedVersion, "")
	if err != nil {
		return asInvalid(err)
	}
	owner := s.kvOwner(scope, ns)
	requestDigest, err = kvOwnerRequestDigest(requestDigest, owner)
	if err != nil {
		return asInvalid(err)
	}
	itemTarget := key + "\x00" + strings.Join(ns, "\x00")
	return s.withLease(ctx, scope, fence.RunID, fence.Worker, fence.Epoch, func(tx pgx.Tx, l *Lease) error {
		if err := s.checkKVPolicies(ctx, tx, scope, [][]string{ns}); err != nil {
			return err
		}
		if err := kvTakeOwnerLocks(ctx, tx, scope, owner.Scope, operationKey, itemTarget); err != nil {
			return err
		}
		res, found, err := kvOpReplay(ctx, tx, scope, operationKey, requestDigest)
		if err != nil {
			return err
		}
		if found {
			if res.receipt.Kind != "delete" {
				return contracts.NewError(contracts.ErrKeyReuse,
					"operation key was already used with a different kv request")
			}
			if _, itemErr := s.getKVRow(ctx, tx, owner.Scope, ns, key); itemErr == nil {
				return contracts.NewError(contracts.ErrKeyReuse,
					"operation key receipt no longer matches item state")
			}
			return nil
		}

		current, err := s.getKVRow(ctx, tx, owner.Scope, ns, key)
		if err != nil {
			return err
		}
		if current.Version != expectedVersion {
			return contracts.NewError(contracts.ErrConflict, "kv version conflict")
		}
		tag, err := tx.Exec(ctx, `
			UPDATE ahp_state.kv
			SET deleted = true, value = 'null'::jsonb, updated_at = clock_timestamp()
			WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND namespace = $4 AND key = $5
			  AND deleted = false AND version = $6`,
			owner.Scope.Tenant, owner.Scope.Deployment, owner.Scope.Subject, ns, key, expectedVersion)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return notFound("kv item")
		}
		tombstone := current
		tombstone.Value = nil
		rec := kvOpReceipt{Kind: "delete", Deleted: true, Item: &tombstone}
		if owner.Kind != "thread" {
			rec.ActorThreadID = l.Thread.ID
		}
		return kvStoreReceipt(ctx, tx, scope, operationKey, requestDigest, rec)
	})
}

// SearchKV lists live items of one scope whose namespace starts with
// namespacePrefix (ordered structural prefix, not unordered containment) and
// whose key starts with keyPrefix as a literal string (SQL LIKE wildcards
// carry no special meaning here), optionally narrowed by a JSONB containment
// filter. limit is clamped to 1..1000 and results come back in stable
// (namespace, key) order. This is exact structural search only; no semantic
// matching is performed.
func (s *Store) SearchKV(ctx context.Context, scope contracts.Scope,
	namespacePrefix []string, keyPrefix string, filter json.RawMessage, limit int) ([]KVItem, error) {
	if err := scope.Validate(); err != nil {
		return nil, asInvalid(err)
	}
	if err := validKVNamespace(namespacePrefix); err != nil {
		return nil, err
	}
	if len(keyPrefix) > kvMaxKeyLen || strings.IndexByte(keyPrefix, 0) >= 0 {
		return nil, invalid("key prefix must be at most 256 bytes without NUL")
	}
	if limit < 1 {
		limit = 1
	}
	if limit > kvMaxSearchLimit {
		limit = kvMaxSearchLimit
	}
	ns := normalizeKVNamespace(namespacePrefix)
	argNum := 5
	args := []any{scope.Tenant, scope.Deployment, scope.Subject, ns}
	ownerClause, args, argNum := s.kvSearchScope(scope, args, argNum)
	// Ordered prefix match: every prefix segment must equal the stored
	// namespace at the same position; the empty prefix matches everything.
	query := `SELECT ` + kvItemColumns + ` FROM ahp_state.kv
		WHERE tenant = $1 AND deployment = $2 AND (` + ownerClause + `) AND deleted = false
		  AND ($4::text[] = '{}'::text[]
		       OR (cardinality(namespace) >= cardinality($4::text[])
		           AND namespace[1:cardinality($4::text[])] = $4::text[]))`
	if keyPrefix != "" {
		query += ` AND key LIKE $` + itoa(argNum) + ` ESCAPE '\'`
		args = append(args, escapeLiteralLike(keyPrefix))
		argNum++
	}
	if len(filter) > 0 {
		if !json.Valid(filter) || len(filter) > kvMaxValueBytes {
			return nil, invalid("filter must be valid JSON of at most 1 MiB")
		}
		query += ` AND value @> $` + itoa(argNum) + `::jsonb`
		args = append(args, string(filter))
		argNum++
	}
	query += ` ORDER BY namespace, key LIMIT $` + itoa(argNum)
	args = append(args, limit)

	var out []KVItem
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := s.checkKVSearchPolicies(ctx, tx, scope); err != nil {
			return err
		}
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		var err error
		out, err = queryKVItems(ctx, tx, query, args...)
		return err
	})
	return out, err
}

// escapeLiteralLike renders a caller key prefix as a literal LIKE pattern:
// the SQL wildcards %, _ and the escape character \ are backslash-escaped so
// they match themselves.
func escapeLiteralLike(prefix string) string {
	var b strings.Builder
	for i := 0; i < len(prefix); i++ {
		switch prefix[i] {
		case '%', '_', '\\':
			b.WriteByte('\\')
		}
		b.WriteByte(prefix[i])
	}
	return b.String() + "%"
}

func itoa(n int) string { return strconv.Itoa(n) }
