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

// KVNamespacePolicy is trusted server configuration, never a request-body selector.
// Only exact reviewed namespaces can outlive sessions or cross subjects.
type KVNamespacePolicy struct {
	Tenant     string
	Deployment string
	Namespace  []string
	Owner      string // subject or deployment; absent policy means thread
}
type kvOwnerTarget struct {
	Scope contracts.Scope
	Kind  string
}
type kvPolicyQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}
type kvItemsQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func kvPolicyID(sc contracts.Scope, ns []string) string {
	return sc.Tenant + "\x00" + sc.Deployment + "\x00" + strings.Join(ns, "\x00")
}
func WithKVNamespacePolicies(policies []KVNamespacePolicy) (StoreOption, error) {
	if len(policies) > 64 {
		return nil, invalid("too many owner namespaces")
	}
	saved := make(map[string]KVNamespacePolicy, len(policies))
	for _, p := range policies {
		sc := contracts.Scope{Tenant: p.Tenant, Deployment: p.Deployment, Subject: "namespace-policy"}
		if err := sc.Validate(); err != nil {
			return nil, asInvalid(err)
		}
		if len(p.Namespace) == 0 {
			return nil, invalid("retained namespace must not be empty")
		}
		if err := validKVNamespace(p.Namespace); err != nil {
			return nil, err
		}
		if p.Owner != "subject" && p.Owner != "deployment" {
			return nil, invalid("unsupported namespace owner")
		}
		id := kvPolicyID(sc, p.Namespace)
		if _, ok := saved[id]; ok {
			return nil, invalid("duplicate namespace policy")
		}
		p.Namespace = append([]string(nil), p.Namespace...)
		saved[id] = p
	}
	return func(s *Store) {
		s.kvOwnerPolicies = make(map[string]KVNamespacePolicy, len(saved))
		for k, v := range saved {
			v.Namespace = append([]string(nil), v.Namespace...)
			s.kvOwnerPolicies[k] = v
		}
	}, nil
}
func (s *Store) kvOwner(sc contracts.Scope, ns []string) kvOwnerTarget {
	out := kvOwnerTarget{Scope: sc, Kind: "thread"}
	if p, ok := s.kvOwnerPolicies[kvPolicyID(sc, ns)]; ok {
		out.Kind = p.Owner
		if p.Owner == "deployment" {
			out.Scope.Subject = ""
		}
	}
	return out
}
func (s *Store) hasKVPolicy(sc contracts.Scope) bool {
	for _, p := range s.kvOwnerPolicies {
		if p.Tenant == sc.Tenant && p.Deployment == sc.Deployment {
			return true
		}
	}
	return false
}
func (s *Store) validateKVPolicyRows(ctx context.Context, q kvPolicyQuerier) error {
	for _, p := range s.kvOwnerPolicies {
		var incompatible bool
		if err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.kv
            WHERE tenant=$1 AND deployment=$2 AND namespace=$3
              AND (thread_id IS NOT NULL OR ($4='deployment' AND subject<>'')
                   OR ($4='subject' AND subject='')))`,
			p.Tenant, p.Deployment, p.Namespace, p.Owner).Scan(&incompatible); err != nil {
			return err
		}
		if incompatible {
			return invalid("namespace ownership requires an explicit data migration")
		}
	}
	return nil
}
func kvTakeOwnerLocks(ctx context.Context, tx pgx.Tx, actor, target contracts.Scope, op, item string) error {
	d, err := kvLockDigest(actor, "op\x00"+op)
	if err != nil {
		return err
	}
	if err = kvAdvisoryXactLock(ctx, tx, kvLockOpSeed, d); err != nil {
		return err
	}
	d, err = kvLockDigest(target, "item\x00"+item)
	if err != nil {
		return err
	}
	return kvAdvisoryXactLock(ctx, tx, kvLockItemSeed, d)
}
func kvOwnerRequestDigest(original string, owner kvOwnerTarget) (string, error) {
	if owner.Kind == "thread" {
		return original, nil
	} // Preserve existing default receipts.
	return contracts.Digest(map[string]any{"request": original, "owner": owner.Kind, "target": owner.Scope})
}
func (s *Store) putKVValue(ctx context.Context, tx pgx.Tx, lease *Lease, owner kvOwnerTarget,
	ns []string, key string, expected int64, stored string) (KVItem, error) {
	target := owner.Scope
	baseline, err := kvStoredVersion(ctx, tx, target, ns, key)
	if err != nil {
		return KVItem{}, err
	}
	current, err := s.getKVRow(ctx, tx, target, ns, key)
	switch {
	case err == nil:
		if expected == 0 || current.Version != expected {
			return KVItem{}, contracts.NewError(contracts.ErrConflict, "kv version conflict")
		}
	case contracts.Code(err) == contracts.ErrNotFound:
		if expected != 0 && expected != baseline {
			return KVItem{}, notFound("kv item")
		}
	default:
		return KVItem{}, err
	}
	var threadID *string
	if owner.Kind == "thread" {
		id := lease.Thread.ID
		threadID = &id
	}
	item := KVItem{Namespace: ns, Key: key, Version: baseline + 1}
	if threadID != nil {
		item.ThreadID = *threadID
	}
	var updated time.Time
	var raw []byte
	err = tx.QueryRow(ctx, `INSERT INTO ahp_state.kv
        (tenant,deployment,subject,namespace,key,value,version,deleted,thread_id,updated_at)
        VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,false,$8,clock_timestamp())
        ON CONFLICT(tenant,deployment,subject,namespace,key)
        DO UPDATE SET value=EXCLUDED.value,version=EXCLUDED.version,deleted=false,
                      thread_id=EXCLUDED.thread_id,updated_at=clock_timestamp()
        RETURNING updated_at,value::text`,
		target.Tenant, target.Deployment, target.Subject, ns, key, stored, item.Version, threadID).Scan(&updated, &raw)
	if err != nil {
		return KVItem{}, err
	}
	item.UpdatedAt = updated.UTC()
	item.Value = canonicalJSON(json.RawMessage(raw), nil)
	return item, nil
}
func queryKVItems(ctx context.Context, q kvItemsQuerier, query string, args ...any) ([]KVItem, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []KVItem{}
	for rows.Next() {
		item, e := scanKVItem(rows)
		if e != nil {
			return nil, e
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) kvSearchScope(sc contracts.Scope, args []any, arg int) (string, []any, int) {
	ids := []string{}
	for id, p := range s.kvOwnerPolicies {
		if p.Tenant == sc.Tenant && p.Deployment == sc.Deployment && p.Owner == "deployment" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return "subject=$3", args, arg
	}
	sort.Strings(ids)
	predicates := []string{}
	for _, id := range ids {
		predicates = append(predicates, "namespace=$"+itoa(arg)+"::text[]")
		args = append(args, s.kvOwnerPolicies[id].Namespace)
		arg++
	}
	ns := strings.Join(predicates, " OR ")
	return "((subject=$3 AND NOT(" + ns + ")) OR (subject='' AND (" + ns + ")))", args, arg
}
