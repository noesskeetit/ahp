package compute

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// LeaseService is the Redis-backed lease layer of the Compute plane. It holds
// the two lease families the PDF 3.6 Computer model coordinates over Redis:
//
//   - the exclusive WRITER lease on a Computer's working tree (B50): a command
//     whose writable file set is unknown takes it; a second holder is refused
//     with CONFLICT until the first expires or releases;
//   - the ACTIVITY leases (B52): one per (computer, kind, holder) for running
//     commands, PTYs, browser workers and exports. Idle is defined by the
//     absence of any unexpired activity lease, not by an open tab.
//
// Every operation first validates that the Computer exists in the caller's
// scope through the durable store, so a lease can never be minted for a
// Computer the caller does not own. Lease state is ephemeral by design (TTL
// keys); the durable ledger in PostgreSQL remains the audit of record.
type LeaseService struct {
	store *Store
	redis *Redis
	now   func() time.Time
}

// NewLeaseService wires the lease layer. redis must not be nil; a nil store
// fails closed.
func NewLeaseService(store *Store, redis *Redis) (*LeaseService, error) {
	if store == nil {
		return nil, invalid("compute: lease service requires a store")
	}
	if redis == nil {
		return nil, invalid("compute: lease service requires a redis client")
	}
	return &LeaseService{store: store, redis: redis, now: time.Now}, nil
}

// LeaseInfo is one observed lease.
type LeaseInfo struct {
	Kind   ActivityLeaseKind `json:"kind"`
	Holder string            `json:"holder"`
	TTL    int64             `json:"ttl_ms"`
}

func (l *LeaseService) activityKey(computerID, kind, holder string) string {
	return RedisKeyPrefix + "act:" + computerID + ":" + string(kind) + ":" + holder
}

func (l *LeaseService) activityIndexKey(computerID string) string {
	return RedisKeyPrefix + "actidx:" + computerID
}

func (l *LeaseService) writerKey(computerID string) string {
	return RedisKeyPrefix + "writer:" + computerID
}

// AcquireActivity takes (or refreshes) an activity lease for (kind, holder).
// The same holder re-acquiring the same kind extends its own lease; a
// different holder takes a distinct slot, which is what lets several chats
// share one Computer in parallel.
func (l *LeaseService) AcquireActivity(ctx context.Context, scope contracts.Scope, computerID string, kind ActivityLeaseKind, holder string, ttl time.Duration) (string, error) {
	tx, err := l.store.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err = l.store.checkOwnerWritable(ctx, tx, scope); err != nil {
		return "", err
	}
	if !kind.Valid() || kind == LeaseWriter {
		return "", invalid("activity lease kind must be one of command, pty, browser, export")
	}
	if err := validIdentifier(holder, "lease holder"); err != nil {
		return "", err
	}
	if ttl < minLeaseTTL || ttl > maxLeaseTTL {
		return "", invalid("lease ttl must be between 1s and 12h")
	}
	if _, err := l.store.getComputer(ctx, tx, scope, computerID); err != nil {
		return "", err
	}
	member := string(kind) + ":" + holder
	id := leaseID()
	ok, err := l.redis.SetNX(ctx, l.activityKey(computerID, string(kind), holder), id, ttl.Milliseconds())
	if err != nil {
		return "", err
	}
	if !ok {
		// The holder already owns this slot: refresh it.
		if err := l.redis.PExpire(ctx, l.activityKey(computerID, string(kind), holder), ttl.Milliseconds()); err != nil {
			return "", err
		}
	}
	if err := l.redis.SAdd(ctx, l.activityIndexKey(computerID), member); err != nil {
		return "", err
	}
	// The index set outlives individual leases on purpose: stale members are
	// dropped lazily by ActiveActivityLeases when their keys have expired.
	if err := l.redis.PExpire(ctx, l.activityIndexKey(computerID), ttl.Milliseconds()+time.Hour.Milliseconds()); err != nil {
		return "", err
	}
	return id, nil
}

// ReleaseActivity drops the holder's activity lease for kind.
func (l *LeaseService) ReleaseActivity(ctx context.Context, scope contracts.Scope, computerID string, kind ActivityLeaseKind, holder string) error {
	if !kind.Valid() || kind == LeaseWriter {
		return invalid("activity lease kind must be one of command, pty, browser, export")
	}
	if _, err := l.store.GetComputer(ctx, scope, computerID); err != nil {
		return err
	}
	if err := l.redis.Del(ctx, l.activityKey(computerID, string(kind), holder)); err != nil {
		return err
	}
	return l.redis.SRem(ctx, l.activityIndexKey(computerID), string(kind)+":"+holder)
}

// ActiveActivityLeases lists the unexpired activity leases of a Computer.
// Stale index members (expired keys) are pruned as a side effect.
func (l *LeaseService) ActiveActivityLeases(ctx context.Context, scope contracts.Scope, computerID string) ([]LeaseInfo, error) {
	if _, err := l.store.GetComputer(ctx, scope, computerID); err != nil {
		return nil, err
	}
	return l.activeActivityLeases(ctx, computerID)
}

// activeActivityLeases is for callers that already validated ownership on their transaction.
func (l *LeaseService) activeActivityLeases(ctx context.Context, computerID string) ([]LeaseInfo, error) {
	members, err := l.redis.SMembers(ctx, l.activityIndexKey(computerID))
	if err != nil {
		return nil, err
	}
	out := make([]LeaseInfo, 0, len(members))
	var stale []string
	for _, m := range members {
		parts := strings.SplitN(m, ":", 2)
		if len(parts) != 2 {
			stale = append(stale, m)
			continue
		}
		kind, holder := ActivityLeaseKind(parts[0]), parts[1]
		if kind == LeaseWriter || !kind.Valid() {
			stale = append(stale, m)
			continue
		}
		val, err := l.redis.Get(ctx, l.activityKey(computerID, string(kind), holder))
		if err != nil {
			return nil, err
		}
		if val == "" {
			stale = append(stale, m)
			continue
		}
		out = append(out, LeaseInfo{Kind: kind, Holder: holder})
	}
	if len(stale) > 0 {
		_ = l.redis.SRem(ctx, l.activityIndexKey(computerID), stale...)
	}
	return out, nil
}

// AcquireWriter takes the exclusive writer lease on a Computer's working tree.
// A live lease held by another holder is refused with CONFLICT carrying the
// current holder; the same holder re-acquiring extends its own lease.
func (l *LeaseService) AcquireWriter(ctx context.Context, scope contracts.Scope, computerID, holder string, ttl time.Duration) (string, error) {
	tx, err := l.store.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err = l.store.checkOwnerWritable(ctx, tx, scope); err != nil {
		return "", err
	}
	if err := validIdentifier(holder, "writer holder"); err != nil {
		return "", err
	}
	if ttl < minLeaseTTL || ttl > maxLeaseTTL {
		return "", invalid("lease ttl must be between 1s and 12h")
	}
	if _, err := l.store.getComputer(ctx, tx, scope, computerID); err != nil {
		return "", err
	}
	key := l.writerKey(computerID)
	ok, err := l.redis.SetNX(ctx, key, holder, ttl.Milliseconds())
	if err != nil {
		return "", err
	}
	if ok {
		return leaseID(), nil
	}
	current, err := l.redis.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if current == holder {
		// Re-acquire by the same holder: extend, no contention.
		if err := l.redis.PExpire(ctx, key, ttl.Milliseconds()); err != nil {
			return "", err
		}
		return leaseID(), nil
	}
	return "", contracts.NewError(contracts.ErrConflict,
		fmt.Sprintf("the working tree is held by writer lease of %q", current))
}

// ReleaseWriter drops the writer lease if holder still owns it.
func (l *LeaseService) ReleaseWriter(ctx context.Context, scope contracts.Scope, computerID, holder string) error {
	if _, err := l.store.GetComputer(ctx, scope, computerID); err != nil {
		return err
	}
	key := l.writerKey(computerID)
	current, err := l.redis.Get(ctx, key)
	if err != nil {
		return err
	}
	if current == "" || current != holder {
		return nil // expired or held by someone else: nothing to release
	}
	return l.redis.Del(ctx, key)
}

// HoldingCount returns the number of leases (activity plus writer) currently
// keeping a Computer awake. Zero is the sleep gate: an idle Computer with no
// holding lease may sleep.
func (l *LeaseService) HoldingCount(ctx context.Context, scope contracts.Scope, computerID string) (int, error) {
	if _, err := l.store.GetComputer(ctx, scope, computerID); err != nil {
		return 0, err
	}
	return l.holdingCount(ctx, computerID)
}
func (l *LeaseService) holdingCount(ctx context.Context, computerID string) (int, error) {
	leases, err := l.activeActivityLeases(ctx, computerID)
	if err != nil {
		return 0, err
	}
	n := len(leases)
	if v, err := l.redis.Get(ctx, l.writerKey(computerID)); err != nil {
		return 0, err
	} else if v != "" {
		n++
	}
	return n, nil
}
