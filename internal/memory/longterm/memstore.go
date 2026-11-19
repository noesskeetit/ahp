package memory

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// MemoryStore is for tests and the explicit development mode only.
// JSON round trips enforce the same copy and rollback semantics as PostgreSQL.
type MemoryStore struct {
	mu         sync.Mutex
	objects    map[string]map[string]json.RawMessage
	due        map[string]time.Time
	identities map[string]Identity
	frozen     map[string]bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: map[string]map[string]json.RawMessage{}, due: map[string]time.Time{}, identities: map[string]Identity{}, frozen: map[string]bool{}}
}

type memTx struct {
	owner      string
	identities map[string]Identity
	frozen     map[string]bool
	objects    map[string]json.RawMessage
	due        time.Time
}

func (m *MemoryStore) Within(ctx context.Context, owner string, fn func(Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t := &memTx{owner: owner, identities: map[string]Identity{}, frozen: m.frozen, objects: map[string]json.RawMessage{}, due: m.due[owner]}
	for k, v := range m.identities {
		t.identities[k] = v
	}
	for k, v := range m.objects[owner] {
		t.objects[k] = append(json.RawMessage(nil), v...)
	}
	if err := fn(t); err != nil {
		return err
	}
	m.objects[owner] = t.objects
	m.due[owner] = t.due
	m.identities = t.identities
	return nil
}
func (m *MemoryStore) FenceTenant(ctx context.Context, tenant string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frozen[tenant] = true
	return nil
}
func (m *MemoryStore) TenantOwners(ctx context.Context, tenant string) ([]Identity, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Identity{}
	for _, id := range m.identities {
		if id.TenantID == tenant {
			out = append(out, id)
		}
	}
	return out, nil
}
func (t *memTx) RegisterIdentity(id Identity) error {
	if id.Owner() != t.owner {
		return Fail(403, "owner_mismatch", "owner directory scope mismatch")
	}
	if t.frozen[id.TenantID] {
		return Fail(410, "tenant_deleted", "memory tenant is fenced")
	}
	t.identities[t.owner] = Identity{TenantID: id.TenantID, UserID: id.UserID, ActorID: id.ActorID}
	return nil
}
func (t *memTx) ForgetIdentity() error       { delete(t.identities, t.owner); return nil }
func (t *memTx) TenantFrozen() (bool, error) { return t.frozen[t.identities[t.owner].TenantID], nil }
func (m *MemoryStore) Due(ctx context.Context, now time.Time, limit int) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []string{}
	for owner, due := range m.due {
		if !due.IsZero() && !due.After(now) {
			out = append(out, owner)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}
func (m *MemoryStore) Ping(ctx context.Context) error { return ctx.Err() }
func (t *memTx) Get(kind, id string, into any) error {
	b, ok := t.objects[kind+"\x00"+id]
	if !ok {
		return ErrMissing
	}
	return json.Unmarshal(b, into)
}
func (t *memTx) Put(kind, id string, v any) error {
	b, err := json.Marshal(v)
	if err == nil {
		t.objects[kind+"\x00"+id] = b
	}
	return err
}
func (t *memTx) Delete(kind, id string) error { delete(t.objects, kind+"\x00"+id); return nil }
func (t *memTx) DeleteKind(kind string) error {
	prefix := kind + "\x00"
	for key := range t.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			delete(t.objects, key)
		}
	}
	return nil
}
func (t *memTx) List(kind string) ([]json.RawMessage, error) {
	out := []json.RawMessage{}
	prefix := kind + "\x00"
	for k, v := range t.objects {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, append(json.RawMessage(nil), v...))
		}
	}
	return out, nil
}
func (t *memTx) Schedule(at time.Time) error { t.due = at; return nil }
