package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Pagination bounds for ListThreads. A request that omits or zero/clamps the
// limit gets the default; the limit is capped so a single call cannot pull an
// unbounded page.
const (
	defaultListThreadsLimit = 50
	maxListThreadsLimit     = 200
)

// threadsCursor is the opaque keyset position: the (created_at, id) of the
// last thread already returned on the previous page. Threads are ordered by
// (created_at DESC, id DESC), so the next page is every in-scope thread whose
// (created_at, id) sorts strictly after the cursor in that order.
type threadsCursor struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"i"`
}

func encodeThreadsCursor(t time.Time, id string) string {
	raw, err := json.Marshal(threadsCursor{CreatedAt: t.UTC(), ID: id})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeThreadsCursor validates and decodes an opaque cursor. An empty cursor
// means the first page. A malformed cursor is a client error (INVALID): it is
// never silently treated as a fresh scan, because that would let a caller jump
// pages by forging garbage.
func decodeThreadsCursor(s string) (time.Time, string, error) {
	if s == "" {
		return time.Time{}, "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, "", contracts.NewError(contracts.ErrInvalid, "list threads: cursor is not well formed")
	}
	var c threadsCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return time.Time{}, "", contracts.NewError(contracts.ErrInvalid, "list threads: cursor is not well formed")
	}
	if c.ID == "" || c.CreatedAt.IsZero() {
		return time.Time{}, "", contracts.NewError(contracts.ErrInvalid, "list threads: cursor is missing fields")
	}
	return c.CreatedAt.UTC(), c.ID, nil
}

// ListThreads returns the caller's threads, newest first, using durable keyset
// pagination. The scope (tenant, deployment, subject) always comes from the
// authenticated token: a caller only ever sees its own threads, and threads in
// a foreign scope cannot be reached or probed, even with a forged cursor.
//
// Ordering is (created_at DESC, id DESC), a deterministic total order over the
// durable rows. Keyset pagination is stable under concurrent insert and delete:
// already-returned rows never change key, so a page is defined by its key bound
// rather than by a mutable offset — a thread created during pagination may enter
// the front of the list but can never duplicate or skip a row the caller has not
// yet seen.
//
// Deleted threads are gone (DeleteThread/DeleteSubject remove the row), so they
// are absent; a thread whose deletion is blocked by legal hold was never
// removed, so it is still listed. This is exactly the documented Lifecycle
// retention semantics: retained means present, deleted means absent.
//
// It returns the page, the next opaque cursor ("" when there is no further
// page), or an error.
func (s *Store) ListThreads(ctx context.Context, scope contracts.Scope, cursor string, limit int) ([]Thread, string, error) {
	if err := scope.Validate(); err != nil {
		return nil, "", asInvalid(err)
	}
	if limit <= 0 {
		limit = defaultListThreadsLimit
	}
	if limit > maxListThreadsLimit {
		limit = maxListThreadsLimit
	}
	curTime, curID, err := decodeThreadsCursor(cursor)
	if err != nil {
		return nil, "", err
	}

	// Fetch one extra row to decide whether a further page exists without a
	// second round trip. The WHERE clause pins the authenticated scope; the
	// cursor only narrows by key inside that scope, so a foreign or forged
	// cursor position can never widen visibility beyond the caller's own rows.
	const cols = threadColumns
	var rows pgx.Rows
	if curID == "" {
		rows, err = s.pool.Query(ctx, `
			SELECT `+cols+`
			FROM ahp_state.threads
			WHERE tenant = $1 AND deployment = $2 AND subject = $3
			ORDER BY created_at DESC, id DESC
			LIMIT $4`,
			scope.Tenant, scope.Deployment, scope.Subject, limit+1)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT `+cols+`
			FROM ahp_state.threads
			WHERE tenant = $1 AND deployment = $2 AND subject = $3
			  AND (created_at < $4 OR (created_at = $4 AND id < $5))
			ORDER BY created_at DESC, id DESC
			LIMIT $6`,
			scope.Tenant, scope.Deployment, scope.Subject, curTime, curID, limit+1)
	}
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	out := make([]Thread, 0, limit)
	for rows.Next() {
		t, err := scanThread(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, t)
		if len(out) == limit+1 {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	next := ""
	if len(out) == limit+1 {
		last := out[limit-1]
		out = out[:limit]
		next = encodeThreadsCursor(last.CreatedAt, last.ID)
	}
	return out, next, nil
}
