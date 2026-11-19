package state

import "context"

const eventsDDL = `
CREATE TABLE IF NOT EXISTS ahp_state.inbox_events (
    id          text PRIMARY KEY,
    thread_id   text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
    key         text NOT NULL,
    kind        text NOT NULL,
    sequence    bigint NOT NULL,
    payload     jsonb NOT NULL,
    digest      text NOT NULL,
    acked       boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (thread_id, key),
    UNIQUE (thread_id, sequence)
);

CREATE TABLE IF NOT EXISTS ahp_state.stream_events (
    id          text PRIMARY KEY,
    thread_id   text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
    run_id      text NOT NULL,
    key         text NOT NULL,
    kind        text NOT NULL,
    sequence    bigint NOT NULL,
    payload     jsonb NOT NULL,
    digest      text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (thread_id, key),
    UNIQUE (thread_id, sequence)
);

CREATE INDEX IF NOT EXISTS inbox_thread_ack_idx
    ON ahp_state.inbox_events (thread_id, acked, sequence);
CREATE INDEX IF NOT EXISTS stream_thread_idx
    ON ahp_state.stream_events (thread_id, sequence);
`

// MigrateEvents creates the inbox and stream event tables owned by this file.
// MigrateEvents applies the events DDL on the store's own pool. Migrate calls the same body on
// the connection that already holds the migrate advisory lock, which is what
// keeps a MaxConns=1 pool from deadlocking at startup.
func (s *Store) MigrateEvents(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return invalid("state: store is not initialised")
	}
	return s.migrateEvents(ctx, s.pool)
}

func (s *Store) migrateEvents(ctx context.Context, ex ddlExec) error {
	_, err := ex.Exec(ctx, eventsDDL)
	return err
}
