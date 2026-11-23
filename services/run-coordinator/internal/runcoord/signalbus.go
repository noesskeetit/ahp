package runcoord

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGSignalBus struct {
	pool        *pgxpool.Pool
	mu          sync.Mutex
	subscribers map[SessionSignal]map[chan struct{}]struct{}
}

func NewPGSignalBus(pool *pgxpool.Pool) *PGSignalBus {
	return &PGSignalBus{pool: pool, subscribers: make(map[SessionSignal]map[chan struct{}]struct{})}
}

func (b *PGSignalBus) Notify(ctx context.Context, signal SessionSignal) error {
	raw, err := json.Marshal(signal)
	if err != nil {
		return err
	}
	_, err = b.pool.Exec(ctx, "SELECT pg_notify($1,$2)", journal.SessionSignalChannel, string(raw))
	return err
}

func (b *PGSignalBus) Subscribe(signal SessionSignal) (<-chan struct{}, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	in := make(chan struct{}, 1)
	subs := b.subscribers[signal]
	if subs == nil {
		subs = make(map[chan struct{}]struct{})
		b.subscribers[signal] = subs
	}
	subs[in] = struct{}{}
	return in, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(subs, in)
		if len(subs) == 0 {
			delete(b.subscribers, signal)
		}
	}
}

func (b *PGSignalBus) runListener(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, b.pool.Config().ConnConfig)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, "LISTEN "+journal.SessionSignalChannel); err != nil {
		return err
	}
	for {
		event, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		var signal SessionSignal
		if json.Unmarshal([]byte(event.Payload), &signal) != nil {
			continue
		}
		b.mu.Lock()
		for ch := range b.subscribers[signal] {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		b.mu.Unlock()
	}
}

func (b *PGSignalBus) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := b.runListener(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("journal notification connection will retry", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
