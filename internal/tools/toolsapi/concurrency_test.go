package toolsapi

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestConcurrentSameKeySingleEffect: many concurrent Calls with the SAME idempotency
// key must produce exactly one effect (the ledger guard serialises dispatch),
// which is the double-effect defence the storage contract requires.
func TestConcurrentSameKeySingleEffect(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t)
	args := `{"n":7}`
	e.allowDigest(t, "write", json.RawMessage(args))

	const writers = 16
	var wg sync.WaitGroup
	codes := make([]int, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			status, _ := e.call(t, "Call", tok, `{"key":"k-conc","tool":"write","arguments":`+args+`}`)
			codes[i] = status
		}(i)
	}
	wg.Wait()
	if n := atomic.LoadInt32(&e.fixture.effCount); n != 1 {
		t.Fatalf("concurrent identical calls produced %d effects, want 1", n)
	}
	for i, c := range codes {
		if c != http.StatusOK {
			t.Fatalf("concurrent writer %d got status %d, want 200", i, c)
		}
	}
}

func TestConcurrentDifferentKeysOnSameGuardReturnConflict(t *testing.T) {
	e := newEnv(t)
	token := e.signTask(t)
	args := `{"n":42}`
	e.allowDigest(t, "write", json.RawMessage(args))
	// Keep the winner's guard held even after the MCP handler returns.
	e.fixture.dropNextCallReply.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Bring both dispatches past the holder lookup before either can update
	// its row. The partial unique index must arbitrate the race.
	schema := pgx.Identifier{e.schema}.Sanitize()
	barrier := schema + `.wait_before_dispatch`
	_, err := e.pool.Exec(ctx, `CREATE FUNCTION `+barrier+`() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN
			PERFORM pg_advisory_xact_lock(hashtextextended(TG_TABLE_SCHEMA || '/test-dispatch', 0));
			RETURN NEW;
		END $$;
		CREATE TRIGGER wait_before_dispatch BEFORE UPDATE ON `+schema+`.operations
		FOR EACH ROW WHEN (NEW.status = 'DISPATCHED') EXECUTE FUNCTION `+barrier+`();`)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Rollback(context.Background())
	if _, err := gate.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, e.schema+"/test-dispatch"); err != nil {
		t.Fatal(err)
	}

	type response struct {
		key    string
		status int
		body   map[string]any
	}
	keys := []string{"guard-a", "guard-b"}
	replies := make(chan response, len(keys))
	for _, key := range keys {
		go func() {
			status, body := e.call(t, "Call", token, `{"key":"`+key+`","tool":"write","arguments":`+args+`}`)
			replies <- response{key, status, body}
		}()
	}
	for {
		var waiting int
		err := gate.QueryRow(ctx, `SELECT count(*) FROM pg_locks waiting
			JOIN pg_locks held USING (locktype, database, classid, objid, objsubid)
			WHERE held.pid = pg_backend_pid() AND held.locktype = 'advisory'
			  AND held.granted AND NOT waiting.granted`).Scan(&waiting)
		if err != nil {
			t.Fatalf("waiting for both dispatches at the SQL barrier: %v", err)
		}
		if waiting == len(keys) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := gate.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	var winners, conflicts int
	for range keys {
		select {
		case r := <-replies:
			switch {
			case r.status == http.StatusOK && r.body["status"] == operations.StatusSideEffectUnknown:
				winners++
			case r.status == http.StatusConflict && r.body["code"] == contracts.ErrConflict:
				conflicts++
				status, body := e.call(t, "Get", token, `{"key":"`+r.key+`"}`)
				mustStatus(t, status, http.StatusOK, body)
				if body["status"] != operations.StatusAccepted {
					t.Errorf("losing operation: status = %v, want ACCEPTED", body["status"])
				}
			default:
				t.Errorf("dispatch %s: HTTP %d, body %v", r.key, r.status, r.body)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	effects := atomic.LoadInt32(&e.fixture.effCount)
	if winners != 1 || conflicts != 1 || effects != 1 {
		t.Fatalf("winners=%d conflicts=%d effects=%d, want one of each", winners, conflicts, effects)
	}
}
