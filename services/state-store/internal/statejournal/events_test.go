package statejournal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAppendEventsPostgres(t *testing.T) {
	ctx, pool, s := journalTestStore(t)
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "adk-app", Subject: "user"}
	leases := make(map[string]Lease)
	for _, session := range []string{"first", "second"} {
		if err := s.CreateSession(ctx, scope, session, "adk", 1); err != nil {
			t.Fatal(err)
		}
		lease, err := s.AcquireSession(ctx, scope, session, "worker", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		leases[session] = lease
	}

	value := Payload{Inline: []byte("before-effect")}
	leaf, parent := "e2", "e1"
	request := AppendRequest{
		Key: "first-batch",
		Events: []EventInput{
			{ID: "e1", Type: "model.tool_call", Role: "tool_call", Codec: "text", Payload: value},
			{ID: "e2", ParentID: "e1", Type: "progress", Role: "system", Codec: "text", Payload: value},
		},
		KV: []KVMutation{
			{ScopeType: "user", Namespace: "adk", Key: "profile", Payload: value},
			{ScopeType: "app", Namespace: "adk", Key: "setting", Payload: value},
		},
		Intents: []PreparedIntent{{
			CallID: "turn1:task:call", ToolName: "test.write",
			ArgsDigest: sha256.Sum256([]byte("arguments")), IdempotencyKey: "effect-1",
		}},
		Pointers: &PointerUpdate{Leaf: &leaf, AppendParent: &parent},
	}

	checkError := func(err error, code string) {
		t.Helper()
		if contracts.Code(err) != code {
			t.Fatalf("expected %s, got %v", code, err)
		}
	}

	_, err := s.AppendEvents(ctx, scope, "first", leases["first"], false, request)
	checkError(err, contracts.ErrForbidden)
	receipt, err := s.AppendEvents(ctx, scope, "first", leases["first"], true, request)
	if err != nil || receipt != (AppendReceipt{FirstSeq: 1, HeadSeq: 2}) {
		t.Fatalf("append: %+v, %v", receipt, err)
	}
	again, err := s.AppendEvents(ctx, scope, "first", leases["first"], true, request)
	if err != nil || again != receipt {
		t.Fatalf("lost ACK replay changed the receipt: %+v, %v", again, err)
	}
	page, err := s.GetEvents(ctx, scope, "first", 1, 1)
	if err != nil || page.HeadSeq != 2 || len(page.Events) != 1 ||
		page.Events[0].Seq != 2 || page.Events[0].ParentID != "e1" ||
		page.Leaf == nil || *page.Leaf != leaf || page.AppendParent == nil || *page.AppendParent != parent {
		t.Fatalf("read page: %+v, %v", page, err)
	}

	mutated := request
	mutated.ExpectedSeq = receipt.HeadSeq
	_, err = s.AppendEvents(ctx, scope, "first", leases["first"], true, mutated)
	checkError(err, contracts.ErrKeyReuse)
	_, err = s.AppendEvents(ctx, scope, "second", leases["second"], true, request)
	checkError(err, contracts.ErrConflict)

	var events, kvs, intents, secondHead int
	var intentStatus string
	err = pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM ahp_journal.session_events WHERE tenant_id=$1),
		       (SELECT count(*) FROM ahp_journal.scoped_kv WHERE tenant_id=$1 AND version=1),
		       (SELECT count(*) FROM ahp_journal.tool_intents WHERE tenant_id=$1),
		       (SELECT head_seq FROM ahp_journal.sessions WHERE tenant_id=$1 AND session_id='second'),
		       (SELECT status FROM ahp_journal.tool_intents WHERE tenant_id=$1)`, scope.Tenant).
		Scan(&events, &kvs, &intents, &secondHead, &intentStatus)
	if err != nil || events != 2 || kvs != 2 || intents != 1 || secondHead != 0 || intentStatus != "PREPARED" {
		t.Fatalf("unexpected committed state: events=%d, kv=%d, intent=%d/%s, second head=%d, err=%v",
			events, kvs, intents, intentStatus, secondHead, err)
	}
	foreign := scope
	foreign.Subject = "different-user"
	_, err = s.AppendEvents(ctx, foreign, "first", leases["first"], true, request)
	checkError(err, contracts.ErrNotFound)
	_, err = s.GetEvents(ctx, foreign, "first", 0, 10)
	checkError(err, contracts.ErrNotFound)

	if _, err := pool.Exec(ctx, `UPDATE ahp_journal.sessions SET lease_expires_at=clock_timestamp()
		WHERE tenant_id=$1 AND session_id='first'`, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	replacement, err := s.AcquireSession(ctx, scope, "first", "replacement", time.Minute)
	if err != nil || replacement.Epoch != leases["first"].Epoch+1 {
		t.Fatalf("replacement lease: %+v, %v", replacement, err)
	}
	_, err = s.AppendEvents(ctx, scope, "first", leases["first"], true, request)
	checkError(err, contracts.ErrStaleEpoch)
	_, err = s.HeartbeatLease(ctx, scope, "first", leases["first"], time.Minute)
	checkError(err, contracts.ErrStaleEpoch)

	intent := IntentTransition{
		CallID: request.Intents[0].CallID, GatewayOperationID: "gateway-1", Status: IntentDispatched,
	}
	if err := s.MutateToolIntent(ctx, scope, "first", replacement, intent); err != nil {
		t.Fatal(err)
	}
	intent.Status = IntentUnknown
	unknown := AppendRequest{
		Key: "unknown", ExpectedSeq: receipt.HeadSeq,
		Events:       []EventInput{{ID: "e3", Type: "tool.unknown", Role: "system", Codec: "text", Payload: value}},
		ToolOutcomes: []IntentTransition{intent},
	}
	waiting, err := s.AppendEvents(ctx, scope, "first", replacement, true, unknown)
	if err != nil || waiting.HeadSeq != 3 {
		t.Fatalf("commit unknown tool outcome: %+v, %v", waiting, err)
	}
	intent.Status, intent.Result = IntentCompleted, &value
	_, err = s.AppendEvents(ctx, scope, "first", replacement, true, AppendRequest{
		Key: "invalid-retry", ExpectedSeq: waiting.HeadSeq,
		Events:       []EventInput{{ID: "e4", Type: "tool.complete", Role: "tool_result", Codec: "text", Payload: value}},
		ToolOutcomes: []IntentTransition{intent},
	})
	checkError(err, contracts.ErrConflict)
	if contracts.ReasonOf(err) != IntentUnknown {
		t.Fatalf("unknown must hold for reconciliation, got %v", err)
	}
	page, err = s.GetEvents(ctx, scope, "first", waiting.HeadSeq, 10)
	if err != nil || page.HeadSeq != waiting.HeadSeq || len(page.Events) != 0 {
		t.Fatalf("rejected tool outcome must roll back the event: %+v, %v", page, err)
	}
}

func journalTestStore(t *testing.T) (context.Context, *pgxpool.Pool, *Store) {
	t.Helper()
	dsn := os.Getenv("AHP_JOURNAL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires an isolated PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(config.ConnConfig.Database, "ahp_carbon_compact") {
		t.Fatal("use an isolated ahp_carbon_compact test database")
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := New(pool)
	for i := range 2 {
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("migrate %d: %v", i, err)
		}
	}

	return ctx, pool, s
}
