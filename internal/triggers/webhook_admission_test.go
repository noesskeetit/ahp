package triggers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

type concurrentWebhookIssuer struct{}

func (concurrentWebhookIssuer) IssueToken(_ context.Context, _ contracts.Scope, _ string, ttl time.Duration) (string, time.Time, error) {
	return "test-issued-task", time.Now().Add(ttl), nil
}

type recordedWebhookFire struct {
	scope   contracts.Scope
	request FireRequest
}
type concurrentWebhookDispatcher struct {
	mu      sync.Mutex
	calls   []recordedWebhookFire
	hold    <-chan struct{}
	started chan struct{}
}

func (d *concurrentWebhookDispatcher) Fire(ctx context.Context, scope contracts.Scope, req FireRequest) (FireResult, error) {
	d.mu.Lock()
	d.calls = append(d.calls, recordedWebhookFire{scope, req})
	n := len(d.calls)
	d.mu.Unlock()
	if d.started != nil {
		d.started <- struct{}{}
	}
	if d.hold != nil {
		select {
		case <-d.hold:
		case <-ctx.Done():
			return FireResult{}, ctx.Err()
		}
	}
	return FireResult{TaskID: fmt.Sprintf("received-%d", n)}, nil
}
func (d *concurrentWebhookDispatcher) snapshot() []recordedWebhookFire {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]recordedWebhookFire(nil), d.calls...)
}
func createAdmissionWebhook(t *testing.T, e *env, ctx context.Context, max int64) Trigger {
	t.Helper()
	tr, err := e.svc.CreateWebhook(ctx, e.scope, CreateWebhookSpec{ParentTask: "parent", DelegationRef: "grant", Target: "agent:echo", SecretRef: "sec-1", MaxFires: max, Payload: json.RawMessage(`{"text":"Process the delivered event"}`)})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}
func TestWebhookDeliveryBodyReachesDispatcher(t *testing.T) {
	e, ctx := newTestService(t)
	tr := createAdmissionWebhook(t, e, ctx, 3)
	body := []byte(`{"event":"delivery-content-unique","tenant":"spoofed","subject":"other"}`)
	_, err := e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "delivery", SignWebhook([]byte("s3cr3t"), canonicalWebhookMessage(tr.TriggerID, "delivery", body)), body)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.disp.reqs) != 1 || !strings.Contains(string(e.disp.reqs[0].Payload), "delivery-content-unique") {
		t.Fatalf("actual webhook body discarded: %+v", e.disp.reqs)
	}
}
func TestWebhookMessageIDBindsExactBody(t *testing.T) {
	e, ctx := newTestService(t)
	tr := createAdmissionWebhook(t, e, ctx, 1)
	first := []byte(`{"event":1}`)
	_, err := e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "delivery", SignWebhook([]byte("s3cr3t"), canonicalWebhookMessage(tr.TriggerID, "delivery", first)), first)
	if err != nil {
		t.Fatal(err)
	}
	changed := []byte(`{"event":2}`)
	_, err = e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "delivery", SignWebhook([]byte("s3cr3t"), canonicalWebhookMessage(tr.TriggerID, "delivery", changed)), changed)
	if contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("same message ID with another authenticated body accepted: %v", err)
	}
	if e.disp.n != 1 {
		t.Fatalf("changed body dispatched %d times", e.disp.n)
	}
}
func TestWebhookConcurrentDeliveriesRespectMaxFiresAcrossReplicas(t *testing.T) {
	e, ctx := newTestService(t)
	hold := make(chan struct{})
	dispatcher := &concurrentWebhookDispatcher{hold: hold, started: make(chan struct{}, 16)}
	cfg := e.svc.cfg
	cfg.Issuer = concurrentWebhookIssuer{}
	cfg.Dispatcher = dispatcher
	first, err := New(ctx, e.pool, e.schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(ctx, e.pool, e.schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.svc = first
	tr := createAdmissionWebhook(t, e, ctx, 3)
	var wg sync.WaitGroup
	errors := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			svc := first
			if i%2 == 1 {
				svc = second
			}
			id := fmt.Sprintf("m%d", i)
			body := []byte(fmt.Sprintf(`{"delivery":%d}`, i))
			_, err := svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, id, SignWebhook([]byte("s3cr3t"), canonicalWebhookMessage(tr.TriggerID, id, body)), body)
			errors <- err
		}(i)
	}
	// Every caller has either reached the held dispatcher or been rejected.
	// No sleep-dependent assumption about the speed of concurrent admissions.
	var rejected []error
	for i := 0; i < 12; i++ {
		select {
		case <-dispatcher.started:
		case err := <-errors:
			rejected = append(rejected, err)
		case <-ctx.Done():
			close(hold)
			wg.Wait()
			t.Fatal(ctx.Err())
		}
	}
	close(hold)
	wg.Wait()
	close(errors)
	for err := range errors {
		rejected = append(rejected, err)
	}
	for _, err := range rejected {
		if err != nil && contracts.Code(err) != contracts.ErrForbidden {
			t.Errorf("unexpected delivery failure: %v", err)
		}
	}
	calls := dispatcher.snapshot()
	if len(calls) != 3 {
		t.Fatalf("MaxFires=3 admitted %d effects", len(calls))
	}
	seen := map[int64]bool{}
	for _, call := range calls {
		if seen[call.request.Sequence] {
			t.Fatalf("sequence reused across messages: %d", call.request.Sequence)
		}
		seen[call.request.Sequence] = true
	}
	got, err := e.svc.Get(ctx, e.scope, tr.TriggerID)
	if err != nil || got.FiredCount != 3 || got.Status != StatusExhausted {
		t.Fatalf("durable admission counter %+v err=%v", got, err)
	}
}

// Hold one caller after its initial lookup while another caller completes the
// same delivery. The slow caller must receive the stored receipt, not a sealed
// operation-key conflict from Accept.
type heldWebhookIssuer struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (i *heldWebhookIssuer) IssueToken(ctx context.Context, scope contracts.Scope, ref string, ttl time.Duration) (string, time.Time, error) {
	hold := false
	i.once.Do(func() { hold = true; close(i.entered) })
	if hold {
		select {
		case <-i.release:
		case <-ctx.Done():
			return "", time.Time{}, ctx.Err()
		}
	}
	return concurrentWebhookIssuer{}.IssueToken(ctx, scope, ref, ttl)
}
func TestWebhookConcurrentReplayAfterOtherCallerCompletes(t *testing.T) {
	e, ctx := newTestService(t)
	issuer := &heldWebhookIssuer{entered: make(chan struct{}), release: make(chan struct{})}
	e.svc.cfg.Issuer = issuer
	dispatcher := &concurrentWebhookDispatcher{}
	e.svc.cfg.Dispatcher = dispatcher
	tr := createAdmissionWebhook(t, e, ctx, 1)
	body := []byte(`{"event":"one"}`)
	sig := SignWebhookDelivery([]byte("s3cr3t"), tr.TriggerID, "same", body)
	result := make(chan error, 1)
	go func() {
		op, err := e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "same", sig, body)
		if err == nil && op.Status != operations.StatusSucceeded {
			err = fmt.Errorf("receipt status=%s", op.Status)
		}
		result <- err
	}()
	select {
	case <-issuer.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	op, err := e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "same", sig, body)
	close(issuer.release)
	if err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("first completion: %+v %v", op, err)
	}
	if err := <-result; err != nil {
		t.Fatalf("identical replay rejected after concurrent completion: %v", err)
	}
	if n := len(dispatcher.snapshot()); n != 1 {
		t.Fatalf("duplicate dispatches=%d", n)
	}
}

// Reconstruct the service and pool at each durable crash boundary. Only an
// admission/ACCEPTED intent is safe to resume. A dispatched or ambiguous effect
// must keep its slot and must never be sent a second time.
func TestWebhookRestartPreservesDeliveryAndDispatchBoundary(t *testing.T) {
	for _, boundary := range []string{"admitted", "accepted", "dispatched", "unknown", "succeeded", "failed"} {
		t.Run(boundary, func(t *testing.T) {
			e, ctx := newTestService(t)
			tr := createAdmissionWebhook(t, e, ctx, 1)
			body := []byte(`{"event":"restart"}`)
			sum := sha256.Sum256(body)
			digest := hex.EncodeToString(sum[:])
			d, err := e.svc.admitWebhook(ctx, tr, "same", digest)
			if err != nil {
				t.Fatal(err)
			}
			key := webhookOperationKey(e.scope, tr.TriggerID, "same")
			if boundary != "admitted" {
				args, _ := json.Marshal(map[string]any{"sequence": d.sequence, "target": tr.Target, "delegated": tr.Delegated, "body_sha256": digest, "configuration": rawOrEmpty(tr.Payload)})
				if _, err = e.svc.ledger.Accept(ctx, e.scope, operations.Request{Key: key, Kind: EffectKind, Target: tr.Target, Arguments: args}); err != nil {
					t.Fatal(err)
				}
			}
			if boundary != "admitted" && boundary != "accepted" {
				if _, err = e.svc.ledger.Dispatch(ctx, e.scope, key); err != nil {
					t.Fatal(err)
				}
				switch boundary {
				case "unknown":
					_, err = e.svc.ledger.MarkUnknown(ctx, e.scope, key)
				case "succeeded", "failed":
					_, err = e.svc.ledger.Complete(ctx, e.scope, key, json.RawMessage(`{"task_id":"before-restart"}`), boundary == "failed")
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			pool, err := pgxpool.New(ctx, e.pool.Config().ConnString())
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			cfg := e.svc.cfg
			dispatcher := &concurrentWebhookDispatcher{}
			cfg.Dispatcher = dispatcher
			recovered, err := New(ctx, pool, e.schema, cfg)
			if err != nil {
				t.Fatal(err)
			}
			op, err := recovered.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "same", SignWebhookDelivery([]byte("s3cr3t"), tr.TriggerID, "same", body), body)
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := 0
			wantStatus := operations.StatusSucceeded
			switch boundary {
			case "admitted", "accepted":
				wantCalls = 1
			case "dispatched":
				wantStatus = operations.StatusDispatched
			case "unknown":
				wantStatus = operations.StatusSideEffectUnknown
			case "failed":
				wantStatus = operations.StatusFailed
			}
			if calls := dispatcher.snapshot(); len(calls) != wantCalls {
				t.Fatalf("boundary=%s dispatches=%d want=%d", boundary, len(calls), wantCalls)
			}
			if op.Status != wantStatus {
				t.Fatalf("status=%s want=%s", op.Status, wantStatus)
			}
			got, err := recovered.Get(ctx, e.scope, tr.TriggerID)
			if err != nil || got.FiredCount != 1 || got.Status != StatusExhausted {
				t.Fatalf("reservation lost: %+v %v", got, err)
			}
			_, err = recovered.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "other", SignWebhookDelivery([]byte("s3cr3t"), tr.TriggerID, "other", body), body)
			if contracts.Code(err) != contracts.ErrForbidden {
				t.Fatalf("restart returned consumed slot: %v", err)
			}
		})
	}
}

type cancelWebhookDispatcher struct {
	calls  int
	cancel context.CancelFunc
}

func (d *cancelWebhookDispatcher) Fire(context.Context, contracts.Scope, FireRequest) (FireResult, error) {
	d.calls++
	d.cancel()
	return FireResult{}, errors.New("delivery response lost")
}
func TestWebhookUnknownSettlesAfterCallerDisconnect(t *testing.T) {
	e, ctx := newTestService(t)
	tr := createAdmissionWebhook(t, e, ctx, 1)
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	dispatcher := &cancelWebhookDispatcher{cancel: cancel}
	e.svc.cfg.Dispatcher = dispatcher
	body := []byte{0, 255, 1}
	sig := SignWebhookDelivery([]byte("s3cr3t"), tr.TriggerID, "lost", body)
	op, err := e.svc.ReceiveWebhook(call, e.scope, tr.TriggerID, "lost", sig, body)
	if err == nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("lost response status=%s err=%v", op.Status, err)
	}
	got, err := e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "lost", sig, body)
	if err != nil || got.Status != operations.StatusSideEffectUnknown || dispatcher.calls != 1 {
		t.Fatalf("UNKNOWN replay status=%s calls=%d err=%v", got.Status, dispatcher.calls, err)
	}
}

func TestWebhookRevokeWinsConcurrentAdmissionVersionChange(t *testing.T) {
	e, ctx := newTestService(t)
	tr := createAdmissionWebhook(t, e, ctx, 3)
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, fmt.Sprintf(`SELECT 1 FROM %s.triggers WHERE trigger_id=$1 FOR UPDATE`, quoteIdent(e.schema)), tr.TriggerID); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		got, err := e.svc.Revoke(ctx, e.scope, tr.TriggerID)
		if err == nil && got.Status != StatusRevoked {
			err = fmt.Errorf("Revoke returned %s", got.Status)
		}
		result <- err
	}()
	for {
		var waiting bool
		err = e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1)`, "UPDATE "+quoteIdent(e.schema)+".triggers%").Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	// Admission has committed a new version after Revoke read the old version.
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.triggers SET fired_count=1,version=version+1 WHERE trigger_id=$1`, quoteIdent(e.schema)), tr.TriggerID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	body := []byte(`{}`)
	_, err = e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "later", SignWebhookDelivery([]byte("s3cr3t"), tr.TriggerID, "later", body), body)
	if contracts.Code(err) != contracts.ErrForbidden || e.disp.n != 0 {
		t.Fatalf("dispatch after revoke: %v calls=%d", err, e.disp.n)
	}
}

func TestWebhookRevokeAfterAdmissionClosesUnsentIntent(t *testing.T) {
	e, ctx := newTestService(t)
	tr := createAdmissionWebhook(t, e, ctx, 3)
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, fmt.Sprintf(`LOCK TABLE %s.operations IN SHARE MODE`, quoteIdent(e.schema))); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	body := []byte(`{"event":"pending"}`)
	go func() {
		_, err := e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "pending", SignWebhookDelivery([]byte("s3cr3t"), tr.TriggerID, "pending", body), body)
		result <- err
	}()
	for {
		var waiting bool
		if err = e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1)`, "INSERT INTO "+quoteIdent(e.schema)+".operations%").Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if _, err = e.svc.Revoke(ctx, e.scope, tr.TriggerID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-result; contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("pending revoked delivery dispatched: %v", err)
	}
	op, err := e.svc.ledger.Get(ctx, e.scope, webhookOperationKey(e.scope, tr.TriggerID, "pending"))
	if err != nil || op.Status != operations.StatusFenced || e.disp.n != 0 {
		t.Fatalf("unsent intent not fenced: %+v calls=%d err=%v", op, e.disp.n, err)
	}
}

type revokedAtDispatchIssuer struct{ n int }

func (i *revokedAtDispatchIssuer) IssueToken(ctx context.Context, scope contracts.Scope, ref string, ttl time.Duration) (string, time.Time, error) {
	i.n++
	if i.n > 1 {
		return "", time.Time{}, contracts.NewError(contracts.ErrForbidden, "delegation revoked after admission")
	}
	return concurrentWebhookIssuer{}.IssueToken(ctx, scope, ref, ttl)
}
func TestWebhookRevalidatesDelegationBeforeDispatch(t *testing.T) {
	e, ctx := newTestService(t)
	tr := createAdmissionWebhook(t, e, ctx, 3)
	e.svc.cfg.Issuer = &revokedAtDispatchIssuer{}
	body := []byte(`{}`)
	_, err := e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "revoked", SignWebhookDelivery([]byte("s3cr3t"), tr.TriggerID, "revoked", body), body)
	if contracts.Code(err) != contracts.ErrForbidden || e.disp.n != 0 {
		t.Fatalf("stale delegated authority dispatched: err=%v calls=%d", err, e.disp.n)
	}
	op, err := e.svc.ledger.Get(ctx, e.scope, webhookOperationKey(e.scope, tr.TriggerID, "revoked"))
	if err != nil || op.Status != operations.StatusFenced {
		t.Fatalf("revoked pending intent=%+v err=%v", op, err)
	}
	got, err := e.svc.Get(ctx, e.scope, tr.TriggerID)
	if err != nil || got.Status != StatusRevoked {
		t.Fatalf("authority loss did not stop trigger: %+v %v", got, err)
	}
}

type lockDispatchWebhookIssuer struct {
	n           int
	pool        *pgxpool.Pool
	schema, key string
	locked      chan struct{}
	release     chan struct{}
}

func (i *lockDispatchWebhookIssuer) IssueToken(ctx context.Context, scope contracts.Scope, ref string, ttl time.Duration) (string, time.Time, error) {
	i.n++
	if i.n == 2 {
		tx, err := i.pool.Begin(ctx)
		if err != nil {
			return "", time.Time{}, err
		}
		if _, err = tx.Exec(ctx, fmt.Sprintf(`SELECT 1 FROM %s.operations WHERE key=$1 FOR UPDATE`, quoteIdent(i.schema)), i.key); err != nil {
			tx.Rollback(ctx)
			return "", time.Time{}, err
		}
		close(i.locked)
		go func() {
			select {
			case <-i.release:
			case <-ctx.Done():
			}
			_ = tx.Rollback(context.Background())
		}()
	}
	return concurrentWebhookIssuer{}.IssueToken(ctx, scope, ref, ttl)
}
func TestWebhookRevokeSerializesWithDispatchRight(t *testing.T) {
	e, ctx := newTestService(t)
	tr := createAdmissionWebhook(t, e, ctx, 3)
	issuer := &lockDispatchWebhookIssuer{pool: e.pool, schema: e.schema, key: webhookOperationKey(e.scope, tr.TriggerID, "pending"), locked: make(chan struct{}), release: make(chan struct{})}
	e.svc.cfg.Issuer = issuer
	done := make(chan error, 1)
	body := []byte(`{}`)
	go func() {
		_, err := e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "pending", SignWebhookDelivery([]byte("s3cr3t"), tr.TriggerID, "pending", body), body)
		done <- err
	}()
	select {
	case <-issuer.locked:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Wait until dispatch is stuck on the operation row, after live checks.
	for {
		var waiting bool
		err := e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1)`, "%FROM "+quoteIdent(e.schema)+".operations%FOR UPDATE%").Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	revoked := make(chan error, 1)
	go func() { _, err := e.svc.Revoke(ctx, e.scope, tr.TriggerID); revoked <- err }()
	committedBeforeRight := false
	for {
		select {
		case err := <-revoked:
			if err != nil {
				t.Fatal(err)
			}
			committedBeforeRight = true
		default:
		}
		if committedBeforeRight {
			break
		}
		var waiting bool
		if err := e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE $1)`, "UPDATE "+quoteIdent(e.schema)+".triggers%").Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	close(issuer.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !committedBeforeRight {
		if err := <-revoked; err != nil {
			t.Fatal(err)
		}
	}
	if committedBeforeRight && e.disp.n != 0 {
		t.Fatal("Revoke committed before dispatch right, yet the queued effect was sent")
	}
	got, err := e.svc.Get(ctx, e.scope, tr.TriggerID)
	if err != nil || got.Status != StatusRevoked {
		t.Fatalf("revocation lost: %+v %v", got, err)
	}
}
