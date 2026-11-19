package gateway_test

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// randSuffix returns a random suffix so concurrent runs and other services
// sharing this database can never collide.
func randSuffix(t *testing.T) string {
	t.Helper()
	id := contracts.NewID()
	if len(id) > 20 {
		id = id[len(id)-20:]
	}
	return strings.ToLower(strings.ReplaceAll(id, "_", ""))
}

// env is one fully migrated execution-plane deployment: gateway schema plus the
// service-owned effect ledger in the same schema.
type env struct {
	pool   *pgxpool.Pool
	store  *gateway.Store
	ledger *operations.Store
	schema string
	scope  contracts.Scope
	caller string
	worker string
	sfx    string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	// DDL-only budget (schema + gateway tables + ledger). 30s was a false-failure
	// generator on a shared instance: gateway/ledger Migrate was still legitimately
	// running while the machine built three other lanes' -race binaries. A genuine
	// migration error still surfaces as itself; only premature giving up is removed.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Fail-closed discipline: a missing real dependency must stop the run with a
	// reason. A silent skip here let the whole durability suite report green over
	// a PostgreSQL instance that was never contacted. The only way down is an
	// explicit opt-out that prints SKIP_EXPLICIT, which acceptance must not count.
	url := requireRealGatewayDB(t)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("cannot build pool for the real PostgreSQL instance: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("real PostgreSQL instance is unreachable: %v", err)
	}
	t.Cleanup(pool.Close)

	sfx := randSuffix(t)
	schema := "ahp_gw_" + sfx
	store, err := gateway.NewStore(pool, schema)
	if err != nil {
		t.Fatalf("NewStore(%q): %v", schema, err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("gateway Migrate failed: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("gateway Migrate is not idempotent: %v", err)
	}
	ledger, err := operations.New(pool, schema)
	if err != nil {
		t.Fatalf("operations.New(%q): %v", schema, err)
	}
	if err := ledger.Migrate(ctx); err != nil {
		t.Fatalf("ledger Migrate failed: %v", err)
	}

	return &env{
		pool:   pool,
		store:  store,
		ledger: ledger,
		schema: schema,
		sfx:    sfx[:12],
		scope: contracts.Scope{
			Tenant:     "t-" + sfx[:12],
			Deployment: "dep-" + sfx[:12],
			Subject:    "subj-" + sfx[:12],
		},
		caller: "caller-" + sfx[:8],
		worker: "worker-" + sfx[:8],
	}
}

func (e *env) service(t *testing.T, exec gateway.Executor, mutate func(*gateway.Config)) *gateway.Service {
	t.Helper()
	cfg := gateway.Config{
		WorkerID:    e.worker,
		Lease:       5 * time.Second,
		Poll:        10 * time.Millisecond,
		MaxBlock:    5 * time.Second,
		MaxAttempts: 2,
		ParkLease:   30 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	svc, err := gateway.NewService(e.store, e.ledger, exec, gateway.NullNotifier{}, cfg)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

// submit accepts a task without running it.
func submit(t *testing.T, svc *gateway.Service, scope contracts.Scope, caller, msgID string, mutate func(*a2a.SendMessageRequest)) (*a2a.Task, error) {
	t.Helper()
	req := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("work please")),
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	}
	req.Message.ID = msgID
	if mutate != nil {
		mutate(req)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return svc.SendMessage(ctx, scope, caller, req)
}

func mustSubmit(t *testing.T, svc *gateway.Service, scope contracts.Scope, caller, msgID string, mutate func(*a2a.SendMessageRequest)) *a2a.Task {
	t.Helper()
	task, err := submit(t, svc, scope, caller, msgID, mutate)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	return task
}

// echoExecutor completes every task immediately with one artifact.
func echoExecutor() gateway.ExecutorFunc {
	return func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{
			State:   a2a.TaskStateCompleted,
			Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done")),
			Artifacts: []*a2a.Artifact{{
				ID:    a2a.ArtifactID("art-" + in.Task.ID),
				Parts: a2a.ContentParts{a2a.NewTextPart("result")},
			}},
		}, nil
	}
}

// waitFor polls until f reports success.
func waitFor(t *testing.T, timeout time.Duration, f func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v: %s", timeout, msg)
}

func TestSendMessageIsIdempotentPerCallerAndTarget(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const msgID = "msg-idempotent-1"
	first := mustSubmit(t, svc, e.scope, e.caller, msgID, nil)
	if first.Status.State != a2a.TaskStateSubmitted {
		t.Fatalf("fresh task state = %q, want SUBMITTED", first.Status.State)
	}

	replay, err := submit(t, svc, e.scope, e.caller, msgID, nil)
	if err != nil {
		t.Fatalf("replayed SendMessage: %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("replay created task %q, want the original %q", replay.ID, first.ID)
	}

	// A different caller may reuse the same message ID: the key is caller+target.
	other := "other-caller-" + e.sfx[:6]
	otherTask := mustSubmit(t, svc, e.scope, other, msgID, nil)
	if otherTask.ID == first.ID {
		t.Fatal("a different caller must not consume another caller's idempotency key")
	}

	// Same caller, different target: also a distinct key.
	named := "ctx-" + e.sfx
	withCtx := mustSubmit(t, svc, e.scope, e.caller, "msg-idempotent-2", func(r *a2a.SendMessageRequest) {
		r.Message.ContextID = named
	})
	if withCtx.ContextID != named {
		t.Fatalf("task context = %q, want the client-named %q", withCtx.ContextID, named)
	}
	again := mustSubmit(t, svc, e.scope, e.caller, "msg-idempotent-2", func(r *a2a.SendMessageRequest) {
		r.Message.ContextID = "ctx-other-" + e.sfx
	})
	if again.ID == withCtx.ID {
		t.Fatal("the same message ID against a different target must be a distinct key")
	}

	// Only the owning caller may continue a task.
	followFromStranger := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("mine now")),
	}
	followFromStranger.Message.ID = "msg-followup-stranger"
	followFromStranger.Message.TaskID = a2a.TaskID(first.ID)
	if _, err := svc.SendMessage(ctx, e.scope, other+"-x", followFromStranger); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("cross-caller follow-up error = %v, want ErrForbidden", err)
	}

	// The owner's follow-up is journaled into the same task, and replayed once.
	follow := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("more detail")),
	}
	follow.Message.ID = "msg-followup-owner"
	follow.Message.TaskID = a2a.TaskID(first.ID)
	// No worker runs in this test, so a blocking send would burn the whole
	// MaxBlock budget on a task that never settles. The journaling contract
	// (the point of this case) is asserted from History, not from the reply.
	follow.Config = &a2a.SendMessageConfig{ReturnImmediately: true}
	row1, err := svc.SendMessage(ctx, e.scope, e.caller, follow)
	if err != nil {
		t.Fatalf("owner follow-up: %v", err)
	}
	if row1.ID != first.ID {
		t.Fatalf("follow-up created task %q, want existing %q", row1.ID, first.ID)
	}
	row2, err := svc.SendMessage(ctx, e.scope, e.caller, follow)
	if err != nil {
		t.Fatalf("follow-up replay: %v", err)
	}
	if row2.ID != row1.ID {
		t.Fatalf("follow-up replay created %q, want %q", row2.ID, row1.ID)
	}
	hist, err := e.store.History(ctx, string(first.ID), 0)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var inbound int
	for _, m := range hist {
		if m.Role == a2a.MessageRoleUser {
			inbound++
		}
	}
	if inbound != 2 {
		t.Fatalf("history has %d inbound messages, want 2 (a replay must not journal twice)", inbound)
	}
}

func TestTaskIsInvisibleOutsideItsScope(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	task := mustSubmit(t, svc, e.scope, e.caller, "msg-isolation", nil)
	id := string(task.ID)

	otherTenant := e.scope
	otherTenant.Tenant += "-x"
	otherDeployment := e.scope
	otherDeployment.Deployment += "-x"
	otherSubject := e.scope
	otherSubject.Subject += "-x"

	for name, scope := range map[string]contracts.Scope{
		"tenant":     otherTenant,
		"deployment": otherDeployment,
		"subject":    otherSubject,
	} {
		if _, err := e.store.Task(ctx, scope, id); !errors.Is(err, a2a.ErrTaskNotFound) {
			t.Errorf("task visible across %s: err = %v, want ErrTaskNotFound", name, err)
		}
	}
	if _, err := e.store.Task(ctx, e.scope, id); err != nil {
		t.Errorf("task must stay visible inside its own scope: %v", err)
	}

	// ListTasks must not leak either.
	rows, _, _, err := e.store.ListTasks(ctx, otherTenant, "", a2a.TaskStateUnspecified, 10, "")
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("ListTasks leaked %d tasks into another tenant", len(rows))
	}
}

func TestDurableWorkerSettlesTaskAndJournalsVersions(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int64
	svc := e.service(t, gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		if in.Task.FencingToken == 0 {
			t.Errorf("executor ran without a fencing token")
		}
		if in.Input == nil {
			t.Errorf("executor received no inbound message")
		}
		calls.Add(1)
		return echoExecutor()(ctx, in)
	}), nil)

	task := mustSubmit(t, svc, e.scope, e.caller, "msg-worker", nil)

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.Run(runCtx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	waitFor(t, 15*time.Second, func() bool {
		row, err := e.store.Task(ctx, e.scope, string(task.ID))
		return err == nil && row.Terminal()
	}, "task never reached a terminal state")

	row, err := e.store.Task(ctx, e.scope, string(task.ID))
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	if row.State != a2a.TaskStateCompleted {
		t.Fatalf("state = %q, want COMPLETED", row.State)
	}
	if row.StatusVersion < 2 {
		t.Fatalf("status_version = %d, want it to advance past the SUBMITTED version", row.StatusVersion)
	}
	if len(row.Artifacts) != 1 {
		t.Fatalf("artifacts = %d, want 1", len(row.Artifacts))
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("executor ran %d times, want exactly 1", got)
	}

	evs, err := e.store.Events(ctx, e.scope, string(task.ID), 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	versions := make([]int64, 0, len(evs))
	for _, ev := range evs {
		versions = append(versions, ev.Version)
	}
	if !sort.SliceIsSorted(versions, func(i, j int) bool { return versions[i] < versions[j] }) {
		t.Fatalf("event versions are not ordered: %v", versions)
	}
	if len(versions) < 2 || versions[0] != 1 {
		t.Fatalf("event versions = %v, want to start at 1 and cover the terminal transition", versions)
	}

	// The journal must show the full observable lifecycle the SSE stream and
	// clients rely on: SUBMITTED (accepted) -> WORKING (the claim IS the
	// transition, a2a-go ClaimTask) -> COMPLETED (settled). A drain that only
	// records SUBMITTED->terminal would let a client subscribe and never observe
	// the in-flight state.
	var states []string
	for _, ev := range evs {
		if su, ok := ev.Payload.(*a2a.TaskStatusUpdateEvent); ok {
			states = append(states, string(su.Status.State))
		}
	}
	sub, working, doneState := string(a2a.TaskStateSubmitted), string(a2a.TaskStateWorking), string(a2a.TaskStateCompleted)
	if len(states) < 3 || states[0] != sub || states[len(states)-1] != doneState {
		t.Fatalf("status-event lifecycle = %v, want %s ... %s", states, sub, doneState)
	}
	sawWorking := false
	for _, s := range states[1 : len(states)-1] {
		if s == working {
			sawWorking = true
		}
	}
	if !sawWorking {
		t.Fatalf("status-event lifecycle never observed WORKING between acceptance and terminal: %v", states)
	}

	// Terminal tasks are never claimed again.
	stop()
	<-done
	if again, err := e.store.ClaimTask(ctx, e.worker+"-2", time.Second); err != nil {
		t.Fatalf("ClaimTask: %v", err)
	} else if again != nil {
		t.Fatalf("ClaimTask returned %q for a settled task", again.ID)
	}
}

func TestEventStreamReplaysAfterServiceRestart(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)
	task := mustSubmit(t, svc, e.scope, e.caller, "msg-restart", nil)

	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.Run(runCtx)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	waitFor(t, 15*time.Second, func() bool {
		row, err := e.store.Task(ctx, e.scope, string(task.ID))
		return err == nil && row.Terminal()
	}, "task never completed before the simulated restart")
	stop()
	<-done

	// The durable journal survives the process: every transition is still there,
	// numbered from 1.
	journal, err := e.store.Events(ctx, e.scope, string(task.ID), 0)
	if err != nil {
		t.Fatalf("store.Events: %v", err)
	}
	if len(journal) < 2 || journal[0].Version != 1 {
		t.Fatalf("journal after restart holds %d events starting at %+v, want >=2 starting at version 1",
			len(journal), journal)
	}

	// A brand new service over the same store, with no in-memory state at all,
	// replays the whole stream and stops on the terminal event.
	fresh := e.service(t, echoExecutor(), func(c *gateway.Config) { c.WorkerID = "restarted-" + e.sfx[:6] })
	var replayed []a2a.Event
	streamCtx, streamCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer streamCancel()
	for ev, err := range fresh.Events(streamCtx, e.scope, string(task.ID), 0) {
		if err != nil {
			t.Fatalf("Events: %v", err)
		}
		replayed = append(replayed, ev)
	}
	if len(replayed) != len(journal) {
		t.Fatalf("replayed %d events, want all %d journalled transitions", len(replayed), len(journal))
	}
	last, ok := replayed[len(replayed)-1].(*a2a.TaskStatusUpdateEvent)
	if !ok || !last.Status.State.Terminal() {
		t.Fatalf("replay must end on a terminal status event, got %T", replayed[len(replayed)-1])
	}

	// Resuming from the last version replays nothing further.
	resumeCtx, resumeCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer resumeCancel()
	var tail int
	for range fresh.Events(resumeCtx, e.scope, string(task.ID), journal[len(journal)-1].Version) {
		tail++
	}
	if tail != 0 {
		t.Fatalf("resuming past the last version replayed %d events, want 0", tail)
	}
}

func TestCancelIsCooperativeAndTerminal(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)
	handler := gateway.NewHandler(svc, gateway.HandlerConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// The public handler derives the caller only from a verified principal set
	// by the auth middleware; a direct call must inject one, exactly as the
	// HTTP stack would, and it must name the task's owner.
	ctx = transport.WithPrincipal(ctx, transport.Principal{
		Claims: auth.Claims{Kind: auth.KindTask, Task: e.caller, Subject: e.caller},
		Scope:  e.scope,
	})

	task := mustSubmit(t, svc, e.scope, e.caller, "msg-cancel", nil)
	if _, err := handler.CancelTask(ctx, &a2a.CancelTaskRequest{ID: a2a.TaskID(task.ID)}); err != nil {
		t.Fatalf("CancelTask on a submitted task: %v", err)
	}

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go svc.Run(runCtx)
	waitFor(t, 15*time.Second, func() bool {
		row, err := e.store.Task(ctx, e.scope, string(task.ID))
		return err == nil && row.State == a2a.TaskStateCanceled
	}, "cancel was never honoured by the worker")

	// A terminal task can no longer be cancelled.
	if _, err := handler.CancelTask(ctx, &a2a.CancelTaskRequest{ID: a2a.TaskID(task.ID)}); !errors.Is(err, a2a.ErrTaskNotCancelable) {
		t.Fatalf("cancel of a terminal task = %v, want ErrTaskNotCancelable", err)
	}
}

func TestAmbiguousSideEffectParksWithoutReplay(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int64
	exec := gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		calls.Add(1)
		// The effect may or may not have escaped; the gateway must never guess.
		return gateway.ExecResult{}, gateway.ErrAmbiguous
	})
	svc := e.service(t, exec, nil)
	task := mustSubmit(t, svc, e.scope, e.caller, "msg-ambiguous", nil)

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go svc.Run(runCtx)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	waitFor(t, 15*time.Second, func() bool {
		op, err := e.ledger.Get(ctx, e.scope, "exec:"+string(task.ID)+":1")
		return err == nil && op.Status == operations.StatusSideEffectUnknown
	}, "ledger never recorded SIDE_EFFECT_UNKNOWN")

	stop()

	// Parking must not turn into a replay: the parked lease keeps the task out
	// of the claimable set for the park duration.
	if got := calls.Load(); got != 1 {
		t.Fatalf("executor ran %d times, want exactly 1 before any reconciliation", got)
	}
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := e.store.ClaimTask(ctx, e.worker+"-probe", 20*time.Millisecond); err != nil {
			t.Fatalf("ClaimTask: %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("executor ran %d times after parking, want 1: ambiguous effects must never auto-replay", got)
	}
	row, err := e.store.Task(ctx, e.scope, string(task.ID))
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	if row.Terminal() {
		t.Fatalf("parked task settled as %q; it must stay non-terminal for a human or reconciler", row.State)
	}
}

func TestTerminalStatusEnqueuesOutboxAtomically(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// A child of another task must enqueue a child-status delivery.
	// The parent is a real task: the gateway audits a declared parent against
	// its own store (tree fields are server-set), so a forged parent id is
	// refused at submit -- see TestTreeFieldsAreServerSet for that leg.
	parent := mustSubmit(t, svc, e.scope, e.caller, "msg-parent-"+e.sfx, nil)
	child := mustSubmit(t, svc, e.scope, e.caller, "msg-child", func(r *a2a.SendMessageRequest) {
		r.Metadata = map[string]any{
			gateway.MetaParentTask: string(parent.ID),
			gateway.MetaTaskKind:   "subagent",
		}
	})
	// A task with a push configuration must enqueue a push delivery too.
	pushed := mustSubmit(t, svc, e.scope, e.caller, "msg-pushed", func(r *a2a.SendMessageRequest) {
		r.Config.PushConfig = &a2a.PushConfig{URL: "https://callbacks.example.invalid/a2a-hook"}
	})
	plain := mustSubmit(t, svc, e.scope, e.caller, "msg-plain", nil)

	var mu sync.Mutex
	kindsByTask := map[string][]string{}
	var deliveryErr error
	runner, err := gateway.NewOutboxRunner(e.store, e.ledger, map[string]gateway.Sink{
		gateway.OutboxKindPush:        gateway.NullSink{},
		gateway.OutboxKindChildStatus: gateway.NullSink{},
	}, gateway.OutboxConfig{WorkerID: "obx-" + e.sfx[:8], Poll: 10 * time.Millisecond, Batch: 16, MaxAttempts: 3, RetryAfter: 10 * time.Millisecond}, gateway.NullNotifier{})
	if err != nil {
		t.Fatalf("NewOutboxRunner: %v", err)
	}

	// Run the worker so tasks settle and their terminal transitions enqueue.
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go svc.Run(runCtx)
	go func() { _ = runner.Run(runCtx) }()

	waitFor(t, 15*time.Second, func() bool {
		rows, err := e.pool.Query(ctx, `SELECT task_id, kind, state FROM `+e.schema+`.outbox ORDER BY seq`)
		if err != nil {
			deliveryErr = err
			return false
		}
		defer rows.Close()
		mu.Lock()
		kindsByTask = map[string][]string{}
		mu.Unlock()
		var total, done int
		for rows.Next() {
			var taskID, kind, state string
			if err := rows.Scan(&taskID, &kind, &state); err != nil {
				deliveryErr = err
				return false
			}
			mu.Lock()
			kindsByTask[taskID] = append(kindsByTask[taskID], kind)
			mu.Unlock()
			total++
			if state == gateway.OutboxDone {
				done++
			}
		}
		return total >= 2 && done == total
	}, "outbox deliveries never settled")

	if deliveryErr != nil {
		t.Fatalf("outbox query: %v", deliveryErr)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := kindsByTask[string(child.ID)]; len(got) != 1 || got[0] != gateway.OutboxKindChildStatus {
		t.Fatalf("child task outbox kinds = %v, want exactly [%s]", got, gateway.OutboxKindChildStatus)
	}
	if got := kindsByTask[string(pushed.ID)]; len(got) != 1 || got[0] != gateway.OutboxKindPush {
		t.Fatalf("pushed task outbox kinds = %v, want exactly [%s]", got, gateway.OutboxKindPush)
	}
	if got := kindsByTask[string(plain.ID)]; len(got) != 0 {
		t.Fatalf("plain task must not enqueue deliveries, got %v", got)
	}
}

// TestChildStatusDeliveryIdempotent proves the receiving side of child status
// delivery is idempotent: a byte-identical redelivery is acknowledged as
// DUPLICATE without re-invoking the observer, a superseded version is STALE, a
// conflicting reuse of the same version with a different body is rejected, and
// Applied reports the highest version durably applied.
func TestChildStatusDeliveryIdempotent(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// The parent must be a task this scope actually owns.
	parent := mustSubmit(t, svc, e.scope, e.caller, "msg-cs-parent", nil)
	parentID := string(parent.ID)
	childID := "child-cs-" + e.sfx

	inbox, err := gateway.NewChildStatusInbox(e.store)
	if err != nil {
		t.Fatalf("NewChildStatusInbox: %v", err)
	}
	var obs atomic.Int64
	obsFn := gateway.ChildStatusObserverFunc(func(context.Context, gateway.ChildStatusEvent) error {
		obs.Add(1)
		return nil
	})

	v1 := gateway.ChildStatusPayload{ParentTaskID: parentID, TaskID: childID, State: "working", StatusVersion: 1, FencingToken: 5}
	v2 := gateway.ChildStatusPayload{ParentTaskID: parentID, TaskID: childID, State: "completed", StatusVersion: 2, FencingToken: 5}

	// First delivery of v1 is accepted and notifies the observer once.
	if out, err := inbox.Receive(ctx, e.scope, "k-v1", v1, obsFn); err != nil || out != gateway.ChildStatusAccepted {
		t.Fatalf("Receive(v1) = %q, %v, want ACCEPTED", out, err)
	}
	// Byte-identical redelivery is a duplicate and does not notify again.
	if out, err := inbox.Receive(ctx, e.scope, "k-v1-retry", v1, obsFn); err != nil || out != gateway.ChildStatusDuplicate {
		t.Fatalf("redeliver(v1) = %q, %v, want DUPLICATE", out, err)
	}
	if got := obs.Load(); got != 1 {
		t.Fatalf("observer fired %d times after duplicate, want 1", got)
	}

	// A newer version is accepted.
	if out, err := inbox.Receive(ctx, e.scope, "k-v2", v2, obsFn); err != nil || out != gateway.ChildStatusAccepted {
		t.Fatalf("Receive(v2) = %q, %v, want ACCEPTED", out, err)
	}
	// Replaying the older version now that a newer one is known is STALE.
	if out, err := inbox.Receive(ctx, e.scope, "k-v1-late", v1, obsFn); err != nil || out != gateway.ChildStatusStale {
		t.Fatalf("stale(v1) = %q, %v, want STALE", out, err)
	}
	// A conflicting body under an already-applied version is rejected.
	conflict := v2
	conflict.State = "failed"
	if _, err := inbox.Receive(ctx, e.scope, "k-v2-conflict", conflict, obsFn); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("conflicting v2 = %v, want ErrKeyReuse", err)
	}
	if got := obs.Load(); got != 2 {
		t.Fatalf("observer fired %d times, want 2 (only ACCEPTED deliveries notify)", got)
	}

	if max, ok, err := inbox.Applied(ctx, e.scope, childID); err != nil || !ok || max != 2 {
		t.Fatalf("Applied = %d,%v,%v, want 2,true,nil", max, ok, err)
	}

	// A delivery for a parent the receiver does not own is refused, not adopted.
	foreign := contracts.Scope{Tenant: "t-" + e.sfx + "-foreign", Deployment: e.scope.Deployment, Subject: e.scope.Subject}
	if _, err := inbox.Receive(ctx, foreign, "k-foreign", v2, obsFn); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign-scope delivery = %v, want ErrNotFound", err)
	}
}

// requireRealGatewayDB returns the DSN of the real PostgreSQL instance these
// durability tests are meaningless without. Missing dependency = FAIL with the
// reason and the remedy; the only downgrade is AHP_ALLOW_SKIP_REAL_DB=1, which
// logs SKIP_EXPLICIT so a green run can never be claimed over an unexercised
// dependency.
func requireRealGatewayDB(t *testing.T) string {
	t.Helper()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url != "" {
		return url
	}
	if os.Getenv("AHP_ALLOW_SKIP_REAL_DB") == "1" {
		t.Log("SKIP_EXPLICIT AHP_TEST_DATABASE_URL unset -- opt-out skip, NOT counted as acceptance")
		t.Skip("SKIP_EXPLICIT: real PostgreSQL required for gateway durability tests")
	}
	t.Fatalf("missing real dependency: AHP_TEST_DATABASE_URL is not set, so the gateway " +
		"schema was never created and nothing durable was proven. Export the isolated " +
		"test DSN (see private/root-acceptance-pg/) and rerun, or set " +
		"AHP_ALLOW_SKIP_REAL_DB=1 to downgrade this to a SKIP_EXPLICIT skip that acceptance does not count.")
	return ""
}
