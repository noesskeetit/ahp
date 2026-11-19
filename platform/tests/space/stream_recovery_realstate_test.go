//go:build state_integration

package space_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
)

type streamWorld struct {
	e           *cEnv
	pool        *pgxpool.Pool
	remote      *execbridge.RemoteModel
	fixture     *wireStreamProvider
	scope       contracts.Scope
	ids         *identity.Store
	modelClient *http.Client
	url         string
}

func newStreamWorld(t *testing.T, realAuthority bool) *streamWorld {
	e := cNewEnv(t)
	pool, err := pgxpool.New(e.ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	world := &streamWorld{e: e, pool: pool, scope: contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}, fixture: &wireStreamProvider{release: make(chan struct{})}}
	authorize := transport.Authorize(func(context.Context, auth.Claims, string) error { return nil })
	if realAuthority {
		world.ids = identity.New(pool)
		if err = world.ids.Migrate(e.ctx); err != nil {
			t.Fatal(err)
		}
		authorize = world.ids.Authorize
		handler, e2 := stateapi.New(state.New(pool), e.verifier, authorize, func(ctx context.Context, scope contracts.Scope, task string) (string, error) {
			return world.ids.MintTask(ctx, scope, task, e.signer, cAudience, 5*time.Minute)
		})
		if e2 != nil {
			t.Fatal(e2)
		}
		e.srv.Config.Handler = handler
	}
	bridge, err := execbridge.New(e.ctx, pool, execbridge.Config{Inference: world.fixture})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(bridge.ModelAPI(e.verifier, authorize))
	srv.TLS = e.srv.TLS.Clone()
	srv.StartTLS()
	t.Cleanup(srv.Close)
	hc := srv.Client()
	tr := hc.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{e.ca.clientCert(t, cWorkloadURI(e.tenant, e.deployment))}
	hc.Transport = tr
	world.remote, err = execbridge.NewRemoteModel(srv.URL, "", hc)
	if err != nil {
		t.Fatal(err)
	}
	world.modelClient, world.url = hc, srv.URL
	t.Cleanup(func() {
		select {
		case <-world.fixture.release:
		default:
			close(world.fixture.release)
		}
	})
	return world
}

type pauseAtBoundary struct {
	inner       space.CoreRunner
	beforeModel bool
	hit         chan struct{}
	resume      chan struct{}
	done        chan struct{}
}

func (p *pauseAtBoundary) Run(ctx context.Context, req *space.CoreRequest, out chan<- space.StepResult) error {
	defer close(p.done)
	ch := make(chan space.StepResult, 128)
	joined := make(chan error, 1)
	go func() { joined <- p.inner.Run(ctx, req, ch); close(ch) }()
	for result := range ch {
		if (p.beforeModel && result.StepType == space.StepModel) || (!p.beforeModel && result.StepType == space.StepDone) {
			close(p.hit)
			select {
			case <-p.resume:
			case <-ctx.Done():
			}
		}
		select {
		case out <- result:
		case <-ctx.Done():
		}
	}
	return <-joined
}

func TestStreamRecoveryWindowsNoDuplicateInferenceBudgetOrAssistant_RealState(t *testing.T) {
	for _, beforeModel := range []bool{true, false} {
		t.Run(fmt.Sprintf("generation_before_model_frame_%v", beforeModel), func(t *testing.T) {
			w := newStreamWorld(t, false)
			e := w.e
			close(w.fixture.release)
			originalHandler := e.srv.Config.Handler
			// Keep checkpoint unavailable throughout both lives: recovery must use the
			// committed model intent and result, never a guessed in-memory offset.
			e.srv.Config.Handler = http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/PutCheckpoint") {
					http.Error(rw, "checkpoint unavailable", 503)
					return
				}
				originalHandler.ServeHTTP(rw, r)
			})
			runtime := core.NewRuntime(core.RuntimeConfig{Model: w.remote, MaxTokens: 128})
			paused := &pauseAtBoundary{inner: runtime, beforeModel: beforeModel, hit: make(chan struct{}), resume: make(chan struct{}), done: make(chan struct{})}
			first := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: paused, WorkerID: "old"})
			token := e.signTask(t, "task-window")
			acc, err := first.AcceptTurn(e.ctx, token, w.scope, space.AcceptTurnRequest{Key: "window", ContextID: "ctx-window", Input: json.RawMessage(`"original business bytes"`), MaxSteps: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err = first.DispatchTurn(e.ctx, token, w.scope); err != nil {
				t.Fatal(err)
			}
			select {
			case <-paused.hit:
			case <-time.After(5 * time.Second):
				t.Fatal("did not hit crash window")
			}
			if !beforeModel {
				deadline := time.Now().Add(3 * time.Second)
				for {
					events, er := e.client.ReadStream(e.ctx, token, acc.ThreadID, 0, 100)
					if er != nil {
						t.Fatal(er)
					}
					found := false
					for _, ev := range events {
						if ev.Kind == "model" {
							found = true
						}
					}
					if found {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("model frame not committed")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			if _, err = w.pool.Exec(e.ctx, `UPDATE ahp_state.threads SET lease_expires=now()-interval '1 second' WHERE id=$1`, acc.ThreadID); err != nil {
				t.Fatal(err)
			}
			second := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: core.NewRuntime(core.RuntimeConfig{Model: w.remote, MaxTokens: 256}), WorkerID: "new"})
			// Deliberately change the new runner's max_tokens: the pending intent must
			// preserve the original 128 instead of changing accepted business bytes.
			if err = second.DispatchTurn(e.ctx, token, w.scope); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				run, er := e.client.GetRun(e.ctx, token, acc.TurnID)
				if er != nil {
					t.Fatal(er)
				}
				if run.Status == "done" {
					break
				}
				if run.Status == "failed" || time.Now().After(deadline) {
					t.Fatalf("recovery status=%s", run.Status)
				}
				time.Sleep(20 * time.Millisecond)
			}
			close(paused.resume)
			select {
			case <-paused.done:
			case <-time.After(time.Second):
				t.Fatal("old runner leak")
			}
			deadline = time.Now().Add(time.Second)
			for {
				old, _ := first.TurnSnapshot(acc.TurnID)
				if old.Status == space.TurnUnknown {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("old owner did not fence")
				}
				time.Sleep(10 * time.Millisecond)
			}
			events, err := e.client.ReadStream(e.ctx, token, acc.ThreadID, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			models := 0
			for _, ev := range events {
				if ev.Kind == "model" {
					models++
				}
			}
			var generations, reservations int
			var spent int64
			if err = w.pool.QueryRow(e.ctx, `SELECT count(*) FROM ahp_model.generations WHERE tenant=$1`, w.scope.Tenant).Scan(&generations); err != nil {
				t.Fatal(err)
			}
			if err = w.pool.QueryRow(e.ctx, `SELECT count(*),sum(actual) FROM ahp_model.budget_reservations WHERE budget_id IN (SELECT id FROM ahp_model.budgets WHERE tenant=$1)`, w.scope.Tenant).Scan(&reservations, &spent); err != nil {
				t.Fatal(err)
			}
			if models != 1 || generations != 1 || reservations != 1 || spent != 7 || w.fixture.calls.Load() != 1 {
				t.Fatalf("duplicates model=%d generation=%d reservations=%d charge=%d calls=%d", models, generations, reservations, spent, w.fixture.calls.Load())
			}
		})
	}
}

// This test seam performs the real Identity checks and mutations. State and
// Model still require actual signed task JWTs over mTLS. It only avoids adding
// an unrelated Identity HTTP server to the cancellation-order fixture.
type storeRunIssuer struct {
	w          *streamWorld
	childToken string
	revoked    atomic.Bool
}

func (i *storeRunIssuer) Delegate(ctx context.Context, parentToken, parentTask string, spec space.RunGrantSpec) (string, error) {
	claims, err := i.w.e.verifier.Verify(parentToken, cAudience, auth.KindTask, auth.WorkloadID(i.w.scope))
	if err != nil {
		return "", err
	}
	if err = i.w.ids.Authorize(ctx, claims, "identity:delegate"); err != nil {
		return "", err
	}
	_, err = i.w.ids.Delegate(ctx, i.w.scope, parentTask, identity.DelegateRequest{Task: spec.Task, Context: spec.Context, Deployment: i.w.scope.Deployment, Capabilities: spec.Capabilities, TTL: spec.TTL})
	if err != nil {
		return "", err
	}
	i.childToken, err = i.w.ids.MintTask(ctx, i.w.scope, spec.Task, i.w.e.signer, cAudience, time.Minute)
	return i.childToken, err
}
func (i *storeRunIssuer) Cancel(ctx context.Context, token, task string) error {
	claims, err := i.w.e.verifier.Verify(token, cAudience, auth.KindTask, auth.WorkloadID(i.w.scope))
	if err != nil {
		return err
	}
	if err = i.w.ids.Authorize(ctx, claims, "identity:cancel"); err != nil {
		return err
	}
	err = i.w.ids.CancelSubtree(ctx, i.w.scope, task)
	if err == nil {
		i.revoked.Store(true)
	}
	return err
}
func TestCancelTurnRevokesRealIdentityBeforeModelCleanup_RealState(t *testing.T) {
	w := newStreamWorld(t, true)
	e := w.e
	caps := []string{"state", "space", "model", "identity:delegate", "identity:cancel"}
	_, err := w.ids.CreateRoot(e.ctx, w.scope, identity.RootRequest{Task: "parent", Context: contracts.NewID(), Deployment: w.scope.Deployment, Capabilities: caps, TTL: time.Hour, AllowSelf: true})
	if err != nil {
		t.Fatal(err)
	}
	token, err := w.ids.MintTask(e.ctx, w.scope, "parent", e.signer, cAudience, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	issuer := &storeRunIssuer{w: w}
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: core.NewRuntime(core.RuntimeConfig{Model: w.remote, MaxTokens: 128}), WorkerID: "cancel-worker", RunGrantIssuer: issuer, RequireRunGrant: true, RunGrantCaps: caps})
	ctx := space.WithAuthorityTask(e.ctx, "parent")
	// The conversation id is the run grant's context claim, so it must be a real
	// conversation identifier: unique per conversation, like contracts.NewID()
	// mints in production. Identity claims a context id for the first scope that
	// uses it and reports a foreign reuse as NOT_FOUND, so a hardcoded id reused
	// by the next run of this suite (a new tenant, the same persistent database)
	// would be refused for a reason that has nothing to do with cancellation.
	cancelChat := "cancel-chat-" + contracts.NewID()
	acc, err := coord.AcceptTurn(ctx, token, w.scope, space.AcceptTurnRequest{Key: "cancel", ContextID: cancelChat, Input: json.RawMessage(`"go"`), MaxSteps: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = coord.DispatchTurn(ctx, token, w.scope); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		events, er := e.client.ReadStream(e.ctx, token, acc.ThreadID, 0, 100)
		if er != nil {
			t.Fatal(er)
		}
		found := false
		for _, ev := range events {
			if ev.Kind == "model_delta" {
				found = true
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no first delta")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err = coord.CancelTurn(ctx, acc.TurnID, token); err != nil {
		t.Fatal(err)
	}
	if !issuer.revoked.Load() {
		t.Fatal("grant not revoked")
	}
	var id string
	if err = w.pool.QueryRow(e.ctx, `SELECT id FROM ahp_model.generations WHERE tenant=$1`, w.scope.Tenant).Scan(&id); err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(e.ctx, http.MethodPost, w.url+"/v1/model/CancelModel", strings.NewReader(`{"id":"`+id+`"}`))
	request.Header.Set("Authorization", "Bearer "+issuer.childToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := w.modelClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 403 {
		t.Fatalf("revoked cleanup accepted: %d", response.StatusCode)
	}
	deadline = time.Now().Add(4 * time.Second)
	for {
		var status, effect string
		var settled bool
		err = w.pool.QueryRow(e.ctx, `SELECT g.status,o.status,r.settled FROM ahp_model.generations g JOIN ahp_model.operations o ON o.key=g.effect_key AND o.tenant=g.tenant JOIN ahp_model.budget_reservations r ON r.budget_id=g.budget_id AND r.key=g.reserve_key WHERE g.id=$1`, id).Scan(&status, &effect, &settled)
		if err != nil {
			t.Fatal(err)
		}
		if status == model.GenCanceled {
			if effect != "SIDE_EFFECT_UNKNOWN" || settled {
				t.Fatalf("cancel accounting %s %v", effect, settled)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("revoked stalled provider still %s", status)
		}
		time.Sleep(30 * time.Millisecond)
	}
	if w.fixture.calls.Load() != 1 {
		t.Fatal("cancel redispatched provider")
	}
}

type toolThenTextProvider struct{ calls atomic.Int32 }

func (p *toolThenTextProvider) Model() string { return "tool-recovery-fixture" }
func (p *toolThenTextProvider) Stream(context.Context, model.ChatRequest) (model.Stream, error) {
	return &toolThenTextStream{tool: p.calls.Add(1) == 1}, nil
}

type toolThenTextStream struct {
	tool bool
	n    int
}

func (s *toolThenTextStream) Next(context.Context) (model.Delta, error) {
	s.n++
	if s.n > 1 {
		return model.Delta{}, model.ErrStreamDone
	}
	if s.tool {
		return model.Delta{FinishReason: "tool_calls", ToolCalls: []model.ToolCall{{ID: "effect-one", Name: "effect", Arguments: json.RawMessage(`{}`)}}}, nil
	}
	return model.Delta{Text: "finished", FinishReason: "stop"}, nil
}
func (s *toolThenTextStream) Usage() model.Usage { return model.Usage{TotalTokens: 7} }
func (s *toolThenTextStream) Close() error       { return nil }

type countingEffect struct{ calls atomic.Int32 }

func (e *countingEffect) Execute(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	e.calls.Add(1)
	return json.RawMessage(`{"effect":"once"}`), nil
}

type pauseBeforeModelIntent struct {
	inner             space.CoreRunner
	hit, resume, done chan struct{}
}

func (p *pauseBeforeModelIntent) Run(ctx context.Context, req *space.CoreRequest, out chan<- space.StepResult) error {
	defer close(p.done)
	journal := req.JournalModel
	req.JournalModel = func(ctx context.Context, step, attempt int, body json.RawMessage) error {
		if step == 1 {
			close(p.hit)
			select {
			case <-p.resume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return journal(ctx, step, attempt, body)
	}
	return p.inner.Run(ctx, req, out)
}
func TestModelReplayKeepsCompletedToolAndEffectCoordinate_RealState(t *testing.T) {
	w := newStreamWorld(t, false)
	e := w.e
	inf := &toolThenTextProvider{}
	bridge, err := execbridge.New(e.ctx, w.pool, execbridge.Config{Inference: inf})
	if err != nil {
		t.Fatal(err)
	}
	// This owns a new fixture Model listener; no live stand is modified.
	srv := httptest.NewUnstartedServer(bridge.ModelAPI(e.verifier, func(context.Context, auth.Claims, string) error { return nil }))
	srv.TLS = e.srv.TLS.Clone()
	srv.StartTLS()
	defer srv.Close()
	hc := srv.Client()
	tr := hc.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{e.ca.clientCert(t, cWorkloadURI(e.tenant, e.deployment))}
	hc.Transport = tr
	remote, err := execbridge.NewRemoteModel(srv.URL, "", hc)
	if err != nil {
		t.Fatal(err)
	}
	effect := &countingEffect{}
	registry := core.NewToolRegistry(effect)
	registry.Register(core.ToolDef{Name: "effect", Parameters: json.RawMessage(`{"type":"object"}`)})
	runtime := core.NewRuntime(core.RuntimeConfig{Model: remote, State: e.client, Tools: registry, MaxTokens: 128})
	paused := &pauseBeforeModelIntent{inner: runtime, hit: make(chan struct{}), resume: make(chan struct{}), done: make(chan struct{})}
	old := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: paused, WorkerID: "tool-old"})
	token := e.signTask(t, "tool-window")
	acc, err := old.AcceptTurn(e.ctx, token, w.scope, space.AcceptTurnRequest{Key: "tool-window", ContextID: "ctx-tool-window", Input: json.RawMessage(`"go"`), MaxSteps: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err = old.DispatchTurn(e.ctx, token, w.scope); err != nil {
		t.Fatal(err)
	}
	select {
	case <-paused.hit:
	case <-time.After(5 * time.Second):
		t.Fatal("next model intent not reached")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		frames, er := e.client.ReadStream(e.ctx, token, acc.ThreadID, 0, 100)
		if er != nil {
			t.Fatal(er)
		}
		found := false
		for _, f := range frames {
			if f.Kind == "tool" {
				found = true
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tool not durable")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err = w.pool.Exec(e.ctx, `UPDATE ahp_state.threads SET lease_expires=now()-interval '1 second' WHERE id=$1`, acc.ThreadID); err != nil {
		t.Fatal(err)
	}
	next := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: runtime, WorkerID: "tool-new"})
	if err = next.DispatchTurn(e.ctx, token, w.scope); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		run, er := e.client.GetRun(e.ctx, token, acc.TurnID)
		if er != nil {
			t.Fatal(er)
		}
		if run.Status == "done" {
			break
		}
		if run.Status == "failed" || time.Now().After(deadline) {
			t.Fatalf("recovery %s", run.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(paused.resume)
	select {
	case <-paused.done:
	case <-time.After(time.Second):
		t.Fatal("old core leaked")
	}
	var intents int
	if err = w.pool.QueryRow(e.ctx, `SELECT count(*) FROM ahp_state.intents WHERE run_id=$1`, acc.TurnID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	frames, err := e.client.ReadStream(e.ctx, token, acc.ThreadID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	models, tools := 0, 0
	for _, f := range frames {
		if f.Kind == "model" {
			models++
		}
		if f.Kind == "tool" {
			tools++
		}
	}
	if inf.calls.Load() != 2 || effect.calls.Load() != 1 || intents != 1 || models != 2 || tools != 1 {
		t.Fatalf("replay duplicated: provider=%d effect=%d intents=%d model frames=%d tool frames=%d", inf.calls.Load(), effect.calls.Load(), intents, models, tools)
	}
}

func TestSecondarySubscriptionRejectsExpiredJWT_RealState(t *testing.T) {
	w := newStreamWorld(t, true)
	contextID := contracts.NewID()
	_, err := w.ids.CreateRoot(w.e.ctx, w.scope, identity.RootRequest{Task: "review-expiry", Context: contextID, Deployment: w.scope.Deployment, Capabilities: []string{"model"}, TTL: time.Hour, AllowSelf: true})
	if err != nil {
		t.Fatal(err)
	}
	longToken, err := w.ids.MintTask(w.e.ctx, w.scope, "review-expiry", w.e.signer, cAudience, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bind := func(token string) context.Context {
		return core.WithRunBinding(core.WithRunScope(ctx, w.scope, token), core.RunBinding{TaskID: "review-expiry", ContextID: contextID})
	}
	req := &core.ModelRequest{Messages: []core.ChatMessage{{Role: "user", Content: json.RawMessage(`"go"`)}}, MaxTokens: 128}
	first := make(chan struct{}, 1)
	ownerDone := make(chan error, 1)
	go func() {
		_, e := w.remote.GenerateStream(bind(longToken), req, func(d core.ModelDelta) error {
			if d.Sequence == 1 {
				first <- struct{}{}
			}
			return nil
		})
		ownerDone <- e
	}()
	select {
	case <-first:
	case <-ctx.Done():
		t.Fatal("owner never emitted")
	}
	shortToken, err := w.ids.MintTask(w.e.ctx, w.scope, "review-expiry", w.e.signer, cAudience, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var lateDeltas atomic.Int32
	var expired atomic.Bool
	second := make(chan struct{}, 1)
	observerDone := make(chan error, 1)
	go func() {
		_, e := w.remote.GenerateStream(bind(shortToken), req, func(d core.ModelDelta) error {
			if d.Sequence == 1 {
				second <- struct{}{}
			}
			if expired.Load() {
				lateDeltas.Add(1)
			}
			return nil
		})
		observerDone <- e
	}()
	select {
	case <-second:
	case <-ctx.Done():
		t.Fatal("observer never emitted")
	}
	time.Sleep(3 * time.Second)
	expired.Store(true)
	close(w.fixture.release)
	observerErr := <-observerDone
	ownerErr := <-ownerDone
	if ownerErr != nil {
		t.Fatalf("long-token owner failed: %v", ownerErr)
	}
	if observerErr == nil || lateDeltas.Load() != 0 {
		t.Fatalf("expired subscriber consumed late_deltas=%d and completed=%v; provider_calls=%d", lateDeltas.Load(), observerErr == nil, w.fixture.calls.Load())
	}
}
