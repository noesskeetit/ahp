package runcoord

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/pkg/transport"
	stateclient "github.com/ahp-platform/ahp/sdk/go/state/journal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestVMTurnNotificationsPostgres(t *testing.T) {
	ctx, pool, db := openPG(t)
	listener, err := pgx.ConnectConfig(ctx, pool.Config().ConnConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close(context.Background())
	if _, err := listener.Exec(ctx, "LISTEN "+journal.SessionSignalChannel); err != nil {
		t.Fatal(err)
	}
	barrier := "test-barrier-" + db.scope.Tenant
	checkSignals := func(t *testing.T, session string, want int) {
		t.Helper()
		// Commit the marker after the operation returns. PostgreSQL delivers
		// notifications in commit order, so it also bounds the no-op checks.
		if _, err := pool.Exec(ctx, "SELECT pg_notify($1,$2)", journal.SessionSignalChannel, barrier); err != nil {
			t.Fatal(err)
		}
		wait, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		var got int
		for {
			msg, err := listener.WaitForNotification(wait)
			if err != nil {
				t.Fatal(err)
			}
			if msg.Payload == barrier {
				break
			}
			var signal journal.SessionSignal
			if json.Unmarshal([]byte(msg.Payload), &signal) == nil && signal == (SessionSignal{Scope: db.scope, SessionID: session}) {
				got++
			}
		}
		if got != want {
			t.Errorf("received %d journal notifications for %q, want %d", got, session, want)
		}
	}

	for _, tc := range []struct {
		name, session string
	}{
		{"enqueue", "enqueue"},
		{"queued cancel", "queued"},
		{"active cancel", "active"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := journal.EnqueueRequest{
				SessionID: tc.session, Harness: "go",
				FormatVersion: 1, Key: "initial", ExecutionMode: journal.ExecutionVMManaged,
				Input: journal.Payload{Inline: []byte(`{"message":"first"}`)},
			}
			in, err := db.EnqueueTurn(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			checkSignals(t, req.SessionID, 1)
			if tc.name == "enqueue" {
				if replay, err := db.EnqueueTurn(ctx, req); err != nil || !replay.Replayed {
					t.Fatalf("replayed admission: %+v %v", replay, err)
				}
				checkSignals(t, req.SessionID, 0)
				return
			}

			wantStatus := "INTERRUPTED"
			wantHead := in.HeadSeq + 1
			if tc.name == "active cancel" {
				_, err := db.store.ClaimVMTurn(ctx, db.scope, req.SessionID, journal.VMTurnClaimRequest{
					TurnSeq: in.HeadSeq, Generation: 1, ExecutionID: journal.DeterministicExecutionID(db.scope, req.SessionID, in.HeadSeq, 1),
				}, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				wantStatus, wantHead = "ACTIVE", in.HeadSeq
				if _, err := db.store.CancelTurn(ctx, db.scope, req.SessionID, in.HeadSeq+1); contracts.Code(err) != contracts.ErrConflict {
					t.Fatalf("cancel stale turn: %v", err)
				}
				checkSignals(t, req.SessionID, 0)
			}
			canceled, err := db.store.CancelTurn(ctx, db.scope, req.SessionID, in.HeadSeq)
			if err != nil || canceled.Status != wantStatus || canceled.HeadSeq != wantHead {
				t.Fatalf("cancel turn: %+v %v", canceled, err)
			}
			checkSignals(t, req.SessionID, 1)
			if _, err := db.store.CancelTurn(ctx, db.scope, req.SessionID, in.HeadSeq); err != nil {
				t.Fatalf("repeated cancellation: %v", err)
			}
			checkSignals(t, req.SessionID, 0)
		})
	}
}

type liveSSEFrame struct {
	id, event, data string
}

func TestSSEStateReadRetry(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		code       string
		failures   int
		failProbe  bool
		cancelRead bool
		wantReads  []string
		wantFrames []string
	}{
		{name: "transient tail", status: 503, code: contracts.ErrUnavailable, failures: 1,
			wantReads: []string{"0/1", "0/128", "2/128", "2/128"}, wantFrames: []string{"journal:2", "journal:3", "journal:4", "end:"}},
		{name: "transient probe", status: 503, code: contracts.ErrUnavailable, failures: 1, failProbe: true,
			wantReads: []string{"0/1", "0/1", "0/128", "2/128"}, wantFrames: []string{"journal:2", "journal:3", "journal:4", "end:"}},
		{name: "exhausted", status: 503, code: contracts.ErrUnavailable, failures: 3,
			wantReads: []string{"0/1", "0/128", "2/128", "2/128"}, wantFrames: []string{"journal:2", "error:"}},
		{name: "forbidden", status: 403, code: contracts.ErrForbidden, failures: 1,
			wantReads: []string{"0/1", "0/128", "2/128"}, wantFrames: []string{"journal:2", "error:"}},
		{name: "internal", status: 500, code: "INTERNAL", failures: 1,
			wantReads: []string{"0/1", "0/128", "2/128"}, wantFrames: []string{"journal:2", "error:"}},
		{name: "stream canceled", status: 503, code: contracts.ErrUnavailable, failures: 1, cancelRead: true,
			wantReads: []string{"0/1", "0/128", "2/128"}, wantFrames: []string{"journal:2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var (
				mu    sync.Mutex
				reads []string
			)
			remaining := tc.failures
			first := journal.Event{Seq: 1, EventInput: journal.EventInput{TurnSeq: 1, Type: "turn.input"}}
			delta := journal.Event{Seq: 2, EventInput: journal.EventInput{TurnSeq: 1, Type: "delta"}}
			reply := journal.Event{Seq: 3, EventInput: journal.EventInput{TurnSeq: 1, Type: "turn.output"}}
			end := journal.Event{Seq: 4, EventInput: journal.EventInput{TurnSeq: 1, Type: "turn.completed",
				Payload: journal.Payload{Inline: []byte(`{"turn_seq":1,"end_seq":4,"outcome":"IDLE"}`)}}}
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				query := r.URL.Query()
				key := query.Get("after_seq") + "/" + query.Get("limit")
				mu.Lock()
				defer mu.Unlock()
				reads = append(reads, key)
				if remaining > 0 && (tc.failProbe && key == "0/1" || !tc.failProbe && key == "2/128") {
					remaining--
					if tc.cancelRead {
						cancel()
					}
					transport.WriteJSON(w, tc.status, &contracts.Error{Code: tc.code})
					return
				}
				switch key {
				case "0/1":
					transport.WriteJSON(w, http.StatusOK, journal.EventPage{HeadSeq: 2, Events: []journal.Event{first}})
				case "0/128":
					transport.WriteJSON(w, http.StatusOK, journal.EventPage{HeadSeq: 2, Events: []journal.Event{first, delta}})
				case "2/128":
					transport.WriteJSON(w, http.StatusOK, journal.EventPage{HeadSeq: 4, Events: []journal.Event{reply, end}})
				default:
					transport.WriteJSON(w, http.StatusBadRequest, &contracts.Error{Code: contracts.ErrInvalid})
				}
			}))
			defer upstream.Close()
			client, err := stateclient.New(stateclient.Config{
				BaseURL: upstream.URL, HTTPClient: upstream.Client(),
				TokenSource: func(context.Context) (string, error) { return "state-token", nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			scope := contracts.Scope{Tenant: "tenant", Deployment: "agent", Subject: "user"}
			r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/stream?turn_seq=1&after_seq=4", nil)
			r.Header.Set("Last-Event-ID", "1")
			w := httptest.NewRecorder()
			c := Coordinator{}
			c.stream(w, r, client.Coordinator().ForScope(scope), SessionSignal{Scope: scope, SessionID: "s"})
			assertSSEFrames(t, w, tc.wantFrames)
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(reads, tc.wantReads) {
				t.Fatalf("State read cursors/limits = %v, want %v", reads, tc.wantReads)
			}
		})
	}
}

func assertSSEFrames(t *testing.T, response *httptest.ResponseRecorder, want []string) {
	t.Helper()
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("expected SSE response, got %d (%s) %s", response.Code, response.Header().Get("Content-Type"), response.Body)
	}
	var frames []string
	var id string
	for line := range strings.SplitSeq(response.Body.String(), "\n") {
		switch {
		case strings.HasPrefix(line, "id: "):
			id = line[4:]
		case strings.HasPrefix(line, "event: "):
			frames = append(frames, line[7:]+":"+id)
			id = ""
		}
	}
	if !slices.Equal(frames, want) {
		t.Fatalf("SSE frames = %v, want %v\n%s", frames, want, response.Body)
	}
}

type observedSSEState struct {
	State
	caughtUp chan struct{}
}

func (s observedSSEState) GetEvents(ctx context.Context, session string, after int64, limit int, ns string) (journal.EventPage, error) {
	page, err := s.State.GetEvents(ctx, session, after, limit, ns)
	if err == nil && after == 1 && page.HeadSeq == 1 {
		select {
		case s.caughtUp <- struct{}{}:
		default:
		}
	}
	return page, err
}

func TestSSELivePostgresSignalAndFallback(t *testing.T) {
	for _, tc := range []struct {
		name        string
		listen      bool
		minDuration time.Duration
		maxDuration time.Duration
	}{
		{name: "notification", listen: true, maxDuration: time.Second},
		{name: "lost notification", minDuration: 1750 * time.Millisecond, maxDuration: 4 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbCtx, pool, db := openPG(t)
			ctx, cancel := context.WithTimeout(dbCtx, 8*time.Second)
			defer cancel()
			const session = "wake"
			if _, err := db.EnqueueTurn(ctx, journal.EnqueueRequest{
				SessionID: session, Harness: "go", FormatVersion: 1, Key: "initial",
				ExecutionMode: journal.ExecutionVMManaged, Input: journal.Payload{Inline: []byte(`{}`)},
			}); err != nil {
				t.Fatal(err)
			}
			bus := NewPGSignalBus(pool)
			if tc.listen {
				busDone := make(chan struct{})
				go func() {
					defer close(busDone)
					bus.Run(ctx)
				}()
				defer func() { cancel(); <-busDone }()
				waitForPostgresSSEListener(t, ctx, pool, bus, SessionSignal{Scope: db.scope, SessionID: "probe"})
			}
			state := observedSSEState{State: db, caughtUp: make(chan struct{}, 1)}
			c := Coordinator{Signals: bus}
			request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/stream?turn_seq=1", nil)
			w := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				defer close(done)
				c.stream(w, request, state, SessionSignal{Scope: db.scope, SessionID: session})
			}()
			defer func() { cancel(); <-done }()
			select {
			case <-state.caughtUp:
			case <-done:
				t.Fatalf("SSE closed before catching up: %s", w.Body)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			start := time.Now()
			if _, err := db.CancelSession(ctx, session, 1); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(tc.maxDuration):
				t.Fatalf("terminal event did not reach SSE within %s", tc.maxDuration)
			}
			elapsed := time.Since(start)
			if elapsed < tc.minDuration || elapsed > tc.maxDuration {
				t.Fatalf("terminal event reached SSE in %s, want %s–%s", elapsed, tc.minDuration, tc.maxDuration)
			}
			t.Logf("terminal event reached SSE in %s", elapsed)
			assertSSEFrames(t, w, []string{"journal:1", "journal:2", "end:"})
		})
	}
}

func waitForPostgresSSEListener(t *testing.T, ctx context.Context, pool *pgxpool.Pool, bus *PGSignalBus, probe SessionSignal) {
	t.Helper()
	ready, release := bus.Subscribe(probe)
	defer release()
	payload, err := json.Marshal(probe)
	if err != nil {
		t.Fatal(err)
	}
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := pool.Exec(ctx, "SELECT pg_notify($1,$2)", journal.SessionSignalChannel, string(payload)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ready:
			return
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestSSEConcurrentReadersLiveTurn(t *testing.T) {
	dbCtx, pool, db := openPG(t)
	ctx, cancel := context.WithTimeout(dbCtx, 15*time.Second)
	defer cancel()
	bus := NewPGSignalBus(pool)
	var background sync.WaitGroup
	background.Go(func() { bus.Run(ctx) })
	t.Cleanup(background.Wait)

	const conversation = "live"
	session := journal.ScopedSessionID(db.scope, conversation)
	vm := &fakeVM{}
	c := scopedTestCoordinator(db.scope, db, vm, bus)
	signer, verifier := coordinatorAuth(t)
	c.AccessPolicy = AccessPolicy{Verifier: verifier, AuthorizeAgent: testAllowAgent}
	token := signedCaller(t, signer, taskCaller(db.scope, "run.coordinate"))
	handler := c.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setVerifiedPeer(r, db.scope)
		r.Header.Set("Authorization", "Bearer "+token)
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	path := "/v1/agents/" + db.scope.Deployment + "/sessions/" + conversation
	request := httptest.NewRequest(http.MethodPost, path+":send", strings.NewReader(`{
		"harness_type":"go","format_version":1,"key":"initial","input":{"inline":"e30="}
	}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.Config.Handler.ServeHTTP(response, request.WithContext(ctx))
	if response.Code != http.StatusAccepted {
		t.Fatalf("start VM turn: HTTP %d: %s", response.Code, response.Body)
	}
	owner := journal.DeterministicExecutionID(db.scope, session, 1, 1)
	if vm.boots[owner] != 1 {
		t.Fatalf("dispatcher started %d instances of %q", vm.boots[owner], owner)
	}
	turn, err := db.store.ClaimVMTurn(ctx, db.scope, session, journal.VMTurnClaimRequest{
		TurnSeq: 1, Generation: 1, ExecutionID: owner,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// The SDK still serializes its append over HTTP. This test fixture binds
	// the worker identity to the actual journal store without a PKI fixture.
	stateMux := http.NewServeMux()
	stateMux.HandleFunc("POST /v1/state/sessions/{session}/events:append", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Epoch int64 `json:"lease_epoch"`
			journal.AppendRequest
		}
		if err := transport.ReadJSON(w, r, &req, transport.DefaultMaxBodyBytes); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		receipt, err := db.store.AppendEvents(r.Context(), db.scope, r.PathValue("session"),
			journal.Lease{Worker: owner, Epoch: req.Epoch}, false, req.AppendRequest)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		transport.WriteJSON(w, http.StatusOK, receipt)
	})
	stateServer := httptest.NewTLSServer(stateMux)
	t.Cleanup(stateServer.Close)
	worker, err := stateclient.New(stateclient.Config{
		BaseURL: stateServer.URL, HTTPClient: stateServer.Client(), TokenSource: func(context.Context) (string, error) { return owner, nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	var streams [8]chan liveSSEFrame
	for i := range streams {
		frames := make(chan liveSSEFrame, 140)
		streams[i] = frames
		background.Go(func() {
			defer close(frames)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+path+"/stream?turn_seq=1", nil)
			if err != nil {
				t.Error(err)
				return
			}
			resp, err := server.Client().Do(req)
			if err != nil {
				t.Errorf("reader %d: %v", i, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
				t.Errorf("reader %d: unexpected SSE response %s (%s)", i, resp.Status, resp.Header.Get("Content-Type"))
				return
			}
			var frame liveSSEFrame
			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				line := scanner.Text()
				switch {
				case strings.HasPrefix(line, "id: "):
					frame.id = line[4:]
				case strings.HasPrefix(line, "event: "):
					frame.event = line[7:]
				case strings.HasPrefix(line, "data: "):
					frame.data = line[6:]
				case line == "" && frame.event != "":
					select {
					case frames <- frame:
					case <-ctx.Done():
						return
					}
					frame = liveSSEFrame{}
				}
			}
			if err := scanner.Err(); err != nil {
				t.Errorf("reader %d: %v", i, err)
			}
		})
	}
	nextFrame := func(i int) liveSSEFrame {
		t.Helper()
		select {
		case frame, ok := <-streams[i]:
			if !ok {
				t.Fatalf("reader %d closed before the expected frame", i)
			}
			return frame
		case <-ctx.Done():
			t.Fatalf("reader %d stalled: %v", i, ctx.Err())
			return liveSSEFrame{}
		}
	}
	wantJournal := func(seq int64, kind, role string) {
		t.Helper()
		for i := range streams {
			frame := nextFrame(i)
			var e journal.Event
			if frame.event != "journal" || frame.id != strconv.FormatInt(seq, 10) ||
				json.Unmarshal([]byte(frame.data), &e) != nil || e.Seq != seq ||
				e.TurnSeq != 1 || e.Type != kind || e.Role != role {
				t.Fatalf("reader %d, expected journal event %d %s (%s), got %+v", i, seq, kind, role, frame)
			}
		}
	}

	wantJournal(1, "turn.input", "user")
	signal := SessionSignal{Scope: db.scope, SessionID: session}
	bus.mu.Lock()
	subs := len(bus.subscribers[signal])
	bus.mu.Unlock()
	if subs != len(streams) {
		t.Fatalf("%d live subscriptions for %d readers", subs, len(streams))
	}
	appendBatch := func(turnSeq, head int64, count int, kind string) {
		t.Helper()
		events := make([]journal.EventInput, count)
		for i := range events {
			events[i] = journal.EventInput{ID: fmt.Sprintf("event-%d", head+int64(i)+1),
				Type: kind, Role: "assistant", Codec: "json", Payload: journal.Payload{Inline: []byte(`{}`)}}
		}
		_, err := worker.AppendEvents(ctx, session, turn.Epoch, journal.AppendRequest{
			Key: fmt.Sprintf("batch-%d", head), ExpectedSeq: head, TurnSeq: turnSeq, Events: events,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Cross the coordinator's 128-event page boundary with all readers live.
	appendBatch(1, 1, 130, "delta")
	for seq := int64(2); seq <= 131; seq++ {
		wantJournal(seq, "delta", "assistant")
	}
	followup, err := db.EnqueueTurn(ctx, journal.EnqueueRequest{
		SessionID: session, Harness: "go", FormatVersion: 1, Key: "followup",
		ExecutionMode: journal.ExecutionVMManaged, DeliveryMode: journal.DeliveryFollowup,
		Input: journal.Payload{Inline: []byte(`{"message":"second turn"}`)},
	})
	if err != nil || followup.HeadSeq != 132 || followup.AcceptedMode != journal.DeliveryFollowup {
		t.Fatalf("followup enqueue: %+v %v", followup, err)
	}
	appendBatch(1, 132, 3, "delta")
	for seq := int64(133); seq <= 135; seq++ {
		wantJournal(seq, "delta", "assistant")
	}
	finished, err := worker.FinishVMTurn(ctx, stateclient.ClaimedTurn{TurnLease: turn}, 135,
		journal.Payload{Inline: []byte(`{"message":"first answer"}`)}, nil, false)
	if err != nil || finished.HeadSeq != 137 || finished.Control == nil ||
		finished.Control.NextTurn == nil || finished.Control.NextTurn.InputSeq != 132 {
		t.Fatalf("finish first turn: %+v %v", finished, err)
	}
	appendBatch(132, 137, 1, "second-turn-delta")

	wantJournal(136, "turn.output", "assistant")
	wantJournal(137, "turn.completed", "system")
	for i, frames := range streams {
		frame := nextFrame(i)
		var end journal.TurnCompleted
		if frame.event != "end" || frame.id != "" || json.Unmarshal([]byte(frame.data), &end) != nil ||
			end.TurnSeq != 1 || end.EndSeq != 137 || end.Outcome != "IDLE" || end.NextTurnSeq != 132 {
			t.Fatalf("reader %d got unexpected terminal frame %+v", i, frame)
		}
		select {
		case frame, ok := <-frames:
			if ok {
				t.Fatalf("reader %d received a frame after end: %+v", i, frame)
			}
		case <-ctx.Done():
			t.Fatalf("reader %d did not close after end: %v", i, ctx.Err())
		}
	}

	bus.mu.Lock()
	orphaned := len(bus.subscribers)
	bus.mu.Unlock()
	if orphaned != 0 {
		t.Fatalf("orphaned SSE subscription groups: %d", orphaned)
	}
}
