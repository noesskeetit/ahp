package space_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// authWithPrincipal injects a verified principal, mimicking the upstream auth
// middleware that real Space deployments mount in front of the Space handler.
func authWithPrincipal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := transport.WithPrincipal(r.Context(), transport.Principal{
			Scope: contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"},
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// newHTTPSpace spins up the real Space HTTP handler (POST /v1/space/{Operation})
// backed by the State API test double. The handler, coordinator, and state
// client are production code; only the State peer is a double (its real server
// does not compile on this baseline — a separate contractor's block).
func newHTTPSpace(t *testing.T, core space.CoreRunner, auth func(http.Handler) http.Handler) *httptest.Server {
	t.Helper()
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        core,
		WorkerID:    "w-http",
	})
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: auth})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return hs
}

func postSpace(t *testing.T, base, op string, body any, bearer string) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal %s: %v", op, err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/v1/space/"+op, rdr)
	if err != nil {
		t.Fatalf("newreq %s: %v", op, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", op, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

func decodeMap(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("decode json %q: %v", string(data), err)
	}
	return m
}

// TestSpaceHTTPRouting proves the Space deployment exposes only POST
// /v1/space/{Operation}, rejects unknown routes/operations, and requires a
// verified principal for AcceptTurn.
func TestSpaceHTTPRouting(t *testing.T) {
	srv := newHTTPSpace(t, &stubCore{}, authWithPrincipal)

	// GET is not a valid Space operation route.
	resp, err := http.Get(srv.URL + "/v1/space/AcceptTurn")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET status = %d, want 404", resp.StatusCode)
	}

	// Unknown operation under the space prefix.
	if code, body := postSpace(t, srv.URL, "BogusOp", map[string]any{}, "tok"); code != http.StatusNotFound {
		t.Fatalf("unknown op status = %d, want 404 (body=%s)", code, body)
	}

	// Unknown route (not under /v1/space/).
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/nope/ping", nil)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unknown route: %v", err)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown route status = %d, want 404", r.StatusCode)
	}
}

// TestSpaceHTTPRequiresPrincipal shows AcceptTurn is forbidden without a
// verified principal (no auth middleware installed).
func TestSpaceHTTPRequiresPrincipal(t *testing.T) {
	srv := newHTTPSpace(t, &stubCore{}, nil) // no auth → no principal in context
	code, body := postSpace(t, srv.URL, "AcceptTurn", map[string]any{
		"key":        "k-noauth",
		"context_id": "ctx-noauth",
		"input":      "hi",
	}, "tok")
	if code != http.StatusForbidden {
		t.Fatalf("AcceptTurn without principal = %d, want 403 (body=%s)", code, body)
	}
}

// TestSpaceHTTPContractJourney exercises the Space API surface over real HTTP:
// accept → checkpoint → HITL submit/respond → outbox → get → list → recover.
func TestSpaceHTTPContractJourney(t *testing.T) {
	srv := newHTTPSpace(t, &stubCore{}, authWithPrincipal)
	ctx := "ctx-" + contracts.NewID()

	// AcceptTurn → pending turn.
	code, body := postSpace(t, srv.URL, "AcceptTurn", map[string]any{
		"key":        "k-" + ctx,
		"context_id": ctx,
		"input":      "hello space",
		"max_steps":  2,
	}, "tok")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)
	if turnID == "" || threadID == "" {
		t.Fatalf("AcceptTurn missing ids: %v", acc)
	}
	if acc["status"] != string(space.TurnPending) {
		t.Fatalf("AcceptTurn status = %v, want pending", acc["status"])
	}

	// SaveCheckpoint from THIS process: it accepted the turn but never claimed the run,
	// so it holds no lease and may not write a snapshot. The honest answer is State's own
	// fence code as a 409 - never a 500, never a snapshot that was never written. (The
	// positive owner leg lives in checkpoint_fence_test.go / checkpoint_lease_realstate_test.go.)
	code, body = postSpace(t, srv.URL, "SaveCheckpoint", map[string]any{
		"turn_id":  turnID,
		"sequence": 1,
		"state":    map[string]any{"step": 1},
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, "tok")
	if code != http.StatusConflict {
		t.Fatalf("SaveCheckpoint without the lease = %d, want 409 (body=%s)", code, body)
	}
	if got, _ := decodeMap(t, body)["code"].(string); got != contracts.ErrStaleEpoch {
		t.Fatalf("SaveCheckpoint body code = %q, want %q (body=%s)", got, contracts.ErrStaleEpoch, body)
	}

	// SubmitHITL → returns a HITL request with an id.
	code, body = postSpace(t, srv.URL, "SubmitHITL", map[string]any{
		"turn_id": turnID,
		"kind":    "approval",
		"prompt":  "approve?",
	}, "tok")
	if code != http.StatusOK {
		t.Fatalf("SubmitHITL = %d, want 200 (body=%s)", code, body)
	}
	hitlID, _ := decodeMap(t, body)["id"].(string)
	if hitlID == "" {
		t.Fatalf("SubmitHITL missing id: %s", body)
	}

	// RespondHITL → ok.
	code, body = postSpace(t, srv.URL, "RespondHITL", map[string]any{
		"hitl_id":   hitlID,
		"approved":  true,
		"thread_id": threadID,
	}, "tok")
	if code != http.StatusOK {
		t.Fatalf("RespondHITL = %d, want 200 (body=%s)", code, body)
	}

	// EmitOutbox → delivery entry with an id.
	code, body = postSpace(t, srv.URL, "EmitOutbox", map[string]any{
		"turn_id":   turnID,
		"key":       "note-1",
		"kind":      "message",
		"payload":   "outboxed",
		"thread_id": threadID,
	}, "tok")
	if code != http.StatusOK {
		t.Fatalf("EmitOutbox = %d, want 200 (body=%s)", code, body)
	}
	if id, _ := decodeMap(t, body)["id"].(string); id == "" {
		t.Fatalf("EmitOutbox missing id: %s", body)
	}

	// GetTurn → the turn we created.
	code, body = postSpace(t, srv.URL, "GetTurn", map[string]any{"turn_id": turnID}, "tok")
	if code != http.StatusOK {
		t.Fatalf("GetTurn = %d, want 200 (body=%s)", code, body)
	}
	if got, _ := decodeMap(t, body)["id"].(string); got != turnID {
		t.Fatalf("GetTurn id = %v, want %s", got, turnID)
	}

	// GetTurn for unknown id → 404.
	if code, _ = postSpace(t, srv.URL, "GetTurn", map[string]any{"turn_id": "missing"}, "tok"); code != http.StatusNotFound {
		t.Fatalf("GetTurn(unknown) = %d, want 404", code)
	}

	// ListTurns → contains our turn.
	code, body = postSpace(t, srv.URL, "ListTurns", map[string]any{}, "tok")
	if code != http.StatusOK {
		t.Fatalf("ListTurns = %d, want 200 (body=%s)", code, body)
	}
	if !listContainsTurn(decodeMap(t, body), turnID) {
		t.Fatalf("ListTurns missing turn %s: %s", turnID, body)
	}

	// Recover → 200 (no Running turns here, so nothing mutated to Unknown).
	code, body = postSpace(t, srv.URL, "Recover", nil, "tok")
	if code != http.StatusOK {
		t.Fatalf("Recover = %d, want 200 (body=%s)", code, body)
	}
	rec := decodeMap(t, body)
	if _, ok := rec["recovered"]; !ok {
		t.Fatalf("Recover response missing 'recovered': %s", body)
	}
	if _, ok := rec["unknown"]; !ok {
		t.Fatalf("Recover response missing 'unknown': %s", body)
	}
}

// TestSpaceHTTPDispatchAndStream proves DispatchTurn runs the Core loop and
// ReadStream returns the emitted events over HTTP.
func TestSpaceHTTPDispatchAndStream(t *testing.T) {
	coreDone := make(chan struct{})
	srv := newHTTPSpace(t, &stubCore{done: coreDone}, authWithPrincipal)
	ctx := "ctx-stream-" + contracts.NewID()

	code, body := postSpace(t, srv.URL, "AcceptTurn", map[string]any{
		"key":        "k-" + ctx,
		"context_id": ctx,
		"input":      "go",
		"max_steps":  2,
	}, "tok")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)

	if code, body = postSpace(t, srv.URL, "DispatchTurn", nil, "tok"); code != http.StatusOK {
		t.Fatalf("DispatchTurn = %d, want 200 (body=%s)", code, body)
	}

	// Wait for the async run to reach a terminal status.
	deadline := time.Now().Add(3 * time.Second)
	var status string
	for time.Now().Before(deadline) {
		c, b := postSpace(t, srv.URL, "GetTurn", map[string]any{"turn_id": turnID}, "tok")
		if c != http.StatusOK {
			t.Fatalf("GetTurn during poll = %d (body=%s)", c, b)
		}
		status, _ = decodeMap(t, b)["status"].(string)
		if status == string(space.TurnDone) || status == string(space.TurnFailed) || status == string(space.TurnCanceled) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status != string(space.TurnDone) {
		t.Fatalf("turn did not complete via HTTP dispatch, status=%s", status)
	}

	// ReadStream → at least one event was recorded for the thread.
	code, body = postSpace(t, srv.URL, "ReadStream", map[string]any{
		"thread_id": threadID,
		"after":     0,
		"limit":     100,
	}, "tok")
	if code != http.StatusOK {
		t.Fatalf("ReadStream = %d, want 200 (body=%s)", code, body)
	}
	events, _ := decodeMap(t, body)["events"].([]any)
	if len(events) == 0 {
		t.Fatalf("ReadStream returned no events after dispatch: %s", body)
	}
	// The browser UI (web/static/app.js pollStream) relies on the OBJECT
	// envelope {"events":[...]} and per-event fields {kind, sequence, payload}
	// (NOT a bare array, NOT "seq"). Assert that wire shape so the UI contract
	// is regression-guarded against the real Space HTTP API.
	var maxSeq float64 = -1
	for _, raw := range events {
		ev, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("event is not an object: %#v", raw)
		}
		if k, _ := ev["kind"].(string); k == "" {
			t.Fatalf("event missing non-empty string kind: %#v", ev)
		}
		seq, ok := ev["sequence"].(float64)
		if !ok {
			t.Fatalf("event missing numeric sequence (UI uses ev.sequence): %#v", ev)
		}
		if seq < maxSeq {
			t.Fatalf("events not ordered by sequence: %v after %v", seq, maxSeq)
		}
		maxSeq = seq
		if _, ok := ev["payload"]; !ok {
			t.Fatalf("event missing payload: %#v", ev)
		}
	}
}

func listContainsTurn(m map[string]any, turnID string) bool {
	turns, _ := m["turns"].([]any)
	for _, it := range turns {
		if t, ok := it.(map[string]any); ok {
			if id, _ := t["id"].(string); id == turnID {
				return true
			}
		}
	}
	return false
}

// artifactCore is a deterministic CoreRunner that completes a turn with a single
// tool step carrying artifact metadata + provenance (display-only payload the
// browser UI consumes). It proves the Space edge surfaces artifacts/provenance on
// the wire; byte-level download through the edge is out of Space scope (Execution/
// Data plane owns artifact bytes) and is deliberately NOT asserted here.
type artifactCore struct{}

func (artifactCore) Run(_ context.Context, _ *space.CoreRequest, results chan<- space.StepResult) error {
	content := json.RawMessage(`{"artifacts":[{"artifact_id":"art-1","name":"revenue.txt","mime_type":"text/plain","sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}],"provenance":{"kind":"tool","tool":"emil.delegate","subject":"s1"}}`)
	results <- space.StepResult{StepType: space.StepTool, Content: content}
	results <- space.StepResult{StepType: space.StepDone, Done: true}
	return nil
}

// TestEdgeStreamSurfacesArtifactProvenance drives a turn through the real Space
// HTTP edge (POST /v1/space/{Operation}) with the auth seam and asserts the
// emitted artifact metadata + provenance are observable to the owning subject via
// ReadStream. This closes the display-only "surface artifacts/provenance" claim at
// the edge, not only inside the Core loop.
func TestEdgeStreamSurfacesArtifactProvenance(t *testing.T) {
	srv := newHTTPSpace(t, artifactCore{}, authWithPrincipal)
	ctx := "ctx-artifact-" + contracts.NewID()

	code, body := postSpace(t, srv.URL, "AcceptTurn", map[string]any{
		"key": "k-" + ctx, "context_id": ctx, "input": "go", "max_steps": 2,
	}, "tok")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)

	if code, body = postSpace(t, srv.URL, "DispatchTurn", nil, "tok"); code != http.StatusOK {
		t.Fatalf("DispatchTurn = %d (body=%s)", code, body)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, b := postSpace(t, srv.URL, "GetTurn", map[string]any{"turn_id": turnID}, "tok")
		if c != http.StatusOK {
			t.Fatalf("GetTurn = %d (body=%s)", c, b)
		}
		if s, _ := decodeMap(t, b)["status"].(string); s == string(space.TurnDone) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	code, body = postSpace(t, srv.URL, "ReadStream", map[string]any{"thread_id": threadID, "after": 0, "limit": 100}, "tok")
	if code != http.StatusOK {
		t.Fatalf("ReadStream = %d (body=%s)", code, body)
	}
	events, _ := decodeMap(t, body)["events"].([]any)
	if len(events) == 0 {
		t.Fatalf("ReadStream returned no events: %s", body)
	}

	var sawArtifact, sawProvenance bool
	for _, ev := range events {
		e, _ := ev.(map[string]any)
		if kind, _ := e["kind"].(string); kind != "tool" {
			continue
		}
		raw, _ := json.Marshal(e["payload"])
		var step struct {
			Content struct {
				Artifacts  []map[string]any `json:"artifacts"`
				Provenance map[string]any   `json:"provenance"`
			} `json:"content"`
		}
		if err := json.Unmarshal(raw, &step); err != nil {
			continue
		}
		for _, a := range step.Content.Artifacts {
			if a["artifact_id"] == "art-1" && a["sha256"] != "" && a["mime_type"] == "text/plain" {
				sawArtifact = true
			}
		}
		if p := step.Content.Provenance; p != nil && p["kind"] == "tool" && p["tool"] == "emil.delegate" {
			sawProvenance = true
		}
	}
	if !sawArtifact {
		t.Fatalf("edge ReadStream did not surface artifact metadata (art-1 + sha256 + mime): %s", body)
	}
	if !sawProvenance {
		t.Fatalf("edge ReadStream did not surface provenance (tool=emil.delegate): %s", body)
	}
}
