package space_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// j10 - the cause a preflight refusal knows must reach a cold reader in ANOTHER
// PROCESS over the same durable State.
//
// The measured defect (live journey j10-terminal, turn
// 20261109T195312.510825000Z_mn62ltykjgwi25jdgn7me7vdmm): terminal_status=failed with
// failure_reason "space: durable transcript carries no cause frame for this turn" for a
// turn that failed in PREFLIGHT. The product topology is the reason it is always lost,
// not only after a crash: /v1/space/AcceptTurn|GetTurn is answered by the FRONT, while
// the preflight that refused the run runs in as-runner. The runner knew the cause, kept
// it only in its own memory, and closed the Run row with the dictionary's generic word -
// so the front's cold read, which can only see State, honestly said "nameless".
//
// The shape of the fix these tests accept: the refusing plane writes its sentence onto
// the Run row itself (state.RunPayloadFailureCause) on the same release that ends the
// lease, and the cold reader ranks causes explicitly - a transcript error frame first
// (the run's own emitted verdict), the run-row note second, and the generic
// "carries no cause frame" only for runs that truly recorded neither. The generic
// sentence stays for the legacy row: it is an honest statement about a record, and
// these tests keep it from being retired prematurely.

// j10Release is one Release this double saw: the terminal status plus both optional
// journals - the dictionary word (close_reason) and the refusing plane's sentence
// (failure_cause).
type j10Release struct {
	Status       string
	CloseReason  string
	FailureCause string
}

// j10Durable is the relay the defect needs: the erunState double proves the RUNNER's
// behavior (which token, how many appends), while the relay persists every Release into
// a Run row the way the real State's release does (payload jsonb_set on a terminal
// transition) and serves GetRun/GetThread scoped by token, the way real State isolates
// rows. A cold reader over the relay therefore sees exactly what a cold reader in the
// product sees: the durable row, and nothing of the runner's memory.
type j10Durable struct {
	*erunState
	mu      sync.Mutex
	runs    map[string]map[string]any
	release chan j10Release
	srv     *httptest.Server
}

func newJ10Durable(t *testing.T, runTask string) *j10Durable {
	t.Helper()
	inner := newERunState(t, runTask)
	d := &j10Durable{
		erunState: inner,
		runs: map[string]map[string]any{
			"run-erun-1": {
				"id": "run-erun-1", "thread_id": "thr-erun-1",
				"task_id": runTask, "status": "pending", "sequence": float64(1),
				"payload": map[string]any{"type": "turn", "input": "hi", "max_steps": float64(3)},
			},
		},
		release: make(chan j10Release, 4),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/state/", func(w http.ResponseWriter, r *http.Request) {
		op := r.URL.Path[len("/v1/state/"):]
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		if op == "Release" {
			d.captureRelease(body)
		}
		if d.rowOp(w, r, op, body) {
			return
		}
		rec := &j10Captured{hdr: http.Header{}, status: http.StatusOK}
		inner.srv.Config.Handler.ServeHTTP(rec, r)
		copyHeader(w.Header(), rec.hdr)
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.buf.Bytes())
	})
	d.srv = httptest.NewServer(mux)
	t.Cleanup(d.srv.Close)
	return d
}

// j10Captured records what the relayed double wrote, so the relay can persist a release
// only if the inner State accepted it - exactly like the real release, where the run-row
// write and the lease end are one transaction: a refused release records nothing.
type j10Captured struct {
	hdr    http.Header
	buf    bytes.Buffer
	status int
}

func (c *j10Captured) Header() http.Header { return c.hdr }
func (c *j10Captured) Write(b []byte) (int, error) {
	return c.buf.Write(b)
}
func (c *j10Captured) WriteHeader(code int) { c.status = code }

func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func (d *j10Durable) captureRelease(body []byte) {
	var in struct {
		RunID        string `json:"run_id"`
		Status       string `json:"status"`
		CloseReason  string `json:"close_reason"`
		FailureCause string `json:"failure_cause"`
	}
	rel := j10Release{}
	// A body that does not even decode as a release is not a release; nothing to record.
	if json.Unmarshal(body, &in) != nil || in.RunID == "" {
		return
	}
	rel = j10Release{Status: in.Status, CloseReason: in.CloseReason, FailureCause: in.FailureCause}
	d.mu.Lock()
	_, known := d.runs[in.RunID]
	if known {
		// Persist exactly what real State's release persists: the terminal status and,
		// per key sent, the journal written INTO the existing payload document
		// (jsonb_set semantics). Keys not sent must not appear - an optional key that
		// was never sent is data nobody stated.
		d.runs[in.RunID]["status"] = in.Status
		if in.CloseReason != "" || in.FailureCause != "" {
			payload, _ := d.runs[in.RunID]["payload"].(map[string]any)
			if payload == nil {
				payload = map[string]any{}
			}
			if in.CloseReason != "" {
				payload["close_reason"] = in.CloseReason
			}
			if in.FailureCause != "" {
				payload["failure_cause"] = in.FailureCause
			}
			d.runs[in.RunID]["payload"] = payload
		}
	}
	d.mu.Unlock()
	select {
	case d.release <- rel:
	default:
	}
}

// rowOp serves the scoped row reads a cold reader needs. Owner check is the documented
// token contract ("Bearer TASK:<subject>" as scopedState words it): a foreign caller
// gets NOT_FOUND, not an empty row it could read as "nothing recorded".
func (d *j10Durable) rowOp(w http.ResponseWriter, r *http.Request, op string, body []byte) bool {
	var in map[string]any
	_ = json.Unmarshal(body, &in)
	owner := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	writeErr := func() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": "NOT_FOUND", "message": op + " not found"})
	}
	switch op {
	case "GetRun":
		id, _ := in["run_id"].(string)
		d.mu.Lock()
		row, ok := d.runs[id]
		d.mu.Unlock()
		rowOwner := strings.TrimPrefix(rowStr(row, "task_id"), "rtask-")
		rowOwner = strings.SplitN(rowOwner, ":", 2)[0]
		// Scoped isolation: the row's own owner may read it; the front's test subject
		// (owner-a) is that owner here. Anyone else gets NOT_FOUND, like real State.
		if !ok || (owner != "TASK:owner-a" && rowOwner != "owner-a") {
			writeErr()
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(row)
		return true
	case "GetThread":
		id, _ := in["thread_id"].(string)
		if id != "thr-erun-1" || owner != "TASK:owner-a" {
			writeErr()
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "context_id": "ctx-erun", "epoch": 1})
		return true
	}
	return false
}

func rowStr(row map[string]any, key string) string {
	if row == nil {
		return ""
	}
	s, _ := row[key].(string)
	return s
}

// waitPlaneRelease waits for the Release that ends the preflight-refused run and
// returns what the runner journaled with it.
func (d *j10Durable) waitPlaneRelease(t *testing.T, d0 time.Duration) (j10Release, bool) {
	t.Helper()
	select {
	case rel := <-d.release:
		return rel, true
	case <-time.After(d0):
		return j10Release{}, false
	}
}

func newJ10Coordinator(u string, core space.CoreRunner) *space.Coordinator {
	return space.NewCoordinator(space.CoordinatorConfig{
		StateClient:     space.NewStateClient(u, nil),
		Core:            core,
		WorkerID:        "w1",
		RunGrantIssuer:  &grantRec{childToken: "MINTED-CHILD-TOK"},
		RequireRunGrant: true,
		RunGrantTTL:     time.Minute,
	})
}

// newJ10ColdFront builds the OTHER process: a fresh handler (empty turn table) whose
// reads go to the same durable relay - the front answering GetTurn in the product
// topology, with nothing inherited from the runner that failed the turn.
func newJ10ColdFront(t *testing.T, u string) *httptest.Server {
	t.Helper()
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(u, nil), Core: &stubCore{}, WorkerID: "w-front"})
	h := space.NewHandler(space.HandlerConfig{
		Coordinator:       coord,
		Auth:              space.ExchangeMiddleware(&recExchange{}, authPerSubject),
		RequireScopedTask: true,
	})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return hs
}

// TestPreflightCauseSurvivesToColdReaderInAnotherProcess is the J10 defect as the
// product topologies it: the runner refuses an authority-rooted run (the deterministic
// plane cause - Tier-2 fail-closed), and a FRESH process over the same durable State
// must be able to say WHY. Pre-fix the runner kept the cause in memory only, so the
// cold reader could only answer "durable transcript carries no cause frame" - the
// generic sentence, which is exactly the failure being fixed.
func TestPreflightCauseSurvivesToColdReaderInAnotherProcess(t *testing.T) {
	const wantCause = "space: run is not rooted on its own child grant"

	d := newJ10Durable(t, erunAuthTask) // authority-rooted -> preflight refuses it
	core := &erunCore{}
	edge := newJ10Coordinator(d.srv.URL, core)
	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "owner-a"}
	if err := edge.DispatchTurn(t.Context(), erunIngressTok, owner); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	rel, ok := d.waitPlaneRelease(t, 10*time.Second)
	if !ok {
		t.Fatal("the refused run never released its lease")
	}
	if rel.Status != "failed" {
		t.Fatalf("preflight refusal must close the run failed, got %q", rel.Status)
	}

	// The durable row: the cause must live on the Run row itself. Memory dying with the
	// runner is the defect; this assert is the same fact the cold read will rely on.
	d.mu.Lock()
	payload, _ := d.runs["run-erun-1"]["payload"].(map[string]any)
	d.mu.Unlock()
	if got, _ := payload["failure_cause"].(string); got != wantCause {
		t.Fatalf("the Run row carries failure_cause %q, want the plane's own sentence %q - "+
			"a cause that lives only in the refusing process's memory is the J10 defect", got, wantCause)
	}
	// The dictionary word stays in its own key: the A->B audit comparison must not
	// start quoting free text where the vocabulary belongs.
	if got, _ := payload["close_reason"].(string); got != "error" {
		t.Fatalf("Run row close_reason = %q, want the dictionary word \"error\" kept separate from the cause", got)
	}

	// The other process, over the same durable State.
	front := newJ10ColdFront(t, d.srv.URL)
	code, body := postSpaceAs(t, front.URL, "GetTurn", map[string]any{"turn_id": "run-erun-1"}, "owner-a")
	if code != http.StatusOK {
		t.Fatalf("cold GetTurn = %d (%s)", code, body)
	}
	got := decodeMap(t, body)
	if status, _ := got["status"].(string); status != "failed" {
		t.Fatalf("cold status = %v, want failed (%v)", got["status"], got)
	}
	if reason, _ := got["failure_reason"].(string); reason != wantCause {
		t.Fatalf("cold failure_reason = %q, want the cause the refusing plane recorded (%q) - "+
			"the front owes the user the runner's real reason, not the generic nameless sentence", reason, wantCause)
	}

	// The preflight contract stays untouched by where the cause goes: zero Core calls,
	// zero durable appends, no StepKey consumed.
	if calls, _, _ := core.snapshot(); calls != 0 {
		t.Fatalf("preflight refusal must never start Core, calls=%d", calls)
	}
	if n := d.totalAppends(); n != 0 {
		t.Fatalf("preflight refusal must never append to the durable stream, appends=%d", n)
	}
}

// TestPreflightReleaseWithoutCauseStaysHonestGeneric keeps the generic sentence alive
// for the rows that truly recorded nothing. A release that stated no cause (the
// historical wire form, byte-identical to the old client) must NOT conjure one, and the
// cold reader must keep saying, in words, that the record carries no cause - the
// honest-negative half that a permissive fix would erase.
func TestPreflightReleaseWithoutCauseStaysHonestGeneric(t *testing.T) {
	const want = "space: durable transcript carries no cause frame for this turn"

	d := newJ10Durable(t, erunAuthTask)
	core := &erunCore{}
	edge := newJ10Coordinator(d.srv.URL, core)
	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "owner-a"}
	if err := edge.DispatchTurn(t.Context(), erunIngressTok, owner); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	if _, ok := d.waitPlaneRelease(t, 10*time.Second); !ok {
		t.Fatal("the refused run never released its lease")
	}
	// This is the LEGACY release: the coordinator this test builds journals the plane's
	// sentence, so strip it back out and leave the row exactly as a pre-J10 release
	// left it - terminal, generic dictionary word, no cause anywhere.
	d.mu.Lock()
	if p, ok := d.runs["run-erun-1"]["payload"].(map[string]any); ok {
		delete(p, "failure_cause")
	}
	d.mu.Unlock()

	front := newJ10ColdFront(t, d.srv.URL)
	code, body := postSpaceAs(t, front.URL, "GetTurn", map[string]any{"turn_id": "run-erun-1"}, "owner-a")
	if code != http.StatusOK {
		t.Fatalf("cold GetTurn = %d (%s)", code, body)
	}
	got := decodeMap(t, body)
	if reason, _ := got["failure_reason"].(string); reason != want {
		t.Fatalf("cold failure_reason = %q, want the honest nameless sentence %q - a record that "+
			"carries no cause must say exactly that, and nothing may invent one from a status", reason, want)
	}
	if calls, _, _ := core.snapshot(); calls != 0 {
		t.Fatalf("preflight refusal must never start Core, calls=%d", calls)
	}
}
