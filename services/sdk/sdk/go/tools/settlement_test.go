package tools_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	state "github.com/ahp-platform/ahp/sdk/go/state/journal"
	tools "github.com/ahp-platform/ahp/sdk/go/tools"
)

func TestSettleLeasedCallWaitsForInFlight(t *testing.T) {
	for _, mode := range []string{"completes", "after_call", "unknown", "caller_cancelled", "lease_expired", "lease_lost", "lease_renewed", "inconsistent_lookup"} {
		t.Run(mode, func(t *testing.T) { testInFlightSettlement(t, mode) })
	}
}

func testInFlightSettlement(t *testing.T, mode string) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	scope := contracts.Scope{Tenant: "tenant", Deployment: "deployment", Subject: "subject"}
	const session, key, callID, tool = "session-1", "operation-1", "call-1", "write"
	args := json.RawMessage(`{"target":"one"}`)
	expires := time.Now().Add(time.Minute)
	if mode == "lease_expired" {
		expires = time.Now().Add(400 * time.Millisecond)
	}
	lease := state.NewVMLease(state.ClaimedTurn{
		TurnLease: journal.TurnLease{
			Lease:     journal.Lease{Worker: "worker", Epoch: 1, ExpiresAt: expires},
			SessionID: session, InputSeq: 1,
		},
		ToolLeaseProof: "first-proof",
	})
	intent := journal.ToolIntent{
		ToolName: tool, IdempotencyKey: key,
		IntentTransition: journal.IntentTransition{CallID: callID, TurnSeq: 1, Status: journal.IntentDispatched, GatewayOperationID: key},
	}
	argsDigest := sha256.Sum256(args)
	intent.ArgsDigest = argsDigest[:]
	payload := json.RawMessage(`{"content":[{"type":"text","text":"written"}]}`)
	digest, err := contracts.Digest(struct {
		Kind      string          `json:"kind"`
		Target    string          `json:"target"`
		Guard     string          `json:"guard"`
		Arguments json.RawMessage `json:"arguments"`
	}{"tool.call", tool, "", args})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := json.Marshal(tools.FencedLookup{
		Status: "COMPLETED", Receipt: &tools.ToolResult{
			Key: key, Target: tool, Status: "SUCCEEDED", Storage: "inline", Payload: payload, Digest: digest,
			Provenance: tools.ResultProvenance{Schema: "tools-result-v1", GrantTask: "worker", GrantContext: journal.VMSessionContextID(scope, session), Source: "connection"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var lookups, calls, writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/tools/FenceAndLookup":
			var req struct {
				Key string `json:"key"`
				tools.LeaseProof
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key != key || req.SessionID != session || req.Epoch != 1 {
				t.Errorf("lookup changed operation or turn: %+v (%v)", req, err)
			}
			n := lookups.Add(1)
			if n == 1 {
				switch mode {
				case "lease_lost":
					lease.ApplyFinish(1, journal.AppendReceipt{})
				case "lease_renewed":
					lease.ApplyHeartbeat(state.SessionLease{
						Lease:          journal.Lease{Worker: "worker", Epoch: 1, ExpiresAt: expires.Add(time.Minute)},
						ToolLeaseProof: "new-proof",
					})
				}
			}
			if mode == "lease_renewed" && n > 1 && req.Proof != "new-proof" {
				t.Errorf("lookup used old signed proof after renewal: %s", req.Proof)
			}
			switch {
			case mode == "after_call" && n == 1:
				writeJSON(w, 404, `{"code":"SAFE_TO_RETRY","message":"no outcome"}`)
			case mode == "caller_cancelled" && n == 2:
				cancel()
				<-r.Context().Done()
			case mode == "lease_expired" && n == 2:
				<-r.Context().Done()
			case (mode == "completes" || mode == "after_call" || mode == "lease_renewed") && n >= 3:
				writeJSON(w, 200, string(completed))
			case mode == "unknown" && n >= 2:
				writeJSON(w, 409, `{"code":"CONFLICT","reason":"OUTCOME_UNKNOWN","message":"outcome unknown"}`)
			case mode == "inconsistent_lookup" && n >= 2:
				writeJSON(w, 404, `{"code":"SAFE_TO_RETRY","message":"no outcome"}`)
			default:
				writeJSON(w, 202, `{"status":"IN_FLIGHT"}`)
			}
		case "/v1/tools/Call":
			n := calls.Add(1)
			if mode != "after_call" || n != 1 {
				t.Error("SDK invoked the already in-flight effect")
			}
			writeJSON(w, 502, `{"code":"UNAVAILABLE","message":"upstream response interrupted"}`)
		case "/v1/state/aux:mutate":
			if r.Method != http.MethodGet {
				t.Error("SDK attempted to redispatch the durable intent")
			}
			raw, err := json.Marshal(intent)
			if err != nil {
				t.Error(err)
			}
			writeJSON(w, 200, string(raw))
		case "/v1/state/sessions/" + session:
			writeJSON(w, 200, `{"head_seq":1}`)
		case "/v1/state/sessions/" + session + "/events:append":
			var req journal.AppendRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.ToolOutcomes) != 1 ||
				req.ToolOutcomes[0].GatewayOperationID != key || req.ToolOutcomes[0].CallID != callID {
				t.Errorf("settlement lost its durable key: %+v (%v)", req, err)
			}
			writes.Add(1)
			writeJSON(w, 200, `{"head_seq":2}`)
		default:
			t.Errorf("unexpected route: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	srv := httptest.NewTLSServer(handler)
	defer srv.Close()
	token := func(context.Context) (string, error) { return "worker-token", nil }
	gw, err := tools.New(tools.Config{BaseURL: srv.URL, HTTPClient: srv.Client(), TokenSource: token})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := state.New(state.Config{BaseURL: srv.URL, HTTPClient: srv.Client(), TokenSource: token})
	if err != nil {
		t.Fatal(err)
	}
	settled, err := gw.SettleLeasedCall(ctx, owner, lease, tools.LeasedCall{Scope: scope, CallID: callID, Key: key, Tool: tool, Arguments: args})
	switch mode {
	case "completes", "after_call", "lease_renewed":
		if err != nil || settled.Status != journal.IntentCompleted || settled.Result == nil || string(settled.Result.Inline) != string(payload) || writes.Load() != 1 {
			t.Fatalf("waiting lost the completed receipt: status=%s appends=%d (%v)", settled.Status, writes.Load(), err)
		}
	case "unknown":
		if err != nil || settled.Status != journal.IntentUnknown || writes.Load() != 1 {
			t.Fatalf("waiting lost the unknown outcome: status=%s appends=%d (%v)", settled.Status, writes.Load(), err)
		}
	case "caller_cancelled":
		if !errors.Is(err, context.Canceled) || lookups.Load() != 2 || writes.Load() != 0 {
			t.Fatalf("cancel did not stop in-flight wait: lookups=%d appends=%d (%v)", lookups.Load(), writes.Load(), err)
		}
	case "lease_expired", "lease_lost":
		if contracts.Code(err) != contracts.ErrStaleEpoch || writes.Load() != 0 {
			t.Fatalf("stale lease did not stop in-flight wait: appends=%d (%v)", writes.Load(), err)
		}
	case "inconsistent_lookup":
		if lookups.Load() != 2 || writes.Load() != 0 || settled.Status != journal.IntentDispatched {
			t.Fatalf("inconsistent lookup changed the dispatch: status=%s lookups=%d appends=%d (%v)", settled.Status, lookups.Load(), writes.Load(), err)
		}
	}
	var expectedCalls int32
	if mode == "after_call" {
		expectedCalls = 1
	}
	if calls.Load() != expectedCalls {
		t.Fatalf("Gateway received %d effect attempts, want %d", calls.Load(), expectedCalls)
	}
}
