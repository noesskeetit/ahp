package toolsapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Both production HTTP handlers run with their real PostgreSQL stores.
// The MCP peer and the CA are local test fixtures.
func TestCompactJournalToolGatewayRecovery(t *testing.T) {
	dsn := os.Getenv("AHP_JOURNAL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires an isolated compact-journal PostgreSQL")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil || !strings.HasPrefix(config.ConnConfig.Database, "ahp_carbon_compact") {
		t.Fatal("use a dedicated ahp_carbon_compact PostgreSQL database")
	}
	t.Setenv("AHP_TEST_DATABASE_URL", dsn)
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	journal := statejournal.New(e.pool)
	if err := journal.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	stateHandler, err := statejournal.NewHTTP(journal, e.verifier, func(context.Context, auth.Claims, string) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	routes := http.NewServeMux()
	routes.Handle("/v1/state/", stateHandler)
	routes.Handle("/v1/tools/", e.srv.Config.Handler)
	var lostGatewayReply atomic.Bool
	var dropNextGatewayReply atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tools/Call" ||
			(r.Header.Get("X-Test-Lose-Reply") != "yes" && !dropNextGatewayReply.Swap(false)) {
			routes.ServeHTTP(w, r)
			return
		}
		reply := httptest.NewRecorder()
		routes.ServeHTTP(reply, r)
		if reply.Code != http.StatusOK {
			t.Errorf("Gateway did not complete before client response loss: %d", reply.Code)
		}
		socket, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("drop Gateway reply: %v", err)
			return
		}
		lostGatewayReply.Store(true)
		_ = socket.Close()
	}))
	server.TLS = e.srv.TLS.Clone()
	server.StartTLS()
	t.Cleanup(server.Close)
	clientTransport := server.Client().Transport.(*http.Transport).Clone()
	clientTransport.TLSClientConfig.Certificates = append(
		clientTransport.TLSClientConfig.Certificates, e.currentClient(),
	)
	client := &http.Client{Transport: clientTransport, Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)

	sign := func(task, contextID string) string {
		t.Helper()
		token, err := e.signer.Sign(auth.Claims{
			Audience: tAudience, Kind: auth.KindTask,
			Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject,
			Task: task, Root: "journal-run", Context: contextID, Grant: "journal-grant",
		}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	request := func(t *testing.T, method, route, token string, input any, drop bool) (int, []byte, error) {
		t.Helper()
		data, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(ctx, method, server.URL+route, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		if drop {
			req.Header.Set("X-Test-Lose-Reply", "yes")
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		payload, err := io.ReadAll(resp.Body)
		return resp.StatusCode, payload, err
	}

	for _, scenario := range []struct {
		name           string
		loseMCP        bool
		gatewayMissing bool
		beforeExpiry   string
		wantState      string
	}{
		{"gateway_reply_lost", false, false, statejournal.IntentDispatched, statejournal.IntentCompleted},
		{"mcp_reply_lost", true, false, statejournal.IntentDispatched, statejournal.IntentUnknown},
		{"prepared_gateway_missing", false, true, statejournal.IntentPrepared, statejournal.IntentCompleted},
		{"dispatched_gateway_missing", false, true, statejournal.IntentDispatched, statejournal.IntentCompleted},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			worker := sign("original", "journal-context")
			replacement := sign("replacement", "journal-context")
			sibling := sign("replacement", "foreign-context")
			call := func(method, path, token string, input any, want int, target any) []byte {
				t.Helper()
				code, payload, err := request(t, method, path, token, input, false)
				if err != nil || code != want {
					t.Fatalf("%s %s: got %d %s (%v), want %d", method, path, code, payload, err, want)
				}
				if target != nil {
					if err := json.Unmarshal(payload, target); err != nil {
						t.Fatalf("decode %s: %v", path, err)
					}
				}
				return payload
			}
			var lease statejournal.Lease
			const claim = "/v1/state/sessions:acquire"
			const intentAPI = "/v1/state/aux:mutate"
			acquire := map[string]any{"session_id": scenario.name, "harness_type": "langgraph", "format_version": 1, "ttl_ms": 1250}
			call(http.MethodPost, claim, worker, acquire, 200, &lease)
			if lease.Epoch != 1 || lease.Worker != "original" {
				t.Fatalf("unexpected original lease %+v", lease)
			}
			key := "journal-" + scenario.name
			// An unresolved effect keeps its guard across scenarios. Use an
			// independent business object for each recovery case.
			args := json.RawMessage(`{"fixture_object":"` + scenario.name + `"}`)
			e.allowDigest(t, "write", args)
			base := "/v1/state/sessions/" + scenario.name
			prepared := statejournal.AppendRequest{
				Key: "before-tool",
				Events: []statejournal.EventInput{{
					ID: "call", Type: "model.tool_call", Role: "assistant", Codec: "json",
					Payload: statejournal.Payload{Inline: args},
				}},
				Intents: []statejournal.PreparedIntent{{
					CallID: "call-1", ToolName: "write", ArgsDigest: sha256.Sum256(args), IdempotencyKey: key,
				}},
			}
			call(http.MethodPost, base+"/events:append", worker, struct {
				Epoch int64 `json:"lease_epoch"`
				statejournal.AppendRequest
			}{lease.Epoch, prepared}, 200, nil)
			transition := statejournal.IntentTransition{
				CallID: "call-1", GatewayOperationID: key, Status: statejournal.IntentDispatched,
			}
			transitionInput := func(epoch int64, change statejournal.IntentTransition) map[string]any {
				return map[string]any{"session_id": scenario.name, "lease_epoch": epoch, "transition": change}
			}
			if scenario.beforeExpiry == statejournal.IntentDispatched {
				call(http.MethodPost, intentAPI, worker, transitionInput(lease.Epoch, transition), 200, nil)
			}

			toolInput := map[string]any{"key": key, "tool": "write", "arguments": args}
			initialEffects := atomic.LoadInt32(&e.fixture.effCount)
			switch {
			case scenario.gatewayMissing:
				call(http.MethodPost, "/v1/tools/GetResult", worker, map[string]string{"key": key}, 404, nil)
			case scenario.loseMCP:
				e.fixture.dropNextCallReply.Store(true)
				var delivery operations.Operation
				call(http.MethodPost, "/v1/tools/Call", worker, toolInput, 200, &delivery)
				if !e.fixture.droppedCallReply.Load() || delivery.Status != operations.StatusSideEffectUnknown {
					t.Fatalf("lost MCP reply is not a durable uncertain outcome: %s", delivery.Status)
				}
			default:
				if _, _, err := request(t, http.MethodPost, "/v1/tools/Call", worker, toolInput, true); err == nil || !lostGatewayReply.Load() {
					t.Fatalf("Gateway reply was not lost after processing: %v", err)
				}
			}
			var wantOriginalEffects int32 = 1
			if scenario.gatewayMissing {
				wantOriginalEffects = 0
			}
			if got := atomic.LoadInt32(&e.fixture.effCount) - initialEffects; got != wantOriginalEffects {
				t.Fatalf("original invocation ran %d effects, want %d", got, wantOriginalEffects)
			}

			if delay := time.Until(lease.ExpiresAt) + 20*time.Millisecond; delay > 0 {
				time.Sleep(delay)
			}
			acquire["ttl_ms"] = 5000
			call(http.MethodPost, claim, replacement, acquire, 200, &lease)
			if lease.Epoch != 2 || lease.Worker != "replacement" {
				t.Fatalf("unexpected replacement lease %+v", lease)
			}
			stale := call(http.MethodPost, intentAPI, worker, transitionInput(1, transition), 409, nil)
			if !bytes.Contains(stale, []byte("STALE_EPOCH")) {
				t.Fatalf("previous worker was refused for an unexpected reason: %s", stale)
			}
			var stored statejournal.ToolIntent
			intentRead := intentAPI + "?session_id=" + scenario.name + "&call_id=call-1"
			call(http.MethodGet, intentRead, replacement, nil, 200, &stored)
			wantGatewayID := key
			if scenario.beforeExpiry == statejournal.IntentPrepared {
				wantGatewayID = ""
			}
			if stored.Status != scenario.beforeExpiry || stored.GatewayOperationID != wantGatewayID {
				t.Fatalf("restored call did not retain the expected state: %+v", stored)
			}
			resultInput := map[string]string{"key": key}
			if scenario.gatewayMissing {
				call(http.MethodPost, "/v1/tools/GetResult", replacement, resultInput, 404, nil)
				call(http.MethodPost, intentAPI, replacement, transitionInput(lease.Epoch, transition), 200, nil)
				for range 2 {
					var delivery operations.Operation
					call(http.MethodPost, "/v1/tools/Call", replacement, toolInput, 200, &delivery)
					if delivery.Status != operations.StatusSucceeded {
						t.Fatalf("Gateway did not complete the recovered operation: %+v", delivery)
					}
				}
				t.Log("authoritative_gateway_missing=404 resumed_with_same_key=true")
			}
			transition.Status = scenario.wantState

			if scenario.loseMCP {
				call(http.MethodPost, "/v1/tools/GetResult", replacement, resultInput, 409, nil)
				call(http.MethodPost, intentAPI, replacement, transitionInput(lease.Epoch, transition), 200, nil)
				var replay operations.Operation
				call(http.MethodPost, "/v1/tools/Call", replacement, toolInput, 200, &replay)
				if replay.Status != operations.StatusSideEffectUnknown {
					t.Fatalf("unknown operation replay changed status to %s", replay.Status)
				}
				forged := transition
				forged.Status = statejournal.IntentCompleted
				forged.Result = &statejournal.Payload{Inline: []byte(`{"invented":true}`)}
				call(http.MethodPost, intentAPI, replacement, transitionInput(lease.Epoch, forged), 409, nil)
			} else {
				call(http.MethodPost, "/v1/tools/GetResult", sibling, resultInput, 403, nil)
				var receipt tools.ResultView
				call(http.MethodPost, "/v1/tools/GetResult", replacement, resultInput, 200, &receipt)
				if receipt.Key != key || receipt.Status != operations.StatusSucceeded || receipt.Storage != "inline" ||
					!bytes.Contains(receipt.Payload, []byte("written")) {
					t.Fatalf("unexpected authoritative receipt: %+v", receipt)
				}
				transition.Result = &statejournal.Payload{Inline: receipt.Payload}
				appendReceipt := struct {
					Epoch int64 `json:"lease_epoch"`
					statejournal.AppendRequest
				}{lease.Epoch, statejournal.AppendRequest{
					Key: "after-tool", ExpectedSeq: 1,
					Events: []statejournal.EventInput{{
						ID: "result", ParentID: "call", Type: "tool.receipt", Role: "tool", Codec: "json",
						Payload: *transition.Result,
					}},
					ToolOutcomes: []statejournal.IntentTransition{transition},
				}}
				call(http.MethodPost, base+"/events:append", replacement, appendReceipt, 200, nil)
				call(http.MethodPost, base+"/events:append", replacement, appendReceipt, 200, nil)
				var events statejournal.EventPage
				call(http.MethodGet, base+"/events", replacement, nil, 200, &events)
				if events.HeadSeq != 2 || len(events.Events) != 2 ||
					!bytes.Equal(events.Events[1].Payload.Inline, receipt.Payload) {
					t.Fatalf("receipt replay did not retain exactly one result event: %+v", events)
				}
			}
			call(http.MethodGet, intentRead, replacement, nil, 200, &stored)
			if stored.Status != scenario.wantState || atomic.LoadInt32(&e.fixture.effCount)-initialEffects != 1 {
				t.Fatalf("recovery changed effect count or status: status=%s effects=%d",
					stored.Status, atomic.LoadInt32(&e.fixture.effCount)-initialEffects)
			}
			if scenario.loseMCP && stored.Result != nil {
				t.Fatal("an unknown effect unexpectedly has a result")
			}
			if !scenario.loseMCP && (stored.Result == nil || !bytes.Equal(stored.Result.Inline, transition.Result.Inline)) {
				t.Fatal("the confirmed intent lost the authoritative Gateway receipt")
			}
			t.Logf("replacement_epoch=%d compact_intent=%s mcp_effects=1 stale_epoch_rejected=true",
				lease.Epoch, stored.Status)
		})
	}
	if python := os.Getenv("AHP_JOURNAL_GATEWAY_NATIVE_PYTHON"); python != "" {
		allowNativeArgs := json.RawMessage(`{"value":1}`)
		e.allowDigest(t, "write", allowNativeArgs)
		for _, scenario := range []string{"known", "unknown"} {
			t.Run("native_langgraph_"+scenario, func(t *testing.T) {
				lostGatewayReply.Store(false)
				e.fixture.droppedCallReply.Store(false)
				e.fixture.dropNextCallReply.Store(scenario == "unknown")
				effectsBefore := atomic.LoadInt32(&e.fixture.effCount)
				runNativeGatewayTrial(t, python, server, e,
					sign("native-original", "native-journal"), sign("native-replacement", "native-journal"),
					scenario, "compact_gateway_faults.py", "")
				if atomic.LoadInt32(&e.fixture.effCount)-effectsBefore != 1 {
					t.Fatal("native graph caused a duplicate or missing upstream effect")
				}
				if lostGatewayReply.Load() != (scenario == "known") ||
					e.fixture.droppedCallReply.Load() != (scenario == "unknown") {
					t.Fatal("native graph did not hit the requested TCP failpoint")
				}
				t.Log("mcp_effects=1")
			})
		}
	}
	if python := os.Getenv("AHP_JOURNAL_GATEWAY_HERMES_PYTHON"); python != "" {
		source := os.Getenv("AHP_JOURNAL_GATEWAY_HERMES_SOURCE")
		if source == "" {
			t.Fatal("set AHP_JOURNAL_GATEWAY_HERMES_SOURCE to the pinned Hermes checkout")
		}
		e.allowDigest(t, "write", json.RawMessage(`{"item":17}`))
		for _, scenario := range []string{"prepared", "unsent", "known", "large-inline", "unknown"} {
			t.Run("native_hermes_"+scenario, func(t *testing.T) {
				lostGatewayReply.Store(false)
				e.fixture.droppedCallReply.Store(false)
				e.fixture.dropNextCallReply.Store(scenario == "unknown")
				if scenario == "large-inline" {
					e.fixture.writePadding.Store(300 << 10)
					defer e.fixture.writePadding.Store(0)
				}
				effectsBefore := atomic.LoadInt32(&e.fixture.effCount)
				runNativeGatewayTrial(t, python, server, e,
					sign("hermes-original", "hermes-journal"), sign("hermes-replacement", "hermes-journal"),
					scenario, "hermes_gateway_probe.py", source)
				if atomic.LoadInt32(&e.fixture.effCount)-effectsBefore != 1 {
					t.Fatal("native Hermes caused a duplicate or missing upstream effect")
				}
				if lostGatewayReply.Load() != (scenario == "known" || scenario == "large-inline") ||
					e.fixture.droppedCallReply.Load() != (scenario == "unknown") {
					t.Fatal("native Hermes did not hit the requested TCP failpoint")
				}
				t.Log("mcp_effects=1")
			})
		}
	}
	if node := os.Getenv("AHP_JOURNAL_GATEWAY_DEEPSEEK_NODE"); node != "" {
		e.allowDigest(t, "write", json.RawMessage(`{}`))
		for _, scenario := range []string{"prepared", "known", "unknown"} {
			t.Run("native_deepseek_"+scenario, func(t *testing.T) {
				lostGatewayReply.Store(false)
				dropNextGatewayReply.Store(scenario == "known")
				e.fixture.droppedCallReply.Store(false)
				e.fixture.dropNextCallReply.Store(scenario == "unknown")
				before := atomic.LoadInt32(&e.fixture.effCount)
				runNativeGatewayTrial(t, node, server, e,
					sign("deepseek-original", "deepseek-journal"), sign("deepseek-replacement", "deepseek-journal"),
					"gateway-"+scenario, "deepseek_compact_probe.mjs", "")
				if atomic.LoadInt32(&e.fixture.effCount)-before != 1 ||
					e.fixture.droppedCallReply.Load() != (scenario == "unknown") ||
					lostGatewayReply.Load() != (scenario == "known") {
					t.Fatal("DeepSeek did not hit the requested effect boundary")
				}
				t.Log("mcp_effects=1")
			})
		}
	}
	if node := os.Getenv("AHP_JOURNAL_GATEWAY_OPENCLAW_NODE"); node != "" {
		e.allowDigest(t, "write", json.RawMessage(`{"amount":7}`))
		for _, scenario := range []string{"prepared", "dispatched", "known", "unknown", "detached"} {
			t.Run("native_openclaw_"+scenario, func(t *testing.T) {
				lostGatewayReply.Store(false)
				dropNextGatewayReply.Store(scenario == "known")
				e.fixture.droppedCallReply.Store(false)
				e.fixture.dropNextCallReply.Store(scenario == "unknown")
				before := atomic.LoadInt32(&e.fixture.effCount)
				runNativeGatewayTrial(t, node, server, e,
					sign("openclaw-original", "openclaw-journal"), sign("openclaw-replacement", "openclaw-journal"),
					"gateway-"+scenario, "openclaw_compact_probe.mjs", "")
				wantEffects := int32(1)
				if scenario == "detached" {
					wantEffects = 0
				}
				if atomic.LoadInt32(&e.fixture.effCount)-before != wantEffects ||
					e.fixture.droppedCallReply.Load() != (scenario == "unknown") ||
					lostGatewayReply.Load() != (scenario == "known") {
					t.Fatal("OpenClaw did not hit the requested effect boundary")
				}
				t.Logf("mcp_effects=%d", wantEffects)
			})
		}
	}
	if python := os.Getenv("AHP_JOURNAL_GATEWAY_ADK_PYTHON"); python != "" {
		// An uncertain operation retains the guard for its argument digest.
		// Give each independent scenario its own resource and exercise the real guard.
		for index, scenario := range []string{"live", "prepared", "unsent", "known", "unknown", "live-unknown"} {
			t.Run("native_adk_"+scenario, func(t *testing.T) {
				item := strconv.Itoa(200 + index)
				t.Setenv("AHP_JOURNAL_ADK_FIXTURE_ITEM", item)
				e.allowDigest(t, "write", json.RawMessage(`{"item":`+item+`}`))
				lostGatewayReply.Store(false)
				dropNextGatewayReply.Store(scenario == "known")
				e.fixture.droppedCallReply.Store(false)
				mcpReplyLost := scenario == "unknown" || scenario == "live-unknown"
				e.fixture.dropNextCallReply.Store(mcpReplyLost)
				before := atomic.LoadInt32(&e.fixture.effCount)
				runNativeGatewayTrial(t, python, server, e,
					sign("adk-original", "adk-journal"), sign("adk-replacement", "adk-journal"),
					scenario, "adk_gateway_probe.py", "")
				if atomic.LoadInt32(&e.fixture.effCount)-before != 1 ||
					e.fixture.droppedCallReply.Load() != mcpReplyLost ||
					lostGatewayReply.Load() != (scenario == "known") {
					t.Fatal("native ADK did not hit the requested effect boundary")
				}
				t.Log("mcp_effects=1")
			})
		}
		for index, scenario := range []string{"confirmation", "hitl", "hitl-answer-lost"} {
			t.Run("native_adk_"+scenario, func(t *testing.T) {
				item := strconv.Itoa(210 + index)
				t.Setenv("AHP_JOURNAL_ADK_FIXTURE_ITEM", item)
				e.allowDigest(t, "write", json.RawMessage(`{"item":`+item+`}`))
				dropNextGatewayReply.Store(false)
				e.fixture.dropNextCallReply.Store(false)
				before := atomic.LoadInt32(&e.fixture.effCount)
				runNativeGatewayTrial(t, python, server, e,
					sign("adk-original", "adk-hitl"), sign("adk-replacement", "adk-hitl"),
					scenario, "adk_hitl_probe.py", "")
				expected := int32(0)
				if scenario != "confirmation" {
					expected = 1
				}
				if got := atomic.LoadInt32(&e.fixture.effCount) - before; got != expected {
					t.Fatalf("ADK effect count %d, want %d", got, expected)
				}
				t.Logf("mcp_effects=%d", expected)
			})
		}
	}
}
