package execution_test

// card_capability_fidelity_test.go -- BLOCK 3 of the contract: the public
// interface must really serve what the published Agent Card declares, or the card
// is a defect ("иначе карточка врёт — это дефект").
//
// served_surface_test.go already proves the METHOD NAMES (every served method is
// dispatched, every A2A v0 name is refused with -32601, the advertised skill
// reaches COMPLETED with a real artifact for both wire shapes). What was never
// driven is the CAPABILITY FLAGS of that same document:
//
//	streaming         = true  -> SendStreamingMessage must really stream to terminal
//	extendedAgentCard = true  -> GetExtendedAgentCard must answer with the same agent
//	pushNotifications = false -> the four push-config methods must REFUSE, honestly
//
// A flag is a promise to a client that picks its transport by reading it. Each
// leg below reads the flag from the card and then contradicts or confirms it on
// the wire, so a future edit to either side -- flipping the card, or gating the
// handler without updating the card -- fails here instead of in a client.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const pushNotSupportedReason = "PUSH_NOTIFICATION_NOT_SUPPORTED"

// sseCall drives one streaming JSON-RPC call and returns the raw HTTP response
// status, content type and the decoded `data:` events, stopping as soon as
// `stop` reports an event it was waiting for (so a hung stream fails on the
// deadline instead of hanging the lane).
func sseCall(t *testing.T, url, method string, params any, sc contracts.Scope, stop func(map[string]any) bool) (int, string, []map[string]any) {
	t.Helper()
	pb, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	bb, _ := json.Marshal(rpcReq{JSONRPC: "2.0", ID: 1, Method: method, Params: pb})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bb))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Test-Tenant", sc.Tenant)
	req.Header.Set("X-Test-Deployment", sc.Deployment)
	req.Header.Set("X-Test-Subject", sc.Subject)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s streaming call: %v", method, err)
	}
	defer resp.Body.Close()
	events := []map[string]any{}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			t.Fatalf("%s sent a non-JSON SSE frame %q: %v", method, payload, err)
		}
		events = append(events, ev)
		if stop != nil && stop(ev) {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("%s stream read: %v", method, err)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), events
}

func streamEventBody(ev map[string]any) (string, map[string]any) {
	res, _ := ev["result"].(map[string]any)
	if res == nil {
		return "", nil
	}
	for _, kind := range []string{"task", "message", "statusUpdate", "artifactUpdate"} {
		if body, ok := res[kind].(map[string]any); ok {
			return kind, body
		}
	}
	return "", nil
}

// TestStreamingCapabilityIsReal: the operator's card and this plane's own card
// fixture both declare capabilities.streaming = true, and the advertised skill
// text names SendStreamingMessage as the way to watch a turn. A client that
// believes the flag must actually receive a stream that carries its task to a
// terminal state.
func TestStreamingCapabilityIsReal(t *testing.T) {
	pool, store, ledger, sfx := gwSetup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	nonce := hex.EncodeToString(rnd[:])
	body := "stream-fidelity-" + nonce[:8]
	ex := newExec(t, ctx, pool, sfx, compModel{body: body})
	svc, err := gateway.NewService(store, ledger, ex, gateway.NullNotifier{}, gateway.Config{
		WorkerID: "stream-" + sfx[:8], Lease: 5 * time.Second, Poll: 10 * time.Millisecond,
		MaxBlock: 2 * time.Second, MaxAttempts: 2, ParkLease: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	go svc.Run(ctx)

	card := platformCardForFace(body)
	url := mountFaceWithCard(t, svc, card)
	owner := contracts.Scope{Tenant: "t-stream-" + nonce[:8], Deployment: "d-stream", Subject: "owner"}

	httpCode, contentType, events := sseCall(t, url, gateway.MethodSendStreamingMessage, map[string]any{
		"message": map[string]any{
			"kind":      "message",
			"role":      "user",
			"messageId": "stream-" + nonce,
			"parts":     []any{map[string]any{"kind": "text", "text": "produce the artifact"}},
		},
	}, owner, func(ev map[string]any) bool {
		kind, b := streamEventBody(ev)
		st, _ := b["status"].(map[string]any)
		state, _ := st["state"].(string)
		return kind == "statusUpdate" && strings.HasPrefix(state, "TASK_STATE_") &&
			(state == "TASK_STATE_COMPLETED" || state == "TASK_STATE_FAILED" || state == "TASK_STATE_CANCELED" || state == "TASK_STATE_REJECTED")
	})

	if !card.Capabilities.Streaming {
		// The face streams; a card that denies it is just as false as one that
		// promises it. Fail with the direction named instead of silently agreeing.
		t.Fatalf("the face really streams (%d events) but the published card declares capabilities.streaming=false", len(events))
	}
	if httpCode != http.StatusOK {
		t.Fatalf("declared streaming, got http=%d (%d events)", httpCode, len(events))
	}
	if !strings.HasPrefix(contentType, "text/event-stream") {
		t.Fatalf("declared streaming, got Content-Type %q -- a client picks its transport by this header", contentType)
	}
	if len(events) == 0 {
		t.Fatalf("declared streaming, but the response carried no SSE frame at all")
	}

	var taskID, firstKind, terminalState string
	seenKinds := map[string]int{}
	for _, ev := range events {
		kind, b := streamEventBody(ev)
		if kind == "" {
			continue
		}
		seenKinds[kind]++
		if id, ok := b["taskId"].(string); ok && taskID == "" {
			taskID = id
		}
		if id, ok := b["id"].(string); ok && taskID == "" {
			taskID = id
		}
		if firstKind == "" {
			firstKind = kind
		}
		if st, ok := b["status"].(map[string]any); ok {
			if s, _ := st["state"].(string); s != "" {
				terminalState = s
			}
		}
	}
	if taskID == "" {
		t.Fatalf("the stream carried %d events but never named a task: %v", len(events), seenKinds)
	}
	if firstKind != "task" {
		t.Fatalf("the first SSE event must be the durably accepted task (a client that disconnects immediately still needs to see its task), got %q", firstKind)
	}
	if terminalState != "TASK_STATE_COMPLETED" {
		t.Fatalf("the streamed task %s never reached COMPLETED on the stream (last state %q, kinds %v)", taskID, terminalState, seenKinds)
	}
	if seenKinds["artifactUpdate"] == 0 {
		t.Fatalf("the streamed task %s completed on the stream but never carried an artifactUpdate, while GetTask would show one", taskID)
	}

	// The artifact references named on the stream must be the ones the task
	// actually carries -- an update event about some other bytes is a nicer-looking
	// lie. The wire names this field artifactId (a2a.Artifact.ID), not id.
	streamed := map[string]bool{}
	for _, ev := range events {
		if kind, b := streamEventBody(ev); kind == "artifactUpdate" {
			art, _ := b["artifact"].(map[string]any)
			if art == nil {
				t.Fatalf("an artifactUpdate frame carried no artifact object: %v", b)
			}
			id, _ := art["artifactId"].(string)
			if id == "" {
				t.Fatalf("artifactUpdate frame carries an artifact without artifactId: %v", art)
			}
			streamed[id] = true
		}
	}

	// The stream's own claim must agree with the durable record.
	raw, e := rpcCall(t, url, gateway.MethodGetTask, &a2a.GetTaskRequest{ID: a2a.TaskID(taskID)}, owner)
	if e != nil {
		t.Fatalf("GetTask after the stream: %+v", e)
	}
	var durable a2a.Task
	if err := json.Unmarshal(raw, &durable); err != nil {
		t.Fatalf("GetTask shape: %s", raw)
	}
	if durable.Status.State != a2a.TaskStateCompleted || len(durable.Artifacts) == 0 {
		t.Fatalf("task %s streamed to COMPLETED but the durable task says state=%s artifacts=%d", taskID, durable.Status.State, len(durable.Artifacts))
	}
	for _, art := range durable.Artifacts {
		if !streamed[string(art.ID)] {
			t.Fatalf("artifact %s (%s) is on the durable task but was never announced on the stream that watched this turn", art.ID, art.Name)
		}
	}

	writeEvidence(t, "a2a.streaming_capability", map[string]any{
		"http_status": httpCode, "content_type": contentType, "task_id": taskID,
		"stream_events": len(events), "stream_kinds": seenKinds,
		"terminal_state":     string(durable.Status.State),
		"artifacts":          len(durable.Artifacts),
		"artifact_names":     artifactNames(durable.Artifacts),
		"declared_streaming": card.Capabilities.Streaming,
	})
	t.Logf("streaming is real: task=%s events=%d kinds=%v terminal=%s artifacts=%d",
		taskID, len(events), seenKinds, durable.Status.State, len(durable.Artifacts))
}

func artifactNames(arts []*a2a.Artifact) []string {
	out := make([]string, 0, len(arts))
	for _, a := range arts {
		if a == nil {
			continue
		}
		out = append(out, string(a.ID)+":"+a.Name)
	}
	return out
}

// TestExtendedAgentCardAgreesWithPublishedCard: capabilities.extendedAgentCard is
// declared true, so GetExtendedAgentCard must answer, and it must answer with THE
// SAME agent -- same name, version, skills and AHP extension. An extended card
// that disagrees is the "card claims an interface the plane does not serve" defect
// in its other direction (a client that escalates to the extended card gets a
// different agent than the one it hired).
func TestExtendedAgentCardAgreesWithPublishedCard(t *testing.T) {
	svc := newServiceWithStub(t)
	card := platformCardForFace("ext-card")
	url := mountFaceWithCard(t, svc, card)
	owner := contracts.Scope{Tenant: "t-extcard", Deployment: "d-extcard", Subject: "owner"}

	wellKnown := strings.TrimSuffix(url, "/a2a/public") + gateway.WellKnownAgentCardPath
	resp, err := http.Get(wellKnown)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200 with the published card", wellKnown, resp.StatusCode)
	}
	var published a2a.AgentCard
	if err := json.NewDecoder(resp.Body).Decode(&published); err != nil {
		t.Fatalf("decode published card: %v", err)
	}

	raw, e := rpcCall(t, url, gateway.MethodGetExtendedAgentCard, map[string]any{}, owner)
	if e != nil {
		t.Fatalf("%s refused although capabilities.extendedAgentCard=true: %+v", gateway.MethodGetExtendedAgentCard, e)
	}
	var extended a2a.AgentCard
	if err := json.Unmarshal(raw, &extended); err != nil {
		t.Fatalf("decode extended card: %s", raw)
	}

	if extended.Name != published.Name || extended.Version != published.Version {
		t.Fatalf("extended card is a different agent: name=%q/%q version=%q/%q",
			extended.Name, published.Name, extended.Version, published.Version)
	}
	if len(extended.Skills) != len(published.Skills) {
		t.Fatalf("extended card advertises %d skills, the published card %d", len(extended.Skills), len(published.Skills))
	}
	for i, sk := range published.Skills {
		if extended.Skills[i].ID != sk.ID {
			t.Fatalf("skill %d differs: published %q extended %q", i, sk.ID, extended.Skills[i].ID)
		}
	}
	if extended.Capabilities.Streaming != published.Capabilities.Streaming ||
		extended.Capabilities.PushNotifications != published.Capabilities.PushNotifications {
		t.Fatalf("transport capabilities differ between the published and extended card: streaming %v/%v push %v/%v",
			published.Capabilities.Streaming, extended.Capabilities.Streaming,
			published.Capabilities.PushNotifications, extended.Capabilities.PushNotifications)
	}
	pubExt := extensionParams(t, published)
	extExt := extensionParams(t, extended)
	for k, want := range pubExt {
		if got := extExt[k]; got != want {
			t.Fatalf("AHP extension param %q differs: published %v extended %v", k, want, got)
		}
	}
	writeEvidence(t, "a2a.extended_card", map[string]any{
		"wellknown_path": wellKnown, "wellknown_status": resp.StatusCode,
		"published_name": published.Name, "published_version": published.Version,
		"extended_name": extended.Name, "extended_version": extended.Version,
		"skills": len(extended.Skills), "extension_params": extExt,
	})
	t.Logf("extended card agrees with the published one: name=%q version=%q skills=%d extension=%v",
		extended.Name, extended.Version, len(extended.Skills), extExt)
}

func extensionParams(t *testing.T, card a2a.AgentCard) map[string]any {
	t.Helper()
	for _, ext := range card.Capabilities.Extensions {
		if ext.URI == gateway.AgentExtensionURI {
			return ext.Params
		}
	}
	t.Fatalf("card carries no %s extension", gateway.AgentExtensionURI)
	return nil
}

// TestPushNotificationsDeclaredFalseIsRefused: the published card says
// pushNotifications=false because outbound push delivery has never been proven on
// this stand. Every push-config method must therefore REFUSE with the honest
// reason -- never store the configuration and never answer a 200 that a client
// would read as "you will be notified".
func TestPushNotificationsDeclaredFalseIsRefused(t *testing.T) {
	svc := newServiceWithStub(t)
	card := platformCardForFace("push-false")
	url := mountFaceWithCard(t, svc, card)
	owner := contracts.Scope{Tenant: "t-push", Deployment: "d-push", Subject: "owner"}
	if card.Capabilities.PushNotifications {
		t.Skip("SKIP_EXPLICIT: this leg only means something while the card declares pushNotifications=false")
	}

	methods := []string{
		gateway.MethodCreatePushConfig, gateway.MethodGetPushConfig,
		gateway.MethodListPushConfigs, gateway.MethodDeletePushConfig,
	}
	seen := map[string]any{}
	for _, m := range methods {
		code, e, raw := rawRPC(t, url, m, map[string]any{
			"taskId": "01A19100000000000000000000",
			"id":     "cfg-push-fidelity",
			"url":    "https://push.example.invalid/hook",
		}, owner)
		if e == nil {
			t.Fatalf("%s accepted a push configuration while the card declares pushNotifications=false: %s", m, raw)
		}
		if e.Code == jsonrpcMethodNotFound {
			t.Fatalf("%s must be dispatched and refused for its own reason, not hidden behind METHOD_NOT_FOUND: %s", m, raw)
		}
		if !strings.Contains(raw, pushNotSupportedReason) {
			t.Fatalf("%s refused with %d %q, want the honest reason %s in the body: %s", m, e.Code, e.Message, pushNotSupportedReason, raw)
		}
		seen[m] = map[string]any{"http": code, "code": e.Code, "reason": pushNotSupportedReason}
		t.Logf("%s refused honestly: http=%d code=%d %q", m, code, e.Code, e.Message)
	}

	// The same promise is made on the send path: attaching a push config to a
	// task must be refused at the door, not accepted and then silently dropped.
	code, e, raw := rawRPC(t, url, gateway.MethodSendMessage, map[string]any{
		"message": map[string]any{
			"kind": "message", "role": "user", "messageId": "push-send-1",
			"parts": []any{map[string]any{"kind": "text", "text": "with a push hook"}},
		},
		"configuration": map[string]any{
			"returnImmediately":          true,
			"taskPushNotificationConfig": map[string]any{"url": "https://push.example.invalid/hook"},
		},
	}, owner)
	if e == nil {
		t.Fatalf("SendMessage accepted a push configuration while push notifications are declared false: %s", raw)
	}
	if !strings.Contains(raw, pushNotSupportedReason) {
		t.Fatalf("SendMessage refused a push config for the wrong reason http=%d code=%d %q", code, e.Code, e.Message)
	}
	writeEvidence(t, "a2a.push_declared_false", map[string]any{
		"declared_push_notifications": false, "methods_refused": seen,
		"send_with_push_config": map[string]any{"http": code, "code": e.Code},
	})
}
