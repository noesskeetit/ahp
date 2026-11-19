package space_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// late_subscribe_finished_chat_test.go covers the hole named honestly in the D4
// report and made a test requirement by the deputy (forum 20261101T105343Z, §2):
// the D4 legs prove the CANCELED thread and the terminal frame behind a lost ACK,
// and nothing in this package proved the shape the production probe measured as
// healthy - a chat whose run finished on its own, opened by a subscriber who
// arrives AFTER the last frame.
//
// That is the everyday user action: reopen a finished chat from the chat list,
// hours later, in a process that may have been restarted many times since. It must
// replay the whole durable transcript (not a tail), report the terminal state, and
// then close, so an EventSource client does not sit on an open-but-silent
// connection and reconnect forever. The SSE tail is a projection of ReadStream, so
// "the JSON path has N events and the SSE path delivered 0" is a contradiction,
// and it is now a test failure rather than a forum argument.

// lateSubscriberSSE reads a late SSE subscription to the end, keeping the durable
// cursor of every frame (readSSEUntilEnd deliberately throws the id line away; a
// replay test cannot). It returns the durable frames in delivery order, the
// end-of-stream frame if one arrived, and whether the server closed the body.
func lateSubscriberSSE(t *testing.T, url, subject string, deadline time.Duration) (durable []sseFrame, end *sseFrame, closed bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("newreq: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer tok-"+subject)
	req.Header.Set("X-Test-Subject", subject)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET sse: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d, want 200 (body=%q)", resp.StatusCode, readAll(resp.Body))
	}
	br := bufio.NewReader(resp.Body)
	var evLine, data string
	var id int64
	for {
		line, rerr := br.ReadString('\n')
		if rerr != nil {
			if rerr == io.EOF {
				closed = true
			}
			return durable, end, closed
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			if data == "" {
				continue
			}
			var env map[string]any
			_ = json.Unmarshal([]byte(data), &env)
			f := sseFrame{id: id, event: strings.TrimSpace(strings.TrimPrefix(evLine, "event:")), envel: env, raw: data}
			if f.event == "end" {
				cp := f
				end = &cp
			} else {
				durable = append(durable, f)
			}
			evLine, data = "", ""
		case strings.HasPrefix(line, "id:"):
			if n, cerr := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "id:")), 10, 64); cerr == nil {
				id = n
			}
		case strings.HasPrefix(line, "event:"):
			evLine = line
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

// durableStreamCount is how many events the JSON path reports for a thread. It is
// the yardstick the SSE replay is compared against: the two paths must agree.
func durableStreamCount(t *testing.T, edge, threadID, subject string) int {
	t.Helper()
	c, b := postSpaceAs(t, edge, "ReadStream", map[string]any{"thread_id": threadID, "after": 0, "limit": 100}, subject)
	if c != http.StatusOK {
		t.Fatalf("ReadStream(%s) = %d, want 200 (body=%s)", threadID, c, b)
	}
	events, _ := decodeMap(t, b)["events"].([]any)
	return len(events)
}

// finishChatOn drives a chat to a NATURAL done (no cancel anywhere) on a
// caller-supplied Space edge and returns (turnID, threadID). It is finishChat with
// the edge passed in, so the same routine can serve a "before restart" server.
func finishChatOn(t *testing.T, base, subject, tag string) (string, string) {
	t.Helper()
	code, body := postSpaceAs(t, base, "AcceptTurn", map[string]any{
		"key":        "late-" + tag + "-" + contracts.NewID(),
		"context_id": "ctx-late-" + tag,
		"input":      map[string]any{"role": "user", "content": tag},
		"max_steps":  3,
	}, subject)
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn(%s) = %d, want 200 (body=%s)", subject, code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)
	if turnID == "" || threadID == "" {
		t.Fatalf("AcceptTurn(%s) missing ids: %s", subject, body)
	}
	if c, b := postSpaceAs(t, base, "DispatchTurn", nil, subject); c != http.StatusOK {
		t.Fatalf("DispatchTurn(%s) = %d, want 200 (body=%s)", subject, c, b)
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		c, b := postSpaceAs(t, base, "GetTurn", map[string]any{"turn_id": turnID}, subject)
		if c != http.StatusOK {
			t.Fatalf("GetTurn(%s) poll = %d (body=%s)", subject, c, b)
		}
		if s, _ := decodeMap(t, b)["status"].(string); s == string(space.TurnDone) {
			waitDurableTerminal(t, base, subject, threadID)
			return turnID, threadID
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("chat(%s) never reached done", subject)
	return "", ""
}

// assertLateReplay states the gate in the acceptance's own words: frames > 0, the
// terminal state delivered, the stream closed. It also pins the cursor contract,
// because a late replay that dropped or repeated frames would pass a bare
// "frames > 0" check.
func assertLateReplay(t *testing.T, edge, threadID, subject string, durableWant int) {
	t.Helper()
	durable, end, closed := lateSubscriberSSE(t, edge+"/v1/space/stream/"+threadID, subject, 12*time.Second)
	if len(durable) == 0 {
		t.Fatalf("late subscriber on a finished chat got 0 durable frames from %s (end=%v closed=%v): the transcript is durable and ReadStream has %d events, so silence is a defect, not a state of the world",
			edge, end != nil, closed, durableWant)
	}
	// The two paths must agree exactly. The one legitimate asymmetry is the JSON
	// reader's own page cap: when the transcript is longer than the limit the yardstick
	// was read with, the tail may correctly deliver MORE, so only a shorter tail is a
	// failure there. Silently demanding equality at any size would make this test fail
	// for a reason that has nothing to do with late replay.
	if len(durable) != durableWant && !(durableWant >= 100 && len(durable) > durableWant) {
		t.Fatalf("late replay delivered %d durable frames while ReadStream reports %d for the same thread - the SSE tail must be the same projection", len(durable), durableWant)
	}
	var last int64
	for i, f := range durable {
		if f.id <= last {
			t.Fatalf("durable frame[%d] cursor %d is not ascending after %d", i, f.id, last)
		}
		last = f.id
		if f.event == "" {
			t.Fatalf("durable frame[%d] carries no event name: %s", i, f.raw)
		}
	}
	if end == nil {
		t.Fatalf("a finished chat's stream never said `event: end` (delivered %d durable frames, closed=%v): an EventSource client keeps reconnecting to a connection that can never produce another frame", len(durable), closed)
	}
	status := ""
	if p, ok := end.envel["payload"].(map[string]any); ok {
		status, _ = p["status"].(string)
	}
	if status != "done" {
		t.Fatalf("the end frame reported status %q, want done (raw=%s)", status, end.raw)
	}
	if !closed {
		t.Fatalf("the end frame was delivered but the connection stayed open - the D4 symptom on a finished chat")
	}
}

// TestLateSubscriberReplaysFinishedChat is the requirement itself: a chat that
// finished on its own, in this process, opened afterwards, replays everything and
// terminates.
func TestLateSubscriberReplaysFinishedChat(t *testing.T) {
	ms := newMockState()
	edge := sharedSpace(t, ms, &stubCore{stepsBeforeDone: 2}, authPerSubject)
	_, threadID := finishChatOn(t, edge.URL, "alice", "finished")

	// The transcript must be durable BEFORE anyone subscribes - otherwise the tail
	// would be watching a live run and the test would prove nothing about late replay.
	want := durableStreamCount(t, edge.URL, threadID, "alice")
	if want == 0 {
		t.Fatalf("a finished chat has no durable stream events at all")
	}
	assertLateReplay(t, edge.URL, threadID, "alice", want)
}

// TestLateSubscriberReplaysFinishedChatAfterRestart is the same user action in the
// shape a real deployment takes: the process that ran the chat is long gone, the
// chat is reopened on a fresh coordinator, and authorization has to survive on the
// durable fallback rather than the in-memory turn table.
func TestLateSubscriberReplaysFinishedChatAfterRestart(t *testing.T) {
	ms := newMockState()
	before := sharedSpace(t, ms, &stubCore{stepsBeforeDone: 2}, authPerSubject)
	_, threadID := finishChatOn(t, before.URL, "alice", "finished-restarted")

	after := sharedSpace(t, ms, &stubCore{}, authPerSubject)
	want := durableStreamCount(t, after.URL, threadID, "alice")
	if want == 0 {
		t.Fatalf("no durable stream events survived the restart")
	}
	assertLateReplay(t, after.URL, threadID, "alice", want)

	// A different subject cannot open it, late or otherwise, and is not told the
	// chat exists.
	req, err := http.NewRequest(http.MethodGet, after.URL+"/v1/space/stream/"+threadID, nil)
	if err != nil {
		t.Fatalf("newreq: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer tok-bob")
	req.Header.Set("X-Test-Subject", "bob")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET sse as bob: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("foreign late subscriber got %d, want 404", resp.StatusCode)
	}
}
