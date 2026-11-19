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

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// sseFrame is one parsed Server-Sent Events frame.
type sseFrame struct {
	id    int64
	event string
	envel map[string]any
	raw   string
}

// readSSE opens the Space SSE tail and reads frames until want is reached, the
// context deadline expires, or the stream errors. Because the tail is a live
// subscription (it blocks waiting for new events), callers pass a short deadline
// and treat the collected prefix as the answer.
func readSSE(t *testing.T, url, subject, lastEventID string, want int, deadline time.Duration) []sseFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("newreq: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer tok-"+subject)
	if subject != "" {
		req.Header.Set("X-Test-Subject", subject)
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET sse: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE status = %d, want 200 (body=%q)", resp.StatusCode, readAll(resp.Body))
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("SSE content-type = %q, want text/event-stream", ct)
	}

	br := bufio.NewReader(resp.Body)
	var frames []sseFrame
	var idLine, evLine, data string
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return frames // deadline / disconnect
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			// frame boundary
			if data != "" {
				var env map[string]any
				_ = json.Unmarshal([]byte(data), &env)
				var id int64
				if idLine != "" {
					id, _ = strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(idLine, "id:")), 10, 64)
				}
				frames = append(frames, sseFrame{id: id, event: strings.TrimSpace(strings.TrimPrefix(evLine, "event:")), envel: env, raw: data})
				idLine, evLine, data = "", "", ""
				if len(frames) >= want {
					return frames
				}
			}
		case strings.HasPrefix(line, "id:"):
			idLine = line
		case strings.HasPrefix(line, "event:"):
			evLine = line
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
}

func readAll(rc io.ReadCloser) string {
	b, _ := io.ReadAll(io.LimitReader(rc, 512))
	return string(b)
}

// dispatchToStream accepts+dispatches one turn as `subject` and waits (bounded)
// until the thread's durable stream has at least 2 events, returning the thread id.
func dispatchToStream(t *testing.T, base, subject, ctxLabel string) string {
	t.Helper()
	code, body := postSpaceAs(t, base, "AcceptTurn", map[string]any{
		"key":        "sse-" + ctxLabel + "-" + subject,
		"context_id": "ctx-" + ctxLabel + "-" + subject,
		"input":      map[string]any{"role": "user", "content": "go"},
		"max_steps":  2,
	}, subject)
	if code != 200 {
		t.Fatalf("AcceptTurn = %d (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	threadID, _ := acc["thread_id"].(string)
	if threadID == "" {
		t.Fatalf("missing thread_id: %s", body)
	}
	if c, b := postSpaceAs(t, base, "DispatchTurn", nil, subject); c != 200 {
		t.Fatalf("DispatchTurn = %d (body=%s)", c, b)
	}
	// Wait for the stream to have events (same guarantee the poll journey relies on).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, b := postSpaceAs(t, base, "ReadStream", map[string]any{"thread_id": threadID, "after": 0, "limit": 100}, subject)
		if c == 200 {
			if evs, ok := decodeMap(t, b)["events"].([]any); ok && len(evs) >= 2 {
				return threadID
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stream never accumulated >=2 events for %s", subject)
	return ""
}

// TestSSEStreamReplay proves the browser SSE contract on the real Space edge:
// frames carry id==durable sequence cursor and an {type,cursor,payload} envelope,
// events arrive in ascending cursor order, and replay via ?after= / Last-Event-ID
// resumes strictly after the given cursor (no re-delivery at/below it).
func TestSSEStreamReplay(t *testing.T) {
	srv := newHTTPSpace(t, artifactCore{}, authPerSubject)
	threadID := dispatchToStream(t, srv.URL, "alice", "replay")

	// Fresh read: collect at least 2 frames.
	url := srv.URL + "/v1/space/stream/" + threadID + "?after=0"
	frames := readSSE(t, url, "alice", "", 2, 2*time.Second)
	if len(frames) < 2 {
		t.Fatalf("expected >=2 SSE frames, got %d", len(frames))
	}
	var last int64 = -1
	for i, f := range frames {
		if f.id <= last {
			t.Fatalf("frame[%d] cursor not ascending: id=%d after %d", i, f.id, last)
		}
		last = f.id
		if f.envel["cursor"] == nil {
			t.Fatalf("frame[%d] missing cursor in envelope: %s", i, f.raw)
		}
		if c, ok := f.envel["cursor"].(float64); !ok || int64(c) != f.id {
			t.Fatalf("frame[%d] envelope cursor (%v) != SSE id (%d)", i, f.envel["cursor"], f.id)
		}
		if tp, _ := f.envel["type"].(string); tp == "" {
			t.Fatalf("frame[%d] envelope missing non-empty type: %s", i, f.raw)
		}
		if _, ok := f.envel["payload"]; !ok {
			t.Fatalf("frame[%d] envelope missing payload: %s", i, f.raw)
		}
	}

	// Replay strictly after the FIRST cursor: the first event must be excluded.
	firstCursor := strconv.FormatInt(frames[0].id, 10)
	replay := readSSE(t, srv.URL+"/v1/space/stream/"+threadID+"?after="+firstCursor, "alice", "", 1, 700*time.Millisecond)
	for _, f := range replay {
		if f.id <= frames[0].id {
			t.Fatalf("replay after cursor %s re-delivered event id=%d (<=)", firstCursor, f.id)
		}
	}

	// Last-Event-ID header is honored equivalently (resume strictly after).
	leFrames := readSSE(t, url, "alice", firstCursor, 1, 700*time.Millisecond)
	for _, f := range leFrames {
		if f.id <= frames[0].id {
			t.Fatalf("Last-Event-ID=%s re-delivered id=%d", firstCursor, f.id)
		}
	}
}

// TestSSEStreamAuthorization proves the SSE edge enforces scope from the verified
// token: a foreign subject gets 404 (never 200, never a stream), and the only GET
// route is /v1/space/stream/* — any other GET (e.g. a POST-only op) stays 404.
func TestSSEStreamAuthorization(t *testing.T) {
	srv := newHTTPSpace(t, artifactCore{}, authPerSubject)
	threadID := dispatchToStream(t, srv.URL, "alice", "authz")

	// Foreign subject must NOT open the stream.
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/space/stream/"+threadID, nil)
	req.Header.Set("Authorization", "Bearer tok-bob")
	req.Header.Set("X-Test-Subject", "bob")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("foreign GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("foreign SSE = %d, want 404 (no leak)", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("foreign request opened an SSE stream (content-type=%q)", ct)
	}

	// Unknown thread for a valid subject also 404s.
	req2, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/space/stream/no-such-thread", nil)
	req2.Header.Set("Authorization", "Bearer tok-alice")
	req2.Header.Set("X-Test-Subject", "alice")
	r2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("unknown GET: %v", err)
	}
	r2.Body.Close()
	if r2.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown-thread SSE = %d, want 404", r2.StatusCode)
	}

	// Only the stream/* GET exists; a POST-only op is still unreachable via GET.
	req3, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/space/AcceptTurn", nil)
	r3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("GET op: %v", err)
	}
	r3.Body.Close()
	if r3.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /v1/space/AcceptTurn = %d, want 404", r3.StatusCode)
	}
	_ = contracts.ErrNotFound
}
