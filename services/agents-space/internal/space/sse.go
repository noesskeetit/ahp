package space

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	// streamWriteTimeout bounds a stalled socket write, not the lifetime of a
	// healthy SSE subscription or the idle wait for its next durable event.
	streamWriteTimeout = 10 * time.Second
	// streamPollInterval is how long the SSE tail waits for new durable events
	// when it has drained the current page. It is a read cadence, not a durability
	// guarantee (events are already persisted in State before they are visible).
	streamPollInterval = 300 * time.Millisecond
	// Only SSE opts into byte-bounded pages. Legacy count-only replay/history
	// readers retain their page size and short-page semantics.
	streamPageSize  = 256
	streamPageBytes = 16 << 20
	// quiesceCheckEveryCycles bounds how often an IDLE tail asks the durable run store
	// whether the thread is finished. The tail itself cannot know no further event will ever
	// arrive - only the run lifecycle can - so it asks, but on a slower cadence than the
	// event poll so a live subscription does not pay for the check twice as often.
	quiesceCheckEveryCycles = 10
)

// runStatusTerminal reports whether a durable run status ends the run's lifecycle. It
// mirrors the published State wire contract (pending/running/waiting are live;
// done/failed/canceled are terminal) instead of importing the State implementation: Space
// reaches State only through its published API.
func runStatusTerminal(status string) bool {
	switch status {
	case "done", "failed", "canceled":
		return true
	}
	return false
}

// threadTerminalStatus reports whether the DURABLE run store says this thread is finished -
// at least one run and every run terminal - together with the status of its last run.
//
// This is the only honest basis for closing an SSE tail: ReadStream returning nothing means
// "nothing yet", never "never again". A ListRuns failure answers "not terminal" (keep
// tailing): a read error must never fabricate a terminal state for a chat.
func (h *Handler) threadTerminalStatus(ctx context.Context, token, threadID string) (string, bool) {
	runs, err := h.coordinator.state.ListRuns(ctx, token, threadID)
	if err != nil || len(runs) == 0 {
		return "", false
	}
	last := runs[0]
	for _, r := range runs {
		if !runStatusTerminal(r.Status) {
			return "", false
		}
		if r.Sequence >= last.Sequence {
			last = r
		}
	}
	return last.Status, true
}

// serveStream projects a thread's durable event stream as Server-Sent Events for
// the browser (PDF rev 3.2: the user chat consumes an SSE replay cursor). It is a
// thin, stateless projection of the accepted State ReadStream — sequence is the
// durable cursor and this adds nothing to State, no new wire, no new writes.
//
//	GET /v1/space/stream/{thread_id}?after=<cursor>
//
// Guarantees that the frontend contract review (ROOT) requires:
//   - scope/subject are taken ONLY from the verified principal via authorizeThread;
//     the browser never sends subject/tenant, and a foreign or unknown thread is
//     answered 404 BEFORE any stream is opened — it is never a leak.
//   - each event is one SSE frame with id == the durable sequence cursor, event ==
//     the event kind, and a JSON data envelope {type,cursor,thread_id,event_id,
//     payload}. Replay/resume uses ?after=<cursor> or the SSE Last-Event-ID header
//     (resumes strictly after that cursor).
//
// The Human -> authorizedSpace -> scopedTask transition is preserved: this edge
// forwards the caller's bearer task-token to State exactly like the POST ops, so
// the server-side task scope (minted from the verified human identity) is what
// bounds the read — the browser cannot widen it.
func (h *Handler) serveStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "space: unknown route"))
		return
	}
	threadID := strings.TrimPrefix(r.URL.Path, "/v1/space/stream/")
	if threadID == "" || strings.Contains(threadID, "/") {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "space: unknown route"))
		return
	}
	// Authorization BEFORE opening the stream: foreign/unknown thread -> 404, no
	// SSE headers written, so no partial leak.
	if _, err := h.authorizeThread(r, threadID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// Resolve the replay cursor: ?after= wins unless Last-Event-ID is higher.
	after := int64(0)
	if q := r.URL.Query().Get("after"); q != "" {
		v, err := strconv.ParseInt(q, 10, 64)
		if err != nil || v < 0 {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "space: invalid after cursor"))
			return
		}
		after = v
	}
	if le := strings.TrimSpace(r.Header.Get("Last-Event-ID")); le != "" {
		if v, err := strconv.ParseInt(le, 10, 64); err == nil && v > after {
			after = v
		}
	}
	_, ok := w.(http.Flusher)
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "space: streaming unsupported"))
		return
	}

	token, err := h.stateToken(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	ctx := r.Context()
	controller := http.NewResponseController(w)
	deadline := func(at time.Time) bool {
		err := controller.SetWriteDeadline(at)
		return err == nil || errors.Is(err, http.ErrNotSupported)
	}
	// Authorization, scoped State token and cursor validation have all finished.
	// The server's absolute response deadline must not expire while this tail is
	// waiting for a durable event. Each actual socket write remains bounded below.
	if !deadline(time.Time{}) {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "space: streaming deadline unavailable"))
		return
	}
	writeFrame := func(format string, args ...any) bool {
		if ctx.Err() != nil || !deadline(time.Now().Add(streamWriteTimeout)) {
			return false
		}
		_, err := fmt.Fprintf(w, format, args...)
		return err == nil
	}
	flush := func() bool {
		if ctx.Err() != nil || !deadline(time.Now().Add(streamWriteTimeout)) {
			return false
		}
		if controller.Flush() != nil || ctx.Err() != nil {
			return false
		}
		// No socket work is pending: an idle State poll must not consume a write
		// deadline. Preserve the existing per-page flush cadence and SSE bytes.
		return deadline(time.Time{})
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	if !deadline(time.Now().Add(streamWriteTimeout)) {
		return
	}
	w.WriteHeader(http.StatusOK)
	if !flush() {
		return
	}

	cursor := after
	// Seeded so the FIRST idle cycle performs the terminal check: a chat that was canceled
	// long ago must terminate the moment it is opened, not several poll lags later (D4).
	idleCycles := quiesceCheckEveryCycles - 1
	for {
		events, hasMore, err := h.coordinator.state.ReadStreamPage(ctx, token, threadID, cursor, streamPageSize, streamPageBytes)
		if err != nil {
			// Emit a terminal error frame and close; the client reconnects with
			// its last cursor. We never panic mid-stream.
			if writeFrame("event: error\ndata: {\"type\":\"error\"}\n\n") {
				flush()
			}
			return
		}
		for _, ev := range events {
			env := map[string]any{
				"type":      ev.Kind,
				"cursor":    ev.Sequence,
				"thread_id": ev.ThreadID,
				"event_id":  ev.ID,
				"key":       ev.Key,
				"payload":   ev.Payload,
			}
			b, merr := json.Marshal(env)
			if merr != nil {
				continue
			}
			// SSE frame: id, event, data (each on its own line, terminated by a
			// blank line per the EventSource spec).
			if !writeFrame("id: %d\nevent: %s\ndata: %s\n\n", ev.Sequence, ev.Kind, b) {
				return
			}
			cursor = ev.Sequence
		}
		if len(events) > 0 && !flush() {
			return
		}
		// Count- or byte-limited prefix: drain it before consulting terminal
		// state, even when fewer than streamPageSize events fit this page.
		if hasMore {
			continue
		}
		if len(events) > 0 {
			idleCycles = 0
		} else {
			idleCycles++
		}
		// D4 - "connection accepted and silent": a thread whose runs are ALL terminal will
		// never append another event, so tailing it forever leaves the browser with an open
		// connection that says nothing about the canceled/finished turn it just opened. Say
		// the terminal state once and close cleanly; an EventSource client reconnects and is
		// told again, immediately.
		if len(events) > 0 || idleCycles%quiesceCheckEveryCycles == 0 {
			if status, terminal := h.threadTerminalStatus(ctx, token, threadID); terminal {
				env := map[string]any{
					"type":      "end",
					"cursor":    cursor,
					"thread_id": threadID,
					"payload": map[string]any{
						"status": status, "terminal": true,
						// Not a durable event: a projection of the run store, emitted so the
						// client learns the thread's terminal state before the close. It
						// carries no id: line below, so no fake resume cursor exists.
						"durable": false,
					},
				}
				if b, merr := json.Marshal(env); merr == nil {
					if writeFrame("event: end\ndata: %s\n\n", b) {
						flush()
					}
				}
				return
			}
		}
		// Drained: wait for the next cursor tick or client disconnect.
		select {
		case <-ctx.Done():
			return
		case <-time.After(streamPollInterval):
		}
	}
}
