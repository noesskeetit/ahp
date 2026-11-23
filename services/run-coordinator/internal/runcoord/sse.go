package runcoord

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func (c *Coordinator) stream(w http.ResponseWriter, r *http.Request, session string) {
	if agent := r.PathValue("agent"); agent != "" && agent != c.Scope.Deployment {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "agent is outside coordinator scope"))
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "stream flushing is unavailable"))
		return
	}
	ctx := r.Context()
	after := r.Header.Get("Last-Event-ID")
	if after == "" {
		after = r.URL.Query().Get("after_seq")
	}
	start, err := streamCursor(after)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	turn, err := streamCursor(r.URL.Query().Get("turn_seq"))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	cursor := start
	if turn != 0 {
		// Inspect the last delivered event again so reconnecting immediately
		// after the terminal event still returns the closing frame.
		cursor = max(turn-1, start-1)
	}
	var signal <-chan struct{}
	if c.Signals != nil {
		var release func()
		signal, release = c.Signals.Subscribe(SessionSignal{Scope: c.Scope, SessionID: session})
		defer release()
	}
	if _, err := c.State.GetEvents(ctx, session, cursor, 1, ""); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	write := func(event string, id int64, data any) bool {
		encoded, err := json.Marshal(data)
		if err != nil {
			return false
		}
		if id > 0 {
			if _, err = fmt.Fprintf(w, "id: %d\n", id); err != nil {
				return false
			}
		}
		if _, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, encoded); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	for {
		page, err := c.State.GetEvents(ctx, session, cursor, 128, "")
		if err != nil {
			write("error", 0, map[string]string{"code": "STATE_READ_FAILED"})
			return
		}
		if !continuous(page, cursor) {
			page, err = c.State.GetEvents(ctx, session, cursor, 128, "")
			if err != nil || !continuous(page, cursor) {
				write("error", 0, map[string]any{"code": "INCOMPLETE_TAIL", "expected_seq": cursor + 1})
				return
			}
		}
		for _, event := range page.Events {
			cursor = event.Seq
			if turn != 0 && event.TurnSeq != turn {
				continue
			}
			if cursor > start && !write("journal", cursor, event) {
				return
			}
			if turn != 0 && event.Type == "turn.completed" {
				var completed journal.TurnCompleted
				if err := json.Unmarshal(event.Payload.Inline, &completed); err != nil || completed.TurnSeq != turn || completed.EndSeq != cursor {
					write("error", 0, map[string]string{"code": "INVALID_TERMINAL_EVENT"})
					return
				}
				write("end", 0, completed)
				return
			}
		}
		if cursor < page.HeadSeq {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
		case <-signal:
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func streamCursor(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	seq, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seq < 0 {
		return 0, contracts.NewError(contracts.ErrInvalid, "stream cursor must be a nonnegative sequence")
	}
	return seq, nil
}

func continuous(page journal.EventPage, after int64) bool {
	if after < page.HeadSeq && len(page.Events) == 0 {
		return false
	}
	for _, event := range page.Events {
		if event.Seq != after+1 {
			return false
		}
		after = event.Seq
	}
	return true
}
