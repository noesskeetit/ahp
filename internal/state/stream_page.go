package state

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// MaxStreamResponseBytes includes the complete ReadStream JSON envelope and
// transport.WriteJSON's final newline, matching both published client readers.
const MaxStreamResponseBytes = 16 << 20

// StreamPage is an explicitly byte-bounded ordered prefix. NextAfter advances
// only through returned events; an excluded lookahead remains for the next read.
type StreamPage struct {
	Events    []StreamEvent `json:"events"`
	HasMore   bool          `json:"has_more"`
	NextAfter int64         `json:"next_after"`
	// ByteLimited lets the legacy count-only API refuse rather than silently
	// returning a short page that its callers would interpret as end-of-stream.
	ByteLimited bool `json:"-"`
}

type streamPageRows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close()
}

// streamPageEnvelopeBytes accounts for the empty array, keys, separators,
// longest boolean (false) and trailing newline. Per-row bytes are measured by
// encoding/json itself, including HTML and U+2028/U+2029 escaping.
func streamPageEnvelopeBytes(after int64) int {
	return len(`{"events":[],"has_more":false,"next_after":}`) + len(strconv.FormatInt(after, 10)) + 1
}

func collectStreamPage(rows streamPageRows, after int64, limit, maxBytes int) (StreamPage, error) {
	defer rows.Close()
	if maxBytes <= 0 || maxBytes > MaxStreamResponseBytes || streamPageEnvelopeBytes(after) > maxBytes {
		return StreamPage{}, contracts.NewError(contracts.ErrInvalid, "state: invalid stream response byte budget")
	}
	page := StreamPage{Events: make([]StreamEvent, 0), NextAfter: after}
	rowBytes := 0
	for rows.Next() {
		if len(page.Events) == capLimit(limit) {
			page.HasMore = true
			break
		}
		var ev StreamEvent
		if err := rows.Scan(&ev.ID, &ev.ThreadID, &ev.Key, &ev.Kind, &ev.Sequence, &ev.Payload); err != nil {
			return StreamPage{}, err
		}
		encoded, err := json.Marshal(ev)
		if err != nil {
			return StreamPage{}, err
		}
		candidateBytes := rowBytes + len(encoded)
		if len(page.Events) > 0 {
			candidateBytes++
		} // comma between array elements
		if streamPageEnvelopeBytes(ev.Sequence)+candidateBytes > maxBytes {
			if len(page.Events) == 0 {
				return StreamPage{}, contracts.NewReasoned(contracts.ErrInvalid, "STREAM_EVENT_EXCEEDS_RESPONSE_BUDGET", "state: stream event exceeds response byte budget")
			}
			page.HasMore = true
			page.ByteLimited = true
			break
		}
		rowBytes = candidateBytes
		page.Events = append(page.Events, ev)
		page.NextAfter = ev.Sequence
	}
	if err := rows.Err(); err != nil {
		return StreamPage{}, err
	}
	return page, nil
}

// ReadStreamPage preserves ReadStream's scope masking and ordering, but stops
// scanning after its byte/count prefix plus one lookahead. It never first
// materializes limit*MaxPayloadBytes before deciding what fits the RPC reader.
func (s *Store) ReadStreamPage(ctx context.Context, scope contracts.Scope, threadID string, after int64, limit, maxBytes int) (StreamPage, error) {
	return s.readStreamPage(ctx, scope, threadID, after, limit, maxBytes, false)
}

// ReadDurableStreamPage never uses the volatile UI tail. Its cursor is suitable
// for recovering execution and skipping only writes PostgreSQL confirmed.
func (s *Store) ReadDurableStreamPage(ctx context.Context, scope contracts.Scope, threadID string, after int64, limit, maxBytes int) (StreamPage, error) {
	return s.readStreamPage(ctx, scope, threadID, after, limit, maxBytes, true)
}

func (s *Store) readStreamPage(ctx context.Context, scope contracts.Scope, threadID string, after int64, limit, maxBytes int, durableOnly bool) (StreamPage, error) {
	if err := scope.Validate(); err != nil {
		return StreamPage{}, err
	}
	if _, err := s.GetThread(ctx, scope, threadID); err != nil {
		return StreamPage{}, err
	}
	if maxBytes <= 0 || maxBytes > MaxStreamResponseBytes || streamPageEnvelopeBytes(after) > maxBytes {
		return StreamPage{}, contracts.NewError(contracts.ErrInvalid, "state: invalid stream response byte budget")
	}
	rows, err := s.pool.Query(ctx,
		`SELECT e.id, e.thread_id, e.key, e.kind, e.sequence, e.payload
		   FROM ahp_state.stream_events e
		   JOIN ahp_state.threads t ON t.id = e.thread_id
		  WHERE e.thread_id = $1 AND t.tenant = $2 AND t.deployment = $3
		    AND t.subject = $4 AND e.sequence > $5
		  ORDER BY e.sequence ASC LIMIT $6`,
		threadID, scope.Tenant, scope.Deployment, scope.Subject, after, capLimit(limit)+1)
	if err != nil {
		return StreamPage{}, err
	}
	if durableOnly || s.boundary == contracts.CheckpointBoundaryChunk {
		// Historical control path: durable rows only, unchanged.
		return collectStreamPage(rows, after, limit, maxBytes)
	}
	// Non-chunk (step / turn / results-only). Combine the eligible DURABLE rows and
	// the volatile tail candidates against the CALLER'S ORIGINAL after, dedupe by
	// sequence, and globally sort -- THEN enforce the count/byte limits once and set
	// NextAfter from the last INCLUDED event. This is what keeps a durable
	// model_result / status (which can carry a sequence ABOVE withheld fragments) from
	// skipping the unread fragment prefix or exposing DONE before the last deltas.
	// The durable lookahead of limit+1 is sufficient: the first `limit` merged events
	// can never require a durable row beyond the (limit+1)-th durable sequence.
	durable, err := scanStreamRows(rows)
	if err != nil {
		return StreamPage{}, err
	}
	volatile := s.tailCandidateEvents(scope, threadID, after)
	combined := combineStreamEvents(durable, volatile, after)
	return collectStreamPageFromEvents(combined, after, limit, maxBytes)
}

// scanStreamRows materialises the durable lookahead rows (bounded by the query's
// limit+1) so they can be merged with the volatile candidates.
func scanStreamRows(rows streamPageRows) ([]StreamEvent, error) {
	defer rows.Close()
	out := make([]StreamEvent, 0, 8)
	for rows.Next() {
		var ev StreamEvent
		if err := rows.Scan(&ev.ID, &ev.ThreadID, &ev.Key, &ev.Kind, &ev.Sequence, &ev.Payload); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// combineStreamEvents merges durable and volatile rows that lie after `after`,
// de-duplicating by sequence (a row being flushed appears in both stores; durable
// wins identity) and ordering strictly ascending so a lagging reader sees every
// original event exactly once in cursor order, fragments and results interleaved as
// produced.
func combineStreamEvents(durable, volatile []StreamEvent, after int64) []StreamEvent {
	seen := make(map[int64]bool, len(durable)+len(volatile))
	combined := make([]StreamEvent, 0, len(durable)+len(volatile))
	for _, e := range durable {
		if e.Sequence > after && !seen[e.Sequence] {
			seen[e.Sequence] = true
			combined = append(combined, e)
		}
	}
	for _, e := range volatile {
		if e.Sequence > after && !seen[e.Sequence] {
			seen[e.Sequence] = true
			combined = append(combined, e)
		}
	}
	sort.Slice(combined, func(i, j int) bool { return combined[i].Sequence < combined[j].Sequence })
	return combined
}

// collectStreamPageFromEvents applies the count and byte budgets exactly ONCE over
// the globally-sorted merged events and sets NextAfter from the last INCLUDED event,
// matching collectStreamPage's accounting for the durable-only path.
func collectStreamPageFromEvents(events []StreamEvent, after int64, limit, maxBytes int) (StreamPage, error) {
	if maxBytes <= 0 || maxBytes > MaxStreamResponseBytes || streamPageEnvelopeBytes(after) > maxBytes {
		return StreamPage{}, contracts.NewError(contracts.ErrInvalid, "state: invalid stream response byte budget")
	}
	page := StreamPage{Events: make([]StreamEvent, 0), NextAfter: after}
	rowBytes := 0
	for _, ev := range events {
		if len(page.Events) == capLimit(limit) {
			page.HasMore = true
			break
		}
		encoded, err := json.Marshal(ev)
		if err != nil {
			return StreamPage{}, err
		}
		candidateBytes := rowBytes + len(encoded)
		if len(page.Events) > 0 {
			candidateBytes++
		}
		if streamPageEnvelopeBytes(ev.Sequence)+candidateBytes > maxBytes {
			if len(page.Events) == 0 {
				return StreamPage{}, contracts.NewReasoned(contracts.ErrInvalid, "STREAM_EVENT_EXCEEDS_RESPONSE_BUDGET", "state: stream event exceeds response byte budget")
			}
			page.HasMore = true
			page.ByteLimited = true
			break
		}
		rowBytes = candidateBytes
		page.Events = append(page.Events, ev)
		page.NextAfter = ev.Sequence
	}
	return page, nil
}
