package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Generates rows lazily: a page must stop decoding after one excluded row,
// rather than first retaining count*MaxPayloadBytes and trimming afterwards.
type streamPageTestRows struct {
	after, end   int64
	payload      json.RawMessage
	nexts, scans int
	closed       bool
	err          error
}

func (r *streamPageTestRows) Next() bool { r.nexts++; return r.after+int64(r.nexts) <= r.end }
func (r *streamPageTestRows) Scan(dst ...any) error {
	r.scans++
	n := r.after + int64(r.nexts)
	*dst[0].(*string) = fmt.Sprint(n)
	*dst[1].(*string) = "thread"
	*dst[2].(*string) = fmt.Sprint(n)
	*dst[3].(*string) = "model_delta"
	*dst[4].(*int64) = n
	*dst[5].(*json.RawMessage) = append(json.RawMessage(nil), r.payload...)
	return nil
}
func (r *streamPageTestRows) Err() error { return r.err }
func (r *streamPageTestRows) Close()     { r.closed = true }

func TestStreamPageEscapedBytesAndReplay(t *testing.T) {
	// Raw valid UTF-8 payload approaches 1MiB; HTML bytes expand sixfold,
	// U+2028 expands twofold, while other non-ASCII remains UTF-8.
	payload := json.RawMessage(`{"text":"` + strings.Repeat("<>&\u2028界", ((1<<20)-20)/9) + `"}`)
	if !json.Valid(payload) || len(payload) > MaxPayloadBytes {
		t.Fatal("invalid large fixture")
	}
	var cursor int64
	var seen []int64
	for cursor < 8 {
		rows := &streamPageTestRows{after: cursor, end: 8, payload: payload}
		page, err := collectStreamPage(rows, cursor, 256, MaxStreamResponseBytes)
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(page)
		if err != nil || len(body)+1 > MaxStreamResponseBytes {
			t.Fatalf("full JSON+newline exceeds budget: %d err=%v", len(body)+1, err)
		}
		if !rows.closed || rows.scans > len(page.Events)+1 {
			t.Fatalf("unbounded row materialization: scans=%d returned=%d closed=%t", rows.scans, len(page.Events), rows.closed)
		}
		if page.HasMore && !page.ByteLimited {
			t.Fatal("large events should stop on bytes, not count256")
		}
		for _, ev := range page.Events {
			seen = append(seen, ev.Sequence)
			if string(ev.Payload) != string(payload) {
				t.Fatal("payload changed")
			}
		}
		if page.NextAfter <= cursor {
			t.Fatal("cursor did not advance")
		}
		cursor = page.NextAfter
	}
	if len(seen) != 8 {
		t.Fatalf("lost replay events: %v", seen)
	}
	for i, n := range seen {
		if n != int64(i+1) {
			t.Fatalf("gap/duplicate at %d: %d", i, n)
		}
	}
}

func TestStreamPageWorstHTMLExpansionStopsAfterLookahead(t *testing.T) {
	payload := json.RawMessage(`{"text":"` + strings.Repeat("<", MaxPayloadBytes-len(`{"text":""}`)) + `"}`)
	rows := &streamPageTestRows{end: 256, payload: payload}
	page, err := collectStreamPage(rows, 0, 256, MaxStreamResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(page)
	if len(page.Events) != 2 || rows.scans != 3 || rows.nexts != 3 || !rows.closed || !page.HasMore || !page.ByteLimited || page.NextAfter != 2 {
		t.Fatalf("6x escaping must retain only2rows plus1lookahead: returned=%d scans=%d nexts=%d cursor=%d", len(page.Events), rows.scans, rows.nexts, page.NextAfter)
	}
	if len(encoded)+1 > MaxStreamResponseBytes || len(encoded) < 12<<20 {
		t.Fatalf("bad full escaped response size=%d", len(encoded)+1)
	}
}

func TestStreamPageTinyEventsAndCountLookahead(t *testing.T) {
	var cursor int64
	pages := 0
	for cursor < 4000 {
		rows := &streamPageTestRows{after: cursor, end: 4000, payload: json.RawMessage(`{"text":"result0000 "}`)}
		page, err := collectStreamPage(rows, cursor, 256, MaxStreamResponseBytes)
		if err != nil {
			t.Fatal(err)
		}
		if !rows.closed || rows.scans > 256 || page.ByteLimited {
			t.Fatal("count page not bounded")
		}
		if page.HasMore != (page.NextAfter < 4000) {
			t.Fatal("lookahead disagrees with cursor")
		}
		if len(page.Events) == 0 || page.Events[0].Sequence != cursor+1 {
			t.Fatal("lost first replay event")
		}
		cursor = page.NextAfter
		pages++
	}
	if pages != 16 {
		t.Fatalf("4000 tiny pieces require %d pages, want16", pages)
	}
}

func TestStreamPageExactEnvelopeBoundaryAndRefusals(t *testing.T) {
	payload := json.RawMessage(`{"text":"<\u2028界"}`)
	all, err := collectStreamPage(&streamPageTestRows{end: 1, payload: payload}, 0, 256, MaxStreamResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(all)
	for _, budget := range []int{len(body) + 1, len(body)} {
		rows := &streamPageTestRows{end: 1, payload: payload}
		page, err := collectStreamPage(rows, 0, 256, budget)
		if budget == len(body)+1 {
			if err != nil || len(page.Events) != 1 {
				t.Fatalf("exact envelope+newline refused: %v", err)
			}
		} else if contracts.Code(err) != contracts.ErrInvalid || page.Events != nil {
			t.Fatalf("single oversize must refuse without partial response: %v", err)
		}
		if !rows.closed {
			t.Fatal("rows not closed")
		}
	}
	for _, budget := range []int{-1, 0, MaxStreamResponseBytes + 1} {
		if _, err := collectStreamPage(&streamPageTestRows{}, 0, 256, budget); contracts.Code(err) != contracts.ErrInvalid {
			t.Fatal("invalid budget accepted")
		}
	}
	want := errors.New("read failure")
	page, err := collectStreamPage(&streamPageTestRows{end: 0, err: want}, 0, 256, MaxStreamResponseBytes)
	if !errors.Is(err, want) || page.Events != nil {
		t.Fatal("row failure became partial success")
	}
}
