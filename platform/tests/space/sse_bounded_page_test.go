package space_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

// Only the scoped State peer is substituted. Space's HTTP handler, cursor
// selection, per-event SSE projection and durable terminal check are real.
func TestSSEBoundedPagesDrainBeforeTerminal(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		count, pageCap, maxReads int
	}{
		{"tiny_4000", 4000, 256, 16},
		{"byte_short", 3, 1, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMockState()
			m.threads["page-thread"] = &threadRecord{ID: "page-thread", Owner: "Bearer tok-alice"}
			m.runs["page-run"] = &runRecord{ID: "page-run", ThreadID: "page-thread", Status: "done", Sequence: 1}
			peer := m.Handler()
			var reads atomic.Int64
			stateServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/state/ReadStream" {
					peer.ServeHTTP(w, r)
					return
				}
				reads.Add(1)
				var in struct {
					After    int64 `json:"after"`
					Limit    int   `json:"limit"`
					MaxBytes int   `json:"max_response_bytes"`
				}
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Error(err)
					return
				}
				limit := in.Limit
				if limit > tc.pageCap {
					limit = tc.pageCap
				}
				end := min(int(in.After)+limit, tc.count)
				rows := make([]streamItem, 0, limit)
				for n := int(in.After) + 1; n <= end; n++ {
					rows = append(rows, streamItem{ID: fmt.Sprint(n), ThreadID: "page-thread", Key: fmt.Sprint(n), Kind: "model_delta", Sequence: int64(n), Payload: json.RawMessage(`{"text":"result0000 "}`)})
				}
				writeJSON(w, 200, map[string]any{"events": rows, "has_more": end < tc.count, "next_after": end})
			}))
			defer stateServer.Close()
			coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: space.NewStateClient(stateServer.URL, stateServer.Client())})
			h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: authPerSubject})
			srv := httptest.NewServer(h)
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/v1/space/stream/page-thread", nil)
			req.Header.Set("X-Test-Subject", "alice")
			req.Header.Set("Authorization", "Bearer tok-alice")
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Count(string(body), "event: model_delta\n"); got != tc.count {
				t.Fatalf("durable terminal overtook byte-short page: got %d events want %d", got, tc.count)
			}
			if reads.Load() > int64(tc.maxReads) {
				t.Fatalf("tiny-page read amplification: %d ReadStream RPCs, want <=%d", reads.Load(), tc.maxReads)
			}
			if !strings.Contains(string(body), "event: end\n") {
				t.Fatal("missing durable terminal")
			}
			for n := 1; n <= tc.count; n++ {
				if strings.Count(string(body), fmt.Sprintf("id: %d\nevent:", n)) != 1 {
					t.Fatalf("cursor %d lost or duplicated", n)
				}
			}
		})
	}
}
