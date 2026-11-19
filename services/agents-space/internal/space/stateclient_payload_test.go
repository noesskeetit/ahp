package space

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCoordinatorClaimSelectsTurnPayload(t *testing.T) {
	seen := make(chan map[string]any, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		seen <- body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"lease":null}`))
	}))
	defer srv.Close()
	client := NewStateClient(srv.URL, srv.Client())
	if _, err := client.ClaimRun(context.Background(), "", "dot-worker", time.Minute); err != nil {
		t.Fatal(err)
	}
	if body := <-seen; body["payload_type"] != "turn" {
		t.Fatalf("coordinator can consume generic SDK runs: %v", body)
	}
}

func TestReadStreamResponseBoundIsExplicit(t *testing.T) {
	for _, count := range []int{24, 40} {
		t.Run(strconv.Itoa(count)+"_frames", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"events":[`))
				encoder := json.NewEncoder(w)
				for i := 0; i < count; i++ {
					if i > 0 {
						_, _ = w.Write([]byte(","))
					}
					_ = encoder.Encode(map[string]any{"id": "e", "key": "k", "kind": "model_delta", "sequence": i + 1, "payload": map[string]string{"content": strings.Repeat("x", 512<<10)}})
				}
				_, _ = w.Write([]byte("]}"))
			}))
			defer srv.Close()
			client := NewStateClient(srv.URL, srv.Client())
			rows, err := client.ReadStream(context.Background(), "token", "thread", 0, count)
			if count == 24 {
				if err != nil || len(rows) != count {
					t.Fatalf("valid bounded response was truncated: %v count=%d", err, len(rows))
				}
			} else if err == nil || !strings.Contains(err.Error(), "response exceeds") || rows != nil {
				t.Fatalf("oversize must be an explicit refusal, not a partial page: %v rows=%d", err, len(rows))
			}
		})
	}
}

func TestReadStreamPageContractAndCursorValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"byte_prefix", `{"events":[{"id":"e","thread_id":"thread","key":"k","kind":"model_delta","sequence":8,"payload":{"text":"result0000 "}}],"has_more":true,"next_after":8}`, true},
		{"empty_drained", `{"events":[],"has_more":false,"next_after":7}`, true},
		{"legacy_response", `{"events":[]}`, false},
		{"empty_more", `{"events":[],"has_more":true,"next_after":7}`, false},
		{"cursor_gap", `{"events":[],"has_more":false,"next_after":8}`, false},
		{"repeated_cursor", `{"events":[{"thread_id":"thread","sequence":7}],"has_more":false,"next_after":7}`, false},
		{"foreign_thread", `{"events":[{"thread_id":"other","sequence":8}],"has_more":false,"next_after":8}`, false},
		{"duplicate_sequence", `{"events":[{"thread_id":"thread","sequence":8},{"thread_id":"thread","sequence":8}],"has_more":false,"next_after":8}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var in map[string]any
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Error(err)
				}
				if in["max_response_bytes"] != float64(16<<20) || in["limit"] != float64(256) || in["after"] != float64(7) || r.Header.Get("Authorization") != "Bearer token" {
					t.Error("bounded request or scope token changed")
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			client := NewStateClient(srv.URL, srv.Client())
			rows, more, err := client.ReadStreamPage(context.Background(), "token", "thread", 7, 256, 16<<20)
			if (err == nil) != tc.valid {
				t.Fatalf("accepted=%t want=%t err=%v", err == nil, tc.valid, err)
			}
			if tc.name == "byte_prefix" && (!more || len(rows) != 1 || rows[0].Sequence != 8) {
				t.Fatal("byte-short page lost continuation")
			}
			if !tc.valid && rows != nil {
				t.Fatal("invalid page leaked partial events")
			}
		})
	}
}
