package a2a_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	a2asdk "github.com/ahp-platform/ahp/sdk/go/a2a"
	"github.com/ahp-platform/ahp/sdk/go/compute"
	"github.com/ahp-platform/ahp/sdk/go/model"
)

func TestStreamBoundaryAndMutationUncertainty(t *testing.T) {
	ctx := context.Background()
	token := func(context.Context) (string, error) { return "fixture", nil }
	for _, state := range []protocol.TaskState{protocol.TaskStateInputRequired, protocol.TaskStateCompleted, protocol.TaskStateWorking} {
		t.Run(string(state), func(t *testing.T) {
			s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req map[string]any
				json.NewDecoder(r.Body).Decode(&req)
				w.Header().Set("Content-Type", "text/event-stream")
				raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req["id"], "result": map[string]any{"task": map[string]any{"id": "task", "status": map[string]any{"state": state}}}})
				fmt.Fprintf(w, "data: %s\n\n", raw)
			}))
			defer s.Close()
			c, _ := a2asdk.New(a2asdk.Config{BaseURL: s.URL, HTTPClient: s.Client(), TokenSource: token})
			for _, stream := range []bool{false, true} {
				seq := c.Resubscribe(ctx, "task")
				if stream {
					seq = c.SendStream(ctx, &protocol.SendMessageRequest{Message: &protocol.Message{ID: "stable"}})
				}
				n := 0
				var err error
				for event, e := range seq {
					if event != nil {
						n++
					}
					if e != nil {
						err = e
					}
				}
				if n != 1 {
					t.Fatal(n)
				}
				if state == protocol.TaskStateWorking {
					var ae *model.Error
					if !errors.As(err, &ae) || !ae.OutcomeUnknown {
						t.Fatal(err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	for _, tc := range []struct {
		status      int
		body, ctype string
		unknown     bool
	}{{503, `{"code":"UNAVAILABLE"}`, "application/json", true}, {503, "proxy failed", "text/html", true}, {200, "bad", "text/html", true}, {200, "bad\n", "application/x-ndjson", true}, {403, `{"code":"FORBIDDEN"}`, "application/json", false}} {
		t.Run(fmt.Sprintf("%d-%s", tc.status, tc.ctype), func(t *testing.T) {
			s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.ctype)
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer s.Close()
			c, _ := model.New(model.Config{BaseURL: s.URL, HTTPClient: s.Client(), TokenSource: token})
			var err error
			for _, e := range c.Stream(ctx, model.Request{Key: "stable", Messages: []model.Message{{Role: "user", Content: "x"}}}) {
				err = e
			}
			var ae *model.Error
			if !errors.As(err, &ae) || ae.OutcomeUnknown != tc.unknown {
				t.Fatalf("%v %#v", err, ae)
			}
			cc, _ := compute.New(compute.Config{BaseURL: s.URL, HTTPClient: s.Client(), TokenSource: token})
			_, _, err = cc.Exec(ctx, "computer", "key", "echo", "/")
			if !errors.As(err, &ae) || ae.OutcomeUnknown != tc.unknown {
				t.Fatalf("compute %v %#v", err, ae)
			}
		})
	}
}
