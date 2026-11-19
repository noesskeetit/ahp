package sdk

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestToolCannotOverrideRuntimeProjectOrEvidence(t *testing.T) {
	c := New("https://memory.internal", func(context.Context) (string, error) { return "actual-task-token", nil })
	c.HTTP.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1/memory/remember" || r.Header.Get("Authorization") != "Bearer actual-task-token" || r.Header.Get("Idempotency-Key") != "durable-key" {
			t.Fatal(r.URL, r.Header)
		}
		var body struct {
			Project string   `json:"project_id"`
			Sources []string `json:"source_refs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Project != "bound-project" || len(body.Sources) != 1 || body.Sources[0] != "state://runs/owned/input" {
			t.Fatal(body)
		}
		return &http.Response{StatusCode: 202, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"operation","state":"queued"}`))}, nil
	})
	_, err := c.Invoke(context.Background(), "memory_remember", json.RawMessage(`{"text":"decision","target":"project","project_id":"another-project","bank_id":"foreign","source_refs":["forged"]}`), "durable-key", "bound-project", []string{"state://runs/owned/input"})
	if err != nil {
		t.Fatal(err)
	}
}
