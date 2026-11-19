package sharedmemory_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/sdk/go/sharedmemory"
)

func TestSharedProfileWireAndCurrentToken(t *testing.T) {
	var calls int
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/v1/memory/shared/Propose" || r.Header.Get("Authorization") != "Bearer current-task" {
			t.Errorf("wrong profile or credential: method=%s path=%s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["op_key"] != "stable-key" || body["workspace"] != "workspace-id" || body["kind"] != "workspace" {
			t.Errorf("wrong mutation fields: %v", body)
		}
		for _, key := range []string{"tenant", "subject", "deployment", "scope"} {
			if _, ok := body[key]; ok {
				t.Error("caller supplied identity")
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"memory": map[string]any{"id": "m", "state": "proposed", "kind": "workspace", "text": "project fact"}, "replay": calls == 2})
	}))
	defer s.Close()
	tokens := 0
	c, err := sharedmemory.New(sharedmemory.Config{BaseURL: s.URL, HTTPClient: s.Client(), TokenSource: func(context.Context) (string, error) { tokens++; return "current-task", nil }})
	if err != nil {
		t.Fatal(err)
	}
	input := sharedmemory.ProposeRequest{Kind: sharedmemory.Workspace, Workspace: "workspace-id", Text: "project fact", SourceKind: "user_statement", Evidence: []sharedmemory.Evidence{{SourceKind: "user_statement", Detail: "explicit synthetic statement"}}}
	first, err := c.Propose(t.Context(), "stable-key", input)
	if err != nil || first.Memory.ID != "m" || first.Memory.State != "proposed" || first.Replay {
		t.Fatalf("proposal result: %+v err=%v", first, err)
	}
	second, err := c.Propose(t.Context(), "stable-key", input)
	if err != nil || !second.Replay || second.Memory.ID != first.Memory.ID || tokens != 2 {
		t.Fatalf("replay/current token: %+v tokens=%d err=%v", second, tokens, err)
	}
}

type lostReply struct{ calls int }

func (l *lostReply) RoundTrip(*http.Request) (*http.Response, error) {
	l.calls++
	return nil, io.ErrUnexpectedEOF
}

func TestMutationsAreUncertainWithoutAutomaticRetry(t *testing.T) {
	wire := &lostReply{}
	c, err := sharedmemory.New(sharedmemory.Config{BaseURL: "https://owner", HTTPClient: &http.Client{Transport: wire}, TokenSource: func(context.Context) (string, error) { return "task", nil }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Propose(t.Context(), "stable", sharedmemory.ProposeRequest{Kind: sharedmemory.User, Text: "fact"})
	var failure *sharedmemory.Error
	if !errors.As(err, &failure) || !failure.OutcomeUnknown || wire.calls != 1 {
		t.Fatalf("mutation lost ACK must be uncertain, one attempt: calls=%d err=%v", wire.calls, err)
	}
	_, err = c.Search(t.Context(), sharedmemory.SearchRequest{Query: "fact"})
	if !errors.As(err, &failure) || failure.OutcomeUnknown || wire.calls != 2 {
		t.Fatalf("read failure became mutation or retried: calls=%d err=%v", wire.calls, err)
	}
	_, err = c.Propose(t.Context(), "", sharedmemory.ProposeRequest{})
	if err == nil || wire.calls != 2 {
		t.Fatal("empty key reached the transport")
	}
}

func TestSharedMemoryNeverFollowsRedirect(t *testing.T) {
	foreignCalls := 0
	foreign := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { foreignCalls++ }))
	defer foreign.Close()
	owner := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, foreign.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	defer owner.Close()
	c, err := sharedmemory.New(sharedmemory.Config{BaseURL: owner.URL, HTTPClient: owner.Client(), TokenSource: func(context.Context) (string, error) { return "task", nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Read(t.Context(), "record"); err == nil || foreignCalls != 0 {
		t.Fatalf("redirect accepted or foreign request sent: %v", err)
	}
}

func TestOperationGetAndFenceAreNotRecordReadOrErasure(t *testing.T) {
	var paths []string
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		var in map[string]string
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
		}
		if len(in) != 1 || in["op_key"] != "cancelled-key" {
			t.Errorf("operation request carried record/subject data: %v", in)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"op_key": "cancelled-key", "status": "FENCED", "result_count": 0})
	}))
	defer s.Close()
	c, err := sharedmemory.New(sharedmemory.Config{BaseURL: s.URL, HTTPClient: s.Client(), TokenSource: func(context.Context) (string, error) { return "task", nil }})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []func(context.Context, string) (sharedmemory.Operation, error){c.GetOperation, c.Fence} {
		op, err := f(t.Context(), "cancelled-key")
		if err != nil || op.Status != "FENCED" || op.OpKey != "cancelled-key" {
			t.Fatalf("operation receipt: %+v err=%v", op, err)
		}
	}
	if len(paths) != 2 || paths[0] != "/v1/memory/shared/Get" || paths[1] != "/v1/memory/shared/Fence" {
		t.Fatalf("wrong operation routes: %v", paths)
	}
	wire := &lostReply{}
	c, err = sharedmemory.New(sharedmemory.Config{BaseURL: "https://owner", HTTPClient: &http.Client{Transport: wire}, TokenSource: func(context.Context) (string, error) { return "task", nil }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Fence(t.Context(), "cancelled-key")
	var failure *sharedmemory.Error
	if !errors.As(err, &failure) || !failure.OutcomeUnknown || wire.calls != 1 {
		t.Fatalf("fence ACK uncertainty: %v", err)
	}
	_, err = c.GetOperation(t.Context(), "cancelled-key")
	if !errors.As(err, &failure) || failure.OutcomeUnknown || wire.calls != 2 {
		t.Fatalf("Get became a mutation or retried: %v", err)
	}
	_, err = c.Fence(t.Context(), "")
	if err == nil || wire.calls != 2 {
		t.Fatal("empty key reached owner")
	}
}
