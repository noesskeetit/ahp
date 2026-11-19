package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestClassifyFailureSanitizes pins the human-readable failure contract: a
// class a UI can act on, and a message that cannot leak endpoints, paths or
// credentials (the raw cause stays server-side).
func TestClassifyFailureSanitizes(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantClass string
		mustHide  []string
	}{
		{
			name:      "unreachable tool server",
			err:       fmt.Errorf(`tools: connect: Post "http://svc:Sup3rS3cret@tool.internal:9000/mcp": dial tcp 127.0.0.1:9000: connect: connection refused`),
			wantClass: FailureUnreachable,
			mustHide:  []string{"Sup3rS3cret", "tool.internal", "http://"},
		},
		{
			name:      "credential unavailable",
			err:       fmt.Errorf("tools: resolve credential: vault read failed at /v1/secret/data/ahp/tool/bearer token=ABCDEF123"),
			wantClass: FailureCredentials,
			mustHide:  []string{"ABCDEF123", "/v1/secret/data"},
		},
		{
			name:      "timeout",
			err:       fmt.Errorf("tools: call: %w", context.DeadlineExceeded),
			wantClass: FailureTimeout,
			mustHide:  nil,
		},
		{
			name:      "ledger",
			err:       errors.New("tools: complete: ERROR: relation does not exist (SQLSTATE 42P01)"),
			wantClass: FailureLedger,
			mustHide:  nil,
		},
		{
			name:      "misconfigured",
			err:       errors.New("tools: connection vanished"),
			wantClass: FailureMisconfigured,
			mustHide:  nil,
		},
		{
			name:      "unclassified still explains",
			err:       errors.New("the remote side hung up mid-response"),
			wantClass: FailureUnknown,
			mustHide:  nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := ClassifyFailure(context.Background(), tc.err)
			if f.Class != tc.wantClass {
				t.Fatalf("class=%q, want %q (message %q)", f.Class, tc.wantClass, f.Message)
			}
			if strings.TrimSpace(f.Message) == "" {
				t.Fatal("a human-facing failure must always carry a message")
			}
			for _, secret := range tc.mustHide {
				if strings.Contains(f.Message, secret) {
					t.Fatalf("message leaked %q: %s", secret, f.Message)
				}
			}
			if len([]rune(f.Message)) > maxFailureMessage {
				t.Fatalf("message not bounded: %d runes", len([]rune(f.Message)))
			}
		})
	}
}

// TestFailureRoundTripThroughPayload checks the journalled summary is readable
// back, and that foreign payloads (a tool's own JSON) are not mistaken for one.
func TestFailureRoundTripThroughPayload(t *testing.T) {
	payload, err := json.Marshal(map[string]any{FailureKey: ClassifyFailure(context.Background(), errors.New("tools: connect: connection refused"))})
	if err != nil {
		t.Fatal(err)
	}
	f, ok := FailureFromPayload(payload)
	if !ok || f.Class != FailureUnreachable {
		t.Fatalf("summary not readable back: ok=%v f=%+v", ok, f)
	}
	if _, ok := FailureFromPayload(json.RawMessage(`{"content":[{"text":"hi"}]}`)); ok {
		t.Fatal("a tool result payload must not be read as a failure summary")
	}
	if _, ok := FailureFromPayload(json.RawMessage(`{"failure":{"class":"","message":""}}`)); ok {
		t.Fatal("an empty summary must not be surfaced")
	}
	if _, ok := FailureFromPayload(nil); ok {
		t.Fatal("no payload, no failure")
	}
}
