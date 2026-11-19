package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// DEF-PROF-1 follow-up: the stream phase collected no usage and no model name,
// yet reported usage_tokens_totals {0,0,0} and model "" -- both read as real
// measurements ("the model consumed zero tokens"). Un-collected is null.
func TestStreamPhaseHasNoFakeZeroUsage(t *testing.T) {
	stats := llmPhaseStats{Phase: "stream_c1", Requests: 1, UsageTotals: nil, ModelName: nil}
	raw, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	if !strings.Contains(body, `"usage_tokens_totals":null`) {
		t.Fatalf("stream usage must be null, got %s", body)
	}
	if !strings.Contains(body, `"model":null`) {
		t.Fatalf("stream model must be null, got %s", body)
	}
	if strings.Contains(body, `"usage_tokens_totals":{`) {
		t.Fatalf("zeroed usage map masquerades as a real measurement: %s", body)
	}
}
