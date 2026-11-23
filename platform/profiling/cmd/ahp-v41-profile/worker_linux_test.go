package main

import (
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestWorkerKeepsFaultArgumentsStableAcrossExecutionGenerations(t *testing.T) {
	input := turnInput{ID: "fault-42", Tools: 2, MCPMillis: 20, Large: true, Fault: true}
	for index, want := range []string{
		`{"effect_id":"fault-42/0","latency_ms":20,"size_bytes":8192,"hold_reply":true}`,
		`{"effect_id":"fault-42/1","latency_ms":20,"size_bytes":327680,"hold_reply":false}`,
	} {
		call, err := workerToolCall(contracts.Scope{}, input, index)
		if err != nil {
			t.Fatal(err)
		}
		if string(call.Arguments) != want {
			t.Fatalf("tool %d arguments = %s, want %s", index, call.Arguments, want)
		}
	}
}
