package main

import "testing"

// The 64-token cap was a harness default, and it manufactured 261 FAILED
// generations on qwen-38-27b-fp8 (durable provider cause
// provider_empty_completion, finish_reason=length, usage_completion == cap)
// against 6 COMPLETED. A measurement budget must not be able to fabricate
// product failures, so the default sits at the platform's own per-response
// default and any lower operator override must be visible.
func TestHarnessDefaultMaxTokensLeavesTheModelRoomToAnswer(t *testing.T) {
	if *llmMaxTokens < 256 {
		t.Fatalf("harness max_tokens default = %d: below 256 the workload measures its own budget (the 64-token cap produced 261/267 failures)", *llmMaxTokens)
	}
	if *llmMaxTokens > 4096 {
		t.Fatalf("harness max_tokens default = %d: the LLM ladder must stay volume-bounded", *llmMaxTokens)
	}
}
