package main

import "github.com/ahp-platform/ahp/internal/state"

func stateCompletionOptions() []state.StoreOption {
	if envOr("AHP_ENABLE_MEMORY", "") == "1" && envOr("AHP_MEMORY_BACKEND", "hindsight") == "hindsight" {
		return []state.StoreOption{state.WithRunCompletionOutbox()}
	}
	return nil
}
