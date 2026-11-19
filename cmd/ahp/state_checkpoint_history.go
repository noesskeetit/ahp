package main

import (
	"log/slog"
	"strconv"

	"github.com/ahp-platform/ahp/internal/state"
)

// stateCheckpointHistoryOptions configures the checkpoint history bound of the
// State store (matrix row 15). The product default is a real bound; the
// environment exists so an operator can widen the readable window deliberately,
// or opt out of retention entirely for a scope that has another retention story.
//
// AHP_STATE_CHECKPOINT_HISTORY_LIMIT
//   - unset: DefaultCheckpointHistoryLimit checkpoints per (thread, namespace);
//   - a positive integer: that many checkpoints per (thread, namespace);
//   - "unbounded" or a negative integer: keep history until the cascade removes
//     it - the pre-existing behaviour, now an explicit choice.
//
// A value that cannot be parsed keeps the bounded default and is logged. A
// retention knob that silently turned a typo into "keep everything forever"
// would be the worst possible failure mode of a retention knob, and a refused
// startup is a worse outcome for a platform that is already serving traffic.
//
// Nothing here deletes data by itself: retention runs when an operator calls
// /v1/state-admin/PruneCheckpointHistory. That is the same posture as the GC
// scheduler, which stays off until it is switched on deliberately.
func stateCheckpointHistoryOptions() []state.StoreOption {
	raw := envOr("AHP_STATE_CHECKPOINT_HISTORY_LIMIT", "")
	if raw == "" {
		return nil
	}
	if raw == "unbounded" {
		return []state.StoreOption{state.WithCheckpointHistory(state.UnboundedCheckpointHistory)}
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		slog.Warn("AHP_STATE_CHECKPOINT_HISTORY_LIMIT is not a number; keeping the bounded default",
			"value_length", len(raw))
		return nil
	}
	if limit < 0 {
		return []state.StoreOption{state.WithCheckpointHistory(state.UnboundedCheckpointHistory)}
	}
	if limit == 0 {
		slog.Warn("AHP_STATE_CHECKPOINT_HISTORY_LIMIT=0 is not a bound; keeping the bounded default")
		return nil
	}
	return []state.StoreOption{state.WithCheckpointHistory(limit)}
}

// stateStoreOptions is every option the composed State store is built with.
func stateStoreOptions() []state.StoreOption {
	return append(stateCompletionOptions(), stateCheckpointHistoryOptions()...)
}
