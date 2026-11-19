package contracts

import (
	"os"
	"strings"
)

// CheckpointBoundary is an experimental, opt-in persistence cadence for the
// runtime stream. It is a measurement knob, NOT a durability guarantee: the
// volatile tail it holds in process memory is lost on a crash. Unset or an
// unrecognised value selects Chunk, which is the historical per-fragment
// behaviour and must remain the default.
//
// Chunk - every provider delta / stream event is a durable write (baseline).
// Step  - deltas are held in a bounded in-memory tail and flushed once per
//
//	LLM/tool (step) boundary; a checkpoint is written after each step.
//
// Turn  - the runtime stream tail and checkpoint are held and flushed only at
//
//	the terminal turn boundary (or a genuine suspend).
type CheckpointBoundary string

const (
	CheckpointBoundaryChunk CheckpointBoundary = "chunk"
	CheckpointBoundaryStep  CheckpointBoundary = "step"
	CheckpointBoundaryTurn  CheckpointBoundary = "turn"
)

// EnvCheckpointBoundary names the environment variable both process roles
// (model service and runtime/runner) inherit to select the experimental
// persistence cadence.
const EnvCheckpointBoundary = "AHP_EXPERIMENT_CHECKPOINT_BOUNDARY"

// CheckpointBoundaryFor reads the experimental boundary from rawEnv (an empty
// value means unset). Values are trimmed and lower-cased; anything that is not
// step or turn resolves to the historical Chunk default, so an unset or
// mistyped environment can never silently change the default write cadence.
func CheckpointBoundaryFor(raw string) CheckpointBoundary {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(CheckpointBoundaryStep):
		return CheckpointBoundaryStep
	case string(CheckpointBoundaryTurn):
		return CheckpointBoundaryTurn
	default:
		return CheckpointBoundaryChunk
	}
}

// CurrentCheckpointBoundary reads the process environment. It is a pure read of
// one variable; no I/O, no configuration parsing beyond it. (Named apart from the
// CheckpointBoundary TYPE to avoid a same-file identifier collision.)
func CurrentCheckpointBoundary() CheckpointBoundary {
	return CheckpointBoundaryFor(os.Getenv(EnvCheckpointBoundary))
}

// StreamRetention is a second, orthogonal experimental dimension to
// CheckpointBoundary. Where the boundary knob changes the WRITE CADENCE (how
// often buffered fragments are committed) while still persisting every original
// fragment, the retention knob changes the PERSISTENCE GRANULARITY: in
// results-only mode the individual provider/stream text fragments are never
// written to PostgreSQL at all -- neither per-fragment (chunk) nor batched at a
// boundary (step/turn). Only completed step results / turn snapshots and one
// aggregate generation result are durable. Live incremental delivery still reads
// the bounded process-local volatile tail; a crash or a reconnect to another
// process cannot recover intermediate text that was never persisted.
//
// It is paired with the boundary knob: results-only is meaningful only when the
// boundary is step or turn (the volatile tail that carries the live fragments).
// In the default chunk boundary there is no volatile tail, so results-only has
// no effect and the historical per-fragment durable write remains.
//
// Journal - the historical behaviour: every fragment is durable (chunk: one
//
//	write per fragment; step/turn: batched one transaction per boundary).
//
// ResultsOnly - text fragments are never persisted; only results/snapshots and
//
//	one aggregate generation row are durable.
type StreamRetention string

const (
	StreamRetentionJournal     StreamRetention = "journal"
	StreamRetentionResultsOnly StreamRetention = "results-only"
)

// EnvStreamRetention names the environment variable both process roles inherit
// to select the experimental persistence granularity.
const EnvStreamRetention = "AHP_EXPERIMENT_STREAM_RETENTION"

// StreamRetentionFor reads the experimental retention from rawEnv (empty means
// unset). Values are trimmed and lower-cased; anything that is not the explicit
// "results-only" resolves to the historical Journal default, so an unset or
// mistyped environment can never silently drop fragment persistence.
func StreamRetentionFor(raw string) StreamRetention {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(StreamRetentionResultsOnly):
		return StreamRetentionResultsOnly
	default:
		return StreamRetentionJournal
	}
}

// CurrentStreamRetention reads the process environment. Pure read of one
// variable; no I/O.
func CurrentStreamRetention() StreamRetention {
	return StreamRetentionFor(os.Getenv(EnvStreamRetention))
}

// ResultsOnlyRetention reports whether the process selected results-only
// persistence AND the boundary actually carries a volatile tail (step or turn).
// In the default chunk boundary there is no tail to withhold, so this returns
// false and the historical durable per-fragment write is preserved -- the new
// selector alone can never change default behaviour.
func ResultsOnlyRetention() bool {
	return CurrentStreamRetention() == StreamRetentionResultsOnly &&
		CurrentCheckpointBoundary() != CheckpointBoundaryChunk
}
