//go:build state_integration

package space_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Observe the real State HTTP boundary while preserving its real TLS/auth and
// PostgreSQL writes. Counting rows alone would miss duplicate deduplicated TXs.
type deltaStateRoundTrips struct {
	base   http.RoundTripper
	deltas atomic.Int32
}

func (r *deltaStateRoundTrips) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasSuffix(req.URL.Path, "/AppendStream") && req.Body != nil {
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		_ = req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(raw))
		var body struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return nil, err
		}
		if body.Kind == string(space.StepModelDelta) {
			r.deltas.Add(1)
		}
	}
	return r.base.RoundTrip(req)
}

type singleAppendPGModel struct{}

func (singleAppendPGModel) Generate(context.Context, *core.ModelRequest) (*core.ModelResponse, error) {
	return nil, errors.New("streaming capability required")
}
func (singleAppendPGModel) GenerateStream(_ context.Context, _ *core.ModelRequest, emit func(core.ModelDelta) error) (*core.ModelResponse, error) {
	for i, text := range []string{"a", "b"} {
		if err := emit(core.ModelDelta{GenerationID: "generation", Sequence: int64(i + 1), Text: text}); err != nil {
			return nil, err
		}
	}
	return &core.ModelResponse{GenerationID: "generation", Content: json.RawMessage(`"ab"`), Stop: "stop"}, nil
}

func TestStreamingDeltaSingleStateAppendAndStandalone_RealState(t *testing.T) {
	for _, coordinated := range []bool{true, false} {
		name := "standalone"
		if coordinated {
			name = "coordinated"
		}
		t.Run(name, func(t *testing.T) {
			e := cNewEnv(t)
			ctx, cancel := context.WithCancel(e.ctx)
			defer cancel()
			client := e.srv.Client()
			counter := &deltaStateRoundTrips{base: client.Transport}
			client.Transport = counter
			human := e.signTask(t, "single-append-"+contracts.NewID())
			th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "single-append-thread")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = e.client.CreateRun(ctx, human, th.ID, "single-append-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
				t.Fatal(err)
			}
			rs := emil.NewStateRunStore(e.client)
			l, err := rs.ClaimLease(ctx, "", "worker", 120*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			req := &space.CoreRequest{RunID: l.RunID, Worker: l.Worker, Epoch: l.Epoch, TaskToken: l.TaskToken, ThreadID: l.ThreadID, Input: json.RawMessage(`"go"`), MaxSteps: 8}
			if coordinated {
				// Existing coordinated-mode signal; this test's consumer supplies
				// the real State append/ACK rather than simulating a durable store.
				req.JournalModel = func(context.Context, int, int, json.RawMessage) error { return nil }
			}
			for delivery := 1; delivery <= 2; delivery++ {
				// A cold delivery has no in-process fingerprint cache. Both
				// deliveries must reach State, which owns durable deduplication.
				sink := emil.NewRunStoreDurabilitySink(rs)
				runtime := core.NewRuntime(core.RuntimeConfig{Model: singleAppendPGModel{}, Durable: sink})
				out := make(chan space.StepResult, 1)
				done := make(chan error, 1)
				go func() {
					done <- runtime.Run(ctx, req, out)
					close(out)
				}()
				seq := 0
				for sr := range out {
					if !coordinated {
						continue
					}
					key := space.StepKey(l.RunID, seq)
					if sr.StepType == space.StepModelDelta {
						key = space.ModelDeltaKey(l.RunID, sr.GenerationID, sr.DeltaSequence)
					} else {
						seq++
					}
					payload, err := json.Marshal(sr)
					if err != nil {
						t.Fatal(err)
					}
					_, err = e.client.AppendStream(ctx, l.TaskToken, l.ThreadID, l.RunID, l.Worker, l.Epoch, key, string(sr.StepType), payload)
					if sr.Persisted != nil {
						sr.Persisted <- err
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if got := counter.deltas.Load(); got != int32(2*delivery) {
					t.Fatalf("two provider deltas per delivery must make two real State HTTP appends: delivery=%d requests=%d", delivery, got)
				}
			}
			events, err := rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 32)
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for _, ev := range events {
				if ev.Kind == string(space.StepModelDelta) {
					if seen[ev.Key] {
						t.Fatal("identical redelivery added a duplicate canonical delta row")
					}
					seen[ev.Key] = true
				}
			}
			if len(seen) != 2 || !seen[l.RunID+":delta:generation:1"] || !seen[l.RunID+":delta:generation:2"] {
				t.Fatalf("canonical real-State delta rows lost: count=%d", len(seen))
			}
		})
	}
}

func TestStreamingDeltaStateFenceACKStopsModel_RealState(t *testing.T) {
	e := cNewEnv(t)
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	human := e.signTask(t, "fenced-append-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "fenced-append-thread")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.client.CreateRun(ctx, human, th.ID, "fenced-append-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatal(err)
	}
	rs := emil.NewStateRunStore(e.client)
	l, err := rs.ClaimLease(ctx, "", "worker", 120*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	runtime := core.NewRuntime(core.RuntimeConfig{Model: singleAppendPGModel{}, Durable: emil.NewRunStoreDurabilitySink(rs)})
	req := &space.CoreRequest{RunID: l.RunID, Worker: l.Worker, Epoch: l.Epoch, TaskToken: l.TaskToken, ThreadID: l.ThreadID, Input: json.RawMessage(`"go"`), MaxSteps: 8, JournalModel: func(context.Context, int, int, json.RawMessage) error { return nil }}
	out := make(chan space.StepResult, 1)
	done := make(chan error, 1)
	go func() {
		done <- runtime.Run(ctx, req, out)
		close(out)
	}()
	deltas := 0
	var fenceErr error
	for sr := range out {
		if sr.StepType == space.StepModelDelta {
			deltas++
			payload, err := json.Marshal(sr)
			if err != nil {
				t.Fatal(err)
			}
			_, fenceErr = e.client.AppendStream(ctx, l.TaskToken, l.ThreadID, l.RunID, l.Worker, l.Epoch+1, space.ModelDeltaKey(l.RunID, sr.GenerationID, sr.DeltaSequence), string(sr.StepType), payload)
			if contracts.Code(fenceErr) != contracts.ErrStaleEpoch {
				t.Fatalf("actual State refused wrong fence with unexpected code %q", contracts.Code(fenceErr))
			}
		}
		if sr.Persisted != nil {
			sr.Persisted <- fenceErr
		}
	}
	if err := <-done; !errors.Is(err, fenceErr) || contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("actual State fence ACK not propagated: code=%q", contracts.Code(err))
	}
	if deltas != 1 {
		t.Fatalf("producer advanced after authoritative State fence refusal: deltas=%d", deltas)
	}
	events, err := rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 32)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Kind == string(space.StepModelDelta) {
			t.Fatal("fence-rejected delta was written by a bypassing direct sink")
		}
	}
}
