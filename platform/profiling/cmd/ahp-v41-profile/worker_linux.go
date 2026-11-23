package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	state "github.com/ahp-platform/ahp/sdk/go/state/journal"
	sdktools "github.com/ahp-platform/ahp/sdk/go/tools"
)

const (
	workerClaimTTL           = 3 * time.Second
	processWorkerMemoryLimit = 16 << 20
	processWorkerMaxProcs    = 1
)

type turnInput struct {
	ID              string `json:"id"`
	Padding         string `json:"padding"`
	Tools           int    `json:"tools"`
	ThinkMillis     int    `json:"think_millis"`
	MCPMillis       int    `json:"mcp_millis"`
	Large           bool   `json:"large"`
	HoldForFollowup bool   `json:"hold_for_followup"`
	Fault           bool   `json:"fault"`
	MeasureExpiry   bool   `json:"measure_expiry,omitempty"`
}

type turnOutput struct {
	ID                   string  `json:"id"`
	ExecutionID          string  `json:"execution_id"`
	LaunchTurnSeq        int64   `json:"launch_turn_seq"`
	Epoch                int64   `json:"epoch"`
	GuestToolNS          []int64 `json:"guest_tool_ns"`
	HistoryInlineBytes   int     `json:"history_inline_bytes"`
	ArtifactResults      int     `json:"artifact_results,omitempty"`
	FollowupWaitNS       int64   `json:"followup_wait_ns,omitempty"`
	WarmHandoffNS        int64   `json:"warm_handoff_ns,omitempty"`
	PreviousFinishRTTNS  int64   `json:"previous_finish_rtt_ns,omitempty"`
	ExpiredProofRejected bool    `json:"expired_proof_rejected,omitempty"`
}

type workerClock struct {
	start, finishAt, finishRTT int64
}

func runWorker(ctx context.Context) (err error) {
	socket := os.Getenv("AHP_INJECTOR_SOCKET")
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "PATH", "HOME", "TMPDIR", "TERM", "AHP_INJECTOR_SOCKET", "AHP_FC_E2E_GUEST":
		default:
			return fmt.Errorf("unexpected guest environment key %s", key)
		}
	}
	var store *state.Client
	var tools *sdktools.Client
	if socket != "" {
		// ProcessDriver deliberately passes a clean environment. Apply the
		// per-worker GC budget before the SDK reads or allocates any payloads.
		runtime.GOMAXPROCS(processWorkerMaxProcs)
		debug.SetMemoryLimit(processWorkerMemoryLimit)
		defer func() {
			if err != nil {
				_ = os.WriteFile("worker.error", []byte(err.Error()), 0600)
			}
		}()
		store, err = state.NewUnixInjectorClient(socket)
		if err != nil {
			return err
		}
		tools, err = sdktools.NewUnixInjectorClient(socket)
	} else {
		interfaces, readErr := os.ReadDir("/sys/class/net")
		if readErr != nil {
			return fmt.Errorf("guest network interfaces: %w", readErr)
		}
		if len(interfaces) != 1 || interfaces[0].Name() != "lo" {
			return errors.New("profiling vsock guest must have only loopback")
		}
		store, err = state.NewVSockInjectorClient(journal.RuntimeInjectorVSockPort)
		if err != nil {
			return err
		}
		tools, err = sdktools.NewVSockInjectorClient(journal.RuntimeInjectorVSockPort)
	}
	if err != nil {
		return err
	}
	return runWorkerTurns(ctx, store, tools)
}

func runWorkerTurns(ctx context.Context, store *state.Client, tools *sdktools.Client) error {
	boot, err := store.RuntimeBootstrap(ctx)
	if err != nil {
		return fmt.Errorf("worker bootstrap: %w", err)
	}
	first, err := store.ClaimVMTurn(ctx, boot.SessionID, journal.VMTurnClaimRequest{
		TurnSeq: boot.LaunchTurnSeq, Generation: boot.Generation, ExecutionID: boot.ExecutionID,
		TTLMilliseconds: workerClaimTTL.Milliseconds(),
	})
	if err != nil {
		return fmt.Errorf("worker claim: %w", err)
	}
	clock := workerClock{start: monotonicNS()}
	if first.ToolLeaseProof == "" || first.Worker != boot.ExecutionID || first.SessionID != boot.SessionID {
		return errors.New("worker claim did not return the assigned execution and proof")
	}
	lease := state.NewVMLease(first)
	ctx, stop := store.RunVMHeartbeatWithControl(ctx, lease, workerClaimTTL, nil)
	defer stop()

	// Read the last 25 historical records on entry, then consume new records
	// incrementally while the same execution owns followups.
	history := workerHistory{cursor: max(0, first.InputSeq-26)}
	for {
		turn := lease.Current()
		var input turnInput
		if err := json.Unmarshal(turn.Input.Inline, &input); err != nil {
			return fmt.Errorf("decode turn %d: %w", turn.InputSeq, err)
		}
		if input.ID == "" || input.Tools < 0 || input.Tools > 2 || input.ThinkMillis < 0 || input.MCPMillis < 0 || input.MCPMillis > 1000 {
			return fmt.Errorf("invalid workload for turn %d", turn.InputSeq)
		}
		receipt, err := runWorkerTurn(ctx, store, tools, boot, first, lease, turn, input, &history, &clock)
		if err != nil {
			if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
				err = fmt.Errorf("VM lease heartbeat ended the worker: %w", errors.Join(cause, err))
			}
			return fmt.Errorf("turn %s (epoch %d): %w", input.ID, turn.Epoch, err)
		}
		if receipt.Control == nil || receipt.Control.NextTurn == nil {
			return nil
		}
		if receipt.Control.ToolLeaseProof == "" || !lease.ApplyFinish(turn.InputSeq, receipt) {
			return errors.New("warm promotion did not preserve the execution with a tool lease proof")
		}
		if next := lease.Current(); next.ToolLeaseProof == first.ToolLeaseProof {
			return errors.New("warm promotion retained the initial tool lease proof")
		}
		clock.start = monotonicNS()
	}
}

func runWorkerTurn(ctx context.Context, store *state.Client, tools *sdktools.Client, boot journal.RuntimeBootstrap,
	first state.ClaimedTurn, lease *state.VMLease, turn state.ClaimedTurn, input turnInput, history *workerHistory, clock *workerClock,
) (journal.AppendReceipt, error) {
	out := turnOutput{ID: input.ID, ExecutionID: boot.ExecutionID, LaunchTurnSeq: boot.LaunchTurnSeq,
		Epoch: turn.Epoch, GuestToolNS: make([]int64, 0, input.Tools)}
	if clock.finishAt != 0 {
		out.WarmHandoffNS = clock.start - clock.finishAt
		out.PreviousFinishRTTNS = clock.finishRTT
	}
	session := turn.SessionID
	bytesRead, err := history.read(ctx, store, session)
	if err != nil {
		return journal.AppendReceipt{}, fmt.Errorf("read session tail: %w", err)
	}
	out.HistoryInlineBytes = bytesRead
	started, err := json.Marshal(struct {
		turnStart
		WarmHandoffNS       int64 `json:"warm_handoff_ns,omitempty"`
		PreviousFinishRTTNS int64 `json:"previous_finish_rtt_ns,omitempty"`
	}{
		turnStart:     turnStart{ID: input.ID, ExecutionID: boot.ExecutionID, Epoch: turn.Epoch, Mono: clock.start},
		WarmHandoffNS: out.WarmHandoffNS, PreviousFinishRTTNS: out.PreviousFinishRTTNS,
	})
	if err != nil {
		return journal.AppendReceipt{}, err
	}
	startedID := fmt.Sprintf("bench.started:%d:%d", turn.InputSeq, turn.Epoch)
	if err := history.append(ctx, store, turn, startedID, "bench.started", "system", started); err != nil {
		return journal.AppendReceipt{}, fmt.Errorf("record worker start: %w", err)
	}
	if input.MeasureExpiry {
		if turn.InputSeq != first.InputSeq {
			return journal.AppendReceipt{}, errors.New("proof expiry measure requires the initial turn")
		}
		if err := workerMeasureExpiry(ctx, tools, first, lease, input.ID); err != nil {
			return journal.AppendReceipt{}, err
		}
		out.ExpiredProofRejected = true
	}
	for i := 0; i < input.Tools; i++ {
		if input.ThinkMillis > 0 {
			if err := workerSleep(ctx, time.Duration(input.ThinkMillis)*time.Millisecond); err != nil {
				return journal.AppendReceipt{}, err
			}
		}
		elapsed, err := workerSettle(ctx, store, tools, lease, boot.Scope, turn, input, i)
		if err != nil {
			return journal.AppendReceipt{}, err
		}
		out.GuestToolNS = append(out.GuestToolNS, elapsed)
		if input.Large && i == 1 {
			out.ArtifactResults++
		}
	}

	// The SDK commits outcome events. Read their tail for the current head
	// and pick up any followup already queued during the tool calls.
	if input.Tools > 0 {
		if _, err := history.read(ctx, store, session); err != nil {
			return journal.AppendReceipt{}, fmt.Errorf("read settled turn tail: %w", err)
		}
	}
	if history.deltaTurn != turn.InputSeq {
		delta, err := json.Marshal(struct {
			ID string `json:"id"`
		}{input.ID})
		if err != nil {
			return journal.AppendReceipt{}, err
		}
		deltaID := "bench.delta:" + strconv.FormatInt(turn.InputSeq, 10)
		if err := history.append(ctx, store, turn, deltaID, "bench.delta", "assistant", delta); err != nil {
			return journal.AppendReceipt{}, fmt.Errorf("stream turn delta: %w", err)
		}
	}
	if input.HoldForFollowup && history.lastFollowup <= turn.InputSeq {
		waitStarted := time.Now()
		if err := history.waitForFollowup(ctx, store, turn); err != nil {
			return journal.AppendReceipt{}, fmt.Errorf("wait for queued followup: %w", err)
		}
		out.FollowupWaitNS = time.Since(waitStarted).Nanoseconds()
	}
	output, err := json.Marshal(out)
	if err != nil {
		return journal.AppendReceipt{}, err
	}
	receipt, err := history.writeAtHead(ctx, store, session, func(expected int64) (journal.AppendReceipt, error) {
		finishStarted := monotonicNS()
		receipt, err := store.FinishVMTurn(ctx, lease.Current(), expected, journal.Payload{Inline: output}, nil, false)
		if err == nil {
			clock.finishAt = monotonicNS()
			clock.finishRTT = clock.finishAt - finishStarted
		}
		return receipt, err
	})
	if err != nil {
		return journal.AppendReceipt{}, fmt.Errorf("finish VM turn: %w", err)
	}
	return receipt, nil
}

// workerHistory retains the read cursor separately from the head used by
// writers. An admission seen during conflict recovery can queue a followup.
type workerHistory struct {
	cursor, head, lastFollowup, deltaTurn int64
}

func (h *workerHistory) read(ctx context.Context, store *state.Client, session string) (int, error) {
	const limit = 128
	var inlineBytes int
	for {
		page, err := store.GetEvents(ctx, session, h.cursor, limit, "")
		if err != nil {
			return 0, err
		}
		h.head = page.HeadSeq
		for _, event := range page.Events {
			h.cursor = event.Seq
			inlineBytes += len(event.Payload.Inline)
			if event.Type == "turn.followup" && event.Active && event.TurnSeq == 0 {
				h.lastFollowup = max(h.lastFollowup, event.Seq)
			}
			if event.Type == "bench.delta" {
				h.deltaTurn = event.TurnSeq
			}
		}
		if h.cursor >= h.head || len(page.Events) < limit {
			return inlineBytes, nil
		}
	}
}

func (h *workerHistory) waitForFollowup(ctx context.Context, store *state.Client, turn state.ClaimedTurn) error {
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for h.lastFollowup <= turn.InputSeq {
		select {
		case <-waitCtx.Done():
			return context.Cause(ctx)
		case <-poll.C:
			if _, err := h.read(waitCtx, store, turn.SessionID); err != nil {
				if waitCtx.Err() != nil {
					return context.Cause(ctx)
				}
				return err
			}
		}
	}
	return nil
}

func (h *workerHistory) append(ctx context.Context, store *state.Client, turn state.ClaimedTurn, id, kind, role string, payload []byte) error {
	_, err := h.writeAtHead(ctx, store, turn.SessionID, func(expected int64) (journal.AppendReceipt, error) {
		return store.AppendEvents(ctx, turn.SessionID, turn.Epoch, journal.AppendRequest{
			TurnSeq: turn.InputSeq, Key: id, ExpectedSeq: expected,
			Events: []journal.EventInput{{ID: id, Type: kind, Role: role, Codec: "json", Payload: journal.Payload{Inline: payload}}},
		})
	})
	return err
}

func (h *workerHistory) writeAtHead(ctx context.Context, store *state.Client, session string, write func(int64) (journal.AppendReceipt, error)) (journal.AppendReceipt, error) {
	const attempts = 8
	for i := range attempts {
		expected := h.head
		receipt, err := write(expected)
		if err == nil {
			h.head = receipt.HeadSeq
			return receipt, nil
		}
		if contracts.Code(err) != contracts.ErrConflict || i+1 == attempts {
			return receipt, err
		}
		if _, readErr := h.read(ctx, store, session); readErr != nil {
			return receipt, readErr
		}
		if h.head <= expected {
			return receipt, err
		}
	}
	panic("unreachable journal append retry")
}

func workerToolCall(scope contracts.Scope, input turnInput, index int) (sdktools.LeasedCall, error) {
	id := input.ID + "/" + strconv.Itoa(index)
	size := 8 << 10
	if input.Large && index == 1 {
		size = 320 << 10
	}
	// The durable digest must agree across every execution generation. The
	// Gateway fixture decides whether this is its first response to hold.
	args, err := json.Marshal(effectArgs{EffectID: id, LatencyMS: input.MCPMillis, SizeBytes: size, HoldReply: input.Fault && index == 0})
	return sdktools.LeasedCall{Scope: scope, CallID: id, Key: id, Tool: profileTool, Arguments: args}, err
}

func workerSettle(ctx context.Context, store *state.Client, tools *sdktools.Client, lease *state.VMLease, scope contracts.Scope,
	turn state.ClaimedTurn, input turnInput, index int,
) (int64, error) {
	call, err := workerToolCall(scope, input, index)
	if err != nil {
		return 0, err
	}
	cp := journal.Checkpoint{Namespace: "profile", ID: call.CallID, SnapshotSeq: turn.InputSeq,
		Metadata: json.RawMessage(`{}`), ChannelVersions: json.RawMessage(`{}`), Codec: "json", Payload: journal.Payload{Inline: call.Arguments}}
	_, err = store.CommitCheckpoint(ctx, turn.SessionID, turn.Epoch, journal.CheckpointCommit{
		TurnSeq: turn.InputSeq, Key: "bench.prepare:" + call.CallID, Checkpoint: cp,
		Intents: []journal.PreparedIntent{{CallID: call.CallID, ToolName: call.Tool, ArgsDigest: sha256.Sum256(call.Arguments), IdempotencyKey: call.Key}},
	})
	if err != nil {
		return 0, fmt.Errorf("prepare %s: %w", call.CallID, err)
	}
	started := time.Now()
	settled, err := tools.SettleLeasedCall(ctx, store, lease, call)
	elapsed := time.Since(started).Nanoseconds()
	if err != nil {
		return 0, fmt.Errorf("settle %s: %w", call.CallID, err)
	}
	if settled.Status != journal.IntentCompleted || settled.Result == nil || settled.Result.Blob != nil || !json.Valid(settled.Result.Inline) {
		return 0, fmt.Errorf("settle %s: expected COMPLETED with a journal result, got %s", call.CallID, settled.Status)
	}
	if input.Large && index == 1 {
		var result struct {
			Storage string               `json:"result_storage"`
			Ref     sdktools.ArtifactRef `json:"artifact_ref"`
		}
		if err := json.Unmarshal(settled.Result.Inline, &result); err != nil || result.Storage != sdktools.StorageArtifact ||
			result.Ref.ArtifactID == "" || result.Ref.Size <= 256<<10 || len(settled.Result.Inline) > 1024 {
			return 0, fmt.Errorf("settle %s: expected a compact artifact reference", call.CallID)
		}
	} else if len(settled.Result.Inline) < 8<<10 || len(settled.Result.Inline) > 256<<10 {
		return 0, fmt.Errorf("settle %s: expected an inline MCP payload", call.CallID)
	}
	return elapsed, nil
}

func workerMeasureExpiry(ctx context.Context, tools *sdktools.Client, first state.ClaimedTurn, lease *state.VMLease, id string) error {
	if err := workerSleep(ctx, workerClaimTTL); err != nil {
		return err
	}
	probe, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	proof := sdktools.LeaseProof{SessionID: first.SessionID, Epoch: first.Epoch, Proof: first.ToolLeaseProof}
	for {
		_, err := tools.FenceAndLookup(probe, id+"/expiry-probe", proof)
		if contracts.Code(err) == contracts.ErrStaleEpoch {
			if lease.Current().ToolLeaseProof == first.ToolLeaseProof {
				return errors.New("heartbeat did not renew the initial tool lease proof")
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("check proof expiry at Gateway: %w", err)
		}
		if err := workerSleep(probe, 50*time.Millisecond); err != nil {
			return fmt.Errorf("Gateway did not reject the initial tool lease proof: %w", err)
		}
	}
}

func workerSleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}
