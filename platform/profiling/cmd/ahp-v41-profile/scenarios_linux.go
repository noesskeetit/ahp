package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ahp-platform/ahp/internal/runcoord"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

type profile struct {
	name                  string
	tools, mcpMS, thinkMS int
	largeEvery            int
	warm, firecracker     bool
	timeout               time.Duration
}

func (p profile) input(id string, ordinal int) turnInput {
	return turnInput{
		ID: id, Padding: strings.Repeat("x", 4<<10), Tools: p.tools, MCPMillis: p.mcpMS,
		ThinkMillis: p.thinkMS, Large: p.largeEvery > 0 && ordinal%p.largeEvery == 0,
		HoldForFollowup: p.warm,
	}
}

func (p profile) turnTimeout() time.Duration {
	if p.timeout != 0 {
		return p.timeout
	}
	return 15 * time.Second
}

func (s *stand) submit(ctx context.Context, target int, conversation string, input turnInput, scheduled int64, delivery string, timeout time.Duration) inflight {
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.postTurn(requestCtx, target, conversation, delivery, input, scheduled)
}

func (s *stand) observe(ctx context.Context, p profile, call inflight) requestResult {
	return withTurnTimeout(ctx, p.turnTimeout(), func(readCtx context.Context) requestResult {
		return s.observeFaultOrNormal(readCtx, call, p.firecracker)
	})
}

func (s *stand) stopExecution(ctx context.Context, target int, r *requestResult) {
	if r.Executed == nil {
		return
	}
	scope := s.Scope(target)
	session := journal.ScopedSessionID(scope, r.Conversation)
	s.stopObservedExecution(ctx, r, runcoord.Execution{
		Scope: scope, SessionID: session, LaunchTurnSeq: r.Executed.LaunchTurnSeq, Generation: r.Executed.Epoch,
		ExecutionID: journal.DeterministicExecutionID(scope, session, r.Executed.LaunchTurnSeq, r.Executed.Epoch),
	})
}

func (s *stand) stopColdSoakExecution(ctx context.Context, target int, r *requestResult) {
	if r.AcceptedAt == 0 {
		return
	}
	if r.Executed != nil && r.Failure == "" {
		s.stopExecution(ctx, target, r)
		return
	}

	// A timed-out turn may have had a replacement even if SSE never
	// produced an output. Look up the published Runtime identity for this
	// launch, including fenced generations, so we cannot choose a stale one.
	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	scope := s.Scope(target)
	execution := runcoord.Execution{Scope: scope, SessionID: journal.ScopedSessionID(scope, r.Conversation), LaunchTurnSeq: r.Seq}
	var status string
	err := s.RuntimePG.QueryRow(lookupCtx, `
		SELECT execution_id,generation,status FROM ahp_runtime.executions
		WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3 AND session_id=$4 AND launch_turn_seq=$5
		ORDER BY generation DESC LIMIT 1`, scope.Tenant, scope.Deployment, scope.Subject, execution.SessionID, execution.LaunchTurnSeq).
		Scan(&execution.ExecutionID, &execution.Generation, &status)
	if err != nil {
		r.FenceTimeError = "runtime_fence_owner_unobserved"
		return
	}
	if status != "FENCED" {
		s.stopObservedExecution(ctx, r, execution)
	}
}

func (s *stand) stopObservedExecution(ctx context.Context, r *requestResult, execution runcoord.Execution) {
	started := monotonicNS()
	fenceCtx, cancel := context.WithTimeout(ctx, runtimeFenceTimeout)
	defer cancel()
	if err := s.dispatch.Fence(fenceCtx, execution); err != nil {
		r.FenceTimeError = "runtime_fence_failed"
		if r.Failure == "" {
			r.Failure = r.FenceTimeError
		}
	}
	r.FenceNS = monotonicNS() - started
}

type closedStep struct {
	name                          string
	work                          profile
	lanes, countPerLane, minTurns int
	duration                      time.Duration
	expiryProbe                   bool
	warmup                        time.Duration
	faultAt                       int
}

func closedLoop(ctx context.Context, s *stand, step closedStep, collect *collector) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	deadline := time.Now().Add(step.duration)
	var next atomic.Int64
	var lanes sync.WaitGroup
	for i := 0; i < step.lanes; i++ {
		lanes.Go(func() {
			target := i % len(s.SubjectTokens)
			laneOrdinal := 0
			newInput := func() turnInput {
				n := int(next.Add(1))
				laneOrdinal++
				input := step.work.input(step.name+"-"+strconv.Itoa(n), n)
				input.Fault = step.faultAt == laneOrdinal && i < step.lanes/2
				return input
			}
			keepGoing := func(n int) bool {
				if runCtx.Err() != nil || collect.stop.Load() {
					return false
				}
				if step.countPerLane > 0 {
					return n < step.countPerLane
				}
				return time.Now().Before(deadline) ||
					(collect.completed.Load() < int64(step.minTurns) && time.Now().Before(deadline.Add(step.duration)))
			}
			if step.work.warm {
				warmLane(runCtx, s, target, step, i, newInput, keepGoing, collect)
				return
			}
			for n := 0; keepGoing(n); n++ {
				input := newInput()
				call := s.submit(runCtx, target, input.ID, input, 0, journal.DeliveryTurn, step.work.turnTimeout())
				r := s.observe(runCtx, step.work, call)
				if step.work.firecracker && r.Executed != nil {
					r.BootNS, _ = s.BootstrapNS(runCtx, r.Executed.ExecutionID)
				}
				s.stopExecution(runCtx, target, &r)
				collect.add(r)
			}
		})
	}
	lanes.Wait()
}

func warmLane(ctx context.Context, s *stand, target int, step closedStep, lane int, newInput func() turnInput, keepGoing func(int) bool, collect *collector) {
	conv := fmt.Sprintf("%s-warm-%d", step.name, lane)
	// The initial host launch is witnessed separately. It keeps one accepted
	// continuation queued so the first measured turn runs on the same worker.
	warmup := turnInput{ID: conv + "-boot", HoldForFollowup: true, MeasureExpiry: step.expiryProbe}
	first := s.submit(ctx, target, conv, warmup, 0, journal.DeliveryTurn, step.work.turnTimeout())
	input := newInput()
	input.HoldForFollowup = keepGoing(1)
	current := s.submit(ctx, target, conv, input, 0, "auto", step.work.turnTimeout())
	boot := s.observe(ctx, step.work, first)
	if boot.Failure != "" || boot.Executed == nil {
		collect.add(requestResult{
			ID: current.result.ID, Conversation: conv, Tenant: s.Scope(target).Tenant, SentAt: current.result.SentAt,
			AcceptedAt: current.result.AcceptedAt, Tools: input.Tools, Failure: "warm_boot/" + boot.Failure,
		})
		return
	}
	previous := boot
	for n := 0; ; n++ {
		var next inflight
		if input.HoldForFollowup {
			if keepGoing(n + 1) {
				successor := newInput()
				successor.HoldForFollowup = keepGoing(n + 2)
				next = s.submit(ctx, target, conv, successor, 0, "auto", step.work.turnTimeout())
				input = successor
			} else {
				// Give the already admitted hold-for-followup turn a
				// committed boundary. Cleanup turns never enter the rate.
				stop := turnInput{ID: conv + "-drain"}
				next = s.submit(ctx, target, conv, stop, 0, "auto", step.work.turnTimeout())
				input = stop
			}
		} else {
			input = turnInput{}
		}
		r := s.observe(ctx, step.work, current)
		if r.Executed != nil {
			reused := r.Executed.ExecutionID == previous.Executed.ExecutionID
			r.Reused = &reused
			previous = r
		}
		if next.result.ID == "" {
			s.stopExecution(ctx, target, &r)
			collect.add(r)
			break
		}
		collect.add(r)
		if input.Tools == 0 {
			drain := s.observe(ctx, step.work, next)
			if drain.Executed == nil {
				drain = previous
			}
			s.stopExecution(ctx, target, &drain)
			if drain.FenceTimeError != "" {
				collect.note("warm drain fence failed")
			}
			break
		}
		if r.Failure != "" && r.Executed == nil {
			queued := next.result
			if queued.Failure == "" {
				queued.Failure = "warm_lane_drain_after_prior_failure"
			}
			collect.add(queued)
			s.stopExecution(ctx, target, &previous)
			break
		}
		current = next
	}
}
