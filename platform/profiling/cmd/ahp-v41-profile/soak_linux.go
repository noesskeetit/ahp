package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

func (s *stand) observeFaultOrNormal(ctx context.Context, call inflight, firecracker bool) requestResult {
	r, _ := s.observeFaultOutcome(ctx, call, firecracker)
	return r
}

func (s *stand) observeFaultOutcome(ctx context.Context, call inflight, firecracker bool) (requestResult, bool) {
	if call.result.Failure != "" || !call.result.Fault {
		return s.readTurn(ctx, call, firecracker), false
	}
	observeCtx, stop := context.WithCancel(ctx)
	defer stop()
	frozen := make(chan int64, 1)
	go func() { frozen <- s.freezeAfterReceipt(observeCtx, call.result) }()
	r := s.readTurn(observeCtx, call, firecracker)
	stop()
	epoch := <-frozen
	if epoch == 0 {
		r.Failure = "fault_freeze_failed"
	} else if r.Executed == nil || r.Executed.Epoch <= epoch {
		r.Failure = "fault_replacement_missing"
	}
	return r, epoch != 0
}

func (s *stand) freezeAfterReceipt(ctx context.Context, r requestResult) int64 {
	key := r.ID + "/0"
	if err := s.Effects.held(ctx, key); err != nil {
		return 0
	}
	for i := range s.SubjectTokens {
		scope := s.Scope(i)
		if scope.Tenant != r.Tenant {
			continue
		}
		session := journal.ScopedSessionID(scope, r.Conversation)
		var owner string
		var launch, gen int64
		// The active turn and its physical owner come from State. A warm
		// owner's launch sequence can predate this turn.
		if err := s.PG.QueryRow(ctx, `
			SELECT lease_owner,lease_epoch FROM ahp_journal.sessions
			WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3 AND session_id=$4
			  AND status='ACTIVE' AND turn_input_seq=$5 AND lease_expires_at>statement_timestamp()`,
			scope.Tenant, scope.Deployment, scope.Subject, session, r.Seq).Scan(&owner, &gen); err != nil {
			return 0
		}
		if err := s.RuntimePG.QueryRow(ctx, `
			SELECT launch_turn_seq FROM ahp_runtime.executions
			WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3 AND session_id=$4 AND execution_id=$5 AND generation=$6`,
			scope.Tenant, scope.Deployment, scope.Subject, session, owner, gen).Scan(&launch); err != nil {
			return 0
		}
		if journal.DeterministicExecutionID(scope, session, launch, gen) != owner {
			return 0
		}
		// The held acknowledgement can expire during the lookups.
		heldCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		err := s.Effects.held(heldCtx, key)
		cancel()
		if err != nil {
			return 0
		}
		if _, err := s.Freeze(ctx, scope, session, launch, gen); err == nil {
			return gen
		}
		return 0
	}
	return 0
}

const soakFaultCutoff = 90 * time.Second

func soakFaultScheduled(enabled bool, ordinal int, arrival, loadEnd time.Time) bool {
	return enabled && ordinal%50 == 0 && arrival.Before(loadEnd.Add(-soakFaultCutoff))
}

type soakFaultAttempt struct {
	ID        string `json:"id"`
	Armed     bool   `json:"hold_reply_sent"`
	Accepted  bool   `json:"accepted"`
	Injected  bool   `json:"sigstop_confirmed"`
	Recovered bool   `json:"recovery_verified"`
}

type soakFaultTotals struct {
	Intended  int `json:"intended"`
	Armed     int `json:"armed"`
	Accepted  int `json:"accepted"`
	Injected  int `json:"injected"`
	Recovered int `json:"recovered"`
}

func (t *soakFaultTotals) add(a soakFaultAttempt) {
	t.Intended++
	if a.Armed {
		t.Armed++
		if a.Accepted {
			t.Accepted++
		}
	}
	if a.Injected {
		t.Injected++
	}
	if a.Recovered {
		t.Recovered++
	}
}

func (t soakFaultTotals) qualified() bool {
	return t.Injected > 0 && t.Accepted == t.Injected && t.Injected == t.Recovered
}

type loadPeriod struct {
	Name    string  `json:"name"`
	Seconds float64 `json:"seconds"`
	Rate    float64 `json:"scheduled_per_second"`
	StartNS int64   `json:"start_mono_ns"`
	EndNS   int64   `json:"end_mono_ns"`
}

func runSoak(ctx context.Context, s *stand, cfg sessionConfig, rate float64, lanes int, cgroup string) error {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || rate > 10000 {
		return errors.New("invalid calibrated soak rate")
	}
	c, err := newCollector(cfg.out, "p4-soak")
	if err != nil {
		return err
	}
	periods := []loadPeriod{{Name: "steady", Seconds: 900, Rate: .85 * rate},
		{Name: "burst", Seconds: 120, Rate: 1.25 * rate},
		{Name: "recovery", Seconds: 180, Rate: .85 * rate}}
	drain := 60 * time.Second
	if cfg.quick > 0 {
		periods[0].Seconds = cfg.quick.Seconds()
		periods[1].Seconds = (cfg.quick / 4).Seconds()
		periods[2].Seconds = (cfg.quick / 3).Seconds()
		drain = max(10*time.Second, cfg.quick/2)
	}
	for i := 0; i < lanes; i++ {
		if err := s.seedLane(ctx, i%len(s.SubjectTokens), fmt.Sprintf("soak-warm-%d", i)); err != nil {
			return err
		}
	}
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	c.started = monotonicNS()
	endSampler := sampleResources(ctx, s, c, cgroup)
	defer endSampler()
	var pending sync.WaitGroup
	var faultMu sync.Mutex
	faults := make(map[string]soakFaultAttempt)
	// The pacing loop stays independent of service backpressure. Capacity
	// misses are recorded against the planned arrival, not silently skipped.
	pendingLimit := make(chan struct{}, max(128, 4*lanes))
	perLane := make([]sync.Mutex, lanes)
	ordinal := 0
	for i := range periods {
		period := &periods[i]
		fmt.Printf("AHP_PROFILE_PERIOD %s rate=%.3f seconds=%.0f faults=%t\n",
			period.Name, period.Rate, period.Seconds, cfg.injectFaults && cfg.quick == 0)
		first := time.Now()
		period.StartNS = monotonicNS()
		total := int(math.Floor(period.Seconds * period.Rate))
		end := first
		for _, remaining := range periods[i:] {
			end = end.Add(time.Duration(remaining.Seconds * float64(time.Second)))
		}
		faultCutoff := end.Add(-soakFaultCutoff)
		for j := 0; j < total; j++ {
			offset := time.Duration(float64(j) / period.Rate * float64(time.Second))
			arrival := first.Add(offset)
			wait := time.Until(arrival)
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
				}
			}
			if ctx.Err() != nil || c.stop.Load() {
				break
			}
			ordinal++
			scheduled := period.StartNS + int64(offset)
			cold := ordinal%20 < 3
			lane := int((uint64(ordinal)*2654435761)>>20) % lanes
			target := lane % len(s.SubjectTokens)
			id := fmt.Sprintf("soak-%d", ordinal)
			conv := fmt.Sprintf("soak-warm-%d", lane)
			if cold {
				conv = id
			}
			input := realistic.input(id, ordinal)
			input.HoldForFollowup = false
			intendedFault := soakFaultScheduled(cfg.injectFaults && cfg.quick == 0, ordinal, arrival, end)
			select {
			case pendingLimit <- struct{}{}:
				pending.Go(func() {
					defer func() { <-pendingLimit }()
					if !cold {
						perLane[lane].Lock()
					}
					input.Fault = intendedFault && time.Now().Before(faultCutoff)
					call := s.submit(workCtx, target, conv, input, scheduled, "auto", 15*time.Second)
					if !cold {
						perLane[lane].Unlock()
					}
					timeout := 15 * time.Second
					if input.Fault {
						timeout = 95 * time.Second
					}
					var injected bool
					result := withTurnTimeout(workCtx, timeout, func(turnCtx context.Context) requestResult {
						r, applied := s.observeFaultOutcome(turnCtx, call, false)
						injected = applied
						return r
					})
					result.Fault = intendedFault
					if result.Executed != nil {
						reused := result.Seq != result.Executed.LaunchTurnSeq
						result.Reused = &reused
					}
					if cold {
						s.stopColdSoakExecution(ctx, target, &result)
					}
					if intendedFault {
						faultMu.Lock()
						faults[id] = soakFaultAttempt{ID: id, Armed: input.Fault && result.SentAt > 0,
							Accepted: result.AcceptedAt > 0, Injected: injected,
							Recovered: injected && result.Failure == "" && result.VerifiedAt > 0}
						faultMu.Unlock()
					}
					c.add(result)
				})
			default:
				if intendedFault {
					faultMu.Lock()
					faults[id] = soakFaultAttempt{ID: id}
					faultMu.Unlock()
				}
				c.add(requestResult{ID: id, Conversation: conv, Tenant: s.Scope(target).Tenant, RequestedAt: scheduled,
					Fault: intendedFault, Tools: input.Tools, Failure: "generator_outstanding_limit"})
			}
		}
		// The configured open-loop interval includes its final fractional
		// gap. Keep its nominal boundary independent of request completion.
		if until := time.Until(first.Add(time.Duration(period.Seconds * float64(time.Second)))); until > 0 && ctx.Err() == nil && !c.stop.Load() {
			timer := time.NewTimer(until)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
		}
		period.EndNS = monotonicNS()
		if ctx.Err() != nil || c.stop.Load() {
			break
		}
	}
	c.stoppedAt = monotonicNS()
	drained := make(chan struct{})
	go func() { pending.Wait(); close(drained) }()
	drainTimer := time.NewTimer(drain)
	defer drainTimer.Stop()
	select {
	case <-drained:
	case <-drainTimer.C:
		c.note("drain deadline elapsed")
		stop()
		<-drained
	case <-ctx.Done():
		stop()
		<-drained
	}
	// Client observation and State draining are distinct. Let the live
	// coordinator complete queued jobs and reconcile leases before fencing.
	drainStarted := monotonicNS()
	beforeFence, drainErr := s.waitSoakStateDrain(ctx, drain)
	if drainErr != nil {
		c.note("State drain failed")
	}
	s.soakCleanup = s.collectSoakCleanup(ctx, beforeFence, drainErr)
	if err := writeJSONFile(filepath.Join(c.dir, "drain.json"), map[string]any{
		"state_drain_started_ns": drainStarted, "finished_ns": monotonicNS(), "cleanup": s.soakCleanup,
	}); err != nil {
		return err
	}
	endSampler()
	if err := writeJSONFile(filepath.Join(c.dir, "schedule.json"), periods); err != nil {
		return err
	}
	summaries := map[string]any{}
	var faultTotals soakFaultTotals
	faultRows := make([]soakFaultAttempt, 0, len(faults))
	normalRows := make([]requestResult, 0, len(c.rows))
	fenceFailures := 0
	for _, r := range c.rows {
		if r.FenceTimeError != "" {
			fenceFailures++
		}
		if fault, ok := faults[r.ID]; ok {
			faultTotals.add(fault)
			faultRows = append(faultRows, fault)
			if fault.Armed && fault.Accepted {
				continue
			}
		}
		r.Fault = false
		normalRows = append(normalRows, r)
	}
	var steady []sample
	if periods[0].EndNS != 0 {
		from := periods[0].StartNS
		for _, sample := range c.samples {
			if sample.MonotonicNS >= from && sample.MonotonicNS <= periods[0].EndNS {
				steady = append(steady, sample)
			}
		}
	}
	for _, period := range periods {
		if period.EndNS == 0 {
			continue
		}
		var scheduled, sent, accepted, done, errorsN, ordinary int
		var intervalFaults soakFaultTotals
		for _, r := range c.rows {
			if r.RequestedAt >= period.StartNS && r.RequestedAt < period.EndNS {
				scheduled++
				if r.SentAt > 0 {
					sent++
				}
				if r.AcceptedAt > 0 {
					accepted++
				}
				fault := faults[r.ID]
				if r.Fault {
					intervalFaults.add(fault)
				}
				if !fault.Armed || !fault.Accepted {
					ordinary++
					if r.Failure != "" {
						errorsN++
					}
				}
			}
			if r.Failure == "" && r.VerifiedAt >= period.StartNS && r.VerifiedAt < period.EndNS {
				done++
			}
		}
		summaries[period.Name] = map[string]any{"scheduled": scheduled, "sent": sent, "accepted": accepted,
			"completed_during_interval": done, "normal_attempts": ordinary, "normal_errors": errorsN, "faults": intervalFaults}
	}
	if err := writeJSONFile(filepath.Join(c.dir, "intervals.json"), summaries); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(c.dir, "faults.json"), map[string]any{
		"totals": faultTotals, "attempts": faultRows, "cutoff_seconds_before_load_end": soakFaultCutoff.Seconds(),
	}); err != nil {
		return err
	}
	steadyResources := summarizeSamples(steady)
	if err := writeJSONFile(filepath.Join(c.dir, "steady-resources.json"), steadyResources); err != nil {
		return err
	}
	summary, err := c.finish()
	if err != nil {
		return err
	}
	qualification := make([]string, 0)
	if cfg.quick > 0 {
		qualification = append(qualification, "shortened diagnostic intervals")
	}
	normalErrors := 0
	for _, r := range normalRows {
		if r.Failure != "" {
			normalErrors++
		}
	}
	worstNormalErrorRate := windowErrors(normalRows)
	if len(normalRows) == 0 || float64(normalErrors)/float64(len(normalRows)) > .001 || worstNormalErrorRate > .001 {
		qualification = append(qualification, "ordinary errors > 0.1%")
	}
	if cfg.injectFaults && !faultTotals.qualified() {
		qualification = append(qualification, "unconfirmed injected recovery")
	}
	cleanup := s.soakCleanup
	qualification = append(qualification, cleanup.qualificationFailures(fenceFailures)...)
	for _, key := range []string{"run-coordinator", "ahp-vm-runtime"} {
		proc, ok := summary.Processes[key]
		if !ok || proc.Final.FD > proc.Initial.FD+15 || proc.Final.Goroutines > proc.Initial.Goroutines+15 {
			qualification = append(qualification, key+" descriptors/goroutines exceeded baseline or were unavailable")
		}
	}
	coordinator := steadyResources["run-coordinator"]
	if coordinator.RSSMiBPerMinute == nil || coordinator.HeapMiBPerMinute == nil ||
		*coordinator.RSSMiBPerMinute > 2 || *coordinator.HeapMiBPerMinute > 2 {
		qualification = append(qualification, "coordinator RSS/heap slope over +2 MiB/min or unobserved")
	}
	if e := s.Effects.snapshot(); e.Duplicates > 0 || e.Unauthorized > 0 {
		qualification = append(qualification, "duplicate or unauthorized MCP request")
	}
	if err := writeJSONFile(filepath.Join(c.dir, "qualification.json"), map[string]any{
		"qualified": len(qualification) == 0, "reasons": qualification, "upstream_p2_rate": rate,
		"normal_attempts": len(normalRows), "normal_errors": normalErrors, "worst_normal_error_fraction": worstNormalErrorRate,
		"faults": faultTotals, "cold_fence_failures": fenceFailures,
	}); err != nil {
		return err
	}

	fmt.Printf("AHP_PROFILE_STAGE p4-soak planned=%d sent=%d completed=%d fault_intended=%d fault_accepted=%d fault_injected=%d fault_recovered=%d rate=%.3f errors=%v\n",
		summary.Scheduled, summary.Offered, summary.Completed, faultTotals.Intended, faultTotals.Accepted,
		faultTotals.Injected, faultTotals.Recovered, summary.Rate, summary.Failure)
	if drainErr != nil {
		return fmt.Errorf("P4 State drain: %w", drainErr)
	}
	if len(cleanup.ObservationErrors) != 0 {
		return fmt.Errorf("P4 cleanup incomplete: %v", cleanup.ObservationErrors)
	}
	return nil
}
