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
	if call.result.Failure != "" || !call.result.Fault {
		return s.readTurn(ctx, call, firecracker)
	}
	observeCtx, stop := context.WithCancel(ctx)
	defer stop()
	frozen := make(chan error, 1)
	go func() { frozen <- s.freezeAfterReceipt(observeCtx, call.result) }()
	r := s.readTurn(observeCtx, call, firecracker)
	stop()
	if err := <-frozen; err != nil {
		r.Failure = "fault_freeze_failed"
	} else if r.Executed == nil || r.Executed.Epoch < 2 {
		r.Failure = "fault_replacement_missing"
	}
	return r
}

func (s *stand) freezeAfterReceipt(ctx context.Context, r requestResult) error {
	if err := s.Effects.held(ctx, r.ID+"/0"); err != nil {
		return err
	}
	for i := range s.SubjectTokens {
		scope := s.Scope(i)
		if scope.Tenant != r.Tenant {
			continue
		}
		session := journal.ScopedSessionID(scope, r.Conversation)
		var launch, gen int64
		if err := s.RuntimePG.QueryRow(ctx, `
          SELECT launch_turn_seq,generation FROM ahp_runtime.executions
          WHERE tenant_id=$1 AND deployment_id=$2 AND session_id=$3 AND status='RUNNING'
          ORDER BY generation DESC LIMIT 1`, scope.Tenant, scope.Deployment, session).Scan(&launch, &gen); err != nil {
			return err
		}
		_, err := s.Freeze(ctx, scope, session, launch, gen)
		return err
	}
	return errors.New("fixture scope missing")
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
		for j := 0; j < total; j++ {
			wait := time.Until(first.Add(time.Duration(float64(j) / period.Rate * float64(time.Second))))
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
			scheduled := period.StartNS + int64(float64(j)/period.Rate*float64(time.Second))
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
			input.Fault = cfg.injectFaults && ordinal%50 == 0 && cfg.quick == 0
			select {
			case pendingLimit <- struct{}{}:
				pending.Go(func() {
					defer func() { <-pendingLimit }()
					if !cold {
						perLane[lane].Lock()
					}
					call := s.submit(workCtx, target, conv, input, scheduled, "auto", 15*time.Second)
					if !cold {
						perLane[lane].Unlock()
					}
					timeout := 15 * time.Second
					if input.Fault {
						timeout = 95 * time.Second
					}
					result := withTurnTimeout(workCtx, timeout, func(turnCtx context.Context) requestResult {
						return s.observeFaultOrNormal(turnCtx, call, false)
					})
					if result.Executed != nil {
						reused := result.Seq != result.Executed.LaunchTurnSeq
						result.Reused = &reused
					}
					if cold {
						s.stopExecution(workCtx, target, &result)
					}
					c.add(result)
				})
			default:
				c.add(requestResult{ID: id, Conversation: conv, Tenant: s.Scope(target).Tenant, RequestedAt: scheduled,
					Fault: input.Fault, Tools: input.Tools, Failure: "generator_outstanding_limit"})
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
	// Fence the last physical owner of each warm lane. Old completed
	// executions have already stopped their attempt on worker exit.
	c.mu.Lock()
	last := map[string]requestResult{}
	for _, r := range c.rows {
		if r.Executed == nil || r.Conversation == r.ID {
			continue
		}
		if previous := last[r.Conversation]; r.Executed.Epoch >= previousEpoch(previous) {
			last[r.Conversation] = r
		}
	}
	c.mu.Unlock()
	for _, r := range last {
		for i := range s.SubjectTokens {
			if s.Scope(i).Tenant == r.Tenant {
				s.stopExecution(ctx, i, &r)
				break
			}
		}
		if r.FenceTimeError != "" {
			c.note("warm final fence failed")
		}
	}
	endSampler()
	if err := writeJSONFile(filepath.Join(c.dir, "schedule.json"), periods); err != nil {
		return err
	}
	summaries := map[string]any{}
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
		for _, r := range c.rows {
			if r.RequestedAt >= period.StartNS && r.RequestedAt < period.EndNS {
				scheduled++
				if r.SentAt > 0 {
					sent++
				}
				if r.AcceptedAt > 0 {
					accepted++
				}
				if !r.Fault {
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
			"completed_during_interval": done, "normal_attempts": ordinary, "normal_errors": errorsN}
	}
	if err := writeJSONFile(filepath.Join(c.dir, "intervals.json"), summaries); err != nil {
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
	if summary.NormalAttempts == 0 || float64(summary.NormalErrors)/float64(summary.NormalAttempts) > .001 ||
		summary.WorstNormalErrorRate > .001 {
		qualification = append(qualification, "ordinary errors > 0.1%")
	}
	if cfg.injectFaults && (summary.FaultsScheduled == 0 || summary.FaultsRecovered != summary.FaultsScheduled) {
		qualification = append(qualification, "unconfirmed injected recovery")
	}
	if summary.LastQueue["QUEUED"]+summary.LastQueue["ACTIVE"]+summary.LastQueue["FOLLOWUPS"] != 0 {
		qualification = append(qualification, "nonempty queues after drain")
	}
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
	}); err != nil {
		return err
	}

	fmt.Printf("AHP_PROFILE_STAGE p4-soak planned=%d sent=%d completed=%d fault_recovered=%d/%d rate=%.3f errors=%v\n",
		summary.Scheduled, summary.Offered, summary.Completed, summary.FaultsRecovered, summary.FaultsScheduled, summary.Rate, summary.Failure)
	return nil
}

func previousEpoch(r requestResult) int64 {
	if r.Executed == nil {
		return 0
	}
	return r.Executed.Epoch
}
