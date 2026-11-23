package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

const p5Duration = 120 * time.Second

type longStep struct {
	benchStep
	Occupancy        longOccupancy `json:"occupancy"`
	MCPDuplicates    int           `json:"mcp_duplicates"`
	MCPUnauthorized  int           `json:"mcp_unauthorized"`
	GenuineErrorRate float64       `json:"genuine_error_fraction"`
}

type countDistribution struct {
	N   int `json:"n"`
	P50 int `json:"p50"`
	P95 int `json:"p95"`
	Max int `json:"max"`
}

type longOccupancy struct {
	ActiveSessions   countDistribution `json:"observed_active_sessions"`
	ExecutingTurns   countDistribution `json:"observed_executing_turns"`
	OpenStreams      countDistribution `json:"observed_open_sse"`
	GuestProcesses   countDistribution `json:"observed_guest_processes"`
	PSSPerVM         countDistribution `json:"pss_bytes_per_vm_at_target_density,omitempty"`
	MissingTerminals int               `json:"journal_starts_without_terminal"`
	ExecutionClock   string            `json:"execution_clock"`
}

type journalInterval struct {
	Tenant       string     `json:"tenant"`
	Session      string     `json:"session_id"`
	TurnSeq      int64      `json:"turn_seq"`
	ID           string     `json:"id"`
	Started      time.Time  `json:"start_event_created_at"`
	Completed    *time.Time `json:"terminal_event_created_at,omitempty"`
	WorkerMonoNS int64      `json:"worker_start_ns"`
	CommitMonoNS int64      `json:"state_commit_ack_ns,omitempty"`
	Superseded   bool       `json:"superseded_start,omitempty"`
}

func runLongProfiles(ctx context.Context, s *stand, cfg sessionConfig) error {
	ladder := []int{16, 32, 64, 128, 192, 256, 384, 512}
	if cfg.maxLanes < 16 {
		ladder = []int{8}
	}
	if cfg.phase == "fcmax" {
		ladder = []int{cfg.fcLanes}
	}
	var rows []longStep
	firstFailed := false
	for _, lanes := range ladder {
		if lanes > cfg.maxLanes {
			break
		}
		name := fmt.Sprintf("%s-c%d", cfg.phase, lanes)
		if cfg.phase == "fcmax" {
			name = fmt.Sprintf("fcmax-m%d", lanes)
		}
		row, err := runLongStage(ctx, s, cfg, name, lanes)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		if err := writeJSONFile(filepath.Join(cfg.out, cfg.phase+"-ladder.json"), rows); err != nil {
			return err
		}
		fmt.Printf("AHP_PROFILE_STAGE %s completed=%d rate=%.3f active_p95=%d executing_p95=%d sse_p95=%d qualified=%t reasons=%v\n",
			name, row.Summary.Completed, row.Summary.Rate, row.Occupancy.ActiveSessions.P95, row.Occupancy.ExecutingTurns.P95,
			row.Occupancy.OpenStreams.P95, row.Accepted, row.Reasons)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if firstFailed {
			break
		}
		firstFailed = !row.Accepted
	}
	return nil
}

func runLongStage(ctx context.Context, s *stand, cfg sessionConfig, name string, lanes int) (longStep, error) {
	duration, warmup := p5Duration, 10*time.Second
	if cfg.quick > 0 {
		duration, warmup = cfg.quick, min(warmup, cfg.quick/3)
	}
	for lane := range lanes {
		if err := s.seedLane(ctx, lane%len(s.SubjectTokens), longConversation(name, lane)); err != nil {
			return longStep{}, err
		}
	}
	c, err := newCollector(cfg.out, name)
	if err != nil {
		return longStep{}, err
	}
	effectsBefore := s.Effects.snapshot()
	dbBefore, pgBeforeErr := snapshotPG(ctx, s)
	c.started = monotonicNS() + int64(warmup)
	deadline := c.started + int64(duration)
	stopSampling := sampleResources(ctx, s, c, s.cfg.SUTCgroup)
	fmt.Printf("AHP_PROFILE_STAGE_START %s lanes=%d warmup=%s duration=%s ttl=%s\n", name, lanes, warmup, duration, cfg.claimTTL)
	var lanesDone sync.WaitGroup
	var ordinal atomic.Int64
	for lane := range lanes {
		lanesDone.Go(func() {
			runLongLane(ctx, s, c, cfg, lane, deadline, &ordinal)
		})
	}
	lanesDone.Wait()
	c.stoppedAt = min(deadline, monotonicNS())
	stopSampling()
	fences, fenceErr := s.closeRunningGuests(context.WithoutCancel(ctx))
	if fenceErr != nil {
		c.note("warm fence failed: " + fenceErr.Error())
	}
	if err := writeJSONFile(filepath.Join(c.dir, "fences.json"), fences); err != nil {
		c.note("warm fence evidence failed: " + err.Error())
	}
	effectsAfter := s.Effects.snapshot()
	dbAfter, pgAfterErr := snapshotPG(ctx, s)
	if pgBeforeErr == nil && pgAfterErr == nil {
		if err := writeJSONFile(filepath.Join(c.dir, "pg-statements.json"), map[string]any{"before": dbBefore, "after": dbAfter}); err != nil {
			c.note("pg-statements evidence: " + err.Error())
		}
	} else {
		c.note("pg_stat_statements unavailable")
	}
	intervals, intervalErr := readLongIntervals(ctx, s, name, lanes)
	useCommitTrace := false
	if intervalErr == nil {
		if cfg.phase == "p5" {
			if err := joinLongCommitTrace(filepath.Join(cfg.out, "state-finish-commits.tsv"), intervals); err != nil {
				c.note("State commit trace unavailable: " + err.Error())
			} else {
				useCommitTrace = true
			}
		}
		intervalErr = writeJSONFile(filepath.Join(c.dir, "execution-intervals.json"), intervals)
		if intervalErr == nil {
			intervalErr = verifyLongIntervals(c.rows, intervals)
		}
	}
	if intervalErr != nil {
		c.note("journal execution evidence: " + intervalErr.Error())
	}
	occupancy := summarizeLongOccupancy(c, intervals, cfg.phase == "fcmax", useCommitTrace, lanes)
	summary, err := c.finish()
	row := longStep{
		benchStep:       benchStep{Summary: summary, Concurrency: lanes},
		Occupancy:       occupancy,
		MCPDuplicates:   effectsAfter.Duplicates - effectsBefore.Duplicates,
		MCPUnauthorized: effectsAfter.Unauthorized - effectsBefore.Unauthorized,
	}
	if summary.NormalAttempts > 0 {
		row.GenuineErrorRate = float64(summary.NormalErrors) / float64(summary.NormalAttempts)
	}
	row.qualify(duration, intervalErr != nil)
	return row, err
}

func (row *longStep) qualify(duration time.Duration, missingIntervals bool) {
	summary := row.Summary
	for _, note := range summary.Notes {
		if strings.Contains(note, "OOM kill") || strings.Contains(note, "memory above") || strings.HasPrefix(note, "warm ") {
			row.Reasons = append(row.Reasons, note)
		}
	}
	if summary.ActiveSeconds < duration.Seconds() {
		row.Reasons = append(row.Reasons, "measurement ended before the configured duration")
	}
	if summary.Offered == 0 || summary.Completed == 0 {
		row.Reasons = append(row.Reasons, "no valid completions")
	}
	if row.GenuineErrorRate > 0.005 {
		row.Reasons = append(row.Reasons, "genuine errors > 0.5%")
	}
	if summary.Latencies["accept"].P95 > 150 {
		row.Reasons = append(row.Reasons, "admission p95 > 150 ms")
	}
	if summary.Latencies["e2e_sse"].P95 > 8000 {
		row.Reasons = append(row.Reasons, "SSE end-to-end p95 > 8 s")
	}
	if row.MCPDuplicates != 0 || row.MCPUnauthorized != 0 || summary.Failure["mcp_effect_count"] > 0 {
		row.Reasons = append(row.Reasons, "MCP duplicates, unauthorized requests or effect count mismatch")
	}
	if summary.ColdOrNew != 0 {
		row.Reasons = append(row.Reasons, "warm execution unexpectedly relaunched")
	}
	if missingIntervals || row.Occupancy.ActiveSessions.N == 0 || row.Occupancy.ExecutingTurns.Max == 0 || row.Occupancy.OpenStreams.Max == 0 {
		row.Reasons = append(row.Reasons, "insufficient ACTIVE, executing-turn or SSE samples")
	}
	if summary.LastQueue["QUEUED"] > 0 || summary.LastQueue["ACTIVE"] > 0 || summary.LastQueue["FOLLOWUPS"] > 0 {
		row.Reasons = append(row.Reasons, "State not drained after stage")
	}
	if strings.HasPrefix(summary.Phase, "fcmax-") && row.Occupancy.PSSPerVM.N == 0 {
		row.Reasons = append(row.Reasons, "no complete Firecracker PSS sample at target density")
	}
	row.Accepted = len(row.Reasons) == 0
}

func longConversation(stage string, lane int) string {
	return fmt.Sprintf("%s-warm-%d", stage, lane)
}

type longWatch struct {
	handoff <-chan struct{}
	done    <-chan requestResult
}

func openLongTurn(ctx context.Context, s *stand, target int, conversation, delivery string, input turnInput, firecracker bool) longWatch {
	const timeout = 30 * time.Second
	turnCtx, cancel := context.WithTimeout(ctx, timeout)
	call := s.postTurn(turnCtx, target, conversation, delivery, input, 0)
	handoff := make(chan struct{})
	done := make(chan requestResult, 1)
	go func() {
		defer cancel()
		result := s.readTurnWithHandoff(turnCtx, call, firecracker, func() { close(handoff) })
		done <- result
	}()
	return longWatch{handoff: handoff, done: done}
}

func runLongLane(ctx context.Context, s *stand, c *collector, cfg sessionConfig, lane int, deadline int64, ordinal *atomic.Int64) {
	conversation := longConversation(c.phase, lane)
	target := lane % len(s.SubjectTokens)
	ttlMS := cfg.claimTTL.Milliseconds()
	firecracker := cfg.phase == "fcmax"
	input := turnInput{ID: conversation + "-boot", WorkerClaimTTLMS: ttlMS, HoldForFollowup: true}
	current := openLongTurn(ctx, s, target, conversation, journal.DeliveryTurn, input, firecracker)
	var lastExecution *turnOutput
	for {
		var result requestResult
		var next longWatch
		var nextInput turnInput
		select {
		case <-current.handoff:
			if input.HoldForFollowup && ctx.Err() == nil {
				nextInput = turnInput{ID: conversation + "-drain", WorkerClaimTTLMS: ttlMS}
				if monotonicNS() < deadline && !c.stop.Load() {
					n := ordinal.Add(1)
					nextInput = turnInput{
						ID: longTurnPrefix(c.phase) + strconv.FormatInt(n, 10), Padding: strings.Repeat("x", 4<<10),
						Tools: 2, ThinkMillis: 2200, MCPMillis: 50, Large: n%8 == 0,
						HoldForFollowup: true, WorkerClaimTTLMS: ttlMS,
					}
				}
				// The worker logs bench.delta after both tools settle, then waits
				// for a followup. Open SSE as soon as the successor is admitted.
				next = openLongTurn(ctx, s, target, conversation, "auto", nextInput, firecracker)
			}
			result = <-current.done
		case result = <-current.done:
		}
		if result.Executed != nil {
			reused := lastExecution != nil && lastExecution.ExecutionID == result.Executed.ExecutionID
			result.Reused = &reused
			lastExecution = result.Executed
		}
		if input.Tools > 0 {
			if result.Failure == "" && (result.Executed == nil || result.Executed.WorkerClaimTTLMS != ttlMS || result.Executed.NativeHistoryBytes != p5HistoryBytes) {
				result.Failure = "long_workload_ttl_or_native_history_mismatch"
			}
			c.add(result)
		} else if result.Failure != "" {
			if input.HoldForFollowup {
				result.Failure = "warm_boot/" + result.Failure
				c.add(result)
			} else {
				c.note("warm drain failed: " + result.Failure)
			}
		}
		if next.done == nil {
			if input.HoldForFollowup && monotonicNS() < deadline && !c.stop.Load() && ctx.Err() == nil {
				c.note("warm lane ended before the measurement deadline")
			}
			break
		}
		current, input = next, nextInput
	}
}

func longTurnPrefix(stage string) string {
	return stage + "-turn-"
}

func readLongIntervals(ctx context.Context, s *stand, stage string, lanes int) ([]journalInterval, error) {
	var tenants, sessions []string
	for lane := range lanes {
		scope := s.Scope(lane % len(s.SubjectTokens))
		tenants = append(tenants, scope.Tenant)
		sessions = append(sessions, journal.ScopedSessionID(scope, longConversation(stage, lane)))
	}
	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	rows, err := s.PG.Query(queryCtx, `
		SELECT b.tenant_id,b.session_id,b.turn_seq,
		       convert_from(b.inline_payload,'UTF8')::jsonb->>'id',b.created_at,t.created_at,
		       (convert_from(b.inline_payload,'UTF8')::jsonb->>'start_monotonic_ns')::bigint
        FROM unnest($1::text[],$2::text[]) AS lane(tenant_id,session_id)
        JOIN ahp_journal.session_events b USING (tenant_id,session_id)
        LEFT JOIN ahp_journal.session_events t
          ON t.tenant_id=b.tenant_id AND t.session_id=b.session_id AND t.event_uid='turn.completed:'||b.turn_seq
		WHERE b.event_type='bench.started' AND starts_with(convert_from(b.inline_payload,'UTF8')::jsonb->>'id', $3)
		ORDER BY b.created_at`, tenants, sessions, longTurnPrefix(stage))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	intervals := make([]journalInterval, 0)
	last := make(map[longTurnKey]int)
	for rows.Next() {
		var row journalInterval
		if err := rows.Scan(&row.Tenant, &row.Session, &row.TurnSeq, &row.ID, &row.Started, &row.Completed, &row.WorkerMonoNS); err != nil {
			return nil, err
		}
		key := longTurnKey{row.Session, row.TurnSeq}
		if i, ok := last[key]; ok {
			intervals[i].Superseded = true
		}
		last[key] = len(intervals)
		intervals = append(intervals, row)
	}
	return intervals, rows.Err()
}

type longTurnKey struct {
	session string
	seq     int64
}

func verifyLongIntervals(turns []requestResult, intervals []journalInterval) error {
	completed := make(map[longTurnKey]string, len(intervals))
	for _, interval := range intervals {
		if interval.Completed != nil && !interval.Superseded {
			completed[longTurnKey{interval.Session, interval.TurnSeq}] = interval.ID
		}
	}
	for _, turn := range turns {
		if turn.VerifiedAt > 0 && turn.Failure == "" && completed[longTurnKey{turn.SessionID, turn.Seq}] != turn.ID {
			return fmt.Errorf("verified turn %s has no matching journal interval", turn.ID)
		}
	}
	return nil
}

func joinLongCommitTrace(path string, intervals []journalInterval) error {
	trace, err := os.Open(path)
	if err != nil {
		return err
	}
	defer trace.Close()
	turns := make(map[longTurnKey]int, len(intervals))
	for i, interval := range intervals {
		if !interval.Superseded {
			turns[longTurnKey{interval.Session, interval.TurnSeq}] = i
		}
	}
	lines := bufio.NewScanner(trace)
	for lines.Scan() {
		fields := strings.Split(lines.Text(), "\t")
		if len(fields) != 4 {
			return fmt.Errorf("invalid State finish commit line %q", lines.Text())
		}
		seq, seqErr := strconv.ParseInt(fields[1], 10, 64)
		ts, timeErr := strconv.ParseInt(fields[3], 10, 64)
		if seqErr != nil || timeErr != nil || ts <= 0 {
			return fmt.Errorf("invalid State finish commit line %q", lines.Text())
		}
		if i, ok := turns[longTurnKey{fields[0], seq}]; ok && ts > intervals[i].WorkerMonoNS {
			if intervals[i].CommitMonoNS == 0 || ts < intervals[i].CommitMonoNS {
				intervals[i].CommitMonoNS = ts
			}
		}
	}
	return lines.Err()
}

func summarizeLongOccupancy(c *collector, intervals []journalInterval, firecracker, useCommitTrace bool, lanes int) longOccupancy {
	var active, executing, sse, guests, pss []int
	var missingTerminals int
	clock := "postgres_event_created_at"
	if useCommitTrace {
		clock = "linux_monotonic_worker_start_to_state_commit_ack"
	}
	for _, interval := range intervals {
		if interval.Completed == nil && !interval.Superseded {
			missingTerminals++
		}
	}
	for i := range c.samples {
		sample := &c.samples[i]
		if sample.MonotonicNS < c.started || sample.MonotonicNS >= c.stoppedAt {
			continue
		}
		guests = append(guests, sample.Guests.N)
		if firecracker && sample.Guests.N == lanes && sample.Guests.ObservedPSS == lanes {
			pss = append(pss, int(sample.Guests.PSS/int64(lanes)))
		}
		streams := 0
		for _, row := range c.rows {
			if row.SSEOpenedAt > 0 && row.SSEOpenedAt <= sample.MonotonicNS && row.SSEClosedAt > sample.MonotonicNS {
				streams++
			}
		}
		sse = append(sse, streams)
		sample.OpenSSE = &streams
		if sample.StateSampleAt == nil {
			continue
		}
		active = append(active, int(sample.Queue["ACTIVE"]))
		// The immutable journal bounds durably observed work. Missing
		// terminals stay explicit and cannot inflate the concurrent count.
		live := make(map[string]struct{})
		for _, interval := range intervals {
			if interval.Superseded || interval.Completed == nil {
				continue
			}
			exists := !interval.Started.After(*sample.StateSampleAt) && interval.Completed.After(*sample.StateSampleAt)
			if useCommitTrace {
				exists = interval.WorkerMonoNS <= sample.MonotonicNS && interval.CommitMonoNS > sample.MonotonicNS
			}
			if exists {
				live[interval.Tenant+"\x00"+interval.Session] = struct{}{}
			}
		}
		liveTurns := len(live)
		sample.ExecutingTurns = &liveTurns
		executing = append(executing, liveTurns)
	}
	return longOccupancy{
		ActiveSessions: observedCounts(active), ExecutingTurns: observedCounts(executing), OpenStreams: observedCounts(sse),
		GuestProcesses: observedCounts(guests), PSSPerVM: observedCounts(pss), MissingTerminals: missingTerminals, ExecutionClock: clock,
	}
}

func observedCounts(values []int) countDistribution {
	if len(values) == 0 {
		return countDistribution{}
	}
	sort.Ints(values)
	n := len(values)
	return countDistribution{N: n, P50: values[(n*50+99)/100-1], P95: values[(n*95+99)/100-1], Max: values[n-1]}
}
