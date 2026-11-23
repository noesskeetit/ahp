package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

var overhead = profile{tools: 2, mcpMS: 1, warm: true}
var realistic = profile{tools: 2, mcpMS: 20, thinkMS: 80, largeEvery: 8, warm: true}

type benchStep struct {
	Summary     stageSummary `json:"summary"`
	Concurrency int          `json:"concurrency"`
	Accepted    bool         `json:"step_qualified"`
	Reasons     []string     `json:"cutoff_reasons,omitempty"`
}

func runProcessProfiles(ctx context.Context, s *stand, cfg sessionConfig, cgroup string) error {
	phases := []string{cfg.phase}
	if cfg.phase == "all-process" {
		phases = []string{"p0", "p1", "p2", "p3", "p4"}
	}
	var rows []benchStep
	var peak float64
	var peakLanes int
	for _, phase := range phases {
		if phase == "p4" {
			rate := cfg.rate
			if rate <= 0 {
				rate = peak
			}
			if rate <= 0 {
				return errors.New("P2 found no healthy steady step to set a P4 rate")
			}
			if peakLanes == 0 {
				peakLanes = min(32, cfg.maxLanes)
			}
			if err := runSoak(ctx, s, cfg, rate, peakLanes, cgroup); err != nil {
				return err
			}
			continue
		}
		var ladder []int
		var work profile
		var duration time.Duration
		switch phase {
		case "p0":
			ladder, work, duration = []int{32}, overhead, 60*time.Second
		case "p1":
			ladder, work, duration = []int{1, 8, 16, 32, 64, 128}, overhead, 120*time.Second
		case "p2":
			ladder, work, duration = []int{8, 16, 32, 64, 128, 192}, realistic, 180*time.Second
			if cfg.maxLanes < 8 {
				ladder = []int{1, 2, 4}
			}
		case "p3":
			ladder, work, duration = []int{1, 8, 16, 32, 64}, profile{tools: 1, mcpMS: 10}, 120*time.Second
		}
		for _, n := range ladder {
			if n > cfg.maxLanes {
				if phase == "p0" {
					n = cfg.maxLanes
				} else {
					break
				}
			}
			step := closedStep{name: fmt.Sprintf("%s-c%d", phase, n), work: work, lanes: n, duration: duration}
			if phase == "p1" && cfg.quick == 0 {
				step.minTurns = 500
			}
			summary, err := runClosedStage(ctx, s, cfg, step, phase == "p0")
			if err != nil {
				return err
			}
			row := benchStep{Summary: summary, Concurrency: n, Accepted: true}
			for _, note := range summary.Notes {
				if strings.Contains(note, "OOM kill") || strings.Contains(note, "memory above") {
					row.Reasons = append(row.Reasons, note)
				}
			}
			if summary.Offered == 0 || summary.Completed == 0 {
				row.Reasons = append(row.Reasons, "no valid completions")
			}
			if summary.Completed < step.minTurns {
				row.Reasons = append(row.Reasons, "minimum completions missed")
			}
			if summary.WorstNormalErrorRate > .01 ||
				(summary.NormalAttempts > 0 && float64(summary.NormalErrors)/float64(summary.NormalAttempts) > .01) {
				row.Reasons = append(row.Reasons, "normal errors > 1%")
			}
			if summary.Latencies["accept"].P95 > 150 {
				row.Reasons = append(row.Reasons, "admission p95 > 150 ms")
			}
			if work.warm && summary.ColdOrNew > 0 {
				row.Reasons = append(row.Reasons, "warm execution unexpectedly relaunched")
			}
			if summary.Failure["mcp_effect_count"] > 0 {
				row.Reasons = append(row.Reasons, "MCP exact effect count violated")
			}
			if summary.LastQueue["QUEUED"] > 0 || summary.LastQueue["ACTIVE"] > 0 {
				row.Reasons = append(row.Reasons, "State not drained after stage")
			}
			row.Accepted = len(row.Reasons) == 0
			rows = append(rows, row)
			if err := writeJSONFile(filepath.Join(cfg.out, "process-ladder.json"), rows); err != nil {
				return err
			}
			if phase == "p2" && row.Accepted && summary.Rate > peak {
				peak, peakLanes = summary.Rate, n
			}
			if !row.Accepted && phase != "p0" {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
	return nil
}

func runClosedStage(ctx context.Context, s *stand, cfg sessionConfig, step closedStep, calibrate bool) (stageSummary, error) {
	if cfg.quick > 0 && step.countPerLane == 0 {
		step.duration = cfg.quick
		step.minTurns = 0
	}
	if step.work.warm && cfg.phase != "smoke" {
		for i := 0; i < step.lanes; i++ {
			if err := s.seedLane(ctx, i%len(s.SubjectTokens), fmt.Sprintf("%s-warm-%d", step.name, i)); err != nil {
				return stageSummary{}, err
			}
		}
	}
	collect, err := newCollector(cfg.out, step.name)
	if err != nil {
		return stageSummary{}, err
	}
	var dbBefore map[string]pgCounters
	var pgError error
	if calibrate {
		step.warmup = 20 * time.Second
		if cfg.quick > 0 {
			step.warmup = min(2*time.Second, cfg.quick/3)
		}
	} else {
		dbBefore, pgError = snapshotPG(ctx, s)
	}
	collect.started = monotonicNS() + int64(step.warmup)
	stopSampling := sampleResources(ctx, s, collect, envDefault("AHP_PROFILE_SUT_CGROUP", ""))
	profilesDone := make(chan struct{})
	if calibrate {
		warmup, measured := step.warmup, step.duration
		go func() {
			defer close(profilesDone)
			select {
			case <-time.After(warmup):
			case <-ctx.Done():
				return
			}
			dbBefore, pgError = snapshotPG(ctx, s)
			captureProfiles(ctx, s, collect.dir, measured)()
		}()
	} else {
		close(profilesDone)
	}
	step.duration += step.warmup
	fmt.Printf("AHP_PROFILE_STAGE_START %s lanes=%d duration=%s count_per_lane=%d\n", step.name, step.lanes, step.duration, step.countPerLane)
	closedLoop(ctx, s, step, collect)
	collect.stoppedAt = monotonicNS()
	<-profilesDone
	stopSampling()
	dbAfter, pgAfterError := snapshotPG(ctx, s)
	if pgError == nil && pgAfterError == nil {
		_ = writeJSONFile(filepath.Join(collect.dir, "pg-statements.json"), map[string]any{"before": dbBefore, "after": dbAfter})
	} else {
		collect.note("pg_stat_statements unavailable")
	}
	summary, err := collect.finish()
	fmt.Printf("AHP_PROFILE_STAGE %s sent=%d completed=%d rate=%.3f p95_e2e_ms=%.3f errors=%v\n",
		step.name, summary.Offered, summary.Completed, summary.Rate, summary.Latencies["e2e_sse"].P95, summary.Failure)
	return summary, err
}

func runFirecracker(ctx context.Context, s *stand, cfg sessionConfig) error {
	cold := profile{tools: 1, mcpMS: 1, firecracker: true}
	warm := profile{tools: 2, mcpMS: 1, firecracker: true, warm: true, largeEvery: 8}
	var steps []closedStep
	if cfg.phase == "all-fc" || cfg.phase == "fc1" {
		paired := warm
		paired.tools, paired.largeEvery = 1, 0
		steps = append(steps,
			closedStep{name: "fc1-cold-c1", work: cold, lanes: 1, countPerLane: 30},
			closedStep{name: "fc1-cold-c4", work: cold, lanes: 4, countPerLane: 8},
			closedStep{name: "fc1-warm-two-tools", work: warm, lanes: 1, countPerLane: 100, expiryProbe: true},
			closedStep{name: "fc1-warm-one-tool", work: paired, lanes: 1, countPerLane: 30},
		)
	}
	if cfg.phase == "all-fc" || cfg.phase == "fc2" {
		for _, n := range []int{1, 4, 8, 16} {
			if n > cfg.maxLanes {
				break
			}
			if cfg.fcLanes != 0 && cfg.fcLanes != n {
				continue
			}
			fault := warm
			fault.timeout = 95 * time.Second
			step := closedStep{name: fmt.Sprintf("fc2-m%d", n), work: fault, lanes: n, countPerLane: 10}
			if cfg.injectFaults {
				step.faultAt = step.countPerLane
			}
			steps = append(steps, step)
		}
	}
	for _, step := range steps {
		if cfg.quick > 0 {
			step.countPerLane = min(step.countPerLane, 3)
			step.faultAt = 0
		}
		summary, err := runClosedStage(ctx, s, cfg, step, false)
		if err != nil {
			return err
		}
		if summary.Completed != step.lanes*step.countPerLane {
			return fmt.Errorf("%s completed %d/%d turns", step.name, summary.Completed, step.lanes*step.countPerLane)
		}
	}
	return nil
}
