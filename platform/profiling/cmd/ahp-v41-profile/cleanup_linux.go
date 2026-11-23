package main

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type guestSample struct {
	N           int   `json:"pids"`
	RSS         int64 `json:"rss_bytes"`
	PSS         int64 `json:"pss_bytes"`
	ObservedPSS int   `json:"pss_samples"`
}

func readGuestResources(s *stand) guestSample {
	dir := s.cfg.GuestCgroup
	if s.cfg.Driver == "firecracker" {
		dir = s.FirecrackerCgroup
	}
	if dir == "" {
		return guestSample{}
	}
	observed := map[int]bool{}
	var out guestSample
	_ = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || !entry.IsDir() {
			return nil
		}
		body, e := os.ReadFile(filepath.Join(path, "cgroup.procs"))
		if e != nil {
			return nil
		}
		for _, field := range strings.Fields(string(body)) {
			pid, e := strconv.Atoi(field)
			if e != nil || observed[pid] {
				continue
			}
			observed[pid] = true
			out.N++
			out.RSS += readProc(pid).RSS
			if text, e := os.ReadFile(filepath.Join("/proc", field, "smaps_rollup")); e == nil {
				for line := range strings.SplitSeq(string(text), "\n") {
					line = strings.TrimSpace(line)
					if !strings.HasPrefix(line, "Pss:") {
						continue
					}
					vals := strings.Fields(line)
					if len(vals) < 2 {
						continue
					}
					kib, e := strconv.ParseInt(vals[1], 10, 64)
					if e == nil {
						out.PSS += 1024 * kib
						out.ObservedPSS++
					}
					break
				}
			}
		}
		return nil
	})
	return out
}

type soakCleanupEvidence struct {
	StateBeforeFence   *soakStateJobs     `json:"state_before_final_fence"`
	FinalState         *soakStateJobs     `json:"final_state"`
	StateDrained       bool               `json:"state_drain_confirmed"`
	FinalFence         *runtimeFenceBatch `json:"final_runtime_fence"`
	RuntimeStatuses    map[string]int64   `json:"runtime_execution_statuses"`
	PendingToolIntents []toolIntentStatus `json:"pending_tool_intent_gateway_statuses"`
	ObservationErrors  []string           `json:"observation_errors"`
}

func (s *stand) collectSoakCleanup(ctx context.Context, before *soakStateJobs, drainErr error) *soakCleanupEvidence {
	e := &soakCleanupEvidence{StateBeforeFence: before, StateDrained: drainErr == nil, ObservationErrors: []string{}}
	if drainErr == nil {
		batch, err := s.closeRunningGuests(ctx)
		e.FinalFence = &batch
		if err != nil {
			e.ObservationErrors = append(e.ObservationErrors, "final_runtime_fence_failed")
		}
	}
	probe, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	jobs, err := s.soakStateJobs(probe)
	if err != nil {
		e.ObservationErrors = append(e.ObservationErrors, "final_state_jobs_unobserved")
	} else {
		e.FinalState = &jobs
	}
	e.RuntimeStatuses, err = s.soakRuntimeStatuses(probe)
	if err != nil {
		e.ObservationErrors = append(e.ObservationErrors, "final_runtime_statuses_unobserved")
	}
	e.PendingToolIntents, err = s.pendingToolIntentStatuses(probe)
	if err != nil {
		e.ObservationErrors = append(e.ObservationErrors, "pending_tool_intent_statuses_unobserved")
	}
	return e
}

func (e *soakCleanupEvidence) qualificationFailures(coldFenceFailures int) []string {
	var failures []string
	if !e.StateDrained || e.FinalState == nil || !e.FinalState.empty() {
		failures = append(failures, "nonempty or unobserved State jobs after drain")
	}
	if len(e.PendingToolIntents) != 0 {
		failures = append(failures, "unresolved State tool intents after drain")
	}
	failures = append(failures, e.ObservationErrors...)
	if e.FinalFence == nil || e.RuntimeStatuses == nil {
		failures = append(failures, "final Runtime fence unobserved")
	} else if e.FinalFence.Succeeded != e.FinalFence.Found {
		failures = append(failures, "Runtime fence batch incomplete")
	}
	for status, count := range e.RuntimeStatuses {
		if status != "FENCED" && count > 0 {
			failures = append(failures, "unfenced Runtime executions after final fence")
			break
		}
	}
	if coldFenceFailures > 0 {
		failures = append(failures, "cold execution fence failed")
	}
	return failures
}

func resourceCleanup(s *stand) map[string]any {
	counts := map[string]any{
		"state": finalLiveState(s),
		"guest": readGuestResources(s),
	}
	if s.soakCleanup != nil {
		counts["p4"] = s.soakCleanup
		if jobs := s.soakCleanup.FinalState; jobs != nil {
			state := counts["state"].(map[string]int64)
			state["followups"] = jobs.Followups
			state["unpromoted_followup_events"] = jobs.FollowupEvents
		}
	}
	var sockets, tombstones, locks, populated int
	var tombstoneBytes int64
	for _, root := range []string{s.SlotDir, s.FCDataDir, s.FirecrackerCgroup} {
		if root == "" {
			continue
		}
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() && root == s.FirecrackerCgroup {
				if b, e := os.ReadFile(filepath.Join(path, "cgroup.events")); e == nil && strings.Contains(string(b), "populated 1") {
					populated++
				}
			} else if d.Type()&os.ModeSocket != 0 {
				sockets++
			} else if filepath.Ext(path) == ".json" && filepath.Dir(path) == s.SlotDir {
				b, e := os.ReadFile(path)
				if e != nil {
					return nil
				}
				var slot struct{ Fenced bool }
				if json.Unmarshal(b, &slot) == nil {
					if slot.Fenced {
						tombstones++
						tombstoneBytes += int64(len(b))
					}
				}
			} else if filepath.Ext(path) == ".lock" && filepath.Dir(path) == s.SlotDir {
				locks++
			}
			return nil
		})
	}
	counts["socket_entries"] = sockets
	counts["populated_cgroups"] = populated
	counts["fenced_tombstones"] = tombstones
	counts["fenced_tombstone_bytes"] = tombstoneBytes
	counts["retained_slot_locks"] = locks
	return counts
}
