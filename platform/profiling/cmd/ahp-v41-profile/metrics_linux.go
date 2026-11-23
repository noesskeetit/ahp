package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type distribution struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

func percentiles(ns []int64) distribution {
	if len(ns) == 0 {
		return distribution{}
	}
	sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
	at := func(p int) float64 { return float64(ns[(len(ns)*p+99)/100-1]) / 1e6 }
	return distribution{N: len(ns), P50: at(50), P95: at(95), P99: at(99), Max: at(100)}
}

type sample struct {
	MonotonicNS    int64                    `json:"mono_ns"`
	StateSampleAt  *time.Time               `json:"state_sample_at,omitempty"`
	OpenSSE        *int                     `json:"open_measured_sse,omitempty"`
	ExecutingTurns *int                     `json:"observed_executing_turns,omitempty"`
	Processes      map[string]processSample `json:"processes"`
	Cgroup         map[string]string        `json:"sut_cgroup"`
	Queue          map[string]int64         `json:"state_sessions"`
	DBWait         map[string]int64         `json:"db_wait"`
	DBWaitDetail   map[string]int64         `json:"db_wait_detail,omitempty"`
	NotifyLocks    *int64                   `json:"pg_notify_lock_waiters,omitempty"`
	Guests         guestSample              `json:"guest_processes"`
	GuestCgroup    map[string]string        `json:"worker_cgroup"`
}

type processSample struct {
	RSS         int64           `json:"rss_bytes"`
	FD          int             `json:"fd"`
	Heap        int64           `json:"heap_inuse"`
	Goroutines  int64           `json:"goroutines"`
	CPUClock    int64           `json:"cpu_ticks"`
	Pools       json.RawMessage `json:"pgx_pools,omitempty"`
	Connections json.RawMessage `json:"upstream_connections,omitempty"`
}

type collector struct {
	dir, phase string
	mu         sync.Mutex
	file       *os.File
	writer     *bufio.Writer
	enc        *json.Encoder
	rows       []requestResult
	samples    []sample
	notes      []string
	err        error
	started    int64
	stoppedAt  int64
	completed  atomic.Int64
	stop       atomic.Bool
}

func newCollector(dir, phase string) (*collector, error) {
	root := filepath.Join(dir, phase)
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(root, "turns.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	buf := bufio.NewWriterSize(f, 64<<10)
	return &collector{dir: root, phase: phase, file: f, writer: buf, enc: json.NewEncoder(buf), started: monotonicNS()}, nil
}

func (c *collector) add(row requestResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = append(c.rows, row)
	if row.Failure == "" && row.VerifiedAt > 0 {
		c.completed.Add(1)
	}
	if c.err == nil {
		c.err = c.enc.Encode(row)
		if len(c.rows)%128 == 0 && c.err == nil {
			c.err = c.writer.Flush()
		}
	}
	if c.err != nil {
		c.stop.Store(true)
	}
}

func (c *collector) note(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notes = append(c.notes, s)
}

type stageSummary struct {
	Phase                string                  `json:"phase"`
	ActiveSeconds        float64                 `json:"active_seconds"`
	Scheduled            int                     `json:"scheduled"`
	Offered              int                     `json:"sent"`
	Accepted             int                     `json:"accepted"`
	Completed            int                     `json:"completed"`
	CompletedDuringLoad  int                     `json:"completed_during_load"`
	Rate                 float64                 `json:"completed_per_s"`
	FaultsScheduled      int                     `json:"faults_scheduled"`
	FaultsRecovered      int                     `json:"faults_completed"`
	Warm                 int                     `json:"warm_reused"`
	ColdOrNew            int                     `json:"new_execution"`
	Recovered            int                     `json:"new_epoch"`
	Large                int                     `json:"large_completed"`
	Effects              int                     `json:"mcp_effects_on_valid_paths"`
	Overlap              int                     `json:"start_preceded_post_ack"`
	Failure              map[string]int          `json:"failures"`
	WorstNormalErrorRate float64                 `json:"worst_normal_error_fraction"`
	NormalErrors         int                     `json:"normal_errors"`
	NormalAttempts       int                     `json:"normal_attempts"`
	Latencies            map[string]distribution `json:"latency"`
	Processes            map[string]procSummary  `json:"processes"`
	LastQueue            map[string]int64        `json:"last_state_sessions"`
	Notes                []string                `json:"notes"`
}

type procSummary struct {
	Initial          processSample `json:"initial"`
	Final            processSample `json:"final"`
	MaxRSS           int64         `json:"peak_rss_bytes"`
	RSSMiBPerMinute  *float64      `json:"rss_mib_per_minute"`
	HeapMiBPerMinute *float64      `json:"heap_mib_per_minute"`
}

func (c *collector) finish() (stageSummary, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	end := c.stoppedAt
	if end <= c.started {
		end = monotonicNS()
	}
	out := stageSummary{
		Phase: c.phase, ActiveSeconds: float64(end-c.started) / 1e9, Scheduled: 0,
		Failure: make(map[string]int), Latencies: make(map[string]distribution), Processes: make(map[string]procSummary),
		Notes: c.notes,
	}
	durations := make(map[string][]int64)
	var measured []requestResult
	for _, r := range c.rows {
		if r.SentAt != 0 && r.SentAt < c.started {
			continue
		}
		measured = append(measured, r)
		out.Scheduled++
		if r.Fault {
			out.FaultsScheduled++
		} else {
			out.NormalAttempts++
		}
		if r.SentAt > 0 {
			out.Offered++
		}
		if r.AcceptedAt > 0 {
			out.Accepted++
		}
		if r.Failure != "" {
			out.Failure[r.Failure]++
			if !r.Fault {
				out.NormalErrors++
			}
		}
		// Successful phase timestamps remain visible when a later phase
		// (for example processing or fencing) failed.
		for name, span := range map[string][2]int64{
			"accept": {r.SentAt, r.AcceptedAt}, "schedule_delay": {r.RequestedAt, r.SentAt},
			"send_to_worker": {r.SentAt, r.ClaimedAt}, "first_event": {r.SentAt, r.FirstAt},
			"e2e_sse": {r.SentAt, r.CompletedAt}, "e2e_durable": {r.SentAt, r.VerifiedAt},
		} {
			if span[0] > 0 && span[1] >= span[0] {
				durations[name] = append(durations[name], span[1]-span[0])
			}
		}
		if r.ClaimedAt > 0 && r.AcceptedAt > 0 {
			if r.ClaimedAt < r.AcceptedAt {
				out.Overlap++
			}
			durations["post_ack_to_worker"] = append(durations["post_ack_to_worker"], max(0, r.ClaimedAt-r.AcceptedAt))
		}
		if r.BootNS > 0 {
			durations["vmm_bootstrap"] = append(durations["vmm_bootstrap"], r.BootNS)
		}
		if r.WarmHandoffNS > 0 {
			durations["warm_handoff_observed"] = append(durations["warm_handoff_observed"], r.WarmHandoffNS)
		}
		if r.PriorFinishNS > 0 {
			durations["previous_finish_request"] = append(durations["previous_finish_request"], r.PriorFinishNS)
		}
		if r.FenceNS > 0 && r.FenceTimeError == "" {
			durations["fence"] = append(durations["fence"], r.FenceNS)
		}
		if r.Failure != "" || r.VerifiedAt == 0 {
			continue
		}
		if r.Executed != nil {
			durations["guest_settle_tool"] = append(durations["guest_settle_tool"], r.Executed.GuestToolNS...)
			if r.Executed.Epoch > 1 {
				out.Recovered++
			}
		}
		if r.Fault {
			out.FaultsRecovered++
		}
		if r.Reused != nil {
			if *r.Reused {
				out.Warm++
			} else {
				out.ColdOrNew++
			}
		} else if r.Executed != nil && r.Seq == r.Executed.LaunchTurnSeq {
			out.ColdOrNew++
		}
		if r.Large {
			out.Large++
		}
		out.Effects += r.Effects
		out.Completed++
		if r.VerifiedAt <= end {
			out.CompletedDuringLoad++
		}
	}
	out.Rate = float64(out.CompletedDuringLoad) / out.ActiveSeconds
	out.WorstNormalErrorRate = windowErrors(measured)
	for k, v := range durations {
		out.Latencies[k] = percentiles(v)
	}
	out.Processes = summarizeSamples(c.samples)
	if len(c.samples) > 0 {
		out.LastQueue = c.samples[len(c.samples)-1].Queue
	}
	if c.err != nil {
		return out, c.err
	}
	if err := c.writer.Flush(); err != nil {
		return out, err
	}
	if err := c.file.Close(); err != nil {
		return out, err
	}
	if err := writeJSONFile(filepath.Join(c.dir, "resources.json"), c.samples); err != nil {
		return out, err
	}
	return out, writeJSONFile(filepath.Join(c.dir, "summary.json"), out)
}

func windowErrors(rows []requestResult) float64 {
	eligible := make([]requestResult, 0, len(rows))
	for _, r := range rows {
		if !r.Fault && r.SentAt > 0 {
			eligible = append(eligible, r)
		}
	}
	if len(eligible) == 0 {
		return 0
	}
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].SentAt < eligible[j].SentAt })
	var worst float64
	errors, left := 0, 0
	for right, r := range eligible {
		if r.Failure != "" {
			errors++
		}
		for r.SentAt-eligible[left].SentAt > int64(60*time.Second) {
			if eligible[left].Failure != "" {
				errors--
			}
			left++
		}
		if r.SentAt-eligible[0].SentAt >= int64(60*time.Second) {
			worst = max(worst, float64(errors)/float64(right-left+1))
		}
	}
	return worst
}

func summarizeSamples(samples []sample) map[string]procSummary {
	out := make(map[string]procSummary)
	if len(samples) == 0 {
		return out
	}
	first, last := samples[0], samples[len(samples)-1]
	for role, initial := range first.Processes {
		info := procSummary{Initial: initial, Final: last.Processes[role]}
		for _, s := range samples {
			info.MaxRSS = max(info.MaxRSS, s.Processes[role].RSS)
		}
		info.RSSMiBPerMinute = slope(samples, role, func(x processSample) int64 { return x.RSS })
		info.HeapMiBPerMinute = slope(samples, role, func(x processSample) int64 { return x.Heap })
		out[role] = info
	}
	return out
}

func slope(samples []sample, role string, field func(processSample) int64) *float64 {
	if len(samples) < 3 {
		return nil
	}
	zero := samples[0].MonotonicNS
	cutoff := samples[len(samples)-1].MonotonicNS - int64(10*time.Minute)
	var n, sx, sy, sxx, sxy float64
	for _, s := range samples {
		if s.MonotonicNS < cutoff {
			continue
		}
		y := field(s.Processes[role])
		if y <= 0 {
			continue
		}
		x := float64(s.MonotonicNS-zero) / float64(time.Minute)
		yy := float64(y) / (1 << 20)
		n++
		sx += x
		sy += yy
		sxx += x * x
		sxy += x * yy
	}
	den := n*sxx - sx*sx
	if n < 3 || den <= 0 {
		return nil
	}
	result := (n*sxy - sx*sy) / den
	return &result
}

func writeJSONFile(path string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(body, '\n'), 0600)
}

func readProc(pid int) processSample {
	root := fmt.Sprintf("/proc/%d", pid)
	out := processSample{}
	if s, err := os.ReadFile(root + "/status"); err == nil {
		for line := range strings.SplitSeq(string(s), "\n") {
			parts := strings.Fields(line)
			if len(parts) >= 2 && parts[0] == "VmRSS:" {
				n, _ := strconv.ParseInt(parts[1], 10, 64)
				out.RSS = n * 1024
			}
		}
	}
	if entries, e := os.ReadDir(root + "/fd"); e == nil {
		out.FD = len(entries)
	}
	if b, err := os.ReadFile(root + "/stat"); err == nil {
		if end := strings.LastIndexByte(string(b), ')'); end >= 0 {
			fields := strings.Fields(string(b[end+1:]))
			if len(fields) >= 13 {
				u, _ := strconv.ParseInt(fields[11], 10, 64)
				v, _ := strconv.ParseInt(fields[12], 10, 64)
				out.CPUClock = u + v
			}
		}
	}
	return out
}

func sampleResources(ctx context.Context, s *stand, c *collector, cgroup string) func() {
	sampleCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	capture := func(sampleCtx context.Context) {
		a := sample{MonotonicNS: monotonicNS(), Processes: map[string]processSample{}, Cgroup: map[string]string{}, Queue: map[string]int64{}, DBWait: map[string]int64{}}
		for role, pid := range s.PIDs {
			info := readProc(pid)
			if origin := s.DebugURLs[role]; origin != "" {
				getStats(sampleCtx, origin, &info)
			}
			a.Processes[role] = info
		}
		for _, file := range []string{"cpu.stat", "memory.current", "memory.peak", "memory.events", "memory.swap.current", "pids.current"} {
			if b, err := os.ReadFile(filepath.Join(cgroup, file)); err == nil {
				a.Cgroup[file] = strings.TrimSpace(string(b))
			}
		}
		a.Guests = readGuestResources(s)
		a.GuestCgroup = map[string]string{}
		guestPath := s.cfg.GuestCgroup
		if s.cfg.Driver == "firecracker" {
			guestPath = s.FirecrackerCgroup
		}
		for _, field := range []string{"cpu.stat", "memory.current", "memory.peak", "memory.events", "pids.current"} {
			if data, e := os.ReadFile(filepath.Join(guestPath, field)); e == nil {
				a.GuestCgroup[field] = strings.TrimSpace(string(data))
			}
		}
		statCtx, stop := context.WithTimeout(sampleCtx, 2*time.Second)
		defer stop()
		if rows, err := s.PG.Query(statCtx, "SELECT status, count(*), statement_timestamp() FROM ahp_journal.sessions GROUP BY status"); err == nil {
			var sampledAt time.Time
			for rows.Next() {
				var key string
				var n int64
				if rows.Scan(&key, &n, &sampledAt) == nil {
					a.Queue[key] = n
				}
			}
			if rows.Err() == nil && !sampledAt.IsZero() {
				a.StateSampleAt = &sampledAt
			}
			rows.Close()
		}
		var followups int64
		if s.PG.QueryRow(statCtx, "SELECT coalesce(sum(followup_count),0) FROM ahp_journal.sessions WHERE status IN ('ACTIVE','QUEUED')").Scan(&followups) == nil {
			a.Queue["FOLLOWUPS"] = followups
		}

		if rows, err := s.PG.Query(statCtx, `SELECT COALESCE(wait_event_type,'active'),
            COALESCE(wait_event,'running'), count(*) FROM pg_stat_activity
            WHERE state <> 'idle' AND pid <> pg_backend_pid() GROUP BY 1,2`); err == nil {
			a.DBWaitDetail = make(map[string]int64)
			for rows.Next() {
				var kind, event string
				var n int64
				if rows.Scan(&kind, &event, &n) == nil {
					a.DBWait[kind] += n
					a.DBWaitDetail[kind+":"+event] = n
				}
			}
			rows.Close()
		}
		// PostgreSQL 18.7 PreCommit_Notify takes this cluster-wide
		// database-object lock until the notifying transaction ends.
		var notifyWaiters int64
		if s.PG.QueryRow(statCtx, `SELECT count(*) FROM pg_locks
            WHERE NOT granted AND locktype='object' AND mode='AccessExclusiveLock'
              AND database=0 AND classid='pg_database'::regclass AND objid=0
              AND objsubid=0`).Scan(&notifyWaiters) == nil {
			a.NotifyLocks = &notifyWaiters
		}
		c.mu.Lock()
		if len(c.samples) > 0 {
			for name, conf := range map[string][2]string{
				"SUT":    {c.samples[0].Cgroup["memory.events"], a.Cgroup["memory.events"]},
				"worker": {c.samples[0].GuestCgroup["memory.events"], a.GuestCgroup["memory.events"]},
			} {
				before, after := kvCounter(conf[0], "oom_kill"), kvCounter(conf[1], "oom_kill")
				if after > before && !c.stop.Swap(true) {
					c.notes = append(c.notes, name+" cgroup OOM kill during stage")
				}
			}
		}
		c.samples = append(c.samples, a)
		if len(c.samples) > 1 && a.MonotonicNS-c.started >= int64(60*time.Second) &&
			(strings.HasPrefix(c.phase, "p1-") || strings.HasPrefix(c.phase, "p2-") || strings.HasPrefix(c.phase, "p3-")) {
			var eligible, failed int
			var admits []int64
			for _, row := range c.rows {
				if row.SentAt < a.MonotonicNS-int64(60*time.Second) {
					continue
				}
				eligible++
				if row.Failure != "" {
					failed++
				}
				if row.AcceptedAt > 0 {
					admits = append(admits, row.AcceptedAt-row.SentAt)
				}
			}
			if eligible > 0 && (float64(failed)/float64(eligible) > .01 || percentiles(admits).P95 > 150) {
				c.stop.Store(true)
				c.notes = append(c.notes, "60-second step exceeded error or admission threshold")
			}
		}
		c.mu.Unlock()
		if n, err := strconv.ParseInt(a.Cgroup["memory.current"], 10, 64); err == nil && n > int64(3900<<20) {
			c.stop.Store(true)
			c.note("SUT memory above 3900 MiB")
		}
	}
	// Put an initial sample in the record before concurrent load.
	capture(sampleCtx)
	go func() {
		defer close(done)
		interval := 5 * time.Second
		if strings.HasPrefix(c.phase, "fc2-") || strings.HasPrefix(c.phase, "fcmax-") {
			interval = time.Second
		}
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-sampleCtx.Done():
				return
			case <-tick.C:
				capture(sampleCtx)
			}
		}
	}()
	return sync.OnceFunc(func() {
		cancel()
		<-done
		final, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		capture(final)
	})
}

func getStats(ctx context.Context, base string, info *processSample) {
	getCtx, stop := context.WithTimeout(ctx, 400*time.Millisecond)
	defer stop()
	req, err := http.NewRequestWithContext(getCtx, http.MethodGet, base+"/debug/ahp-profile/stats", nil)
	if err != nil {
		return
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	var state struct {
		Heap        int64           `json:"heap_inuse"`
		Goroutines  int64           `json:"goroutines"`
		Pools       json.RawMessage `json:"pools"`
		Connections json.RawMessage `json:"upstream_connections"`
	}
	if res.StatusCode == 200 && json.NewDecoder(io.LimitReader(res.Body, 8192)).Decode(&state) == nil {
		info.Heap, info.Goroutines = state.Heap, state.Goroutines
		info.Pools, info.Connections = state.Pools, state.Connections
	}
}

func captureProfiles(ctx context.Context, s *stand, dir string, duration time.Duration) func() {
	var profiles sync.WaitGroup
	for role, base := range s.DebugURLs {
		if base == "" {
			continue
		}
		profiles.Go(func() {
			for _, kind := range []string{"profile", "mutex", "allocs"} {
				suffix := ""
				if kind == "profile" {
					suffix = "?seconds=" + strconv.Itoa(max(1, int(duration/time.Second)))
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/debug/pprof/"+kind+suffix, nil)
				if err != nil {
					return
				}
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					continue
				}
				body, err := io.ReadAll(io.LimitReader(res.Body, 64<<20))
				_ = res.Body.Close()
				if res.StatusCode == 200 && err == nil {
					_ = os.WriteFile(filepath.Join(dir, role+"-"+kind+".pprof"), body, 0600)
				}
			}
		})
	}
	return profiles.Wait
}

// Kernel cgroup counters are whitespace-delimited key/value lines.
func kvCounter(raw, key string) int64 {
	for line := range strings.SplitSeq(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == key {
			v, _ := strconv.ParseInt(fields[1], 10, 64)
			return v
		}
	}
	return 0
}
