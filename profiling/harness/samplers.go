package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Sample is one point-in-time observation across platform and dependencies.
type Sample struct {
	AtUTC string `json:"at_utc"`
	// Platform process (external, via ps). ProcPID names the process this
	// observation belongs to: without it an RSS jump cannot be told apart
	// from the sampler having observed a different process.
	// Every metric below is a POINTER (metrics_contract.go, DEF-PROF-3): nil
	// means "this collector answered nothing on this tick" and serializes as
	// JSON null / prints "not_measured" in CSV. A 0 states that the value was
	// observed and was zero -- which is exactly what the density smoke got
	// wrong ("0 MiB" memory accounting over 17 samples, indistinguishable from
	// an idle process).
	ProcCPUSec *float64 `json:"proc_cpu_sec"`
	ProcRSS    *uint64  `json:"proc_rss_bytes"`
	ProcPID    *int     `json:"proc_pid"`
	// ProcIdentityMismatch marks an observation whose pid is not the one this
	// stream pinned. The values are kept (they are real observations) but the
	// step that contains them is no longer attributable to one process.
	ProcIdentityMismatch bool `json:"proc_identity_mismatch,omitempty"`
	// Platform runtime (via /debug/stats).
	Goroutines *int    `json:"goroutines"`
	HeapAlloc  *uint64 `json:"heap_alloc_bytes"`
	SysBytes   *uint64 `json:"sys_bytes"`
	StatsRSS   *uint64 `json:"stats_rss_bytes"`
	// PostgreSQL cumulative counters (pg_stat_database, lane database).
	PGBlksRead       *int64 `json:"pg_blks_read"`
	PGBlksHit        *int64 `json:"pg_blks_hit"`
	PGTupFetched     *int64 `json:"pg_tup_fetched"`
	PGTupReturned    *int64 `json:"pg_tup_returned"`
	PGxactCommit     *int64 `json:"pg_xact_commit"`
	PGxactRollback   *int64 `json:"pg_xact_rollback"`
	PGTempFiles      *int64 `json:"pg_temp_files"`
	PGTempBytes      *int64 `json:"pg_temp_bytes"`
	PGDeadlocks      *int64 `json:"pg_deadlocks"`
	PGActiveBackends *int64 `json:"pg_active_backends"`
	PGDBSizeBytes    *int64 `json:"pg_db_size_bytes"`
	// Redis INFO fields.
	RedisUsedMemory       *int64 `json:"redis_used_memory"`
	RedisConnectedClients *int64 `json:"redis_connected_clients"`
	RedisTotalCommands    *int64 `json:"redis_total_commands_processed"`
	RedisInstantaneousOPS *int64 `json:"redis_instantaneous_ops_per_sec"`
	RedisEvokedKeys       *int64 `json:"redis_evicted_keys"`
	RedisRejectedConn     *int64 `json:"redis_rejected_connections"`
	RedisNetInputBytes    *int64 `json:"redis_net_input_bytes"`
	RedisNetOutputBytes   *int64 `json:"redis_net_output_bytes"`
	// Container block/network IO via docker stats (cumulative bytes).
	PGContainerBlockRead  *int64 `json:"pg_container_block_read_bytes"`
	PGContainerBlockWrite *int64 `json:"pg_container_block_write_bytes"`
	PGContainerNetRx      *int64 `json:"pg_container_net_rx_bytes"`
	PGContainerNetTx      *int64 `json:"pg_container_net_tx_bytes"`
	MinioBlockRead        *int64 `json:"minio_block_read_bytes"`
	MinioBlockWrite       *int64 `json:"minio_block_write_bytes"`
	MinioHealthOK         *bool  `json:"minio_health_ok"`
	// DockerSnapshotAgeMS: container columns on this sample came from a
	// background refresh this many ms ago. Honest staleness disclosure
	// instead of pretending every sample saw docker; null = no snapshot.
	DockerSnapshotAgeMS *int64 `json:"docker_snapshot_age_ms"`
	// ObsMS is how long this observation took end to end. Recorded so a
	// report can prove the stream cadence, not just claim it.
	ObsMS int64 `json:"obs_ms"`
	// Unobserved names every collector that produced NO value on this tick,
	// as "source:reason" ("ps:not_configured", "redis:dial tcp ...",
	// "pg:partial:pg_database_size: ..."). Nulls are honest only when the
	// reader can see which collector caused them.
	Unobserved []string `json:"unobserved,omitempty"`
	// Errors encountered while sampling (samplers never fail a measurement).
	Errors []string `json:"errors,omitempty"`
}

// SamplerConfig names the sources to poll.
type SamplerConfig struct {
	Platform       *Platform
	Pool           *pgxpool.Pool // lane database
	RedisAddr      string        // 127.0.0.1:26379
	MinioHealth    string        // http://127.0.0.1:29000/minio/health/live
	PGContainer    string        // ahp-handbook-acceptance-pg
	MinioContainer string        // ahp-handbook-go-minio-1
	Interval       time.Duration
	DockerEvery    int // sample docker stats every N-th tick (it costs ~1s)
	// ObservationDelay artificially slows each observation (test seam): the
	// stamping and attribution rules must hold for slow samples too.
	ObservationDelay time.Duration
	// DockerStatsFn overrides container stats collection (test seam). The
	// density rule below requires that a slow container-stats call must not
	// slow the sample STREAM; tests inject multi-hundred-ms delays here.
	DockerStatsFn func(names []string) (map[string]containerStat, error)
	// DockerRefresh overrides the background container-stats refresh period
	// (default: Interval*DockerEvery). Container stats run OFF the tick: a
	// docker stats call can never slow the sample stream (density rule).
	DockerRefresh time.Duration
	// SuppressContainerProbes implements measurement-protocol rule 2: observer
	// probes that cost real CPU in the shared Docker VM (docker stats) run
	// strictly OUTSIDE the measured window. While set, the stream never calls
	// docker stats, and the container columns say why they are empty -- they do
	// not silently read as zero IO.
	SuppressContainerProbes bool
}

// SamplerStream stores samples taken while a load phase runs.
type SamplerStream struct {
	mu       sync.Mutex
	samples  []Sample
	interval time.Duration
	stop     chan struct{}
	done     sync.WaitGroup
	// pinnedPID is the process identity this stream measures. It is fixed by
	// the first observation; a later tick that sees another pid is marked as
	// a mismatch and the pin is NOT moved, so a restart mid-step shows up in
	// the data instead of silently becoming "the process shrank".
	pinnedPID int
}

// bindIdentity pins the measured pid and flags any observation taken from a
// different one.
func (s *SamplerStream) bindIdentity(smp *Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if smp.ProcPID == nil || *smp.ProcPID <= 0 {
		return
	}
	if s.pinnedPID == 0 {
		s.pinnedPID = *smp.ProcPID
		return
	}
	if *smp.ProcPID != s.pinnedPID {
		smp.ProcIdentityMismatch = true
	}
}

// StartSampling launches periodic sampling until Stop.
func StartSampling(cfg SamplerConfig) *SamplerStream {
	s := &SamplerStream{stop: make(chan struct{})}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	s.interval = cfg.Interval
	if cfg.DockerEvery <= 0 {
		cfg.DockerEvery = 3
	}
	// Container stats are the most expensive observation (docker stats costs
	// 1-4s under load). Inlining them on the tick made the interim overhead
	// run deliver 22 samples where 45 were configured: a ticker cannot fire
	// while the previous observation is still running. They are collected on
	// their own background period and shared into ticks as snapshots; the
	// per-tick collectors run concurrently so one slow dependency cannot set
	// the pace of the whole stream.
	var dcache *dockerCache
	dockerConfigured := (cfg.PGContainer != "" || cfg.MinioContainer != "" || cfg.DockerStatsFn != nil) && !cfg.SuppressContainerProbes
	if dockerConfigured {
		refresh := cfg.DockerRefresh
		if refresh <= 0 {
			refresh = cfg.Interval * time.Duration(cfg.DockerEvery)
		}
		dcache = newDockerCache(cfg, refresh, s.stop)
	}
	s.done.Add(1)
	go func() {
		defer s.done.Done()
		tick := time.NewTicker(cfg.Interval)
		defer tick.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-tick.C:
				if cfg.ObservationDelay > 0 {
					time.Sleep(cfg.ObservationDelay)
				}
				smp := TakeSample(cfg, dcache)
				// Stamp at COMPLETION, not at entry: an observation that took
				// seconds (docker stats, PG under load) stamped at entry lands
				// in a window that had already closed, and a whole run can
				// report no sampler stream although every tick fired. The
				// completion time is when the evidence is true.
				smp.AtUTC = time.Now().UTC().Format(time.RFC3339Nano)
				s.bindIdentity(&smp)
				s.mu.Lock()
				s.samples = append(s.samples, smp)
				s.mu.Unlock()
			}
		}
	}()
	return s
}

// dockerCache collects container stats on a fixed background period and
// shares the last snapshot with every tick. One refresh runs at a time.
type dockerCache struct {
	mu      sync.Mutex
	stats   map[string]containerStat
	err     error
	at      time.Time
	refresh time.Duration
	costMS  int64
}

func newDockerCache(cfg SamplerConfig, refresh time.Duration, stop chan struct{}) *dockerCache {
	c := &dockerCache{refresh: refresh}
	names := []string{}
	if cfg.PGContainer != "" {
		names = append(names, cfg.PGContainer)
	}
	if cfg.MinioContainer != "" {
		names = append(names, cfg.MinioContainer)
	}
	fn := cfg.DockerStatsFn
	if fn == nil {
		fn = dockerStats
	}
	go func() {
		one := func() {
			t0 := time.Now()
			stats, err := fn(names)
			c.mu.Lock()
			c.stats, c.err, c.at = stats, err, time.Now()
			c.costMS = time.Since(t0).Milliseconds()
			c.mu.Unlock()
		}
		one() // warm before the first tick consumes it
		t := time.NewTicker(refresh)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				one()
			}
		}
	}()
	return c
}

// snapshot returns the freshest container stats or the error explaining why
// there are none; age is how old the snapshot is (0 when never collected).
func (c *dockerCache) snapshot() (map[string]containerStat, error, time.Duration) {
	if c == nil {
		return nil, nil, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.at.IsZero() {
		return nil, c.err, 0
	}
	return c.stats, c.err, time.Since(c.at)
}

// Stop ends sampling and returns all samples.
func (s *SamplerStream) Stop() []Sample {
	close(s.stop)
	s.done.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Sample, len(s.samples))
	copy(out, s.samples)
	return out
}

// TickInterval is the configured sampling period of this stream.
func (s *SamplerStream) TickInterval() time.Duration { return s.interval }

// Samples returns a snapshot without stopping.
func (s *SamplerStream) Samples() []Sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Sample, len(s.samples))
	copy(out, s.samples)
	return out
}

// TakeSample collects one observation. Collectors run CONCURRENTLY: the
// observation costs as much as its slowest source, not the sum of all of
// them (the sequential version cost ~4s and halved stream density).
// Container stats come from the background dockerCache (nil = not collected).
func TakeSample(cfg SamplerConfig, dcache *dockerCache) Sample {
	t0 := time.Now()
	var smp Sample
	noteMu := &sync.Mutex{}
	var notes []string
	var silent []string
	note := func(f string, a ...any) {
		noteMu.Lock()
		notes = append(notes, fmt.Sprintf(f, a...))
		noteMu.Unlock()
	}
	// silentOn records "source:reason" for a collector that produced nothing.
	// "not_configured" and an error stay distinct: the first is a choice about
	// this run, the second is a measurement that broke -- acceptance needs to
	// tell those apart before it can trust the nulls.
	silentOn := func(source, reason string) {
		noteMu.Lock()
		silent = append(silent, source+":"+reason)
		noteMu.Unlock()
	}
	var wg sync.WaitGroup
	collect := func(f func()) {
		wg.Add(1)
		go func() { defer wg.Done(); f() }()
	}
	collect(func() {
		if cfg.Platform == nil || cfg.Platform.PID() <= 0 {
			silentOn(SourceProcess, "not_configured")
			silentOn(SourceDebugStats, "not_configured")
			return
		}
		if st, err := cfg.Platform.ProcessIdentity(cfg.Platform.PID()); err == nil {
			smp.ProcCPUSec, smp.ProcRSS, smp.ProcPID = fptr(st.CPUSec), pU64(st.RSSBytes), pInt(st.PID)
		} else {
			note("ps: %v", err)
			silentOn(SourceProcess, err.Error())
		}
		if st, err := fetchDebugStats(cfg.Platform); err == nil {
			smp.Goroutines, smp.HeapAlloc = pInt(st.Goroutines), pU64(st.HeapAlloc)
			smp.SysBytes, smp.StatsRSS = pU64(st.Sys), pU64(st.RSS)
		} else {
			note("debugstats: %v", err)
			silentOn(SourceDebugStats, err.Error())
		}
	})
	collect(func() {
		if cfg.Pool == nil {
			silentOn(SourcePostgres, "not_configured")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		observed := false
		var failures []string
		var blksRead, blksHit, tupFetched, tupReturned, xactCommit, xactRollback, tempFiles, tempBytes, deadlocks int64
		row := cfg.Pool.QueryRow(ctx, `SELECT blks_read, blks_hit, tup_fetched, tup_returned, xact_commit, xact_rollback, temp_files, temp_bytes, deadlocks FROM pg_stat_database WHERE datname=current_database()`)
		if err := row.Scan(&blksRead, &blksHit, &tupFetched, &tupReturned, &xactCommit, &xactRollback, &tempFiles, &tempBytes, &deadlocks); err != nil {
			note("pg_stat_database: %v", err)
			failures = append(failures, "pg_stat_database: "+err.Error())
		} else {
			observed = true
			smp.PGBlksRead, smp.PGBlksHit = pI64(blksRead), pI64(blksHit)
			smp.PGTupFetched, smp.PGTupReturned = pI64(tupFetched), pI64(tupReturned)
			smp.PGxactCommit, smp.PGxactRollback = pI64(xactCommit), pI64(xactRollback)
			smp.PGTempFiles, smp.PGTempBytes = pI64(tempFiles), pI64(tempBytes)
			smp.PGDeadlocks = pI64(deadlocks)
		}
		row = cfg.Pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state <> 'idle'`)
		var active int64
		if err := row.Scan(&active); err != nil {
			note("pg_stat_activity: %v", err)
			failures = append(failures, "pg_stat_activity: "+err.Error())
		} else {
			observed = true
			smp.PGActiveBackends = pI64(active)
		}
		row = cfg.Pool.QueryRow(ctx, `SELECT pg_database_size(current_database())`)
		var dbSize int64
		if err := row.Scan(&dbSize); err != nil {
			note("pg_database_size: %v", err)
			failures = append(failures, "pg_database_size: "+err.Error())
		} else {
			observed = true
			smp.PGDBSizeBytes = pI64(dbSize)
		}
		if len(failures) > 0 {
			reason := strings.Join(failures, "; ")
			if observed {
				reason = "partial: " + reason
			}
			silentOn(SourcePostgres, reason)
		}
	})
	collect(func() {
		if cfg.RedisAddr == "" {
			silentOn(SourceRedis, "not_configured")
			return
		}
		if info, err := redisInfo(cfg.RedisAddr); err == nil {
			g := func(k string) *int64 { v, _ := strconv.ParseInt(info[k], 10, 64); return pI64(v) }
			smp.RedisUsedMemory = g("used_memory")
			smp.RedisConnectedClients = g("connected_clients")
			smp.RedisTotalCommands = g("total_commands_processed")
			smp.RedisInstantaneousOPS = g("instantaneous_ops_per_sec")
			smp.RedisEvokedKeys = g("evicted_keys")
			smp.RedisRejectedConn = g("rejected_connections")
			smp.RedisNetInputBytes = g("total_net_input_bytes")
			smp.RedisNetOutputBytes = g("total_net_output_bytes")
		} else {
			note("redis: %v", err)
			silentOn(SourceRedis, err.Error())
		}
	})
	collect(func() {
		if cfg.MinioHealth == "" {
			silentOn(SourceMinio, "not_configured")
			return
		}
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(cfg.MinioHealth)
		if err == nil {
			resp.Body.Close()
			smp.MinioHealthOK = pBool(resp.StatusCode == http.StatusOK)
		} else {
			note("minio-health: %v", err)
			silentOn(SourceMinio, err.Error())
		}
	})
	wg.Wait()
	if dcache == nil {
		if cfg.SuppressContainerProbes {
			silentOn(SourceDocker, "suppressed: probe policy is outside-only (observer cost runs outside the measured window)")
		} else {
			silentOn(SourceDocker, "not_configured")
		}
	} else {
		stats, err, age := dcache.snapshot()
		switch {
		case err != nil:
			note("docker: %v", err)
			silentOn(SourceDocker, err.Error())
		case stats == nil:
			// Warm-up race: the docker refresh is background and has not
			// completed yet. Density wins over docker immediacy; the columns
			// stay null and say why, instead of reading as zero IO.
			silentOn(SourceDocker, "not_ready: background refresh has not completed")
		default:
			named := false
			if st, ok := stats[cfg.PGContainer]; ok && cfg.PGContainer != "" {
				smp.PGContainerBlockRead = pI64(st.BlockRead)
				smp.PGContainerBlockWrite = pI64(st.BlockWrite)
				smp.PGContainerNetRx = pI64(st.NetRx)
				smp.PGContainerNetTx = pI64(st.NetTx)
				named = true
			}
			if st, ok := stats[cfg.MinioContainer]; ok && cfg.MinioContainer != "" {
				smp.MinioBlockRead = pI64(st.BlockRead)
				smp.MinioBlockWrite = pI64(st.BlockWrite)
				named = true
			}
			smp.DockerSnapshotAgeMS = pI64(age.Milliseconds())
			if !named {
				silentOn(SourceDocker, "partial: no stats row for the configured container names")
			}
		}
	}
	smp.ObsMS = time.Since(t0).Milliseconds()
	smp.Errors = notes
	smp.Unobserved = silent
	return smp
}
func fetchDebugStats(p *Platform) (struct {
	Goroutines int
	HeapAlloc  uint64
	Sys        uint64
	RSS        uint64
}, error) {
	type statsJSON struct {
		Goroutines int    `json:"goroutines"`
		HeapAlloc  uint64 `json:"heap_alloc_bytes"`
		SysBytes   uint64 `json:"sys_bytes"`
		RSSBytes   uint64 `json:"rss_bytes"`
	}
	client, err := HTTPClient(p.cfg.Certs, ClientCertFile{Cert: p.cfg.Certs.ServerCert, Key: p.cfg.Certs.ServerKey})
	if err != nil {
		return fetchDebugStatsFallback(p, err)
	}
	defer client.CloseIdleConnections()
	client.Timeout = 2 * time.Second
	resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/debug/stats", p.Port))
	if err != nil {
		return fetchDebugStatsFallback(p, err)
	}
	defer resp.Body.Close()
	var sj statsJSON
	if err := json.NewDecoder(resp.Body).Decode(&sj); err != nil {
		return fetchDebugStatsFallback(p, err)
	}
	return struct {
		Goroutines int
		HeapAlloc  uint64
		Sys        uint64
		RSS        uint64
	}{sj.Goroutines, sj.HeapAlloc, sj.SysBytes, sj.RSSBytes}, nil
}

func fetchDebugStatsFallback(p *Platform, cause error) (struct {
	Goroutines int
	HeapAlloc  uint64
	Sys        uint64
	RSS        uint64
}, error) {
	return struct {
		Goroutines int
		HeapAlloc  uint64
		Sys        uint64
		RSS        uint64
	}{}, cause
}

// redisInfo issues INFO all over a bare RESP connection (no client dep).
func redisInfo(addr string) (map[string]string, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	// AUTH if AHP_REDIS_PASSWORD is set in the environment.
	if pw := envLookup("AHP_REDIS_PASSWORD"); pw != "" {
		fmt.Fprintf(conn, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(pw), pw)
		r := bufio.NewReader(conn)
		line, _ := r.ReadString('\n')
		if strings.HasPrefix(line, "-") {
			return nil, fmt.Errorf("redis auth: %s", strings.TrimSpace(line))
		}
	}
	fmt.Fprintf(conn, "*2\r\n$4\r\nINFO\r\n$3\r\nall\r\n")
	r := bufio.NewReader(conn)
	// Read the declared bulk length and stop there. Reading until a read
	// error (EOF) is the density bug: redis keeps the connection open, so
	// every sample paid the full deadline (3s at 2s tick = half the stream).
	head, err := r.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("redis info header: %w", err)
	}
	head = strings.TrimRight(head, "\r\n")
	if !strings.HasPrefix(head, "$") {
		return nil, fmt.Errorf("redis info: unexpected reply %q", head)
	}
	n, err := strconv.Atoi(head[1:])
	if err != nil || n < 0 {
		return nil, fmt.Errorf("redis info: bad bulk length %q", head)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("redis info body: %w", err)
	}
	info := map[string]string{}
	for _, line := range strings.Split(string(buf), "\r\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		// RESP bulk line body: keys and values may contain spaces.
		if i := strings.Index(line, ":"); i > 0 {
			info[line[:i]] = line[i+1:]
		}
	}
	return info, nil
}
func envLookup(key string) string {
	for _, kv := range environ() {
		if strings.HasPrefix(kv, key+"=") {
			return kv[len(key)+1:]
		}
	}
	return ""
}

// containerStat holds parsed docker stats counters.
type containerStat struct {
	BlockRead  int64
	BlockWrite int64
	NetRx      int64
	NetTx      int64
	MemBytes   int64
	CPUPercent float64
}

func dockerStats(names []string) (map[string]containerStat, error) {
	args := append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, names...)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		return nil, err
	}
	res := map[string]containerStat{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		var raw map[string]string
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}
		st := containerStat{}
		st.BlockRead, st.BlockWrite = parseBlockIO(raw["BlockIO"])
		st.NetRx, st.NetTx = parseBlockIO(raw["NetIO"])
		st.MemBytes = parseSize(raw["MemUsage"])
		st.CPUPercent, _ = strconv.ParseFloat(strings.TrimSuffix(raw["CPUPerc"], "%"), 64)
		res[strings.TrimSpace(raw["Name"])] = st
	}
	return res, nil
}

// parseBlockIO parses docker's "1.23MB / 4.56MB" style counters.
func parseBlockIO(s string) (int64, int64) {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return 0, 0
	}
	return parseSize(strings.TrimSpace(parts[0])), parseSize(strings.TrimSpace(parts[1]))
}

func parseSize(s string) int64 {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && (s[i] == '.' || s[i] == '-' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	numStr, unit := s[:i], strings.TrimSpace(s[i:])
	v, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0
	}
	mult := map[string]float64{"": 1, "B": 1, "kB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
		"KiB": 1024, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": float64(1 << 40)}
	if m, ok := mult[unit]; ok {
		return int64(v * m)
	}
	return int64(v)
}
