package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type appendProfileTracer struct {
	enabled        atomic.Bool
	queries, sqlns atomic.Int64
}

func (t *appendProfileTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	if t.enabled.Load() {
		t.queries.Add(1)
		return context.WithValue(ctx, t, time.Now())
	}
	return ctx
}
func (t *appendProfileTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if start, ok := ctx.Value(t).(time.Time); ok {
		t.sqlns.Add(time.Since(start).Nanoseconds())
	}
}
func appendProfileCPU() float64 {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}
func appendProfilePercentile(values []float64, q float64) float64 {
	v := append([]float64{}, values...)
	sort.Float64s(v)
	return v[int(float64(len(v)-1)*q)]
}

// This isolates State write/restore cost. It is NOT an LLM or capacity test.
func BenchmarkCommonAppendTrace(b *testing.B) {
	variant := os.Getenv("COMPLETION_APPEND_VARIANT")
	if variant != "interval" && variant != "append" {
		b.Fatal("explicit interval/append variant required")
	}
	if b.N != 1 {
		b.Fatal("run -benchtime=1x")
	}
	isolateRunDatabase(stateDSN())
	if isolateName == "" {
		b.Fatal("owned DB required")
	}
	ctx := context.Background()
	cfg := isolateCfg.Copy()
	cfg.MaxConns = 1
	tracer := &appendProfileTracer{}
	cfg.ConnConfig.Tracer = tracer
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		b.Fatal(e)
	}
	defer pool.Close()
	options := []StoreOption{WithCommonRecordVersions()}
	if variant == "append" {
		options = append(options, WithCommonIncrementalAppend())
	}
	retention, e := WithCommonRetention([]string{"trace-100k-200-events/1"})
	if e != nil {
		b.Fatal(e)
	}
	options = append(options, retention)
	store := New(pool, options...)
	if e = store.Migrate(ctx); e != nil {
		b.Fatal(e)
	}
	scope := contracts.Scope{Tenant: "append-profile", Deployment: "bench", Subject: "alice"}
	thread, e := store.CreateThread(ctx, scope, "trace", contracts.NewID())
	if e != nil {
		b.Fatal(e)
	}
	run, e := store.CreateRun(ctx, scope, thread.ID, "trace", contracts.NewID(), json.RawMessage(`{}`))
	if e != nil {
		b.Fatal(e)
	}
	lease, e := store.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "profile-worker", run.ID, 5*time.Minute)
	if e != nil || lease == nil {
		b.Fatal(e)
	}
	fence := Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	deterministic := func(seed string, size int) json.RawMessage {
		raw := make([]byte, 0, size)
		for i := 0; len(raw) < size; i++ {
			h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
			raw = append(raw, h[:]...)
		}
		out, _ := json.Marshal(map[string]string{"bytes": base64.StdEncoding.EncodeToString(raw[:size])})
		return out
	}
	first := CommonDocument{Format: CommonFormat, Profile: "trace-100k-200-events/1", Root: "session", Records: map[string]CommonRecord{
		"session": {Kind: "session", Attributes: json.RawMessage(`{"cursor":0}`), Relations: map[string][]string{"events": {}, "input": {"input"}}},
		"input":   {Kind: "message", Attributes: deterministic("input", 76800), Relations: map[string][]string{}},
	}}
	if _, e = store.CommitRecords(ctx, scope, fence, CommonCommit{Namespace: "trace", Key: "initial", Document: first}); e != nil {
		b.Fatal(e)
	}
	const count = 200
	requests := make([]CommonCommit, count)
	for i := 0; i < count; i++ {
		n := i
		id := fmt.Sprintf("event-%04d", i)
		requests[i] = CommonCommit{Namespace: "trace", Key: id, ExpectedRevision: int64(i + 1), Delta: &CommonDelta{Format: CommonFormat, Profile: first.Profile, Root: "session", Put: map[string]CommonRecord{
			"session": {Kind: "session", Attributes: json.RawMessage(fmt.Sprintf(`{"cursor":%d}`, i+1)), Relations: map[string][]string{}},
			id:        {Kind: "event", Sequence: json.RawMessage(fmt.Sprint(i)), Attributes: deterministic(id, 3072), Relations: map[string][]string{}},
		}, Relations: []CommonRelationDelta{{Source: "session", Role: "events", Mode: "append", ExpectedLength: &n, Targets: []string{id}}}}}
	}
	runtime.GC()
	cpu := appendProfileCPU()
	tracer.enabled.Store(true)
	started := time.Now()
	latencies := []float64{}
	maxHeap := uint64(0)
	for i, request := range requests {
		start := time.Now()
		_, e = store.CommitRecords(ctx, scope, fence, request)
		latencies = append(latencies, float64(time.Since(start).Nanoseconds())/1e6)
		if e != nil {
			b.Fatal("write", i, e)
		}
		if i%10 == 0 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > maxHeap {
				maxHeap = m.HeapAlloc
			}
		}
	}
	elapsed := time.Since(started)
	cpu = appendProfileCPU() - cpu
	tracer.enabled.Store(false)
	// Verify distributed historical revisions against native bytes and ordering.
	checks := 0
	for _, n := range []int{0, 1, 17, 50, 99, 150, 200} {
		read, e := store.ReadRecords(ctx, scope, thread.ID, "trace", int64(n+1))
		if e != nil {
			b.Fatal("historic", n, e)
		}
		if len(read.Document.Records) != n+2 || len(read.Document.Records["session"].Relations["events"]) != n || !bytes.Equal(read.Document.Records["input"].Attributes, first.Records["input"].Attributes) {
			b.Fatal("historical native state differs", n)
		}
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("event-%04d", i)
			if read.Document.Records["session"].Relations["events"][i] != id || !bytes.Equal(read.Document.Records[id].Attributes, requests[i].Delta.Put[id].Attributes) {
				b.Fatal("native event changed", n, i)
			}
		}
		checks++
	}
	var storage int64
	if e = pool.QueryRow(ctx, `SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='ahp_state' AND c.relkind='r'`).Scan(&storage); e != nil {
		b.Fatal(e)
	}
	if e = store.Release(ctx, scope, run.ID, fence.Worker, fence.Epoch, RunDone); e != nil {
		b.Fatal(e)
	}
	prune, e := store.PruneCommonHistory(ctx, scope, CommonPruneRequest{ThreadID: thread.ID, Namespace: "trace", Key: "prune-profile", ExpectedRevision: count + 1, KeepFromRevision: count + 1, Limit: 512})
	if e != nil || prune.ArchivedCuts != count {
		b.Fatal("retention", prune, e)
	}
	if _, e = store.ReadRecords(ctx, scope, thread.ID, "trace", count+1); e != nil {
		b.Fatal("retained native head", e)
	}
	// PostgreSQL DELETE reclaims logical rows. Physical size is measured after
	// explicit offline VACUUM FULL on THIS OWNED fresh benchmark database only.
	if _, e = pool.Exec(ctx, `VACUUM(FULL, ANALYZE)`); e != nil {
		b.Fatal("owned database compaction", e)
	}
	var compacted int64
	if e = pool.QueryRow(ctx, `SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='ahp_state' AND c.relkind='r'`).Scan(&compacted); e != nil {
		b.Fatal(e)
	}
	// Bounded concurrent smoke, same algorithm per independent session. This is
	// deliberately a correctness exercise, not a saturation/capacity threshold.
	const workers = 16
	const rounds = 10
	cfg2 := isolateCfg.Copy()
	cfg2.MaxConns = 4
	p2, e := pgxpool.NewWithConfig(ctx, cfg2)
	if e != nil {
		b.Fatal(e)
	}
	defer p2.Close()
	parallel := New(p2, options...)
	var wg sync.WaitGroup
	failures := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			sc := scope
			sc.Subject = fmt.Sprint("parallel-", w)
			th, e := parallel.CreateThread(ctx, sc, fmt.Sprint("ctx-", w), contracts.NewID())
			if e != nil {
				failures <- e
				return
			}
			run, e := parallel.CreateRun(ctx, sc, th.ID, "task", contracts.NewID(), json.RawMessage(`{}`))
			if e != nil {
				failures <- e
				return
			}
			lease, e := parallel.ClaimRunFor(ctx, sc.Tenant, sc.Deployment, fmt.Sprint("worker-", w), run.ID, time.Minute)
			if e != nil || lease == nil {
				failures <- fmt.Errorf("parallel claim: %v", e)
				return
			}
			f := Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
			if _, e = parallel.CommitRecords(ctx, sc, f, CommonCommit{Namespace: "trace", Key: "initial", Document: first}); e != nil {
				failures <- e
				return
			}
			for i := 0; i < rounds; i++ {
				if _, e = parallel.CommitRecords(ctx, sc, f, requests[i]); e != nil {
					failures <- e
					return
				}
			}
			read, e := parallel.ReadRecords(ctx, sc, th.ID, "trace", 0)
			if e != nil || read.Receipt.Revision != rounds+1 {
				failures <- fmt.Errorf("parallel restore: %v", e)
				return
			}
		}(w)
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		b.Fatal(e)
	}
	result := map[string]any{"variant": variant, "events": count, "initial_input_bytes": len(first.Records["input"].Attributes), "event_attribute_bytes": len(requests[0].Delta.Put["event-0000"].Attributes), "mode": "direct Go State+real local PostgreSQL; no HTTP/LLM; same delta requests", "pool_max_conns": 1, "write_wall_ms": float64(elapsed.Nanoseconds()) / 1e6, "process_cpu_seconds": cpu, "write_latency_ms": latencies, "write_p50_ms": appendProfilePercentile(latencies, .5), "write_p95_ms": appendProfilePercentile(latencies, .95), "last50_p50_ms": appendProfilePercentile(latencies[150:], .5), "last50_p95_ms": appendProfilePercentile(latencies[150:], .95), "query_round_trips": tracer.queries.Load(), "query_wall_ms": float64(tracer.sqlns.Load()) / 1e6, "max_sampled_go_heap_bytes": maxHeap, "state_physical_bytes_after_serial": storage, "historic_exact_reads": checks, "concurrent_smoke_sessions": workers, "concurrent_smoke_writes": workers * (rounds + 1), "errors": 0, "capacity_measured": false, "retention": prune, "state_physical_bytes_after_retention_and_offline_vacuum": compacted}
	raw, _ := json.MarshalIndent(result, "", "  ")
	path := filepath.Join(os.Getenv("INTEG27_PRIVATE"), "trace.json")
	if e = os.WriteFile(path, append(raw, '\n'), 0600); e != nil {
		b.Fatal(e)
	}
	b.ReportMetric(float64(elapsed.Nanoseconds())/1e6, "serial-ms")
}
