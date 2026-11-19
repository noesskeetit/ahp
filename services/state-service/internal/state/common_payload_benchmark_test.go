package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Not a turns/LLM benchmark. One isolated test process/database per selected
// layout; 100 serial revisions, identical opaque bytes, one changing cursor.
func BenchmarkRev29StorageTrace(b *testing.B) {
	variant := os.Getenv("REV29_TRACE_VARIANT")
	if variant != "inline" && variant != "objects" {
		b.Fatal("explicit variant required")
	}
	if b.N != 1 {
		b.Fatal("run exactly -benchtime=1x")
	}
	isolateRunDatabase(stateDSN())
	if isolateName == "" {
		b.Fatal("owned fresh DB required")
	}
	ctx := context.Background()
	cfg := isolateCfg.Copy()
	cfg.MaxConns = 1
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		b.Fatal("pool")
	}
	defer pool.Close()
	opts := []StoreOption{WithCommonRecords()}
	if variant == "objects" {
		opts = append(opts, WithCommonPayloadObjects())
	}
	s := New(pool, opts...)
	if e = s.Migrate(ctx); e != nil {
		b.Fatal(e)
	}
	sc := contracts.Scope{Tenant: "trace29", Deployment: "trace", Subject: "alice"}
	th, e := s.CreateThread(ctx, sc, "trace", contracts.NewID())
	if e != nil {
		b.Fatal(e)
	}
	run, e := s.CreateRun(ctx, sc, th.ID, "trace", contracts.NewID(), json.RawMessage(`{}`))
	if e != nil {
		b.Fatal(e)
	}
	lease, e := s.ClaimRunFor(ctx, sc.Tenant, sc.Deployment, "trace-worker", run.ID, time.Minute)
	if e != nil || lease == nil {
		b.Fatal("claim", e)
	}
	f := Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	// Deterministic SHA-derived high-entropy material; not a repeated 'xxxxx'
	// payload that PostgreSQL TOAST would heavily compress.
	raw := make([]byte, 0, 76800)
	for n := 0; len(raw) < 76800; n++ {
		v := sha256.Sum256([]byte(fmt.Sprint("rev29-seed-1/", n)))
		raw = append(raw, v[:]...)
	}
	payload := []byte(`{"data":"` + base64.StdEncoding.EncodeToString(raw[:76800]) + `"}`)
	makeDoc := func(i int) CommonDocument {
		return CommonDocument{Format: CommonFormat, Profile: "trace-static-100KiB/1", Root: "session", Records: map[string]CommonRecord{
			"session": {Kind: "session", Attributes: json.RawMessage(fmt.Sprintf(`{"cursor":%d}`, i)), Relations: map[string][]string{"history": {"stable"}}},
			"stable":  {Kind: "message", Attributes: payload, Relations: map[string][]string{}},
		}}
	}
	const cuts = 100
	writes := []float64{}
	reads := []float64{}
	errors := 0
	for i := 1; i <= cuts; i++ {
		start := time.Now()
		_, e := s.CommitRecords(ctx, sc, f, CommonCommit{Namespace: "trace", Key: fmt.Sprint("cut-", i), ExpectedRevision: int64(i - 1), Document: makeDoc(i)})
		writes = append(writes, float64(time.Since(start).Nanoseconds())/1e6)
		if e != nil {
			errors++
			b.Fatal("write", i, e)
		}
	}
	exact := 0
	for n := 0; n < 100; n++ {
		i := (n*37)%cuts + 1
		start := time.Now()
		got, e := s.ReadRecords(ctx, sc, th.ID, "trace", int64(i))
		reads = append(reads, float64(time.Since(start).Nanoseconds())/1e6)
		if e != nil {
			errors++
			b.Fatal("read", i, e)
		}
		want, _ := json.Marshal(makeDoc(i))
		actual, _ := json.Marshal(got.Document)
		if !bytes.Equal(want, actual) || got.Receipt.Revision != int64(i) {
			b.Fatal("historical revision mismatch", i)
		}
		exact++
	}
	stats := map[string]any{"variant": variant, "cuts": cuts, "records_per_cut": 2, "payload_bytes": len(payload), "errors": errors, "historical_reads_exact": exact, "serial_writes_ms": writes, "serial_reads_ms": reads, "mode": "direct State Go API + real local PostgreSQL; no HTTPS, no LLM", "pool_max_conns": 1, "history_retained": true, "client_full_document_each_time": true}
	sum := sha256.Sum256(payload)
	stats["payload_sha256"] = hex.EncodeToString(sum[:])
	var recordBytes, objectBytes, recordRows, objectRows, tables int64
	if e = pool.QueryRow(ctx, `SELECT count(*),coalesce(sum(octet_length(attributes)),0) FROM ahp_state.common_records`).Scan(&recordRows, &recordBytes); e != nil {
		b.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT count(*),coalesce(sum(octet_length(attributes)),0) FROM ahp_state.common_payloads`).Scan(&objectRows, &objectBytes); e != nil {
		b.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema='ahp_state' AND table_type='BASE TABLE'`).Scan(&tables); e != nil {
		b.Fatal(e)
	}
	stats["record_rows"] = recordRows
	stats["record_attribute_bytes"] = recordBytes
	stats["object_rows"] = objectRows
	stats["object_attribute_bytes"] = objectBytes
	stats["retained_attribute_bytes"] = recordBytes + objectBytes
	stats["state_tables"] = tables
	var logicalFull int64
	for i := 1; i <= cuts; i++ {
		d := makeDoc(i)
		for _, r := range d.Records {
			logicalFull += int64(len(r.Attributes))
		}
	}
	stats["full_copy_attribute_bytes"] = logicalFull
	relations := []map[string]any{}
	var total int64
	rows, e := pool.Query(ctx, `SELECT c.relname,pg_total_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='ahp_state' AND c.relkind='r' AND c.relname LIKE 'common_%' ORDER BY c.relname`)
	if e != nil {
		b.Fatal(e)
	}
	for rows.Next() {
		var name string
		var size int64
		if e = rows.Scan(&name, &size); e != nil {
			b.Fatal(e)
		}
		relations = append(relations, map[string]any{"name": name, "bytes": size})
		total += size
	}
	rows.Close()
	if rows.Err() != nil {
		b.Fatal(rows.Err())
	}
	stats["physical_common_bytes_including_indexes_toast"] = total
	stats["relations"] = relations
	quant := func(v []float64) map[string]float64 {
		w := append([]float64(nil), v...)
		sort.Float64s(w)
		return map[string]float64{"p50": w[(len(w)-1)*50/100], "p95": w[(len(w)-1)*95/100], "max": w[len(w)-1]}
	}
	stats["write_ms"] = quant(writes)
	stats["read_ms"] = quant(reads)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	stats["go_heap_alloc_bytes_at_end"] = m.HeapAlloc
	stats["go_total_alloc_bytes_process"] = m.TotalAlloc
	// Exact old cuts are still present; this is rollback-by-reading, not mutating
	// an active writer's head or promising automatic workflow rewind.
	out, e := json.MarshalIndent(stats, "", "  ")
	if e != nil {
		b.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(os.Getenv("INTEG27_PRIVATE"), "trace.json"), append(out, '\n'), 0600); e != nil {
		b.Fatal(e)
	}
}
