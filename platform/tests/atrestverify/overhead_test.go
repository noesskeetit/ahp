package atrestverify_test

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"
)

// L7 - overhead of the MOUNTED path, not of the seam in isolation.
//
// The earlier overhead table measured the cipher seam directly, which is not what a deployment
// pays: a deployment pays BeginUpload + presigned PUT + Seal and a full Open through the artifact
// store with the mount wired in. This leg measures exactly those two operations, OFF versus ON,
// over the same bucket / same tenant / same database, one handle with the cipher and one without.
//
// It is a functional Mac under ambient load, so every number is published with the load average
// before and after it. The comparison OFF versus ON under the SAME load is the usable part; the
// absolute milliseconds are not a platform capacity claim and are not presented as one.
func TestL7OverheadOnMountedPath(t *testing.T) {
	if !envFlag("AHP_ATREST_VERIFY_OVERHEAD") {
		t.Skip("set AHP_ATREST_VERIFY_OVERHEAD=1 to run the overhead leg (it moves ~0.5 GiB to the object store)")
	}
	l := startLeg(t, "L7_overhead_on_mounted_path")
	// cipherOn=false: the leg's primary handle writes plaintext (OFF), and onStore is the mounted
	// handle over the same bucket/tenant. Same data, same database, the mount is the only variable.
	v := newEnv(t, false, 0)
	loadStart := loadAverage()

	type sizeCase struct {
		name string
		n    int
		reps int
	}
	cases := []sizeCase{
		{"1KiB", 1 << 10, 12},
		{"64KiB", 64 << 10, 12},
		{"1MiB", 1 << 20, 8},
		{"32MiB", 32 << 20, 4},
	}
	results := map[string]any{}
	loadEnd := loadAverage()
	for _, s := range cases {
		plain := bodyOf(s.n)
		body := string(plain)
		row := map[string]any{"plain_bytes": s.n, "reps": s.reps}
		for _, mode := range []struct {
			name string
			on   bool
		}{{"OFF", false}, {"ON", true}} {
			store := v.offStore
			if mode.on {
				store = v.onStore
			}
			// Warm-up iteration, discarded: the first request pays connection setup, which is not
			// an at-rest cost.
			warmID, warmUp := v.uploadSeal(t, store, fmt.Sprintf("oh-%s-%s-warm", s.name, mode.name), body)
			_ = v.rawGetVersion(t, warmUp.Object, warmID.Version)
			_ = readAllBytes(t, mustOpen(t, store, v, warmID.ID))

			var putMs, getMs []float64
			ids := make([]string, 0, s.reps)
			storedLens := make([]int, 0, s.reps)
			for i := 0; i < s.reps; i++ {
				key := fmt.Sprintf("oh-%s-%s-%d", s.name, mode.name, i)
				start := time.Now()
				ref, up := v.uploadSeal(t, store, key, body)
				putMs = append(putMs, float64(time.Since(start).Microseconds())/1000.0)
				ids = append(ids, ref.ID)
				storedLens = append(storedLens, len(v.rawGetVersion(t, up.Object, ref.Version)))
			}
			for i := 0; i < s.reps; i++ {
				start := time.Now()
				got := readAllBytes(t, mustOpen(t, store, v, ids[i]))
				getMs = append(getMs, float64(time.Since(start).Microseconds())/1000.0)
				if len(got) != len(plain) {
					t.Fatalf("%s/%s: GET returned %d of %d bytes", s.name, mode.name, len(got), len(plain))
				}
			}
			sort.Float64s(putMs)
			sort.Float64s(getMs)
			avgStored := 0
			for _, n := range storedLens {
				avgStored += n
			}
			avgStored /= len(storedLens)
			row[mode.name] = map[string]any{
				"put_p50_ms": pct(putMs, 0.50), "put_p95_ms": pct(putMs, 0.95),
				"put_min_ms": putMs[0], "put_max_ms": putMs[len(putMs)-1],
				"get_p50_ms": pct(getMs, 0.50), "get_p95_ms": pct(getMs, 0.95),
				"get_min_ms": getMs[0], "get_max_ms": getMs[len(getMs)-1],
				"stored_bytes_avg":  avgStored,
				"put_p50_mib_per_s": mibPerSec(s.n, pct(putMs, 0.50)),
				"get_p50_mib_per_s": mibPerSec(s.n, pct(getMs, 0.50)),
			}
		}
		off := row["OFF"].(map[string]any)
		on := row["ON"].(map[string]any)
		row["put_p50_overhead_pct"] = ratioPct(on["put_p50_ms"].(float64), off["put_p50_ms"].(float64))
		row["get_p50_overhead_pct"] = ratioPct(on["get_p50_ms"].(float64), off["get_p50_ms"].(float64))
		row["storage_expansion_pct"] = ratioPct(float64(on["stored_bytes_avg"].(int)), float64(off["stored_bytes_avg"].(int)))
		results[s.name] = row
		loadEnd = loadAverage()
	}
	l.set("sizes", results)
	l.set("loadavg_before_leg", loadStart)
	l.set("loadavg_after_leg", loadEnd)
	l.set("machine", evEnv.Hostname)
	l.set("product_revision", evEnv.ProductRevision)
	l.set("kms", "SYNTHETIC (in-repo DynamicAuthority)")
	l.set("segment_size", "product default 1 MiB")
	l.set("method", "PUT = BeginUpload + presigned PUT + Seal; GET = full Open through the artifact store; "+
		"one warm-up iteration per cell discarded; nearest-rank percentiles; OFF and ON alternate over the same bucket/tenant/database")
	l.set("interpretation_limit", "functional Mac under ambient load: the OFF/ON comparison is the finding, "+
		"absolute milliseconds are not a capacity claim")
	note("L7 overhead ran under ambient load %v -> %v; numbers are a same-load comparison, not a capacity claim", loadStart, loadEnd)
}

// pct takes a nearest-rank percentile of an already-sorted sample.
func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func ratioPct(a, b float64) float64 {
	if b == 0 {
		return math.NaN()
	}
	return (a - b) / b * 100.0
}

func mibPerSec(bytes int, ms float64) float64 {
	if ms <= 0 {
		return math.NaN()
	}
	return (float64(bytes) / (1 << 20)) / (ms / 1000.0)
}
