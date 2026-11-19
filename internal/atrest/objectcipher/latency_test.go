//go:build ahp_data

package objectcipher

import (
	"context"
	"math"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// TestArtifactPutGetLatencyPercentiles measures the real effect of the artifact
// encryption seam on object PUT/GET latency at the lane's MinIO, per payload
// size and per profile state (enabled vs disabled). Percentiles are computed
// from wall-clock samples. The synthetic operator KMS is flagged SYNTHETIC:
// real vendor KMS latency is an external prerequisite and is NOT measured here.
func TestArtifactPutGetLatencyPercentiles(t *testing.T) {
	if os.Getenv("AHP_S3_ENDPOINT") == "" {
		t.Fatal("AHP_S3_ENDPOINT must be set for the latency harness")
	}
	t.Log("LATENCY NOTE: KMS=SYNTHETIC(operator in-process); transport=lane MinIO; no LLM time included")
	client, bucket := newBucket(t)
	on := cipherFor(t, client, true)
	off := cipherFor(t, client, false)
	ctx := context.Background()

	type sizeSpec struct {
		name    string
		n       int
		samples int
	}
	sizes := []sizeSpec{
		{"1KiB", 1 << 10, 40},
		{"64KiB", 64 << 10, 40},
		{"1MiB", 1 << 20, 30},
		{"32MiB", 32 << 20, 12},
	}
	for _, sz := range sizes {
		body := make([]byte, sz.n)
		for _, mode := range []struct {
			name string
			c    *Cipher
		}{{"disabled", off}, {"enabled", on}} {
			object := "bench/" + sz.name + "/" + mode.name
			// Warm-up so first-connection TLS/HTTP2 setup is excluded.
			if _, err := mode.c.PutObject(ctx, tenant, bucket, object, body, minio.PutObjectOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := mode.c.GetObject(ctx, tenant, bucket, object, 0, 0); err != nil {
				t.Fatal(err)
			}
			put := make([]float64, 0, sz.samples)
			get := make([]float64, 0, sz.samples)
			for i := 0; i < sz.samples; i++ {
				t0 := time.Now()
				if _, err := mode.c.PutObject(ctx, tenant, bucket, object, body, minio.PutObjectOptions{}); err != nil {
					t.Fatal(err)
				}
				put = append(put, ms(time.Since(t0)))

				t1 := time.Now()
				if _, err := mode.c.GetObject(ctx, tenant, bucket, object, 0, 0); err != nil {
					t.Fatal(err)
				}
				get = append(get, ms(time.Since(t1)))
			}
			logPct(t, sz.name, mode.name, "put", put, sz.n)
			logPct(t, sz.name, mode.name, "get", get, sz.n)
		}
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

func logPct(t *testing.T, size, mode, op string, samples []float64, bytes int) {
	t.Helper()
	sort.Float64s(samples)
	p50 := percentile(samples, 0.50)
	p95 := percentile(samples, 0.95)
	mbps := float64(bytes) / (p50 / 1000.0) / (1 << 20)
	t.Logf("LAT size=%s mode=%s op=%s samples=%d p50_ms=%.3f p95_ms=%.3f p50_MBs=%.1f",
		size, mode, op, len(samples), p50, p95, mbps)
}

// percentile returns the nearest-rank p-th percentile of a sorted slice.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Round(p * float64(len(sorted)-1)))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
