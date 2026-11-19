package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Overhead on the MOUNTED path. Opt-in only: the director runs this in a
// declared quiet window so the numbers are trustworthy. It never runs under a
// normal gate because it requires AHP_R6_BENCH=1.
//
// OFF and ON are measured in SEPARATE processes (phase selector
// AHP_R6_BENCH_PHASE=off|on) so peak RSS is per-phase, not the max of both.
// PUT = BeginUpload + presigned PUT + Seal; GET = full Open + a mid range read.
// Prints one CSV row per (size, op) plus the process peak RSS.
func TestR6MountedOverhead(t *testing.T) {
	if os.Getenv("AHP_R6_BENCH") != "1" {
		t.Skip("AHP_R6_BENCH!=1")
	}
	phase := os.Getenv("AHP_R6_BENCH_PHASE")
	if phase != "off" && phase != "on" {
		t.Fatal("AHP_R6_BENCH_PHASE must be off|on")
	}
	iters := 25
	if v := os.Getenv("AHP_R6_BENCH_ITERS"); v != "" {
		fmt.Sscanf(v, "%d", &iters)
	}
	e := setup(t)
	if phase == "on" {
		mountCipher(e, 1<<20)
	}
	sizes := []int{1 << 10, 64 << 10, 1 << 20, 32 << 10 * 1024}
	ctx := context.Background()
	for _, n := range sizes {
		body := strings.Repeat("x", n)
		put := timed(t, iters, func(i int) {
			e.uploadSealT(t, fmt.Sprintf("bench-%s-%d-%d", phase, n, i), body)
		})
		ref, _ := e.uploadSeal(t, fmt.Sprintf("bench-rd-%s-%d", phase, n), body)
		getFull := timed(t, iters, func(int) {
			rc, _, err := e.store.Open(ctx, e.scope, ref.ID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			_, _ = io.Copy(io.Discard, rc)
			_ = rc.Close()
		})
		half := int64(n / 2)
		getRange := timed(t, iters, func(int) {
			rc, _, err := e.store.OpenRange(ctx, e.scope, ref.ID, half, 4096)
			if err != nil {
				t.Fatalf("range: %v", err)
			}
			_, _ = io.Copy(io.Discard, rc)
			_ = rc.Close()
		})
		fmt.Printf("R6BENCH,%s,%dB,PUT,%s\n", phase, n, put.csv())
		fmt.Printf("R6BENCH,%s,%dB,GET_FULL,%s\n", phase, n, getFull.csv())
		fmt.Printf("R6BENCH,%s,%dB,GET_RANGE,%s\n", phase, n, getRange.csv())
	}
	fmt.Printf("R6BENCH,%s,PEAK_RSS_BYTES,%d\n", phase, peakRSSBytes())
}

// uploadSealT mirrors env.uploadSeal but with a per-iteration unique key so
// the bench measures a fresh PUT+Seal each time, including seal-transform.
func (e *env) uploadSealT(t *testing.T, key, body string) {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	up, err := e.store.BeginUpload(context.Background(), e.scope, key, UploadRequest{
		Name: "b.bin", MIME: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, body)
	if _, err := e.store.Seal(context.Background(), e.scope, up.ID); err != nil {
		t.Fatalf("Seal: %v", err)
	}
}

type samples struct{ d []time.Duration }

func timed(t *testing.T, n int, fn func(int)) samples {
	t.Helper()
	s := samples{d: make([]time.Duration, 0, n)}
	for i := 0; i < n; i++ {
		start := time.Now()
		fn(i)
		s.d = append(s.d, time.Since(start))
	}
	return s
}

func (s samples) pct(p float64) time.Duration {
	cp := append([]time.Duration(nil), s.d...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(p * float64(len(cp)-1))
	return cp[idx]
}

func (s samples) csv() string {
	return fmt.Sprintf("p50_ms=%.3f,p95_ms=%.3f",
		float64(s.pct(0.50))/float64(time.Millisecond),
		float64(s.pct(0.95))/float64(time.Millisecond))
}

func peakRSSBytes() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return -1
	}
	// ru_maxrss is bytes on Darwin, kilobytes on Linux.
	if runtime.GOOS == "darwin" {
		return ru.Maxrss
	}
	return ru.Maxrss * 1024
}
