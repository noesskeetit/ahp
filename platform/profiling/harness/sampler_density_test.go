package harness

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Root acceptance rejects profiles whose steps carry only a handful of
// samples ("sample_count=1", "21..24 of 45"). The report must contain at
// least floor(T/I)-1 samples per step of duration T at tick interval I. The
// interim overhead run of 2026-11-09 20:26 delivered 22 samples for 45 ticks
// because TakeSample ran every collector SEQUENTIALLY and inline docker stats
// (1-4s) backed the ticker up: the stream ran at half the configured cadence.
// Expensive container stats must never set the pace of the stream.
func TestStreamDensityMatchesConfiguredInterval(t *testing.T) {
	const interval = 100 * time.Millisecond
	const duration = 1100 * time.Millisecond
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	cfg := SamplerConfig{
		Interval:    interval,
		DockerEvery: 1, // every tick wants container stats: the worst case
		DockerStatsFn: func(names []string) (map[string]containerStat, error) {
			mu.Lock()
			inFlight++
			if inFlight > maxInFlight {
				maxInFlight = inFlight
			}
			mu.Unlock()
			time.Sleep(700 * time.Millisecond) // measured cost of `docker stats`
			mu.Lock()
			inFlight--
			mu.Unlock()
			return map[string]containerStat{}, nil
		},
	}
	start := time.Now()
	s := StartSampling(cfg)
	time.Sleep(duration)
	got := s.Stop()
	elapsed := time.Since(start)
	want := int(elapsed/interval) - 1
	if len(got) < want {
		t.Fatalf("stream density: got %d samples in %v at %v interval, report requires at least floor(T/I)-1 = %d; the tick cadence must not depend on the most expensive collector", len(got), elapsed, interval, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if maxInFlight > 1 {
		t.Fatalf("container stats collection overlapped itself (max concurrent=%d): refresh must be serialized", maxInFlight)
	}
	// Container stats still reach the samples (density must not be bought by
	// dropping the docker columns entirely).
	seen := 0
	for _, x := range got {
		for _, e := range x.Errors {
			if len(e) > 6 && e[:6] == "docker" {
				t.Fatalf("docker stats surfaced as an error: %q", e)
			}
		}
		seen++
	}
	if seen == 0 {
		t.Fatal("no samples at all")
	}
	fmt.Printf("density ok: %d samples / %v at %v tick (required >= %d)\n", len(got), elapsed.Round(time.Millisecond), interval, want)
}
