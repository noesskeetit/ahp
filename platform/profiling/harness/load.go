package harness

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// StepResult aggregates one closed-loop load step.
type StepResult struct {
	Workload      string         `json:"workload"`
	Concurrency   int            `json:"concurrency"`
	DurationSec   float64        `json:"duration_s"`
	Requests      int            `json:"requests"`
	Errors        int            `json:"errors"`
	ErrorRatePct  float64        `json:"error_rate_pct"`
	ErrorClasses  map[string]int `json:"error_classes,omitempty"`
	ThroughputRPS float64        `json:"throughput_rps"`
	TotalBytes    int64          `json:"total_resp_bytes"`
	// Latency percentiles, ms (full request time).
	// A step whose calls never succeeded measured no latency at all: null,
	// never the old -1 sentinel that read like a (broken) measurement.
	P50  *float64 `json:"p50_ms"`
	P95  *float64 `json:"p95_ms"`
	Max  *float64 `json:"max_ms"`
	Mean *float64 `json:"mean_ms"`
	// TTFB percentiles, ms. nil means NEVER MEASURED (DEF-PROF-1): a
	// non-streaming call has no first-delta moment, and a JSON 0 there used to
	// read as "the first byte arrived in zero milliseconds" -- a placeholder
	// indistinguishable from a measurement. null is the honest answer.
	TTFBP50 *float64 `json:"ttfb_p50_ms"`
	TTFBP95 *float64 `json:"ttfb_p95_ms"`
	TTFBP99 *float64 `json:"ttfb_p99_ms"`
	// ErrP* are latencies of FAILED calls (ms). null = no failures happened,
	// NOT "failures were instant" -- read them together with error_classes.
	ErrP50 *float64 `json:"err_p50_ms"`
	ErrP95 *float64 `json:"err_p95_ms"`
	ErrP99 *float64 `json:"err_p99_ms"`
	ErrMax *float64 `json:"err_max_ms"`
	P99    *float64 `json:"p99_ms"`
	// All* is the distribution over EVERY attempt -- successful and failed -- in
	// this step. Failures are usually the slow tail (timeouts, queueing), so a
	// step that answers 95 requests in 2 ms and then hangs 5 of them for 30 s
	// reports p95=2 ms: true of the successful calls, and an understatement of
	// what the caller experienced. p95_ms keeps its accepted meaning (successes
	// only, comparable with the profiles already signed); these fields are the
	// same step read as what actually happened to all attempts. Both are
	// published because dropping either one loses a different truth.
	AllP50 *float64 `json:"all_p50_ms"`
	AllP95 *float64 `json:"all_p95_ms"`
	AllP99 *float64 `json:"all_p99_ms"`
	// AllObservations is Requests (every attempt is one observation), published
	// so a reader can check the tail rule on this family without recomputing.
	AllObservations *int `json:"all_observations"`
	// PercentileNote explains a suppressed percentile: at small n a p99 is a
	// restatement of the maximum, not a percentile. Publishing it as a claim
	// (as the LLM rungs with n=20 did) is a false precision.
	PercentileNote    string `json:"percentile_note,omitempty"`
	ErrPercentileNote string `json:"err_percentile_note,omitempty"`
	// AllPercentileNote explains a nulled all-attempt quantile, same convention
	// as the other families.
	AllPercentileNote string `json:"all_percentile_note,omitempty"`
	// TTFBPercentileNote carries the same explanation for the ttfb family, so
	// a null ttfb_p99_ms reads as "not asserted at this n" rather than as a
	// probe that broke.
	TTFBPercentileNote string `json:"ttfb_percentile_note,omitempty"`
	// TailJustified spares the reader from recomputing n: were p95/p99 actually
	// justified by the number of observations, or are they null because at this
	// n a "tail" is only a restatement of the maximum? Acceptance asked for this
	// flag to be visible next to the numbers, not inferable from prose.
	TailJustified bool `json:"tail_justified"`
	// Offered/Dropped exist only in open-loop mode: closed loop measures what
	// the system finished, never what arrived, so saturation is invisible.
	Offered  *int `json:"offered_requests,omitempty"`
	Dropped  *int `json:"dropped_at_capacity,omitempty"`
	MaxQueue *int `json:"max_queue_depth,omitempty"`
	// Repeat indexes the Nth repetition of this rung (--repeats); CacheState
	// records whether the caches were cold or warm for this measurement.
	Repeat     int         `json:"repeat"`
	CacheState *CacheProbe `json:"cache_state,omitempty"`
	Sessions   string      `json:"sessions_mode,omitempty"`
	// LoadPattern records how arrivals were generated for THIS step
	// (closed-loop throughput vs open-loop offered load). A rung's numbers
	// mean different things in the two patterns, so the pattern belongs on
	// the row, not only in the phase header.
	LoadPattern string `json:"load_pattern,omitempty"`
	// TTFBObservations is how many calls in this step actually measured a first
	// response byte. The TTFB family cannot be read as a distribution of
	// `requests`; it is a subset, and the tail rule has to be checked against
	// the subset (a step of 400 non-streaming calls observed ZERO first bytes,
	// and a "ttfb_p99" quoted there restates a number nobody measured).
	TTFBObservations *int            `json:"ttfb_observations"`
	Samplers         *SamplerSummary `json:"samplers"`
}

// Tail-quantile rule, declared ONCE for every latency family (total latency,
// failed-call latency, TTFB, hand-built smoke steps): a quantile is asserted
// only when enough observations exist to be one. The thresholds themselves live
// in metrics_contract.go -- the file that owns what a metric may claim -- so a
// reader looking for "when is a number allowed to exist" finds one answer, and
// EnforceTailSampleRule refuses an artifact that breaks the rule on the way to
// disk instead of trusting every producer to have called Tails.
//
//	p50 -- from 1 observation (the median of what was seen is what was seen)
//	p95 -- from 20 observations
//	p99 -- from 100 observations
//
// Below the threshold the quantile is null with a reason naming n. Acceptance
// caught ttfb_p99_ms=761.174 quoted for a step with requests=1 while p99_ms and
// model_usage.phases.ttfb_p99_ms were null: one number wearing a percentile's
// clothes, asserted in one field and correctly refused in two others. A tail
// estimate needs a tail to stand on.

// TailPercentiles is the result of applying that rule to one sorted series.
type TailPercentiles struct {
	P50, P95, P99 *float64
	reasons       []string
}

// Note explains every nulled quantile; empty when nothing was suppressed.
func (t TailPercentiles) Note() string { return strings.Join(t.reasons, "; ") }

// Tails applies the rule. sortedVals must be ascending (as the recorder keeps
// it). note, when given, receives the reason for suppressed quantiles.
func Tails(sortedVals []float64, note *string) TailPercentiles {
	t := TailPercentiles{}
	n := len(sortedVals)
	if n >= MinSamplesP50 {
		t.P50 = fptr(pct(sortedVals, 0.50))
	} else {
		t.reasons = append(t.reasons, "p50 not asserted: n=0 observations")
	}
	for _, q := range []struct {
		p    float64
		min  int
		name string
		dst  **float64
	}{
		{0.95, MinSamplesP95, "p95", &t.P95},
		{0.99, MinSamplesP99, "p99", &t.P99},
	} {
		if n >= q.min {
			*q.dst = fptr(pct(sortedVals, q.p))
		} else {
			t.reasons = append(t.reasons, fmt.Sprintf("%s not asserted: n=%d observations (<%d) -- at this n the value would be the maximum, not a tail estimate", q.name, n, q.min))
		}
	}
	if note != nil && *note == "" {
		*note = t.Note()
	}
	return t
}

// LatencyRecorder keeps raw samples so percentiles can be recomputed exactly.
type LatencyRecorder struct {
	mu     sync.Mutex
	totals []float64
	ttfts  []float64
	// failed holds the latency of calls that did NOT succeed. Cutting the
	// record at c.OK hid exactly the slow tail that failures produce: a step
	// whose errors were timeouts reported the latency of its healthy calls
	// only, which reads better and is false.
	failed []float64
	// offered/dropped/maxQueue are set by the open-loop runner: what arrived
	// and what was refused is a property of the measurement, not of the
	// caller that happens to remember to copy it.
	offered, dropped, maxQueue *int
	good                       int
	errs                       int
	classes                    map[string]int
	bytes                      int64
}

// NewLatencyRecorder creates an empty recorder.
func NewLatencyRecorder() *LatencyRecorder {
	return &LatencyRecorder{classes: map[string]int{}}
}

// Record adds one call result.
func (r *LatencyRecorder) Record(c Call) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bytes += int64(c.RespBytes)
	if c.OK {
		r.good++
		r.totals = append(r.totals, c.Totalms)
		// A call that never observed a first response byte carries TTFBms==0
		// by absence, not by measurement: recording it would poison the
		// percentile with a fake instant first byte.
		if c.TTFBms > 0 {
			r.ttfts = append(r.ttfts, c.TTFBms)
		}
		return
	}
	r.errs++
	r.failed = append(r.failed, c.Totalms)
	key := c.ErrClass
	if key == "" {
		key = "unknown"
	}
	r.classes[key]++
}

// Summarize closes a step.
// SetOffered attaches the arrival-side accounting of an open-loop run.
func (r *LatencyRecorder) SetOffered(offered, dropped, maxQueue int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.offered, r.dropped, r.maxQueue = &offered, &dropped, &maxQueue
}

func (r *LatencyRecorder) Summarize(workload string, concurrency int, wall time.Duration) StepResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	totals := append([]float64(nil), r.totals...)
	ttfts := append([]float64(nil), r.ttfts...)
	sort.Float64s(totals)
	sort.Float64s(ttfts)
	ttfn := len(ttfts)
	res := StepResult{
		Workload:     workload,
		Concurrency:  concurrency,
		DurationSec:  wall.Seconds(),
		Requests:     r.good + r.errs,
		Errors:       r.errs,
		ErrorClasses: map[string]int{},
		TotalBytes:   r.bytes,
	}
	for k, v := range r.classes {
		res.ErrorClasses[k] = v
	}
	if res.Requests > 0 {
		res.ErrorRatePct = 100 * float64(res.errsCount()) / float64(res.Requests)
	}
	if res.goodCount() > 0 && wall > 0 {
		res.ThroughputRPS = float64(res.goodCount()) / wall.Seconds()
	}
	if r.offered != nil {
		o, d, q := *r.offered, *r.dropped, *r.maxQueue
		res.Offered, res.Dropped, res.MaxQueue = &o, &d, &q
	}
	res.TTFBObservations = &ttfn
	tails := Tails(totals, &res.PercentileNote)
	res.P50, res.P95, res.P99 = tails.P50, tails.P95, tails.P99
	res.TailJustified = tails.P95 != nil && tails.P99 != nil
	res.Mean = fptr(mean(totals))
	if n := len(totals); n > 0 {
		m := totals[n-1]
		res.Max = &m
	}
	// Failures carry their own latency distribution: errors are usually the
	// slow tail (timeouts, queueing), and omitting them understates the tail
	// exactly when the system is behaving worst.
	bad := append([]float64(nil), r.failed...)
	sort.Float64s(bad)
	if len(bad) > 0 {
		failedTails := Tails(bad, &res.ErrPercentileNote)
		res.ErrP50, res.ErrP95, res.ErrP99 = failedTails.P50, failedTails.P95, failedTails.P99
		if m := len(bad); m > 0 {
			v := bad[m-1]
			res.ErrMax = &v
		}
	}
	// The all-attempt family: successes and failures together, so a step whose
	// failures are slow cannot publish a tail that pretends they are absent.
	allN := res.Requests // successes + failures: every attempt is one observation
	attempts := append(append([]float64{}, totals...), bad...)
	sort.Float64s(attempts)
	at := Tails(attempts, &res.AllPercentileNote)
	res.AllP50, res.AllP95, res.AllP99 = at.P50, at.P95, at.P99
	res.AllObservations = &allN

	// Percentiles only over calls that actually measured a first byte; with no
	// such call the fields stay null ("not measured"), never 0.
	if len(ttfts) > 0 {
		tt := Tails(ttfts, &res.TTFBPercentileNote)
		res.TTFBP50, res.TTFBP95, res.TTFBP99 = tt.P50, tt.P95, tt.P99
	} else if res.TTFBPercentileNote == "" {
		res.TTFBPercentileNote = "ttfb not measured: no call observed a first response byte (non-streaming calls report null, never 0)"
	}
	return res
}

func (r StepResult) errsCount() int { return r.Errors }
func (r StepResult) goodCount() int { return r.Requests - r.Errors }

// fptr turns a computed figure into its measured-or-null form: NaN/Inf (no
// samples, empty variance) serialize as null instead of the old -1 sentinel
// that a reader could mistake for a real value.
func I64ptr(v int64) *int64 { return &v }

func fptr(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

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

func mean(v []float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	var s float64
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

// RunClosedLoop drives fn from concurrency workers for duration and records
// every call. Workers stop at the deadline; in-flight calls are awaited.
func RunClosedLoop(concurrency int, duration time.Duration, fn func(worker int) Call) (*LatencyRecorder, time.Duration) {
	rec := NewLatencyRecorder()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				rec.Record(fn(worker))
			}
		}(w)
	}
	wg.Wait()
	return rec, time.Since(start)
}

// RunFixedWork drives exactly total calls with concurrency workers and stops
// when the counter is exhausted. Latency is measured per real call only.
func RunFixedWork(total, concurrency int, fn func(i int) Call) (*LatencyRecorder, time.Duration) {
	rec := NewLatencyRecorder()
	var next atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1)
				if i > int64(total) {
					return
				}
				rec.Record(fn(int(i)))
			}
		}()
	}
	wg.Wait()
	return rec, time.Since(start)
}
