package dote2e_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type pressureTurn struct {
	TurnID    string             `json:"turn_id,omitempty"`
	Worker    int                `json:"worker"`
	Iteration int                `json:"iteration"`
	Stages    map[string]float64 `json:"stages_ms"`
	Models    int                `json:"model_frames"`
	Tools     int                `json:"tool_frames"`
	Error     string             `json:"error,omitempty"`
}

type pressureRung struct {
	Started      time.Time                     `json:"started_at"`
	Ended        time.Time                     `json:"ended_at"`
	Concurrency  int                           `json:"concurrency"`
	PeakInFlight int64                         `json:"peak_client_in_flight"`
	PeakActive   int64                         `json:"peak_active"`
	Completed    int                           `json:"completed"`
	Errors       map[string]int                `json:"errors"`
	PerSecond    float64                       `json:"completed_per_second"`
	Stages       map[string]map[string]float64 `json:"stages_ms"`
	Turns        []pressureTurn                `json:"turns"`
}

type pressurePrincipal struct {
	client *http.Client
	token  string
}

func pressureLogin(t *testing.T, stand *instance, email, password string) pressurePrincipal {
	t.Helper()
	client := *stand.HTTP
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client.Jar = jar
	return pressurePrincipal{client: &client, token: stand.humanToken(t, &client, email, password)}
}

type pressureFrame struct {
	Type    string          `json:"type"`
	Cursor  int64           `json:"cursor"`
	Thread  string          `json:"thread_id"`
	Payload json.RawMessage `json:"payload"`
}

// TestDotsTurnPressure drives the same HTTP and SSE endpoints as the browser.
// Use a stand with a deterministic provider behind the real Model Gateway.
// Every successful turn must show the durable model, MCP tool and final status.
func TestDotsTurnPressure(t *testing.T) {
	if os.Getenv("AHP_DOT_E2E_PRESSURE") != "1" {
		t.Skip("SKIP_ENV[tag=dot_e2e_layout] reason=pressure_lane_not_enabled missing=AHP_DOT_E2E_PRESSURE")
	}
	stand := loadStand(t)
	ladder := pressureInts(t, "AHP_DOT_E2E_PRESSURE_LADDER", "1,4,10,20", 128)
	perWorker := pressureInts(t, "AHP_DOT_E2E_PRESSURE_TURNS_PER_WORKER", "5", 1000)
	if len(perWorker) != 1 {
		t.Fatal("pressure turns per worker requires one number")
	}
	publishDotAgentConfig(t, stand, "pressure-profile", []string{"list_notes"},
		"Call list_notes once, then reply with DOT-PRESSURE-OK.", 4)
	email, password := mustLogin(t, "email"), mustLogin(t, "password")
	human := pressureLogin(t, stand, email, password)
	shared := os.Getenv("AHP_DOT_E2E_PRESSURE_SHARED_SESSION") == "1"
	stamp := strconv.FormatInt(time.Now().UnixNano(), 36)

	// Warm the HTTP connection, Registry path, MCP connection, and Go/PG caches.
	var warmActive, warmPeak atomic.Int64
	warm := pressureOne(stand, human, stamp+"-warm", 0, 0, &warmActive, &warmPeak)
	if warm.Error != "" {
		t.Fatalf("warm-up turn failed: %s (stages=%v, models=%d, tools=%d)",
			warm.Error, warm.Stages, warm.Models, warm.Tools)
	}
	report := struct {
		Rev    string         `json:"product_revision"`
		Scope  map[string]any `json:"scope"`
		Warmup pressureTurn   `json:"warmup"`
		Ladder []pressureRung `json:"rungs"`
	}{
		Rev: stand.Revision, Warmup: warm,
		Scope: map[string]any{
			"browser":          "HTTP client follows browser HTTP/SSE contract, no rendering",
			"compute":          "no Compute operation in the workload",
			"model":            "real Model Gateway, deterministic OpenAI-compatible provider",
			"tool":             "real Tool Gateway and read-only list_notes MCP",
			"turns_per_worker": perWorker[0], "same_subject": true, "single_browser_session": shared,
		},
	}

	for _, concurrency := range ladder {
		workers := make([]pressurePrincipal, concurrency)
		for i := range workers {
			if shared {
				workers[i] = human
			} else {
				workers[i] = pressureLogin(t, stand, email, password)
			}
		}
		rung := pressureSweep(stand, workers, fmt.Sprintf("%s-%d", stamp, concurrency), perWorker[0])
		report.Ladder = append(report.Ladder, rung)
		t.Logf("C%d active=%d complete=%d/%d %.1f turns/s p95=%.2f ms errors=%v",
			concurrency, rung.PeakActive, rung.Completed, len(rung.Turns), rung.PerSecond,
			rung.Stages["completion"]["p95"], rung.Errors)
	}
	if err := os.MkdirAll(stand.Evidence, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(stand.Evidence, "dots-pressure-"+stamp+".json")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("pressure evidence: %s", path)
	for _, rung := range report.Ladder {
		if rung.Completed == 0 {
			t.Errorf("no complete turns at C%d", rung.Concurrency)
		}
	}
}

func pressureInts(t *testing.T, env, fallback string, maxAllowed int) []int {
	t.Helper()
	text := os.Getenv(env)
	if text == "" {
		text = fallback
	}
	var result []int
	for _, field := range strings.Split(text, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || n < 1 || n > maxAllowed {
			t.Fatalf("%s requires numbers from 1 to %d", env, maxAllowed)
		}
		result = append(result, n)
	}
	return result
}

func pressureSweep(stand *instance, principals []pressurePrincipal, stamp string, each int) pressureRung {
	concurrency := len(principals)
	rung := pressureRung{Concurrency: concurrency, Errors: map[string]int{},
		Turns: make([]pressureTurn, concurrency*each), Stages: map[string]map[string]float64{}}
	var active, peak, inFlight, inFlightPeak atomic.Int64
	var agents sync.WaitGroup
	start := make(chan struct{})
	for worker, human := range principals {
		agents.Add(1)
		go func() {
			defer agents.Done()
			<-start
			for iteration := range each {

				pressurePeak(&inFlightPeak, inFlight.Add(1))
				rung.Turns[worker*each+iteration] = pressureOne(stand, human, stamp, worker, iteration, &active, &peak)
				inFlight.Add(-1)
			}
		}()
	}
	rung.Started = time.Now()
	close(start)
	agents.Wait()
	rung.Ended = time.Now()
	wall := rung.Ended.Sub(rung.Started)
	rung.PeakActive = peak.Load()
	rung.PeakInFlight = inFlightPeak.Load()
	samples := map[string][]float64{}
	for _, turn := range rung.Turns {
		if turn.Error != "" {
			rung.Errors[turn.Error]++
			continue
		}
		rung.Completed++
		for name, duration := range turn.Stages {
			samples[name] = append(samples[name], duration)
		}
	}
	rung.PerSecond = float64(rung.Completed) / wall.Seconds()
	for name, values := range samples {
		sort.Float64s(values)
		pct := func(p float64) float64 { return values[int(math.Ceil(p*float64(len(values))))-1] }
		rung.Stages[name] = map[string]float64{"p50": pct(.5), "p95": pct(.95), "p99": pct(.99)}
	}
	return rung
}

func pressurePeak(peak *atomic.Int64, active int64) {
	for current := peak.Load(); current < active && !peak.CompareAndSwap(current, active); current = peak.Load() {
	}
}

func pressureOne(stand *instance, human pressurePrincipal, stamp string, worker, iteration int, active, peak *atomic.Int64) pressureTurn {
	out := pressureTurn{Worker: worker, Iteration: iteration, Stages: map[string]float64{}}
	fail := func(stage string, err error) pressureTurn {
		// Record only the stage and closed error shape. Gateways can include
		// free text in refusal messages, which is not benchmark evidence.
		out.Error = fmt.Sprintf("%s HTTP=%d code=%s", stage, status(err), errorCode(err))
		return out
	}
	duration := func(a, b time.Time) float64 { return float64(b.Sub(a)) / float64(time.Millisecond) }
	key := fmt.Sprintf("dot-pressure-%s-%d-%d", stamp, worker, iteration)
	begun := time.Now()
	var accepted struct {
		TurnID   string `json:"turn_id"`
		ThreadID string `json:"thread_id"`
	}
	err := stand.call(human.client, "/v1/space/AcceptTurn", map[string]any{
		"key": key, "context_id": key, "max_steps": 4,
		"input": map[string]any{"parts": []any{map[string]any{
			"kind": "text", "text": "Call list_notes, then reply with DOT-PRESSURE-OK.",
		}}},
	}, human.token, &accepted)
	admitted := time.Now()
	out.Stages["accept"] = duration(begun, admitted)
	if err != nil {
		return fail("accept", err)
	}
	if accepted.TurnID == "" || accepted.ThreadID == "" {
		out.Error = "accept_missing_ids"
		return out
	}
	out.TurnID = accepted.TurnID
	nowActive := active.Add(1)
	defer active.Add(-1)
	pressurePeak(peak, nowActive)
	err = stand.call(human.client, "/v1/space/DispatchTurn", map[string]any{"turn_id": accepted.TurnID}, human.token, nil)
	dispatched := time.Now()
	out.Stages["dispatch"] = duration(admitted, dispatched)
	if err != nil {
		return fail("dispatch", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		stand.Origin+"/v1/space/stream/"+accepted.ThreadID+"?after=0", nil)
	if err != nil {
		return fail("sse_request", err)
	}
	req.Header.Set("Authorization", "Bearer "+human.token)
	req.Header.Set("X-AHP-Session", "1")
	res, err := human.client.Do(req)
	if err != nil {
		return fail("sse_open", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		out.Error = fmt.Sprintf("sse_http=%d", res.StatusCode)
		return out
	}

	var firstFrame, toolFrame, terminal time.Time
	var cursor int64
	var finalStatus string
	scanner := bufio.NewScanner(res.Body)
	scanner.Buffer(make([]byte, 16<<10), 2<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		got := time.Now()
		var frame pressureFrame
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &frame) != nil {
			out.Error = "sse_bad_frame"
			return out
		}
		switch {
		case frame.Type == "error":
			out.Error = "sse_error_frame"
		case frame.Type == "end":
			out.Error = "sse_end_without_terminal"
		case frame.Thread != accepted.ThreadID:
			out.Error = "sse_wrong_thread"
		case frame.Cursor <= cursor:
			out.Error = "sse_out_of_order"
		}
		if out.Error != "" {
			return out
		}
		cursor = frame.Cursor
		if firstFrame.IsZero() {
			firstFrame = got
		}
		switch frame.Type {
		case "model":
			out.Models++
		case "tool":
			out.Tools++
			toolFrame = got
		case "status":
			var disposition struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(frame.Payload, &disposition) == nil {
				switch disposition.Status {
				case "done", "failed", "canceled":
					terminal, finalStatus = got, disposition.Status
				}
			}
		}
		if !terminal.IsZero() {
			break
		}
	}
	if err := scanner.Err(); err != nil || terminal.IsZero() {
		return fail("missing_terminal", err)
	}
	res.Body.Close()
	out.Stages["dispatch_to_first_frame"] = duration(dispatched, firstFrame)
	out.Stages["first_frame_to_terminal"] = duration(firstFrame, terminal)
	if !toolFrame.IsZero() {
		out.Stages["first_frame_to_tool"] = duration(firstFrame, toolFrame)
		out.Stages["tool_to_terminal"] = duration(toolFrame, terminal)
	}
	var turn struct {
		Status string `json:"status"`
	}
	err = stand.call(human.client, "/v1/space/GetTurn", map[string]any{"turn_id": accepted.TurnID}, human.token, &turn)
	observed := time.Now()
	out.Stages["terminal_to_status"] = duration(terminal, observed)
	out.Stages["completion"] = duration(begun, observed)
	if err != nil {
		return fail("get_turn", err)
	}
	if finalStatus != "done" || turn.Status != "done" || out.Models != 2 || out.Tools != 1 {
		out.Error = fmt.Sprintf("terminal=%s status=%s models=%d tools=%d", finalStatus, turn.Status, out.Models, out.Tools)
	}
	return out
}
