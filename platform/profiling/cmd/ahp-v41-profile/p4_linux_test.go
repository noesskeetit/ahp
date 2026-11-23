package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHeldGatewayReplyReturnsCommittedReceiptAtDeadline(t *testing.T) {
	const receipt = "{\"status\":\"SUCCEEDED\",\"result\":{\"fixture\":\"committed\"}}"
	const request = "{\"arguments\":{\"hold_reply\":true,\"effect_id\":\"soak-fixture/0\"}}"
	routes := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(receipt))
	})
	var held sync.Map
	faults := holdFaultReplies(routes, &held, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rec := httptest.NewRecorder()
	faults.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/tools/Call", strings.NewReader(request)).WithContext(ctx))
	if rec.Code != http.StatusOK || rec.Body.String() != receipt || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("committed response was lost: code=%d content-type=%q body=%q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
}

func TestHeldGatewayReplyIsReleasedWhenGuestDisconnects(t *testing.T) {
	const request = "{\"arguments\":{\"hold_reply\":true,\"effect_id\":\"soak-fixture/0\"}}"
	routes := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{\"status\":\"SUCCEEDED\"}"))
	})
	var held sync.Map
	faults := holdFaultReplies(routes, &held, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		faults.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/tools/Call", strings.NewReader(request)).WithContext(ctx))
	}()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for !faultReplyHeld(&held, "soak-fixture/0") {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("committed response did not become eligible for the freeze")
		}
	}
	cancel()
	select {
	case <-done:
	case <-deadline.C:
		t.Fatal("disconnected guest left its acknowledgement held")
	}
	if rec.Body.Len() != 0 || faultReplyHeld(&held, "soak-fixture/0") {
		t.Fatal("disconnected guest still has an acknowledgement eligible for a freeze")
	}
	retry := httptest.NewRecorder()
	faults.ServeHTTP(retry, httptest.NewRequest(http.MethodPost, "/v1/tools/Call", strings.NewReader(request)))
	if retry.Body.String() != "{\"status\":\"SUCCEEDED\"}" {
		t.Fatal("the replacement did not receive the committed acknowledgement")
	}
}

func TestSoakFaultCohortEndsNinetySecondsBeforeLoad(t *testing.T) {
	recoveryEnd := time.Unix(1200, 0)
	for _, tc := range []struct {
		name    string
		enabled bool
		ordinal int
		arrival time.Time
		want    bool
	}{
		{"control", false, 50, time.Unix(10, 0), false},
		{"regular arrival", true, 51, time.Unix(10, 0), false},
		{"before cutoff", true, 50, time.Unix(1109, 999999999), true},
		{"at cutoff", true, 50, time.Unix(1110, 0), false},
		{"after cutoff", true, 50, time.Unix(1120, 0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := soakFaultScheduled(tc.enabled, tc.ordinal, tc.arrival, recoveryEnd); got != tc.want {
				t.Fatalf("scheduled = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestSoakQualificationRequiresConfirmedFaultRecovery(t *testing.T) {
	for _, tc := range []struct {
		name string
		data soakFaultTotals
		want bool
	}{
		{"accepted and recovered", soakFaultTotals{Intended: 2, Armed: 2, Accepted: 1, Injected: 1, Recovered: 1}, true},
		{"no freeze confirmed", soakFaultTotals{Intended: 2, Armed: 2, Accepted: 1}, false},
		{"one freeze missed", soakFaultTotals{Intended: 2, Armed: 2, Accepted: 2, Injected: 1, Recovered: 1}, false},
		{"no durable recovery", soakFaultTotals{Intended: 2, Armed: 2, Accepted: 2, Injected: 2, Recovered: 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.data.qualified(); got != tc.want {
				t.Fatalf("qualified = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestSoakQualificationRequiresEmptyStateAndObservedFences(t *testing.T) {
	observed := soakCleanupEvidence{
		StateDrained: true, FinalState: &soakStateJobs{},
		FinalFence: &runtimeFenceBatch{Found: 3, Succeeded: 3}, RuntimeStatuses: map[string]int64{"FENCED": 3},
	}
	for _, tc := range []struct {
		name          string
		change        func(*soakCleanupEvidence)
		coldFenceErrs int
		want          string
	}{
		{"clean", func(*soakCleanupEvidence) {}, 0, ""},
		{"queued", func(e *soakCleanupEvidence) { e.FinalState = &soakStateJobs{Queued: 1} }, 0, "State jobs"},
		{"active", func(e *soakCleanupEvidence) { e.FinalState = &soakStateJobs{Active: 1} }, 0, "State jobs"},
		{"projected followup", func(e *soakCleanupEvidence) { e.FinalState = &soakStateJobs{Followups: 1} }, 0, "State jobs"},
		{"unpromoted followup", func(e *soakCleanupEvidence) { e.FinalState = &soakStateJobs{FollowupEvents: 1} }, 0, "State jobs"},
		{"missing fence status", func(e *soakCleanupEvidence) { e.RuntimeStatuses = nil }, 0, "fence unobserved"},
		{"confirmed partial", func(e *soakCleanupEvidence) { e.FinalFence = &runtimeFenceBatch{Found: 3, Succeeded: 2} }, 0, "batch incomplete"},
		{"still running", func(e *soakCleanupEvidence) { e.RuntimeStatuses = map[string]int64{"RUNNING": 1} }, 0, "unfenced Runtime"},
		{"cold fence failed", func(*soakCleanupEvidence) {}, 1, "cold execution fence"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := observed
			tc.change(&e)
			failures := e.qualificationFailures(tc.coldFenceErrs)
			if (tc.want == "" && len(failures) != 0) || (tc.want != "" && !strings.Contains(strings.Join(failures, " "), tc.want)) {
				t.Fatalf("qualification failures = %v, want %q", failures, tc.want)
			}
		})
	}
}
