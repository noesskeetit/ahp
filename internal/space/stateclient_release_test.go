package space

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DEF-H22-3: a run must always be able to close. These three tests pin the release call
// itself, because the failure this fixes was not a missing verdict but a run that could
// never reach a terminal status - and so was re-claimed and re-executed by the orphan
// sweep forever.

func captureReleases(t *testing.T, handle func(body map[string]any) (int, string)) (*StateClient, *[]map[string]any) {
	t.Helper()
	var seen []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("request body is not readable JSON: %v", err)
		}
		seen = append(seen, body)
		code, msg := handle(body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		if code >= 400 {
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "INVALID_ARGUMENT", "message": msg})
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return NewStateClient(srv.URL, srv.Client()), &seen
}

// An optional field is never sent empty: the State face decodes strictly, so an empty
// close_reason on the wire is a claim about a field the peer may not have.
func TestReleaseSendsNoReasonWhenThereIsNone(t *testing.T) {
	c, seen := captureReleases(t, func(map[string]any) (int, string) { return http.StatusOK, "" })
	if err := c.Release(context.Background(), "tok", "run-1", "worker-1", 3, "done"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if len(*seen) != 1 {
		t.Fatalf("expected one call, got %d", len(*seen))
	}
	if _, present := (*seen)[0]["close_reason"]; present {
		t.Fatalf("Release must not carry close_reason at all, body=%v", (*seen)[0])
	}
	for _, k := range []string{"run_id", "worker", "epoch", "status"} {
		if _, ok := (*seen)[0][k]; !ok {
			t.Errorf("release body lost the required field %q", k)
		}
	}
}

func TestReleaseSendsTheReasonWhenThereIsOne(t *testing.T) {
	c, seen := captureReleases(t, func(map[string]any) (int, string) { return http.StatusOK, "" })
	if err := c.ReleaseWithReason(context.Background(), "tok", "run-1", "worker-1", 3, "done", "model_finished"); err != nil {
		t.Fatalf("ReleaseWithReason: %v", err)
	}
	if got := (*seen)[0]["close_reason"]; got != "model_finished" {
		t.Fatalf("close_reason on the wire = %v, want model_finished", got)
	}
	if len(*seen) != 1 {
		t.Fatalf("a peer that accepts the reason must be called once, got %d calls", len(*seen))
	}
}

// DEF-H22-3, on the one status where it does not collide with DEF-RELEASE-REASON-1:
// `done` is lawful without a stated reason - the durable checkpoint and the transcript
// carry the verdict that ended the run - so a peer that does not know the field still
// closes the run, which is the whole point of the original fix. The re-executed turn the
// defect measured had finished its model work and was closing completed, so the hazard
// stays pinned on the status that actually produced it.
func TestReleaseLandsWhenStateRefusesTheReason(t *testing.T) {
	c, seen := captureReleases(t, func(body map[string]any) (int, string) {
		if _, ok := body["close_reason"]; ok {
			return http.StatusBadRequest, `INVALID_ARGUMENT: transport: request body has an unknown field "close_reason"`
		}
		return http.StatusOK, ""
	})
	err := c.ReleaseWithReason(context.Background(), "tok", "run-1", "worker-1", 3, "done", "model_finished")
	if err != nil {
		t.Fatalf("a refusal of the OPTIONAL reason must never fail a release that may land without it: %v", err)
	}
	if len(*seen) != 2 {
		t.Fatalf("expected the refused call to be retried once without the reason, got %d calls: %v", len(*seen), *seen)
	}
	first, second := (*seen)[0], (*seen)[1]
	if _, ok := first["close_reason"]; !ok {
		t.Errorf("first attempt must have carried the reason, body=%v", first)
	}
	if _, ok := second["close_reason"]; ok {
		t.Errorf("the retry must drop the reason the peer cannot store, body=%v", second)
	}
	if second["status"] != "done" {
		t.Errorf("the retry lost the terminal status: %v", second["status"])
	}
}

// DEF-RELEASE-REASON-1 bounds the workaround above: for failed and canceled there is no
// wire form of this call that the door accepts without a reason, so dropping the refused
// key would not rescue the run - it would be the client committing the unexplained
// terminal the door just made impossible. The refusal must reach the caller instead, and
// the client must not even offer the forbidden second request.
func TestReleaseNeverDegradesToAnUnexplainedTerminal(t *testing.T) {
	for _, status := range []string{"failed", "canceled"} {
		t.Run(status, func(t *testing.T) {
			c, seen := captureReleases(t, func(body map[string]any) (int, string) {
				if _, ok := body["close_reason"]; ok {
					return http.StatusBadRequest, `INVALID_ARGUMENT: transport: request body has an unknown field "close_reason"`
				}
				return http.StatusOK, ""
			})
			err := c.ReleaseWithReasonCause(context.Background(), "tok", "run-1", "worker-1", 3, status, "error", "preflight refused the run")
			if err == nil {
				t.Fatalf("a peer that cannot take the reason must not be allowed to end a %s run unexplained; calls=%v", status, *seen)
			}
			if code := contracts.Code(err); code != contracts.ErrUnavailable {
				t.Errorf("the caller must see an operator-actionable code, got %q (%v)", code, err)
			}
			for _, want := range []string{"close_reason", status} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the error must name %q so the deployment mismatch is diagnosable: %v", want, err)
				}
			}
			if len(*seen) != 1 {
				t.Fatalf("a second request without the reason is the forbidden write, got %d calls: %v", len(*seen), *seen)
			}
		})
	}
}

// Every other refusal is returned unchanged: a fence is not a reason to release anyway.
func TestReleaseNeverWorksAroundAnotherRefusal(t *testing.T) {
	c, seen := captureReleases(t, func(map[string]any) (int, string) {
		return http.StatusConflict, `INVALID_ARGUMENT: state: STALE_EPOCH: this worker no longer holds the lease`
	})
	err := c.ReleaseWithReason(context.Background(), "tok", "run-1", "worker-1", 3, "done", "model_finished")
	if err == nil {
		t.Fatal("a fence refusal must reach the caller, not be retried away")
	}
	if !strings.Contains(err.Error(), "STALE_EPOCH") {
		t.Errorf("error lost the State verdict: %v", err)
	}
	if len(*seen) != 1 {
		t.Fatalf("only one call is allowed when the peer is healthy but refusing, got %d", len(*seen))
	}
}
