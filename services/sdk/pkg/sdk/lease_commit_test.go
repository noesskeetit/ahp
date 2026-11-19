package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

func leaseEndInput() sdkstate.CommonCommit {
	return sdkstate.CommonCommit{Key: "final", Namespace: "native", RunStatus: &sdkstate.RunStatusOp{Status: sdkstate.RunWaiting, WaitFor: "answer"}, Document: &sdkstate.CommonDocument{Format: sdkstate.CommonFormat, Profile: "native-test/1", Root: "task", Records: map[string]sdkstate.CommonRecord{"task": {Kind: "task", Attributes: json.RawMessage(`{}`), Relations: map[string][]string{}}}}}
}

// Validation must not silently replace foreign authority or abandon an active
// graph's heartbeat for a request that cannot represent an atomic lease end.
func TestLeaseCommitRejectsBadInputBeforeStopping(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*sdkstate.CommonCommit)
	}{
		{"foreign fence", func(r *sdkstate.CommonCommit) { r.Fence = sdkstate.Fence{RunID: "foreign", Worker: "test", Epoch: 1} }},
		{"no transition", func(r *sdkstate.CommonCommit) { r.RunStatus = nil }},
		{"running", func(r *sdkstate.CommonCommit) { r.RunStatus.Status = sdkstate.RunRunning }},
		{"pending", func(r *sdkstate.CommonCommit) { r.RunStatus.Status = sdkstate.RunPending }},
		{"missing key", func(r *sdkstate.CommonCommit) { r.Key = "" }},
		{"negative revision", func(r *sdkstate.CommonCommit) { r.ExpectedRevision = -1 }},
		{"missing snapshot", func(r *sdkstate.CommonCommit) { r.Document = nil }},
		{"two snapshots", func(r *sdkstate.CommonCommit) { r.Delta = &sdkstate.CommonDelta{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := executorFixture(t, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			lease, err := ClaimTaskRun(ctx, cfg, "run-new")
			if err != nil || lease == nil {
				t.Fatal("claim", err)
			}
			defer lease.finish()
			input := leaseEndInput()
			tc.edit(&input)
			if _, err := lease.CommitAndEnd(ctx, input); contracts.Code(err) != contracts.ErrInvalid {
				t.Fatal("invalid input accepted", err)
			}
			if lease.Context().Err() != nil {
				t.Fatal("bad request canceled ongoing execution")
			}
			select {
			case <-lease.done:
				t.Fatal("bad request stopped heartbeat")
			default:
			}
		})
	}
}

func TestLeaseCommitRefusesLostEpoch(t *testing.T) {
	var commits atomic.Int32
	cfg, _ := executorFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/Heartbeat") {
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, `{"code":"STALE_EPOCH","message":"replacement claimed the run"}`)
			return true
		}
		if strings.HasSuffix(r.URL.Path, "/CommitRecords") {
			commits.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		return false
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lease, err := ClaimTaskRun(ctx, cfg, "run-new")
	if err != nil || lease == nil {
		t.Fatal("claim", err)
	}
	defer lease.finish()
	select {
	case <-lease.Context().Done():
	case <-ctx.Done():
		t.Fatal("stale heartbeat did not cancel execution")
	}
	if _, err := lease.CommitAndEnd(context.Background(), leaseEndInput()); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatal("lost owner tried to publish", err)
	}
	if commits.Load() != 0 {
		t.Fatal("known-stale owner reached commit transport")
	}
}
