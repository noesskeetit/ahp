package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// ActiveLease binds the issued task credential, execution context and heartbeat
// to one claim. Loss cancels Context; callers must Release, Suspend or
// CommitAndEnd when done.
type ActiveLease struct {
	cfg        *Config
	claim      sdkstate.Claimed
	fence      sdkstate.Fence
	ctx        context.Context
	cancel     context.CancelFunc
	stop       context.CancelFunc
	done       chan struct{}
	mu         sync.Mutex
	lost       bool
	lostReason error
}

func (al *ActiveLease) ClaimResult() sdkstate.Claimed { return al.claim }
func (al *ActiveLease) Token() string                 { return al.claim.TaskToken }
func (al *ActiveLease) Fence() sdkstate.Fence         { return al.fence }
func (al *ActiveLease) ThreadID() string              { return al.claim.Lease.Thread.ID }
func (al *ActiveLease) RunID() string                 { return al.claim.Lease.Run.ID }
func (al *ActiveLease) Context() context.Context      { return al.ctx }
func (al *ActiveLease) IsLost() bool                  { al.mu.Lock(); defer al.mu.Unlock(); return al.lost }
func (al *ActiveLease) LostReason() error             { al.mu.Lock(); defer al.mu.Unlock(); return al.lostReason }

// ClaimTaskRun claims ONE named run and binds its credential, fence and heartbeat.
//
// There is deliberately no unbound form of this call. An unbound ClaimRun returns
// the eligible FIFO head of ANY thread, so a caller that claims generically and
// then executes ends up running whatever it got -- typically the payload of an
// older run, under that older run's fence, while the caller believes it is serving
// the task it was just handed (PDF rev 3.2 sec.4.2: a task's run is the unit of
// lease, budget and audit). A caller that wants work must first name the run it
// intends to execute, which is exactly what the A2A adapter does with the run it
// created for the message it accepted.
func ClaimTaskRun(ctx context.Context, cfg *Config, runID string) (*ActiveLease, error) {
	return claimAndHold(ctx, cfg, runID)
}

func claimAndHold(ctx context.Context, cfg *Config, runID string) (*ActiveLease, error) {
	if runID == "" {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"sdk: a claim must name the run it intends to execute; an unbound ClaimRun can hand back an unrelated run and the graph would run its payload under that run's fence")
	}
	claimed, err := cfg.StateClient.ClaimRunFor(ctx, cfg.Worker, cfg.LeaseTTLMS(), runID)
	if err != nil {
		return nil, fmt.Errorf("sdk: ClaimRun: %w", err)
	}
	return holdClaim(ctx, cfg, claimed, runID)
}

// holdClaim is shared by a named invocation and the explicitly filtered native
// worker. It never chooses work or substitutes the caller's payload for a claim.
func holdClaim(ctx context.Context, cfg *Config, claimed *sdkstate.Claimed, runID string) (*ActiveLease, error) {
	if claimed == nil {
		return nil, nil
	}
	if (runID != "" && claimed.Lease.Run.ID != runID) || claimed.Lease.Run.ThreadID != claimed.Lease.Thread.ID || claimed.Lease.Run.ID == "" || claimed.TaskToken == "" {
		return nil, fmt.Errorf("sdk: ClaimRun returned an inconsistent lease or missing task credential")
	}
	bound := *cfg
	bound.StateClient = cfg.StateClient.WithTokenSource(func(context.Context) (string, error) { return claimed.TaskToken, nil })
	runCtx, cancel := context.WithCancel(ctx)
	hbCtx, stop := context.WithCancel(runCtx)
	al := &ActiveLease{cfg: &bound, claim: *claimed, fence: sdkstate.Fence{RunID: claimed.Lease.Run.ID, Worker: claimed.Lease.Worker, Epoch: claimed.Lease.Epoch}, ctx: runCtx, cancel: cancel, stop: stop, done: make(chan struct{})}
	go al.heartbeatLoop(hbCtx)
	return al, nil
}
func (al *ActiveLease) finish() error {
	al.stop()
	<-al.done
	defer al.cancel()
	return al.LostReason()
}

// Release ends the lease without stating a reason. Since DEF-RELEASE-REASON-1 that is
// lawful for one status only: done. A failed or canceled run cannot reach its end through
// the State door with nobody saying why, so this call now returns the door's
// INVALID_ARGUMENT for those two - which is the door being honest, not a regression in it.
// Callers that end a run with a verdict use ReleaseWithReason.
func (al *ActiveLease) Release(ctx context.Context, status string) error {
	return al.ReleaseWithReason(ctx, status, "")
}

// ReleaseWithReason ends the lease and names why the run ended. close_reason is the
// execution plane's dictionary word ("error", "step_limit", "canceled", ...) - the same
// vocabulary internal/gateway journals on a task, repeated here as a literal rather than
// importing that plane, for exactly the layering reason internal/state/close_reason.go
// states for the payload key. An empty reason sends no key at all, which keeps the wire
// form byte-identical to the historical Release for every caller that has none to give.
func (al *ActiveLease) ReleaseWithReason(ctx context.Context, status, closeReason string) error {
	if err := al.finish(); err != nil {
		return fmt.Errorf("sdk: lease lost before Release: %w", err)
	}
	if err := al.cfg.StateClient.ReleaseWithReason(ctx, al.fence, status, closeReason); err != nil {
		return fmt.Errorf("sdk: Release: %w", err)
	}
	return nil
}
func (al *ActiveLease) Suspend(ctx context.Context, waitFor string, deadline *time.Time) error {
	if err := al.finish(); err != nil {
		return fmt.Errorf("sdk: lease lost before Suspend: %w", err)
	}
	if err := al.cfg.StateClient.Suspend(ctx, al.fence, waitFor, deadline); err != nil {
		return fmt.Errorf("sdk: Suspend: %w", err)
	}
	return nil
}

// CommitAndEnd publishes a common snapshot/delta, inbox ACKs and stream event
// together with a waiting or terminal run transition. Unlike a separate
// CommitRecords followed by Suspend/Release, State commits the whole transition
// under the same epoch and clears the lease in that transaction.
//
// The caller must have finished the graph and supply a bounded cleanup context
// independent of Context(): this method stops heartbeat AND cancels execution
// before publishing. It binds an omitted fence to this claim and refuses a
// foreign fence. Heartbeat cannot race the successful commit and incorrectly
// report the deliberately cleared lease as lost.
//
// A transport error may mean the commit succeeded but its reply was lost. The
// execution stays stopped: retry this method with the identical key/body and a
// fresh cleanup context to retrieve State's receipt. Never invent a new key or
// restart the graph because the outcome is unknown. The State service still
// checks authorization and rejects new writes from the ended epoch.
func (al *ActiveLease) CommitAndEnd(ctx context.Context, in sdkstate.CommonCommit) (sdkstate.CommonReceipt, error) {
	var out sdkstate.CommonReceipt
	invalid := func(message string) (sdkstate.CommonReceipt, error) {
		return out, contracts.NewError(contracts.ErrInvalid, "sdk: atomic lease end: "+message)
	}
	if ctx == nil {
		return invalid("cleanup context is required")
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if in.Fence != (sdkstate.Fence{}) && in.Fence != al.fence {
		return invalid("fence does not belong to this claim")
	}
	if in.RunStatus == nil || (in.RunStatus.Status != sdkstate.RunWaiting && !sdkstate.IsTerminalStatus(in.RunStatus.Status)) {
		return invalid("waiting or terminal run status is required")
	}
	if in.Key == "" || in.ExpectedRevision < 0 || (in.Document == nil) == (in.Delta == nil) {
		return invalid("key, nonnegative revision and exactly one document or delta are required")
	}
	in.Fence = al.fence
	if err := al.finish(); err != nil {
		return out, fmt.Errorf("sdk: lease lost before atomic commit: %w", err)
	}
	out, err := al.cfg.StateClient.CommitRecords(ctx, in)
	if err != nil {
		return out, fmt.Errorf("sdk: atomic lease end: %w", err)
	}
	return out, nil
}

func (al *ActiveLease) lose(err error) {
	al.mu.Lock()
	al.lost = true
	al.lostReason = err
	al.mu.Unlock()
	al.cancel()
}
func (al *ActiveLease) heartbeatLoop(ctx context.Context) {
	defer close(al.done)
	expires := time.Now().Add(al.cfg.LeaseTTL)
	if !al.claim.Lease.ExpiresAt.IsZero() && al.claim.Lease.ExpiresAt.Before(expires) {
		expires = al.claim.Lease.ExpiresAt
	}
	ticker := time.NewTicker(al.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !time.Now().Before(expires) {
				al.lose(contracts.NewError(contracts.ErrLeaseLost, "lease renewal deadline expired"))
				return
			}
			hbCtx, cancel := context.WithDeadline(ctx, expires)
			next, err := al.cfg.StateClient.Heartbeat(hbCtx, al.fence, al.cfg.LeaseTTLMS())
			cancel()
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				expires = time.Now().Add(al.cfg.LeaseTTL)
				if !next.IsZero() && next.Before(expires) {
					expires = next
				}
			} else if isStaleOrLost(err) || contracts.Code(err) == contracts.ErrForbidden || !time.Now().Before(expires) {
				al.lose(err)
				return
			}
		}
	}
}
func isStaleOrLost(err error) bool {
	var ce *contracts.Error
	return errors.As(err, &ce) && (ce.Code == contracts.ErrStaleEpoch || ce.Code == contracts.ErrLeaseLost)
}
