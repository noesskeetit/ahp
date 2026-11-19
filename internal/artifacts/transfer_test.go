package artifacts

import (
	"context"
	"errors"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// The fault lane uses deterministic authority; production Identity is covered
// separately by cmd/ahp TestCheckpointRecoveryRealOwners with current PG grants.
type transferTestAuthority struct {
	mu         sync.Mutex
	identities map[string]TransferIdentity
}

func (a *transferTestAuthority) Evaluate(ctx context.Context, in TransferIdentity) (context.Context, TransferIdentity, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	live, ok := a.identities[in.Binding.TaskID]
	if !ok || live.Scope != in.Scope || live.GrantID != in.GrantID || live.Binding != in.Binding || (in.Epoch != 0 && live.Epoch != in.Epoch) || time.Now().After(live.ExpiresAt) {
		return ctx, TransferIdentity{}, transferDenied()
	}
	return WithCallerContext(ctx, live.Binding.ContextID), live, nil
}
func (a *transferTestAuthority) Accumulate(_ context.Context, in TransferIdentity, set labels.Set) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	live, ok := a.identities[in.Binding.TaskID]
	if !ok {
		return transferDenied()
	}
	live.Labels = labels.Merge(live.Labels, set)
	a.identities[in.Binding.TaskID] = live
	return nil
}

type transferFaultCopier struct {
	delegate TransferCopier
	calls    int
	after    func() error
	before   func() error
}

func (c *transferFaultCopier) CopyExact(ctx context.Context, bucket, source, version, target string) error {
	c.calls++
	if c.before != nil {
		if err := c.before(); err != nil {
			return err
		}
	}
	if err := c.delegate.CopyExact(ctx, bucket, source, version, target); err != nil {
		return err
	}
	if c.after != nil {
		return c.after()
	}
	return nil
}

func transferFixture(t *testing.T) (*env, *transferTestAuthority, *transferFaultCopier, TransferIdentity, TransferIdentity, ArtifactRef, TransferGrant) {
	t.Helper()
	e := setup(t)
	a := &transferTestAuthority{identities: map[string]TransferIdentity{}}
	source := TransferIdentity{Scope: e.scope, Binding: Binding{ContextID: "source", TaskID: "source-task"}, GrantID: "source-grant", Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	target := TransferIdentity{Scope: e.scope, Binding: Binding{ContextID: "target", TaskID: "target-task"}, GrantID: "target-grant", Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	a.identities[source.Binding.TaskID] = source
	a.identities[target.Binding.TaskID] = target
	copier := newTestTransferCopier(t)
	faulty := &transferFaultCopier{delegate: copier}
	WithTransferAuthority(a, TransferLimits{})(e.store)
	WithTransferCopier(faulty)(e.store)
	ref, _ := e.sealBoundWithKey(t, source.Binding, "source-bytes", "immutable recovery bytes")
	grant, err := e.store.MintTransferGrant(context.Background(), source, TransferGrantRequest{ID: ref.ID, Version: ref.Version, TargetDeployment: e.scope.Deployment, TargetContext: target.Binding.ContextID, TargetTask: target.Binding.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	return e, a, faulty, source, target, ref, grant
}

func newTestTransferCopier(t *testing.T) TransferCopier {
	t.Helper()
	endpoint, err := url.Parse(os.Getenv("AHP_S3_ENDPOINT"))
	if err != nil {
		t.Fatal(err)
	}
	copier, err := NewTransferCopier(endpoint.Host, minio.Options{Creds: credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""), Secure: endpoint.Scheme == "https"})
	if err != nil {
		t.Fatal(err)
	}
	return copier
}

// The GC import route must retain a reference in its actual recipient context,
// not silently admit a source-context descriptor that a real SDK cannot read.
func rehomeGCImport(t *testing.T, e *checkpointTestEnv, exp state.ControlExport) state.ControlExport {
	t.Helper()
	ctx := context.Background()
	source := TransferIdentity{Scope: e.scope, Binding: Binding{ContextID: e.ref.ContextID, TaskID: e.ref.TaskID}, GrantID: "source", Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	target := TransferIdentity{Scope: e.scope, Binding: Binding{ContextID: "restored", TaskID: "restored-task"}, GrantID: "target", Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	authority := &transferTestAuthority{identities: map[string]TransferIdentity{source.Binding.TaskID: source, target.Binding.TaskID: target}}
	cfg := e.store.pool.Config().Copy()
	cfg.MaxConns = 3
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	owner, err := New(pool, e.client, e.bucket, e.store.authorize, WithTransferAuthority(authority, TransferLimits{}), WithTransferCopier(newTestTransferCopier(t)))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := owner.MintTransferGrant(ctx, source, TransferGrantRequest{ID: e.ref.ID, Version: e.ref.Version, TargetDeployment: e.scope.Deployment, TargetContext: target.Binding.ContextID, TargetTask: target.Binding.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := owner.CopyFromTransferGrant(ctx, target, grant.Grant, "gc-import-rehome")
	if err != nil {
		t.Fatal(err)
	}
	if len(exp.Checkpoints) != 1 {
		t.Fatal("GC fixture expected one checkpoint")
	}
	exp.Checkpoints[0].Metadata = checkpointDescriptor(ref)
	e.ref = ref
	if err := pool.QueryRow(ctx, `SELECT object FROM ahp_artifacts.artifacts WHERE id=$1`, ref.ID).Scan(&e.upload.Object); err != nil {
		t.Fatal(err)
	}
	return exp
}

func TestTransferLostACKReconcilesAfterRestartWithoutCopy(t *testing.T) {
	e, a, copy, _, target, ref, grant := transferFixture(t)
	ctx := context.Background()
	copy.after = func() error { return errors.New("injected lost S3 acknowledgement") }
	if _, err := e.store.CopyFromTransferGrant(ctx, target, grant.Grant, "stable"); contracts.ReasonOf(err) != "transfer_outcome_unknown" {
		t.Fatal("expected UNKNOWN", err)
	}
	restarted, err := New(e.store.pool, e.client, e.bucket, e.store.authorize, WithTransferAuthority(a, TransferLimits{}), WithTransferCopier(copy))
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.CopyFromTransferGrant(ctx, target, grant.Grant, "stable")
	if err != nil {
		t.Fatal("reconcile", err)
	}
	if copy.calls != 1 || got.SHA256 != ref.SHA256 || got.ContextID != target.Binding.ContextID {
		t.Fatalf("bad reconciliation copies=%d got=%+v", copy.calls, got)
	}
	rc, _, err := restarted.Open(WithCallerContext(ctx, target.Binding.ContextID), target.Scope, got.ID)
	if err != nil {
		t.Fatal(err)
	}
	readAll(t, rc, "immutable recovery bytes")
}

func TestTransferRefusalBeforePublicationLeavesNoReadableTarget(t *testing.T) {
	for _, change := range []string{"revoke", "labels"} {
		t.Run(change, func(t *testing.T) {
			e, a, copier, source, target, sourceRef, grant := transferFixture(t)
			ctx := context.Background()
			copier.after = func() error {
				a.mu.Lock()
				defer a.mu.Unlock()
				if change == "revoke" {
					delete(a.identities, source.Binding.TaskID)
				} else {
					live := a.identities[source.Binding.TaskID]
					live.Labels = labels.Set{Workspaces: []string{"restricted-after-copy"}}
					a.identities[source.Binding.TaskID] = live
				}
				return nil
			}
			if _, err := e.store.CopyFromTransferGrant(ctx, target, grant.Grant, "publication-race"); err == nil {
				t.Fatal("changed source authority accepted")
			}
			var id string
			if err := e.store.pool.QueryRow(ctx, `SELECT target_id FROM ahp_artifacts.checkpoint_transfer_operations WHERE source_ref->>'id'=$1`, sourceRef.ID).Scan(&id); err != nil {
				t.Fatal(err)
			}
			targetCtx := WithCallerContext(ctx, target.Binding.ContextID)
			if _, err := e.store.Get(targetCtx, target.Scope, id); contracts.Code(err) != contracts.ErrNotFound {
				t.Fatal("refused copy left a readable target", err)
			}
			if _, err := e.store.Seal(targetCtx, target.Scope, id); contracts.Code(err) != contracts.ErrForbidden {
				t.Fatal("public Seal bypassed transfer publication", err)
			}
			refs, err := e.store.ListByContext(targetCtx, target.Scope, target.Binding.ContextID)
			if err != nil || len(refs) != 0 {
				t.Fatal("unpublished target visible in context list", len(refs), err)
			}
		})
	}
}

func TestTransferOwnTargetTaintDoesNotChangeReplayIdentity(t *testing.T) {
	e, _, copier, source, target, refA, grantA := transferFixture(t)
	WithGrantLabels(func(context.Context, contracts.Scope) (labels.Set, error) {
		return labels.Set{Workspaces: []string{"different-source-label"}}, nil
	})(e.store)
	refB, _ := e.sealBoundWithKey(t, source.Binding, "source-b", "other immutable bytes")
	WithGrantLabels(nil)(e.store)
	ctx := context.Background()
	grantB, err := e.store.MintTransferGrant(ctx, source, TransferGrantRequest{ID: refB.ID, Version: refB.Version, TargetDeployment: target.Scope.Deployment, TargetContext: target.Binding.ContextID, TargetTask: target.Binding.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	var copiedA ArtifactRef
	for round := 0; round < 2; round++ {
		for _, step := range []struct {
			key    string
			grant  TransferGrant
			source ArtifactRef
		}{{"copy-A", grantA, refA}, {"copy-B", grantB, refB}, {"copy-A", grantA, refA}} {
			got, err := e.store.CopyFromTransferGrant(ctx, target, step.grant.Grant, step.key)
			if err != nil {
				t.Fatalf("round=%d step=%s: %v", round, step.key, err)
			}
			if got.SHA256 != step.source.SHA256 {
				t.Fatal("different source bytes")
			}
			if step.key == "copy-A" {
				if copiedA.ID != "" && (copiedA.ID != got.ID || copiedA.Version != got.Version) {
					t.Fatal("replay changed immutable A")
				}
				copiedA = got
			}
		}
	}
	if copier.calls != 2 {
		t.Fatalf("dispatch count=%d want2", copier.calls)
	}
}

func TestTransferUnknownWithMissingObjectDoesNotRedispatch(t *testing.T) {
	e, a, copy, _, target, _, grant := transferFixture(t)
	ctx := context.Background()
	copy.before = func() error { return errors.New("unknown network outcome") }
	for i := 0; i < 3; i++ {
		restarted, err := New(e.store.pool, e.client, e.bucket, e.store.authorize, WithTransferAuthority(a, TransferLimits{}), WithTransferCopier(copy))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = restarted.CopyFromTransferGrant(ctx, target, grant.Grant, "stable-unknown"); contracts.ReasonOf(err) != "transfer_outcome_unknown" {
			t.Fatal("expected durable UNKNOWN", err)
		}
	}
	if copy.calls != 1 {
		t.Fatalf("UNKNOWN redispatched %d times", copy.calls)
	}
}

func TestTransferPurposeExpiryLabelsAndBounds(t *testing.T) {
	e, a, copy, source, target, ref, grant := transferFixture(t)
	ctx := context.Background()
	if _, _, err := e.store.OpenGrantedRange(ctx, target.Scope, grant.Grant, 0, 0); err == nil {
		t.Fatal("copy ticket used as download ticket")
	}
	dg, err := e.store.MintDownloadGrant(WithCallerContext(ctx, source.Binding.ContextID), source.Scope, ref.ID, GrantRequest{Version: ref.Version})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.store.CopyFromTransferGrant(ctx, target, dg.Grant, "wrong-purpose"); err == nil {
		t.Fatal("download ticket used as copy")
	}
	target.Scope.Tenant = "foreign"
	if _, err = e.store.CopyFromTransferGrant(ctx, target, grant.Grant, "foreign-tenant"); err == nil {
		t.Fatal("foreign tenant accepted")
	}
	target.Scope = source.Scope
	live := a.identities[source.Binding.TaskID]
	live.Labels = labels.Set{Subjects: []string{"mallory"}}
	a.identities[source.Binding.TaskID] = live
	if _, err = e.store.CopyFromTransferGrant(ctx, target, grant.Grant, "foreign-labels"); err == nil {
		t.Fatal("source reader label laundering")
	}
	live.Labels = labels.Set{}
	a.identities[source.Binding.TaskID] = live
	e.store.transferLimits.MaxBytes = 1
	if _, err = e.store.CopyFromTransferGrant(ctx, target, grant.Grant, "too-large"); err == nil {
		t.Fatal("configured size ignored")
	}
	e.store.transferLimits.MaxBytes = MaxSize
	digest, _ := grantDigest(grant.Grant)
	if _, err = e.store.pool.Exec(ctx, `UPDATE ahp_artifacts.checkpoint_transfer_grants SET expires_at=now()-interval '1 second' WHERE digest=$1`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = e.store.CopyFromTransferGrant(ctx, target, grant.Grant, "expired"); err == nil {
		t.Fatal("expired ticket accepted")
	}
	if copy.calls != 0 {
		t.Fatal("negative cases dispatched object copy")
	}
}

func TestTransferSourceLockLossAllowsGCWithoutBrokenSuccess(t *testing.T) {
	e, a, copy, _, target, ref, grant := transferFixture(t)
	ctx := context.Background()
	cfg := e.store.pool.Config().Copy()
	app := "transfer-fault-" + contracts.NewID()
	if len(app) > 63 {
		app = app[:63]
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	interrupted, err := New(pool, e.client, e.bucket, e.store.authorize, WithTransferAuthority(a, TransferLimits{}), WithTransferCopier(copy))
	if err != nil {
		t.Fatal(err)
	}
	copy.before = func() error {
		// This unique application owns only this test's copy. Kill exactly its
		// idle transaction, not other fixtures or the ordinary owner connections.
		var killed bool
		err := e.store.pool.QueryRow(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=$1 AND state='idle in transaction'`, app).Scan(&killed)
		if err != nil || !killed {
			t.Fatalf("failed to inject transaction loss: %v killed=%v", err, killed)
		}
		if err := e.store.Delete(ctx, e.scope, ref.ID, "during-copy"); err != nil {
			return err
		}
		if _, err := e.store.pool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET deleted_at=now()-interval '2 hours' WHERE id=$1`, ref.ID); err != nil {
			return err
		}
		gc, err := NewGC(e.store, WithLiveness(LivenessFunc(func(_ context.Context, _ string, refs []Ref) (map[Ref]bool, error) {
			out := map[Ref]bool{}
			for _, r := range refs {
				out[r] = false
			}
			return out, nil
		})), WithReferenceScan(ReferenceScanFunc(func(_ context.Context, _ string, items []ArtifactQuery) (map[string]bool, error) {
			out := map[string]bool{}
			for _, item := range items {
				out[item.ID] = false
			}
			return out, nil
		})))
		if err != nil {
			return err
		}
		if report, e2 := gc.RunOnce(ctx, e.scope, fastPolicy()); e2 != nil {
			t.Fatalf("fault GC failed: %v", e2)
			return err
		} else if report.PurgedTombstoned != 1 {
			t.Fatalf("fault GC did not remove source: %+v", report)
		}
		return nil
	}
	got, err := interrupted.CopyFromTransferGrant(ctx, target, grant.Grant, "lost-lock")
	if err == nil {
		rc, _, e2 := e.store.Open(WithCallerContext(ctx, target.Binding.ContextID), target.Scope, got.ID)
		if e2 != nil {
			t.Fatal("successful broken copy", e2)
		}
		readAll(t, rc, "immutable recovery bytes")
	}
	var n int
	if e2 := e.store.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_artifacts.artifacts WHERE id=$1`, ref.ID).Scan(&n); e2 != nil || n != 0 {
		t.Fatal("fault did not collect source", n, e2)
	}
	if copy.calls != 1 {
		t.Fatal("copy retried after lock loss")
	}
	t.Log("source transaction killed, GC removed source; copy returned", contracts.Code(err))
}

func TestTransferParallelCopiesDoNotExhaustSmallOwnerPool(t *testing.T) {
	e, a, copier, _, target, _, grant := transferFixture(t)
	cfg := e.store.pool.Config().Copy()
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	owner, err := New(pool, e.client, e.bucket, e.store.authorize, WithTransferAuthority(a, TransferLimits{Timeout: 2 * time.Second}), WithTransferCopier(copier.delegate))
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	out := make(chan error, 2)
	for _, key := range []string{"copy-a", "copy-b"} {
		go func(k string) {
			<-start
			_, err := owner.CopyFromTransferGrant(context.Background(), target, grant.Grant, k)
			out <- err
		}(key)
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-out; err != nil {
			t.Fatal("parallel copy exhausted owner pool", err)
		}
	}
}

func TestTransferDeletionKeepsReceiptAndRemovesSourceCapability(t *testing.T) {
	e, _, copier, source, target, ref, grant := transferFixture(t)
	ctx := context.Background()
	copied, err := e.store.CopyFromTransferGrant(ctx, target, grant.Grant, "erase")
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{copied.ID, ref.ID} {
		if err := e.store.Delete(ctx, e.scope, id, "erase-"+id); err != nil {
			t.Fatal(err)
		}
		if err := e.store.Purge(ctx, e.scope, id); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if _, err := e.store.CopyFromTransferGrant(ctx, target, grant.Grant, "erase"); contracts.ReasonOf(err) != "transfer_outcome_unknown" {
				t.Fatal("deleted target must not erase dispatch identity", err)
			}
			if copier.calls != 1 {
				t.Fatal("deletion caused blind redispatch")
			}
		}
	}
	var grants, operations int
	if err := e.store.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_artifacts.checkpoint_transfer_grants WHERE source_identity->'scope'->>'tenant'=$1`, source.Scope.Tenant).Scan(&grants); err != nil {
		t.Fatal(err)
	}
	if err := e.store.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_artifacts.checkpoint_transfer_operations WHERE target_id=$1`, copied.ID).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if grants != 0 || operations != 1 {
		t.Fatalf("source capability must disappear but durable receipt remain: grants=%d operations=%d", grants, operations)
	}
}
