//go:build ahp_data

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func completeControlDeletion(t *testing.T, st *state.Store, art *artifacts.Store, scope contracts.Scope) {
	t.Helper()
	srv := cascadeServer(t, st, art, scope)
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"fence-subject"}`)
	if code != http.StatusOK {
		t.Fatalf("delete=%d %v", code, body)
	}
}

func TestControlDeletionFencesNewStateAcrossDeployments(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	completeControlDeletion(t, st, art, scope)
	next := scope
	next.Deployment = "late-deployment"
	if _, err := st.CreateThread(ctx, next, "after-erasure", "new-thread"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("erased subject recreated State: %v", err)
	}
	next.Subject = "other-human"
	if _, err := st.CreateThread(ctx, next, "foreign-context", "foreign-thread"); err != nil {
		t.Fatalf("foreign subject write denied: %v", err)
	}
}

func TestControlDeletionRejectsAlreadyAuthorizedLateUpload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	authorize := func(ctx context.Context, _ contracts.Scope, action, _ string) error {
		if action == "upload" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	pool, art, scope := cascadeEnv(t, ctx, authorize)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	next := scope
	next.Deployment = "late-upload-deployment"
	go func() {
		_, err := art.BeginUpload(ctx, next, "late", artifacts.UploadRequest{Name: "late.txt", MIME: "text/plain", Size: 4, SHA256: strings.Repeat("a", 64)})
		result <- err
	}()
	<-entered
	completeControlDeletion(t, st, art, scope)
	close(release)
	if err := <-result; contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("already-authorized upload recreated subject: %v", err)
	}
	scopes, err := art.SubjectScopes(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if len(scopes) != 0 {
		t.Fatalf("late upload metadata survived: %+v", scopes)
	}
}

func TestControlDeletionPreventsRestoreAndSealOfPresignedUpload(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	sealed := sealArtifact(t, ctx, art, scope, artifacts.Binding{}, "original bytes")
	body := "bytes uploaded after erasure"
	sum := sha256.Sum256([]byte(body))
	up, err := art.BeginUpload(ctx, scope, "presigned", artifacts.UploadRequest{Name: "late.txt", MIME: "text/plain", Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:])})
	if err != nil {
		t.Fatal(err)
	}
	completeControlDeletion(t, st, art, scope)
	if _, err = art.Restore(ctx, scope, sealed); contracts.Code(err) != contracts.ErrForbidden {
		t.Errorf("restore resurrected erased artifact: %v", err)
	}
	// A previously-issued S3 URL cannot be revoked individually. It may still
	// upload bytes, but no service operation may revive that artifact metadata.
	request, err := http.NewRequest(http.MethodPut, up.PresignedURL, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("late PUT error type=%T", err)
	}
	response.Body.Close()
	if response.StatusCode/100 != 2 {
		t.Fatalf("late PUT=%d", response.StatusCode)
	}
	if _, err = art.Seal(ctx, scope, up.ID); err == nil {
		t.Fatal("late presigned upload became readable")
	}
	if _, err = art.Get(ctx, scope, up.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("late artifact is readable: %v", err)
	}
}

type unavailableArtifactFence struct{ artifactCascade }

func (c unavailableArtifactFence) subjectScopes(ctx context.Context, scope contracts.Scope) ([]contracts.Scope, error) {
	return c.artifactCascade.(interface {
		subjectScopes(context.Context, contracts.Scope) ([]contracts.Scope, error)
	}).subjectScopes(ctx, scope)
}
func (unavailableArtifactFence) fenceSubject(context.Context, contracts.Scope) error {
	return errors.New("artifact fence unavailable")
}

func TestControlDeletionPendingFenceResumesBeforeDeletingState(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	th, err := st.CreateThread(ctx, scope, "existing", "create")
	if err != nil {
		t.Fatal(err)
	}
	srv := cascadeServerConfigured(t, st, art, scope, func(s *server) {
		c := s.cascade.(*memoryAwareCascade)
		c.artifactCascade = unavailableArtifactFence{c.artifactCascade}
	})
	tok, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteSubject", tok, scope, `{"key":"fence-retry"}`)
	if code != 503 || body["state"] != "pending" {
		t.Fatalf("missing fence=%d %v", code, body)
	}
	if _, err = st.GetThread(ctx, scope, th.ID); err != nil {
		t.Fatalf("State deleted before artifact fence: %v", err)
	}
	if _, err = state.New(pool).CreateThread(ctx, scope, "late", "late-create"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("State fence did not survive handler failure: %v", err)
	}
	lateScope := scope
	lateScope.Deployment = "during-partial"
	lateArtifact := sealArtifact(t, ctx, art, lateScope, artifacts.Binding{}, "arrived before artifact fence")
	restarted := cascadeServer(t, state.New(pool), art, scope)
	tok, err = restarted.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, replayed := restarted.adminCall(t, "ControlDeleteSubject", tok, scope, `{"key":"fence-retry"}`)
	if code != 200 || body["deletion_id"] != replayed["deletion_id"] {
		t.Fatalf("fence retry=%d %v", code, replayed)
	}
	if _, err = st.GetThread(ctx, scope, th.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("State survived completed receipt: %v", err)
	}
	if _, err = art.Get(ctx, lateScope, lateArtifact); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("partial-attempt upload escaped discovery: %v", err)
	}
}

func TestControlDeletionDoesNotUpgradeUnsafeLegacyReceiptToCompleted(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	completeControlDeletion(t, st, art, scope)
	// Reconstruct an interrupted receipt from the pre-fence release. A writer
	// could then add data after a scope had already been acknowledged deleted.
	if _, err := pool.Exec(ctx, `DELETE FROM ahp_state.erased_subjects WHERE tenant=$1 AND subject=$2`, scope.Tenant, scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM ahp_artifacts.erased_subjects WHERE tenant=$1 AND subject=$2`, scope.Tenant, scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE ahp_state.control_deletions SET completed=false,steps=array_remove(array_remove(steps,'state-fence'),'artifacts-fence') WHERE tenant=$1 AND subject=$2`, scope.Tenant, scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateThread(ctx, scope, "legacy-late", "legacy-late"); err != nil {
		t.Fatal(err)
	}
	srv := cascadeServer(t, st, art, scope)
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"fence-subject"}`)
	if code != http.StatusConflict || body["reason"] != "deletion_fence_upgrade_required" {
		t.Fatalf("legacy receipt falsely completed=%d %v", code, body)
	}
	code, body = srv.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"new-fenced-operation"}`)
	if code != http.StatusOK {
		t.Fatalf("new fenced operation=%d %v", code, body)
	}
}
