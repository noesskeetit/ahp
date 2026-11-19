package model

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

type copySource struct {
	ref   artifacts.ArtifactRef
	data  []byte
	allow atomic.Bool
}

func (s *copySource) Get(ctx context.Context, scope contracts.Scope, id string) (artifacts.ArtifactRef, error) {
	if !s.allow.Load() || scope.Tenant != s.ref.Tenant || id != s.ref.ID {
		return artifacts.ArtifactRef{}, contracts.NewError(contracts.ErrNotFound, "source absent")
	}
	return s.ref, nil
}
func (s *copySource) Open(ctx context.Context, scope contracts.Scope, id string) (io.ReadCloser, artifacts.ArtifactRef, error) {
	ref, e := s.Get(ctx, scope, id)
	if e != nil {
		return nil, ref, e
	}
	return io.NopCloser(bytes.NewReader(s.data)), ref, nil
}

type copyProvider struct {
	upload, del            atomic.Int32
	failUpload, failDelete atomic.Bool
	entered, release       chan struct{}
}

func (p *copyProvider) Identity() string { return "fixture-account-v1" }
func (p *copyProvider) Upload(ctx context.Context, name, mime string, data []byte) (string, error) {
	p.upload.Add(1)
	if p.entered != nil {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if p.failUpload.Load() {
		return "", errors.New("lost acknowledgement")
	}
	return "file-" + name, nil
}
func (p *copyProvider) Delete(ctx context.Context, id string) error {
	p.del.Add(1)
	if p.failDelete.Load() {
		return errors.New("unknown delete")
	}
	return nil
}
func fileFixture(t *testing.T) (*modelEnv, *FileService, FileActor, *copySource, *copyProvider) {
	e := newModelEnv(t)
	a := FileActor{Scope: e.scope, Task: "file-task", Context: "file-context"}
	data := []byte("synthetic sealed file")
	sha := sha256.Sum256(data)
	source := &copySource{data: data, ref: artifacts.ArtifactRef{ID: "artifact-" + e.u, Tenant: e.scope.Tenant, SHA256: hex.EncodeToString(sha[:]), Size: int64(len(data)), Version: "v1", MIME: "text/plain", Labels: labels.Set{Subjects: []string{e.scope.Subject}}}}
	source.allow.Store(true)
	p := &copyProvider{}
	cfg := FileConfig{Source: source, Authorize: func(context.Context, FileActor) error { return nil }, AddLabels: func(context.Context, FileActor, labels.Set) error { return nil }, Route: func(_ context.Context, _ FileActor, set labels.Set) (string, error) {
		return "private", labels.CheckWrite(set, labels.Audience{Kind: labels.KindPrivate, Subject: e.scope.Subject})
	}, Providers: map[string]FileProvider{"private": p}}
	s, err := NewFileService(context.Background(), e.pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e, s, a, source, p
}
func TestProviderFileSingleUploadOwnershipAndDelete(t *testing.T) {
	_, s, a, source, p := fileFixture(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Copy(ctx, a, "same", source.ref.ID)
			if err != nil {
				t.Errorf("copy: %v", err)
			}
		}()
	}
	wg.Wait()
	f, err := s.Copy(ctx, a, "same", source.ref.ID)
	if err != nil || f.Status != FileReady || p.upload.Load() != 1 {
		t.Fatalf("single upload status=%s calls=%d err=%v", f.Status, p.upload.Load(), err)
	}
	other := a
	other.Scope.Subject = "other"
	if _, err = s.Get(ctx, other, f.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign read=%v", err)
	}
	other = a
	other.Task = "other-task"
	if _, err = s.Get(ctx, other, f.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign task read=%v", err)
	}
	other = a
	other.Context = "other-context"
	if _, err = s.Get(ctx, other, f.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign context read=%v", err)
	}
	source.ref.ID = "different-artifact"
	if _, err = s.Copy(ctx, a, "same", source.ref.ID); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("changed key=%v", err)
	}
	got, err := s.Delete(ctx, a, "delete", f.ID)
	if err != nil || got.Status != FileDeleted {
		t.Fatalf("delete=%+v %v", got, err)
	}
	if _, err = s.Delete(ctx, a, "delete", f.ID); err != nil {
		t.Fatal(err)
	}
	if p.del.Load() != 1 {
		t.Fatalf("replay deleted twice=%d", p.del.Load())
	}
}
func TestProviderFileUploadLostAckSurvivesRestart(t *testing.T) {
	e, s, a, source, p := fileFixture(t)
	ctx := context.Background()
	p.failUpload.Store(true)
	f, err := s.Copy(ctx, a, "lost", source.ref.ID)
	if contracts.ReasonOf(err) != "provider_file_upload_unknown" || f.Status != FileUnknown {
		t.Fatalf("lost ack=%+v %v", f, err)
	}
	cold, err := NewFileService(ctx, e.pool, s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if f, err = cold.Copy(ctx, a, "lost", source.ref.ID); err != nil || f.Status != FileUnknown || p.upload.Load() != 1 {
		t.Fatalf("replay=%+v %v calls=%d", f, err, p.upload.Load())
	}
	if err = cold.DeleteTenant(ctx, a.Scope.Tenant); contracts.ReasonOf(err) != "tenant_model_files_pending" {
		t.Fatalf("unknown erased=%v", err)
	}
	if _, err = cold.Copy(ctx, a, "late", source.ref.ID); err == nil {
		t.Fatal("fenced tenant uploaded")
	}
	if p.upload.Load() != 1 {
		t.Fatal("unknown reuploaded")
	}
}
func TestProviderFileTenantFenceDuringUploadRetainsReceiptAndCleans(t *testing.T) {
	_, s, a, source, p := fileFixture(t)
	ctx := context.Background()
	p.entered = make(chan struct{})
	p.release = make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := s.Copy(ctx, a, "inflight", source.ref.ID); done <- err }()
	<-p.entered
	if err := s.FenceTenant(ctx, a.Scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTenant(ctx, a.Scope.Tenant); err == nil {
		t.Fatal("inflight copy erased")
	}
	close(p.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteTenant(ctx, a.Scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if p.del.Load() != 1 {
		t.Fatalf("remote not cleaned=%d", p.del.Load())
	}
}
func TestProviderFileTTLDeleteLostAckAndSubjectFence(t *testing.T) {
	e, s, a, source, p := fileFixture(t)
	ctx := context.Background()
	f, err := s.Copy(ctx, a, "ttl", source.ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.pool.Exec(ctx, `UPDATE ahp_model.provider_files SET expires_at=now()-interval '1 second' WHERE id=$1`, f.ID); err != nil {
		t.Fatal(err)
	}
	p.failDelete.Store(true)
	if _, err = s.sweep(ctx, a.Scope.Tenant, "", 100); contracts.ReasonOf(err) != "provider_file_delete_pending" {
		t.Fatal(err)
	}
	p.failDelete.Store(false)
	if _, err = s.sweep(ctx, a.Scope.Tenant, "", 100); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteSubject(ctx, a.Scope.Tenant, a.Scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Copy(ctx, a, "late", source.ref.ID); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("subject late write=%v", err)
	}
}
func TestProviderFilePolicyReadJournalAndRevocationDenyBeforeDispatch(t *testing.T) {
	_, s, a, source, p := fileFixture(t)
	ctx := context.Background()
	s.cfg.AddLabels = func(context.Context, FileActor, labels.Set) error {
		return contracts.NewError(contracts.ErrUnavailable, "journal down")
	}
	if _, err := s.Copy(ctx, a, "journal", source.ref.ID); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatal(err)
	}
	s.cfg.AddLabels = func(context.Context, FileActor, labels.Set) error { return nil }
	s.cfg.Route = func(_ context.Context, _ FileActor, set labels.Set) (string, error) {
		return "private", labels.CheckWrite(set, labels.Audience{Kind: labels.KindExternal})
	}
	if _, err := s.Copy(ctx, a, "external", source.ref.ID); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal(err)
	}
	s.cfg.Authorize = func(context.Context, FileActor) error { return contracts.NewError(contracts.ErrForbidden, "revoked") }
	if _, err := s.Copy(ctx, a, "revoked", source.ref.ID); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal(err)
	}
	if p.upload.Load() != 0 {
		t.Fatal("denied bytes reached provider")
	}
}
func TestProviderFileReceiptAdoptionAfterInventoryCrash(t *testing.T) {
	e, s, a, source, p := fileFixture(t)
	ctx := context.Background()
	f, err := s.Copy(ctx, a, "receipt", source.ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The receipt committed; inventory publication was lost with the process.
	if _, err = e.pool.Exec(ctx, `UPDATE ahp_model.provider_files SET remote_id='',status='SIDE_EFFECT_UNKNOWN' WHERE id=$1`, f.ID); err != nil {
		t.Fatal(err)
	}
	f, err = s.Copy(ctx, a, "receipt", source.ref.ID)
	if err != nil || f.Status != FileReady || p.upload.Load() != 1 {
		t.Fatalf("receipt not adopted: %s %v", f.Status, err)
	}
}
func TestProviderFileBoundedCancellationLeavesUnknown(t *testing.T) {
	_, s, a, source, p := fileFixture(t)
	p.entered = make(chan struct{})
	p.release = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.Copy(ctx, a, "cancel", source.ref.ID); done <- err }()
	<-p.entered
	cancel()
	select {
	case err := <-done:
		if contracts.ReasonOf(err) != "provider_file_upload_unknown" {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation blocked")
	}
}

func TestProviderFileUnknownDoesNotStarveScheduledCleanup(t *testing.T) {
	e, s, a, source, p := fileFixture(t)
	ctx := context.Background()
	p.failUpload.Store(true)
	if _, err := s.Copy(ctx, a, "unknown", source.ref.ID); err == nil {
		t.Fatal("fixture ACK was not lost")
	}
	source.ref.ID += "-known"
	p.failUpload.Store(false)
	f, err := s.Copy(ctx, a, "known", source.ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.pool.Exec(ctx, `UPDATE ahp_model.provider_files SET expires_at=now()-interval '1 hour',delete_requested=true WHERE tenant=$1`, a.Scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = s.sweepScheduled(ctx, a.Scope.Tenant, "", 1, true); err == nil {
		t.Fatal("unknown unexpectedly settled")
	}
	n, err := s.sweepScheduled(ctx, a.Scope.Tenant, "", 1, true)
	if err != nil || n != 1 {
		t.Fatalf("known copy starved %d %v", n, err)
	}
	got, err := s.lookup(ctx, a, f.ID)
	if err != nil || got.Status != FileDeleted {
		t.Fatal("known copy survives", err)
	}
}
