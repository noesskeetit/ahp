package toolsapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func newReceiptArtifactOwner(t *testing.T, ctx context.Context, pool *pgxpool.Pool, gateway func() *tools.Service) *artifacts.Store {
	t.Helper()
	endpoint := os.Getenv("AHP_JOURNAL_TEST_S3")
	if endpoint == "" {
		t.Skip("large receipts require the local MinIO fixture")
	}
	if host, _, err := net.SplitHostPort(endpoint); err != nil || host != "127.0.0.1" {
		t.Fatal("use an isolated local MinIO")
	}
	s3, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(
		os.Getenv("AHP_JOURNAL_TEST_S3_ACCESS"), os.Getenv("AHP_JOURNAL_TEST_S3_SECRET"), "",
	)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := pgxpool.NewWithConfig(ctx, pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	journalReader := statejournal.New(reader)
	keep := func(ctx context.Context, tenant string, queries []artifacts.ArtifactQuery) (map[string]bool, error) {
		journal, err := journalReader.ArtifactsReferenced(ctx, tenant, queries)
		if err != nil {
			return nil, err
		}
		receipts, err := gateway().ReceiptArtifactsReferenced(ctx, tenant, queries)
		if err != nil {
			return nil, err
		}
		for id, live := range receipts {
			journal[id] = journal[id] || live
		}
		return journal, nil
	}
	owner, err := artifacts.New(pool, s3, "ahp-receipts-"+randomHex(t, 6),
		func(context.Context, contracts.Scope, string, string) error { return nil },
		artifacts.WithReferenceKeeper(artifacts.ReferenceScanFunc(keep)),
		artifacts.WithCheckpointSettlement(journalReader.BlobPublicationSettled))
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return owner
}

func verifySpilledReceipt(t *testing.T, ctx context.Context, e *env, owner *artifacts.Store, session string, raw []byte) statejournal.BlobRef {
	t.Helper()
	var result tools.FencedResult
	if err := json.Unmarshal(raw, &result); err != nil || result.Status != "COMPLETED" || result.Receipt == nil ||
		result.Receipt.Storage != "artifact" || result.Receipt.Ref == nil || result.Receipt.Payload != nil {
		t.Fatalf("Gateway returned an invalid out-of-line receipt: %v", err)
	}
	ref := result.Receipt.Ref
	if ref.Size != 307246 || ref.Version == "" {
		t.Fatalf("unexpected receipt descriptor: size=%d version=%s", ref.Size, ref.Version)
	}
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject}
	principal := artifacts.WithCallerContext(ctx, session)
	content, observed, err := owner.Open(principal, scope, ref.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	defer content.Close()
	raw, err = io.ReadAll(io.LimitReader(content, ref.Size+1))
	digest := sha256.Sum256(raw)
	if err != nil || int64(len(raw)) != ref.Size || hex.EncodeToString(digest[:]) != ref.Digest ||
		observed.Version != ref.Version || observed.ContextID != session || observed.TaskID != "replacement" ||
		!strings.Contains(string(raw[:min(200, len(raw))]), "written") {
		t.Fatalf("hydrated receipt does not match the Tool Gateway record: bytes=%d (%v)", len(raw), err)
	}
	protected, err := e.svc.ReceiptArtifactsReferenced(ctx, scope.Tenant, []artifacts.ArtifactQuery{{ID: ref.ArtifactID}})
	if err != nil || !protected[ref.ArtifactID] {
		t.Fatalf("receipt reference invisible before State publication: %v", err)
	}
	t.Logf("Gateway recorded an immutable %d-byte receipt in MinIO, owner and SHA-256 verified", len(raw))
	return statejournal.BlobRef{ArtifactID: ref.ArtifactID, Version: ref.Version, SHA256: ref.Digest, SizeBytes: ref.Size}
}
