package artifacts

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
)

// TransferIdentity is an attested reader, not a credential. Epoch and Labels
// are obtained from the live Identity owner; request bodies cannot supply them.
type TransferIdentity struct {
	Scope     contracts.Scope `json:"scope"`
	Binding   Binding         `json:"binding"`
	GrantID   string          `json:"grant_id"`
	Epoch     int64           `json:"epoch"`
	Labels    labels.Set      `json:"labels"`
	ExpiresAt time.Time       `json:"expires_at"`
}

// TransferAuthority re-evaluates the issuing reader and recipient. Evaluate
// must match grant ID/context/epoch, enforce expiry/ancestors/revocation, and
// return a context carrying only that verified principal for owner read gates.
// Accumulate must durably taint the target task, ancestors and context journal.
type TransferAuthority interface {
	Evaluate(context.Context, TransferIdentity) (context.Context, TransferIdentity, error)
	Accumulate(context.Context, TransferIdentity, labels.Set) error
}

// TransferLimits bounds one checkpoint recovery copy, including SHA readback.
type TransferLimits struct {
	MaxBytes int64
	Timeout  time.Duration
}

// TransferCopier dispatches exactly once. The constructor below overrides
// MinIO's otherwise retrying transport; UNKNOWN reconciliation belongs here.
type TransferCopier interface {
	CopyExact(context.Context, string, string, string, string) error
}
type transferCopier struct{ client *minio.Client }

func NewTransferCopier(endpoint string, options minio.Options) (TransferCopier, error) {
	options.MaxRetries = 1
	client, err := minio.New(endpoint, &options)
	if err != nil {
		return nil, err
	}
	return transferCopier{client}, nil
}
func (c transferCopier) CopyExact(ctx context.Context, bucket, source, version, target string) error {
	_, err := c.client.CopyObject(ctx, minio.CopyDestOptions{Bucket: bucket, Object: target}, minio.CopySrcOptions{Bucket: bucket, Object: source, VersionID: version})
	return err
}
func WithTransferCopier(c TransferCopier) Option { return func(s *Store) { s.transferCopier = c } }

// WithTransferAuthority enables checkpoint transfer. Absent wiring fails closed.
func WithTransferAuthority(a TransferAuthority, limits TransferLimits) Option {
	return func(s *Store) { s.transferAuthority = a; s.transferLimits = limits }
}

// TransferEnabled does not imply configured identity is available at call time.
func (s *Store) TransferEnabled() bool { return s.transferAuthority != nil && s.transferCopier != nil }

func (s *Store) transferBounds() TransferLimits {
	b := s.transferLimits
	if b.MaxBytes <= 0 || b.MaxBytes > MaxSize {
		b.MaxBytes = MaxSize
	}
	if b.Timeout <= 0 || b.Timeout > time.Minute {
		b.Timeout = time.Minute
	}
	return b
}

const ddlTransfers = `
CREATE TABLE IF NOT EXISTS ahp_artifacts.checkpoint_transfer_grants (
 digest text PRIMARY KEY,
 source_id text REFERENCES ahp_artifacts.artifacts(id) ON DELETE CASCADE,
 source_identity jsonb NOT NULL,
 source_ref jsonb NOT NULL,
 target_deployment text NOT NULL,
 target_context text NOT NULL,
 target_task text NOT NULL,
 expires_at timestamptz NOT NULL
);
ALTER TABLE ahp_artifacts.checkpoint_transfer_grants ADD COLUMN IF NOT EXISTS source_id text REFERENCES ahp_artifacts.artifacts(id) ON DELETE CASCADE;
UPDATE ahp_artifacts.checkpoint_transfer_grants g SET source_id=source_ref->>'id' WHERE source_id IS NULL AND EXISTS(SELECT 1 FROM ahp_artifacts.artifacts a WHERE a.id=g.source_ref->>'id');
CREATE INDEX IF NOT EXISTS checkpoint_transfer_grants_expiry_idx ON ahp_artifacts.checkpoint_transfer_grants(expires_at);
CREATE TABLE IF NOT EXISTS ahp_artifacts.checkpoint_transfer_operations (
 operation text PRIMARY KEY,
 fingerprint text NOT NULL,
 source_ref jsonb NOT NULL,
 source_identity jsonb NOT NULL DEFAULT '{}',
 target_id text NOT NULL,
 status text NOT NULL CHECK (status IN ('dispatched','complete')),
 created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE ahp_artifacts.checkpoint_transfer_operations ADD COLUMN IF NOT EXISTS source_identity jsonb NOT NULL DEFAULT '{}';
CREATE INDEX IF NOT EXISTS checkpoint_transfer_target_idx ON ahp_artifacts.checkpoint_transfer_operations(target_id);
-- A target may be collected while S3 outcome is unknown. Its dispatch receipt
-- MUST survive or a retry would blindly dispatch again under the old key.
`

// TransferGrant is a short, exact-version capability solely for CopyFromTransferGrant.
// It is never accepted by OpenDownloadGrant and contains no reusable task token.
type TransferGrant struct {
	Grant      string    `json:"grant"`
	ArtifactID string    `json:"artifact_id"`
	Version    string    `json:"version"`
	SHA256     string    `json:"sha256"`
	ExpiresAt  time.Time `json:"expires_at"`
}
type TransferGrantRequest struct {
	ID               string `json:"id"`
	Version          string `json:"version"`
	TargetDeployment string `json:"target_deployment"`
	TargetContext    string `json:"target_context"`
	TargetTask       string `json:"target_task"`
}

func transferDenied() error {
	return contracts.NewError(contracts.ErrForbidden, "checkpoint transfer authority unavailable or mismatched")
}
func transferUnknown() error {
	return contracts.NewReasoned(contracts.ErrUnavailable, "transfer_outcome_unknown", "checkpoint copy was dispatched; reconciliation has not proven its sealed outcome")
}

// MintTransferGrant pins the issuing live reader identity and exact version.
// Tenant/subject are fixed to the verified source reader, never request inputs.
func (s *Store) MintTransferGrant(ctx context.Context, reader TransferIdentity, req TransferGrantRequest) (TransferGrant, error) {
	if !s.TransferEnabled() {
		return TransferGrant{}, transferDenied()
	}
	sourceCtx, live, err := s.transferAuthority.Evaluate(ctx, reader)
	if err != nil {
		return TransferGrant{}, err
	}
	for _, v := range []string{req.TargetDeployment, req.TargetContext, req.TargetTask} {
		if err := requireBindingValue("transfer target", v); err != nil {
			return TransferGrant{}, err
		}
	}
	ref, err := s.getForGrant(sourceCtx, live.Scope, req.ID)
	if err != nil {
		return TransferGrant{}, err
	}
	if ref.Tenant != live.Scope.Tenant || ref.Subject != live.Scope.Subject {
		return TransferGrant{}, transferDenied()
	}
	if ref.Version == "" || ref.Version != req.Version {
		return TransferGrant{}, transferDenied()
	}
	if ref.Size > s.transferBounds().MaxBytes {
		return TransferGrant{}, contracts.NewError(contracts.ErrInvalid, "checkpoint transfer exceeds configured byte limit")
	}
	deleted, hold, err := s.lifecycleFlags(ctx, ref.ID)
	if err != nil {
		return TransferGrant{}, err
	}
	if err = grantLifecycleError(deleted, hold); err != nil {
		return TransferGrant{}, err
	}
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return TransferGrant{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	digest, _ := grantDigest(token)
	expires := time.Now().UTC().Add(2 * time.Minute)
	if live.ExpiresAt.Before(expires) {
		expires = live.ExpiresAt
	}
	if !expires.After(time.Now()) {
		return TransferGrant{}, transferDenied()
	}
	identityDoc, _ := json.Marshal(live)
	ref.EffectiveLabels = labels.Set{}
	refDoc, _ := json.Marshal(ref)
	_, err = s.pool.Exec(ctx, `INSERT INTO ahp_artifacts.checkpoint_transfer_grants(digest,source_identity,source_ref,target_deployment,target_context,target_task,expires_at,source_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, digest, identityDoc, refDoc, req.TargetDeployment, req.TargetContext, req.TargetTask, expires, ref.ID)
	if err != nil {
		return TransferGrant{}, storageError("mint transfer", err)
	}
	// Cap expired ticket metadata independently of permanent provenance on
	// completed copies. The cleanup never touches a still-valid capability.
	_, err = s.pool.Exec(ctx, `DELETE FROM ahp_artifacts.checkpoint_transfer_grants WHERE digest IN (SELECT digest FROM ahp_artifacts.checkpoint_transfer_grants WHERE expires_at<now()-interval '1 hour' ORDER BY expires_at LIMIT 256)`)
	if err != nil {
		return TransferGrant{}, storageError("expire transfer grants", err)
	}
	return TransferGrant{token, ref.ID, ref.Version, ref.SHA256, expires}, nil
}

// CopyFromTransferGrant is bounded and restart-safe. Source and target GC locks
// live only for the bounded copy transaction. A durable dispatch record is
// committed BEFORE S3 dispatch. An ambiguous attempt can only reconcile Seal;
// it never blindly dispatches another S3 operation, including after restart.
func (s *Store) CopyFromTransferGrant(ctx context.Context, target TransferIdentity, ticket, key string) (ArtifactRef, error) {
	if !s.TransferEnabled() {
		return ArtifactRef{}, transferDenied()
	}
	if err := validateKey(key); err != nil {
		return ArtifactRef{}, err
	}
	bounds := s.transferBounds()
	ctx, cancel := context.WithTimeout(ctx, bounds.Timeout)
	defer cancel()
	// One bounded transfer per Store keeps a connection available for the
	// durable dispatch and Seal while the GC lock transaction is held. With
	// a one-connection pool this contract is impossible; refuse before effect.
	if s.pool.Config().MaxConns < 2 {
		return ArtifactRef{}, contracts.NewError(contracts.ErrUnavailable, "checkpoint transfer requires at least two owner DB connections")
	}
	select {
	case s.transferSlot <- struct{}{}:
		defer func() { <-s.transferSlot }()
	case <-ctx.Done():
		return ArtifactRef{}, ctx.Err()
	}
	digest, err := grantDigest(ticket)
	if err != nil {
		return ArtifactRef{}, transferDenied()
	}
	var sourceDoc, refDoc []byte
	var dep, contextID, taskID string
	var expires time.Time
	err = s.pool.QueryRow(ctx, `SELECT source_identity,source_ref,target_deployment,target_context,target_task,expires_at FROM ahp_artifacts.checkpoint_transfer_grants WHERE digest=$1`, digest).Scan(&sourceDoc, &refDoc, &dep, &contextID, &taskID, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return ArtifactRef{}, transferDenied()
	}
	if err != nil {
		return ArtifactRef{}, storageError("read transfer", err)
	}
	var source TransferIdentity
	var expected ArtifactRef
	if json.Unmarshal(sourceDoc, &source) != nil || json.Unmarshal(refDoc, &expected) != nil {
		return ArtifactRef{}, transferDenied()
	}
	if target.Scope.Tenant != source.Scope.Tenant || target.Scope.Subject != source.Scope.Subject || target.Scope.Deployment != dep || target.Binding.ContextID != contextID || target.Binding.TaskID != taskID || !time.Now().Before(expires) {
		return ArtifactRef{}, transferDenied()
	}
	if expected.Size > bounds.MaxBytes {
		return ArtifactRef{}, contracts.NewError(contracts.ErrInvalid, "checkpoint transfer exceeds configured byte limit")
	}
	sourceCtx, source, err := s.transferAuthority.Evaluate(ctx, source)
	if err != nil {
		return ArtifactRef{}, err
	}
	targetCtx, target, err := s.transferAuthority.Evaluate(ctx, target)
	if err != nil {
		return ArtifactRef{}, err
	}
	// The ticket never promises retention. A source removed before consumption
	// is refused; a copy already under the owner lock is protected from GC.
	var result ArtifactRef
	publicationAttempted := false
	operation, _ := contracts.Digest([]string{"checkpoint-transfer-v1", target.Scope.Tenant, dep, target.Scope.Subject, contextID, taskID, key})
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockMarkers(ctx, tx, []string{"ahp_artifacts:transfer:" + operation}); err != nil {
			return err
		}
		if err := lockMarkers(ctx, tx, []string{artifactLockKey(source.Scope.Tenant, expected.ID)}); err != nil {
			return err
		}
		var object string
		if err := tx.QueryRow(ctx, `SELECT object FROM ahp_artifacts.artifacts WHERE id=$1 AND tenant=$2 AND NOT deleted FOR UPDATE`, expected.ID, source.Scope.Tenant).Scan(&object); err != nil {
			return storageError("lock transfer source", err)
		}
		ref, err := s.getForGrant(sourceCtx, source.Scope, expected.ID)
		if err != nil {
			return err
		}
		if !sameTransferVersion(ref, expected) {
			return transferDenied()
		}
		deleted, hold, err := s.lifecycleFlags(ctx, ref.ID)
		if err != nil {
			return err
		}
		if err := grantLifecycleError(deleted, hold); err != nil {
			return err
		}
		merged := labels.Merge(ref.Labels, source.Labels, target.Labels)
		if _, err := s.admit(targetCtx, target.Scope, merged); err != nil {
			return err
		}
		// Taint is durable before any bytes become visible to the recipient.
		if err := s.transferAuthority.Accumulate(targetCtx, target, merged); err != nil {
			return err
		}
		sourceLabels := labels.Merge(ref.Labels, source.Labels)
		fingerprint, _ := contracts.Digest(struct {
			Source       ArtifactRef
			Target       TransferIdentity
			SourceLabels labels.Set
		}{expected, TransferIdentity{Scope: target.Scope, Binding: target.Binding}, sourceLabels})
		var priorFingerprint, id, status string
		err = tx.QueryRow(ctx, `SELECT fingerprint,target_id,status FROM ahp_artifacts.checkpoint_transfer_operations WHERE operation=$1`, operation).Scan(&priorFingerprint, &id, &status)
		dispatched := err == nil
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return storageError("read transfer outcome", err)
		}
		if dispatched && priorFingerprint != fingerprint {
			return contracts.NewError(contracts.ErrKeyReuse, "checkpoint transfer key reused with changed provenance or labels")
		}
		var upload Upload
		if !dispatched {
			upload, err = s.BeginUploadWithBinding(targetCtx, target.Scope, target.Binding, "transfer-"+operation, UploadRequest{Name: "checkpoint.bin", MIME: "application/octet-stream", SHA256: ref.SHA256, Size: ref.Size, Labels: merged})
			if err != nil {
				return err
			}
			id = upload.ID
		}
		if err := lockMarkers(ctx, tx, []string{artifactLockKey(target.Scope.Tenant, id)}); err != nil {
			return err
		}
		if !dispatched {
			// Independent commit intentionally survives rollback of the GC lock tx.
			_, err = s.pool.Exec(ctx, `INSERT INTO ahp_artifacts.checkpoint_transfer_operations(operation,fingerprint,source_ref,target_id,status,source_identity) VALUES($1,$2,$3,$4,'dispatched',$5)`, operation, fingerprint, refDoc, id, sourceDoc)
			if err != nil {
				return storageError("record transfer dispatch", err)
			}
			err = s.transferCopier.CopyExact(ctx, s.bucket, object, ref.Version, upload.Object)
			if err != nil {
				return transferUnknown()
			}
		}
		var alreadyPublished bool
		result, alreadyPublished, err = s.verifyTransferCandidate(ctx, tx, target, id, expected)
		if err != nil {
			return err
		}
		if alreadyPublished && status != "complete" {
			return transferUnknown()
		}
		// A source revoke during the bounded copy is not concealed by its ticket.
		_, nowSource, err := s.transferAuthority.Evaluate(ctx, source)
		if err != nil {
			return err
		}
		finalTargetCtx, nowTarget, err := s.transferAuthority.Evaluate(ctx, target)
		if err != nil {
			return err
		}
		current := labels.Merge(ref.Labels, nowSource.Labels, nowTarget.Labels)
		oldDigest, _ := contracts.Digest(sourceLabels)
		newDigest, _ := contracts.Digest(labels.Merge(ref.Labels, nowSource.Labels))
		if oldDigest != newDigest {
			return contracts.NewError(contracts.ErrConflict, "checkpoint transfer authority labels changed during copy")
		}
		if !time.Now().Before(expires) {
			return transferDenied()
		}
		if _, err := s.admit(finalTargetCtx, nowTarget.Scope, current); err != nil {
			return err
		}
		if err := s.transferAuthority.Accumulate(finalTargetCtx, nowTarget, current); err != nil {
			return err
		}
		if !alreadyPublished {
			result.Labels = labels.Merge(result.Labels, current)
		}
		result.EffectiveLabels = labels.Merge(result.Labels, nowTarget.Labels)
		// This is the last authorization decision. A concurrent later revoke
		// orders after this accepted transfer. No fallible authorization or
		// metadata processing runs after the single publication commit.
		publicationAttempted = true
		return publishTransfer(ctx, tx, operation, result, alreadyPublished)
	})
	if err != nil {
		if publicationAttempted {
			return ArtifactRef{}, transferUnknown()
		}
		return ArtifactRef{}, err
	}
	return result, nil
}

func sameTransferVersion(a, b ArtifactRef) bool {
	return a.ID == b.ID && a.Tenant == b.Tenant && a.Deployment == b.Deployment && a.Subject == b.Subject && a.ContextID == b.ContextID && a.TaskID == b.TaskID && a.Version == b.Version && a.SHA256 == b.SHA256 && a.Size == b.Size
}
