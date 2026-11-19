package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	FilePending       = "PENDING"
	FileReady         = "READY"
	FileUnknown       = "SIDE_EFFECT_UNKNOWN"
	FileDeletePending = "DELETE_PENDING"
	FileDeleted       = "DELETED"
	// FileMaxBytes bounds one materialized immutable upload before network dispatch.
	FileMaxBytes = 8 << 20
)

// FileActor comes exclusively from the verified task principal, never JSON.
type FileActor struct {
	Scope         contracts.Scope
	Task, Context string
}

// FileSource is the owning Artifact service, including current source ACLs.
type FileSource interface {
	Get(context.Context, contracts.Scope, string) (artifacts.ArtifactRef, error)
	Open(context.Context, contracts.Scope, string) (io.ReadCloser, artifacts.ArtifactRef, error)
}

// FileProvider owns one configured provider account. Identity pins account and
// endpoint across restarts; rotating credentials must not silently change it.
// Delete must be idempotent for the exact opaque ID (including already absent).
type FileProvider interface {
	Identity() string
	Upload(context.Context, string, string, []byte) (string, error)
	Delete(context.Context, string) error
}

// FileConfig deliberately requires authority, source read labels and routing.
// No default audience or direct URL can bypass the platform's export policy.
type FileConfig struct {
	Source    FileSource
	Authorize func(context.Context, FileActor) error
	AddLabels func(context.Context, FileActor, labels.Set) error
	Route     func(context.Context, FileActor, labels.Set) (string, error)
	Providers map[string]FileProvider
	TTL       time.Duration
	Timeout   time.Duration
}

// FileCopy is an opaque platform reference. Provider IDs stay server-side.
type FileCopy struct {
	ID               string          `json:"id"`
	ArtifactID       string          `json:"artifact_id"`
	SHA256           string          `json:"sha256"`
	Provider         string          `json:"provider"`
	Status           string          `json:"status"`
	ExpiresAt        time.Time       `json:"expires_at"`
	Scope            contracts.Scope `json:"-"`
	Task             string          `json:"-"`
	Context          string          `json:"-"`
	Key              string          `json:"-"`
	RemoteID         string          `json:"-"`
	ProviderIdentity string          `json:"-"`
	EffectKey        string          `json:"-"`
	DeleteRequested  bool            `json:"-"`
	DeleteKey        string          `json:"-"`
}

type FileService struct {
	pool   *pgxpool.Pool
	ledger *operations.Store
	cfg    FileConfig
}

// FilesNotSupported makes missing provider capability an explicit refusal;
// it never assumes that a chat-completions endpoint has a Files API.
func FilesNotSupported() error {
	return contracts.NewReasoned(contracts.ErrUnavailable, "provider_files_not_supported", "model: provider file copies are not configured")
}

func NewFileService(ctx context.Context, pool *pgxpool.Pool, cfg FileConfig) (*FileService, error) {
	if pool == nil || cfg.Source == nil || cfg.Authorize == nil || cfg.AddLabels == nil || cfg.Route == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "model files: source, authority, read-label sink and route are required")
	}
	if cfg.TTL == 0 {
		cfg.TTL = time.Hour
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.TTL < time.Second || cfg.TTL > 24*time.Hour || cfg.Timeout < time.Second || cfg.Timeout > time.Minute {
		return nil, contracts.NewError(contracts.ErrInvalid, "model files: TTL or timeout outside bounds")
	}
	providers := make(map[string]FileProvider, len(cfg.Providers))
	for name, p := range cfg.Providers {
		if name == "" || p == nil || p.Identity() == "" {
			return nil, contracts.NewError(contracts.ErrInvalid, "model files: named provider identity required")
		}
		providers[name] = p
	}
	cfg.Providers = providers
	ledger, err := operations.New(pool, "ahp_model")
	if err != nil {
		return nil, err
	}
	if err = ledger.Migrate(ctx); err != nil {
		return nil, err
	}
	s := &FileService{pool: pool, ledger: ledger, cfg: cfg}
	err = migrateModel(ctx, pool, `CREATE TABLE IF NOT EXISTS ahp_model.provider_files (
 id text PRIMARY KEY,tenant text NOT NULL,subject text NOT NULL,deployment text NOT NULL,
 task text NOT NULL,context_id text NOT NULL,key text NOT NULL,artifact_id text NOT NULL,
 sha256 text NOT NULL,provider text NOT NULL,provider_identity text NOT NULL,
 status text NOT NULL CHECK(status IN ('PENDING','READY','SIDE_EFFECT_UNKNOWN','DELETE_PENDING','DELETED')),
 remote_id text NOT NULL DEFAULT '',effect_key text NOT NULL,delete_requested boolean NOT NULL DEFAULT false,
 delete_key text NOT NULL DEFAULT '',expires_at timestamptz NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
 next_cleanup_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(tenant,deployment,key));
 ALTER TABLE ahp_model.provider_files ADD COLUMN IF NOT EXISTS next_cleanup_at timestamptz NOT NULL DEFAULT now();
 CREATE INDEX IF NOT EXISTS provider_files_cleanup ON ahp_model.provider_files(expires_at) WHERE status<>'DELETED';
 CREATE UNIQUE INDEX IF NOT EXISTS provider_files_delete_key ON ahp_model.provider_files(tenant,deployment,delete_key) WHERE delete_key<>'';
 CREATE TABLE IF NOT EXISTS ahp_model.file_erased_subjects(tenant text NOT NULL,subject text NOT NULL,PRIMARY KEY(tenant,subject));`+tenantInsertGuards("provider_files"))
	return s, err
}

func (a FileActor) validate() error {
	if err := a.Scope.Validate(); err != nil {
		return err
	}
	if a.Task == "" || a.Context == "" || len(a.Task) > 256 || len(a.Context) > 256 {
		return contracts.NewError(contracts.ErrInvalid, "model files: task and context required")
	}
	return nil
}

const fileColumns = `id,tenant,subject,deployment,task,context_id,key,artifact_id,sha256,provider,provider_identity,status,remote_id,effect_key,delete_requested,delete_key,expires_at`

func scanFile(row pgx.Row) (FileCopy, error) {
	var f FileCopy
	err := row.Scan(&f.ID, &f.Scope.Tenant, &f.Scope.Subject, &f.Scope.Deployment, &f.Task, &f.Context, &f.Key, &f.ArtifactID, &f.SHA256, &f.Provider, &f.ProviderIdentity, &f.Status, &f.RemoteID, &f.EffectKey, &f.DeleteRequested, &f.DeleteKey, &f.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrNotFound, "model file not found")
	}
	return f, err
}
func (s *FileService) lookup(ctx context.Context, a FileActor, id string) (FileCopy, error) {
	return scanFile(s.pool.QueryRow(ctx, `SELECT `+fileColumns+` FROM ahp_model.provider_files WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND task=$4 AND context_id=$5 AND id=$6`, a.Scope.Tenant, a.Scope.Subject, a.Scope.Deployment, a.Task, a.Context, id))
}
func (s *FileService) driver(f FileCopy) (FileProvider, error) {
	p := s.cfg.Providers[f.Provider]
	if p == nil || p.Identity() != f.ProviderIdentity {
		return nil, FilesNotSupported()
	}
	return p, nil
}
func (s *FileService) source(ctx context.Context, a FileActor, id string) (artifacts.ArtifactRef, string, error) {
	if err := s.cfg.Authorize(ctx, a); err != nil {
		return artifacts.ArtifactRef{}, "", err
	}
	ref, err := s.cfg.Source.Get(artifacts.WithCallerContext(ctx, a.Context), a.Scope, id)
	if err != nil {
		return ref, "", err
	}
	if ref.ID != id || ref.Tenant != a.Scope.Tenant || ref.Size < 0 || ref.Size > FileMaxBytes || len(ref.SHA256) != 64 {
		return ref, "", contracts.NewError(contracts.ErrInvalid, "model files: invalid or oversized sealed artifact")
	}
	if err = s.cfg.AddLabels(ctx, a, labels.Merge(ref.Labels, ref.EffectiveLabels)); err != nil {
		return ref, "", err
	}
	provider, err := s.cfg.Route(ctx, a, labels.Merge(ref.Labels, ref.EffectiveLabels))
	return ref, provider, err
}

// Copy creates/replays a copy. An ambiguous upload is never dispatched twice,
// even after process restart or when another replica receives the same key.
func (s *FileService) Copy(ctx context.Context, a FileActor, key, artifactID string) (FileCopy, error) {
	ctx, stop := context.WithTimeout(ctx, 2*s.cfg.Timeout)
	defer stop()
	if err := a.validate(); err != nil {
		return FileCopy{}, err
	}
	if key == "" || len(key) > 128 || artifactID == "" || len(artifactID) > 256 {
		return FileCopy{}, contracts.NewError(contracts.ErrInvalid, "model files: bounded key and artifact required")
	}
	ref, provider, err := s.source(ctx, a, artifactID)
	if err != nil {
		return FileCopy{}, err
	}
	p := s.cfg.Providers[provider]
	if p == nil {
		return FileCopy{}, FilesNotSupported()
	}
	effectKey := contracts.OperationKey(a.Scope, a.Task, "model.file", key)
	var f FileCopy
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, "ahp_model/file-subject/"+a.Scope.Tenant+"/"+a.Scope.Subject); e != nil {
			return e
		}
		var erased bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_model.file_erased_subjects WHERE tenant=$1 AND subject=$2)`, a.Scope.Tenant, a.Scope.Subject).Scan(&erased); e != nil {
			return e
		}
		if erased {
			return contracts.NewError(contracts.ErrForbidden, "model files: subject erased")
		}
		_, e := tx.Exec(ctx, `INSERT INTO ahp_model.provider_files(id,tenant,subject,deployment,task,context_id,key,artifact_id,sha256,provider,provider_identity,status,effect_key,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'PENDING',$12,now()+$13*interval '1 millisecond') ON CONFLICT(tenant,deployment,key) DO NOTHING`, contracts.NewID(), a.Scope.Tenant, a.Scope.Subject, a.Scope.Deployment, a.Task, a.Context, key, artifactID, ref.SHA256, provider, p.Identity(), effectKey, s.cfg.TTL.Milliseconds())
		if e != nil {
			return e
		}
		f, e = scanFile(tx.QueryRow(ctx, `SELECT `+fileColumns+` FROM ahp_model.provider_files WHERE tenant=$1 AND deployment=$2 AND key=$3`, a.Scope.Tenant, a.Scope.Deployment, key))
		if e != nil {
			return e
		}
		if f.Scope.Subject != a.Scope.Subject {
			return contracts.NewError(contracts.ErrNotFound, "model file not found")
		}
		if f.Task != a.Task || f.Context != a.Context || f.ArtifactID != artifactID || f.SHA256 != ref.SHA256 || f.Provider != provider || f.ProviderIdentity != p.Identity() {
			return contracts.NewError(contracts.ErrKeyReuse, "model file key reused with different request")
		}
		return nil
	})
	if err != nil {
		return FileCopy{}, err
	}
	if f.Status != FilePending && f.Status != FileUnknown {
		return f, nil
	}
	args, _ := json.Marshal(struct{ Artifact, SHA, Provider string }{artifactID, ref.SHA256, f.ProviderIdentity})
	op, err := s.ledger.Get(ctx, a.Scope, effectKey)
	if contracts.Code(err) == contracts.ErrNotFound {
		guard, _ := contracts.Digest([]string{a.Scope.Tenant, p.Identity(), artifactID})
		op, err = s.ledger.Accept(ctx, a.Scope, operations.Request{Key: effectKey, Kind: "model.file.upload", Target: f.ID, Guard: guard, Arguments: args})
	}
	if err != nil {
		return FileCopy{}, err
	}
	if op.Status == operations.StatusSucceeded {
		return s.finishUpload(ctx, f, stringResult(op.Result))
	}
	if op.Status != operations.StatusAccepted {
		f.Status = FileUnknown
		return f, nil
	}
	rc, opened, err := s.cfg.Source.Open(artifacts.WithCallerContext(ctx, a.Context), a.Scope, artifactID)
	if err != nil {
		return FileCopy{}, err
	}
	data, err := io.ReadAll(io.LimitReader(rc, FileMaxBytes+1))
	_ = rc.Close()
	if err != nil {
		return FileCopy{}, contracts.NewError(contracts.ErrUnavailable, "model files: artifact bytes unavailable")
	}
	sum := sha256.Sum256(data)
	if opened.SHA256 != ref.SHA256 || opened.Version != ref.Version || int64(len(data)) != ref.Size || hex.EncodeToString(sum[:]) != ref.SHA256 {
		return FileCopy{}, contracts.NewError(contracts.ErrConflict, "model files: sealed artifact mismatch")
	}
	// Refresh both authority and source labels after materializing the bytes.
	fresh, route, err := s.source(ctx, a, artifactID)
	if err != nil {
		return FileCopy{}, err
	}
	if fresh.SHA256 != f.SHA256 || route != f.Provider {
		return FileCopy{}, contracts.NewError(contracts.ErrForbidden, "model files: export route changed")
	}
	_, err = s.ledger.DispatchChecked(ctx, a.Scope, effectKey, func(ctx context.Context, tx pgx.Tx) error {
		var deny bool
		if e := tx.QueryRow(ctx, `SELECT delete_requested OR expires_at<=now() OR EXISTS(SELECT 1 FROM ahp_model.file_erased_subjects WHERE tenant=$2 AND subject=$3) FROM ahp_model.provider_files WHERE id=$1 FOR UPDATE`, f.ID, a.Scope.Tenant, a.Scope.Subject).Scan(&deny); e != nil {
			return e
		}
		if deny {
			return contracts.NewError(contracts.ErrForbidden, "model files: copy is fenced")
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, operations.ErrAlreadyDispatched) {
			f.Status = FileUnknown
			return f, nil
		}
		return FileCopy{}, err
	}
	_, err = s.pool.Exec(ctx, `UPDATE ahp_model.provider_files SET status='SIDE_EFFECT_UNKNOWN' WHERE id=$1 AND status='PENDING'`, f.ID)
	if err != nil {
		return FileCopy{}, err
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	remote, uploadErr := p.Upload(callCtx, f.ID+".bin", ref.MIME, data)
	// Persist known outcomes even if the request disappeared after the effect.
	settle, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	if uploadErr != nil || !remoteFileID.MatchString(remote) {
		_, _ = s.ledger.Fence(settle, a.Scope, effectKey)
		f.Status = FileUnknown
		return f, contracts.NewReasoned(contracts.ErrConflict, "provider_file_upload_unknown", "model: provider file upload requires reconciliation")
	}
	receipt, _ := json.Marshal(remote)
	if _, err = s.ledger.Complete(settle, a.Scope, effectKey, receipt, false); err != nil {
		existing, e := s.ledger.Get(settle, a.Scope, effectKey)
		if e != nil {
			return FileCopy{}, e
		}
		if existing.Status != operations.StatusSideEffectUnknown {
			return FileCopy{}, err
		}
		if _, err = s.ledger.Reconcile(settle, a.Scope, effectKey, receipt, false); err != nil {
			return FileCopy{}, err
		}
	}
	return s.finishUpload(settle, f, remote)
}

var remoteFileID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,255}$`)

func stringResult(raw json.RawMessage) string { var s string; _ = json.Unmarshal(raw, &s); return s }
func (s *FileService) finishUpload(ctx context.Context, f FileCopy, remote string) (FileCopy, error) {
	if !remoteFileID.MatchString(remote) {
		return FileCopy{}, contracts.NewError(contracts.ErrConflict, "model files: invalid provider receipt")
	}
	return scanFile(s.pool.QueryRow(ctx, `UPDATE ahp_model.provider_files SET remote_id=$2,status=CASE WHEN delete_requested OR expires_at<=now() OR EXISTS(SELECT 1 FROM ahp_model.erased_tenants WHERE tenant=$3) THEN 'DELETE_PENDING' ELSE 'READY' END WHERE id=$1 AND status IN ('PENDING','SIDE_EFFECT_UNKNOWN','READY','DELETE_PENDING') RETURNING `+fileColumns, f.ID, remote, f.Scope.Tenant))
}

func (s *FileService) Get(ctx context.Context, a FileActor, id string) (FileCopy, error) {
	if err := a.validate(); err != nil {
		return FileCopy{}, err
	}
	if err := s.cfg.Authorize(ctx, a); err != nil {
		return FileCopy{}, err
	}
	f, err := s.lookup(ctx, a, id)
	if err != nil {
		return f, err
	}
	if f.Status == FileReady {
		_, route, e := s.source(ctx, a, f.ArtifactID)
		if e != nil {
			return FileCopy{}, e
		}
		if route != f.Provider {
			return FileCopy{}, contracts.NewError(contracts.ErrForbidden, "model files: export policy changed")
		}
		if !time.Now().Before(f.ExpiresAt) {
			f.Status = FileDeletePending
		}
	}
	return f, nil
}

// Delete is owner/task scoped and stable by key. Cleanup never exports bytes.
func (s *FileService) Delete(ctx context.Context, a FileActor, key, id string) (FileCopy, error) {
	if err := a.validate(); err != nil {
		return FileCopy{}, err
	}
	if key == "" || len(key) > 128 {
		return FileCopy{}, contracts.NewError(contracts.ErrInvalid, "model files: deletion key required")
	}
	if err := s.cfg.Authorize(ctx, a); err != nil {
		return FileCopy{}, err
	}
	f, err := s.lookup(ctx, a, id)
	if err != nil {
		return f, err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE ahp_model.provider_files SET delete_requested=true,delete_key=CASE WHEN delete_key='' THEN $2 ELSE delete_key END WHERE id=$1 AND (delete_key='' OR delete_key=$2)`, f.ID, key)
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" {
			return FileCopy{}, contracts.NewError(contracts.ErrKeyReuse, "model file deletion key already used")
		}
		return f, err
	}
	if tag.RowsAffected() != 1 {
		return FileCopy{}, contracts.NewError(contracts.ErrKeyReuse, "model file deletion key differs")
	}
	return s.cleanup(ctx, f)
}

func (s *FileService) cleanup(ctx context.Context, f FileCopy) (FileCopy, error) {
	// Receipt adoption closes a crash between ledger commit and inventory update.
	if f.RemoteID == "" && f.Status != FileDeleted {
		op, err := s.ledger.Fence(ctx, f.Scope, f.EffectKey)
		if err != nil && contracts.Code(err) != contracts.ErrNotFound {
			return f, err
		}
		if op.Status == operations.StatusSucceeded {
			f, err = s.finishUpload(ctx, f, stringResult(op.Result))
			if err != nil {
				return f, err
			}
		}
		if f.RemoteID == "" {
			if op.Status == operations.StatusDispatched || op.Status == operations.StatusSideEffectUnknown {
				return f, contracts.NewReasoned(contracts.ErrUnavailable, "tenant_model_files_pending", "model: provider upload outcome is unknown")
			}
			return scanFile(s.pool.QueryRow(ctx, `UPDATE ahp_model.provider_files SET status='DELETED',delete_requested=true WHERE id=$1 AND remote_id='' RETURNING `+fileColumns, f.ID))
		}
	}
	if f.Status == FileDeleted {
		return f, nil
	}
	p, err := s.driver(f)
	if err != nil {
		return f, err
	}
	if _, err = s.pool.Exec(ctx, `UPDATE ahp_model.provider_files SET delete_requested=true,status='DELETE_PENDING' WHERE id=$1 AND status<>'DELETED'`, f.ID); err != nil {
		return f, err
	}
	callCtx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	if err = p.Delete(callCtx, f.RemoteID); err != nil {
		return f, contracts.NewReasoned(contracts.ErrUnavailable, "provider_file_delete_pending", "model: provider copy deletion is unconfirmed")
	}
	settle, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	return scanFile(s.pool.QueryRow(settle, `UPDATE ahp_model.provider_files SET status='DELETED',delete_requested=true WHERE id=$1 RETURNING `+fileColumns, f.ID))
}

// Sweep is a bounded owner-maintenance operation, never exposed to task HTTP.
// It only deletes known exact copies; an unknown upload remains pending.
func (s *FileService) Sweep(ctx context.Context, limit int) (int, error) {
	return s.sweepScheduled(ctx, "", "", limit, true)
}
func (s *FileService) sweep(ctx context.Context, tenant, subject string, limit int) (int, error) {
	return s.sweepScheduled(ctx, tenant, subject, limit, false)
}
func (s *FileService) sweepScheduled(ctx context.Context, tenant, subject string, limit int, scheduled bool) (int, error) {
	if limit < 1 || limit > 1000 {
		return 0, contracts.NewError(contracts.ErrInvalid, "model files: cleanup limit outside bounds")
	}
	rows, err := s.pool.Query(ctx, `SELECT `+fileColumns+` FROM ahp_model.provider_files WHERE status<>'DELETED' AND (delete_requested OR expires_at<=now()) AND ($1='' OR tenant=$1) AND ($2='' OR subject=$2) AND (NOT $4 OR next_cleanup_at<=now()) ORDER BY next_cleanup_at,expires_at,id LIMIT $3`, tenant, subject, limit, scheduled)
	if err != nil {
		return 0, err
	}
	var files []FileCopy
	for rows.Next() {
		f, e := scanFile(rows)
		if e != nil {
			rows.Close()
			return 0, e
		}
		files = append(files, f)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	count := 0
	var first error
	for _, f := range files {
		if ctx.Err() != nil {
			return count, ctx.Err()
		}
		// Rotate failures behind other due copies; UNKNOWN cannot starve cleanup.
		if _, e := s.pool.Exec(ctx, `UPDATE ahp_model.provider_files SET next_cleanup_at=now()+interval '30 seconds' WHERE id=$1`, f.ID); e != nil {
			return count, e
		}
		if _, e := s.cleanup(ctx, f); e != nil {
			if first == nil {
				first = e
			}
		} else {
			count++
		}
	}
	return count, first
}

func (s *FileService) FenceTenant(ctx context.Context, tenant string) error {
	if tenant == "" {
		return contracts.NewError(contracts.ErrInvalid, "model files: tenant required")
	}
	if err := s.ledger.FenceTenant(ctx, tenant); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `UPDATE ahp_model.provider_files SET delete_requested=true WHERE tenant=$1`, tenant)
	return err
}
func (s *FileService) DeleteTenant(ctx context.Context, tenant string) error {
	if err := s.FenceTenant(ctx, tenant); err != nil {
		return err
	}
	if _, err := s.sweep(ctx, tenant, "", 1000); err != nil {
		return err
	}
	var pending bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_model.provider_files WHERE tenant=$1 AND status<>'DELETED')`, tenant).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return contracts.NewReasoned(contracts.ErrUnavailable, "tenant_model_files_pending", "model: provider copy cleanup pending")
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM ahp_model.provider_files WHERE tenant=$1 AND status='DELETED'`, tenant)
	return err
}

// DeleteSubject is a trusted owner hook; it does not require the now-revoked
// user's token. Its permanent barrier precedes the inventory of copies.
func (s *FileService) FenceSubject(ctx context.Context, tenant, subject string) error {
	if tenant == "" || subject == "" {
		return contracts.NewError(contracts.ErrInvalid, "model files: tenant and subject required")
	}
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "ahp_model/file-subject/"+tenant+"/"+subject); e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, `INSERT INTO ahp_model.file_erased_subjects VALUES($1,$2) ON CONFLICT DO NOTHING`, tenant, subject); e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
func (s *FileService) DeleteSubject(ctx context.Context, tenant, subject string) error {
	if err := s.FenceSubject(ctx, tenant, subject); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `UPDATE ahp_model.provider_files SET delete_requested=true WHERE tenant=$1 AND subject=$2`, tenant, subject); err != nil {
		return err
	}
	var err error
	if _, err = s.sweep(ctx, tenant, subject, 1000); err != nil {
		return err
	}
	var pending bool
	if err = s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_model.provider_files WHERE tenant=$1 AND subject=$2 AND status<>'DELETED')`, tenant, subject).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return contracts.NewReasoned(contracts.ErrUnavailable, "subject_model_files_pending", "model: provider copy cleanup pending")
	}
	_, err = s.pool.Exec(ctx, `DELETE FROM ahp_model.provider_files WHERE tenant=$1 AND subject=$2 AND status='DELETED'`, tenant, subject)
	return err
}
