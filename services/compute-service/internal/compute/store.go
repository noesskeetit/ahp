package compute

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaFS embed.FS

const (
	maxKeyBytes     = 512
	maxCommandBytes = 1 << 20
	maxOutputBytes  = 4 << 20
	maxSpecBytes    = 64 << 10
	minLeaseTTL     = time.Second
	maxLeaseTTL     = 12 * time.Hour
	maxSessionTTL   = 24 * time.Hour
)

// Store is the durable Compute core bound to one PostgreSQL pool. It is safe
// for concurrent use; every method runs in a single short transaction.
type Store struct {
	pool *pgxpool.Pool
}

// New binds the Compute core to an existing pgx pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool exposes the pool for health checks only; never reach ahp_compute tables
// through it.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Migrate creates the ahp_compute schema. Idempotent and non-destructive.
func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return invalid("compute: store is not initialised")
	}
	ddl, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("compute: read embedded schema: %w", err)
	}
	if _, err := s.pool.Exec(ctx, string(ddl)); err != nil {
		return fmt.Errorf("compute: migrate: %w", err)
	}
	if _, err := s.pool.Exec(ctx, exportBindingDDL); err != nil {
		return err
	}
	return s.tenantGuards(ctx)
}

// ---------------------------------------------------------------------------
// Computers
// ---------------------------------------------------------------------------

type computerColumns struct {
	id, tenant, deployment, subject, class, profile, status, volumeID, workspaceID, realm string
	incarnation                                                                           int64
	quota                                                                                 []byte
	createdAt, updatedAt                                                                  time.Time
}

// NewComputerRequest carries the body-only inputs of CreateComputer. Scope is
// supplied separately from verified identity and never from this struct.
type NewComputerRequest struct {
	Class       Class
	Profile     Profile
	WorkspaceID string
	Realm       string
	Quota       Quota
}

// CreateComputer creates or idempotently replays a Computer. A personal
// Computer requires the trusted class S; a second active personal Computer for
// the same (tenant, deployment, subject, realm) is rejected as a conflict.
func (s *Store) CreateComputer(ctx context.Context, scope contracts.Scope, req NewComputerRequest, key string) (Computer, error) {
	if err := scope.Validate(); err != nil {
		return Computer{}, asInvalid(err)
	}
	if !req.Class.Valid() {
		return Computer{}, invalid("class must be one of S, C, X")
	}
	if !req.Profile.Valid() {
		return Computer{}, invalid("profile must be one of ephemeral, project, personal")
	}
	if !PersonalRequiresTrustedClass(req.Profile, req.Class) {
		return Computer{}, contracts.NewError(contracts.ErrForbidden,
			"personal Computers are reserved for the trusted class S acting as the employee")
	}
	if err := validOptionalIdentifier(req.WorkspaceID, "workspace id"); err != nil {
		return Computer{}, err
	}
	if err := validOptionalIdentifier(req.Realm, "realm"); err != nil {
		return Computer{}, err
	}
	if req.Profile == ProfileProject && req.WorkspaceID == "" {
		return Computer{}, invalid("project Computers require a workspace id")
	}
	if err := validIdentifier(key, "idempotency key"); err != nil {
		return Computer{}, err
	}
	if err := validateQuota(req.Quota); err != nil {
		return Computer{}, err
	}
	// Persist the resolved class envelope, not the raw request: a caller that
	// omits quota gets the class hard limits (so the process budget enforced at
	// dispatch is real), and class X stays clamped to its hard ceiling.
	req.Quota = ResolveQuota(req.Class, req.Quota)
	digest, err := contracts.Digest(map[string]any{
		"class": string(req.Class), "profile": string(req.Profile),
		"workspace_id": req.WorkspaceID, "realm": req.Realm, "quota": req.Quota,
	})
	if err != nil {
		return Computer{}, asInvalid(err)
	}
	quotaJSON, err := json.Marshal(req.Quota)
	if err != nil {
		return Computer{}, asInvalid(err)
	}
	id := contracts.NewID()
	var out Computer
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.computers
				(id, tenant, deployment, subject, class, profile, status, volume_id,
				 workspace_id, realm, quota, idem_key, body_digest)
			VALUES ($1,$2,$3,$4,$5,$6,'provisioning',$7,$8,$9,$10::jsonb,$11,$12)
			ON CONFLICT (tenant, deployment, subject, idem_key) DO NOTHING
			RETURNING `+computerSelect,
			id, scope.Tenant, scope.Deployment, scope.Subject, string(req.Class), string(req.Profile),
			"vol-"+id, req.WorkspaceID, req.Realm, string(quotaJSON), key, digest)
		c, err := scanComputer(row)
		if err == nil {
			out = c
			// The personal Computer's state starts as a revision (PDF 3.6):
			// an empty main tree at seq 0, appended to by typed patches.
			if c.Profile == ProfilePersonal {
				digest, err := stateTreeDigest(StateTree{})
				if err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `
					INSERT INTO ahp_compute.state_revisions
						(id, computer_id, seq, branch, parent_id, base_seq, tree, digest)
					VALUES ($1,$2,0,'main','',0,'{}'::jsonb,$3)
					ON CONFLICT (computer_id, branch, seq) DO NOTHING`,
					contracts.NewID(), c.ID, digest); err != nil {
					return err
				}
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		// The idempotency key already exists in this scope.
		existing, getErr := s.getComputerByKey(ctx, tx, scope, key)
		if getErr != nil {
			return getErr
		}
		if existing.digest != digest {
			return contracts.NewError(contracts.ErrKeyReuse,
				"idempotency key was already used with a different computer body")
		}
		out = existing.Computer
		return nil
	})
	if isUniqueViolation(err) {
		return Computer{}, contracts.NewError(contracts.ErrConflict,
			"an active Computer already exists for this subject, profile and workspace/realm")
	}
	if err != nil {
		return Computer{}, err
	}
	return out, nil
}

// GetComputer returns a Computer in scope, or NOT_FOUND.
func (s *Store) GetComputer(ctx context.Context, scope contracts.Scope, id string) (Computer, error) {
	if err := validIdentifier(id, "computer id"); err != nil {
		return Computer{}, err
	}
	var out Computer
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getComputer(ctx, tx, scope, id)
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

func (s *Store) getComputer(ctx context.Context, tx pgx.Tx, scope contracts.Scope, id string) (Computer, error) {
	row := tx.QueryRow(ctx, `SELECT `+computerSelect+`
		FROM ahp_compute.computers
		WHERE id = $1 AND tenant = $2 AND deployment = $3 AND subject = $4`,
		id, scope.Tenant, scope.Deployment, scope.Subject)
	c, err := scanComputer(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Computer{}, notFound("computer")
	}
	return c, err
}

type computerWithDigest struct {
	Computer
	digest string
}

func (s *Store) getComputerByKey(ctx context.Context, tx pgx.Tx, scope contracts.Scope, key string) (computerWithDigest, error) {
	row := tx.QueryRow(ctx, `SELECT `+computerSelect+`, body_digest
		FROM ahp_compute.computers
		WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND idem_key = $4`,
		scope.Tenant, scope.Deployment, scope.Subject, key)
	var c computerColumns
	var digest string
	if err := row.Scan(&c.id, &c.tenant, &c.deployment, &c.subject, &c.class, &c.profile,
		&c.status, &c.volumeID, &c.workspaceID, &c.realm, &c.incarnation, &c.quota,
		&c.createdAt, &c.updatedAt, &digest); err != nil {
		return computerWithDigest{}, err
	}
	cmp, err := computerFromColumns(c)
	if err != nil {
		return computerWithDigest{}, err
	}
	return computerWithDigest{Computer: cmp, digest: digest}, nil
}

const computerSelect = `id, tenant, deployment, subject, class, profile, status, volume_id,
	workspace_id, realm, incarnation, quota, created_at, updated_at`

func scanComputer(row scanner) (Computer, error) {
	var c computerColumns
	if err := row.Scan(&c.id, &c.tenant, &c.deployment, &c.subject, &c.class, &c.profile,
		&c.status, &c.volumeID, &c.workspaceID, &c.realm, &c.incarnation, &c.quota,
		&c.createdAt, &c.updatedAt); err != nil {
		return Computer{}, err
	}
	return computerFromColumns(c)
}

func computerFromColumns(c computerColumns) (Computer, error) {
	var q Quota
	if len(c.quota) > 0 {
		if err := json.Unmarshal(c.quota, &q); err != nil {
			return Computer{}, fmt.Errorf("compute: decode quota: %w", err)
		}
	}
	return Computer{
		ID:    c.id,
		Scope: contracts.Scope{Tenant: c.tenant, Deployment: c.deployment, Subject: c.subject},
		Class: Class(c.class), Profile: Profile(c.profile), Status: ComputerStatus(c.status),
		VolumeID: c.volumeID, WorkspaceID: c.workspaceID, Realm: c.realm,
		Incarnation: c.incarnation, Quota: q, CreatedAt: c.createdAt.UTC(), UpdatedAt: c.updatedAt.UTC(),
	}, nil
}

// ---------------------------------------------------------------------------
// Incarnations + volume fencing
// ---------------------------------------------------------------------------

// AcquireIncarnation starts a fresh disposable VM for a Computer: it takes the
// next incarnation sequence, raises the volume fence (so any older holder is
// fenced off) and returns the new incarnation. VMGenID and Backend are filled
// by the caller from the actual backend after the VM is up; store records the
// provisioning fact and fencing atomically.
func (s *Store) AcquireIncarnation(ctx context.Context, scope contracts.Scope, computerID, backend, node, vmgenID string) (Incarnation, error) {
	if err := validIdentifier(computerID, "computer id"); err != nil {
		return Incarnation{}, err
	}
	if err := validOptionalIdentifier(backend, "backend"); err != nil {
		return Incarnation{}, err
	}
	if err := validOptionalIdentifier(node, "node"); err != nil {
		return Incarnation{}, err
	}
	if err := validOptionalIdentifier(vmgenID, "vmgenid"); err != nil {
		return Incarnation{}, err
	}
	id := contracts.NewID()
	var out Incarnation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		var seq int64
		if err := tx.QueryRow(ctx, `
			UPDATE ahp_compute.computers
			SET incarnation = incarnation + 1, status = 'active', updated_at = clock_timestamp()
			WHERE id = $1
			RETURNING incarnation`, computerID).Scan(&seq); err != nil {
			return err
		}
		// Raise the volume fence for this incarnation's attach.
		var fence int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.volume_fences (computer_id, volume_id, fence, incarnation, updated_at)
			SELECT $1, c.volume_id, 1, $2, clock_timestamp()
			FROM ahp_compute.computers c WHERE c.id = $1
			ON CONFLICT (computer_id, volume_id) DO UPDATE
				SET fence = ahp_compute.volume_fences.fence + 1,
				    incarnation = EXCLUDED.incarnation,
				    updated_at = clock_timestamp()
			RETURNING fence`, computerID, id).Scan(&fence); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.incarnations
				(id, computer_id, seq, status, backend, node, vmgenid, fence)
			VALUES ($1,$2,$3,'running',$4,$5,$6,$7)
			RETURNING `+incarnationSelect,
			id, computerID, seq, backend, node, vmgenID, fence).Scan(
			&out.ID, &out.ComputerID, &out.Seq, &out.Status, &out.Backend, &out.Node,
			&out.VMGenID, &out.Fence, &out.AcquiredAt, &out.StoppedAt); err != nil {
			return err
		}
		out.AcquiredAt = out.AcquiredAt.UTC()
		// Awakening a Computer starts billing: the open cost window (if none
		// is already open) covers this incarnation and everything else that
		// keeps the Computer awake until the next sleep.
		if _, err := tx.Exec(ctx, `
			INSERT INTO ahp_compute.cost_ledger (id, computer_id, kind, started_at)
			VALUES ($1, $2, 'active', clock_timestamp())
			ON CONFLICT (computer_id) WHERE ended_at IS NULL DO NOTHING`,
			contracts.NewID(), computerID); err != nil {
			return err
		}
		return nil
	})
	return out, err
}

const incarnationSelect = `id, computer_id, seq, status, backend, node, vmgenid, fence, acquired_at, stopped_at`

// StopIncarnation destroys an incarnation. A stopped VM never returns to the
// pool; the pool is refilled by a fresh acquire.
func (s *Store) StopIncarnation(ctx context.Context, scope contracts.Scope, computerID, incarnationID string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		// Serialize lifecycle mutation with the quiescent export publication guard.
		if _, err := tx.Exec(ctx, `SELECT id FROM ahp_compute.computers WHERE id=$1 FOR UPDATE`, computerID); err != nil {
			return err
		}

		tag, err := tx.Exec(ctx, `
			UPDATE ahp_compute.incarnations
			SET status = 'stopped', stopped_at = clock_timestamp()
			WHERE id = $1 AND computer_id = $2 AND status <> 'stopped'`, incarnationID, computerID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return notFound("incarnation")
		}
		return nil
	})
}

// SetComputerStatus updates the lifecycle status of a Computer (suspend,
// destroy, activate). Destroy is terminal.
func (s *Store) SetComputerStatus(ctx context.Context, scope contracts.Scope, computerID string, status ComputerStatus) (Computer, error) {
	switch status {
	case ComputerProvisioning, ComputerActive, ComputerSuspended, ComputerDestroyed:
	default:
		return Computer{}, invalid("unknown computer status")
	}
	var out Computer
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		before, err := s.getComputer(ctx, tx, scope, computerID)
		if err != nil {
			return err
		}
		if before.Status == ComputerDestroyed {
			return contracts.NewError(contracts.ErrConflict, "a destroyed computer cannot be revived")
		}
		c, err := scanComputer(tx.QueryRow(ctx, `
			UPDATE ahp_compute.computers
			SET status = $5, updated_at = clock_timestamp()
			WHERE id = $1 AND tenant = $2 AND deployment = $3 AND subject = $4
			RETURNING `+computerSelect,
			computerID, scope.Tenant, scope.Deployment, scope.Subject, string(status)))
		if err != nil {
			return err
		}
		out = c
		return nil
	})
	return out, err
}

// CurrentFence returns the live volume fence of a Computer's volume.
func (s *Store) CurrentFence(ctx context.Context, scope contracts.Scope, computerID string) (VolumeFence, error) {
	var out VolumeFence
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getComputer(ctx, tx, scope, computerID)
		if err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
			SELECT computer_id, volume_id, fence, incarnation, updated_at
			FROM ahp_compute.volume_fences WHERE computer_id = $1 AND volume_id = $2`,
			computerID, c.VolumeID).Scan(&out.ComputerID, &out.VolumeID, &out.Fence, &out.Incarnation, &out.UpdatedAt)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return VolumeFence{}, notFound("volume fence")
	}
	out.UpdatedAt = out.UpdatedAt.UTC()
	return out, err
}

// CheckFence reports whether incarnation incarnationID still holds the live
// volume fence. A backend presenting a stale fence must be rejected with
// STALE_EPOCH so two incarnations never write the same volume.
func (s *Store) CheckFence(ctx context.Context, scope contracts.Scope, computerID, incarnationID string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getComputer(ctx, tx, scope, computerID)
		if err != nil {
			return err
		}
		var holder string
		if err := tx.QueryRow(ctx, `
			SELECT incarnation FROM ahp_compute.volume_fences
			WHERE computer_id = $1 AND volume_id = $2`, computerID, c.VolumeID).Scan(&holder); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return staleEpoch()
			}
			return err
		}
		if holder != incarnationID {
			return staleEpoch()
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Activity leases
// ---------------------------------------------------------------------------

// AcquireLease takes an activity lease that keeps the Computer awake. A writer
// lease is exclusive: taking one while another holder owns it fails with
// LEASE_LOST semantics surfaced as CONFLICT.
func (s *Store) AcquireLease(ctx context.Context, scope contracts.Scope, computerID string, kind ActivityLeaseKind, holder string, ttl time.Duration) (string, error) {
	if !kind.Valid() {
		return "", invalid("unknown activity lease kind")
	}
	if err := validIdentifier(holder, "lease holder"); err != nil {
		return "", err
	}
	if ttl < minLeaseTTL || ttl > maxLeaseTTL {
		return "", invalid("lease ttl must be between 1s and 12h")
	}
	id := contracts.NewID()
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		if kind == LeaseWriter {
			var busy bool
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM ahp_compute.activity_leases
					WHERE computer_id = $1 AND kind = 'writer' AND expires > clock_timestamp() AND holder <> $2)`,
				computerID, holder).Scan(&busy); err != nil {
				return err
			}
			if busy {
				return contracts.NewError(contracts.ErrConflict,
					"the working tree is held by another writer lease")
			}
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO ahp_compute.activity_leases (id, computer_id, kind, holder, expires)
			VALUES ($1,$2,$3,$4, clock_timestamp() + $5::interval)
			ON CONFLICT (computer_id, kind, holder)
			DO UPDATE SET expires = clock_timestamp() + $5::interval`,
			id, computerID, string(kind), holder, fmt.Sprintf("%d microseconds", int64(ttl/time.Microsecond)))
		return err
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

// ReleaseLease drops an activity lease held by holder.
func (s *Store) ReleaseLease(ctx context.Context, scope contracts.Scope, computerID string, kind ActivityLeaseKind, holder string) error {
	if !kind.Valid() {
		return invalid("unknown activity lease kind")
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			DELETE FROM ahp_compute.activity_leases
			WHERE computer_id = $1 AND kind = $2 AND holder = $3`,
			computerID, string(kind), holder)
		return err
	})
}

// ActiveLeases counts unexpired leases on a Computer. Zero means the Computer
// may sleep.
func (s *Store) ActiveLeases(ctx context.Context, scope contracts.Scope, computerID string) (int, error) {
	var n int
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
			SELECT count(*) FROM ahp_compute.activity_leases
			WHERE computer_id = $1 AND expires > clock_timestamp()`, computerID).Scan(&n)
	})
	return n, err
}

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	if s == nil || s.pool == nil {
		return invalid("compute: store is not initialised")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("compute: begin: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("compute: commit: %w", err)
	}
	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func validateQuota(q Quota) error {
	if q.MaxPTYs < 0 || q.MaxPIDs < 0 || q.MaxMemoryMB < 0 || q.MaxCPUMillisPerSec < 0 ||
		q.MaxOpenFiles < 0 || q.MaxStorageMB < 0 {
		return invalid("quota values must not be negative")
	}
	if q.MaxSessionTTL < 0 || q.MaxSessionTTL > maxSessionTTL {
		return invalid("session ttl must be between 0 and 24h")
	}
	return nil
}

func validIdentifier(v, what string) error {
	if v == "" {
		return invalid(what + " must not be empty")
	}
	if len(v) > maxKeyBytes {
		return invalid(what + " exceeds 512 bytes")
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '-', c == '_', c == ':', c == '@', c == '/':
		default:
			return invalid(what + " contains an unsafe identifier character")
		}
	}
	return nil
}

func validOptionalIdentifier(v, what string) error {
	if v == "" {
		return nil
	}
	return validIdentifier(v, what)
}

func canonicalCommand(command, cwd string) (cmd string, dir string, digest string, err error) {
	if command == "" {
		return "", "", "", invalid("command must not be empty")
	}
	if len(command) > maxCommandBytes {
		return "", "", "", invalid("command exceeds 1 MiB")
	}
	if err := validOptionalIdentifier(cwd, "cwd"); err != nil {
		return "", "", "", err
	}
	d, err := contracts.Digest(map[string]any{"command": command, "cwd": cwd})
	if err != nil {
		return "", "", "", asInvalid(err)
	}
	return command, cwd, d, nil
}

func boundOutput(s string) string {
	if len(s) > maxOutputBytes {
		return s[:maxOutputBytes]
	}
	return s
}

func invalid(msg string) error { return contracts.NewError(contracts.ErrInvalid, msg) }

func asInvalid(err error) error {
	if contracts.Code(err) != "" {
		return err
	}
	return invalid(err.Error())
}

func notFound(what string) error {
	return contracts.NewError(contracts.ErrNotFound, what+" not found")
}

func staleEpoch() error {
	return contracts.NewError(contracts.ErrStaleEpoch, "a newer incarnation holds the volume fence")
}

func isUniqueViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}
