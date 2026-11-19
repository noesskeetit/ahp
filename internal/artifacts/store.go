// Package artifacts stores immutable artifact bytes in S3-compatible object
// storage with PostgreSQL metadata. A sealed artifact is bound to one exact
// object version forever; later uploads to the same path never change what
// existing ids serve.
package artifacts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Schema is the exclusive PostgreSQL schema of this service.
const Schema = "ahp_artifacts"

// MaxSize is the largest accepted artifact (64 MiB).
const MaxSize = 64 << 20

// presignExpiry bounds the lifetime of an upload PUT URL.
const presignExpiry = 10 * time.Minute

// Authorize is the current authorization check. It is consulted on every
// upload, seal, read and delete; a revoked grant takes effect immediately.
type Authorize func(ctx context.Context, scope contracts.Scope, action, resource string) error

// ArtifactRef is one sealed, immutable artifact.
type ArtifactRef struct {
	ID      string `json:"id"`
	Tenant  string `json:"tenant"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
	Name    string `json:"name"`
	MIME    string `json:"mime"`
	Size    int64  `json:"size"`
	// Deployment is the deployment the artifact was written by. Together with
	// Tenant, Subject and ContextID it is the support the PDF names for
	// deletion, retention and legal hold: "каждая запись знает tenant, деплой,
	// субъект и контекст".
	Deployment string `json:"deployment"`
	// Subject is the owning subject frozen on the row.
	Subject string `json:"subject"`
	// ContextID is the State/A2A context (thread) the artifact belongs to. It
	// is what a thread cascade selects on.
	ContextID string `json:"context_id,omitempty"`
	// TaskID is the task whose run produced the artifact. It is the cascade
	// support for writes made by a run whose context the writer does not hold.
	TaskID string `json:"task_id,omitempty"`
	// Labels is the label set frozen on the artifact row.
	Labels labels.Set `json:"labels"`
	// EffectiveLabels is Labels merged with the reading caller's current
	// grant labels, i.e. the exact set the visibility decision used.
	EffectiveLabels labels.Set `json:"effective_labels"`
}

// UploadRequest describes one intended upload.
type UploadRequest struct {
	Name         string     `json:"name"`
	MIME         string     `json:"mime"`
	SHA256       string     `json:"sha256"`
	Size         int64      `json:"size"`
	Labels       labels.Set `json:"labels"`
	JournalStage bool       `json:"journal_stage,omitempty"`
}

// Upload is an unsealed upload with its presigned PUT URL. The URL is
// returned to the caller only and is never stored or logged.
type Upload struct {
	ID           string            `json:"id"`
	PresignedURL string            `json:"presigned_url"`
	Object       string            `json:"object"`
	ExpectedSHA  string            `json:"sha256"`
	ExpectedSize int64             `json:"size"`
	Headers      map[string]string `json:"headers,omitempty"`
	ExpiresAt    time.Time         `json:"expires_at"`
}

// Store is safe for concurrent use.
type Store struct {
	transferAuthority TransferAuthority
	transferCopier    TransferCopier
	transferSlot      chan struct{}
	transferLimits    TransferLimits
	parser            ParserCommand
	parserSlots       chan struct{}
	pool              *pgxpool.Pool
	client            *minio.Client
	uploadClient      *minio.Client
	// cipher is the optional at-rest object seam mount (WithObjectCipher).
	// nil or disabled == the store behaves exactly as the pre-encryption store.
	cipher    *objectcipher.Cipher
	bucket    string
	authorize Authorize
	// grantLabels and wsAccess are optional collaborators configured via
	// Option. When nil, the store fails closed on grant labels (treated as
	// empty) and on any workspace-restricted read.
	grantLabels GrantLabels
	wsAccess    WorkspaceAccess
	// keep answers "does anything outside this plane still name this artifact?"
	// for the deletion cascade. Nil means the cascade has no outside view and
	// behaves as it did before the option existed. See WithReferenceKeeper.
	keep                 ReferenceScan
	checkpointSettlement references.CheckpointSettlement
	// idemIndexPending records that the durable unique index over the upload
	// idempotency key could not be installed because pre-constraint duplicate
	// rows exist. See ensureIdempotencyIndex.
	idemIndexPending atomic.Bool
}

// New wires the service. Authorize must not be nil: every data path is
// gated by the current authorization decision.
func New(pool *pgxpool.Pool, client *minio.Client, bucket string, authorize Authorize, opts ...Option) (*Store, error) {
	if pool == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "artifacts: nil pool")
	}
	if client == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "artifacts: nil object client")
	}
	if authorize == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "artifacts: nil Authorize")
	}
	if !bucketPattern.MatchString(bucket) {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"artifacts: bucket must match ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
	}
	s := &Store{pool: pool, client: client, bucket: bucket, authorize: authorize, parserSlots: make(chan struct{}, 2), transferSlot: make(chan struct{}, 1)}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$`)

// Binding is the control-plane location one artifact write happens in. The PDF
// makes tenant, deployment, subject and context the basis of deletion,
// retention and legal hold, so a write carries them alongside its scope:
// ContextID is the State thread / A2A context the artifact belongs to, TaskID
// is the task whose run produced it. Both are optional at the API boundary -
// an artifact without them is simply not reachable by a context cascade, which
// is exactly the orphan the cascade must not create.
type Binding struct {
	ContextID string `json:"context_id,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
}

// validate reports that a binding is either empty or made of bounded, safe
// identifier characters. A binding is not an authorization input (isolation is
// the scope's job), so an empty binding is legal and only malformed ones are
// refused.
func (b Binding) validate() error {
	if err := validateBindingValue("context id", b.ContextID); err != nil {
		return err
	}
	return validateBindingValue("task id", b.TaskID)
}

const ddl = `
CREATE SCHEMA IF NOT EXISTS ahp_artifacts;

CREATE TABLE IF NOT EXISTS ahp_artifacts.artifacts (
    id            text PRIMARY KEY,
    tenant        text NOT NULL,
    deployment    text NOT NULL,
    subject       text NOT NULL,
    context_id    text NOT NULL DEFAULT '',
    task_id       text NOT NULL DEFAULT '',
    object        text NOT NULL,
    name          text NOT NULL,
    mime          text NOT NULL,
    expected_sha  text NOT NULL,
    expected_size bigint NOT NULL,
    version       text,
    sha256        text,
    size          bigint,
    labels        jsonb NOT NULL,
    idem_key      text NOT NULL,
    request_digest text NOT NULL,
    deleted       boolean NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now(),
    sealed_at     timestamptz,
    CONSTRAINT artifacts_expected_sha_ck CHECK (expected_sha ~ '^[0-9a-f]{64}$'),
    CONSTRAINT artifacts_sha_ck CHECK (sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT artifacts_seal_ck CHECK (
        (version IS NULL AND sha256 IS NULL AND size IS NULL) OR
        (version IS NOT NULL AND sha256 IS NOT NULL AND size IS NOT NULL)
    ),
    CONSTRAINT artifacts_size_ck CHECK (expected_size > 0 AND expected_size <= 67108864)
);
CREATE UNIQUE INDEX IF NOT EXISTS artifacts_object_uidx
    ON ahp_artifacts.artifacts (object);
ALTER TABLE ahp_artifacts.artifacts ADD COLUMN IF NOT EXISTS journal_stage boolean NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS artifacts_gc_stage_idx
    ON ahp_artifacts.artifacts (tenant,COALESCE(sealed_at,created_at),id)
    WHERE NOT deleted AND (version IS NULL OR journal_stage);
`

// idempotencyIndexName is the historical full unique index over the upload
// idempotency key. It kept reserving a key after the artifact it named was
// deleted, which made the same file impossible to save again in that tenant:
// the runner derives the key from (name, digest), so every later turn ran into
// the tombstone's reservation (DEF-H24-8). Kept here only so the migration can
// drop it.
const idempotencyIndexName = "artifacts_idem_uidx"

// idemLiveIndexName is the durable backstop of the upload idempotency key: the
// key is unique among the artifacts that are still here. That is exactly the
// invariant the arbitration needs -- at most one live registration per key --
// while a tombstone stays as a record without holding the key hostage. A deleted
// row plus a live row on one key is therefore the normal shape of "the chat was
// deleted and the file saved again", and two LIVE rows on one key stays
// impossible at the database level, with or without the advisory lock.
const idemLiveIndexName = "artifacts_idem_live_uidx"

// ddlIdempotencyIndex is migration-applied on its own because its failure
// carries information the table creation does not: 23505 means duplicate LIVE
// rows produced before the constraint existed, which the operator reads out,
// while any other failure is a real migration error. Bundled into the table
// script it took the whole data plane down on restart instead.
//
// The legacy full index is dropped only after the partial one exists. The full
// index is strictly stronger, so at every instant of this migration at least one
// of the two stops two live rows from sharing a key, and no window opens.
//
// Both statements name their schema. A DROP INDEX that does not is resolved
// against the connection's search_path, and when the table lives in another
// schema IF EXISTS turns the statement into a success that dropped nothing --
// which is how a deployed front ended up running with BOTH indexes: the live-key
// index present, the legacy full index still reserving every deleted key, and
// DEF-H24-8 inactive while the migration reported nothing pending.
const ddlIdempotencyIndex = `CREATE UNIQUE INDEX IF NOT EXISTS ` + idemLiveIndexName + `
    ON ` + Schema + `.artifacts (tenant, deployment, subject, idem_key)
    WHERE NOT deleted;
DROP INDEX IF EXISTS ` + Schema + `.` + idempotencyIndexName + `;`

// Migrate creates the ahp_artifacts schema and the configured bucket with
// object versioning enabled. Idempotent and non-destructive.
func (s *Store) Migrate(ctx context.Context) error {
	if err := s.execDDL(ctx, "create artifacts schema", ddl+subjectFenceSchema); err != nil {
		return err
	}
	if err := s.ensureIdempotencyIndex(ctx); err != nil {
		return err
	}
	if err := s.execDDL(ctx, "create artifacts lifecycle", ddlLifecycle+ddlIngestion); err != nil {
		return err
	}
	if err := s.execDDL(ctx, "create artifacts gc", ddlGC); err != nil {
		return err
	}
	if err := s.execDDL(ctx, "create artifacts grants", ddlGrants+ddlTransfers); err != nil {
		return err
	}
	if err := s.execDDL(ctx, "create artifacts context grants", ddlContextGrants); err != nil {
		return err
	}
	if err := s.execDDL(ctx, "converge artifacts context grant audit key", ddlContextGrantAuditUpgrade); err != nil {
		return err
	}
	found, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return storageError("check bucket", err)
	}
	if !found {
		if err := s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{}); err != nil {
			return storageError("create bucket", err)
		}
	}
	return s.client.SetBucketVersioning(ctx, s.bucket,
		minio.BucketVersioningConfiguration{Status: "Enabled"})
}

// execDDL applies a migration script one statement at a time, tolerating the
// error PostgreSQL raises when a CONCURRENT migration won the race.
//
// IF NOT EXISTS is not race-free: composed and as-runner both migrate at
// startup, and two processes can pass the existence check together, so the
// loser hears 42P06/42P07/42701 ("... already exists"). For a script whose
// whole contract is idempotency that outcome is success, and failing it made
// data-plane startup depend on which process happened to lose. Statements are
// applied one by one so a won race cannot roll the rest of the script back.
func (s *Store) execDDL(ctx context.Context, op, script string) error {
	for _, stmt := range splitStatements(script) {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			if isDuplicateObject(err) {
				continue
			}
			return storageError(op, err)
		}
	}
	return nil
}

// splitStatements cuts a DDL script into individual statements. A semicolon
// inside a string literal, inside a "--" comment, or inside a dollar-quoted
// body does not end a statement.
//
// Dollar quoting matters because a conditional migration is written as a DO
// block whose body is full of semicolons: cutting it produced "unterminated
// dollar-quoted string" and took the data plane down on restart. One nesting
// level is tracked, which is what the migrations here use; a body that opened
// another level with a different tag would have to be its own script.
func splitStatements(script string) []string {
	var (
		out     []string
		current strings.Builder
		quoted  bool
		tag     string
	)
	for i := 0; i < len(script); i++ {
		c := script[i]
		if tag != "" {
			if strings.HasPrefix(script[i:], tag) {
				current.WriteString(tag)
				i += len(tag) - 1
				tag = ""
				continue
			}
			current.WriteByte(c)
			continue
		}
		switch {
		case quoted:
			if c == '\'' && !isEscape(script, i) {
				quoted = false
			}
		case c == '\'' && !isEscape(script, i):
			quoted = true
		case c == '$':
			if end := dollarTagEnd(script, i); end > i {
				tag = script[i:end]
				current.WriteString(tag)
				i = end - 1
				continue
			}
		case c == '-' && i+1 < len(script) && script[i+1] == '-':
			for i < len(script) && script[i] != '\n' {
				i++
			}
			continue
		case c == ';':
			if stmt := strings.TrimSpace(current.String()); stmt != "" {
				out = append(out, stmt)
			}
			current.Reset()
			continue
		}
		current.WriteByte(c)
	}
	if stmt := strings.TrimSpace(current.String()); stmt != "" {
		out = append(out, stmt)
	}
	return out
}

// dollarTagEnd returns the index just past a dollar-quote tag starting at i, or
// i when the bytes there open nothing ($$ or $tag$).
func dollarTagEnd(script string, i int) int {
	for j := i + 1; j < len(script); j++ {
		if script[j] == '$' {
			return j + 1
		}
		if !(script[j] == '_' || script[j] >= 'a' && script[j] <= 'z' ||
			script[j] >= 'A' && script[j] <= 'Z' || j > i+1 && script[j] >= '0' && script[j] <= '9') {
			return i
		}
	}
	return i
}

// isEscape reports whether the byte at i is escaped by a backslash run.
func isEscape(s string, i int) bool {
	backslashes := 0
	for j := i - 1; j >= 0 && s[j] == '\\'; j-- {
		backslashes++
	}
	return backslashes%2 == 1
}

// isDuplicateObject reports the PostgreSQL class "what you asked for already
// exists", which for an idempotent migration means another process created the
// same object a moment ago.
func isDuplicateObject(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "42P06", // duplicate_schema
		"42P07", // duplicate_table (also raised for an index or relation name)
		"42P16", // duplicate_object
		"42701", // duplicate_column
		"42710": // duplicate_object
		return true
	case "23505":
		// IF NOT EXISTS races also surface as a unique violation on PostgreSQL's
		// OWN catalogs (pg_class_relname_nsp_index, pg_type_typname_nsp_index,
		// pg_namespace_nspname_index): two sessions passed the existence check and
		// one lost the insert. A violation of a constraint the migration itself
		// declared is a real conflict over data and stays an error.
		return strings.HasPrefix(pgErr.ConstraintName, "pg_")
	}
	return false
}

// BeginUpload validates the request, checks authorization, registers
// metadata and returns a 10-minute presigned PUT URL.
//
// Idempotency is scoped to (tenant, deployment, subject, key): a replay of
// an identical request returns the same upload, a different body under the
// same key is KEY_REUSE_MISMATCH. The signed URL is never stored.
func (s *Store) BeginUpload(ctx context.Context, scope contracts.Scope, key string, req UploadRequest) (Upload, error) {
	return s.BeginUploadWithBinding(ctx, scope, Binding{}, key, req)
}

// BeginUploadWithBinding is BeginUpload with the control-plane binding (context
// and task) the artifact is written under. The binding is recorded on the row
// and is what makes the artifact reachable by a thread or subject cascade
// without any agent hook; an empty binding leaves the artifact reachable only
// by its id.
func (s *Store) BeginUploadWithBinding(ctx context.Context, scope contracts.Scope, b Binding, key string, req UploadRequest) (Upload, error) {
	if err := scope.Validate(); err != nil {
		return Upload{}, err
	}
	if err := b.validate(); err != nil {
		return Upload{}, err
	}
	if req.JournalStage && (b.ContextID == "" || b.TaskID == "" || s.keep == nil || s.checkpointSettlement == nil) {
		return Upload{}, contracts.NewError(contracts.ErrUnavailable, "journal staging requires a bound State reference owner")
	}
	if err := validateKey(key); err != nil {
		return Upload{}, err
	}
	if err := validateRequest(req); err != nil {
		return Upload{}, err
	}
	if err := s.authorize(ctx, scope, "upload", "artifact"); err != nil {
		return Upload{}, err
	}

	// Private by default: the artifact starts visible to its owner subject
	// only; caller labels accumulate on top.
	requested := labels.Merge(labels.Set{Subjects: []string{scope.Subject}}, req.Labels)
	if err := labels.CheckWrite(requested, labels.Audience{
		Kind: labels.KindPrivate, Subject: scope.Subject,
	}); err != nil {
		return Upload{}, err
	}
	doc, err := json.Marshal(requested)
	if err != nil {
		return Upload{}, contracts.NewError(contracts.ErrInvalid, "labels cannot be stored")
	}
	legacyDigest, err := requestDigest(req, requested)
	if err != nil {
		return Upload{}, err
	}
	digest, err := requestDigestWithBinding(req, requested, b)
	if err != nil {
		return Upload{}, err
	}

	var out Upload
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		// The key is arbitrated inside this transaction, before anything is
		// written, by the database itself.
		//
		// The unique index is the durable backstop and cannot be the mechanism.
		// A database whose artifacts table predates that index has no index at
		// all - the migration could not build it over duplicates an older binary
		// produced - and there INSERT ... ON CONFLICT DO NOTHING has nothing to
		// conflict on, so a reused key silently registers a second row, which is
		// what made the migration unrepeatable and the front lose its data planes
		// on every restart. The advisory lock is taken on exactly the columns the
		// index covers, so winner and loser are ordered by PostgreSQL with or
		// without the index, and the loser answers with the contract instead of
		// inserting.
		if err := lockIdempotencyKey(ctx, tx, scope, key); err != nil {
			return err
		}
		existing, found, err := loadUploadKey(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if found {
			out, err = resolveUploadKey(existing, scope, digest, legacyDigest, b, req)
			return err
		}
		const ins = `
INSERT INTO ahp_artifacts.artifacts
    (id, tenant, deployment, subject, context_id, task_id, object, name, mime,
     expected_sha, expected_size, idem_key, request_digest, labels, journal_stage)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb, $15)
ON CONFLICT DO NOTHING
RETURNING id, object`
		var id, object string
		rowErr := tx.QueryRow(ctx, ins,
			contracts.NewID(), scope.Tenant, scope.Deployment, scope.Subject,
			b.ContextID, b.TaskID,
			scope.Tenant+"/"+contracts.NewID(), req.Name, req.MIME, req.SHA256, req.Size,
			key, digest, doc, req.JournalStage,
		).Scan(&id, &object)
		if rowErr == nil {
			out = Upload{ID: id, Object: object, ExpectedSHA: req.SHA256, ExpectedSize: req.Size}
			return nil
		}
		if !errors.Is(rowErr, pgx.ErrNoRows) {
			return storageError("register upload", rowErr)
		}
		// Something else won between the read and the insert: either another
		// writer that does not take this lock, or a collision on the object path.
		// Replay only for the identical scoped request. The query is deliberately
		// not subject-scoped here, unlike the locked pre-read above: a conflict on
		// another subject's key is reported as not found, and only a conflict with
		// no matching key at all can be the object path.
		existing, found, err = loadUploadKeyAnySubject(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrConflict, "upload object path already exists")
		}
		out, err = resolveUploadKey(existing, scope, digest, legacyDigest, b, req)
		return err
	})
	if err != nil {
		return Upload{}, err
	}

	signer := s.uploadClient
	if signer == nil {
		signer = s.client
	}
	url, err := signer.PresignHeader(ctx, http.MethodPut, s.bucket, out.Object, presignExpiry, nil, http.Header{"Content-Length": []string{strconv.FormatInt(out.ExpectedSize, 10)}})
	if err != nil {
		return Upload{}, storageError("presign upload", err)
	}
	out.PresignedURL = url.String()
	out.Headers = map[string]string{"Content-Length": strconv.FormatInt(out.ExpectedSize, 10)}
	out.ExpiresAt = time.Now().UTC().Add(presignExpiry)
	return out, nil
}

// idempotencyLockKey is the advisory-lock name of one upload idempotency key.
// It covers exactly the columns the unique index covers, so the lock and the
// index can never disagree about what "the same key" means.
func idempotencyLockKey(scope contracts.Scope, key string) string {
	return scope.Tenant + "|upload|" + scope.Deployment + "|" + scope.Subject + "|" + key
}

// lockIdempotencyKey serialises registrations of one idempotency key across
// sessions. Transaction-scoped: the lock is released when the registration
// commits or aborts, so a loser never proceeds against a half-written row.
func lockIdempotencyKey(ctx context.Context, tx pgx.Tx, scope contracts.Scope, key string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		idempotencyLockKey(scope, key)); err != nil {
		return storageError("lock upload key", err)
	}
	return nil
}

// existingUpload is the row one idempotency key already owns.
type existingUpload struct {
	id      string
	subject string
	digest  string
	object  string
	binding Binding
}

// uploadKeySelect resolves a key to the LIVE row that owns it. A key whose row
// was deleted owns nothing -- deletion releases the key (DEF-H24-8) -- so the
// tombstone is deliberately invisible here and the next BeginUpload starts a new
// generation with its own id and its own object path.
const uploadKeySelect = `SELECT id, subject, request_digest, object, context_id, task_id
                          FROM ahp_artifacts.artifacts
                         WHERE tenant = $1 AND deployment = $2 AND idem_key = $3
                           AND NOT deleted`

// loadUploadKey reads the row this scope's key owns. Subject is part of the
// lookup because it is part of the key: another subject registering the same
// string owns a different key and must not be answered with this row.
func loadUploadKey(ctx context.Context, tx pgx.Tx, scope contracts.Scope, key string) (existingUpload, bool, error) {
	var ex existingUpload
	err := tx.QueryRow(ctx, uploadKeySelect+` AND subject = $4`,
		scope.Tenant, scope.Deployment, key, scope.Subject).
		Scan(&ex.id, &ex.subject, &ex.digest, &ex.object, &ex.binding.ContextID, &ex.binding.TaskID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return existingUpload{}, false, nil
	case err != nil:
		return existingUpload{}, false, storageError("read upload key", err)
	}
	return ex, true, nil
}

// loadUploadKeyAnySubject is the post-conflict read, where the conflicting row
// may belong to another subject and that distinction is itself the answer.
func loadUploadKeyAnySubject(ctx context.Context, tx pgx.Tx, scope contracts.Scope,
	key string) (existingUpload, bool, error) {
	var ex existingUpload
	err := tx.QueryRow(ctx, uploadKeySelect, scope.Tenant, scope.Deployment, key).
		Scan(&ex.id, &ex.subject, &ex.digest, &ex.object, &ex.binding.ContextID, &ex.binding.TaskID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return existingUpload{}, false, nil
	case err != nil:
		return existingUpload{}, false, storageError("read upload key", err)
	}
	return ex, true, nil
}

// resolveUploadKey says, in one place, what a key that already owns a live row
// means: another subject's key is not visible, a different request under the
// same live key is a key reuse, and the identical request is the same upload. A
// key no live row owns is not a conflict at all -- that is the freed-key path,
// and it registers a new generation instead of answering with a tombstone.
func resolveUploadKey(ex existingUpload, scope contracts.Scope, digest, legacyDigest string, binding Binding,
	req UploadRequest) (Upload, error) {
	if ex.subject != scope.Subject {
		return Upload{}, contracts.NewError(contracts.ErrNotFound, "upload not found")
	}
	// Old rows carry a pre-binding digest. They may replay only when both
	// persisted binding columns match; no migration reattributes their bytes.
	if ex.binding != binding || (ex.digest != digest && ex.digest != legacyDigest) {
		return Upload{}, contracts.NewError(contracts.ErrKeyReuse,
			"idempotency key was already used for a different upload request")
	}
	return Upload{ID: ex.id, Object: ex.object, ExpectedSHA: req.SHA256, ExpectedSize: req.Size}, nil
}

// ensureIdempotencyIndex installs the durable unique index over the LIVE upload
// idempotency key and drops the historical full one.
//
// It is a migration step of its own because one of its failures is not a schema
// problem: PostgreSQL refuses to build a unique index over duplicate rows (23505
// naming the index), and those rows were produced by a binary from before the
// constraint. Failing the whole migration for them is what stopped every restart
// of the composed front from mounting its data planes. The step therefore stays
// green over that one condition and says so through IdempotencyIndexPending:
// BeginUpload arbitrates the key with an advisory lock, so no new duplicate can
// be created while the operator reads the historical rows out, and the next
// migration installs the index and clears the condition.
//
// What is NOT done here: no row is deleted, and the invariant is not relaxed -
// the predicate narrows the key's lifetime to the artifact's lifetime, which is
// what makes a deleted chat stop reserving its file names (DEF-H24-8) while two
// live registrations of one key stay impossible.
func (s *Store) ensureIdempotencyIndex(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, ddlIdempotencyIndex); err != nil {
		if isIdempotencyDataConflict(err) {
			s.idemIndexPending.Store(true)
			return nil
		}
		return storageError("create artifacts idempotency index", err)
	}
	s.idemIndexPending.Store(false)
	// A schema migration is done when the catalog says it is, not when the
	// statement returned without an error: the failure above is silent by
	// construction. Left in place the legacy index would keep the old contract
	// alive, so the honest answer is a migration that refuses to pretend. The
	// remedy belongs to the operator -- make the migration role the owner of the
	// schema, or drop the index once by hand.
	legacy, err := s.legacyIdempotencyIndexPresent(ctx)
	if err != nil {
		return err
	}
	if legacy {
		return storageError("drop legacy idempotency index", fmt.Errorf(
			"%s.%s still present after migration: drop it or grant its ownership to this role",
			Schema, idempotencyIndexName))
	}
	return nil
}

// legacyIdempotencyIndexPresent reports whether the historical full index is
// still in the catalog of this schema.
func (s *Store) legacyIdempotencyIndexPresent(ctx context.Context) (bool, error) {
	var found bool
	err := s.pool.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2)`,
		Schema, idempotencyIndexName).Scan(&found)
	if err != nil {
		return false, storageError("read legacy idempotency index", err)
	}
	return found, nil
}

// IdempotencyIndexPending reports that the unique index over the upload
// idempotency key is missing because pre-constraint duplicate rows exist. The
// plane is safe to serve - registrations are arbitrated by an advisory lock -
// but the durable backstop returns only after the duplicates are read out and
// the migration runs again.
func (s *Store) IdempotencyIndexPending() bool { return s.idemIndexPending.Load() }

// isIdempotencyDataConflict names the single failure that is data rather than
// schema: a unique violation attributed to this index. isDuplicateObject stays
// exactly as it was - a violation of a constraint the migration declared is a
// real conflict, and only THIS statement treats it as a pending condition.
func isIdempotencyDataConflict(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return false
	}
	return pgErr.ConstraintName == idemLiveIndexName ||
		strings.Contains(pgErr.Message, idemLiveIndexName) ||
		pgErr.ConstraintName == idempotencyIndexName ||
		strings.Contains(pgErr.Message, idempotencyIndexName)
}

// Get returns the sealed reference of one artifact after the current
// authorization and label check. A foreign or deleted artifact is NOT_FOUND.
func (s *Store) Get(ctx context.Context, scope contracts.Scope, id string) (ArtifactRef, error) {
	return s.get(ctx, scope, id, true)
}

// Open proxies the exact sealed object version. No presigned GET is ever
// issued; every read re-checks authorization and labels.
func (s *Store) Open(ctx context.Context, scope contracts.Scope, id string) (io.ReadCloser, ArtifactRef, error) {
	ref, err := s.get(ctx, scope, id, true)
	if err != nil {
		return nil, ArtifactRef{}, err
	}
	var object string
	if err := s.pool.QueryRow(ctx,
		`SELECT object FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&object); err != nil {
		return nil, ArtifactRef{}, storageError("read artifact object path", err)
	}
	if err := s.checkContent(ctx, id); err != nil {
		return nil, ArtifactRef{}, err
	}
	if s.cipherOn(scope.Tenant) {
		rc, err := s.cipher.OpenReader(ctx, scope.Tenant, s.bucket, object, ref.Version)
		if err != nil {
			return nil, ArtifactRef{}, storageError("open object", err)
		}
		// The durable plaintext size is an attestation, not a hint: a reader that ends early
		// (for example a segmented object whose manifest footer was destroyed) must refuse
		// instead of serving a short body as the whole artifact.
		return &attestedSizeReader{rc: rc, id: ref.ID, want: ref.Size}, ref, nil
	}
	obj, err := s.client.GetObject(ctx, s.bucket, object, minio.GetObjectOptions{VersionID: ref.Version})
	if err != nil {
		return nil, ArtifactRef{}, storageError("open object", err)
	}
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		return nil, ArtifactRef{}, storageError("open object version", err)
	}
	return obj, ref, nil
}

// get loads one row, enforcing tenant, current authorization and labels. A
// label denial reads as NOT_FOUND: see load, B114 read parity.
func (s *Store) get(ctx context.Context, scope contracts.Scope, id string, checkAuth bool) (ArtifactRef, error) {
	return s.load(ctx, scope, id, checkAuth, true)
}

// getForGrant runs the same read gate as get but keeps the real refusal code.
// Issuing a download grant is an action, not a read: its contract-visible
// answer to a subject the artifact is private to is FORBIDDEN with a
// machine-readable reason (ReasonGrantRecipientMismatch class), so the B114
// read-parity mask is deliberately not applied here.
func (s *Store) getForGrant(ctx context.Context, scope contracts.Scope, id string) (ArtifactRef, error) {
	return s.load(ctx, scope, id, true, false)
}

// load is the shared read gate. maskLabelDenial is the B114 read parity: on a
// read, a subject the labels exclude gets the same answer as a missing id, so
// existence cannot be probed. It is off for capability issuance.
func (s *Store) load(ctx context.Context, scope contracts.Scope, id string, checkAuth, maskLabelDenial bool) (ArtifactRef, error) {
	if err := scope.Validate(); err != nil {
		return ArtifactRef{}, err
	}
	if err := validateID(id); err != nil {
		return ArtifactRef{}, err
	}
	const q = `SELECT id, tenant, COALESCE(version, ''), COALESCE(sha256, ''),
	           COALESCE(size, 0), name, mime, labels, deleted,
	           deployment, subject, context_id, task_id
	           FROM ahp_artifacts.artifacts WHERE id = $1`
	var (
		ref     ArtifactRef
		doc     []byte
		deleted bool
	)
	err := s.pool.QueryRow(ctx, q, id).Scan(
		&ref.ID, &ref.Tenant, &ref.Version, &ref.SHA256, &ref.Size, &ref.Name, &ref.MIME,
		&doc, &deleted, &ref.Deployment, &ref.Subject, &ref.ContextID, &ref.TaskID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ArtifactRef{}, contracts.NewError(contracts.ErrNotFound, "artifact not found")
	case err != nil:
		return ArtifactRef{}, storageError("read artifact", err)
	}
	if deleted || ref.Tenant != scope.Tenant || ref.Version == "" || ref.SHA256 == "" {
		return ArtifactRef{}, contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	if err := json.Unmarshal(doc, &ref.Labels); err != nil {
		return ArtifactRef{}, contracts.NewError(contracts.ErrConflict, "stored labels are unreadable")
	}
	if checkAuth {
		if err := s.authorize(ctx, scope, "read", "artifact:"+ref.ID); err != nil {
			return ArtifactRef{}, err
		}
	}
	// Admission merges the stored labels with the caller's current grant
	// labels and re-checks them on every read: a restriction added to the
	// grant (or a workspace restriction) is enforced now, not from a copy.
	// B114: on a read a label-based denial is a NOT_FOUND, never a FORBIDDEN,
	// so a foreign subject cannot probe existence.
	effective, err := s.admit(ctx, scope, ref.Labels)
	if err != nil {
		if maskLabelDenial {
			return ArtifactRef{}, contracts.NewError(contracts.ErrNotFound, "artifact not found")
		}
		return ArtifactRef{}, err
	}
	if checkAuth {
		// The conversation gate runs AFTER admission on purpose: it answers
		// FORBIDDEN and is never masked, so for a caller the labels already
		// exclude it would betray that the id exists (measured on 34aed0fe:
		// foreign subject + another conversation's claim -> FORBIDDEN against
		// NOT_FOUND for a missing id). The least-disclosing answer wins first;
		// a caller the labels DO admit still reads only inside its own
		// conversation - the subject boundary above is the human's, this one
		// is the task's, and it stays visible as FORBIDDEN.
		if err := s.checkReadScope(ctx, ref); err != nil {
			return ArtifactRef{}, err
		}
	}
	ref.EffectiveLabels = effective
	return ref, nil
}

// Delete writes a durable tombstone and its audit record in one transaction.
// All reads of the id fail afterwards. The S3 objects and their versions are
// NOT removed here: bytes go when the retention floor the control plane set
// elapses, which is the GC's decision (see gc.go), not this call's.
func (s *Store) Delete(ctx context.Context, scope contracts.Scope, id, key string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}
	if _, err := s.get(ctx, scope, id, false); err != nil {
		return err
	}
	if err := s.authorize(ctx, scope, "delete", "artifact:"+id); err != nil {
		return err
	}
	var hold bool
	if err := s.pool.QueryRow(ctx,
		`SELECT legal_hold FROM ahp_artifacts.artifacts WHERE id = $1 AND tenant = $2`,
		id, scope.Tenant).Scan(&hold); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "artifact not found")
		}
		return storageError("read artifact hold", err)
	}
	if hold {
		return contracts.NewError(contracts.ErrConflict,
			"artifact is under legal hold and cannot be deleted")
	}
	// The tombstone and its audit record commit together: a deletion that the
	// audit trail does not name did not happen, which is the contract the PDF
	// gives this plane ("только идентификаторы и размеры", с. 34).
	var tagged bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockMarkers(ctx, tx, []string{artifactLockKey(scope.Tenant, id)}); err != nil {
			return err
		}
		var digest string
		var deleted, held bool
		if err := tx.QueryRow(ctx, `SELECT COALESCE(sha256,''),deleted,legal_hold FROM ahp_artifacts.artifacts WHERE id=$1 AND tenant=$2 FOR UPDATE`, id, scope.Tenant).Scan(&digest, &deleted, &held); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return contracts.NewError(contracts.ErrNotFound, "artifact not found")
			}
			return storageError("lock tombstone artifact", err)
		}
		if deleted {
			return contracts.NewError(contracts.ErrNotFound, "artifact not found")
		}
		if held {
			return contracts.NewError(contracts.ErrConflict, "artifact is under legal hold and cannot be deleted")
		}
		referenced, err := s.tombstoneReferences(ctx, tx, scope.Tenant, []ArtifactQuery{{ID: id, Digest: digest}})
		if err != nil {
			return err
		}
		if referenced[id] {
			return contracts.NewError(contracts.ErrConflict, "artifact is required by a live reference or checkpoint publication")
		}
		var (
			object, version, sha, contextID, taskID string
			size                                    int64
		)
		qerr := tx.QueryRow(ctx, `
UPDATE ahp_artifacts.artifacts
   SET deleted = true, deleted_at = now()
 WHERE id = $1 AND tenant = $2 AND NOT deleted AND NOT legal_hold
RETURNING object, COALESCE(sha256, ''), COALESCE(version, ''), COALESCE(size, 0), context_id, task_id`,
			id, scope.Tenant).Scan(&object, &sha, &version, &size, &contextID, &taskID)
		if errors.Is(qerr, pgx.ErrNoRows) {
			return nil
		}
		if qerr != nil {
			return storageError("tombstone artifact", qerr)
		}
		tagged = true
		return s.logGCEventTx(ctx, tx, GCEvent{
			Event: GCEventTombstone, Tenant: scope.Tenant, Deployment: scope.Deployment,
			Subject: scope.Subject, ArtifactID: id, Object: object, Version: version,
			SHA256: sha, Size: size, Reason: "delete",
		})
	})
	if err != nil {
		return err
	}
	if !tagged {
		return contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Validation and error mapping
// ---------------------------------------------------------------------------

var (
	shaHex      = regexp.MustCompile(`^[0-9a-f]{64}$`)
	mimePattern = regexp.MustCompile(`^[a-zA-Z0-9!#$&^_.+-]{1,63}/[a-zA-Z0-9!#$&^_.+-]{1,127}$`)
)

func validateRequest(req UploadRequest) error {
	if req.Name == "" || len(req.Name) > 255 || strings.ContainsAny(req.Name, `/\`+"\x00") ||
		req.Name == "." || req.Name == ".." || strings.ContainsRune(req.Name, '\n') ||
		strings.ContainsRune(req.Name, '\r') {
		return contracts.NewError(contracts.ErrInvalid, "name must be a safe filename of at most 255 bytes")
	}
	if !mimePattern.MatchString(req.MIME) {
		return contracts.NewError(contracts.ErrInvalid, "mime type is malformed")
	}
	if !shaHex.MatchString(req.SHA256) {
		return contracts.NewError(contracts.ErrInvalid, "sha256 must be 64 lowercase hex characters")
	}
	if req.Size <= 0 || req.Size > MaxSize {
		return contracts.NewError(contracts.ErrInvalid, "size must be in 1..64MiB")
	}
	return nil
}

func validateKey(key string) error {
	if key == "" || len(key) > 128 {
		return contracts.NewError(contracts.ErrInvalid, "idempotency key must be 1..128 bytes")
	}
	return nil
}

func validateID(id string) error {
	if id == "" || len(id) > 128 {
		return contracts.NewError(contracts.ErrInvalid, "artifact id must be 1..128 bytes")
	}
	return nil
}

func requestDigest(req UploadRequest, merged labels.Set) (string, error) {
	return requestDigestWithBinding(req, merged, Binding{})
}
func requestDigestWithBinding(req UploadRequest, merged labels.Set, binding Binding) (string, error) {
	payload := map[string]any{
		"name":   req.Name,
		"mime":   req.MIME,
		"sha256": req.SHA256,
		"size":   req.Size,
		"labels": merged,
	}
	if binding.ContextID != "" {
		payload["context_id"] = binding.ContextID
	}
	if binding.TaskID != "" {
		payload["task_id"] = binding.TaskID
	}
	if req.JournalStage {
		payload["journal_stage"] = true
	}
	d, err := contracts.Digest(payload)
	if err != nil {
		return "", contracts.NewError(contracts.ErrInvalid, "upload request cannot be canonicalised")
	}
	return d, nil
}

func storageError(op string, err error) error {
	if err == nil {
		return nil
	}
	if code := contracts.Code(err); code != "" {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return contracts.NewError(contracts.ErrConflict, op+": already exists")
	}
	return fmt.Errorf("artifacts: %s: %w", op, err)
}
