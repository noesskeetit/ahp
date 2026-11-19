// Package memory implements the AHP product Memory service: versioned,
// provenance-bearing memories in three separately-stored scopes (user,
// workspace, agent), with operation-key idempotency, second-brain write gates
// (review_required, source_kind+evidence, supersedes, Secret Firewall),
// hybrid search (BM25 + pgvector + provenance graph) whose right-to-read is re-checked at
// emission, and subject-scoped deletion/tombstone.
//
// This is the AHP product Memory described by the architecture PDF; it is NOT
// the developers' Hindsight memory and must not be substituted by it.
//
// Rights model (non-negotiable, from the PDF):
//   - the isolation scope always comes from the verified task token; bodies
//     never carry tenant/subject/deployment;
//   - a record's visibility is never wider than the writing task's ceiling:
//     a private source yields private_to_user even inside a project category,
//     and a private-ceiling write into scope agent is refused outright;
//   - the index never grants read on the source: search first narrows the
//     admissible set (tenant, project, visibility, owner) and re-checks the
//     audience label of every emitted hit.
package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Schema is the exclusive PostgreSQL schema of this service.
const Schema = "ahp_memory"

// EmbeddingDim is the fixed pgvector dimensionality. The production embedder
// is configured out of band; the column type is pinned to this width so the
// cosine operator is always well defined.
const EmbeddingDim = 64

// Memory kinds (the three separately-stored scopes).
const (
	KindUser      = "user"
	KindWorkspace = "workspace"
	KindAgent     = "agent"
)

// Memory states.
const (
	StateProposed   = "proposed"
	StateActive     = "active"
	StateSuperseded = "superseded"
	StateRejected   = "rejected"
	StateRevoked    = "revoked"
	StateDeleted    = "deleted"
)

// Authorize is the coarse capability check consulted per request; the
// fine-grained visibility decision lives in this store and is re-checked on
// every read.
type Authorize func(ctx context.Context, scope contracts.Scope, action, resource string) error

// Embedder maps a text to a fixed-dimension float vector used for the
// pgvector signal of hybrid search. A nil Embedder degrades search to the
// lexical (tsvector) signal only.
type Embedder func(ctx context.Context, text string) ([]float32, error)

// Source is an embedder that declares the width it produces. The store takes
// its pgvector column width from that declaration, so an embedder and a column
// cannot be wired at two different widths - the disagreement that would either
// write NULL embeddings into a wider column or overflow a narrower one.
type Source interface {
	Embed(ctx context.Context, text string) ([]float32, error)
	Dimensions() int
}

// maxEmbeddingWidth is pgvector's ceiling for the vector type.
const maxEmbeddingWidth = 16000

// WithEmbedding installs a width-declaring embedder and pins the pgvector
// column to the width it declares. It is the only honest way to turn semantic
// search on: passing a bare Embedder leaves the column at the historical width
// and every vector signal then fails its own width check.
func WithEmbedding(src Source) Option {
	return func(s *Store) {
		if src == nil {
			s.embedder = nil
			return
		}
		if src.Dimensions() <= 0 || src.Dimensions() > maxEmbeddingWidth {
			s.embeddingError = contracts.NewError(contracts.ErrInvalid, fmt.Sprintf(
				"memory: embedder declares %d dimensions, must be within 1..%d",
				src.Dimensions(), maxEmbeddingWidth))
			return
		}
		s.embedder = src.Embed
		s.embedDim = src.Dimensions()
		if identified, ok := src.(interface{ Identity() string }); ok {
			s.embeddingSpace = identified.Identity()
		}
	}
}

// Workspaces reports the caller's current workspace memberships. It backs the
// admissible-set filter and the per-hit workspace re-check, so search never
// trusts a stale membership copy.
type Workspaces func(ctx context.Context, scope contracts.Scope) ([]string, error)

// Ceiling reports the caller's live label ceiling (the task's current label
// set). A record's visibility is never wider than this at write time.
type Ceiling func(ctx context.Context, scope contracts.Scope) (labels.Set, error)

// ReadLabels adds the emitted labels to the reading task before any content leaves Memory.
type ReadLabels func(context.Context, contracts.Scope, labels.Set) error

// EmbeddingPolicy verifies the tenant route against server-owned labels before egress.
type EmbeddingPolicy func(context.Context, contracts.Scope, labels.Set) error

func WithReadLabels(f ReadLabels) Option           { return func(s *Store) { s.readLabels = f } }
func WithEmbeddingPolicy(f EmbeddingPolicy) Option { return func(s *Store) { s.embeddingPolicy = f } }

// Option configures optional collaborators of a Store.
type Option func(*Store)

// WithWorkspaces installs the membership hook used to scope workspace memories.
func WithWorkspaces(f Workspaces) Option {
	return func(s *Store) { s.workspaces = f }
}

// WithLabels installs the live ceiling hook used to bound a record's visibility.
func WithLabels(f Ceiling) Option { return func(s *Store) { s.ceiling = f } }

// Store is the Memory service.
type Store struct {
	pool            *pgxpool.Pool
	art             *artifacts.Store
	authorize       Authorize
	embedder        Embedder
	workspaces      Workspaces
	ceiling         Ceiling
	embedDim        int
	embeddingSpace  string
	readLabels      ReadLabels
	embeddingPolicy EmbeddingPolicy
	// embeddingError records a refused WithEmbedding so the misconfiguration
	// surfaces at New instead of being dropped by the option pattern.
	embeddingError error
}

// New wires a Memory store. pool and authorize are mandatory; art may be nil
// when no evidence-artifact publication checks are needed (the store then
// refuses evidence-bound proposals fail-closed); embedder may be nil.
func New(pool *pgxpool.Pool, art *artifacts.Store, authorize Authorize, embedder Embedder, opts ...Option) (*Store, error) {
	switch {
	case pool == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "memory: pool must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "memory: authorize must not be nil")
	}
	s := &Store{pool: pool, art: art, authorize: authorize, embedder: embedder}
	if embedder != nil {
		// A bare Embedder keeps the historical width: that path is the
		// in-process and test one, never the production mount.
		s.embedDim = EmbeddingDim
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.embedder != nil && s.embedDim == 0 {
		s.embedDim = EmbeddingDim
	}
	if s.embeddingError != nil {
		return nil, s.embeddingError
	}
	return s, nil
}

// EmbeddingWidth reports the pgvector column width this store writes and
// queries. It is derived from the wired embedder, never guessed.
func (s *Store) EmbeddingWidth() int {
	if s.embedDim == 0 {
		return EmbeddingDim
	}
	return s.embedDim
}

// checkEmbedding rejects a vector whose width contradicts the column. The
// search path used to skip such an answer silently, which let a search report
// hits as if the semantic signal had run while it had not; a lying or
// misconfigured embedder is a dependency failure and is reported as one.
func (s *Store) checkEmbedding(vec []float32) error {
	if len(vec) != s.EmbeddingWidth() {
		return contracts.NewError(contracts.ErrUnavailable, fmt.Sprintf(
			"memory: embedder returned %d dimensions, the vector column is %d wide",
			len(vec), s.EmbeddingWidth()))
	}
	var norm float64
	for _, x := range vec {
		f := float64(x)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return contracts.NewError(contracts.ErrUnavailable, "memory: non-finite embedding")
		}
		norm += f * f
	}
	if norm == 0 {
		return contracts.NewError(contracts.ErrUnavailable, "memory: zero embedding has no cosine direction")
	}
	return nil
}

// Evidence records the provenance and source kind of a proposal. At least one
// evidence entry is required before a record may leave the proposing subject
// (workspace/agent scopes); the source taints feed the Secret Firewall and the
// review gate.
type Evidence struct {
	SourceKind string   `json:"source_kind"`
	Artifacts  []string `json:"artifacts,omitempty"`
	Detail     string   `json:"detail,omitempty"`
	Taints     []string `json:"taints,omitempty"`
}

// Memory is one versioned memory record.
type Memory struct {
	Deployment     string         `json:"-"`
	ID             string         `json:"id"`
	Kind           string         `json:"kind"`
	Workspace      string         `json:"workspace,omitempty"`
	Subject        string         `json:"subject"`
	Text           string         `json:"text"`
	State          string         `json:"state"`
	OpKey          string         `json:"op_key"`
	SourceKind     string         `json:"source_kind,omitempty"`
	Evidence       []Evidence     `json:"evidence,omitempty"`
	Supersedes     string         `json:"supersedes,omitempty"`
	ReviewRequired bool           `json:"review_required,omitempty"`
	Provenance     map[string]any `json:"provenance,omitempty"`
	Labels         labels.Set     `json:"labels"`
	TTL            *time.Time     `json:"ttl,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

const idPattern = `^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$`

var idRe = regexp.MustCompile(idPattern)

// ddlFor builds the schema for one pgvector width. The width is a parameter
// because it belongs to the wired embedder, not to this package.
func ddlFor(width int) string {
	return `
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS ahp_memory;
CREATE TABLE IF NOT EXISTS ahp_memory.memories (
    id          text PRIMARY KEY,
    tenant      text NOT NULL,
    deployment  text NOT NULL,
    kind        text NOT NULL CHECK (kind IN ('user','workspace','agent')),
    workspace   text NOT NULL DEFAULT '',
    subject     text NOT NULL,
    text        text NOT NULL,
    text_digest text NOT NULL,
    search      tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED,
    embedding   vector(` + strconv.Itoa(width) + `),
    state       text NOT NULL DEFAULT 'proposed'
                CHECK (state IN ('proposed','active','superseded','rejected','revoked','deleted')),
    op_key      text NOT NULL,
    op_digest   text NOT NULL,
    source_kind text NOT NULL DEFAULT '',
    evidence    jsonb NOT NULL DEFAULT '[]'::jsonb,
    supersedes  text NOT NULL DEFAULT '',
    review_required boolean NOT NULL DEFAULT false,
    provenance  jsonb NOT NULL DEFAULT '{}'::jsonb,
    labels      jsonb NOT NULL DEFAULT '{}'::jsonb,
    ttl         timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE ahp_memory.memories ADD COLUMN IF NOT EXISTS embedding_space text NOT NULL DEFAULT '';
CREATE UNIQUE INDEX IF NOT EXISTS memories_idem
    ON ahp_memory.memories (tenant, deployment, subject, op_key);
CREATE INDEX IF NOT EXISTS memories_search
    ON ahp_memory.memories USING gin (search);
CREATE INDEX IF NOT EXISTS memories_visible
    ON ahp_memory.memories (tenant, kind, workspace, subject, state);
CREATE INDEX IF NOT EXISTS memories_dedup
    ON ahp_memory.memories (tenant, kind, workspace, text_digest, state);
CREATE TABLE IF NOT EXISTS ahp_memory.erased_tenants (tenant text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS ahp_memory.erased_subjects (tenant text NOT NULL, subject text NOT NULL, PRIMARY KEY(tenant,subject));
CREATE TABLE IF NOT EXISTS ahp_memory.operations (
 tenant text NOT NULL, deployment text NOT NULL, subject text NOT NULL,
 op_key text NOT NULL, digest text NOT NULL,
 result_id text NOT NULL DEFAULT '', result_count bigint NOT NULL DEFAULT 0,
 PRIMARY KEY (tenant,deployment,subject,op_key)
);
-- Existing committed rows are successful. New reservations stay invisible
-- until their transaction also commits the memory write and final status.
ALTER TABLE ahp_memory.operations ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'SUCCEEDED'
 CHECK(status IN ('ACCEPTED','SUCCEEDED','FENCED'));
INSERT INTO ahp_memory.operations (tenant,deployment,subject,op_key,digest,result_id)
 SELECT tenant,deployment,subject,op_key,op_digest,id FROM ahp_memory.memories
 ON CONFLICT DO NOTHING;
-- Backfill under the previous subject key BEFORE enforcing the deployment
-- namespace. Fail closed on historical collisions; never pick one receipt.
CREATE UNIQUE INDEX IF NOT EXISTS operations_deployment_key
 ON ahp_memory.operations(tenant,deployment,op_key);
`
}

// Migrate creates the schema, the pgvector extension and the memories table,
// then reconciles the embedding column against the wired embedder.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, ddlFor(s.EmbeddingWidth()))
	if err != nil {
		return storageError("migrate memory schema", err)
	}
	return s.reconcileEmbeddingWidth(ctx)
}

// reconcileEmbeddingWidth compares an existing column against the wired
// embedder. CREATE TABLE IF NOT EXISTS keeps an existing column untouched, so
// a stand created at 64 dimensions and later wired to a 1024-wide embedder
// would otherwise reject every write with a database error that names nothing
// useful. An empty column is rescaled: no vector exists to invalidate. A column
// that already holds vectors is refused, because recomputing someone's memory
// corpus is an operator's decision, never a side effect of a restart.
func (s *Store) reconcileEmbeddingWidth(ctx context.Context) error {
	const q = `SELECT format_type(a.atttypid, a.atttypmod),
       EXISTS (SELECT 1 FROM ahp_memory.memories WHERE embedding IS NOT NULL)
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'ahp_memory' AND c.relname = 'memories' AND a.attname = 'embedding'`
	var typ string
	var holdsVectors bool
	if err := s.pool.QueryRow(ctx, q).Scan(&typ, &holdsVectors); err != nil {
		return storageError("read memory embedding column", err)
	}
	if s.embedder == nil {
		return nil
	} // Lexical mode does not reinterpret an existing vector space.
	want := fmt.Sprintf("vector(%d)", s.EmbeddingWidth())
	if typ == want {
		return nil
	}
	if holdsVectors {
		return contracts.NewError(contracts.ErrConflict, fmt.Sprintf(
			"memory: the embedding column is %s but the wired embedder produces %s, and the table already holds vectors; "+
				"re-embedding existing memories is an operator decision - point the embedding configuration at the matching model or migrate the column deliberately",
			typ, want))
	}
	if _, err := s.pool.Exec(ctx, "ALTER TABLE ahp_memory.memories ALTER COLUMN embedding TYPE "+want); err != nil {
		return storageError("rescale memory embedding column", err)
	}
	return nil
}

// columns is the shared projection used by every read path.
const columns = `id, deployment, kind, workspace, subject, text, state, op_key, source_kind,
       evidence, supersedes, review_required, provenance, labels, ttl, created_at, updated_at`

// scanMemory projects one row onto a Memory.
func scanMemory(row pgx.Row) (Memory, error) {
	var (
		m        Memory
		evDoc    []byte
		provDoc  []byte
		labelDoc []byte
		ttl      *time.Time
	)
	err := row.Scan(&m.ID, &m.Deployment, &m.Kind, &m.Workspace, &m.Subject, &m.Text, &m.State,
		&m.OpKey, &m.SourceKind, &evDoc, &m.Supersedes, &m.ReviewRequired, &provDoc,
		&labelDoc, &ttl, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		return Memory{}, err
	}
	m.TTL = ttl
	if len(evDoc) > 0 {
		_ = json.Unmarshal(evDoc, &m.Evidence)
	}
	if len(provDoc) > 0 {
		_ = json.Unmarshal(provDoc, &m.Provenance)
	}
	if len(labelDoc) > 0 {
		_ = json.Unmarshal(labelDoc, &m.Labels)
	}
	return m, nil
}

// isUnique reports a 23505 unique-violation.
func isUnique(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}

// storageError turns an unexpected database error into a contract error without
// letting the driver's text reach the client (it carries the DSN user, host and
// database name).
//
// The class is chosen from what actually happened, because a wrong class is a
// wrong diagnosis: until this classification existed every storage failure was
// reported as CONFLICT, which is how a missing pgvector extension -- a
// provisioning fact an operator must fix -- reached a caller as "your write
// collided with committed state". Fail-closed is unchanged: every path still
// refuses the operation, nothing is downgraded to success, and the SQLSTATE is
// carried as a stable `reason` so the class stays distinguishable without
// parsing prose.
func storageError(op string, err error) error {
	if err == nil {
		return nil
	}
	reason, code := classifyStorage(err)
	return contracts.NewReasoned(code, "memory:storage:"+reason, "memory: "+op+": storage failure")
}

// classifyStorage maps a database error onto (stable reason, contract code).
// Reasons are enumerations, never driver text.
func classifyStorage(err error) (string, string) {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		// Class 08 connection exception, 57 operator intervention / prepared
		// transaction unavailable, 58 system error, 53 resource shortage: the
		// database is not serving this request.
		switch {
		case pe.Code == "23505":
			return "unique_violation", contracts.ErrConflict
		case pe.Code == "40001", pe.Code == "40P01":
			return "pgstate_" + pe.Code + "_retryable_conflict", contracts.ErrConflict
		case strings.HasPrefix(pe.Code, "08"), strings.HasPrefix(pe.Code, "57"),
			strings.HasPrefix(pe.Code, "58"), strings.HasPrefix(pe.Code, "53"):
			return "pgstate_" + pe.Code + "_storage_unreachable", contracts.ErrUnavailable
		case pe.Code == "3D000", pe.Code == "3F000", pe.Code == "42P01",
			pe.Code == "42704", pe.Code == "42883", pe.Code == "PP000":
			// Missing database / schema / relation / type or extension: the
			// dependency this service was built against is not provisioned.
			return "pgstate_" + pe.Code + "_dependency_missing", contracts.ErrUnavailable
		case pe.Code != "":
			return "pgstate_" + pe.Code, "INTERNAL"
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return "storage_timeout", contracts.ErrUnavailable
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return "storage_unreachable", contracts.ErrUnavailable
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return "storage_unreachable", contracts.ErrUnavailable
	}
	return "unclassified", "INTERNAL"
}

// audienceByKind returns the sink audience a record of this kind writes into.
func audienceByKind(kind, workspace string) labels.Audience {
	switch kind {
	case KindUser:
		return labels.Audience{Kind: labels.KindPrivate}
	case KindWorkspace:
		return labels.Audience{Kind: labels.KindWorkspace, Workspace: workspace}
	default:
		return labels.Audience{Kind: labels.KindTenant}
	}
}

// privateCeiling reports whether a ceiling is private (subject-restricted,
// secret-tainted, or externally tainted). A private ceiling may never widen a
// record beyond its writer, and may never enter scope agent.
func privateCeiling(ceiling labels.Set) bool {
	if len(distinctStrings(ceiling.Subjects)) > 0 {
		return true
	}
	for _, t := range ceiling.Taints {
		if t == labels.TaintSecret || t == labels.TaintExternal || t == labels.TaintUntrustedExternal {
			return true
		}
	}
	return false
}

func hasSecret(vs ...[]string) bool {
	for _, list := range vs {
		for _, t := range list {
			if t == labels.TaintSecret {
				return true
			}
		}
	}
	return false
}

func (s *Store) liveCeiling(ctx context.Context, scope contracts.Scope) (labels.Set, error) {
	if s.ceiling == nil {
		return labels.Set{}, nil
	}
	set, err := s.ceiling(ctx, scope)
	if err != nil {
		return labels.Set{}, err
	}
	return set, nil
}

func (s *Store) callerWorkspaces(ctx context.Context, scope contracts.Scope) ([]string, error) {
	if s.workspaces == nil {
		return nil, nil
	}
	return s.workspaces(ctx, scope)
}

func distinctStrings(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, v := range in {
		if v == "" {
			continue
		}
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// serializeVector encodes a float slice as a pgvector text literal.
func serializeVector(v []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(f), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}
