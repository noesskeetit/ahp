// Package workspace implements immutable, copy-on-write collaborative
// workspaces on top of the artifact store.
//
// Model (non-negotiable):
//   - a workspace has an ordered, immutable revision history; head_rev is the
//     current revision id (NULL until the first merge);
//   - work happens in candidates created against an immutable base_rev; a
//     candidate copies the base file set (CoW) and never mutates history;
//   - a merge is a compare-and-swap on expected_head: if head moved, or the
//     candidate touches a path that changed since its base, the merge fails
//     and a conflict branch records the candidate snapshot;
//   - every artifact referenced by a candidate must be publishable to the
//     workspace under the merging caller's current rights, so a private fact
//     can never enter a workspace the reader is not allowed to see.
package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Schema is the exclusive PostgreSQL schema of this service.
const Schema = "ahp_workspace"

// Authorize is the coarse capability check consulted per request by the HTTP
// layer; fine-grained membership and rights live in this store.
type Authorize func(ctx context.Context, scope contracts.Scope, action, resource string) error

// File is one entry in a revision or candidate.
type File struct {
	Path       string `json:"path"`
	ArtifactID string `json:"artifact_id"`
	Mode       int    `json:"mode"`
	Digest     string `json:"digest"`
}

// Candidate is a copy-on-write working set derived from one base revision.
type Candidate struct {
	ID             string    `json:"id"`
	WorkspaceID    string    `json:"workspace_id"`
	BaseRev        string    `json:"base_rev"`
	HeadAtCreate   string    `json:"head_at_create"`
	Digest         string    `json:"digest"`
	State          string    `json:"state"`
	ExpectedHead   string    `json:"expected_head"`
	MergedRev      string    `json:"merged_rev,omitempty"`
	ConflictBranch string    `json:"conflict_branch,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// Revision is one immutable commit of a workspace.
type Revision struct {
	ID          string    `json:"id"`
	WorkspaceID string    `json:"workspace_id"`
	Seq         int64     `json:"seq"`
	ParentRev   string    `json:"parent_rev,omitempty"`
	Digest      string    `json:"digest"`
	Author      string    `json:"author_subject"`
	CreatedAt   time.Time `json:"created_at"`
}

// Workspace is a collaborative container of revisions.
type Workspace struct {
	ID      string `json:"id"`
	Tenant  string `json:"tenant"`
	Name    string `json:"name"`
	HeadRev string `json:"head_rev,omitempty"`
}

// Member is one subject's membership record in a workspace. Role is the
// authoritative tier ("owner", "editor", "viewer"); IsOwner is kept as a derived
// compatibility flag equal to (Role == "owner").
type Member struct {
	Subject string `json:"subject"`
	IsOwner bool   `json:"is_owner"`
	Role    string `json:"role"`
}

// GetWorkspace returns a workspace the caller is a member of.
func (s *Store) GetWorkspace(ctx context.Context, scope contracts.Scope, workspace string) (Workspace, error) {
	return s.requireMember(ctx, scope, workspace, "read")
}

// Store is safe for concurrent use.
type Store struct {
	pool              *pgxpool.Pool
	artifacts         *artifacts.Store
	authorize         Authorize
	resourceAuthorize ResourceAuthorize
}

// New wires a workspace store. store must not be nil: it is used to verify
// publication rights and to proxy file reads.
func New(pool *pgxpool.Pool, art *artifacts.Store, authorize Authorize) (*Store, error) {
	switch {
	case pool == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "workspace: pool must not be nil")
	case art == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "workspace: artifact store must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "workspace: authorize must not be nil")
	}
	return &Store{pool: pool, artifacts: art, authorize: authorize}, nil
}

const ddl = `
CREATE SCHEMA IF NOT EXISTS ahp_workspace;
CREATE TABLE IF NOT EXISTS ahp_workspace.erased_tenants(tenant text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS ahp_workspace.workspaces (
    id        text PRIMARY KEY,
    tenant    text NOT NULL,
    name      text NOT NULL,
    head_rev  text,
    labels    jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant, name)
);
CREATE TABLE IF NOT EXISTS ahp_workspace.members (
    workspace_id text NOT NULL REFERENCES ahp_workspace.workspaces(id) ON DELETE CASCADE,
    subject      text NOT NULL,
    is_owner     boolean NOT NULL DEFAULT false,
    role         text NOT NULL DEFAULT 'editor' CHECK (role IN ('owner','editor','viewer')),
    added_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, subject)
);
CREATE TABLE IF NOT EXISTS ahp_workspace.revisions (
    id             text PRIMARY KEY,
    workspace_id   text NOT NULL REFERENCES ahp_workspace.workspaces(id) ON DELETE CASCADE,
    seq            bigint NOT NULL,
    parent_rev     text,
    digest         text NOT NULL,
    author_subject text NOT NULL,
    labels         jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, seq)
);
CREATE TABLE IF NOT EXISTS ahp_workspace.files (
    workspace_id text NOT NULL,
    rev_id       text NOT NULL REFERENCES ahp_workspace.revisions(id) ON DELETE CASCADE,
    path         text NOT NULL,
    artifact_id  text NOT NULL,
    mode         integer NOT NULL DEFAULT 0,
    digest       text NOT NULL,
    PRIMARY KEY (workspace_id, rev_id, path)
);
CREATE TABLE IF NOT EXISTS ahp_workspace.candidates (
    id             text PRIMARY KEY,
    workspace_id   text NOT NULL REFERENCES ahp_workspace.workspaces(id) ON DELETE CASCADE,
    base_rev       text NOT NULL DEFAULT '',
    head_at_create text NOT NULL DEFAULT '',
    digest         text NOT NULL DEFAULT '',
    state          text NOT NULL DEFAULT 'pending'
                   CHECK (state IN ('pending', 'merged', 'rejected', 'conflict')),
    merged_rev     text NOT NULL DEFAULT '',
    conflict_branch text NOT NULL DEFAULT '',
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ahp_workspace.candidate_files (
    candidate_id text NOT NULL REFERENCES ahp_workspace.candidates(id) ON DELETE CASCADE,
    path         text NOT NULL,
    artifact_id  text NOT NULL,
    mode         integer NOT NULL DEFAULT 0,
    digest       text NOT NULL,
    PRIMARY KEY (candidate_id, path)
);
CREATE TABLE IF NOT EXISTS ahp_workspace.checks (
    id            text PRIMARY KEY,
    candidate_id  text NOT NULL REFERENCES ahp_workspace.candidates(id) ON DELETE CASCADE,
    name          text NOT NULL,
    status        text NOT NULL CHECK (status IN ('pending', 'passed', 'failed')),
    target_digest text NOT NULL,
    detail        text NOT NULL DEFAULT '',
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (candidate_id, name)
);
CREATE TABLE IF NOT EXISTS ahp_workspace.pins (
    workspace_id text NOT NULL REFERENCES ahp_workspace.workspaces(id) ON DELETE CASCADE,
    rev_id       text NOT NULL,
    subject      text NOT NULL,
    PRIMARY KEY (workspace_id, rev_id, subject)
);
CREATE TABLE IF NOT EXISTS ahp_workspace.branches (
    workspace_id text NOT NULL REFERENCES ahp_workspace.workspaces(id) ON DELETE CASCADE,
    name         text NOT NULL,
    rev_id       text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, name)
);
ALTER TABLE ahp_workspace.members
    ADD COLUMN IF NOT EXISTS role text NOT NULL DEFAULT 'editor';
CREATE TABLE IF NOT EXISTS ahp_workspace.ci_runners (
    workspace_id text NOT NULL REFERENCES ahp_workspace.workspaces(id) ON DELETE CASCADE,
    subject      text NOT NULL,
    added_by     text NOT NULL,
    added_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, subject)
);
-- Trusted CI provenance: a check remembers who ran it and whether that
-- workload is a registered CI runner. Rows written before this column exist
-- keep trusted = false, which is the fail-closed value.
ALTER TABLE ahp_workspace.checks
    ADD COLUMN IF NOT EXISTS trusted boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS runner_task text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS runner_root text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS runner_subject text NOT NULL DEFAULT '';
`

// Migrate creates the schema. It is idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, ddl)
	return err
}

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$`)

func validateName(name string) error {
	if !namePattern.MatchString(name) {
		return contracts.NewError(contracts.ErrInvalid, "workspace name is malformed")
	}
	return nil
}

func validatePath(p string) error {
	if p == "" || len(p) > 1024 || p[0] == '/' {
		return contracts.NewError(contracts.ErrInvalid, "path is malformed")
	}
	for _, seg := range splitPath(p) {
		if seg == "" || seg == "." || seg == ".." {
			return contracts.NewError(contracts.ErrInvalid, "path is malformed")
		}
	}
	return nil
}

func splitPath(p string) []string {
	var out []string
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			out = append(out, p[start:i])
			start = i + 1
		}
	}
	return append(out, p[start:])
}

// ---------------------------------------------------------------------------
// Membership & workspace lifecycle
// ---------------------------------------------------------------------------

// Create makes a workspace and records the caller as owner and member.
func (s *Store) Create(ctx context.Context, scope contracts.Scope, name string) (Workspace, error) {
	if err := scope.Validate(); err != nil {
		return Workspace{}, err
	}
	if err := validateName(name); err != nil {
		return Workspace{}, err
	}
	if err := s.authorize(ctx, scope, "create", "workspace"); err != nil {
		return Workspace{}, err
	}
	id := contracts.NewID()
	tx, err := s.beginWritable(ctx, scope)
	if err != nil {
		return Workspace{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	doc, _ := json.Marshal(labels.Set{})
	if _, err := tx.Exec(ctx,
		`INSERT INTO ahp_workspace.workspaces (id, tenant, name, labels) VALUES ($1,$2,$3,$4)`,
		id, scope.Tenant, name, doc); err != nil {
		if isUnique(err) {
			return Workspace{}, contracts.NewError(contracts.ErrConflict, "workspace name already exists")
		}
		return Workspace{}, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO ahp_workspace.members (workspace_id, subject, is_owner, role) VALUES ($1,$2,true,'owner')`,
		id, scope.Subject); err != nil {
		return Workspace{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Workspace{}, err
	}
	return Workspace{ID: id, Tenant: scope.Tenant, Name: name}, nil
}

// AddMember grants a subject read/write membership. Caller must be a member.
func (s *Store) AddMember(ctx context.Context, scope contracts.Scope, workspace, subject string) error {
	w, err := s.requireMember(ctx, scope, workspace, "member:add")
	if err != nil {
		return err
	}
	if subject == "" {
		return contracts.NewError(contracts.ErrInvalid, "subject is required")
	}
	_, err = s.writeExec(ctx, scope,
		`INSERT INTO ahp_workspace.members (workspace_id, subject) VALUES ($1,$2)
		 ON CONFLICT DO NOTHING`, w.ID, subject)
	return err
}

// RemoveMember revokes membership. The owner cannot be removed.
func (s *Store) RemoveMember(ctx context.Context, scope contracts.Scope, workspace, subject string) error {
	w, err := s.requireMember(ctx, scope, workspace, "member:remove")
	if err != nil {
		return err
	}
	tag, err := s.writeExec(ctx, scope,
		`DELETE FROM ahp_workspace.members WHERE workspace_id=$1 AND subject=$2 AND NOT is_owner`,
		w.ID, subject)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return contracts.NewError(contracts.ErrForbidden, "cannot remove that member")
	}
	return nil
}

// ListMembers returns the subjects who belong to a workspace. Caller must be a
// member; membership listing is itself gated so it cannot be an oracle for
// workspace existence across tenants.
func (s *Store) ListMembers(ctx context.Context, scope contracts.Scope, workspace string) ([]Member, error) {
	w, err := s.requireMember(ctx, scope, workspace, "member:list")
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT subject, is_owner, role FROM ahp_workspace.members WHERE workspace_id=$1 ORDER BY subject`,
		w.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Member
	for rows.Next() {
		var m Member
		if err := rows.Scan(&m.Subject, &m.IsOwner, &m.Role); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// IsMember is the membership hook handed to the artifact store via
// artifacts.WithWorkspaceAccess, closing the loop between the two services.
func (s *Store) IsMember(ctx context.Context, scope contracts.Scope, project string) error {
	facts, err := s.ReadMembershipFacts(ctx, scope, project)
	if err != nil {
		return err
	}
	if s.resourceAuthorize != nil {
		return s.resourceAuthorize(ctx, scope, facts, "read")
	}
	return nil
}

// ListWorkspaces reports the names of the workspaces the caller's subject is a
// member of, within the caller's tenant. It backs the Memory service's
// admissible-set filter and per-hit workspace re-check, so a read never trusts
// a stale membership copy: it is re-run against live membership on every call.
func (s *Store) ListWorkspaces(ctx context.Context, scope contracts.Scope) ([]string, error) {
	return s.listWorkspaceReferences(ctx, scope, false)
}

// ListWorkspaceIDs exposes canonical resource IDs for consumers whose scopes
// and label journals use IDs. The historical name-based caller stays unchanged.
func (s *Store) ListWorkspaceIDs(ctx context.Context, scope contracts.Scope) ([]string, error) {
	return s.listWorkspaceReferences(ctx, scope, true)
}

func (s *Store) listWorkspaceReferences(ctx context.Context, scope contracts.Scope, ids bool) ([]string, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
SELECT w.id, w.name
  FROM ahp_workspace.members m
  JOIN ahp_workspace.workspaces w ON w.id = m.workspace_id
 WHERE m.subject = $1
   AND w.tenant = $2
 ORDER BY w.name`,
		scope.Subject, scope.Tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		if ids {
			out = append(out, id)
		} else {
			out = append(out, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if s.resourceAuthorize != nil {
		visible := make([]string, 0, len(out))
		for _, name := range out {
			if err := s.IsMember(ctx, scope, name); err != nil {
				if contracts.Code(err) == contracts.ErrForbidden || contracts.Code(err) == contracts.ErrNotFound {
					continue
				}
				return nil, err
			}
			visible = append(visible, name)
		}
		return visible, nil
	}
	return out, nil
}

// resolveWorkspace loads a workspace by id or name within the caller's tenant.
// Cross-tenant and unknown workspaces both report NOT FOUND so existence is
// never leaked between tenants.
func (s *Store) resolveWorkspace(ctx context.Context, scope contracts.Scope, idOrName string) (Workspace, error) {
	if err := scope.Validate(); err != nil {
		return Workspace{}, err
	}
	var w Workspace
	err := s.pool.QueryRow(ctx,
		`SELECT id, tenant, name, COALESCE(head_rev, '')
		   FROM ahp_workspace.workspaces
		  WHERE tenant = $1 AND (id = $2 OR name = $2)`,
		scope.Tenant, idOrName).Scan(&w.ID, &w.Tenant, &w.Name, &w.HeadRev)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Workspace{}, contracts.NewError(contracts.ErrNotFound, "workspace not found")
	case err != nil:
		return Workspace{}, err
	}
	return w, nil
}

// requireMember resolves a workspace for the tenant and checks that the caller
// is a member, after the coarse capability gate.
func (s *Store) requireMember(ctx context.Context, scope contracts.Scope, workspace, action string) (Workspace, error) {
	w, err := s.resolveWorkspace(ctx, scope, workspace)
	if err != nil {
		return Workspace{}, err
	}
	if err := s.authorize(ctx, scope, action, "workspace:"+w.ID); err != nil {
		return Workspace{}, err
	}
	if err := s.authorizeMember(ctx, scope, w.ID, action); err != nil {
		return Workspace{}, err
	}
	return w, nil
}

// Roles are the three membership tiers that back every per-call rights
// decision inside this store: owner manages, editor writes, viewer reads.
// is_owner is retained as a derived compatibility flag (role == RoleOwner) and
// now drives many rules, not just owner-removal.
const (
	RoleOwner  = "owner"
	RoleEditor = "editor"
	RoleViewer = "viewer"
)

// actionAllows is the single owner/editor/viewer policy table. Every action the
// store performs is mapped to the tiers that may perform it; an action absent
// from this table is denied, so a forged or unmapped action can never widen
// access (fail closed).
func actionAllows(role, action string) bool {
	switch action {
	case "read":
		return role == RoleOwner || role == RoleEditor || role == RoleViewer
	case "write", "candidate:create", "candidate:list", "candidate:write", "candidate:check":
		return role == RoleOwner || role == RoleEditor
	case "publish", "candidate:reject", "purge", "member:add", "member:remove", "member:list":
		return role == RoleOwner
	case "ci:register", "ci:list", "ci:remove":
		// Registering a trusted CI runner widens who may inject verdicts into
		// this project's merge gate, so it is owner-only, fail closed.
		return role == RoleOwner
	default:
		return false
	}
}

// authorizeMember loads the caller's role in workspaceID (within the caller's
// tenant) and checks it against action. A subject with no membership row is
// refused FORBIDDEN; a member whose tier does not cover the action is refused
// FORBIDDEN too. Cross-tenant and unknown workspaces are refused earlier by
// resolveWorkspace (NOT_FOUND), so 403 and 404 remain distinguishable and no
// workspace existence is leaked across tenants.
func (s *Store) authorizeMember(ctx context.Context, scope contracts.Scope, workspaceID, action string) error {
	if s.resourceAuthorize != nil {
		facts, err := s.ReadMembershipFacts(ctx, scope, workspaceID)
		if err != nil {
			return err
		}
		return s.resourceAuthorize(ctx, scope, facts, action)
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	var role string
	err := s.pool.QueryRow(ctx, `
SELECT m.role
  FROM ahp_workspace.members m
  JOIN ahp_workspace.workspaces w ON w.id = m.workspace_id
 WHERE m.workspace_id = $1 AND m.subject = $2 AND w.tenant = $3`,
		workspaceID, scope.Subject, scope.Tenant).Scan(&role)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return contracts.NewError(contracts.ErrForbidden, "not a member of workspace "+workspaceID)
	case err != nil:
		return err
	}
	if !actionAllows(role, action) {
		return contracts.NewError(contracts.ErrForbidden,
			"role "+role+" is not permitted to "+action+" in this workspace")
	}
	return nil
}

func isUnique(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}
