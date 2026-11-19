package identity

import (
	"context"
	"errors"
	"github.com/ahp-platform/ahp/pkg/auth"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Resource identifies a container resolved by its owner, never a free-form URL.
type Resource struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// ResourceRequest is an internal authority contract. ProjectRole is supplied
// only by the Workspace owner's live membership read; it is not an HTTP field.
type ResourceRequest struct {
	Scope       contracts.Scope
	Resource    Resource
	Action      string
	GrantID     string
	GrantEpoch  int64
	ProjectRole string
}

// ResourceEvaluator adds container policy to, and never replaces, live grants.
type ResourceEvaluator interface {
	Require(context.Context, ResourceRequest) error
}
type ResourceEvaluatorFunc func(context.Context, ResourceRequest) error

func (f ResourceEvaluatorFunc) Require(ctx context.Context, r ResourceRequest) error {
	return f(ctx, r)
}

type StoreOption func(*Store)

type resourceAuthenticator struct {
	Authenticator
	evaluator ResourceEvaluator
}

// NewResourceAuthenticator checks tenant entry permission after proving the
// identity. It never takes subject or tenant from the login request body.
func NewResourceAuthenticator(a Authenticator, e ResourceEvaluator) Authenticator {
	return &resourceAuthenticator{a, e}
}
func (a *resourceAuthenticator) Login(ctx context.Context, in LoginInput) (BrowserUser, error) {
	u, err := a.Authenticator.Login(ctx, in)
	if err != nil {
		return BrowserUser{}, err
	}
	r := ResourceRequest{Scope: contracts.Scope{Tenant: u.Tenant, Subject: u.Subject, Deployment: "public"}, Resource: Resource{Kind: "tenant", ID: u.Tenant}, Action: "login"}
	if a.evaluator == nil || validateResource(r) != nil {
		return BrowserUser{}, ErrAuthentication
	}
	if err := a.evaluator.Require(ctx, r); err != nil {
		return BrowserUser{}, ErrAuthentication
	}
	return u, nil
}

func WithResourceEvaluator(e ResourceEvaluator) StoreOption {
	return func(s *Store) { s.resources = e }
}

func validateResource(r ResourceRequest) error {
	if err := r.Scope.Validate(); err != nil {
		return err
	}
	switch r.Resource.Kind {
	case "tenant", "deployment", "project", "connection":
	default:
		return invalid("unsupported resource kind")
	}
	if err := (contracts.Scope{Tenant: r.Scope.Tenant, Deployment: r.Resource.Kind, Subject: r.Resource.ID}).Validate(); err != nil {
		return invalid("invalid resource identifier")
	}
	if r.Resource.Kind == "tenant" && r.Resource.ID != r.Scope.Tenant {
		return forbidden("resource tenant mismatch")
	}
	if r.Action == "" || len(r.Action) > 128 || strings.ContainsAny(r.Action, " *?%[\x00\r\n\t") {
		return invalid("invalid resource action")
	}
	return nil
}

// RequireResource fails closed unless a resource evaluator was explicitly wired.
// Callers must authenticate the principal and resolve the owner container first.
func (s *Store) RequireResource(ctx context.Context, r ResourceRequest) error {
	if err := validateResource(r); err != nil {
		return err
	}
	if s.resources == nil {
		return contracts.NewError(contracts.ErrUnavailable, "identity: resource authority is not configured")
	}
	if err := s.resources.Require(ctx, r); err != nil {
		if contracts.Code(err) == contracts.ErrForbidden || contracts.Code(err) == contracts.ErrNotFound {
			return forbidden("identity: resource access denied")
		}
		return contracts.NewError(contracts.ErrUnavailable, "identity: resource authority unavailable")
	}
	return nil
}

func (s *Store) requireContainers(ctx context.Context, scope contracts.Scope, capabilities []string, grant string, epoch int64) error {
	if s.resources == nil {
		return nil
	} // explicitly selected legacy/local composition
	for _, capability := range capabilities {
		for _, resource := range []Resource{{Kind: "tenant", ID: scope.Tenant}, {Kind: "deployment", ID: scope.Deployment}} {
			if err := s.RequireResource(ctx, ResourceRequest{Scope: scope, Resource: resource, Action: capability, GrantID: grant, GrantEpoch: epoch}); err != nil {
				return err
			}
		}
	}
	return nil
}

// RequireHumanContainers supplements an already verified live human session
// and entitlement. Deployment must come from the verified workload certificate,
// as transport.TaskOrUser does; a caller body must never supply it.
func (s *Store) RequireHumanContainers(ctx context.Context, c auth.Claims, capability string) error {
	if c.Kind != auth.KindHuman {
		return forbidden("identity: human container policy requires a human principal")
	}
	return s.requireContainers(ctx, c.Scope(), []string{capability}, "", 0)
}

const resourceDDL = `
CREATE TABLE IF NOT EXISTS ahp_identity.resource_rules (
 tenant text NOT NULL, subject text NOT NULL, kind text NOT NULL, resource_id text NOT NULL,
 action text NOT NULL, allowed boolean NOT NULL, revision bigint NOT NULL,
 PRIMARY KEY(tenant,subject,kind,resource_id,action));
`

// ResourceRule is operator-provisioned synthetic IAM policy. It contains no
// credential. An absent tenant/deployment/connection rule means DENY.
type ResourceRule struct {
	Tenant   string   `json:"tenant"`
	Subject  string   `json:"subject"`
	Resource Resource `json:"resource"`
	Action   string   `json:"action"`
	Allowed  bool     `json:"allowed"`
}

// SyntheticResourcePolicy implements the agreed PoC IAM boundary in Identity's
// own schema. There is no decision cache: revocation is read on every request.
type SyntheticResourcePolicy struct{ pool *pgxpool.Pool }

func NewSyntheticResourcePolicy(pool *pgxpool.Pool) *SyntheticResourcePolicy {
	return &SyntheticResourcePolicy{pool: pool}
}

// PutRule is a trusted operator provisioning API, deliberately not a public
// HTTP route. CAS prevents a stale configuration from undoing a revocation.
func (p *SyntheticResourcePolicy) PutRule(ctx context.Context, r ResourceRule, expected int64) (int64, error) {
	if err := validateResource(ResourceRequest{Scope: contracts.Scope{Tenant: r.Tenant, Subject: r.Subject, Deployment: "policy"}, Resource: r.Resource, Action: r.Action}); err != nil {
		return 0, err
	}
	if expected < 0 {
		return 0, invalid("negative policy revision")
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0)),pg_advisory_xact_lock_shared(hashtextextended($2,0))`, "identity-tenant-erasure/"+r.Tenant, "identity-subject-erasure/"+r.Tenant+"/"+r.Subject); err != nil {
		return 0, err
	}
	var erased bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_identity.erased_tenants WHERE tenant=$1) OR EXISTS(SELECT 1 FROM ahp_identity.subjects WHERE tenant=$1 AND subject=$2)`, r.Tenant, r.Subject).Scan(&erased); err != nil {
		return 0, err
	}
	if erased {
		return 0, forbidden("identity: erased principal")
	}
	var revision int64
	if expected == 0 {
		err = tx.QueryRow(ctx, `INSERT INTO ahp_identity.resource_rules(tenant,subject,kind,resource_id,action,allowed,revision) VALUES($1,$2,$3,$4,$5,$6,1) ON CONFLICT DO NOTHING RETURNING revision`, r.Tenant, r.Subject, r.Resource.Kind, r.Resource.ID, r.Action, r.Allowed).Scan(&revision)
	} else {
		err = tx.QueryRow(ctx, `UPDATE ahp_identity.resource_rules SET allowed=$6,revision=revision+1 WHERE tenant=$1 AND subject=$2 AND kind=$3 AND resource_id=$4 AND action=$5 AND revision=$7 RETURNING revision`, r.Tenant, r.Subject, r.Resource.Kind, r.Resource.ID, r.Action, r.Allowed, expected).Scan(&revision)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, contracts.NewError(contracts.ErrConflict, "identity: resource policy revision changed")
	}
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return revision, nil
}

func (p *SyntheticResourcePolicy) Require(ctx context.Context, r ResourceRequest) error {
	if err := validateResource(r); err != nil {
		return err
	}
	if p == nil || p.pool == nil {
		return contracts.NewError(contracts.ErrUnavailable, "identity: missing synthetic policy store")
	}
	var allowed, erased bool
	err := p.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_identity.erased_tenants WHERE tenant=$1) OR EXISTS(SELECT 1 FROM ahp_identity.subjects WHERE tenant=$1 AND subject=$2)`, r.Scope.Tenant, r.Scope.Subject).Scan(&erased)
	if err != nil {
		return err
	}
	if erased {
		return forbidden("identity: principal is revoked")
	}
	err = p.pool.QueryRow(ctx, `SELECT allowed FROM ahp_identity.resource_rules WHERE tenant=$1 AND subject=$2 AND kind=$3 AND resource_id=$4 AND action=$5`, r.Scope.Tenant, r.Scope.Subject, r.Resource.Kind, r.Resource.ID, r.Action).Scan(&allowed)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if r.Resource.Kind == "project" {
		// Membership is read by Workspace on this request. Explicit IAM denial
		// still wins even for an owner; an ACL cannot invent membership.
		if err == nil && !allowed {
			return forbidden("identity: project access denied")
		}
		if !projectActionAllowed(r.ProjectRole, r.Action) {
			return forbidden("identity: project role does not permit action")
		}
		return nil
	}
	if err != nil || !allowed {
		return forbidden("identity: resource access denied")
	}
	return nil
}

func projectActionAllowed(role, action string) bool {
	switch action {
	case "read":
		return role == "owner" || role == "editor" || role == "viewer"
	case "write", "candidate:create", "candidate:list", "candidate:write", "candidate:check":
		return role == "owner" || role == "editor"
	case "publish", "candidate:reject", "purge", "member:add", "member:remove", "member:list":
		return role == "owner"
	default:
		return false
	}
}
