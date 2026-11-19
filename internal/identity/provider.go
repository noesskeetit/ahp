package identity

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// EnsureBrowserUserBySubject mirrors a federated (OIDC) human into the local
// account row so the session, token-binding and revocation machinery treats a
// verified IdP subject exactly like a locally provisioned one. This is
// just-in-time provisioning from a TRUSTED source: subject and tenant arrive
// only from a verified assertion, never from a request body, and there is no
// open sign-up path. Re-authentication refreshes the display fields; it never
// invents a new subject.
func (s *Store) EnsureBrowserUserBySubject(ctx context.Context, tenant, subject, displayName, email string) (BrowserUser, error) {
	if s.pool == nil {
		return BrowserUser{}, invalid("identity: store has no pool")
	}
	tenant = strings.TrimSpace(tenant)
	subject = strings.TrimSpace(subject)
	if tenant == "" || subject == "" {
		return BrowserUser{}, invalid("identity: federated subject requires tenant and subject")
	}
	userID, err := randomToken()
	if err != nil {
		return BrowserUser{}, err
	}
	var u BrowserUser
	err = s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `
			INSERT INTO ahp_identity.browser_users
				(user_id, tenant, subject, display_name, email)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (tenant, subject) DO UPDATE
				SET display_name = COALESCE(NULLIF(EXCLUDED.display_name,''), ahp_identity.browser_users.display_name),
				    email        = COALESCE(NULLIF(EXCLUDED.email,''), ahp_identity.browser_users.email)
			RETURNING user_id, tenant, subject, COALESCE(display_name,''), COALESCE(email,'')`,
			userID, tenant, subject, displayName, email).
			Scan(&u.UserID, &u.Tenant, &u.Subject, &u.DisplayName, &u.Email)
	})
	if err != nil {
		return BrowserUser{}, fmt.Errorf("identity: ensure federated browser user: %w", err)
	}
	roles, perms, err := s.browserGrants(ctx, u.UserID)
	if err != nil {
		return BrowserUser{}, err
	}
	u.Roles, u.Permissions = roles, perms
	return u, nil
}

// Authentication boundary.
//
// The architecture (PDF rev3.2, public entry "login via IdP ... staff JWT",
// IAM ExtAuthz) makes a corporate identity provider the source of truth for
// who a human is. This repository has no corporate IdP available, so the
// concrete providers here are an explicit LOCAL profile and a REAL OIDC/
// JWKS/IAM adapter. The adapter is not a stand-in that replaces the corporate
// boundary: it is the seam a real IdP plugs into. Only the deployment that
// points it at a real issuer, real JWKS and real IAM turns it into production;
// until then any external verification it would perform stays unverified, and
// the LOCAL profile must never be described as the corporate IdP.
//
// Both profiles honour the invariant the whole block rests on: the Subject and
// Tenant of the identity a session is opened for come from the trusted source
// (the account row, or the verified IdP assertion) and never from the request.

// ErrAuthentication is the single failure a client may surface. Bad
// credentials and a bad IdP assertion collapse into it so the login endpoint
// cannot be used to enumerate accounts, local or federated.
var ErrAuthentication = errors.New("identity: authentication failed")

// LoginInput carries whatever proof a client presents. Which fields are
// meaningful depends on the profile: the local profile reads Username and
// Password, an OIDC profile reads Assertion. A caller can never name a
// Subject, Tenant or deployment here.
type LoginInput struct {
	Username  string
	Password  string
	Assertion string // IdP-issued token (OIDC id_token / access assertion)
}

// Authenticator proves a caller's identity and returns the authoritative
// account it maps to. Implementations MUST fail closed: any trouble reaching
// or trusting the authority is a denial, never a grant.
type Authenticator interface {
	// Profile is a short label surfaced to the UI and logs. It is descriptive
	// only; "local" is explicitly a development profile, not corporate IAM.
	Profile() string
	// Login returns the BrowserUser the trusted source vouches for. The
	// Subject and Tenant on the result are authoritative; a nil error with an
	// empty Subject is a bug in the implementation, never a grant.
	Login(ctx context.Context, in LoginInput) (BrowserUser, error)
}

// localAuthenticator is the development profile: a real argon2id credential
// store with real server-side sessions, backed by this repository's own
// database. It is the corporate IdP's placeholder and must be labelled so.
type localAuthenticator struct {
	s *Store
}

// NewLocalAuthenticator wraps the store's credential store as a development
// profile. Callers that need production identity must inject an OIDCAuthenticator
// pointed at a real issuer instead.
func NewLocalAuthenticator(s *Store) Authenticator { return &localAuthenticator{s: s} }

// Profile reports the local development profile.
func (*localAuthenticator) Profile() string { return "local" }

// Login authenticates a username/password against the local store. Unknown
// account and wrong password are indistinguishable, including in timing,
// because AuthenticateBrowser still runs the key derivation for an unknown
// name.
func (a *localAuthenticator) Login(ctx context.Context, in LoginInput) (BrowserUser, error) {
	if in.Assertion != "" {
		return BrowserUser{}, invalid("identity: the local profile authenticates by credential, not assertion")
	}
	user, ok, err := a.s.AuthenticateBrowser(ctx, in.Username, in.Password)
	if err != nil {
		return BrowserUser{}, err
	}
	if !ok {
		return BrowserUser{}, ErrAuthentication
	}
	return user, nil
}
