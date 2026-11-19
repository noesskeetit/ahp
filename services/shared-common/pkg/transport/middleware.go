package transport

import (
	"context"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Authorize is consulted on every request with the verified claims and the
// capability the route requires. A non-nil error (or a panic-free deny) stops
// the request; a nil authorize fails closed.
type Authorize func(ctx context.Context, claims auth.Claims, capability string) error

// Task guards a workload endpoint: a verified client certificate whose single
// SPIFFE URI matches spiffe://ahp/tenant/{tenant}/deployment/{deployment},
// plus a KindTask bearer token bound to that exact workload. Headers are
// never a source of workload identity.
func Task(verifier *auth.Verifier, audience, capability string, authorize Authorize) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			scope, err := workloadScope(r)
			if err != nil {
				WriteError(w, r, err)
				return
			}
			if authorize == nil {
				WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
					"transport: route has no authorization callback"))
				return
			}
			token, err := bearerToken(r)
			if err != nil {
				WriteError(w, r, err)
				return
			}
			claims, err := verifier.Verify(token, audience, auth.KindTask, auth.WorkloadID(scope))
			if err != nil {
				WriteError(w, r, err)
				return
			}
			if claims.Tenant != scope.Tenant || claims.Deployment != scope.Deployment {
				WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
					"transport: token scope does not match the certificate workload"))
				return
			}
			if err := authorize(r.Context(), claims, capability); err != nil {
				WriteError(w, r, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(),
				Principal{Claims: claims, Scope: claims.Scope()})))
		})
	}
}

// TaskOrUser guards a plane that serves platform workloads AND the end users a
// sanctioned edge forwards to it. It is the director's ruling
// 20261101T154234Z item 2 (erratum 20261101T154241Z) turned into code: the user
// edge presents its own client certificate and does not rewrite Authorization,
// so the user's own bearer arrives here and this plane -- not the edge -- is the
// one that has to verify it.
//
// The verified client certificate stays mandatory for BOTH credentials, so this
// can never become a plane a browser talks to directly. A task token is verified
// exactly as Task verifies it, workload binding included. A user credential is
// verified for userAudience and KindHuman with no workload binding, because a
// browser is not a deployment: its token carries tenant and subject, and the
// deployment comes from the verified certificate and from nowhere else. The
// token's tenant must equal the certificate's tenant, so a bearer stolen from
// one tenant cannot be pushed through another tenant's edge.
//
// Passing an empty userAudience reproduces Task byte for byte, including the
// error text existing legs assert on, which keeps the mount default-off. When
// both credentials fail, the task-path error is the one reported, again so that
// adding this path cannot silently change a refusal an existing leg measures.
// Policy that narrows further -- which deployments may carry user credentials at
// all -- belongs to authorize, which sees the filled scope.
func TaskOrUser(verifier *auth.Verifier, taskAudience, userAudience, capability string, authorize Authorize) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			scope, err := workloadScope(r)
			if err != nil {
				WriteError(w, r, err)
				return
			}
			if authorize == nil {
				WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
					"transport: route has no authorization callback"))
				return
			}
			token, err := bearerToken(r)
			if err != nil {
				WriteError(w, r, err)
				return
			}
			claims, taskErr := verifier.Verify(token, taskAudience, auth.KindTask, auth.WorkloadID(scope))
			if taskErr == nil {
				if claims.Tenant != scope.Tenant || claims.Deployment != scope.Deployment {
					WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
						"transport: token scope does not match the certificate workload"))
					return
				}
				if err := authorize(r.Context(), claims, capability); err != nil {
					WriteError(w, r, err)
					return
				}
				next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(),
					Principal{Claims: claims, Scope: claims.Scope()})))
				return
			}
			if userAudience == "" {
				WriteError(w, r, taskErr)
				return
			}
			userClaims, userErr := verifier.Verify(token, userAudience, auth.KindHuman, "")
			if userErr != nil {
				WriteError(w, r, taskErr)
				return
			}
			if userClaims.Tenant != scope.Tenant {
				WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
					"transport: token tenant does not match the certificate workload"))
				return
			}
			// A browser credential carries no deployment and no header may supply
			// one, so the boundary is completed from the proven workload only.
			userClaims.Deployment = scope.Deployment
			if err := authorize(r.Context(), userClaims, capability); err != nil {
				WriteError(w, r, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(),
				Principal{Claims: userClaims, Scope: userClaims.Scope()})))
		})
	}
}

// Human guards a public endpoint: a KindHuman bearer token for audience and
// nothing else. No mTLS is required or consulted here.
func Human(verifier *auth.Verifier, audience string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, err := bearerToken(r)
			if err != nil {
				WriteError(w, r, err)
				return
			}
			claims, err := verifier.Verify(token, audience, auth.KindHuman, "")
			if err != nil {
				WriteError(w, r, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(),
				Principal{Claims: claims, Scope: claims.Scope()})))
		})
	}
}

// Workload guards a deployment-only endpoint (ClaimRun): the verified client
// certificate alone is the identity. No JWT is accepted or required, and no
// subject is selected; the principal scope carries a fixed placeholder
// subject so Scope.Validate still applies.
func Workload() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			scope, err := workloadScope(r)
			if err != nil {
				WriteError(w, r, err)
				return
			}
			scope.Subject = deploymentSubject
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(),
				Principal{Scope: scope})))
		})
	}
}

// deploymentSubject is the dummy subject used to validate a deployment-only
// scope; handlers must never treat it as a real actor.
const deploymentSubject = "deployment"

// workloadScope derives the isolation scope exclusively from the verified
// certificate chain. Any ambiguity is rejected.
func workloadScope(r *http.Request) (contracts.Scope, error) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"transport: a verified client certificate is required")
	}
	var tenant, deployment string
	var count int
	for _, chain := range r.TLS.VerifiedChains {
		if len(chain) == 0 {
			continue
		}
		leaf := chain[0]
		for _, uri := range leaf.URIs {
			count++
			if count > 1 {
				return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
					"transport: certificate carries more than one workload identity")
			}
			t, d, err := parseWorkloadURI(uri.String())
			if err != nil {
				return contracts.Scope{}, err
			}
			tenant, deployment = t, d
		}
	}
	if count != 1 {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"transport: certificate must carry exactly one workload identity")
	}
	scope := contracts.Scope{Tenant: tenant, Deployment: deployment, Subject: deploymentSubject}
	if err := scope.Validate(); err != nil {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"transport: certificate workload identity is not a valid scope")
	}
	return scope, nil
}

// parseWorkloadURI accepts exactly spiffe://ahp/tenant/{t}/deployment/{d}.
// The path follows the SPIFFE-ID standard §2.2: each segment is non-empty and
// made only of ASCII [a-zA-Z0-9._-], is never "." or "..", and carries no
// percent encoding, query, fragment or trailing slash. The trust domain must
// be exactly "ahp": no userinfo, port or extra characters.
func parseWorkloadURI(uri string) (tenant, deployment string, err error) {
	reject := contracts.NewError(contracts.ErrForbidden,
		"transport: certificate URI is not a valid AHP workload identity")
	const prefix = auth.WorkloadScheme + "/tenant/"
	if !strings.HasPrefix(uri, prefix) {
		return "", "", reject
	}
	rest := uri[len(prefix):]
	segments := strings.Split(rest, "/")
	if len(segments) != 3 || segments[1] != "deployment" {
		return "", "", reject
	}
	tenant, deployment = segments[0], segments[2]
	for _, seg := range []string{tenant, deployment} {
		if !validSPIFFEPathSegment(seg) {
			return "", "", reject
		}
	}
	return tenant, deployment, nil
}

// validSPIFFEPathSegment reports whether seg is a legal SPIFFE-ID path
// segment: one or more bytes from [a-zA-Z0-9._-], never "." or "..". This
// rejects empty segments, percent signs, "@", ":", "?", "#", "/" and every
// non-ASCII byte.
func validSPIFFEPathSegment(seg string) bool {
	if seg == "" || seg == "." || seg == ".." {
		return false
	}
	for i := 0; i < len(seg); i++ {
		c := seg[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

// bearerToken extracts the Authorization bearer token, rejecting every other
// scheme and duplicate headers.
func bearerToken(r *http.Request) (string, error) {
	missing := contracts.NewError(contracts.ErrForbidden,
		"transport: a bearer token is required")
	if len(r.Header.Values("Authorization")) != 1 {
		return "", missing
	}
	v := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(v) <= len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
		return "", missing
	}
	token := strings.TrimSpace(v[len(prefix):])
	if token == "" || strings.ContainsAny(token, " \t") {
		return "", missing
	}
	return token, nil
}
