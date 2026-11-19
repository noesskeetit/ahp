package identity

import (
	"context"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// IssueTaskToken reissues a task credential to any verified replica of its
// owning workload (PDF 3.2 §§2.4, 6.2). The caller supplies only its transport-
// verified tenant/deployment and a task; Subject is deliberately discarded.
// Ownership is resolved inside that workload, never by a caller-selected subject.
// Ambiguous task IDs, including inactive collisions, fail closed. The common
// MintTask path retains live policy, ancestry, tombstone, expiry and audit checks.
func (s *Store) IssueTaskToken(ctx context.Context, workload contracts.Scope, task string,
	signer *auth.Signer, audience string, ttl time.Duration) (string, error) {
	workload.Subject = "deployment"
	if err := workload.Validate(); err != nil {
		return "", err
	}
	if err := validateTask(task); err != nil {
		return "", err
	}
	if s == nil || s.resources == nil {
		return "", contracts.NewError(contracts.ErrUnavailable, "identity: resource authority unavailable")
	}
	token, err := s.mintTask(ctx, workload, task, signer, audience, ttl, "", true)
	if err == nil {
		return token, nil
	}
	switch contracts.Code(err) {
	case contracts.ErrInvalid:
		return "", err
	case contracts.ErrForbidden, contracts.ErrNotFound:
		return "", issueDenied()
	default:
		return "", contracts.NewError(contracts.ErrUnavailable, "identity: task authority unavailable")
	}
}

func issueDenied() error { return forbidden("identity: task token issuance denied") }

// The LIMIT bounds collision handling; even revoked rows remain candidates so
// reusing a task cannot change its subject. No rows from another actor are read.
func findWorkloadGrant(ctx context.Context, tx pgx.Tx, workload contracts.Scope, task string) (Grant, bool, error) {
	const q = `SELECT * FROM (
 SELECT 'root' AS kind, ` + grantColumns + ` FROM ahp_identity.grants_root
 WHERE tenant=$1 AND deployment=$2 AND task=$3
 UNION ALL
 SELECT 'child' AS kind, ` + grantColumns + ` FROM ahp_identity.grants_child
 WHERE tenant=$1 AND deployment=$2 AND task=$3
 ) AS candidates LIMIT 2`
	g, found, err := scanGrants(tx.Query(ctx, q, workload.Tenant, workload.Deployment, task))
	if contracts.Code(err) == contracts.ErrConflict {
		return Grant{}, false, issueDenied()
	}
	return g, found, err
}
