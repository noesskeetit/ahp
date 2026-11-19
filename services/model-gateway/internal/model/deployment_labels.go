package model

// Server-side label records for routing (PDF rev3.2 :1620-1630, "маршруты с учётом
// меток"; :1542 "квоты по tenant и классу исполнения").
//
// WHY THIS TABLE EXISTS, AND WHY NOTHING ELSE MAY FEED ROUTING LABELS
//
// A routing decision that gates a provider on data labels is only worth what its
// label source is worth. The alternatives were measured and rejected by the
// director (2026-11-02): a label claim inside the token is signed INTENT from the
// caller, and a field on the request is a client assertion -- in both cases the
// party asking to be routed to a less restricted upstream is the party stating the
// labels, which is not a gate. The identity grant does carry labels, but it labels
// the SUBJECT, not the place the data is being sent FROM, and a deployment is the
// unit the platform isolates and attests (the SPIFFE workload in the verified
// certificate).
//
// So the source of truth is a server-maintained record per (tenant, deployment),
// read by the gateway with the deployment id the transport ATTESTED (the
// certificate's workload, never a body field). There is deliberately no HTTP face
// on this store: the writer is the operator plane, and an unauthenticated writer
// would put the client assertion back through a different door.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const deploymentLabelsDDL = `CREATE SCHEMA IF NOT EXISTS ahp_model;
CREATE TABLE IF NOT EXISTS ahp_model.deployment_labels (
  tenant     text NOT NULL,
  deployment text NOT NULL,
  labels     jsonb NOT NULL,
  source     text NOT NULL DEFAULT 'operator',
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant, deployment),
  CONSTRAINT deployment_labels_tenant_check CHECK (tenant <> ''),
  CONSTRAINT deployment_labels_deployment_check CHECK (deployment <> ''),
  CONSTRAINT deployment_labels_source_check CHECK (source <> '')
);`

// DeploymentLabelStore is the operator-owned mapping from a workload deployment to
// the labels its data carries. It adds no policy and decides nothing: it answers
// one question -- what labels does this deployment's data carry -- and the routing
// gate does the deciding.
type DeploymentLabelStore struct {
	pool *pgxpool.Pool
}

// NewDeploymentLabelStore builds the store on an existing pool.
func NewDeploymentLabelStore(pool *pgxpool.Pool) *DeploymentLabelStore {
	return &DeploymentLabelStore{pool: pool}
}

// Migrate creates the table. It changes no existing plane: budgets, reservations,
// generations and provider quotas are untouched, so an accepted deployment keeps
// its accounting byte-for-byte and the label column simply has no records until an
// operator writes them.
func (s *DeploymentLabelStore) Migrate(ctx context.Context) error {
	if err := migrateModel(ctx, s.pool, deploymentLabelsDDL+tenantInsertGuards("deployment_labels")); err != nil {
		return fmt.Errorf("model: deployment labels migrate: %w", err)
	}
	return nil
}

// Set records the labels for one deployment. The write REPLACES the record: the
// record states the deployment's current classification, it is not a history, and
// pretending otherwise (an append-only widening here) would let a stale record
// keep a deployment locked to an old classification after the operator corrected
// it. Widening vs narrowing is the operator's decision, not something a stored
// procedure may silently veto.
func (s *DeploymentLabelStore) Set(ctx context.Context, tenant, deployment string, set labels.Set, source string) error {
	if strings.TrimSpace(tenant) == "" || strings.TrimSpace(deployment) == "" {
		return errors.New("model: deployment labels need a tenant and a deployment")
	}
	if strings.TrimSpace(source) == "" {
		source = "operator"
	}
	raw, err := json.Marshal(set)
	if err != nil {
		return fmt.Errorf("model: deployment labels encode: %w", err)
	}
	const q = `
INSERT INTO ahp_model.deployment_labels (tenant, deployment, labels, source, updated_at)
VALUES ($1, $2, $3::jsonb, $4, now())
ON CONFLICT (tenant, deployment) DO UPDATE
   SET labels = EXCLUDED.labels, source = EXCLUDED.source, updated_at = now()`
	if _, err := s.pool.Exec(ctx, q, tenant, deployment, raw, source); err != nil {
		return fmt.Errorf("model: deployment labels set: %w", err)
	}
	return nil
}

// LabelsFor returns the labels this deployment's data carries. No record is NOT a
// failure and is NOT "unrestricted": it is the empty set, which the routing gate
// still checks against the destination. Returning an error here would turn "the
// operator classified nobody" into an outage, and an operator reading an error
// would not learn that the classification is simply missing.
func (s *DeploymentLabelStore) LabelsFor(ctx context.Context, tenant, deployment string) (labels.Set, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `
SELECT labels FROM ahp_model.deployment_labels WHERE tenant = $1 AND deployment = $2`,
		tenant, deployment).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return labels.Set{}, nil
	}
	if err != nil {
		return labels.Set{}, fmt.Errorf("model: deployment labels read: %w", err)
	}
	var set labels.Set
	if uerr := json.Unmarshal(raw, &set); uerr != nil {
		// Fail closed with a named cause: a record that cannot be read is a
		// classification the gateway cannot vouch for, and routing it as
		// unlabelled would be the one way to smuggle tainted data out.
		return labels.Set{}, fmt.Errorf("model: deployment labels record for %s is unreadable: %w", deployment, uerr)
	}
	return set, nil
}

// RecordCount is an operator/verifier figure: how many deployments are classified.
// It answers "is the label gate actually fed on this stand" without reading any
// label value.
func (s *DeploymentLabelStore) RecordCount(ctx context.Context) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*)::int FROM ahp_model.deployment_labels`).Scan(&n); err != nil {
		return 0, fmt.Errorf("model: deployment labels count: %w", err)
	}
	return n, nil
}
