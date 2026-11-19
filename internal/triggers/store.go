package triggers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Trigger lifecycle status values.
const (
	StatusActive    = "ACTIVE"
	StatusRevoked   = "REVOKED"
	StatusExhausted = "EXHAUSTED"
)

// Trigger kinds.
const (
	KindSchedule = "SCHEDULE"
	KindWebhook  = "WEBHOOK"
)

// Trigger is a bounded, explicitly delegated background-work descriptor. It
// stores a delegation reference, never a bearer token (see doc.go).
type Trigger struct {
	TriggerID     string          `json:"trigger_id"`
	Tenant        string          `json:"tenant"`
	Deployment    string          `json:"deployment"`
	Subject       string          `json:"subject"`
	Kind          string          `json:"kind"`
	ParentTask    string          `json:"parent_task"`
	DelegationRef string          `json:"delegation_ref"`
	Delegated     bool            `json:"delegated"`
	Target        string          `json:"target"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	Status        string          `json:"status"`
	IntervalMS    *int64          `json:"interval_ms,omitempty"`
	NextFireAt    *time.Time      `json:"next_fire_at,omitempty"`
	MaxFires      *int64          `json:"max_fires,omitempty"`
	FiredCount    int64           `json:"fired_count"`
	SecretRef     string          `json:"secret_ref,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	Version       int64           `json:"version"`
}

// scope returns the contracts.Scope embedded in the trigger row.
func (t Trigger) scope() contracts.Scope {
	return contracts.Scope{Tenant: t.Tenant, Deployment: t.Deployment, Subject: t.Subject}
}

// triggerSelect is the canonical projection used by every read so scanTrigger
// always sees a stable column order.
const triggerSelect = `trigger_id, tenant, deployment, subject, kind, parent_task,
	delegation_ref, delegated, target, payload::text, status, interval_ms,
	next_fire_at, max_fires, fired_count, secret_ref, created_at, updated_at, version`

// quoteIdent safely quotes a PostgreSQL identifier (schema/table/column) by
// doubling embedded double quotes.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// triggersDDL returns the DDL scripts that create the service schema and its
// triggers table. It is created idempotently and never dropped here.
func triggersDDL(schema string) []string {
	s := quoteIdent(schema)
	return []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, s),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.triggers (
			trigger_id     text        PRIMARY KEY,
			tenant         text        NOT NULL,
			deployment     text        NOT NULL,
			subject        text        NOT NULL,
			kind           text        NOT NULL,
			parent_task    text        NOT NULL,
			delegation_ref text        NOT NULL,
			delegated      bool        NOT NULL DEFAULT false,
			target         text        NOT NULL,
			payload        jsonb       NOT NULL DEFAULT '{}'::jsonb,
			status         text        NOT NULL,
			interval_ms    bigint      NULL,
			next_fire_at   timestamptz NULL,
			max_fires      bigint      NULL,
			fired_count    bigint      NOT NULL DEFAULT 0,
			secret_ref     text        NOT NULL DEFAULT '',
			created_at     timestamptz NOT NULL DEFAULT now(),
			updated_at     timestamptz NOT NULL DEFAULT now(),
			version        bigint      NOT NULL DEFAULT 1
		)`, s),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.webhook_deliveries (
            trigger_id text NOT NULL REFERENCES %s.triggers(trigger_id) ON DELETE CASCADE,
            message_id text NOT NULL,
            body_digest text NOT NULL,
            sequence bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (trigger_id, message_id),
            UNIQUE (trigger_id, sequence)
        )`, s, s),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_triggers_due_idx ON %s.triggers (tenant, deployment, next_fire_at) WHERE kind = 'SCHEDULE' AND status = 'ACTIVE'`, schema, s),
	}
}

// scanTrigger maps a row onto a Trigger. It accepts anything satisfying
// pgx.Row (including pgx.Rows during iteration).
func scanTrigger(row pgx.Row) (Trigger, error) {
	var (
		t        Trigger
		payload  string
		interval *int64
		next     *time.Time
		maxFires *int64
	)
	err := row.Scan(
		&t.TriggerID, &t.Tenant, &t.Deployment, &t.Subject, &t.Kind, &t.ParentTask,
		&t.DelegationRef, &t.Delegated, &t.Target, &payload, &t.Status, &interval,
		&next, &maxFires, &t.FiredCount, &t.SecretRef, &t.CreatedAt, &t.UpdatedAt, &t.Version,
	)
	if err != nil {
		return Trigger{}, err
	}
	t.Payload = json.RawMessage(payload)
	t.IntervalMS = interval
	t.NextFireAt = next
	t.MaxFires = maxFires
	return t, nil
}

// insert persists a new trigger row.
func insertTrigger(ctx context.Context, pool *pgxpool.Pool, schema string, t Trigger) error {
	q := fmt.Sprintf(`INSERT INTO %s.triggers
		(trigger_id, tenant, deployment, subject, kind, parent_task, delegation_ref,
		 delegated, target, payload, status, interval_ms, next_fire_at, max_fires,
		 fired_count, secret_ref, version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16,1)`, quoteIdent(schema))
	payload := string(rawOrEmpty(t.Payload))
	_, err := pool.Exec(ctx, q,
		t.TriggerID, t.Tenant, t.Deployment, t.Subject, t.Kind, t.ParentTask, t.DelegationRef,
		t.Delegated, t.Target, payload, t.Status, t.IntervalMS, t.NextFireAt, t.MaxFires,
		t.FiredCount, t.SecretRef,
	)
	if err != nil {
		return fmt.Errorf("triggers: insert: %w", err)
	}
	return nil
}

// rawOrEmpty returns a non-empty JSON object for a nil/empty raw payload so the
// jsonb column's NOT NULL constraint is satisfied.
func rawOrEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}
