package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Provider quotas. The Model Gateway owns them (spec: it "владеет ... квотами
// провайдеров" beside the generation operations, the attempts and the tree
// budget), and the row that says so on the architecture picture names them
// together with attempts: RPM and TPM are what a real provider answers with 429
// under load, and a gateway that discovers that by exhausting the provider's own
// limit turns a quota into a storm of failed generations.
//
// Two properties the design turns on:
//
//   - The gate sits where a generation operation is ACCEPTED (Service.Submit,
//     which is also where the budget Reserve happens), not in afterwards-written
//     statistics. An exhausted window refuses the operation with a stable reason
//     code and creates nothing: no generation row, no budget hold, no provider
//     call, so there is no silent overrun to reconcile later.
//   - A quota is tenant-scoped configuration (spec: "свои квоты ... на
//     установку/проект", "квоты по tenant"), so limits are rows keyed by
//     (provider, tenant), not a process-wide environment guess. No row means no
//     limit, which keeps an unconfigured deployment byte-identical to the one
//     that has never heard of quotas.
//
// The unit of RPM is one generation ATTEMPT, not one logical operation: a retry
// is a second call the provider will really see, and counting only logical
// operations would understate the window by exactly the retries.
//
// The window is tumbling (bucket = epoch seconds / window_seconds). The known
// edge of a tumbling window is that a full window immediately followed by another
// admits up to 2x the limit across the boundary; the bucketed row shape is
// deliberate so a stricter sliding window can be layered on the same rows without
// inventing a second ledger. This is stated rather than hidden: an operator
// tuning RPM has to know which of the two they bought.

const (
	// ReasonProviderRPMExhausted and ReasonProviderTPMExhausted are the stable
	// reasons a refused generation carries. Two different limits fail for two
	// different operator actions (fewer concurrent requests versus shorter
	// context/completion), and a caller that sees only one code cannot tell which
	// one to back off.
	ReasonProviderRPMExhausted = "provider_rpm_exhausted"
	ReasonProviderTPMExhausted = "provider_tpm_exhausted"

	// QuotaAnyTenant is the tenant value of a provider-wide default limit row.
	// A tenant-specific row always wins over it.
	QuotaAnyTenant = ""

	// DefaultQuotaWindowSeconds is one minute, because the limits are named RPM
	// and TPM.
	DefaultQuotaWindowSeconds = 60
)

// ProviderQuota is one limit row: at most RPM requests and TPM tokens per
// WindowSeconds, for one tenant of one provider. A zero dimension means that
// dimension is not limited -- an operator can cap requests without doing token
// arithmetic, which is how RPM-only providers are actually run.
type ProviderQuota struct {
	Provider      string `json:"provider"`
	Tenant        string `json:"tenant"`
	RPM           int64  `json:"rpm"`
	TPM           int64  `json:"tpm"`
	WindowSeconds int    `json:"window_seconds"`
}

// QuotaWindow is the accounting view of one (provider, tenant) window: what is
// counted in it and what is left. It is also the out-of-band answer --
// ahp_model.quota_windows holds the same numbers in plain rows.
type QuotaWindow struct {
	Provider      string `json:"provider"`
	Tenant        string `json:"tenant"`
	Bucket        int64  `json:"bucket"`
	WindowSeconds int    `json:"window_seconds"`
	Requests      int64  `json:"requests"`
	Tokens        int64  `json:"tokens"`
	LimitRPM      int64  `json:"limit_rpm"`
	LimitTPM      int64  `json:"limit_tpm"`
	// Unlimited says no limit row matched at all, so nothing was counted.
	Unlimited         bool  `json:"unlimited"`
	RemainingRequests int64 `json:"remaining_requests"`
	RemainingTokens   int64 `json:"remaining_tokens"`
}

// ReasonQuotaUnboundedTenant: this deployment declared provider quotas
// mandatory, and the asking tenant carries no limit row. PDF rev3.2 bounds each
// installation's model quotas per tenant (:2495) and keys quotas by tenant and
// execution class (:1542); "mandatory" cannot mean "unbounded for whoever was
// not seeded", so the tenant is refused with a reason instead of being served an
// unlimited window.
const ReasonQuotaUnboundedTenant = "provider_quota_unbounded_tenant"

// QuotaStore owns the quota limit, window and admission tables.
type QuotaStore struct {
	pool *pgxpool.Pool
	log  *slog.Logger
	// requireExplicitLimits turns "no row" from unlimited into a refusal. Off by
	// default, which is what keeps an unconfigured deployment byte-identical to a
	// deployment without the feature.
	requireExplicitLimits bool
}

// NewQuotaStore builds the store over the Model Gateway's pool.
func NewQuotaStore(pool *pgxpool.Pool) *QuotaStore {
	return &QuotaStore{pool: pool, log: slog.Default()}
}

// RequireExplicitLimits sets the operator's fail-closed profile and reports the
// store, so the composition root can state the policy where it mounts the gate.
func (s *QuotaStore) RequireExplicitLimits(required bool) *QuotaStore {
	s.requireExplicitLimits = required
	return s
}

// RequireExplicitLimitsReports whether this gate refuses unseeded tenants.
func (s *QuotaStore) RequireExplicitLimitsReports() bool { return s.requireExplicitLimits }

// Migrate creates the three quota tables. It is idempotent, like every other
// Migrate in this package, and it does not touch ahp_model.budgets or
// ahp_model.budget_reservations: a budget answers "may this task spend", a quota
// answers "may this tenant call this provider again in this minute", and merging
// the two would make one ledger answer two questions with one set of invariants.
//
// Rollback is the documented counterpart: dropping these three tables removes
// the feature completely and leaves every other ahp_model table intact. Limits
// are configuration an operator re-seeds, and window counters are, by definition,
// expired a minute later -- neither is data the product cannot live without.
func quotaMigrationDDL() string {
	return `
CREATE SCHEMA IF NOT EXISTS ahp_model;
CREATE TABLE IF NOT EXISTS ahp_model.provider_quotas (
  provider       text NOT NULL,
  tenant         text NOT NULL,
  rpm            bigint NOT NULL DEFAULT 0 CHECK (rpm >= 0),
  tpm            bigint NOT NULL DEFAULT 0 CHECK (tpm >= 0),
  window_seconds int    NOT NULL DEFAULT 60 CHECK (window_seconds BETWEEN 1 AND 3600),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, tenant)
);
CREATE TABLE IF NOT EXISTS ahp_model.quota_windows (
  provider text   NOT NULL,
  tenant   text   NOT NULL,
  bucket   bigint NOT NULL,
  requests bigint NOT NULL DEFAULT 0 CHECK (requests >= 0),
  tokens   bigint NOT NULL DEFAULT 0 CHECK (tokens >= 0),
  PRIMARY KEY (provider, tenant, bucket)
);
CREATE TABLE IF NOT EXISTS ahp_model.quota_admits (
  provider        text   NOT NULL,
  tenant          text   NOT NULL,
  op_key          text   NOT NULL,
  bucket          bigint NOT NULL,
  requests        bigint NOT NULL CHECK (requests > 0),
  tokens          bigint NOT NULL CHECK (tokens >= 0),
  refunded_tokens bigint NOT NULL DEFAULT 0 CHECK (refunded_tokens >= 0),
  settled         boolean NOT NULL DEFAULT false,
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, tenant, op_key)
);` + tenantInsertGuards("provider_quotas", "quota_windows", "quota_admits")
}
func (s *QuotaStore) Migrate(ctx context.Context) error {
	return migrateModel(ctx, s.pool, quotaMigrationDDL())
}

// Rollback removes the quota tables. It is the explicit rollback of Migrate: a
// deployment that turns the feature off must be able to prove the schema plane it
// added is gone.
func (s *QuotaStore) Rollback(ctx context.Context) error {
	sum := sha256.Sum256([]byte(quotaMigrationDDL()))
	version := hex.EncodeToString(sum[:])
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('ahp_model/schema-migration',0))`); e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, `DROP TABLE IF EXISTS ahp_model.quota_admits;DROP TABLE IF EXISTS ahp_model.quota_windows;DROP TABLE IF EXISTS ahp_model.provider_quotas;`); e != nil {
			return e
		}
		_, e := tx.Exec(ctx, `DELETE FROM ahp_model.schema_migrations WHERE version=$1`, version)
		return e
	})
}

// Set publishes one limit row (upsert). Tenant "" sets the provider-wide default.
func (s *QuotaStore) Set(ctx context.Context, q ProviderQuota) error {
	if q.Provider == "" {
		return contracts.NewError(contracts.ErrInvalid, "quota: provider required")
	}
	if q.RPM < 0 || q.TPM < 0 {
		return contracts.NewError(contracts.ErrInvalid, "quota: limits cannot be negative")
	}
	win := q.WindowSeconds
	if win == 0 {
		win = DefaultQuotaWindowSeconds
	}
	if win < 1 || win > 3600 {
		return contracts.NewError(contracts.ErrInvalid, "quota: window_seconds must be between 1 and 3600")
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO ahp_model.provider_quotas (provider, tenant, rpm, tpm, window_seconds, updated_at)
VALUES ($1,$2,$3,$4,$5, now())
ON CONFLICT (provider, tenant) DO UPDATE
   SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
       window_seconds = EXCLUDED.window_seconds, updated_at = now()`,
		q.Provider, q.Tenant, q.RPM, q.TPM, win)
	return err
}

// Delete removes one limit row, which returns that (provider, tenant) to
// unlimited.
func (s *QuotaStore) Delete(ctx context.Context, provider, tenant string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM ahp_model.provider_quotas WHERE provider=$1 AND tenant=$2`, provider, tenant)
	return err
}

// Limits reads the effective limit for one (provider, tenant): the tenant's own
// row when it has one, otherwise the provider-wide default, otherwise none.
func (s *QuotaStore) Limits(ctx context.Context, provider, tenant string) (ProviderQuota, bool, error) {
	var q ProviderQuota
	err := s.pool.QueryRow(ctx, `
SELECT provider, tenant, rpm, tpm, window_seconds
  FROM ahp_model.provider_quotas
 WHERE provider=$1 AND tenant = ANY($2::text[])
 ORDER BY (tenant = $3) DESC, tenant DESC
 LIMIT 1`, provider, []string{tenant, QuotaAnyTenant}, tenant,
	).Scan(&q.Provider, &q.Tenant, &q.RPM, &q.TPM, &q.WindowSeconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderQuota{}, false, nil
	}
	if err != nil {
		return ProviderQuota{}, false, err
	}
	return q, true, nil
}

// Admit is the quota gate: it books one operation's worth of requests and tokens
// against the current window and refuses if either limit would be crossed.
//
// opKey is the caller's own operation/attempt idempotency key (for a generation
// it is the generation key), which is what makes a retry of an ALREADY ADMITTED
// attempt a replay instead of a second charge against the window -- the case that
// would otherwise turn every provider timeout into double-counted quota.
//
// Atomicity: one transaction that takes a row lock on the window before deciding,
// so parallel admissions serialise on it and cannot both pass a limit they would
// jointly cross. The admission row is inserted in the same transaction and
// vanishes with the rollback when the answer is no, so a refusal leaves nothing
// behind.
//
// The refusal is a structured contracts error with a stable reason
// (ReasonProviderRPMExhausted / ReasonProviderTPMExhausted) and a message that
// names only limits and counters: the rejected request's content is never echoed,
// so a refusal cannot be used to read a prompt back.
func (s *QuotaStore) Admit(ctx context.Context, provider, tenant, opKey string, requests, tokens int64) (QuotaWindow, error) {
	if provider == "" || tenant == "" || opKey == "" {
		return QuotaWindow{}, contracts.NewError(contracts.ErrInvalid,
			"quota: provider, tenant and operation key are required")
	}
	if requests <= 0 {
		return QuotaWindow{}, contracts.NewError(contracts.ErrInvalid, "quota: at least one request is admitted")
	}
	if tokens < 0 {
		return QuotaWindow{}, contracts.NewError(contracts.ErrInvalid, "quota: tokens cannot be negative")
	}
	limits, found, err := s.Limits(ctx, provider, tenant)
	if err != nil {
		return QuotaWindow{}, fmt.Errorf("model: quota limits: %w", err)
	}
	if !found {
		if s.requireExplicitLimits {
			s.log.Warn("model: provider quota is mandatory and this tenant carries no limit row - operation refused",
				"provider", provider, "tenant", tenant, "reason", ReasonQuotaUnboundedTenant,
				"remedy", "seed ahp_model.provider_quotas for this tenant (scripts/apply-provider-quotas.sh)")
			return QuotaWindow{}, contracts.NewReasoned(contracts.ErrConflict, ReasonQuotaUnboundedTenant,
				fmt.Sprintf("model: provider %q quotas are mandatory here and tenant %q has no limit row in ahp_model.provider_quotas: refusing rather than serving an unbounded window",
					provider, tenant))
		}
		// No limit row: not "unlimited inside a window", but no window at all.
		// Nothing is written, so an unconfigured deployment leaves the quota
		// tables empty and pays no cost for a feature it did not turn on.
		return QuotaWindow{Provider: provider, Tenant: tenant, Unlimited: true}, nil
	}
	window := limits.WindowSeconds
	if window <= 0 {
		window = DefaultQuotaWindowSeconds
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return QuotaWindow{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The bucket is computed by the database, not by this process: two gateway
	// replicas whose clocks disagree by seconds must not land in different windows
	// and admit the same request twice.
	var bucket int64
	if err := tx.QueryRow(ctx,
		`SELECT floor(EXTRACT(epoch FROM now()) / $1)::bigint`, window).Scan(&bucket); err != nil {
		return QuotaWindow{}, err
	}

	var replayed bool
	var bookedRequests, bookedTokens int64
	err = tx.QueryRow(ctx, `
INSERT INTO ahp_model.quota_admits (provider, tenant, op_key, bucket, requests, tokens)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (provider, tenant, op_key) DO NOTHING
RETURNING requests, tokens`,
		provider, tenant, opKey, bucket, requests, tokens,
	).Scan(&bookedRequests, &bookedTokens)
	if errors.Is(err, pgx.ErrNoRows) {
		// This attempt was admitted before (a retry, or a replay after a crash).
		// Its cost is already in the window; charging it again would be the
		// double-count this key exists to prevent.
		replayed = true
	} else if err != nil {
		return QuotaWindow{}, fmt.Errorf("model: quota admit journal: %w", err)
	}

	// Open the window row and lock it: every parallel admission of this
	// (provider, tenant) waits here, which is what makes the decision below
	// atomic instead of advisory.
	if _, err := tx.Exec(ctx, `
INSERT INTO ahp_model.quota_windows (provider, tenant, bucket, requests, tokens)
VALUES ($1,$2,$3,0,0) ON CONFLICT (provider, tenant, bucket) DO NOTHING`,
		provider, tenant, bucket); err != nil {
		return QuotaWindow{}, err
	}
	var curRequests, curTokens int64
	if err := tx.QueryRow(ctx, `
SELECT requests, tokens FROM ahp_model.quota_windows
 WHERE provider=$1 AND tenant=$2 AND bucket=$3 FOR UPDATE`,
		provider, tenant, bucket).Scan(&curRequests, &curTokens); err != nil {
		return QuotaWindow{}, err
	}

	if replayed {
		if err := tx.Commit(ctx); err != nil {
			return QuotaWindow{}, err
		}
		return s.windowOf(provider, tenant, limits, curRequests, curTokens), nil
	}

	addRequests, addTokens := requests, tokens
	if limits.RPM > 0 && curRequests+addRequests > limits.RPM {
		// Refusal: the transaction is rolled back, so the admission row above
		// never exists and nothing was spent.
		return QuotaWindow{}, s.refuse(provider, tenant, limits,
			ReasonProviderRPMExhausted, "rpm", limits.RPM, curRequests, addRequests)
	}
	if limits.TPM > 0 && curTokens+addTokens > limits.TPM {
		return QuotaWindow{}, s.refuse(provider, tenant, limits,
			ReasonProviderTPMExhausted, "tpm", limits.TPM, curTokens, addTokens)
	}
	if _, err := tx.Exec(ctx, `
UPDATE ahp_model.quota_windows SET requests = requests + $4, tokens = tokens + $5
 WHERE provider=$1 AND tenant=$2 AND bucket=$3`,
		provider, tenant, bucket, addRequests, addTokens); err != nil {
		return QuotaWindow{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return QuotaWindow{}, err
	}
	return s.windowOf(provider, tenant, limits, curRequests+addRequests, curTokens+addTokens), nil
}

// refuse builds the structured refusal. Only numbers the operator needs and the
// identities the caller already knows appear in it -- never the request content,
// so a refusal cannot be used to read a rejected prompt back.
func (s *QuotaStore) refuse(provider, tenant string, limits ProviderQuota,
	reason, dimension string, limit, counted, needed int64) error {
	s.log.Warn("model: provider quota exhausted - operation refused",
		"provider", provider, "tenant", tenant, "reason", reason,
		"limit", limit, "window_seconds", limits.WindowSeconds,
		"counted", counted, "needed", needed)
	return contracts.NewReasoned(contracts.ErrConflict, reason, fmt.Sprintf(
		"model: provider %q %s quota exhausted for tenant %q: window holds %d of %d, this operation needs %d",
		provider, dimension, tenant, counted, limit, needed))
}

func (s *QuotaStore) windowOf(provider, tenant string, limits ProviderQuota, requests, tokens int64) QuotaWindow {
	w := QuotaWindow{
		Provider: provider, Tenant: tenant, WindowSeconds: limits.WindowSeconds,
		Requests: requests, Tokens: tokens, LimitRPM: limits.RPM, LimitTPM: limits.TPM,
	}
	if limits.RPM > 0 {
		w.RemainingRequests = limits.RPM - requests
		if w.RemainingRequests < 0 {
			w.RemainingRequests = 0
		}
	} else {
		w.RemainingRequests = -1 // unlimited in this dimension
	}
	if limits.TPM > 0 {
		w.RemainingTokens = limits.TPM - tokens
		if w.RemainingTokens < 0 {
			w.RemainingTokens = 0
		}
	} else {
		w.RemainingTokens = -1
	}
	return w
}

// Window reads the current window of one (provider, tenant) without admitting
// anything. This is the operator/verifier read; the same numbers sit in
// ahp_model.quota_windows as plain rows.
func (s *QuotaStore) Window(ctx context.Context, provider, tenant string) (QuotaWindow, error) {
	limits, found, err := s.Limits(ctx, provider, tenant)
	if err != nil {
		return QuotaWindow{}, err
	}
	if !found {
		return QuotaWindow{Provider: provider, Tenant: tenant, Unlimited: true}, nil
	}
	window := limits.WindowSeconds
	if window <= 0 {
		window = DefaultQuotaWindowSeconds
	}
	var bucket, requests, tokens int64
	err = s.pool.QueryRow(ctx, `
SELECT floor(EXTRACT(epoch FROM now()) / $1)::bigint AS bucket,
       COALESCE(w.requests,0), COALESCE(w.tokens,0)
  FROM (SELECT 1) d
  LEFT JOIN ahp_model.quota_windows w
         ON w.provider=$2 AND w.tenant=$3 AND w.bucket = floor(EXTRACT(epoch FROM now()) / $1)::bigint`,
		window, provider, tenant).Scan(&bucket, &requests, &tokens)
	if err != nil {
		return QuotaWindow{}, err
	}
	w := s.windowOf(provider, tenant, limits, requests, tokens)
	w.Bucket = bucket
	return w, nil
}

// Refund converges one admission with what the provider actually consumed.
//
// A generation is admitted with an upper bound (a request cannot be admitted for
// fewer tokens than it may generate), and the window must end up holding the real
// usage, not the pessimistic bound, or a tenant would pay for tokens it never got
// and be throttled by a debt it does not owe.
//
// It is once-only per opKey and clamps in both directions: an overrunning provider
// cannot push the window negative, and a replay of a refund changes nothing.
func (s *QuotaStore) Refund(ctx context.Context, provider, tenant, opKey string, actualTokens int64) error {
	if provider == "" || tenant == "" || opKey == "" {
		return contracts.NewError(contracts.ErrInvalid,
			"quota: provider, tenant and operation key are required to refund")
	}
	if actualTokens < 0 {
		actualTokens = 0
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var bucket, booked int64
	var settled bool
	err = tx.QueryRow(ctx, `
SELECT bucket, tokens, settled FROM ahp_model.quota_admits
 WHERE provider=$1 AND tenant=$2 AND op_key=$3 FOR UPDATE`,
		provider, tenant, opKey).Scan(&bucket, &booked, &settled)
	if errors.Is(err, pgx.ErrNoRows) {
		// Nothing was booked under this key -- either quotas were not mounted when
		// the generation was admitted, or the operation never reached admission.
		// There is nothing to converge, and that is not an error.
		return nil
	}
	if err != nil {
		return fmt.Errorf("model: quota refund read: %w", err)
	}
	if settled {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return nil
	}
	delta := booked - actualTokens
	if delta < 0 {
		delta = 0 // the provider overran the bound: the window keeps the booked figure
	}
	if _, err := tx.Exec(ctx, `
UPDATE ahp_model.quota_windows SET tokens = GREATEST(tokens - $4, 0)
 WHERE provider=$1 AND tenant=$2 AND bucket=$3`,
		provider, tenant, bucket, delta); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
UPDATE ahp_model.quota_admits SET settled = true, refunded_tokens = $4
 WHERE provider=$1 AND tenant=$2 AND op_key=$3`,
		provider, tenant, opKey, delta); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// AdmittedProviders lists which provider copies booked an operation key for one
// tenant. It exists because the copy a generation was routed to is decided per
// request (label-aware routing can send two turns of one tenant to two copies),
// while the settle path runs later, possibly in another process after a crash,
// with only the generation key in hand. The admit journal is the durable record of
// what was actually booked, so convergence asks the journal instead of guessing the
// name from the environment. Content-free: a provider name and a key, no request
// bytes.
func (s *QuotaStore) AdmittedProviders(ctx context.Context, tenant, opKey string) ([]string, error) {
	if tenant == "" || opKey == "" {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"quota: tenant and operation key are required to list admissions")
	}
	rows, err := s.pool.Query(ctx, `
SELECT DISTINCT provider FROM ahp_model.quota_admits
 WHERE tenant=$1 AND op_key=$2 ORDER BY provider`, tenant, opKey)
	if err != nil {
		return nil, fmt.Errorf("model: quota admission lookup: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("model: quota admission lookup scan: %w", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("model: quota admission lookup: %w", err)
	}
	return out, nil
}

// RefundByOpKey converges the window of whichever copy actually booked the
// operation. With one configured provider it converges exactly the row Refund would
// have converged, so a deployment that has never heard of routing behaves as
// before; with several copies it stops the default copy from being charged for a
// turn the router sent elsewhere, and stops the restricted copy from keeping a
// pessimistic window forever. Once-only per booking, so every settle path can call
// it. An unsettled booking for a key that was never admitted is not an error.
func (s *QuotaStore) RefundByOpKey(ctx context.Context, tenant, opKey string, actualTokens int64) error {
	providers, err := s.AdmittedProviders(ctx, tenant, opKey)
	if err != nil {
		return err
	}
	for _, name := range providers {
		if err := s.Refund(ctx, name, tenant, opKey, actualTokens); err != nil {
			return err
		}
	}
	return nil
}
