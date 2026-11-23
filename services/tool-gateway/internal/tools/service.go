package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// EffectKind is the ledger journal kind for a tool-call side effect.
const EffectKind = "tool.call"

// schemaRe guards the interpolated schema identifier so the caller-supplied
// schema cannot become an injection vector into the DDL/DML below.
var schemaRe = regexp.MustCompile(`^ahp_[a-z][a-z0-9_]{0,40}$`)

// isDigest reports whether s looks like a contracts.Digest value.
func isDigest(s string) bool {
	const p = "sha256:"
	return strings.HasPrefix(s, p) && len(s) == len(p)+64
}

// Config wires a Service to its collaborators. Every field except Audience and
// Verifier is required: a Service that cannot reach a Guardian is unsafe to
// construct.
type Config struct {
	// OAuth handles personal connector consent, rotation and revocation.
	OAuth *OAuthManager

	Guardian    Guardian
	Credentials CredentialResolver
	Connections map[string]Connector
	Tools       map[string]ToolSpec
	Verifier    *auth.Verifier
	Audience    string
	// ApprovalTTL bounds how long a pending human approval stays consumable.
	// Zero selects defaultApprovalTTL. Expiry is checked at the gate and inside
	// the atomic consume (root audit 2026-11-02, P1 requirement 3).
	ApprovalTTL time.Duration
	// ApprovalAuthorize verifies that the owner explicitly assigned this human as approver.
	// Nil restricts approvals to the owner. It must not grant tenant-wide approval.
	ApprovalAuthorize func(context.Context, contracts.Scope, string) error
	// Grants revalidates that the personal connection grant behind a stored
	// result is STILL active at read time, without touching the external
	// provider. *OAuthManager implements it; a Service without OAuth leaves it
	// nil and the check is skipped (no personal consent exists to check).
	Grants GrantChecker
	// Artifacts, when set, receives results larger than ArtifactThreshold so
	// they are stored out of line and referenced immutably instead of failing
	// the completion. Nil keeps the accepted behaviour: an oversized result is
	// an explicit error, never a silent truncation.
	Artifacts ArtifactSink
	// JournalArtifacts spills fenced results under their verified session.
	JournalArtifacts *artifacts.Store
	// ArtifactThreshold is the byte size above which Artifacts is consulted.
	// Zero selects defaultArtifactThreshold.
	ArtifactThreshold int
	// MaxUpstreamBytes is the service-side ceiling for one tool result. It
	// backs the transport bound (UpstreamLimiter) with a second check that a
	// connector cannot bypass, and it is what makes "no silent truncation"
	// decidable: anything above it is refused explicitly. Zero selects
	// DefaultMaxUpstreamBytes; a negative value disables the ceiling.
	MaxUpstreamBytes int

	// UserAudience is the audience of HUMAN browser credentials on an
	// Identity-composed front (DEF-TGW-2, director contract 2026-11-02): the
	// sanctioned human token is minted with this audience by the browser flow,
	// and the human Approve route re-verifies its bearer for Audience OR
	// UserAudience (KindHuman only, both cases). Empty keeps the pre-existing
	// single-audience behaviour byte-identical.
	UserAudience string
}

// Service is the execution-plane tool plane: it authorises a call through the
// independent Guardian, enforces human approvals for gated tools, journals the
// effect in the service-owned ledger (so an ambiguous transport outcome yields
// SIDE_EFFECT_UNKNOWN rather than an automatic replay), then performs the MCP
// call.
type Service struct {
	pool     *pgxpool.Pool
	schema   string
	ledger   *operations.Store
	cfg      Config
	verifier *auth.Verifier
	audience string
	policy   *DBPolicySource
}

// New validates configuration, ensures the ledger and the approval/audit
// tables exist, and returns a ready Service.
func New(ctx context.Context, pool *pgxpool.Pool, schema string, cfg Config) (*Service, error) {
	if !schemaRe.MatchString(schema) {
		return nil, contracts.NewError(contracts.ErrInvalid, "tools: schema name must match ^ahp_[a-z][a-z0-9_]{0,40}$")
	}
	if cfg.Guardian == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "tools: Guardian is required")
	}
	if cfg.Credentials == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "tools: CredentialResolver is required")
	}
	if len(cfg.Connections) == 0 || len(cfg.Tools) == 0 {
		return nil, contracts.NewError(contracts.ErrInvalid, "tools: Connections and Tools are required")
	}
	for name, spec := range cfg.Tools {
		if _, ok := cfg.Connections[spec.Connection]; !ok {
			return nil, contracts.NewError(contracts.ErrInvalid, "tools: tool "+name+" references unknown connection "+spec.Connection)
		}
	}
	ledger, err := operations.New(pool, schema)
	if err != nil {
		return nil, err
	}
	if err := ledger.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("tools: migrate ledger: %w", err)
	}
	s := &Service{
		pool:     pool,
		schema:   schema,
		ledger:   ledger,
		cfg:      cfg,
		verifier: cfg.Verifier,
		audience: cfg.Audience,
	}
	s.policy = newDBPolicySource(pool, schema)
	s.policy.ledger = s.ledger
	if pg, ok := s.cfg.Guardian.(*PolicyGuardian); ok {
		pg.SetPolicySource(s.policy)
	}
	if err := s.applyDDL(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// applyDDL creates the approval and Guardian audit tables inside the service
// schema. The operations table is created by ledger.Migrate.
func (s *Service) applyDDL(ctx context.Context) error {
	stmts := []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, s.schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.approvals (
			tenant      text        NOT NULL,
			digest      text        NOT NULL,
			request_id  text        NOT NULL,
			tool        text        NOT NULL,
			approved_by text        NULL,
			created_at  timestamptz NOT NULL DEFAULT now(),
			approved_at timestamptz NULL,
			deployment  text        NOT NULL DEFAULT '',
			subject     text        NOT NULL DEFAULT '',
			op_key      text        NOT NULL DEFAULT '',
			expires_at  timestamptz NULL,
			consumed_at timestamptz NULL,
			declined_by text        NULL,
			declined_at timestamptz NULL,
			PRIMARY KEY (tenant, digest, tool, deployment, subject, op_key)
		)`, s.schema),
		// Idempotent in-place migration for tables created before the P1
		// one-shot contract: widen the row (operation binding + spend/expiry
		// stamps) and replace the tenant+digest primary key with the operation
		// identity. Legacy rows keep empty bindings, which no keyed Call matches
		// (fail closed), and remain visible to a human until they expire.
		fmt.Sprintf(`ALTER TABLE %s.approvals ADD COLUMN IF NOT EXISTS deployment  text NOT NULL DEFAULT ''`, s.schema),
		fmt.Sprintf(`ALTER TABLE %s.approvals ADD COLUMN IF NOT EXISTS subject     text NOT NULL DEFAULT ''`, s.schema),
		fmt.Sprintf(`ALTER TABLE %s.approvals ADD COLUMN IF NOT EXISTS op_key      text NOT NULL DEFAULT ''`, s.schema),
		fmt.Sprintf(`ALTER TABLE %s.approvals ADD COLUMN IF NOT EXISTS expires_at  timestamptz NULL`, s.schema),
		fmt.Sprintf(`ALTER TABLE %s.approvals ADD COLUMN IF NOT EXISTS consumed_at timestamptz NULL`, s.schema),
		// GAP-DOTE2E-7: the human answer is not binary-implicit any more. A
		// decline is stored on the same operation-bound row (no second ledger)
		// with who refused and when.
		fmt.Sprintf(`ALTER TABLE %s.approvals ADD COLUMN IF NOT EXISTS declined_by text NULL`, s.schema),
		fmt.Sprintf(`ALTER TABLE %s.approvals ADD COLUMN IF NOT EXISTS declined_at timestamptz NULL`, s.schema),
		fmt.Sprintf(`ALTER TABLE %s.approvals DROP CONSTRAINT IF EXISTS approvals_pkey`, s.schema),
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS approvals_op_identity_uk ON %s.approvals (tenant, digest, tool, deployment, subject, op_key)`, s.schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS approvals_tenant_pending_ix ON %s.approvals (tenant, digest, tool) WHERE approved_by IS NOT NULL AND consumed_at IS NULL`, s.schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.guardian_log (
			id             bigserial   PRIMARY KEY,
			tenant         text        NOT NULL,
			deployment     text        NOT NULL,
			subject        text        NOT NULL,
			tool           text        NOT NULL,
			side_effecting bool        NOT NULL,
			digest         text        NOT NULL,
			decision       text        NOT NULL,
			reason         text        NOT NULL,
			class          text        NOT NULL,
			created_at     timestamptz NOT NULL DEFAULT now()
		)`, s.schema),
	}
	if s.policy != nil {
		stmts = append(stmts, s.policy.policyDDL()...)
	}
	for _, q := range stmts {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("tools: apply ddl: %w", err)
		}
	}
	return nil
}

// Call runs the full authorisation -> journal -> execute path for a tool call
// keyed by an idempotency key. Re-running with the same key and identical
// arguments returns the already-recorded operation; a different payload under
// the same key is rejected as KEY_REUSE_MISMATCH. Side effects are never
// automatically replayed: an operation left in SIDE_EFFECT_UNKNOWN is returned
// as-is for reconciliation.
func (s *Service) Call(ctx context.Context, scope contracts.Scope, key, tool string, args json.RawMessage) (operations.Operation, error) {
	return s.call(ctx, scope, key, tool, args, CallProvenance{}, nil, artifacts.Binding{})
}

// CallWithProvenance is Call plus the immutable labels that describe WHO was
// authorised to send this exact effect. prov must come from a VERIFIED
// credential (the HTTP face builds it from verified claims); it is never read
// from a request body, so a caller cannot forge or choose its own provenance.
// The labels are written in the same transaction that grants the dispatch,
// which is what lets GetResult prove that the reading credential belongs to
// the same context that paid for the effect.
func (s *Service) CallWithProvenance(ctx context.Context, scope contracts.Scope, key, tool string, args json.RawMessage, prov CallProvenance) (operations.Operation, error) {
	return s.call(ctx, scope, key, tool, args, prov, nil, artifacts.Binding{})
}

func (s *Service) call(ctx context.Context, scope contracts.Scope, key, tool string, args json.RawMessage, prov CallProvenance, check func(context.Context, pgx.Tx) error, binding artifacts.Binding) (operations.Operation, error) {
	if err := scope.Validate(); err != nil {
		return operations.Operation{}, err
	}
	if key == "" || len(key) > 128 {
		return operations.Operation{}, contracts.NewError(contracts.ErrInvalid, "tools: key must be 1..128 bytes")
	}
	spec, ok := s.cfg.Tools[tool]
	if !ok {
		return operations.Operation{}, contracts.NewError(contracts.ErrNotFound, "tools: unknown tool "+tool)
	}
	// AUTH_REQUIRED is raised before effect dispatch or approval consumption.
	// The task receives only a status; the consent URL is confined to the human API.
	if s.cfg.OAuth != nil {
		if err := s.cfg.OAuth.Require(ctx, scope, spec.Connection); err != nil {
			return operations.Operation{}, err
		}
	}
	digest, err := ApprovalDigest(tool, args)
	if err != nil {
		return operations.Operation{}, err
	}

	guard := ""
	if spec.SideEffecting {
		guard = digest
	}
	request := operations.Request{Key: key, Kind: EffectKind, Target: tool, Guard: guard, Arguments: args}
	fingerprint, err := operations.RequestFingerprint(request)
	if err != nil {
		return operations.Operation{}, err
	}
	replay := func(prior operations.Operation, err error) (operations.Operation, error) {
		if err != nil {
			return operations.Operation{}, err
		}
		if prior.Target != tool || prior.Digest != fingerprint {
			return operations.Operation{}, contracts.NewError(contracts.ErrKeyReuse, "tools: operation key was used with a different request")
		}
		return prior, nil
	}

	// Human approval gate FIRST (DEF-TGW-1:2). A tool that forces approval must
	// already carry a human sign-off for the canonical digest before any Guardian
	// decision or ledger mutation. The first refusal also registers the pending
	// approval request the human then signs, so the whole flow is drivable over
	// the public face with no second gate. A missing approval is reported
	// distinctly from a Guardian deny (approval => 409 sentinel, guardian => 403).
	if spec.RequiresApproval {
		approved, reason, err := s.approvalGate(ctx, scope, key, digest, tool)
		if err != nil {
			return operations.Operation{}, err
		}
		switch {
		case approved:
			// Eligible signature: fall through to the Guardian; the durable
			// authorisation is the atomic consume at the fresh-dispatch point.
		case reason == "missing" || reason == "unapproved":
			// First refusal registers the pending request BOUND to this exact
			// operation (scope + key). A spent or expired approval is never
			// silently re-raised: a human signature funds one effect; a second
			// effect needs a fresh request and a fresh signature.
			if _, _, err := s.CreateApprovalRequest(ctx, scope, key, tool, args); err != nil {
				return operations.Operation{}, err
			}
			return operations.Operation{}, ErrApprovalRequired
		case reason == "expired":
			return operations.Operation{}, fmt.Errorf("%w (reason: expired)", ErrApprovalRequired)
		case reason == "declined":
			// The employee already answered this operation. Refund nothing,
			// re-raise nothing: a new intent needs a new operation key and a
			// fresh human decision.
			return operations.Operation{}, fmt.Errorf("%w (operation key refused)", ErrApprovalDeclined)
		case reason == "consumed":
			// A CONSUMED row under this exact key means the bound operation was
			// already authorised once. Only a REPLAY is possible from here: if
			// the ledger holds a completed or UNKNOWN record for this key, return
			// it verbatim and never re-run the Guardian or the effect. Without a
			// record (consume won but the process died before dispatch) the
			// approval is lost by design: the call falls through to the
			// Guardian, which default-denies a spent approval, so a signature
			// still funds at most ONE effect. A consumed row under any OTHER key
			// is refused by the cases above (one-shot).
			if prior, perr := s.ledger.Get(ctx, scope, key); perr == nil &&
				(prior.Status == operations.StatusSucceeded ||
					prior.Status == operations.StatusSideEffectUnknown ||
					prior.Status == operations.StatusFailed ||
					prior.Status == operations.StatusFenced) {
				return replay(prior, nil)
			}
		default:
			return operations.Operation{}, ErrApprovalRequired
		}
	}

	// Independent Guardian path, backed by the durable PolicySource: evaluated
	// before any ledger mutation and audited whether it allows or denies. A side
	// effect is allowed only by explicit in-process allow-listing (tests), a
	// human approval for this exact digest, or an operator (tenant,connection,
	// tool) grant; everything else stays default-deny.
	dec, gerr := s.cfg.Guardian.Decide(ctx, scope, GuardRequest{
		Tool:          tool,
		Connection:    spec.Connection,
		SideEffecting: spec.SideEffecting,
		Digest:        digest,
		Arguments:     args,
	})
	s.logGuardian(ctx, scope, tool, spec.SideEffecting, digest, dec)
	if gerr != nil {
		return operations.Operation{}, fmt.Errorf("tools: guardian: %w", gerr)
	}
	if !dec.Allow {
		return operations.Operation{}, contracts.NewReasoned(contracts.ErrForbidden, "POLICY_DENIED", "tools: guardian denied: "+dec.Reason)
	}

	// A missing or expired personal credential is an auth-required request, not
	// a dispatched effect. Refresh/label failure must leave the operation key reusable.
	if s.cfg.OAuth != nil && s.cfg.OAuth.has(spec.Connection) {
		if _, err := s.cfg.OAuth.Resolve(ctx, scope, spec.Connection); err != nil {
			if errors.Is(err, ErrAuthRequired) {
				if reqErr := s.cfg.OAuth.Require(ctx, scope, spec.Connection); reqErr != nil {
					return operations.Operation{}, reqErr
				}
			}
			return operations.Operation{}, err
		}
	}
	op, err := s.ledger.Accept(ctx, scope, request)
	if err != nil {
		// A sealed prior operation under this key (SUCCEEDED/FAILED/FENCED) is a
		// replay of the terminal record rather than a fresh dispatch.
		if contracts.Code(err) == contracts.ErrConflict {
			return replay(s.ledger.Get(ctx, scope, key))
		}
		return operations.Operation{}, err
	}
	if op.Status != operations.StatusAccepted {
		// Replay: already accepted earlier and moved on (incl. SIDE_EFFECT_UNKNOWN).
		return replay(op, nil)
	}

	if _, derr := s.ledger.DispatchCheckedWithProvenance(ctx, scope, key, check, prov.marshal(spec.Connection)); derr != nil {
		if errors.Is(derr, operations.ErrAlreadyDispatched) {
			return replay(s.ledger.Get(ctx, scope, key))
		}
		// Ordinary guard conflict: a sibling side effect holds the digest guard.
		return operations.Operation{}, derr
	}
	// Durable one-shot authorisation for human-gated tools: the approval bound
	// to THIS operation is consumed atomically at the point where a fresh
	// dispatch has just been granted (exactly once per key). No row consumed ->
	// no effect, whatever happened after the gate read (race, expiry, replay).
	if spec.RequiresApproval {
		if cerr := s.TryConsumeApproval(ctx, scope, key, digest, tool); cerr != nil {
			return operations.Operation{}, cerr
		}
	}
	return s.dispatch(ctx, scope, key, spec, args, binding)
}

// dispatch performs the MCP call after dispatch was granted. It settles the
// ledger on success, fails it on a clean (non-side-effecting) error, and marks
// SIDE_EFFECT_UNKNOWN when a side-effecting call returns an ambiguous error.
func (s *Service) dispatch(ctx context.Context, scope contracts.Scope, key string, spec ToolSpec, args json.RawMessage, binding artifacts.Binding) (operations.Operation, error) {
	connector, ok := s.cfg.Connections[spec.Connection]
	if !ok {
		return s.fail(ctx, scope, key, contracts.NewError(contracts.ErrNotFound, "tools: connection vanished"))
	}
	cred, err := s.cfg.Credentials.Resolve(ctx, scope, spec.Connection)
	if err != nil {
		return s.fail(ctx, scope, key, fmt.Errorf("tools: resolve credential: %w", err))
	}
	client, err := connector(ctx, cred)
	if err != nil {
		return s.fail(ctx, scope, key, fmt.Errorf("tools: connect: %w", err))
	}
	defer func() { _ = client.Close() }()

	res, cerr := client.CallTool(ctx, &mcp.CallToolParams{Name: spec.Name, Arguments: rawToAny(args)})
	if cerr != nil {
		if spec.SideEffecting {
			// Ambiguous: the effect may or may not have reached the external
			// system. Mark UNKNOWN and never auto-replay.
			if _, merr := s.ledger.MarkUnknown(ctx, scope, key); merr != nil {
				return operations.Operation{}, fmt.Errorf("tools: mark unknown after call error (%v): %w", cerr, merr)
			}
			op, gerr := s.ledger.Get(ctx, scope, key)
			if gerr != nil {
				return operations.Operation{}, gerr
			}
			return op, fmt.Errorf("tools: side-effect outcome unknown: %w", cerr)
		}
		return s.fail(ctx, scope, key, cerr)
	}

	// A tool that ANSWERS with an error result round-tripped fine but refused or
	// failed: journal the tool's own words as a sanitized failure summary so the
	// human surface can say why. The status transition stays the ledger's
	// business (an answered error does not prove an effect was absent, so this
	// must not silently become FAILED for side-effecting tools).
	if res != nil && res.IsError {
		outcome := map[string]any{FailureKey: ToolFailure(ctx, toolErrorExcerpt(res))}
		if binding.ContextID != "" {
			outcome["isError"] = true
		}
		payload, _ := json.Marshal(outcome)
		op, cerr2 := s.ledger.Complete(ctx, scope, key, payload, false)
		if cerr2 != nil {
			return operations.Operation{}, fmt.Errorf("tools: complete: %w", cerr2)
		}
		return op, nil
	}
	payload, merr := json.Marshal(res)
	if merr != nil {
		payload = json.RawMessage(`{"marshal_error":true}`)
	}
	// Service-side ceiling: a connector that did not install the transport
	// bound still cannot hand the gateway an unbounded result. A
	// side-effecting tool whose answer cannot be journalled stays
	// SIDE_EFFECT_UNKNOWN -- the effect happened, and calling it FAILED would
	// contradict the ledger's own meaning.
	if ceiling := s.maxUpstreamBytes(); ceiling > 0 && len(payload) > ceiling {
		return s.rejectUnstoredResult(ctx, scope, key, spec.SideEffecting,
			fmt.Errorf("%w: %d bytes over a %d byte ceiling", ErrUpstreamTooLarge, len(payload), ceiling))
	}
	// Out-of-line storage for large output: the ledger keeps an immutable
	// reference and Artifacts keeps the bytes.
	if ref, spilled, serr := s.storeResult(ctx, scope, binding, key, payload); serr != nil {
		return s.rejectUnstoredResult(ctx, scope, key, spec.SideEffecting,
			fmt.Errorf("tools: spill result: %w", serr))
	} else if spilled {
		payload = ref
	}
	if len(payload) > operations.MaxResultBytes {
		return s.rejectUnstoredResult(ctx, scope, key, spec.SideEffecting,
			fmt.Errorf("tools: result exceeds journal limit of %d bytes", operations.MaxResultBytes))
	}
	op, err := s.ledger.Complete(ctx, scope, key, payload, false)
	if err != nil {
		return operations.Operation{}, fmt.Errorf("tools: complete: %w", err)
	}
	return op, nil
}

// rejectUnstoredResult settles a failure detected before attempting to commit
// a result. A mutation may already have happened, so it keeps its guard.
func (s *Service) rejectUnstoredResult(ctx context.Context, scope contracts.Scope, key string, sideEffecting bool, cause error) (operations.Operation, error) {
	if !sideEffecting {
		return s.fail(ctx, scope, key, cause)
	}
	op, err := s.ledger.MarkUnknown(ctx, scope, key)
	if err != nil {
		return operations.Operation{}, fmt.Errorf("tools: mark unknown after result storage failure (%v): %w", cause, err)
	}
	return op, cause
}

// fail records a deterministic (non-ambiguous) failure and returns the original
// cause so callers see why the tool errored.
func (s *Service) fail(ctx context.Context, scope contracts.Scope, key string, cause error) (operations.Operation, error) {
	// Only the sanitized summary is journalled. The raw cause -- endpoint URLs,
	// credential/Vault paths, driver text -- is returned to the caller for
	// server-side handling and never lands in a client-visible payload, while
	// the human surface still gets a class and an actionable sentence.
	payload, _ := json.Marshal(map[string]any{FailureKey: ClassifyFailure(ctx, cause)})
	op, _ := s.ledger.Complete(ctx, scope, key, payload, true)
	return op, cause
}

// logGuardian writes one audit row per Guardian verdict. Best-effort: a logging
// failure must not change the authorisation outcome.
func (s *Service) logGuardian(ctx context.Context, scope contracts.Scope, tool string, sideEffecting bool, digest string, dec Decision) {
	q := fmt.Sprintf(`INSERT INTO %s.guardian_log
		(tenant, deployment, subject, tool, side_effecting, digest, decision, reason, class)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, s.schema)
	decision := "deny"
	if dec.Allow {
		decision = "allow"
	}
	tx, err := s.tenantWrite(ctx, scope)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, q, scope.Tenant, scope.Deployment, scope.Subject, tool, sideEffecting, digest, decision, dec.Reason, dec.Class); err == nil {
		_ = tx.Commit(ctx)
	}
}

// maxUpstreamBytes resolves the configured ceiling: 0 means the default, a
// negative value means no service-side ceiling (the transport bound still
// applies where it was installed).
func (s *Service) maxUpstreamBytes() int {
	if s.cfg.MaxUpstreamBytes == 0 {
		return DefaultMaxUpstreamBytes
	}
	if s.cfg.MaxUpstreamBytes < 0 {
		return 0
	}
	return s.cfg.MaxUpstreamBytes
}

// Get returns the recorded operation for a key.
func (s *Service) Get(ctx context.Context, scope contracts.Scope, key string) (operations.Operation, error) {
	return s.ledger.Get(ctx, scope, key)
}

// Reconcile moves a SIDE_EFFECT_UNKNOWN operation to a terminal state once an
// operator has established the real outcome.
func (s *Service) Reconcile(ctx context.Context, scope contracts.Scope, key string, result json.RawMessage, failed bool) (operations.Operation, error) {
	return s.ledger.Reconcile(ctx, scope, key, result, failed)
}

// Fence forbids any further effect for an operation.
func (s *Service) Fence(ctx context.Context, scope contracts.Scope, key string) (operations.Operation, error) {
	return s.ledger.Fence(ctx, scope, key)
}

// Connections lists the configured MCP connection names, sorted. It returns
// only identifiers: neither the connection endpoints nor any credential are
// exposed, so the result is safe to serve on the observability face.
func (s *Service) Connections() []string {
	names := make([]string, 0, len(s.cfg.Connections))
	for name := range s.cfg.Connections {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Drop removes every table this plane owns and then its schema, in one cascade.
// The schema name is validated at construction (schemaRe), so the DROP can only
// ever target this plane's own ahp_ tools schema and never a neighbouring
// plane's tables -- the rollback the storage contract requires. It is
// idempotent (DROP ... IF EXISTS) and intended for teardown and tests.
func (s *Service) Drop(ctx context.Context) error {
	if !schemaRe.MatchString(s.schema) {
		return contracts.NewError(contracts.ErrInvalid, "tools: schema name must match ^ahp_[a-z][a-z0-9_]{0,40}$")
	}
	if _, err := s.pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+s.schema+` CASCADE`); err != nil {
		return fmt.Errorf("tools: drop schema: %w", err)
	}
	return nil
}

// rawToAny decodes raw JSON into a generic value for the MCP Arguments field,
// passing nil through for empty payloads.
func rawToAny(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return v
}

// GrantOperatorPolicy records a durable, audited operator standing grant for
// (tenant, connection, tool). A grant lets a side-effecting tool run WITHOUT a
// per-call human approval, but it never weakens the Guardian: with no grant, no
// approval and no in-process allow-listing the tool is still default-denied.
// This is operator-plane tooling -- it is intentionally NOT a task-facing HTTP
// route -- so a task bearer can never widen its own side-effect authority.
func (s *Service) GrantOperatorPolicy(ctx context.Context, scope contracts.Scope, connection, tool, actor string) error {
	if s.policy == nil {
		return contracts.NewError(contracts.ErrInvalid, "tools: policy source not configured")
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	return s.policy.GrantOperatorPolicy(ctx, scope, connection, tool, actor)
}

// RevokeOperatorPolicy removes a standing operator grant (audited).
func (s *Service) RevokeOperatorPolicy(ctx context.Context, scope contracts.Scope, connection, tool, actor string) error {
	if s.policy == nil {
		return contracts.NewError(contracts.ErrInvalid, "tools: policy source not configured")
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	return s.policy.RevokeOperatorPolicy(ctx, scope, connection, tool, actor)
}

// toolErrorExcerpt pulls the text a tool reported with an error result.
func toolErrorExcerpt(res *mcp.CallToolResult) string {
	if res == nil {
		return ""
	}
	var sb strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			if sb.Len() > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(tc.Text)
		}
	}
	return sb.String()
}
