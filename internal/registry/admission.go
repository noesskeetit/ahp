// Deploy admission: the Registry-side enforcement of the CI supply chain.
//
// Once an operator installs trust roots (WithSupplyChain), an immutable OCI
// image may be installed or promoted only when its signed provenance verifies:
// a publisher signature chaining to an operator root, an SBOM that hashes to
// what the signature bound, a current passing evaluation verdict, and -- for
// stage and prod -- a current passing security ("проверка ИБ") verdict. Every
// check re-derives its authority from stored facts; none of them trusts a
// caller-supplied "ok" flag, and a store without trust roots never pretends to
// enforce a supply chain it was not given one for.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// hexPattern matches a bare 64-char lowercase sha256 without a scheme prefix.
var hexPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// supplyChainDDL is created alongside the base schema. Both tables record facts
// the Registry owns for admission; neither is ever read as product state.
const supplyChainDDL = `
CREATE TABLE IF NOT EXISTS ahp_registry.security_verdicts (
    id          text PRIMARY KEY,
    oci_digest  text NOT NULL,
    agent_id    text NOT NULL,
    outcome     text NOT NULL,
    source      text NOT NULL DEFAULT '',
    checked_at  timestamptz NOT NULL,
    expires_at  timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT security_verdicts_digest_format CHECK (oci_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT security_verdicts_outcome_check CHECK (outcome IN ('pass', 'fail'))
);
CREATE INDEX IF NOT EXISTS security_verdicts_digest_idx
    ON ahp_registry.security_verdicts (oci_digest, checked_at DESC, id DESC);

-- chaos_verdicts is the stage restart/availability plane: "stage с оценками на
-- эталонных задачах и chaos-тестом рестарта пода, проверка ИБ и prod" (PDF rev
-- 3.2, c. 41). Like the security clearance it is keyed to one immutable image
-- digest, is written by a platform verification authority rather than the
-- publisher, and expires, so a restart test that stopped passing blocks the
-- rollout instead of merely having once happened.
CREATE TABLE IF NOT EXISTS ahp_registry.chaos_verdicts (
    id          text PRIMARY KEY,
    oci_digest  text NOT NULL,
    agent_id    text NOT NULL,
    outcome     text NOT NULL,
    source      text NOT NULL DEFAULT '',
    checked_at  timestamptz NOT NULL,
    expires_at  timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT chaos_verdicts_digest_format CHECK (oci_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT chaos_verdicts_outcome_check CHECK (outcome IN ('pass', 'fail'))
);
CREATE INDEX IF NOT EXISTS chaos_verdicts_digest_idx
    ON ahp_registry.chaos_verdicts (oci_digest, checked_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS ahp_registry.admissions (
    oci_digest          text PRIMARY KEY,
    agent_id            text NOT NULL,
    publisher_id        text NOT NULL,
    version_digest      text NOT NULL,
    sbom_digest         text NOT NULL,
    eval_verdict_digest text NOT NULL,
    security_verdict_id text NOT NULL,
    expires_at          timestamptz NOT NULL,
    attestation         jsonb NOT NULL,
    verified_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT admissions_digest_format CHECK (oci_digest ~ '^sha256:[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS admissions_agent_idx
    ON ahp_registry.admissions (agent_id, version_digest);
`

// SecurityVerdict is the security/"проверка ИБ" clearance an independent review
// grants for one immutable OCI image. It is written by an operator/security
// authority (never by the artifact publisher or the invoking task) and is what
// the stage/prod admission gate reads. Like an evaluation verdict it is keyed to
// a content digest, so a clearance for one image can never clear another, and it
// carries an expiry so a stale clearance stops unlocking deployment.
type SecurityVerdict struct {
	ID        string    `json:"id"`
	OCIDigest string    `json:"oci_digest"`
	AgentID   string    `json:"agent_id"`
	Outcome   string    `json:"outcome"`
	Source    string    `json:"source,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Admission is the persisted result of verifying one image's supply chain: the
// publisher, digests and verdict references the signature actually bound.
type Admission struct {
	OCIDigest         string    `json:"oci_digest"`
	AgentID           string    `json:"agent_id"`
	PublisherID       string    `json:"publisher_id"`
	VersionDigest     string    `json:"version_digest"`
	SBOMDigest        string    `json:"sbom_digest"`
	EvalVerdictDigest string    `json:"eval_verdict_digest"`
	SecurityVerdictID string    `json:"security_verdict_id"`
	ExpiresAt         time.Time `json:"expires_at"`
	VerifiedAt        time.Time `json:"verified_at"`
}

// WithSupplyChain turns on deploy admission for a Store. Passing nil leaves the
// Store unenforced; enforcement is an operator decision expressed by the roots
// the operator installs, not something a request can toggle.
func WithSupplyChain(roots *TrustRoots) Option {
	return func(s *Store) { s.supply = roots }
}

// supplyChainEnforced reports whether this Store enforces deploy admission.
func (s *Store) supplyChainEnforced() bool { return s != nil && s.supply != nil }

// ReasonVerdictCheckedInFuture is the stable machine reason carried by every
// refusal of a security or restart-chaos verdict whose checked_at leads the
// registry clock by more than verdictClockSkewAllowance. It is a reason, not a
// new wire code: the code stays INVALID_ARGUMENT on the write side and
// CONFLICT on the read side, so clients that only map codes keep working and
// only a client that asks for contracts.ReasonOf learns the specific cause.
const ReasonVerdictCheckedInFuture = "registry_verdict_checked_in_future"

// verdictClockSkewAllowance is the largest forward difference between the clock
// that stamped a verdict and the registry clock that judges it which admission
// still treats as "now". A verdict stamped in the future is not freshness, it is
// an authority whose clock is wrong, and counting it as current would let a host
// with advanced clocks mint a clearance that is instantly "fresh" for as long as
// it likes -- the freshness bound stops meaning anything.
//
// The number is 60s because that is the ceiling the ecosystem already uses for
// exactly this judgement: RFC 7519 (JWT) recommends at most a minute of leeway on
// time claims, and RFC 9700 (OAuth security BCP) repeats it for clock skew. It is
// three orders of magnitude above NTP-synchronised drift (tens of ms), and it
// stays far below the shortest freshness window this gate enforces in practice
// (the one-hour authority/attestation windows used here and in the fixtures), so
// it absorbs realistic skew without eating a measurable slice of any TTL.
// Anything larger is no longer skew compensation -- it is two different clocks.
const verdictClockSkewAllowance = 60 * time.Second

// rejectFutureVerdict reports whether a verdict stamped at checkedAt is too far
// ahead of now to be believed. It is called on both sides of the plane: when a
// verdict is recorded (so a broken or hostile authority cannot store a future
// clearance at all) and when the gate reads one (so rows written before this
// check existed, or by a writer with a different clock, are still refused).
func rejectFutureVerdict(now time.Time, what string, checkedAt time.Time, code string) error {
	skew := checkedAt.UTC().Sub(now.UTC())
	if skew <= verdictClockSkewAllowance {
		return nil
	}
	return contracts.NewReasoned(code, ReasonVerdictCheckedInFuture, fmt.Sprintf(
		"registry: %s checked_at is %s ahead of the registry clock, beyond the %s allowed skew",
		what, skew.Round(time.Millisecond), verdictClockSkewAllowance))
}

// now is the single clock the admission path uses. When trust roots are
// configured their clock is reused so certificate, attestation and verdict
// expiry are all judged against one consistent time.
func (s *Store) now() time.Time {
	if s.supply != nil {
		return s.supply.now()
	}
	return time.Now().UTC()
}

// AdmitImageInput carries the provenance the Registry must verify before an image
// may be deployed. The image and version are cross-checked against the stored
// manifest; only the attestation and SBOM are external inputs.
type AdmitImageInput struct {
	AgentID     string
	VersionID   string
	Image       string
	Attestation Attestation
	SBOM        []byte
}

// AdmitImage verifies an image's signed provenance and records the admission.
// It fails closed when supply-chain enforcement is not configured, when the
// signature/SBOM/digest/publisher do not all line up, when the evaluation verdict
// is not currently passing, or when no current passing security verdict exists
// for the exact image digest.
func (s *Store) AdmitImage(ctx context.Context, in AdmitImageInput) (Admission, error) {
	if !s.supplyChainEnforced() {
		return Admission{}, contracts.NewError(contracts.ErrUnavailable,
			"deploy admission: no supply-chain trust roots configured (fail closed)")
	}
	if err := requireID("agent id", in.AgentID); err != nil {
		return Admission{}, err
	}
	version, err := s.GetVersion(ctx, in.VersionID)
	if err != nil {
		return Admission{}, err
	}
	if version.AgentID != in.AgentID {
		return Admission{}, contracts.NewError(contracts.ErrConflict, "version belongs to a different agent")
	}
	oci := ociDigestOfImage(in.Image)
	if oci == "" {
		return Admission{}, invalid("deploy admission: image must be pinned to an immutable sha256 digest")
	}
	if !versionDeclaresImage(version.Manifest, in.Image) {
		return Admission{}, contracts.NewError(contracts.ErrConflict, "image is not declared by the version manifest")
	}
	publisher, err := s.publisherOf(ctx, nil, in.AgentID)
	if err != nil {
		return Admission{}, err
	}

	facts, err := s.supply.Verify(in.Attestation, in.SBOM, oci)
	if err != nil {
		return Admission{}, supplyRejected(err)
	}
	if facts.Publisher != publisher {
		return Admission{}, contracts.NewError(contracts.ErrForbidden,
			"deploy admission: attestation publisher does not own this agent")
	}
	if facts.VersionDigest != version.Digest {
		return Admission{}, contracts.NewError(contracts.ErrConflict,
			"deploy admission: attestation binds a different version digest")
	}
	// The evaluation plane must still show a passing verdict for this version.
	if err := s.requireEvalVerdict(ctx, version.Digest); err != nil {
		return Admission{}, err
	}
	// The provenance may reference a security ("проверка ИБ") record; if that
	// record already exists it must have been issued for exactly this image, so a
	// publisher cannot borrow another image's clearance. Requiring it to EXIST
	// here would make the dev/draft step depend on a clearance the pipeline only
	// issues after stage, i.e. a publication cycle. Existence, current passing
	// outcome and the binding to this admission are enforced where they matter: at
	// stage/prod deploy time by requireDeployAdmission.
	if issued, ok, err := s.securityVerdictByID(ctx, facts.SecurityVerdictID); err != nil {
		return Admission{}, err
	} else if ok && normalizeDigest(issued.OCIDigest) != oci {
		return Admission{}, contracts.NewError(contracts.ErrConflict,
			"deploy admission: attestation references a security verdict issued for a different image")
	}

	return s.recordAdmission(ctx, in.AgentID, facts)
}

// RecordSecurityVerdict persists a security clearance for one image. It is the
// write side of the stage/prod gate and belongs to the security authority, not
// to the artifact publisher or a task.
func (s *Store) RecordSecurityVerdict(ctx context.Context, v SecurityVerdict) (SecurityVerdict, error) {
	v.OCIDigest = normalizeDigest(v.OCIDigest)
	if !digestPattern.MatchString(v.OCIDigest) {
		return SecurityVerdict{}, invalid("security verdict: oci digest must be sha256:<64 hex>")
	}
	if err := requireID("agent id", v.AgentID); err != nil {
		return SecurityVerdict{}, err
	}
	if v.Outcome != "pass" && v.Outcome != "fail" {
		return SecurityVerdict{}, invalid("security verdict: outcome must be pass or fail")
	}
	if v.CheckedAt.IsZero() {
		v.CheckedAt = s.now().UTC()
	}
	if err := rejectFutureVerdict(s.now(), "security verdict", v.CheckedAt, contracts.ErrInvalid); err != nil {
		return SecurityVerdict{}, err
	}
	if !v.ExpiresAt.After(v.CheckedAt) {
		return SecurityVerdict{}, invalid("security verdict: expires_at must be after checked_at")
	}
	if v.ID == "" {
		v.ID = contracts.NewID()
	} else if err := requireID("verdict id", v.ID); err != nil {
		return SecurityVerdict{}, err
	}
	v.CheckedAt = v.CheckedAt.UTC()
	v.ExpiresAt = v.ExpiresAt.UTC()
	_, err := s.pool.Exec(ctx, `
INSERT INTO ahp_registry.security_verdicts (id, oci_digest, agent_id, outcome, source, checked_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		v.ID, v.OCIDigest, v.AgentID, v.Outcome, v.Source, v.CheckedAt, v.ExpiresAt)
	if err != nil {
		return SecurityVerdict{}, storageError("record security verdict", err)
	}
	return v, nil
}

// currentSecurityVerdict returns the most recently checked verdict for a digest.
func (s *Store) currentSecurityVerdict(ctx context.Context, oci string) (SecurityVerdict, bool, error) {
	oci = normalizeDigest(oci)
	var (
		v   SecurityVerdict
		src *string
	)
	err := s.pool.QueryRow(ctx, `
SELECT id, oci_digest, agent_id, outcome, source, checked_at, expires_at
FROM ahp_registry.security_verdicts WHERE oci_digest = $1
ORDER BY checked_at DESC, id DESC LIMIT 1`, oci).
		Scan(&v.ID, &v.OCIDigest, &v.AgentID, &v.Outcome, &src, &v.CheckedAt, &v.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return SecurityVerdict{}, false, nil
	}
	if err != nil {
		return SecurityVerdict{}, false, storageError("load security verdict", err)
	}
	if src != nil {
		v.Source = *src
	}
	v.CheckedAt = v.CheckedAt.UTC()
	v.ExpiresAt = v.ExpiresAt.UTC()
	return v, true, nil
}

// securityVerdictByID loads the clearance carrying this id, if the security
// authority issued one at all.
func (s *Store) securityVerdictByID(ctx context.Context, id string) (SecurityVerdict, bool, error) {
	var (
		v   SecurityVerdict
		src *string
	)
	err := s.pool.QueryRow(ctx, `
SELECT id, oci_digest, agent_id, outcome, source, checked_at, expires_at
FROM ahp_registry.security_verdicts WHERE id = $1`, id).
		Scan(&v.ID, &v.OCIDigest, &v.AgentID, &v.Outcome, &src, &v.CheckedAt, &v.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return SecurityVerdict{}, false, nil
	}
	if err != nil {
		return SecurityVerdict{}, false, storageError("load security verdict", err)
	}
	if src != nil {
		v.Source = *src
	}
	return v, true, nil
}

// ChaosVerdict is the stage restart/availability clearance for one immutable
// image: the recorded outcome of the manifest's "chaos: рестарт пода" stage
// check. It is written by the platform verification authority, never by the
// artifact publisher or a task, and it is keyed to the image digest, so a pass
// for one image can never clear another.
type ChaosVerdict struct {
	ID        string    `json:"id"`
	OCIDigest string    `json:"oci_digest"`
	AgentID   string    `json:"agent_id"`
	Outcome   string    `json:"outcome"`
	Source    string    `json:"source,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// RecordChaosVerdict persists a restart-chaos clearance for one image. Together
// with the security clearance it forms the stage/prod plane that
// requireDeployAdmission reads.
func (s *Store) RecordChaosVerdict(ctx context.Context, v ChaosVerdict) (ChaosVerdict, error) {
	v.OCIDigest = normalizeDigest(v.OCIDigest)
	if !digestPattern.MatchString(v.OCIDigest) {
		return ChaosVerdict{}, invalid("chaos verdict: oci digest must be sha256:<64 hex>")
	}
	if err := requireID("agent id", v.AgentID); err != nil {
		return ChaosVerdict{}, err
	}
	if v.Outcome != "pass" && v.Outcome != "fail" {
		return ChaosVerdict{}, invalid("chaos verdict: outcome must be pass or fail")
	}
	if v.CheckedAt.IsZero() {
		v.CheckedAt = s.now().UTC()
	}
	if err := rejectFutureVerdict(s.now(), "chaos verdict", v.CheckedAt, contracts.ErrInvalid); err != nil {
		return ChaosVerdict{}, err
	}
	if !v.ExpiresAt.After(v.CheckedAt) {
		return ChaosVerdict{}, invalid("chaos verdict: expires_at must be after checked_at")
	}
	if v.ID == "" {
		v.ID = contracts.NewID()
	} else if err := requireID("verdict id", v.ID); err != nil {
		return ChaosVerdict{}, err
	}
	v.CheckedAt = v.CheckedAt.UTC()
	v.ExpiresAt = v.ExpiresAt.UTC()
	_, err := s.pool.Exec(ctx, `
INSERT INTO ahp_registry.chaos_verdicts (id, oci_digest, agent_id, outcome, source, checked_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		v.ID, v.OCIDigest, v.AgentID, v.Outcome, v.Source, v.CheckedAt, v.ExpiresAt)
	if err != nil {
		return ChaosVerdict{}, storageError("record chaos verdict", err)
	}
	return v, nil
}

// currentChaosVerdict returns the most recently checked restart-chaos verdict
// for one image digest.
func (s *Store) currentChaosVerdict(ctx context.Context, oci string) (ChaosVerdict, bool, error) {
	oci = normalizeDigest(oci)
	var (
		v   ChaosVerdict
		src *string
	)
	err := s.pool.QueryRow(ctx, `
SELECT id, oci_digest, agent_id, outcome, source, checked_at, expires_at
FROM ahp_registry.chaos_verdicts WHERE oci_digest = $1
ORDER BY checked_at DESC, id DESC LIMIT 1`, oci).
		Scan(&v.ID, &v.OCIDigest, &v.AgentID, &v.Outcome, &src, &v.CheckedAt, &v.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ChaosVerdict{}, false, nil
	}
	if err != nil {
		return ChaosVerdict{}, false, storageError("load chaos verdict", err)
	}
	if src != nil {
		v.Source = *src
	}
	v.CheckedAt = v.CheckedAt.UTC()
	v.ExpiresAt = v.ExpiresAt.UTC()
	return v, true, nil
}

func (s *Store) recordAdmission(ctx context.Context, agentID string, f *AttestedFacts) (Admission, error) {
	// Persist a bounded, secret-free projection of the verified statement.
	binding, err := json.Marshal(map[string]any{
		"verification_contract": supplyChainVerificationContract,
		"publisher":             f.Publisher,
		"oci_digest":            f.OCIDigest,
		"version_digest":        f.VersionDigest,
		"sbom_digest":           f.SBOMDigest,
		"eval_verdict_digest":   f.EvalVerdictDigest,
		"security_verdict_id":   f.SecurityVerdictID,
		"expires_at":            f.ExpiresAt,
	})
	if err != nil {
		return Admission{}, err
	}
	var verifiedAt time.Time
	err = s.pool.QueryRow(ctx, `
INSERT INTO ahp_registry.admissions
    (oci_digest, agent_id, publisher_id, version_digest, sbom_digest, eval_verdict_digest, security_verdict_id, expires_at, attestation)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
ON CONFLICT (oci_digest) DO UPDATE SET
    agent_id = EXCLUDED.agent_id, publisher_id = EXCLUDED.publisher_id,
    version_digest = EXCLUDED.version_digest, sbom_digest = EXCLUDED.sbom_digest,
    eval_verdict_digest = EXCLUDED.eval_verdict_digest,
    security_verdict_id = EXCLUDED.security_verdict_id,
    expires_at = EXCLUDED.expires_at, attestation = EXCLUDED.attestation,
    verified_at = now()
RETURNING verified_at`,
		f.OCIDigest, agentID, f.Publisher, f.VersionDigest, f.SBOMDigest, f.EvalVerdictDigest, f.SecurityVerdictID, f.ExpiresAt, binding).Scan(&verifiedAt)
	if err != nil {
		return Admission{}, storageError("record admission", err)
	}
	return Admission{
		OCIDigest: f.OCIDigest, AgentID: agentID, PublisherID: f.Publisher, VersionDigest: f.VersionDigest,
		SBOMDigest: f.SBOMDigest, EvalVerdictDigest: f.EvalVerdictDigest, SecurityVerdictID: f.SecurityVerdictID,
		ExpiresAt: f.ExpiresAt, VerifiedAt: verifiedAt.UTC(),
	}, nil
}

// currentAdmission loads the persisted admission for one image digest.
func (s *Store) currentAdmission(ctx context.Context, querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, oci string) (Admission, bool, error) {
	oci = normalizeDigest(oci)
	var a Admission
	err := querier.QueryRow(ctx, `
SELECT oci_digest, agent_id, publisher_id, version_digest, sbom_digest, eval_verdict_digest, security_verdict_id, expires_at, verified_at
FROM ahp_registry.admissions WHERE oci_digest = $1
  AND attestation->>'verification_contract' = $2`, oci, supplyChainVerificationContract).
		Scan(&a.OCIDigest, &a.AgentID, &a.PublisherID, &a.VersionDigest, &a.SBOMDigest, &a.EvalVerdictDigest, &a.SecurityVerdictID, &a.ExpiresAt, &a.VerifiedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Admission{}, false, nil
	}
	if err != nil {
		return Admission{}, false, storageError("load admission", err)
	}
	a.ExpiresAt = a.ExpiresAt.UTC()
	a.VerifiedAt = a.VerifiedAt.UTC()
	return a, true, nil
}

// requireDeployAdmission enforces the supply chain inside a deployment
// transaction. It is a no-op unless enforcement is on and the version actually
// carries an immutable OCI image; otherwise a manifest-only version is deployed
// exactly as before. For an image-bearing version it requires a current admission
// bound to this version's digest and a still-passing evaluation verdict, which is
// everything the CI pipeline produces before dev.
//
// Stage and prod add the two planes that only exist after a rollout has been
// exercised there, and every one of them must currently pass for this exact
// image: the security clearance ("проверка ИБ") the publisher's signature binds,
// and the stage restart-chaos verdict ("chaos: рестарт пода"). A failing or
// expired plane, or a signature that names a clearance other than the current
// passing one, makes the rollout impossible; there is no way to opt out of a
// plane from the request side.
func (s *Store) requireDeployAdmission(ctx context.Context, tx pgx.Tx, version Version, environment string) error {
	if !s.supplyChainEnforced() {
		return nil
	}
	images := versionOCIImages(version.Manifest)
	if len(images) == 0 {
		return nil
	}
	now := s.now()
	admitted := make(map[string]Admission, len(images))
	for _, oci := range images {
		adm, ok, err := s.currentAdmission(ctx, tx, oci)
		if err != nil {
			return err
		}
		if !ok {
			return contracts.NewError(contracts.ErrConflict,
				"deploy admission: no verified supply-chain record for image "+shortDigest(oci))
		}
		if adm.VersionDigest != version.Digest {
			return contracts.NewError(contracts.ErrConflict,
				"deploy admission: image record belongs to a different version digest")
		}
		if now.After(adm.ExpiresAt) {
			return contracts.NewError(contracts.ErrConflict,
				"deploy admission: supply-chain record for image "+shortDigest(oci)+" has expired")
		}
		admitted[oci] = adm
	}
	// The evaluation verdict must still pass for this exact version digest; a
	// later failing or superseded verdict blocks promotion ("устаревший eval").
	if err := s.requireEvalVerdict(ctx, version.Digest); err != nil {
		return err
	}
	if environment == EnvironmentStage || environment == EnvironmentProd {
		for _, oci := range images {
			sec, ok, err := s.currentSecurityVerdict(ctx, oci)
			if err != nil {
				return err
			}
			// A clearance stamped in the future is an authority with a broken
			// clock, not a fresh one: refuse it before the freshness comparison,
			// which a future checked_at would otherwise always survive.
			if ok {
				if err := rejectFutureVerdict(now, "security verdict for image "+shortDigest(oci),
					sec.CheckedAt, contracts.ErrConflict); err != nil {
					return err
				}
			}
			if !ok || sec.Outcome != "pass" || now.After(sec.ExpiresAt) {
				return contracts.NewError(contracts.ErrConflict,
					"deploy admission: security verdict for image "+shortDigest(oci)+" is failing or expired")
			}
			// The clearance must be the one the verified signature binds, so a
			// publisher cannot deploy under a clearance it never attested to, and
			// an operator cannot swap a clearance under an existing admission.
			if id := admitted[oci].SecurityVerdictID; id == "" || id != sec.ID {
				return contracts.NewError(contracts.ErrConflict,
					"deploy admission: verified signature for image "+shortDigest(oci)+
						" does not bind the current passing security clearance")
			}
			chaos, ok, err := s.currentChaosVerdict(ctx, oci)
			if err != nil {
				return err
			}
			if !ok {
				return contracts.NewError(contracts.ErrConflict,
					"deploy admission: image "+shortDigest(oci)+" has no stage restart-chaos verdict")
			}
			if err := rejectFutureVerdict(now, "stage restart-chaos verdict for image "+shortDigest(oci),
				chaos.CheckedAt, contracts.ErrConflict); err != nil {
				return err
			}
			if chaos.Outcome != "pass" {
				return contracts.NewError(contracts.ErrConflict,
					"deploy admission: stage restart-chaos verdict for image "+shortDigest(oci)+" failed")
			}
			if now.After(chaos.ExpiresAt) {
				return contracts.NewError(contracts.ErrConflict,
					"deploy admission: stage restart-chaos verdict for image "+shortDigest(oci)+" has expired")
			}
		}
	}
	return nil
}

// versionOCIImages returns the set of immutable image digests a manifest pins.
func versionOCIImages(m Manifest) []string {
	var out []string
	seen := map[string]bool{}
	for _, c := range m.Components {
		if d := ociDigestOfImage(c.Image); d != "" && !seen[d] {
			seen[d] = true
			out = append(out, d)
		}
	}
	return out
}

// versionDeclaresImage reports whether the manifest pins this exact image ref.
func versionDeclaresImage(m Manifest, image string) bool {
	for _, c := range m.Components {
		if c.Image == image {
			return true
		}
	}
	return false
}

func normalizeDigest(d string) string {
	d = strings.ToLower(d)
	if digestPattern.MatchString(d) {
		return d
	}
	if i := strings.Index(d, "@sha256:"); i >= 0 {
		return d[i+1:]
	}
	if hexPattern.MatchString(d) {
		return "sha256:" + d
	}
	return d
}

func shortDigest(d string) string {
	if len(d) > 18 {
		return d[:18]
	}
	return d
}

// supplyRejected maps a verification failure onto CONFLICT so the HTTP layer
// reports a supply-chain refusal as a policy rejection, not a server fault.
func supplyRejected(err error) error {
	if err == nil {
		return nil
	}
	return contracts.NewError(contracts.ErrConflict, err.Error())
}
