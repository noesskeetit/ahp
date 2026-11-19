// Package gateway: accepting a delegated child whose parent lives in another
// deployment.

package gateway

import (
	"context"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Parent attestation markers. Both keys are gateway-set: a caller that sends
// them is ignored, exactly like the tree keys, because "who my parent was, and
// whether I could see the parent's own plane" is a fact the accepting plane
// establishes, not a claim it accepts.
const (
	// MetaParentAttested records that the declared parent was resolved through
	// the presenting credential instead of a local task row.
	MetaParentAttested = "ahp.parent_attested"
	// MetaParentAttestedDeployment names the deployment the attested parent
	// belongs to, so the lineage can be followed out of band.
	MetaParentAttestedDeployment = "ahp.parent_attested_deployment"
	// ParentAttestedExternal is the only value MetaParentAttested takes today.
	ParentAttestedExternal = "external"
	// MetaAcceptedByDeployment names the deployment that ACCEPTED the task. It is
	// not the same fact as the task's scope column: a task is filed under the
	// credential that presented it, and on a delegation hop that credential belongs
	// to the DELEGATING plane. Without this attribute a child accepted by a second
	// plane carries the parent's name in every durable field and the accepting
	// plane's name in none of them, so "B did it" is unanswerable by query
	// (DEF-J4-DEPLOY-ATTRIBUTION, PDF rev3.2 board row 1: parent, subject and
	// deployment of the child must be in the durable journal). Gateway-set: a caller
	// that sends it is ignored, exactly like the parent-attestation keys.
	MetaAcceptedByDeployment = "ahp.accepted_by_deployment"
	// AcceptedByUnconfigured is what the journal records when this plane was never
	// told which deployment it serves. The gap is written instead of being left
	// blank on purpose: to a query, "no attribute" and "the parent's deployment" are
	// indistinguishable, and reading the parent's name where the accepting plane
	// should be is exactly the misattribution this attribute exists to prevent.
	AcceptedByUnconfigured = "unconfigured"
)

// Attestation refusal reasons. Every one of them is written to the operator
// journal beside the refusal the caller receives, because "the parent is not in
// my table" is not a diagnosis an operator can act on -- the whole DEF-H10-8
// blocker stayed unfound for two legs for exactly that reason.
const (
	ReasonAttestationMissing         = "parent_attestation_missing"
	ReasonAttestationUnverifiable    = "parent_attestation_unverifiable"
	ReasonAttestationNotTask         = "parent_attestation_not_task"
	ReasonAttestationTaskMismatch    = "parent_attestation_task_mismatch"
	ReasonAttestationTenantMismatch  = "parent_attestation_tenant_mismatch"
	ReasonAttestationSubjectMismatch = "parent_attestation_subject_mismatch"
	ReasonAttestationGrantMissing    = "parent_attestation_grant_missing"
)

// attestedParent is the lineage of a parent this plane cannot read from its own
// rows, taken from the credential that presents itself with the request.
type attestedParent struct {
	Tenant     string
	Deployment string
	Subject    string
	TaskID     string
	RootID     string
	Depth      int
}

// resolveParentChain audits a declared parent and returns the tree root and the
// depth the new child occupies.
//
// A parent this plane holds is audited against its rows -- unchanged, including
// the cycle and depth guards, which stay in force because the audit must never
// be able to spin. A parent it does NOT hold used to be an outright refusal,
// which made cross-deployment delegation impossible: the delegating run's task
// lives in the delegating plane's database, so "not found" was the only answer
// ResolveParentChainTx could give, and the auditable tree stopped at the
// deployment boundary (PDF rev3.2 :975, :1064 -- B accepts A's child under the
// same subject, with the remaining reserve travelling with the task).
//
// Such a parent is now resolved through the credential that is presenting
// itself with this very request, verified the same way the platform verifies
// every task credential (published Identity keys, task kind, workload binding,
// expiry): its signed claims are the lineage. Nothing from the request body is
// trusted for it, and no new cryptography is introduced. If no credential
// attests this parent, the caller gets exactly the refusal it got before.
func (s *Service) resolveParentChain(ctx context.Context, tx pgx.Tx, scope contracts.Scope, caller, parentID string, budget stepBudget) (string, int, *attestedParent, error) {
	root, depth, err := s.store.ResolveParentChainTx(ctx, tx, scope, parentID)
	if err == nil {
		return root, depth, nil, nil
	}
	if contracts.ReasonOf(err) != ReasonParentUnavailable {
		// A cycle or a depth breach is a decision, not a gap in visibility:
		// attestation must not walk around either guard.
		return "", 0, nil, err
	}
	att, aerr := s.attestParent(ctx, scope, parentID)
	if aerr != nil {
		s.refuse(ctx, MethodSendMessage, "parent_attestation",
			refusalIDs{scope: scope, caller: caller, taskID: parentID}, aerr)
		// The caller keeps the refusal this revision has always returned; the
		// journal says why the attestation did not lift it.
		return "", 0, nil, err
	}
	// The reserve must travel with the task (PDF rev3.2 :975, :2452). Without a
	// stated budget the child would run under THIS deployment's default, which is
	// a bound nobody granted: the parent's remaining reserve is the only honest
	// ceiling for a child the accepting plane cannot audit locally. Refusing is
	// the fail-closed answer; clamping or defaulting would be silent over-spend.
	if budget.Source != BudgetSourceCaller {
		// Returned unsaid: the protocol face journals the refusal it is about to
		// hand back, with this same reason on it. Writing it here too would put
		// two identical lines in the journal for one refusal.
		return "", 0, nil, contracts.NewReasoned(contracts.ErrInvalid, ReasonAttestationGrantMissing,
			"gateway: a child accepted on an attested parent must carry the step reserve that travels with it")
	}
	// Depth comes from the attested parent's own signed depth claim, so the
	// lineage keeps the number the delegating plane's Identity grant carried
	// rather than a locally invented one; the child sits one below it. The guard
	// is the same MaxTaskTreeDepth the local audit enforces.
	if att.Depth < 0 || att.Depth+1 > MaxTaskTreeDepth {
		return "", 0, nil, contracts.NewReasoned(contracts.ErrInvalid, ReasonTreeDepth,
			"gateway: task tree exceeds depth guard")
	}
	return att.RootID, att.Depth + 1, &att, nil
}

// attestParent reads the lineage of parentID out of the credential presenting
// itself with this request. It never infers anything from the request body.
func (s *Service) attestParent(ctx context.Context, scope contracts.Scope, parentID string) (attestedParent, error) {
	claims, err := s.presentedTaskClaims(ctx)
	if err != nil {
		return attestedParent{}, err
	}
	if claims.Kind != auth.KindTask {
		return attestedParent{}, attestationError(ReasonAttestationNotTask,
			"gateway: the presenting credential is not a task credential, so it attests no parent")
	}
	if claims.Task == "" || claims.Root == "" {
		return attestedParent{}, attestationError(ReasonAttestationUnverifiable,
			"gateway: the presenting task credential is not bound to a task and a root")
	}
	if claims.Task != parentID {
		return attestedParent{}, attestationError(ReasonAttestationTaskMismatch,
			"gateway: the presenting credential attests a different task than the declared parent")
	}
	if claims.Tenant != scope.Tenant {
		return attestedParent{}, attestationError(ReasonAttestationTenantMismatch,
			"gateway: the presenting credential belongs to another tenant")
	}
	if claims.Subject == "" || claims.Subject != scope.Subject {
		return attestedParent{}, attestationError(ReasonAttestationSubjectMismatch,
			"gateway: the presenting credential belongs to another subject")
	}
	return attestedParent{
		Tenant:     claims.Tenant,
		Deployment: claims.Deployment,
		Subject:    claims.Subject,
		TaskID:     claims.Task,
		RootID:     claims.Root,
		Depth:      claims.Depth,
	}, nil
}

// presentedTaskClaims obtains the claims of the credential presenting itself.
//
// When this plane was given a verifier and an audience it verifies the bearer
// itself, at the acceptance point, with the same verifier the platform publishes
// and the same task-kind and workload constraints every other face applies -- so
// the lineage of a delegated child never rests on a header or on a mount that
// happens to be permissive. Where no verifier is mounted, the plane falls back to
// the principal its ingress middleware installed, which that middleware produced
// by verifying the same credential; the task-kind requirement below still holds.
func (s *Service) presentedTaskClaims(ctx context.Context) (auth.Claims, error) {
	if s.cfg.TaskVerifier != nil && s.cfg.TaskAudience != "" {
		token := bearerFromContext(ctx)
		if token == "" {
			return auth.Claims{}, attestationError(ReasonAttestationMissing,
				"gateway: no task credential was presented to attest the parent")
		}
		workload := ""
		if frame, ok := transport.FromContext(ctx); ok && frame.Scope.Deployment != "" {
			workload = auth.WorkloadID(frame.Scope)
		}
		claims, verr := s.cfg.TaskVerifier.Verify(token, s.cfg.TaskAudience, auth.KindTask, workload)
		if verr != nil {
			return auth.Claims{}, attestationError(ReasonAttestationUnverifiable,
				"gateway: the presented task credential is not verifiable: "+verr.Error())
		}
		return claims, nil
	}
	frame, ok := transport.FromContext(ctx)
	if !ok {
		return auth.Claims{}, attestationError(ReasonAttestationMissing,
			"gateway: no verified principal carried the parent attestation")
	}
	return frame.Claims, nil
}

// attestationError builds the operator-facing cause of an attestation failure.
// It carries a code and a reason only; a credential never appears in it.
func attestationError(reason, message string) error {
	return contracts.NewReasoned(contracts.ErrForbidden, reason, message)
}

// bearerFromContext returns the bearer token of the request being served, as
// captured at the ingress. It is kept for the duration of one request only and
// is never logged: refusal details are sanitised and no refusal path prints it.
func bearerFromContext(ctx context.Context) string {
	v, _ := ctx.Value(bearerContextKey{}).(string)
	return v
}

type bearerContextKey struct{}

// refuse writes an operator journal line from inside the service, using the same
// projection the handler writes refusals with.
func (s *Service) refuse(ctx context.Context, op, stage string, ids refusalIDs, err error) {
	if err == nil {
		return
	}
	writeRefusal(s.logger(), op, stage, ids, err)
}

// logger returns the operator journal for this service.
func (s *Service) logger() *slog.Logger {
	if s == nil || s.cfg.Logger == nil {
		return slog.Default()
	}
	return s.cfg.Logger
}

// attestationMarkers is the metadata a gateway-set acceptance adds to the child.
func (a *attestedParent) markers() map[string]any {
	if a == nil {
		return nil
	}
	out := map[string]any{MetaParentAttested: ParentAttestedExternal}
	if a.Deployment != "" {
		out[MetaParentAttestedDeployment] = a.Deployment
	}
	return out
}

// acceptedByMarkers answers "which plane took this task" from this plane's own
// identity -- the workload its certificate carries, handed to the gateway by the
// composition root. It is never derived from the request, because the request's
// scope belongs to whoever is asking, and on a delegation hop that is the parent.
func (s *Service) acceptedByMarkers() map[string]any {
	plane := s.cfg.PlaneDeployment
	if plane == "" {
		plane = AcceptedByUnconfigured
	}
	return map[string]any{MetaAcceptedByDeployment: plane}
}

// isAttestationMarker reports whether a metadata key may only be set by this
// plane. Caller-supplied copies are dropped, as the tree keys already are.
func isAttestationMarker(key string) bool {
	return strings.EqualFold(key, MetaParentAttested) ||
		strings.EqualFold(key, MetaParentAttestedDeployment) ||
		strings.EqualFold(key, MetaAcceptedByDeployment)
}
