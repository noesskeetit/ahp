package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// runScopeAttester is the ONE canonical source of the scope a run's records are
// filed under: the run's own task token, verified against the published Identity
// keys and bound to this process's workload.
//
// Why one shared rule instead of one rule per consumer: a browser turn carries the
// HUMAN principal's scope, which has no deployment (a human token carries no azp),
// while the run itself executes under a TASK credential that does -- the Run
// Coordinator mints that credential next to the request and hands it over as
// space.CoreRequest.TaskToken and as core.WithRunScope's token. A consumer that
// files under the scope it was handed therefore labels the same logical run
// differently from a consumer that reads the credential. That divergence is
// DEF-H10-9: with AHP_DELEGATE_TREE_BUDGET=1 a browser-initiated run died at step 0
// ("scope deployment must not be empty") while the delegation leg of the SAME run
// wrote its ledger row WITH a deployment -- one run, two scope labels, no
// comparable numbers at the two ends of an A->B chain.
//
// Completing a scope is not a bypass and must not become one. The deployment comes
// only from a token that (a) really verifies against the published Identity keys,
// (b) is a task token bound to a task/root, and (c) names THIS process's workload
// -- the SPIFFE identity of our own workload certificate, because Identity sets a
// run token's tenant/deployment from exactly that certificate. An unverifiable
// token, a foreign workload, a tenant mismatch or an unpinned workload yields NO
// scope, and every consumer that needs one fails closed instead of filing bytes or
// numbers under a label nobody authorised.
type runScopeAttester struct {
	verifier     *auth.Verifier
	taskAudience string
	workload     string
}

// enabled reports whether this process can attest a scope at all. It is a mount
// fact, logged once at startup: a deployment that accounts budgets but cannot name
// a scope is a misconfiguration an operator has to see.
func (a runScopeAttester) enabled() bool { return a.verifier != nil }

// bindHumanScope runs AFTER the trusted Identity exchange. Human JWTs carry no
// deployment; cold-rehydrated turns carry the State lease's full scope. Use the
// verified exchange token to give warm and cold ownership the same scope, while
// retaining human claims for human-only checks. Request fields supply no scope.
func (a runScopeAttester) bindHumanScope(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, present := transport.FromContext(r.Context())
		token, exchanged := space.ScopedTaskFromContext(r.Context())
		deny := func() {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "space: exchanged workload scope does not match the verified human"))
		}
		if !present || !exchanged || a.verifier == nil || a.workload == "" || p.Claims.Kind != auth.KindHuman {
			deny()
			return
		}
		claims, err := a.verifier.Verify(token, a.taskAudience, auth.KindTask, a.workload)
		if err != nil || claims.Task == "" || claims.Root == "" || claims.Tenant != p.Scope.Tenant || claims.Subject != p.Scope.Subject ||
			p.Claims.Tenant != p.Scope.Tenant || p.Claims.Subject != p.Scope.Subject ||
			(p.Scope.Deployment != "" && p.Scope.Deployment != claims.Deployment) {
			deny()
			return
		}
		p.Scope = claims.Scope()
		next.ServeHTTP(w, r.WithContext(transport.WithPrincipal(r.Context(), p)))
	})
}

// runAuthorityScope is the process-wide attester a delegating tool uses to name the
// task its OWN credential attests. Installed by main; a zero value means this
// process cannot attest, and the consumer below says so instead of guessing.
var runAuthorityScope runScopeAttester

// buildRunScopeAttester builds the canonical attester from the two sources every
// other run-scope consumer already uses: the published Identity keys
// (artifactScopeVerifier) and this process's pinned workload (its own workload
// certificate). It is deliberately not a new authority -- it is the same one
// DEF-H10-9 established, reused so a run cannot be labelled one way by the ledger
// and another way by the hop.
func buildRunScopeAttester() (runScopeAttester, error) {
	verifier, taskAudience, err := artifactScopeVerifier()
	if err != nil {
		return runScopeAttester{}, err
	}
	workload := ""
	if verifier != nil {
		workload, err = expectedWorkloadID(os.Getenv("AS_WORKLOAD_CERT"))
		if err != nil {
			return runScopeAttester{}, fmt.Errorf("as-runner: run authority scope attester: %w", err)
		}
	}
	return runScopeAttester{verifier: verifier, taskAudience: taskAudience, workload: workload}, nil
}

// attestedTask names the task the run's own task credential attests: the identity
// grant task (rtask-<subject>:<key>), which is what ahp_state.runs.task_id stores
// and what internal/gateway/outbox.go declares canonical for ahp_parent_task.
//
// This is not a decoding shortcut. The token is VERIFIED -- signature against the
// published Identity keys, kind=task, audience, expiry, and binding to this
// process's own workload -- exactly as runScopeAttester.complete does; an
// unverifiable or foreign-workload credential yields an error, never a task id.
// A credential that verifies but names no task/root attests no parent, so it too
// is an error rather than an empty string a caller could mistake for a value.
func (a runScopeAttester) attestedTask(taskToken string) (string, error) {
	if a.verifier == nil || taskToken == "" {
		return "", errors.New("no published keys to verify the run credential against, or no run credential presented")
	}
	if a.workload == "" {
		return "", errors.New("runner workload identity is not pinned")
	}
	claims, err := a.verifier.Verify(taskToken, a.taskAudience, auth.KindTask, a.workload)
	if err != nil {
		return "", fmt.Errorf("run task token is not verifiable: %w", err)
	}
	if claims.Task == "" || claims.Root == "" {
		return "", errors.New("run task token is bound to neither a task nor a root")
	}
	return claims.Task, nil
}

// complete returns the scope to file under. A scope that already names its
// deployment is the authority's own statement and is never rewritten here; a scope
// that does not is completed from the credential, never from a request field.
func (a runScopeAttester) complete(scope contracts.Scope, taskToken string) (contracts.Scope, error) {
	if scope.Deployment != "" {
		return scope, nil
	}
	if a.verifier == nil || taskToken == "" {
		return contracts.Scope{}, errors.New("run scope names no deployment and no verified task token is available to complete it")
	}
	if a.workload == "" {
		return contracts.Scope{}, errors.New("runner workload identity is not pinned")
	}
	claims, verr := a.verifier.Verify(taskToken, a.taskAudience, auth.KindTask, a.workload)
	if verr != nil {
		return contracts.Scope{}, fmt.Errorf("run task token is not verifiable: %w", verr)
	}
	if claims.Tenant != "" && claims.Tenant != scope.Tenant {
		return contracts.Scope{}, errors.New("run task token tenant does not match the run scope")
	}
	scope.Deployment = claims.Deployment
	return scope, nil
}
