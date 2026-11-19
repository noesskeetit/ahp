package gateway_test

// DEF-J4-DEPLOY-ATTRIBUTION (P1, PDF rev3.2 board row 1 "A2A A->B": parent,
// subject and DEPLOYMENT of a delegated child must be in the durable journal).
//
// Measured by the reviewer on canon 4b99a287 (reports/mac-director/
// a2b-j4-4b99a287-20261110T0519Z.md, section 4 item 1): a child accepted by a
// SECOND plane carries the delegating plane's deployment everywhere the journal
// looks -- ahp_exec.tasks.deployment is the caller's scope, and the only
// deployment attribute is ahp.parent_attested_deployment, which is the parent's
// own. The workload that accepted the task is computed for verification and then
// dropped, so "B" is absent from the durable record: no tasks row, no
// generations/budgets row names the accepting plane.
//
// This leg pins both halves of the requirement: the parent's attribution must
// STAY as it is (it is a fact about the parent, and renaming it would launder one
// actor into another), and the accepting plane must become visible in the durable
// journal as its own attribute, readable without opening any payload.

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/internal/gateway"
)

// durableDeploymentColumn reads the scope column the child was journalled under,
// straight out of the tasks table: the same query the reviewer ran.
func durableDeploymentColumn(t *testing.T, e *env, taskID string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var deployment string
	if err := e.pool.QueryRow(ctx, `SELECT deployment FROM `+e.schema+`.tasks WHERE task_id=$1`,
		taskID).Scan(&deployment); err != nil {
		t.Fatalf("read the journalled deployment of %s: %v", taskID, err)
	}
	return deployment
}

func TestDelegatedChildNamesTheAcceptingPlaneInDurable(t *testing.T) {
	e := newEnv(t)
	// This plane knows its own deployment: that is the identity the journal must
	// carry, and it is not readable off a request (see Config.PlaneDeployment).
	svc := e.service(t, echoExecutor(), func(cfg *gateway.Config) {
		cfg.PlaneDeployment = e.scope.Deployment
	})

	const externalRoot = "task-runner-a-root"
	const externalParent = "task-runner-a-leaf"
	claims := externalRunnerClaims(e, externalParent, externalRoot, 2)

	ctx, cancel := context.WithTimeout(attestCtx(claims), 60*time.Second)
	defer cancel()
	task, err := svc.SendMessage(ctx, claims.Scope(), externalParent,
		delegatedSend("msg-accepted-by-"+e.sfx, externalParent, 4))
	if err != nil {
		t.Fatalf("the cross-deployment child was refused: %v", err)
	}

	scopeOfThisPlane := e.scope
	column := durableDeploymentColumn(t, e, string(task.ID))
	md := treeMetaIn(t, e, claims.Scope(), string(task.ID))
	parentAttr, _ := md["ahp.parent_attested_deployment"].(string)

	// Two different planes, which is the whole premise of the board row: the
	// parent lives in the runner's deployment, the child is accepted here.
	if claims.Deployment == scopeOfThisPlane.Deployment {
		t.Fatalf("the fixture stopped being a cross-deployment hop: both planes are %q",
			claims.Deployment)
	}
	t.Logf("parent deployment %q, accepting deployment %q, journalled scope column %q",
		claims.Deployment, scopeOfThisPlane.Deployment, column)

	// (1) The parent's attribution must not be re-dressed. The scope column keeps
	// placing the task under the credential that presented it, and the attestation
	// attribute keeps naming the parent's own plane. Neither may be repurposed to
	// carry the accepting plane: that would erase the parent instead of adding the child.
	if column != claims.Deployment {
		t.Fatalf("the parent's attribution changed: scope column = %q, want the presenting credential's deployment %q",
			column, claims.Deployment)
	}
	if parentAttr != claims.Deployment {
		t.Fatalf("ahp.parent_attested_deployment = %q, want the parent's plane %q unchanged",
			parentAttr, claims.Deployment)
	}

	// (2) The accepting plane must be named by the journal itself, as its own
	// attribute, without reading a payload.
	const acceptedByKey = "ahp.accepted_by_deployment"
	got, present := md[acceptedByKey]
	if !present {
		t.Fatalf("DEF-J4-DEPLOY-ATTRIBUTION: the durable journal of the accepted child names only the parent "+
			"(scope column %q, ahp.parent_attested_deployment %q); the plane that ACCEPTED the task (%q) is "+
			"nowhere in %v, so B cannot be seen in durable and board row 1 cannot be signed by a query",
			column, parentAttr, scopeOfThisPlane.Deployment, sortedKeys(md))
	}
	if got != scopeOfThisPlane.Deployment {
		t.Fatalf("%s = %v, want this plane's own deployment %q", acceptedByKey, got, scopeOfThisPlane.Deployment)
	}
	if got == parentAttr {
		t.Fatalf("%s repeats the parent's deployment %v instead of naming the accepting plane", acceptedByKey, got)
	}
	// The attribute must survive the task's own later merges (attempt stats, close
	// reason): a fact about acceptance cannot be the first thing a transition drops.
	// The merge below is the production writer the worker itself uses.
	if err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		return e.store.MergeMetadataTx(ctx, tx, string(task.ID), map[string]any{
			gateway.MetadataStepsConsumed: 1,
		})
	}); err != nil {
		t.Fatalf("post-accept merge with the production writer: %v", err)
	}
	if again, _ := treeMetaIn(t, e, claims.Scope(), string(task.ID))[acceptedByKey].(string); again != scopeOfThisPlane.Deployment {
		t.Fatalf("%s = %q after a later metadata merge, want it to persist", acceptedByKey, again)
	}
}

// TestAcceptedByIsVisibleWhenThePlaneCannotNameItself refuses the quiet variant of
// the defect: a plane that was never told its deployment must SAY so in the
// journal. An absent attribute is indistinguishable, to a query, from the parent's
// name having been written there instead.
func TestAcceptedByIsVisibleWhenThePlaneCannotNameItself(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil) // no PlaneDeployment: today's mounts
	task := mustSubmit(t, svc, e.scope, e.caller, "accepted-by-unconfigured-"+e.sfx, nil)
	md := treeMetaIn(t, e, e.scope, string(task.ID))
	got, present := md["ahp.accepted_by_deployment"]
	if !present {
		t.Fatalf("an unnamed plane must journal %q rather than nothing: with no attribute a reader "+
			"cannot tell \"the plane is unknown\" from \"the parent accepted it\" (keys %v)",
			gateway.AcceptedByUnconfigured, sortedKeys(md))
	}
	if got != gateway.AcceptedByUnconfigured {
		t.Fatalf("ahp.accepted_by_deployment = %v, want %q", got, gateway.AcceptedByUnconfigured)
	}
	if got == e.scope.Deployment {
		t.Fatal("an unconfigured plane must not be credited with a deployment it was never given")
	}
}

// TestCallerCannotForgeAcceptedByDeployment: the caller cannot decide what the
// journal says about which plane took the task. The protection is the unconditional
// gateway write on acceptance -- whatever the caller sent, this plane's own identity
// is journalled over it (removing that write is what reddens this leg, see the
// MUT-1 evidence). The ingress list is a SECOND, different protection -- it is what
// drops spellings the canonical write cannot reach, and it is proven separately by
// TestIngressFilterIsTheOnlyThingBetweenACallerAndAFakeAcceptingPlane (MUT-2),
// because this leg alone cannot see it.
func TestCallerCannotForgeAcceptedByDeployment(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), func(cfg *gateway.Config) {
		cfg.PlaneDeployment = e.scope.Deployment
	})
	task := mustSubmit(t, svc, e.scope, e.caller, "accepted-by-forged-"+e.sfx, func(req *a2a.SendMessageRequest) {
		req.Metadata = map[string]any{"ahp.accepted_by_deployment": "dep-the-victim-plane"}
	})
	md := treeMetaIn(t, e, e.scope, string(task.ID))
	if got := md["ahp.accepted_by_deployment"]; got != e.scope.Deployment {
		t.Fatalf("a caller wrote the acceptance attribute: got %v, want this plane %q",
			got, e.scope.Deployment)
	}
	// The forged key must not survive under any other spelling either.
	for k := range md {
		if strings.EqualFold(k, "ahp.accepted_by_deployment") && k != "ahp.accepted_by_deployment" {
			t.Fatalf("the forged attribute survived under a second spelling %q", k)
		}
	}
	if _, ok := md["dep-the-victim-plane"]; ok {
		t.Fatal("the forged value leaked into the metadata as its own key")
	}
}

func sortedKeys(md map[string]any) []string {
	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestIngressFilterIsTheOnlyThingBetweenACallerAndAFakeAcceptingPlane makes the
// ingress filter observable, which the legs above cannot (C-INGRESS).
//
// The unconditional gateway write can only ever overwrite the CANONICAL spelling of
// the key, so it is not what stops a caller from planting the attribute under a
// different spelling -- isAttestationMarker is. Without that filter the durable
// journal keeps a second, caller-authored claim about which plane took the task, and
// a reader that folds case (or a consumer reading JSON loosely) believes a plane that
// never saw the task. Drop the key from the ingress list -- mutation MUT-2 -- and this
// leg is the one that reddens; the write-order legs stay green, which is precisely why
// they never proved the filter and why this leg exists.
func TestIngressFilterIsTheOnlyThingBetweenACallerAndAFakeAcceptingPlane(t *testing.T) {
	const acceptedByKey = "ahp.accepted_by_deployment"
	const decoySpelling = "AHP.Accepted_By_Deployment"
	const decoyValue = "dep-a-plane-that-never-saw-this-task"

	e := newEnv(t)
	svc := e.service(t, echoExecutor(), func(cfg *gateway.Config) {
		cfg.PlaneDeployment = e.scope.Deployment
	})
	task := mustSubmit(t, svc, e.scope, e.caller, "accepted-by-decoy-"+e.sfx,
		func(req *a2a.SendMessageRequest) {
			req.Metadata = map[string]any{decoySpelling: decoyValue}
		})

	md := treeMetaIn(t, e, e.scope, string(task.ID))
	var spellings []string
	for k, v := range md {
		if !strings.EqualFold(k, acceptedByKey) {
			continue
		}
		spellings = append(spellings, k)
		if k != acceptedByKey {
			t.Fatalf("DEF-J4-DEPLOY-ATTRIBUTION C-INGRESS: the caller planted an acceptance claim in the "+
				"durable journal as %q=%v; the gateway write cannot remove it because it writes the canonical "+
				"key only, so isAttestationMarker is the sole thing between a caller and a fake accepting "+
				"plane. Journal keys: %v", k, v, sortedKeys(md))
		}
	}
	if len(spellings) != 1 || spellings[0] != acceptedByKey {
		t.Fatalf("the journal must carry exactly one spelling of %s (the plane's own), got %v in %v",
			acceptedByKey, spellings, sortedKeys(md))
	}
	if got := md[acceptedByKey]; got != e.scope.Deployment {
		t.Fatalf("%s = %v, want this plane's own deployment %q, want the decoy %q nowhere",
			acceptedByKey, got, e.scope.Deployment, decoyValue)
	}
}
