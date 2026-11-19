package deploycontroller

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// This opt-in leg runs kubectl against the operator's isolated local cluster.
// The object file is rendered by ahp-deploy, with explicit test-network wiring.
func TestApplyRealOwnedCluster(t *testing.T) {
	path := os.Getenv("AHP_DEPLOY_CLUSTER_OBJECTS")
	if path == "" {
		t.Skip("requires explicit isolated cluster, rendered objects and private KUBECONFIG; no cluster proof without prerequisites")
	}
	target := os.Getenv("AHP_DEPLOY_CLUSTER_CONTEXT")
	namespace := os.Getenv("AHP_DEPLOY_CLUSTER_NAMESPACE")
	if !strings.HasPrefix(target, "k3d-ahp-handbook-closure-") || !strings.HasPrefix(namespace, "ahp-closure-") || os.Getenv("KUBECONFIG") == "" {
		t.Fatal("owned explicit local cluster and namespace required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Items []map[string]any `json:"items"`
	}
	if err = json.Unmarshal(raw, &list); err != nil {
		t.Fatal(err)
	}
	deployments := 0
	for _, o := range list.Items {
		meta, ok := o["metadata"].(map[string]any)
		if !ok {
			t.Fatal("object metadata missing")
		}
		switch o["kind"] {
		case "Namespace":
			if meta["name"] != namespace {
				t.Fatal("foreign namespace")
			}
		case "CustomResourceDefinition":
			if meta["name"] != "statescopes.ahp.github.io" {
				t.Fatal("unexpected cluster-wide object")
			}
		default:
			if meta["namespace"] != namespace {
				t.Fatal("foreign namespaced object")
			}
		}
		if o["kind"] == "Deployment" {
			deployments++
		}
	}
	if deployments != 1 {
		t.Fatal("one isolated actual runtime deployment required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	if err = Apply(ctx, target, list.Items); err != nil {
		t.Fatal(err)
	}
	t.Log("actual server dry-run, CRD/namespace bootstrap, apply and Deployment rollout completed; readiness alone is not LLM E2E")
}
