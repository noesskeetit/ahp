// Package deploycontroller applies rendered objects through the operator's
// explicit Kubernetes context. It reports partial apply honestly; Kubernetes
// has no multi-resource transaction and rollback cannot safely delete objects
// owned by previous releases.
package deploycontroller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

func Apply(ctx context.Context, kubeContext string, objects []map[string]any) error {
	if strings.TrimSpace(kubeContext) == "" {
		return fmt.Errorf("explicit Kubernetes context required")
	}
	if len(objects) == 0 {
		return fmt.Errorf("no objects to deploy")
	}
	run := func(input []byte, args ...string) error {
		base := []string{"--context", kubeContext, "--request-timeout=30s"}
		cmd := exec.CommandContext(ctx, "kubectl", append(base, args...)...)
		cmd.Stdin = bytes.NewReader(input)
		// Do not echo kubectl stdout (Secrets may be present in admission diagnostics).
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("kubectl %s failed: %w", args[0], err)
		}
		return nil
	}
	encode := func(items []map[string]any) []byte {
		b, _ := json.Marshal(map[string]any{"apiVersion": "v1", "kind": "List", "items": items})
		return b
	}
	// CRDs must exist before server-side discovery can validate custom resources.
	// First validate/apply infrastructure, then validate all namespaced resources.
	var bootstrap, workloads []map[string]any
	for _, o := range objects {
		if o["kind"] == "CustomResourceDefinition" || o["kind"] == "Namespace" {
			bootstrap = append(bootstrap, o)
		} else {
			workloads = append(workloads, o)
		}
	}
	if len(bootstrap) > 0 {
		if err := run(encode(bootstrap), "apply", "--server-side", "--dry-run=server", "--field-manager=ahp-deploy", "-f", "-"); err != nil {
			return fmt.Errorf("bootstrap validation: %w", err)
		}
		if err := run(encode(bootstrap), "apply", "--server-side", "--field-manager=ahp-deploy", "-f", "-"); err != nil {
			return fmt.Errorf("bootstrap may be partially applied: %w", err)
		}
		for _, o := range bootstrap {
			if o["kind"] == "CustomResourceDefinition" {
				meta := o["metadata"].(map[string]any)
				if err := run(nil, "wait", "--for=condition=Established", "--timeout=60s", "crd/"+fmt.Sprint(meta["name"])); err != nil {
					return err
				}
			}
		}
	}
	if err := run(encode(workloads), "apply", "--server-side", "--dry-run=server", "--field-manager=ahp-deploy", "-f", "-"); err != nil {
		return fmt.Errorf("workload validation failed; only namespace/CRD bootstrap may exist: %w", err)
	}
	if err := run(encode(workloads), "apply", "--server-side", "--field-manager=ahp-deploy", "-f", "-"); err != nil {
		return fmt.Errorf("objects may be partially applied: %w", err)
	}
	for _, o := range workloads {
		if o["kind"] == "Deployment" {
			m := o["metadata"].(map[string]any)
			if err := run(nil, "-n", fmt.Sprint(m["namespace"]), "rollout", "status", "deployment/"+fmt.Sprint(m["name"]), "--timeout=180s"); err != nil {
				return fmt.Errorf("deployment not ready: %w", err)
			}
		}
	}
	return nil
}
