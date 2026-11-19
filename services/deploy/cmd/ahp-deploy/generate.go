package main

// Object is a Kubernetes object represented as a generic map so it serialises
// to JSON (a JSON document is also a valid YAML document, so the controller's
// `---`-separated output can be piped straight into `kubectl apply -f`).

// Generate turns a VALID manifest into the platform objects the deploy
// controller owns (R6.6-02): Namespace, ServiceAccount, NetworkPolicy, the
// State API scope grant, and RBAC grants. It assumes Validate() returned no
// errors. Deterministic ordering for reproducible output.
func (m *Manifest) Generate() []map[string]any {
	if len(m.Validate()) > 0 || len(m.ValidateRuntime()) > 0 {
		return nil
	}
	ns := m.namespace()
	svc := m.serviceName()

	objects := []map[string]any{
		namespaceObj(ns, m),
		stateScopeCRD(),
		serviceAccountObj(ns, svc),
		networkPolicyObj(ns, svc, m),
		stateScopeObj(ns, svc, m),
		roleObj(ns, svc, m),
		roleBindingObj(ns, svc),
	}
	return append(objects, workloadObjects(ns, svc, m)...)
}

func objMeta(apiVersion, kind, name, ns string, labels map[string]string) map[string]any {
	meta := map[string]any{"name": name, "labels": labels}
	if ns != "" {
		meta["namespace"] = ns
	}
	return map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": meta}
}

func (m *Manifest) commonLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":        m.serviceName(),
		"app.kubernetes.io/part-of":     "ahp",
		"ahp.github.io/system":          m.System,
		"ahp.github.io/kind":            m.Kind,
		"ahp.github.io/visibility":      m.Visibility,
		"ahp.github.io/audience":        m.Audience,
		"ahp.github.io/state-partition": m.State.Partition,
	}
}

func namespaceObj(ns string, m *Manifest) map[string]any {
	labels := m.commonLabels()
	labels["pod-security.kubernetes.io/enforce"] = "restricted"
	o := objMeta("v1", "Namespace", ns, "", labels)
	// R10/R6.6: tier + boundary metadata is a label/annotation, never a
	// source==deployment claim.
	o["metadata"].(map[string]any)["annotations"] = map[string]string{
		"ahp.github.io/managed-by":     "deploy-controller",
		"ahp.github.io/manifest-scope": "namespace",
	}
	return o
}

func serviceAccountObj(ns, svc string) map[string]any {
	o := objMeta("v1", "ServiceAccount", svc, ns, map[string]string{
		"app.kubernetes.io/name":    svc,
		"app.kubernetes.io/part-of": "ahp",
	})
	o["automountServiceAccountToken"] = false
	return o
}

// networkPolicyObj enforces the PDF boundary: pods accept traffic only from the
// gateway (single A2A entry) and talk only to the State API, model gateway, and
// the deployments they are allowed to `calls`. Everything else is denied — the
// "всё, что не нарисовано, закрыто NetworkPolicy" invariant (PDF §4.2).
func networkPolicyObj(ns, svc string, m *Manifest) map[string]any {
	o := objMeta("networking.k8s.io/v1", "NetworkPolicy", svc+"-netpol", ns, map[string]string{
		"app.kubernetes.io/name": svc,
	})
	podSel := map[string]any{"matchLabels": map[string]string{"app.kubernetes.io/name": svc}}

	// Egress: control-plane + State API + model gateway + explicit calls only.
	egressPeers := []any{
		map[string]any{"namespaceSelector": map[string]any{"matchLabels": map[string]string{"ahp.github.io/tier": "control-plane"}}}, // gateway, identity, state
	}
	for _, c := range m.Capability.Calls {
		egressPeers = append(egressPeers, map[string]any{
			"namespaceSelector": map[string]any{"matchLabels": map[string]string{"ahp.github.io/system": c}},
		})
	}
	// A2A entry can be called back only by the gateway (control-plane ns).
	ingressFrom := []any{
		map[string]any{"namespaceSelector": map[string]any{"matchLabels": map[string]string{"ahp.github.io/tier": "control-plane"}}},
	}
	o["spec"] = map[string]any{
		"podSelector": podSel,
		"policyTypes": []string{"Ingress", "Egress"},
		"ingress": []any{
			map[string]any{"from": ingressFrom, "ports": []any{
				map[string]any{"port": 8443, "protocol": "TCP"},
			}},
		},
		"egress": []any{
			map[string]any{"to": []any{map[string]any{"namespaceSelector": map[string]any{"matchLabels": map[string]string{"kubernetes.io/metadata.name": "kube-system"}}, "podSelector": map[string]any{"matchLabels": map[string]string{"k8s-app": "kube-dns"}}}}, "ports": []any{map[string]any{"port": 53, "protocol": "UDP"}, map[string]any{"port": 53, "protocol": "TCP"}}},
			map[string]any{"to": egressPeers, "ports": []any{
				map[string]any{"port": 8443, "protocol": "TCP"},
			}},
		},
	}
	return o
}

// stateScopeObj is the declarative State API partition/scope grant the deploy
// controller registers so Identity & Policy can enforce tenant/deployment
// isolation and the shared-namespace review gate (R6.6-02, R6.8).
func stateScopeObj(ns, svc string, m *Manifest) map[string]any {
	o := objMeta("ahp.github.io/v1alpha1", "StateScope", svc+"-state", ns, map[string]string{
		"app.kubernetes.io/name": svc,
	})
	shared := m.State.Shared
	if shared == nil {
		shared = []string{}
	}
	o["spec"] = map[string]any{
		"partition":  m.State.Partition,
		"shared":     shared,
		"compat":     m.State.Compat,
		"drain":      m.State.Drain,
		"ownTables":  m.State.OwnTables,
		"deleteHook": m.State.DeleteHook,
	}
	return o
}

// roleObj encodes the capability CEILING from the manifest (R6.6-03). The
// tenant admin may only NARROW this at install time, never widen it, so the
// generated Role is the maximum permission set.
func roleObj(ns, svc string, m *Manifest) map[string]any {
	o := objMeta("rbac.authorization.k8s.io/v1", "Role", svc+"-caps", ns, map[string]string{
		"app.kubernetes.io/name": svc,
	})
	rules := []any{
		map[string]any{ // access own State scope only
			"apiGroups":     []string{"ahp.github.io"},
			"resources":     []string{"statescopes"},
			"verbs":         []string{"get"},
			"resourceNames": []string{svc + "-state"},
		},
	}
	// Capability ceiling recorded as annotations consumed by Identity & Policy;
	// models/tools/memory/compute are ceilings, tenant may narrow.
	o["metadata"].(map[string]any)["annotations"] = map[string]string{
		"ahp.github.io/cap-models":  joinList(m.Capability.Models),
		"ahp.github.io/cap-tools":   joinList(m.Capability.Tools),
		"ahp.github.io/cap-memory":  joinList(m.Capability.Memory),
		"ahp.github.io/cap-compute": joinList(m.Capability.Compute),
		"ahp.github.io/cap-calls":   joinList(m.Capability.Calls),
		"ahp.github.io/max-fanout":  itoa(m.Capability.SelfTasks.MaxFanout),
		"ahp.github.io/limits":      m.Limits.String(),
		"ahp.github.io/capability":  "ceiling",
	}
	o["rules"] = rules
	return o
}

func roleBindingObj(ns, svc string) map[string]any {
	o := objMeta("rbac.authorization.k8s.io/v1", "RoleBinding", svc+"-caps", ns, map[string]string{
		"app.kubernetes.io/name": svc,
	})
	o["subjects"] = []any{
		map[string]any{"kind": "ServiceAccount", "name": svc, "namespace": ns},
	}
	o["roleRef"] = map[string]any{
		"apiGroup": "rbac.authorization.k8s.io",
		"kind":     "Role",
		"name":     svc + "-caps",
	}
	return o
}
