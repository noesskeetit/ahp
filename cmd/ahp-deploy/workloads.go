package main

import (
	"fmt"
	"regexp"
)

var immutableImage = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]{0,255}@sha256:[0-9a-f]{64}$`)
var dnsLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

// Drain lifecycle kinds a component may declare. See Runtime.DrainHooks.
const (
	drainHookAhpAgent = "ahp-agent"
	drainHookSigterm  = "sigterm"
)

// Runtime is the build/deploy attachment to the PDF manifest. Images are build
// artifacts, not new user-facing capabilities. No implicit image or tag exists.
type Runtime struct {
	Images           map[string]string `json:"images"`
	TLSSecret        string            `json:"tls_secret"`
	ImagePullSecrets []string          `json:"image_pull_secrets,omitempty"`
	// Operator-provisioned env config, per component. Values stay in Secrets,
	// outside the release manifest; workloads never receive a provider key here.
	ConfigSecrets map[string]string `json:"config_secrets,omitempty"`
	// DrainHooks declares, per component, how that container drains when the pod
	// is stopped, so the renderer never emits a lifecycle hook the image cannot
	// run. "ahp-agent" renders a preStop that runs the platform agent's in-process
	// drain (/ahp-agent --drain); "sigterm" renders no preStop at all, relying on
	// Kubernetes SIGTERM + terminationGracePeriodSeconds + the registry-side drain
	// -- exactly how the Python custom-agent container (python -m
	// ahp_state.container) stops. Every component must declare one; an undeclared
	// or unknown value is refused, never defaulted to the Go agent's hook.
	DrainHooks map[string]string `json:"drain_hooks,omitempty"`
}

func (m *Manifest) ValidateRuntime() []string {
	var errs []string
	if !dnsLabel.MatchString(m.System) || len(m.System) > 50 {
		errs = append(errs, "system must fit a Kubernetes DNS label of at most 50 characters")
	}
	if !dnsLabel.MatchString(m.Runtime.TLSSecret) {
		errs = append(errs, "runtime.tls_secret must name a pre-provisioned workload TLS Secret")
	}
	for _, name := range m.Runtime.ImagePullSecrets {
		if !dnsLabel.MatchString(name) {
			errs = append(errs, "invalid imagePullSecret name")
		}
	}
	components := make(map[string]bool, len(m.Components))
	for _, component := range m.Components {
		components[component.Name] = true
	}
	for component, name := range m.Runtime.ConfigSecrets {
		if !components[component] || !dnsLabel.MatchString(name) {
			errs = append(errs, fmt.Sprintf("runtime.config_secrets.%s must reference an existing component and a valid Secret name", component))
		}
	}
	for _, c := range m.Components {
		if len(m.System)+1+len(c.Name) > 63 {
			errs = append(errs, "combined system/component name exceeds Kubernetes service name limit")
		}
		if !dnsLabel.MatchString(c.Name) {
			errs = append(errs, "component name must be a Kubernetes DNS label")
		}
		if !immutableImage.MatchString(m.Runtime.Images[c.Name]) {
			errs = append(errs, fmt.Sprintf("runtime.images.%s must be repository@sha256:<64 hex>", c.Name))
		}
		switch hook := m.Runtime.DrainHooks[c.Name]; hook {
		case drainHookAhpAgent, drainHookSigterm:
		case "":
			errs = append(errs, fmt.Sprintf("runtime.drain_hooks.%s is required (ahp-agent|sigterm): the controller will not assume /ahp-agent --drain on an image that may not ship it", c.Name))
		default:
			errs = append(errs, fmt.Sprintf("runtime.drain_hooks.%s must be one of ahp-agent|sigterm, got %q", c.Name, hook))
		}
	}
	return errs
}

func stateScopeCRD() map[string]any {
	o := objMeta("apiextensions.k8s.io/v1", "CustomResourceDefinition", "statescopes.ahp.github.io", "", map[string]string{"app.kubernetes.io/part-of": "ahp"})
	o["spec"] = map[string]any{"group": "ahp.github.io", "scope": "Namespaced", "names": map[string]any{"plural": "statescopes", "singular": "statescope", "kind": "StateScope"}, "versions": []any{map[string]any{"name": "v1alpha1", "served": true, "storage": true, "schema": map[string]any{"openAPIV3Schema": map[string]any{"type": "object", "properties": map[string]any{"spec": map[string]any{"type": "object", "required": []string{"partition", "drain"}, "properties": map[string]any{"partition": map[string]any{"type": "string", "enum": []string{"subject", "tenant"}}, "shared": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}, "compat": map[string]any{"type": "string"}, "drain": map[string]any{"type": "string"}, "ownTables": map[string]any{"type": "boolean"}, "deleteHook": map[string]any{"type": "string"}}}}}}}}}
	return o
}

func workloadObjects(ns, svc string, m *Manifest) []map[string]any {
	var out []map[string]any
	for _, c := range m.Components {
		name := svc + "-" + c.Name
		labels := map[string]string{"app.kubernetes.io/name": svc, "app.kubernetes.io/component": c.Name, "app.kubernetes.io/part-of": "ahp", "app.kubernetes.io/managed-by": "ahp-deploy"}
		selector := map[string]string{"app.kubernetes.io/name": svc, "app.kubernetes.io/component": c.Name}
		replicas := 2
		if c.Replicas != nil {
			replicas = c.Replicas.Min
		}
		probe := func(path string) map[string]any {
			return map[string]any{"httpGet": map[string]any{"path": path, "port": "https", "scheme": "HTTPS"}, "periodSeconds": 5, "timeoutSeconds": 2}
		}
		container := map[string]any{"name": c.Name, "image": m.Runtime.Images[c.Name], "imagePullPolicy": "IfNotPresent", "ports": []any{map[string]any{"name": "https", "containerPort": 8443}}, "resources": map[string]any{"requests": map[string]string{"cpu": "100m", "memory": "128Mi"}, "limits": map[string]string{"cpu": "1000m", "memory": "512Mi"}}, "securityContext": map[string]any{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": map[string]any{"drop": []string{"ALL"}}}, "startupProbe": probe("/healthz"), "readinessProbe": probe("/readyz"), "livenessProbe": probe("/healthz"), "env": []any{map[string]any{"name": "AHP_LISTEN", "value": ":8443"}, map[string]any{"name": "AHP_TLS_CERT", "value": "/var/run/ahp/tls/tls.crt"}, map[string]any{"name": "AHP_TLS_KEY", "value": "/var/run/ahp/tls/tls.key"}, map[string]any{"name": "AHP_TLS_CA", "value": "/var/run/ahp/tls/ca.crt"}}, "volumeMounts": []any{map[string]any{"name": "tls", "mountPath": "/var/run/ahp/tls", "readOnly": true}, map[string]any{"name": "tmp", "mountPath": "/tmp"}}}
		if secret := m.Runtime.ConfigSecrets[c.Name]; secret != "" {
			container["envFrom"] = []any{map[string]any{"secretRef": map[string]any{"name": secret}}}
		}
		// Render the drain lifecycle the component declared. Only the platform Go
		// agent ships /ahp-agent --drain; an image that drains on SIGTERM (the
		// Python custom-agent container) must NOT be handed a preStop it cannot
		// execute, which would fail pod shutdown. ValidateRuntime guarantees the
		// value is one of these two before generation runs.
		if m.Runtime.DrainHooks[c.Name] != drainHookSigterm {
			container["lifecycle"] = map[string]any{"preStop": map[string]any{"exec": map[string]any{"command": []string{"/ahp-agent", "--drain"}}}}
		}
		container["startupProbe"].(map[string]any)["failureThreshold"] = 30
		container["livenessProbe"].(map[string]any)["failureThreshold"] = 3
		podSpec := map[string]any{"serviceAccountName": svc, "automountServiceAccountToken": false, "terminationGracePeriodSeconds": 45, "securityContext": map[string]any{"runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532, "seccompProfile": map[string]any{"type": "RuntimeDefault"}}, "containers": []any{container}, "volumes": []any{map[string]any{"name": "tls", "secret": map[string]any{"secretName": m.Runtime.TLSSecret, "defaultMode": 288}}, map[string]any{"name": "tmp", "emptyDir": map[string]any{"sizeLimit": "64Mi"}}}, "topologySpreadConstraints": []any{map[string]any{"maxSkew": 1, "topologyKey": "kubernetes.io/hostname", "whenUnsatisfiable": "ScheduleAnyway", "labelSelector": map[string]any{"matchLabels": selector}}}}
		if len(m.Runtime.ImagePullSecrets) > 0 {
			var secrets []any
			for _, name := range m.Runtime.ImagePullSecrets {
				secrets = append(secrets, map[string]any{"name": name})
			}
			podSpec["imagePullSecrets"] = secrets
		}
		d := objMeta("apps/v1", "Deployment", name, ns, labels)
		d["spec"] = map[string]any{"replicas": replicas, "revisionHistoryLimit": 3, "selector": map[string]any{"matchLabels": selector}, "strategy": map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxUnavailable": 0, "maxSurge": 1}}, "template": map[string]any{"metadata": map[string]any{"labels": labels}, "spec": podSpec}}
		service := objMeta("v1", "Service", name, ns, labels)
		service["spec"] = map[string]any{"selector": selector, "ports": []any{map[string]any{"name": "https", "port": 443, "targetPort": "https"}}}
		out = append(out, d, service)
		if replicas > 1 {
			pdb := objMeta("policy/v1", "PodDisruptionBudget", name, ns, labels)
			pdb["spec"] = map[string]any{"minAvailable": 1, "selector": map[string]any{"matchLabels": selector}}
			out = append(out, pdb)
		}
	}
	return out
}
