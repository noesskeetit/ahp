// Package main implements the AHP deploy controller (PDF §6.6, R6.6-01/02/03).
//
// It reads a deployment manifest, validates it against the schema fixed by the
// PDF, and — only if it is fully valid — generates the Kubernetes objects the
// deploy controller is responsible for creating: Namespace, ServiceAccount,
// NetworkPolicy, the State API partition/scope grant, and RBAC grants.
//
// Generation is the L2-testable part of R6.6-02 ("генерация"); applying the
// objects to a real cluster is the L4 part (G-K8S-MTLS-STRICT) and is NOT
// claimed here. Validation is atomic: if the manifest is invalid the
// controller refuses the deploy and produces NO objects (SC-delivery-01
// "при отказе ни одного частичного объекта не осталось").
package main

import (
	"fmt"
	"strings"
)

// Manifest mirrors the PDF §6.6 deployment manifest. Field set == R6.6-01.
type Manifest struct {
	Runtime    Runtime     `json:"runtime"`
	Kind       string      `json:"kind"`       // agent | tool
	System     string      `json:"system"`     // agent system, shared release
	Visibility string      `json:"visibility"` // system | tenant | catalog
	Audience   string      `json:"audience"`   // shared | personal
	Components []Component `json:"components"`
	State      State       `json:"state"`
	Capability Capability  `json:"capabilities"`
	Limits     Limits      `json:"limits"`
}

type Component struct {
	Name     string    `json:"name"`
	A2A      bool      `json:"a2a"`
	Replicas *Replicas `json:"replicas,omitempty"`
}

type Replicas struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

type State struct {
	Partition  string   `json:"partition"` // subject | tenant
	Shared     []string `json:"shared"`
	Compat     string   `json:"compat"` // forward | "" (none)
	Drain      string   `json:"drain"`
	OwnTables  bool     `json:"own_tables"`
	DeleteHook string   `json:"delete_hook"`
}

type Capability struct {
	Models    []string  `json:"models"`
	Tools     []string  `json:"tools"`
	Memory    []string  `json:"memory"`
	Compute   []string  `json:"compute"`
	Calls     []string  `json:"calls"`
	SelfTasks SelfTasks `json:"self_tasks"`
}

type SelfTasks struct {
	MaxFanout int `json:"max_fanout"`
}

type Limits struct {
	Depth       int    `json:"depth"`
	TaskTimeout string `json:"task_timeout"`
	ActiveTasks int    `json:"active_tasks"`
}

var (
	validKind       = map[string]bool{"agent": true, "tool": true}
	validVisibility = map[string]bool{"system": true, "tenant": true, "catalog": true}
	validAudience   = map[string]bool{"shared": true, "personal": true}
	validPartition  = map[string]bool{"subject": true, "tenant": true}
)

// Identifier check mirrors the platform-wide validIdentifier (state/identity):
// non-empty and only [A-Za-z0-9._:/@-]. Kept local so the deploy controller has
// no import coupling to other owners' packages.
func validIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '.' || r == '_' || r == ':' || r == '/' || r == '@' || r == '-'
		if !ok {
			return false
		}
	}
	return true
}

// Validate returns every schema violation found. A non-empty slice means the
// deploy MUST be refused (no partial objects generated).
func (m *Manifest) Validate() []string {
	var errs []string
	add := func(format string, a ...any) { errs = append(errs, fmt.Sprintf(format, a...)) }

	if !validIdentifier(m.System) {
		add("system: must be a non-empty identifier")
	}
	if !validKind[m.Kind] {
		add("kind: must be one of agent|tool, got %q", m.Kind)
	}
	// R5.6-01 / SC-delivery-03: invalid visibility -> refuse.
	if !validVisibility[m.Visibility] {
		add("visibility: must be one of system|tenant|catalog, got %q", m.Visibility)
	}
	if !validAudience[m.Audience] {
		add("audience: must be one of shared|personal, got %q", m.Audience)
	}

	// components: at least one, and a2a is the SINGLE entry point (R6.6-01).
	if len(m.Components) == 0 {
		add("components: at least one component is required")
	}
	entries := 0
	seen := map[string]bool{}
	for i, c := range m.Components {
		if !validIdentifier(c.Name) {
			add("components[%d].name: must be a non-empty identifier", i)
		} else if seen[c.Name] {
			add("components[%d].name: duplicate %q", i, c.Name)
		} else {
			seen[c.Name] = true
		}
		if c.A2A {
			entries++
		}
		if c.Replicas != nil {
			if c.Replicas.Min < 0 {
				add("components[%d].replicas.min: must be >= 0", i)
			}
			if c.Replicas.Max < c.Replicas.Min {
				add("components[%d].replicas.max: must be >= min", i)
			}
		}
	}
	if entries != 1 {
		add("components: exactly one component must set a2a:true (single A2A entry point), got %d", entries)
	}

	// state block.
	if !validPartition[m.State.Partition] {
		add("state.partition: must be one of subject|tenant, got %q", m.State.Partition)
	}
	for _, ns := range m.State.Shared {
		if !validIdentifier(ns) {
			add("state.shared: %q is not a valid namespace identifier", ns)
		}
	}
	if m.State.Drain == "" {
		add("state.drain: is required (drain window, e.g. 7d)")
	}
	// delete_hook is required ONLY when own_tables is set (R6.8 / SC-delivery-01).
	if m.State.OwnTables && m.State.DeleteHook == "" {
		add("state.delete_hook: is required when state.own_tables is true")
	}
	if !m.State.OwnTables && m.State.DeleteHook != "" {
		add("state.delete_hook: must be empty when state.own_tables is false")
	}

	// capabilities ceiling.
	if m.Capability.SelfTasks.MaxFanout <= 0 {
		add("capabilities.self_tasks.max_fanout: must be > 0, got %d", m.Capability.SelfTasks.MaxFanout)
	}
	for _, c := range m.Capability.Calls {
		if !validIdentifier(c) {
			add("capabilities.calls: %q is not a valid identifier", c)
		}
	}

	// limits.
	if m.Limits.Depth <= 0 {
		add("limits.depth: must be > 0, got %d", m.Limits.Depth)
	}
	if m.Limits.TaskTimeout == "" {
		add("limits.task_timeout: is required")
	}
	if m.Limits.ActiveTasks <= 0 {
		add("limits.active_tasks: must be > 0, got %d", m.Limits.ActiveTasks)
	}

	return errs
}

// namespace is the deterministic namespace name for this deploy.
func (m *Manifest) namespace() string {
	return "ahp-" + strings.ToLower(m.System)
}

// serviceName is the ServiceAccount / app label name.
func (m *Manifest) serviceName() string {
	return strings.ToLower(m.System)
}
