package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Skill is a loadable capability module: a named bundle of tool definitions
// plus a handler that executes them. Skills let a deployment assemble its
// tool surface from reusable, independently testable units rather than one
// monolithic executor. A skill may restrict itself to a subset of scopes via
// Visible.
type Skill struct {
	Name        string
	Description string
	// Visible reports whether the skill applies to a request scope. A nil
	// Visible makes the skill available to every scope.
	Visible func(scope contracts.Scope) bool
	// Tools are the tool definitions this skill contributes. Names must be
	// globally unique across all registered skills.
	Tools []ToolDef
	// Execute runs one of this skill's tools.
	Execute func(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error)
}

// VisibleFor reports whether the skill applies to the given scope.
func (s *Skill) VisibleFor(scope contracts.Scope) bool {
	if s.Visible == nil {
		return true
	}
	return s.Visible(scope)
}

// SkillRegistry holds installed skills and routes tool calls to the skill
// that owns each tool name. It implements ToolExecutor so it can back a
// ToolRegistry directly, letting the Core loop invoke skill tools without
// knowing about skills.
type SkillRegistry struct {
	mu     sync.RWMutex
	skills map[string]*Skill
	owners map[string]string // tool name -> skill name
}

// NewSkillRegistry creates an empty skill registry.
func NewSkillRegistry() *SkillRegistry {
	return &SkillRegistry{
		skills: make(map[string]*Skill),
		owners: make(map[string]string),
	}
}

// Register installs a skill. It fails if the skill name or any of its tool
// names collide with an already-installed skill, keeping the tool surface
// unambiguous.
func (r *SkillRegistry) Register(s *Skill) error {
	if s == nil || s.Name == "" {
		return fmt.Errorf("skill: name required")
	}
	if s.Execute == nil {
		return fmt.Errorf("skill %s: handler required", s.Name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.skills[s.Name]; ok {
		return fmt.Errorf("skill %s: already registered", s.Name)
	}
	for _, t := range s.Tools {
		if owner, ok := r.owners[t.Name]; ok {
			return fmt.Errorf("skill %s: tool %s already owned by %s", s.Name, t.Name, owner)
		}
	}
	r.skills[s.Name] = s
	for _, t := range s.Tools {
		r.owners[t.Name] = s.Name
	}
	return nil
}

// Unregister removes a skill and its tool ownership.
func (r *SkillRegistry) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.skills[name]
	if !ok {
		return
	}
	for _, t := range s.Tools {
		delete(r.owners, t.Name)
	}
	delete(r.skills, name)
}

// Names returns the installed skill names in sorted order.
func (r *SkillRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.skills))
	for n := range r.skills {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Visible returns the skills applicable to a scope, sorted by name.
func (r *SkillRegistry) Visible(scope contracts.Scope) []*Skill {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Skill, 0, len(r.skills))
	for _, s := range r.skills {
		if s.VisibleFor(scope) {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ToolDefs returns the tool definitions contributed by skills visible to the
// scope, sorted by tool name.
func (r *SkillRegistry) ToolDefs(scope contracts.Scope) []ToolDef {
	var defs []ToolDef
	for _, s := range r.Visible(scope) {
		defs = append(defs, s.Tools...)
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })
	return defs
}

// Execute implements ToolExecutor, routing a call to the owning skill.
func (r *SkillRegistry) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	r.mu.RLock()
	ownerName, ok := r.owners[name]
	var skill *Skill
	if ok {
		skill = r.skills[ownerName]
	}
	r.mu.RUnlock()
	if !ok || skill == nil {
		return nil, fmt.Errorf("skill: no handler for tool %s", name)
	}
	return skill.Execute(ctx, name, args)
}

// BindToolRegistry wires the skill registry into a ToolRegistry as its
// executor and registers every visible tool for the default (all) scope. The
// Core loop still filters which tools it advertises per-request via DefsFor.
func (r *SkillRegistry) BindToolRegistry() *ToolRegistry {
	reg := NewToolRegistry(r)
	for _, d := range r.ToolDefs(contracts.Scope{}) {
		reg.Register(d)
	}
	return reg
}

// Bind registers the tools visible to the given scope into reg. Combined with
// the registry executor (r) this gives per-request tool exposure while a
// single dispatcher handles execution.
func (r *SkillRegistry) Bind(reg *ToolRegistry, scope contracts.Scope) {
	for _, d := range r.ToolDefs(scope) {
		reg.Register(d)
	}
}
