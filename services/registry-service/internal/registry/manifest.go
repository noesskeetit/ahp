// Package registry is the AHP artifact registry: it stores immutable agent
// manifests and their versions, tenant installations with the capability
// ceiling that was granted at install time, per-tenant agent configuration,
// and the immutable version pins that running tasks depend on.
//
// The store owns the ahp_registry schema exclusively. Nothing outside this
// package may read or write those tables, and no other service schema is ever
// touched from here.
package registry

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Manifest kinds. An agent exposes components over A2A; a tool does not.
const (
	KindAgent = "agent"
	KindTool  = "tool"
)

// Manifest visibility values. System visibility is reserved for platform
// artifacts, catalog visibility publishes an artifact to every tenant.
const (
	VisibilitySystem  = "system"
	VisibilityTenant  = "tenant"
	VisibilityCatalog = "catalog"
)

// Manifest audience values. Personal artifacts run for one subject and may
// never reach for shared or privileged resources.
const (
	AudienceShared   = "shared"
	AudiencePersonal = "personal"
)

// Manifest classes. Class C is the constrained, unprivileged class; class S is
// the platform class allowed to hold system privileges.
const (
	ClassS = "S"
	ClassC = "C"
)

// MaxDepth bounds the delegation tree a manifest may describe.
const MaxDepth = 16

// Component is one deployable unit of an artifact. Exactly one component of an
// agent must speak A2A; a tool must not speak A2A at all.
type Component struct {
	Image       string `json:"image,omitempty"`
	Name        string `json:"name"`
	A2A         bool   `json:"a2a"`
	MinReplicas int    `json:"min_replicas"`
	MaxReplicas int    `json:"max_replicas"`
}

// StateConfig declares how an artifact uses the platform State API.
//
// Partition is the subject whose namespace holds the artifact's own state.
// Shared lists the additional, already declared named spaces the artifact may
// touch. Compat is the state compatibility identifier a new version must match
// for a task that outlives the drain window to be carried over to it. Drain is
// the drain WINDOW -- a duration such as "7d" (PDF rev 3.2, c. 41: "старая
// версия работает, пока у неё есть незавершённые задачи, но не дольше окна drain
// из манифеста") -- not a hook path; DeleteHook is a bounded hook path, and
// OwnTables marks artifacts that own their tables and therefore must declare it.
type StateConfig struct {
	Partition  string   `json:"partition"`
	Shared     []string `json:"shared,omitempty"`
	Compat     string   `json:"compat,omitempty"`
	Drain      string   `json:"drain,omitempty"`
	DeleteHook string   `json:"delete_hook,omitempty"`
	OwnTables  bool     `json:"own_tables,omitempty"`
}

// maxDrainWindow bounds the window a manifest may ask for. The PDF's example is
// 7d; an unbounded or absurd window would keep old replicas pinned forever, so
// the Registry refuses it instead of honouring it.
const maxDrainWindow = 30 * 24 * time.Hour

// ParseDrainWindow parses the manifest's state.drain value into the window the
// drain must not outlive. It accepts a sequence of non-negative count/unit pairs
// over days, hours, minutes and seconds ("7d", "12h", "1d6h", "45s"), and the
// empty string, which means the manifest declared no window. Anything else --
// including a hook path, which is what this field was misread as -- is refused:
// a window that cannot be parsed cannot be enforced.
func ParseDrainWindow(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	const bad = "state drain must be a drain window duration such as 7d, 12h or 45s"
	var total time.Duration
	for i := 0; i < len(raw); {
		start := i
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			i++
		}
		if i == start || i == len(raw) {
			return 0, invalid(bad)
		}
		n, err := strconv.ParseInt(raw[start:i], 10, 64)
		if err != nil {
			return 0, invalid(bad)
		}
		var unit time.Duration
		switch raw[i] {
		case 'd':
			unit = 24 * time.Hour
		case 'h':
			unit = time.Hour
		case 'm':
			unit = time.Minute
		case 's':
			unit = time.Second
		default:
			return 0, invalid(bad)
		}
		i++
		if n > int64(maxDrainWindow/unit) {
			return 0, invalid("state drain window must not exceed 30d")
		}
		total += time.Duration(n) * unit
		if total < 0 || total > maxDrainWindow {
			return 0, invalid("state drain window must not exceed 30d")
		}
	}
	return total, nil
}

// DrainWindow is the drain window this manifest declares, or zero when it
// declares none. A declared zero ("0s") means no drain is granted: waiting
// tasks are decided the moment the version is replaced.
func (s StateConfig) DrainWindow() (time.Duration, error) { return ParseDrainWindow(s.Drain) }

// Capabilities is the capability set of a manifest. In a manifest it is the
// ceiling an installation may be granted; in an installation it is the granted
// subset. Wildcards are never allowed.
type Capabilities struct {
	Models    []string `json:"models,omitempty"`
	Tools     []string `json:"tools,omitempty"`
	Memory    []string `json:"memory,omitempty"`
	Compute   []string `json:"compute,omitempty"`
	Calls     []string `json:"calls,omitempty"`
	SelfTasks int      `json:"self_tasks"`
}

// Limits bounds the runtime footprint of an artifact. TaskTimeout is a Go
// duration string such as "15m" or "45s".
type Limits struct {
	Depth       int    `json:"depth"`
	ActiveTasks int    `json:"active_tasks"`
	TaskTimeout string `json:"task_timeout"`
}

// Manifest is the declarative description of one artifact version.
type Manifest struct {
	Kind         string       `json:"kind"`
	System       string       `json:"system"`
	Visibility   string       `json:"visibility"`
	Audience     string       `json:"audience"`
	Class        string       `json:"class"`
	Components   []Component  `json:"components"`
	State        StateConfig  `json:"state"`
	Capabilities Capabilities `json:"capabilities"`
	Limits       Limits       `json:"limits"`
}

// Identifier patterns. Identifiers are path-safe and never contain separators
// that could be confused with a path or a capability wildcard.
var (
	systemPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9._:@-]{0,127}$`)
	componentPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._:@-]{0,63}$`)
	spaceNamePattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._:@-]{0,63}$`)
	compatPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@+-]{0,127}$`)
	capabilityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$`)
	digestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	imageRefPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]{0,255}@sha256:[0-9a-f]{64}$`)
	hookPathPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/@-]{0,127}$`)
)

// maxHookPathSegments bounds the depth of a hook path.
const maxHookPathSegments = 8

// ValidateManifest checks a manifest against the platform invariants. Every
// rejection carries INVALID_ARGUMENT so callers can surface the reason without
// inspecting internals.
func ValidateManifest(m Manifest) error {
	switch m.Kind {
	case KindAgent, KindTool:
	default:
		return invalid("manifest kind must be " + KindAgent + " or " + KindTool)
	}
	if !systemPattern.MatchString(m.System) {
		return invalid("manifest system must be a lowercase identifier of at most 128 bytes")
	}
	switch m.Visibility {
	case VisibilitySystem, VisibilityTenant, VisibilityCatalog:
	default:
		return invalid("manifest visibility must be system, tenant or catalog")
	}
	switch m.Audience {
	case AudienceShared, AudiencePersonal:
	default:
		return invalid("manifest audience must be shared or personal")
	}
	switch m.Class {
	case ClassS, ClassC:
	default:
		return invalid(`manifest class must be "S" or "C"`)
	}

	a2a, err := validateComponents(m)
	if err != nil {
		return err
	}
	switch m.Kind {
	case KindAgent:
		if a2a != 1 {
			return invalid("an agent manifest must declare exactly one A2A component")
		}
	case KindTool:
		if a2a != 0 {
			return invalid("a tool manifest must not declare an A2A component")
		}
	}

	if err := validateState(m); err != nil {
		return err
	}
	if err := validateCapabilities(m); err != nil {
		return err
	}
	if err := validateLimits(m.Limits); err != nil {
		return err
	}
	if m.Class == ClassC {
		if err := validateClassC(m); err != nil {
			return err
		}
	}
	return nil
}

// validateComponents checks component naming, uniqueness and replica bounds,
// and returns how many components speak A2A.
func validateComponents(m Manifest) (int, error) {
	if len(m.Components) == 0 {
		return 0, invalid("manifest must declare at least one component")
	}
	if len(m.Components) > 64 {
		return 0, invalid("manifest must declare at most 64 components")
	}
	seen := make(map[string]struct{}, len(m.Components))
	a2a := 0
	for i := range m.Components {
		c := m.Components[i]
		if c.Image != "" && !imageRefPattern.MatchString(c.Image) {
			return 0, invalid("component image must be an immutable repository@sha256 reference")
		}
		if !componentPattern.MatchString(c.Name) {
			return 0, invalid("component name must be a lowercase identifier of at most 64 bytes")
		}
		if _, dup := seen[c.Name]; dup {
			return 0, invalid("component name " + c.Name + " is declared more than once")
		}
		seen[c.Name] = struct{}{}
		if c.MinReplicas < 1 {
			return 0, invalid("component " + c.Name + " min_replicas must be at least 1")
		}
		if c.MaxReplicas < c.MinReplicas {
			return 0, invalid("component " + c.Name + " max_replicas must be at least min_replicas")
		}
		if c.MaxReplicas > 4096 {
			return 0, invalid("component " + c.Name + " max_replicas must be at most 4096")
		}
		if c.A2A {
			a2a++
		}
	}
	return a2a, nil
}

// validateState checks the declared state footprint.
func validateState(m Manifest) error {
	if !spaceNamePattern.MatchString(m.State.Partition) {
		return invalid("state partition must name the subject whose namespace holds the artifact state")
	}
	if len(m.State.Shared) > 64 {
		return invalid("state may declare at most 64 shared spaces")
	}
	seen := make(map[string]struct{}, len(m.State.Shared))
	for _, s := range m.State.Shared {
		if !spaceNamePattern.MatchString(s) {
			return invalid("shared state space must be a lowercase identifier of at most 64 bytes")
		}
		if s == m.State.Partition {
			return invalid("shared state space must not repeat the partition subject")
		}
		if _, dup := seen[s]; dup {
			return invalid("shared state space " + s + " is declared more than once")
		}
		seen[s] = struct{}{}
	}
	if m.State.Compat != "" && !compatPattern.MatchString(m.State.Compat) {
		return invalid("state compat must be a bounded identifier without separators")
	}
	if _, err := ParseDrainWindow(m.State.Drain); err != nil {
		return err
	}
	if err := validateHookPath("state delete hook", m.State.DeleteHook); err != nil {
		return err
	}
	if m.State.OwnTables && m.State.DeleteHook == "" {
		return invalid("state own_tables requires a safe delete hook path")
	}
	return nil
}

// validateHookPath accepts only a bounded, relative, unrooted path made of
// safe segments. It never accepts "." or ".." and never accepts a path that
// could escape the hook root.
func validateHookPath(field, path string) error {
	if path == "" {
		return nil
	}
	if strings.ContainsRune(path, '\\') || strings.ContainsRune(path, 0) || !utf8.ValidString(path) {
		return invalid(field + " must be a plain relative path")
	}
	if strings.HasPrefix(path, "/") {
		return invalid(field + " must be relative to the hook root")
	}
	if !hookPathPattern.MatchString(path) {
		return invalid(field + " contains characters outside the safe path alphabet")
	}
	segments := strings.Split(path, "/")
	if len(segments) > maxHookPathSegments {
		return invalid(field + " is nested too deeply")
	}
	for _, seg := range segments {
		if seg == "" || seg == "." || seg == ".." {
			return invalid(field + " must not contain empty, dot or dot-dot segments")
		}
	}
	return nil
}

// validateCapabilities checks every capability list for shape, uniqueness and
// the absence of wildcards, and bounds the self-task allowance.
func validateCapabilities(m Manifest) error {
	if m.Capabilities.SelfTasks < 0 {
		return invalid("capabilities self_tasks must not be negative")
	}
	if m.Capabilities.SelfTasks > 1024 {
		return invalid("capabilities self_tasks must be at most 1024")
	}
	for _, list := range []struct {
		field  string
		values []string
	}{
		{"models", m.Capabilities.Models},
		{"tools", m.Capabilities.Tools},
		{"memory", m.Capabilities.Memory},
		{"compute", m.Capabilities.Compute},
		{"calls", m.Capabilities.Calls},
	} {
		if len(list.values) > 256 {
			return invalid("capabilities " + list.field + " must hold at most 256 entries")
		}
		seen := make(map[string]struct{}, len(list.values))
		for _, v := range list.values {
			if !capabilityPattern.MatchString(v) {
				return invalid("capability " + list.field + " entry " + strconv.Quote(v) + " is not a valid capability name")
			}
			if _, dup := seen[v]; dup {
				return invalid("capability " + list.field + " entry " + v + " is declared more than once")
			}
			seen[v] = struct{}{}
		}
	}
	return nil
}

// validateLimits checks the numeric and duration bounds of a limits block.
func validateLimits(l Limits) error {
	if l.Depth < 1 {
		return invalid("limits depth must be at least 1")
	}
	if l.Depth > MaxDepth {
		return invalid("limits depth must not exceed " + strconv.Itoa(MaxDepth))
	}
	if l.ActiveTasks < 1 {
		return invalid("limits active_tasks must be at least 1")
	}
	if l.ActiveTasks > 1_000_000 {
		return invalid("limits active_tasks is too large")
	}
	d, err := ParseTaskTimeout(l.TaskTimeout)
	if err != nil {
		return err
	}
	if d > 24*time.Hour {
		return invalid("limits task_timeout must not exceed 24h")
	}
	return nil
}

// ParseTaskTimeout parses a manifest task timeout. Bare numbers are rejected:
// a timeout must state its unit.
func ParseTaskTimeout(s string) (time.Duration, error) {
	if s == "" {
		return 0, invalid("limits task_timeout must be a duration such as 30s or 15m")
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, invalid("limits task_timeout is not a valid duration")
	}
	if d <= 0 {
		return 0, invalid("limits task_timeout must be positive")
	}
	return d, nil
}

// validateClassC enforces the constrained-class rules: no personal compute, no
// system privileges and no system visibility.
func validateClassC(m Manifest) error {
	for _, c := range m.Capabilities.Compute {
		if isPersonalCapability(c) {
			return invalid("class C must not request personal compute capability " + c)
		}
	}
	for _, list := range [][]string{
		m.Capabilities.Models,
		m.Capabilities.Tools,
		m.Capabilities.Memory,
		m.Capabilities.Compute,
		m.Capabilities.Calls,
	} {
		for _, c := range list {
			if isSystemCapability(c) {
				return invalid("class C must not request system privilege " + c)
			}
		}
	}
	if m.Visibility == VisibilitySystem {
		return invalid("class C must not be published with system visibility")
	}
	return nil
}

// isPersonalCapability reports whether a capability reaches a personal
// (subject-owned) resource class.
func isPersonalCapability(c string) bool {
	return hasSegment(c, "personal")
}

// isSystemCapability reports whether a capability carries system privilege.
func isSystemCapability(c string) bool {
	return hasSegment(c, "system") || hasSegment(c, "platform")
}

// hasSegment reports whether word is one of the '/'-separated segments of a
// capability name. Segment matching keeps "personalization" unprivileged.
func hasSegment(capability, word string) bool {
	for _, seg := range strings.Split(capability, "/") {
		if seg == word {
			return true
		}
	}
	return false
}

// ValidateImageDigest accepts only an immutable registry reference pinned to a
// sha256 content digest. Mutable tags are always rejected.
func ValidateImageDigest(ref string) error {
	if !imageRefPattern.MatchString(ref) {
		return invalid("image reference must be pinned to an immutable sha256 digest")
	}
	sum := ref[strings.Index(ref, "@sha256:")+len("@sha256:"):]
	if _, err := hex.DecodeString(sum); err != nil {
		return invalid("image digest is not a valid sha256 hex digest")
	}
	return nil
}

// ValidateVersionDigest accepts a canonical "sha256:<64 lowercase hex>" digest.
func ValidateVersionDigest(digest string) error {
	if !digestPattern.MatchString(digest) {
		return invalid("version digest must be sha256:<64 lowercase hex characters>")
	}
	return nil
}

// invalid builds the INVALID_ARGUMENT error every validation path returns.
func invalid(msg string) error {
	return contracts.NewError(contracts.ErrInvalid, msg)
}

// manifestPayload is the JSON shape accepted by the registry HTTP layer.
type manifestPayload struct {
	Manifest Manifest `json:"manifest"`
}

// decodeManifestJSON decodes a manifest document. Either a bare manifest
// object or {"manifest":{...}} is accepted; unknown fields are rejected so a
// typo cannot silently drop a safety-relevant declaration.
func decodeManifestJSON(data []byte) (Manifest, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return Manifest{}, invalid("manifest document is empty")
	}
	if !json.Valid([]byte(trimmed)) {
		return Manifest{}, invalid("manifest document is not valid JSON")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &fields); err != nil {
		return Manifest{}, invalid("manifest document must be a JSON object")
	}
	// The wrapper key is reserved and never appears in a bare manifest, so it
	// is peeled before the unknown-field check of the inner document.
	if inner, ok := fields["manifest"]; ok {
		if len(fields) != 1 {
			return Manifest{}, invalid(`manifest document must not mix "manifest" with other fields`)
		}
		return decodeManifestJSON(inner)
	}
	if err := requireKnownFields(fields, "kind", "system", "visibility", "audience", "class",
		"components", "state", "capabilities", "limits"); err != nil {
		return Manifest{}, err
	}
	var m Manifest
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, invalid("manifest document is not valid JSON: " + err.Error())
	}
	return m, nil
}

// requireKnownFields rejects document fields the manifest does not define.
func requireKnownFields(fields map[string]json.RawMessage, known ...string) error {
	for name := range fields {
		found := false
		for _, k := range known {
			if k == name {
				found = true
				break
			}
		}
		if !found {
			return invalid("manifest document has unknown field " + strconv.Quote(name))
		}
	}
	return nil
}

// String renders a manifest summary safe for logs: identifiers only, never
// document content.
func (m Manifest) String() string {
	return fmt.Sprintf("Manifest{kind=%s system=%s visibility=%s audience=%s class=%s components=%d}",
		m.Kind, m.System, m.Visibility, m.Audience, m.Class, len(m.Components))
}
