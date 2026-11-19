package routing

import (
	"log/slog"
	"os"
	"strings"
	"sync/atomic"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Request is everything routing looks at. It deliberately carries no prompt and
// no payload: a routing decision has to be describable, loggable and testable
// without touching content (PDF :347/:277 -- traces and metrics carry no
// content), and a field that holds content would end up in a journal line
// someday.
type Request struct {
	// Labels are the labels the data being sent carries. Empty means unlabelled,
	// not " unrestricted".
	Labels labels.Set
	// Class is the execution class the request runs under (PDF :1532/:1542 --
	// priority classes and quotas per tenant and class). Empty means the
	// deployment did not state one.
	Class string
	// Model is what the caller asked for. A route may honour it or override it;
	// what no route may do is send the request to a provider that does not
	// advertise the model it ends up choosing.
	Model string
}

// Route is the decision, with the rule that produced it. The rule name is part
// of the answer because "why did this go there" is the first question an operator
// asks, and the route alone cannot answer it.
type Route struct {
	Provider string
	Model    string
	RuleName string
	Class    string
}

// loaded is the immutable pair of a table and the bytes it came from. Swapping
// the pointer is the whole reload policy: a request either sees the old table
// whole or the new table whole, never a half-applied one.
type loaded struct {
	table   *Table
	digest  string
	path    string
	modTime string
	size    int64
}

// Router serves routing decisions from a file, and can be reloaded.
type Router struct {
	path string
	log  *slog.Logger
	cur  atomic.Pointer[loaded]
}

// ErrNoPolicy is returned when a table is loaded but states no routes. Callers
// treat it as "this deployment did not adopt routing" and keep prior behaviour;
// it is never treated as "any provider will do".
var ErrNoPolicy = contracts.NewError(contracts.ErrInvalid, "routing: no routes configured")

// New loads the table at path and fails closed if it is invalid: a deployment
// that starts is a deployment that routes as configured.
func New(path string, log *slog.Logger) (*Router, error) {
	r := &Router{path: path, log: log}
	ld, err := read(path)
	if err != nil {
		return nil, err
	}
	r.cur.Store(ld)
	r.logInfo("model: label-aware routing mounted", ld)
	return r, nil
}

func (r *Router) logInfo(msg string, ld *loaded) {
	if r.log == nil {
		return
	}
	r.log.Info(msg,
		slog.String("provider_config", ld.path),
		slog.String("config_digest", ld.digest),
		slog.Int("providers", len(ld.table.Providers)),
		slog.Int("routes", len(ld.table.Routes)),
		slog.Int("config_version", ld.table.Version),
	)
}

// Reload re-reads the file when it changed. An invalid new file never replaces
// the working one: the gateway keeps routing by the last known-good table and
// reports the failure, instead of dropping every tenant's traffic because of a
// typo typed at 03:00.
func (r *Router) Reload() (bool, error) {
	prev := r.cur.Load()
	next, err := read(r.path)
	if err != nil {
		return false, err
	}
	if prev != nil && prev.digest == next.digest {
		return false, nil
	}
	r.cur.Store(next)
	r.logInfo("model: provider config reloaded", next)
	return true, nil
}

// Digest reports which bytes this router is deciding from.
func (r *Router) Digest() string {
	if ld := r.cur.Load(); ld != nil {
		return ld.digest
	}
	return ""
}

// Table reports the live table.
func (r *Router) Table() *Table {
	if ld := r.cur.Load(); ld != nil {
		return ld.table
	}
	return nil
}

func read(path string) (*loaded, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "routing: read provider config: "+path)
	}
	tbl, err := Parse(raw)
	if err != nil {
		return nil, err
	}
	st, serr := os.Stat(path)
	mod, size := "", int64(0)
	if serr == nil {
		mod, size = st.ModTime().UTC().Format("2006-01-02T15:04:05Z"), st.Size()
	}
	return &loaded{table: tbl, digest: Digest(raw), path: path, modTime: mod, size: size}, nil
}

// Resolve picks the provider and model for one request.
//
// Evaluation order is the whole policy: routes in file order, first match wins,
// and a match is only usable if the labels permit that destination. That order
// matters -- policy may choose among providers the labels allow, and may never
// overrule the label gate, so a rule cannot be written that exports
// secret-tainted data.
func (r *Router) Resolve(scope contracts.Scope, req Request) (Route, error) {
	ld := r.cur.Load()
	if ld == nil {
		return Route{}, ErrNoPolicy
	}
	if len(ld.table.Routes) == 0 {
		return Route{}, ErrNoPolicy
	}
	byName := ld.table.ByName()
	for _, rule := range ld.table.Routes {
		if !rule.When.matches(scope, req) {
			continue
		}
		provider, ok := byName[rule.Then.Provider]
		if !ok {
			// Unreachable for a table that passed Validate; kept because a
			// nil-map lookup that silently routed nowhere would be the worst
			// possible failure mode for a gate.
			return Route{}, contracts.NewError(contracts.ErrInvalid,
				"routing: route "+rule.Name+" names provider "+rule.Then.Provider+" which is not configured")
		}
		// The label gate, evaluated on the chosen destination. This is the line
		// that makes the routing label-aware.
		audience := provider.Audience // Local value: concurrent callers never mutate the loaded table.
		if provider.SubjectFromScope {
			if err := scope.Validate(); err != nil {
				return Route{}, err
			}
			audience.Subject = scope.Subject
		}
		if err := labels.CheckWrite(req.Labels, audience); err != nil {
			return Route{}, contracts.NewReasoned(contracts.ErrForbidden, ReasonLabelGate,
				"routing: provider "+provider.Name+" may not receive data with these labels: "+err.Error())
		}
		model := rule.Then.Model
		if model == "" {
			model = req.Model
		}
		if model == "" && len(provider.Models) > 0 {
			model = provider.Models[0]
		}
		if !offers(&provider, model) {
			return Route{}, contracts.NewReasoned(contracts.ErrInvalid, ReasonModelNotOffered,
				"routing: provider "+provider.Name+" does not advertise model "+model+" (route "+rule.Name+")")
		}
		return Route{Provider: provider.Name, Model: model, RuleName: rule.Name, Class: provider.Class}, nil
	}
	// Configured, and nothing accepted it: a refusal, naming the reason, never a
	// fallback. An operator who later adds a route sees this refusal disappear.
	return Route{}, contracts.NewReasoned(contracts.ErrInvalid, ReasonNoRoute,
		"routing: no route in "+ld.digest+" accepts this request (tenant, class, labels and requested model as a set)")
}

// matches evaluates one rule's left-hand side. Every dimension omitted matches
// anything; the tenant list accepts the literal "*" as a wildcard entry.
func (m Match) matches(scope contracts.Scope, req Request) bool {
	if !matchOne(m.Tenants, scope.Tenant, true) {
		return false
	}
	if len(m.Classes) > 0 {
		// A request with no stated class can only match a rule that also did not
		// constrain class: a class-specific route must not pick up unclassified
		// traffic by accident.
		if req.Class == "" || !matchOne(m.Classes, req.Class, false) {
			return false
		}
	}
	if len(m.TaintsAny) > 0 {
		hit := false
		for _, want := range m.TaintsAny {
			for _, have := range req.Labels.Taints {
				if want == have {
					hit = true
				}
			}
		}
		if !hit {
			return false
		}
	}
	for _, banned := range m.TaintsNone {
		for _, have := range req.Labels.Taints {
			if banned == have {
				return false
			}
		}
	}
	if len(m.ModelPrefixes) > 0 {
		hit := false
		for _, p := range m.ModelPrefixes {
			if req.Model != "" && strings.HasPrefix(req.Model, p) {
				hit = true
			}
		}
		if !hit {
			return false
		}
	}
	return true
}

func matchOne(list []string, value string, allowWildcard bool) bool {
	if len(list) == 0 {
		return true
	}
	for _, v := range list {
		if v == value {
			return true
		}
		if allowWildcard && v == "*" {
			return true
		}
	}
	return false
}
