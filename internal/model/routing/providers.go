// Package routing decides which provider and which model serve one generation,
// from a file-sourced provider table, under the platform's own label policy.
//
// PDF rev3.2 :1620-1630 gives the Model Gateway "маршруты с учётом меток" --
// routes with label awareness. The platform already has exactly one label
// vocabulary (pkg/labels: subject / workspace visibility plus taints) and
// exactly one write gate for it (labels.CheckWrite), which answers "may data
// carrying these labels be written to that destination". A model provider IS
// such a destination: it is a sink outside this plane, usually external. Before
// this package the model plane contained no reference to labels at all, so a
// request carrying a `secret` taint and a request carrying none picked the same
// provider -- which is not routing, it is blindness.
//
// Two rules shape everything below.
//
//   - Labels are a gate, not a hint. A route may pick a provider the labels
//     allow; it may never waive the label policy. `secret`-tainted data is not
//     exportable, so no tenant policy can route it out; a rule that tries gets
//     refused, and the refusal names the reason.
//   - Absence of configuration is not absence of limits, and it is not a licence
//     to invent a route either. A deployment that configures no table keeps
//     exactly the behaviour it has today (one provider from the environment);
//     a deployment that configures a table with no matching route is refused,
//     never quietly sent to whichever provider happens to be mounted.
package routing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Refusal reasons. Stable strings: an operator scripts on them, and the whole
// reason a cross-plane refusal stayed unfound for two legs was refusals that
// named nothing.
const (
	// ReasonNoRoute: a table is configured and nothing in it accepts this
	// request. Not "fall back to the default provider".
	ReasonNoRoute = "no_route_for_labels"
	// ReasonLabelGate: the candidate provider is a destination these labels may
	// not write to (labels.CheckWrite refused).
	ReasonLabelGate = "labels_forbid_provider"
	// ReasonModelNotOffered: the chosen provider does not advertise the model the
	// request asked for. A route cannot conjure a model into existence.
	ReasonModelNotOffered = "model_not_offered_by_provider"
	// ReasonClassUnknown: the request states an execution class no provider serves.
	ReasonClassUnknown = "class_not_served"
)

// A secret REFERENCE names where a credential lives -- an environment variable,
// or a KV path inside the secret store -- and never the credential itself. Both
// accepted shapes are strict, and strictness is the guard: a pasted high-entropy
// key matches neither, so a credential typed into a committed file cannot load.
var (
	envRefPattern  = regexp.MustCompile(`^[A-Z][A-Z0-9_]{2,64}$`)
	kvPathPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]{2,127}$`)
	secretPrefixes = []string{"sk-", "hvs.", "vs.", "eyJ", "Bearer ", "://"}
)

// looksLikeCredential reports whether ref is a value rather than a reference.
func looksLikeCredential(ref string) bool {
	for _, p := range secretPrefixes {
		if strings.Contains(ref, p) {
			return true
		}
	}
	return false
}

// idPattern bounds names to something an operator can grep for.
var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.:@-]{0,63}$`)

// Target is the routing decision: which provider, optionally with a model
// override (a policy may send a tenant to a smaller model without the caller
// asking for it).
type Target struct {
	Provider string `json:"provider"`
	Model    string `json:"model,omitempty"`
}

// Match is the left-hand side of a route. Every dimension is optional, and the
// semantics of an omitted dimension is "any". An empty Match matches everything,
// which is how a default route is written -- but only where the operator put it,
// because routes are evaluated in file order and the first match wins.
type Match struct {
	Tenants       []string `json:"tenants,omitempty"`
	Classes       []string `json:"classes,omitempty"`
	TaintsAny     []string `json:"taints_any,omitempty"`
	TaintsNone    []string `json:"taints_none,omitempty"`
	ModelPrefixes []string `json:"model_prefixes,omitempty"`
}

// Rule is one route.
type Rule struct {
	Name string `json:"name"`
	When Match  `json:"when"`
	Then Target `json:"then"`
}

// Provider is one upstream the gateway may route to. It carries no credential
// and no host: hosts and keys are runtime material resolved per deployment, and
// keeping them out of this file is what makes the file safe to commit and to
// diff.
type Provider struct {
	Name  string `json:"name"`
	Class string `json:"class,omitempty"`
	// SubjectFromScope explicitly trusts this provider with private data from
	// the named route tenants. The audience subject is bound per request to the
	// VERIFIED scope; source labels and AllowExternal are never relaxed.
	SubjectFromScope bool `json:"subject_from_scope,omitempty"`
	// Audience is the destination this provider represents, in the platform's
	// own label vocabulary. A public upstream is labels.KindExternal.
	Audience  labels.Audience `json:"audience"`
	SecretRef string          `json:"secret_ref"`
	Models    []string        `json:"models"`
	// Quota is the operator's declaration of what this COPY may spend per window.
	// It is declared here, in the same committed file that names the copy, for one
	// reason: a limit that lives only in somebody's psql history is invisible to
	// the person reading the routing table, and "we assumed the default covers the
	// second copy" is precisely how a copy ends up unbounded. Admission still
	// charges ahp_model.provider_quotas (the durable counters need a database);
	// this is the declaration that scripts/quotactl -action sync publishes, so the
	// file and the gate cannot drift silently.
	Quota *QuotaDecl `json:"quota,omitempty"`
}

// QuotaDecl is one copy's declared bound. Both dimensions must be positive: in
// this plane 0 means "not bounded", and a committed 0 would be the silent
// unbounded state this whole feature exists to remove.
type QuotaDecl struct {
	RPM           int64 `json:"rpm"`
	TPM           int64 `json:"tpm"`
	WindowSeconds int   `json:"window_seconds,omitempty"`
}

// MaxQuotaWindowSeconds bounds a declared window, matching the quota plane's own
// column check so a config cannot declare a window the database would reject.
const MaxQuotaWindowSeconds = 3600

func fmtI(v int) string     { return strconv.Itoa(v) }
func fmtI64(v int64) string { return strconv.FormatInt(v, 10) }

// DeclaredQuota is one copy's declared bound, with the window defaulted. Consumed
// by scripts/quotactl -action sync, which publishes these numbers into
// ahp_model.provider_quotas; nothing here enforces them at request time.
type DeclaredQuota struct {
	Provider      string
	RPM           int64
	TPM           int64
	WindowSeconds int
}

// DeclaredQuotas lists every copy that declares a bound, in file order. A copy
// with no quota block is absent, and absence is an audit finding (quotactl -action
// audit), not an implicit unlimited pass.
func (t *Table) DeclaredQuotas() []DeclaredQuota {
	var out []DeclaredQuota
	for _, p := range t.Providers {
		if p.Quota == nil {
			continue
		}
		window := p.Quota.WindowSeconds
		if window <= 0 {
			window = 60
		}
		out = append(out, DeclaredQuota{Provider: p.Name, RPM: p.Quota.RPM, TPM: p.Quota.TPM, WindowSeconds: window})
	}
	return out
}

// Table is the whole provider configuration: what exists, and who may be sent
// where.
type Table struct {
	Version   int        `json:"version"`
	Providers []Provider `json:"providers"`
	Routes    []Rule     `json:"routes"`
}

// strictDecoder rejects unknown keys. A typo in a committed config file that
// silently parses is worse than one that fails: the operator believes a limit or
// a route exists.
func strictDecoder(raw []byte) *json.Decoder {
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	return d
}

// Parse decodes and validates a provider table.
func Parse(raw []byte) (*Table, error) {
	var t Table
	if err := strictDecoder(raw).Decode(&t); err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "routing: provider config: "+err.Error())
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return &t, nil
}

// LoadFile reads and validates the provider table from path.
func LoadFile(path string) (*Table, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "routing: read provider config: "+path)
	}
	return Parse(raw)
}

// Digest is the identity of the exact bytes that were loaded, so an operator can
// tell which file a running gateway is actually routing by.
func Digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:16]
}

// Validate checks the table for the mistakes that turn into silent
// misrouting: unknown destinations, duplicate identities, a credential pasted
// into a reference field, an empty provider, a route that can never match.
func (t *Table) Validate() error {
	if t.Version == 0 {
		return contracts.NewError(contracts.ErrInvalid, "routing: provider config needs a version")
	}
	if len(t.Providers) == 0 {
		return contracts.NewError(contracts.ErrInvalid, "routing: provider config lists no providers")
	}
	byName := make(map[string]*Provider, len(t.Providers))
	for i := range t.Providers {
		p := &t.Providers[i]
		if !idPattern.MatchString(p.Name) {
			return contracts.NewError(contracts.ErrInvalid,
				fmt.Sprintf("routing: provider name %q is not a usable identifier", p.Name))
		}
		if _, dup := byName[p.Name]; dup {
			return contracts.NewError(contracts.ErrInvalid, "routing: duplicate provider "+p.Name)
		}
		if len(p.Models) == 0 {
			return contracts.NewError(contracts.ErrInvalid,
				"routing: provider "+p.Name+" advertises no model, so no route to it can ever be satisfied")
		}
		for _, m := range p.Models {
			if strings.TrimSpace(m) == "" {
				return contracts.NewError(contracts.ErrInvalid, "routing: provider "+p.Name+" has an empty model entry")
			}
		}
		if looksLikeCredential(p.SecretRef) || (!envRefPattern.MatchString(p.SecretRef) && !kvPathPattern.MatchString(p.SecretRef)) {
			return contracts.NewError(contracts.ErrInvalid,
				"routing: provider "+p.Name+" secret_ref must be a reference -- an environment variable (AHP_QWEN_SECRET_PATH) or a secret-store path (ahp-hive/execution/qwen); a credential value does not belong in a committed file")
		}
		if p.Quota != nil {
			if p.Quota.RPM <= 0 || p.Quota.TPM <= 0 {
				return contracts.NewError(contracts.ErrInvalid,
					"routing: provider "+p.Name+" declares quota with rpm="+fmtI64(p.Quota.RPM)+
						" tpm="+fmtI64(p.Quota.TPM)+"; a declared limit must be positive -- zero means unbounded, and an unbounded copy written into a committed config is the state this gate exists to prevent")
			}
			if p.Quota.WindowSeconds < 0 || p.Quota.WindowSeconds > MaxQuotaWindowSeconds {
				return contracts.NewError(contracts.ErrInvalid,
					"routing: provider "+p.Name+" quota window "+fmtI(p.Quota.WindowSeconds)+" is outside 0.."+fmtI(MaxQuotaWindowSeconds))
			}
		}
		// validateAudience is exercised through CheckWrite with an empty set:
		// the cheapest way to reuse the platform's vocabulary check instead of
		// writing a second one that can disagree with it.
		audience := p.Audience
		if p.SubjectFromScope {
			if audience.Kind != labels.KindPrivate || audience.Subject != "" || audience.Workspace != "" {
				return contracts.NewError(contracts.ErrInvalid, "routing: subject_from_scope requires private audience with no fixed subject/workspace")
			}
			audience.Subject = "policy-validation"
		}
		if err := labels.CheckWrite(labels.Set{}, audience); err != nil {
			return contracts.NewError(contracts.ErrInvalid,
				"routing: provider "+p.Name+" audience is not valid: "+err.Error())
		}
		if p.Class != "" && !idPattern.MatchString(p.Class) {
			return contracts.NewError(contracts.ErrInvalid,
				fmt.Sprintf("routing: provider %q class %q is not a usable identifier", p.Name, p.Class))
		}
		byName[p.Name] = p
	}
	seenRule := make(map[string]bool, len(t.Routes))
	for _, r := range t.Routes {
		if !idPattern.MatchString(r.Name) {
			return contracts.NewError(contracts.ErrInvalid,
				fmt.Sprintf("routing: route name %q is not a usable identifier", r.Name))
		}
		if seenRule[r.Name] {
			return contracts.NewError(contracts.ErrInvalid, "routing: duplicate route "+r.Name)
		}
		seenRule[r.Name] = true
		p, ok := byName[r.Then.Provider]
		if !ok {
			return contracts.NewError(contracts.ErrInvalid,
				"routing: route "+r.Name+" names unknown provider "+r.Then.Provider)
		}
		if p.SubjectFromScope {
			if len(r.When.Tenants) == 0 {
				return contracts.NewError(contracts.ErrInvalid, "routing: caller-bound private provider requires explicit tenant IDs")
			}
			for _, tenant := range r.When.Tenants {
				if tenant == "*" {
					return contracts.NewError(contracts.ErrInvalid, "routing: wildcard tenant cannot trust a caller-bound private provider")
				}
				if err := (contracts.Scope{Tenant: tenant, Deployment: "policy", Subject: "policy"}).Validate(); err != nil {
					return contracts.NewError(contracts.ErrInvalid, "routing: invalid tenant in caller-bound private route")
				}
			}
		}
		if r.Then.Model != "" && !offers(p, r.Then.Model) {
			return contracts.NewError(contracts.ErrInvalid,
				"routing: route "+r.Name+" selects model "+r.Then.Model+" which provider "+p.Name+" does not advertise")
		}
		// A taint the platform does not know would make the route match nothing
		// forever, which is indistinguishable from a typo. Reject it here.
		for _, ta := range append(append([]string{}, r.When.TaintsAny...), r.When.TaintsNone...) {
			if !knownTaint(ta) {
				return contracts.NewError(contracts.ErrInvalid,
					"routing: route "+r.Name+" matches unknown taint "+ta)
			}
		}
	}
	return nil
}

func knownTaint(t string) bool {
	switch t {
	case labels.TaintExternal, labels.TaintUntrustedExternal, labels.TaintSecret:
		return true
	}
	return false
}

func offers(p *Provider, model string) bool {
	for _, m := range p.Models {
		if m == model {
			return true
		}
	}
	return false
}

// ByName indexes the table's providers by name.
func (t *Table) ByName() map[string]Provider {
	out := make(map[string]Provider, len(t.Providers))
	for _, p := range t.Providers {
		out[p.Name] = p
	}
	return out
}
