package gateway

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// WellKnownAgentCardPath is the canonical public location of the A2A Agent
// Card, re-exported from the official transport so callers that mount the
// gateway handler behind a path prefix (the execution role mounts the plane
// under /a2a/) can also route the well-known card path to it. It is untrusted,
// public and served without authentication.
const WellKnownAgentCardPath = a2asrv.WellKnownAgentCardPath

// AgentExtensionURI is the fixed URI of the AHP Agent Card extension
// (PDF rev3.2 section 6.1). A conformant execution-plane card advertises it via
// the standard library mechanism capabilities.extensions[]; the extension is
// required, so a client that does not understand it must not interact blindly.
const AgentExtensionURI = "https://ahp.cloud.ru/extensions/agent/v1"

// AHP extension parameter names.
const (
	ExtParamHistory      = "history"
	ExtParamSteering     = "steering"
	ExtParamRunTimeoutMS = "run_timeout_ms"
)

// historyModes / steeringModes are the closed enumerations the extension must
// declare. owned is the default history owner for LangGraph / Core Agent;
// queue/interrupt/none decide where Space routes a mid-run steering replica.
var (
	historyModes  = map[string]bool{"owned": true, "pushed": true}
	steeringModes = map[string]bool{"queue": true, "interrupt": true, "none": true}
)

// credentialURL matches a scheme whose authority embeds user:pass credentials
// (a DSN or a basic-auth URL). A published Agent Card must never carry one.
var credentialURL = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^/\s@]*:[^/\s@]+@`)

// dbSchemes are service endpoints that are never a public A2A interface. Their
// presence in a card is treated as a leaked DSN / internal path.
var dbSchemes = []string{
	"postgres://", "postgresql://", "mysql://", "mongodb://", "mongodb+srv://",
	"redis://", "redis+tls://", "sqlserver://", "amqp://", "clickhouse://",
}

// LoadAgentCardFile loads the public Agent Card the execution plane will serve
// from a JSON file named by the deployment (an env var such as
// AHP_EXEC_AGENT_CARD_FILE). It is deliberately fail-closed and invents nothing:
//
//   - path empty  -> (nil, nil). The operator chose to publish no card, so the
//     plane behaves exactly as before and serves 404 for the well-known path.
//     No fabricated card is ever returned.
//   - path set    -> the file must exist, decode as an a2a.AgentCard, satisfy the
//     platform contract (name/version/url/skills) and leak nothing (ValidatePublicAgentCard).
//     Any failure is returned as an error so the operator fixes the card rather
//     than the plane publishing a wrong or secret-bearing document.
func LoadAgentCardFile(path, internalPath string) (*a2a.AgentCard, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("gateway: read agent card %q: %w", path, err)
	}
	var card a2a.AgentCard
	if err := json.Unmarshal(raw, &card); err != nil {
		return nil, fmt.Errorf("gateway: decode agent card %q: %w", path, err)
	}
	if err := ValidatePublicAgentCard(&card, internalPath); err != nil {
		return nil, fmt.Errorf("gateway: agent card %q: %w", path, err)
	}
	return &card, nil
}

// ValidatePublicAgentCard enforces the platform Agent Card contract for the
// public face (PDF rev3.2 section 2): a named, versioned service that
// advertises a public A2A interface and at least one skill, and that publishes
// NOTHING private. It rejects the internal face path, credential-bearing URLs
// and database DSNs so a card can never disclose /a2a/internal or a secret.
func ValidatePublicAgentCard(card *a2a.AgentCard, internalPath string) error {
	if card == nil {
		return contracts.NewError(contracts.ErrInvalid, "gateway: agent card is nil")
	}
	if strings.TrimSpace(card.Name) == "" {
		return contracts.NewError(contracts.ErrInvalid, "gateway: agent card name is required")
	}
	if strings.TrimSpace(card.Version) == "" {
		return contracts.NewError(contracts.ErrInvalid, "gateway: agent card version is required")
	}
	if len(card.Skills) == 0 {
		return contracts.NewError(contracts.ErrInvalid, "gateway: agent card must advertise at least one skill")
	}
	for i, s := range card.Skills {
		if strings.TrimSpace(s.ID) == "" || strings.TrimSpace(s.Name) == "" {
			return contracts.NewError(contracts.ErrInvalid,
				fmt.Sprintf("gateway: agent card skill %d needs an id and a name", i))
		}
	}
	pub := false
	for _, in := range card.SupportedInterfaces {
		if in == nil {
			continue
		}
		if strings.TrimSpace(in.URL) == "" {
			continue
		}
		// A declared interface is a promise to the client about how to talk to
		// this URL. The plane speaks exactly one binding/version (the official
		// A2A JSON-RPC transport), so any other claim is a promise the public
		// face cannot keep: reject the card at load time instead of letting a
		// client connect and fail.
		if string(in.ProtocolBinding) != ServedProtocolBinding {
			return contracts.NewError(contracts.ErrInvalid,
				"gateway: agent card interface "+in.URL+" declares protocolBinding "+
					quote(string(in.ProtocolBinding))+", but this plane serves only "+ServedProtocolBinding)
		}
		if string(in.ProtocolVersion) != ServedProtocolVersion {
			return contracts.NewError(contracts.ErrInvalid,
				"gateway: agent card interface "+in.URL+" declares protocolVersion "+
					quote(string(in.ProtocolVersion))+", but this plane serves only "+ServedProtocolVersion)
		}
		pub = true
	}
	if claims := unservedMethodClaims(card); len(claims) > 0 {
		// The v0 names are answered METHOD_NOT_FOUND by this build (the v0
		// compatibility layer is a separate module and is not compiled in). A
		// card naming them tells the client to call a method that does not
		// exist, which is a false claim about the deployment, so it never loads.
		return contracts.NewError(contracts.ErrInvalid,
			"gateway: agent card advertises methods this plane does not serve: "+
				strings.Join(claims, ", ")+"; the served A2A "+ServedProtocolVersion+
				" JSONRPC methods are: "+strings.Join(ServedMethods, ", "))
	}
	if !pub {
		return contracts.NewError(contracts.ErrInvalid,
			"gateway: agent card must advertise at least one public interface url")
	}
	if err := validateAHPExtension(card); err != nil {
		return err
	}
	if err := assertNoLeak(card, internalPath); err != nil {
		return err
	}
	return nil
}

// validateAHPExtension enforces the mandatory AHP Agent Card extension
// (PDF rev3.2 section 6.1): exactly one required extension at AgentExtensionURI
// declaring a closed history mode, a closed steering mode and a positive
// run_timeout_ms. It is fail-closed -- a missing extension, a wrong URI, a
// non-required flag, an out-of-enum mode or a non-positive timeout is an error
// that stops the load; nothing is defaulted in. (message/stream support is the
// standard capabilities.streaming boolean and is not duplicated here.)
func validateAHPExtension(card *a2a.AgentCard) error {
	var found *a2a.AgentExtension
	for i := range card.Capabilities.Extensions {
		ext := &card.Capabilities.Extensions[i]
		if ext.URI != AgentExtensionURI {
			continue
		}
		if found != nil {
			return contracts.NewError(contracts.ErrInvalid,
				"gateway: agent card declares the AHP extension more than once")
		}
		found = ext
	}
	if found == nil {
		return contracts.NewError(contracts.ErrInvalid,
			"gateway: agent card must carry the required AHP extension "+AgentExtensionURI)
	}
	if !found.Required {
		return contracts.NewError(contracts.ErrInvalid,
			"gateway: the AHP extension must be marked required")
	}
	hist, err := extParamString(found.Params, ExtParamHistory)
	if err != nil {
		return err
	}
	if !historyModes[hist] {
		return contracts.NewError(contracts.ErrInvalid,
			"gateway: agent card history must be one of owned|pushed")
	}
	steer, err := extParamString(found.Params, ExtParamSteering)
	if err != nil {
		return err
	}
	if !steeringModes[steer] {
		return contracts.NewError(contracts.ErrInvalid,
			"gateway: agent card steering must be one of queue|interrupt|none")
	}
	timeout, ok := extParamPositiveInt(found.Params, ExtParamRunTimeoutMS)
	if !ok {
		return contracts.NewError(contracts.ErrInvalid,
			"gateway: agent card run_timeout_ms must be a positive integer")
	}
	_ = timeout
	return nil
}

func extParamString(params map[string]any, key string) (string, error) {
	raw, ok := params[key]
	if !ok {
		return "", contracts.NewError(contracts.ErrInvalid,
			"gateway: AHP extension is missing param "+key)
	}
	s, ok := raw.(string)
	if !ok {
		return "", contracts.NewError(contracts.ErrInvalid,
			"gateway: AHP extension param "+key+" must be a string")
	}
	return s, nil
}

// extParamPositiveInt accepts the JSON-number shapes the decoder produces
// (float64 for a card read back from JSON, and integer types when a card is
// built in Go) and requires the value to be a whole number greater than zero.
func extParamPositiveInt(params map[string]any, key string) (int64, bool) {
	raw, ok := params[key]
	if !ok {
		return 0, false
	}
	var n int64
	switch v := raw.(type) {
	case float64:
		if v != float64(int64(v)) {
			return 0, false
		}
		n = int64(v)
	case int:
		n = int64(v)
	case int64:
		n = v
	case json.Number:
		parsed, err := v.Int64()
		if err != nil {
			return 0, false
		}
		n = parsed
	default:
		return 0, false
	}
	if n <= 0 {
		return 0, false
	}
	return n, true
}

// assertNoLeak serialises the card and rejects it if it would reveal the
// internal face path, a credential-bearing URL, or a database DSN.
func assertNoLeak(card *a2a.AgentCard, internalPath string) error {
	raw, err := json.Marshal(card)
	if err != nil {
		return fmt.Errorf("gateway: encode agent card: %w", err)
	}
	s := string(raw)
	if ip := strings.TrimSpace(internalPath); ip != "" && strings.Contains(s, ip) {
		return contracts.NewError(contracts.ErrInvalid,
			"gateway: agent card must not expose the internal face path")
	}
	if credentialURL.MatchString(s) {
		return contracts.NewError(contracts.ErrInvalid,
			"gateway: agent card must not embed credentials")
	}
	lower := strings.ToLower(s)
	for _, scheme := range dbSchemes {
		if strings.Contains(lower, scheme) {
			return contracts.NewError(contracts.ErrInvalid,
				"gateway: agent card must not embed a database DSN")
		}
	}
	return nil
}
