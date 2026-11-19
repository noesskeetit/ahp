package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const toolManifestMaxBytes = 1 << 20
const gatewayResponseMaxBytes = 16 << 20

var gatewayToolName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

type gatewayToolManifest struct {
	Version int                           `json:"version"`
	Tools   []space.GatewayToolDescriptor `json:"tools"`
}

func toolDigest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func validToolDigest(s string) bool {
	decoded, err := hex.DecodeString(s)
	return err == nil && len(decoded) == sha256.Size && s == strings.ToLower(s)
}

func strictToolJSON(data []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return fmt.Errorf("invalid tool manifest structure")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("tool manifest must contain one JSON object")
	}
	return nil
}

// canonicalToolDescriptors validates and copies every schema. No caller can
// mutate the deployment manifest or a different run through RawMessage aliases.
func canonicalToolDescriptors(defs []space.GatewayToolDescriptor) ([]space.GatewayToolDescriptor, []byte, error) {
	if len(defs) > 128 {
		return nil, nil, fmt.Errorf("tool manifest exceeds 128 descriptors")
	}
	out := make([]space.GatewayToolDescriptor, 0, len(defs))
	seen := make(map[string]bool, len(defs))
	for _, def := range defs {
		if !gatewayToolName.MatchString(def.Name) || seen[def.Name] || len(def.Description) > 4096 {
			return nil, nil, fmt.Errorf("tool manifest contains invalid or duplicate descriptors")
		}
		var schema map[string]any
		decoder := json.NewDecoder(bytes.NewReader(def.Parameters))
		decoder.UseNumber()
		_, schemaErr := contracts.Digest(def.Parameters)
		if len(def.Parameters) == 0 || schemaErr != nil || decoder.Decode(&schema) != nil || schema == nil || schema["type"] != "object" {
			return nil, nil, fmt.Errorf("tool manifest parameters must be an object JSON schema")
		}
		// Canonical map encoding normalizes whitespace/key order for the digest.
		parameters, err := json.Marshal(schema)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid tool schema")
		}
		def.Parameters = parameters
		out = append(out, def)
		seen[def.Name] = true
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	canonical, err := json.Marshal(out)
	return out, canonical, err
}

func readGatewayToolManifest(path, expectedSHA string) (*space.ToolManifestSnapshot, error) {
	if !validToolDigest(expectedSHA) {
		return nil, fmt.Errorf("AS_TOOLS_DESCRIPTOR_SHA256 must pin the descriptor file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open tool descriptor file")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, toolManifestMaxBytes+1))
	if err != nil || len(data) > toolManifestMaxBytes || toolDigest(data) != expectedSHA {
		return nil, fmt.Errorf("tool descriptor file unreadable, oversized or SHA256 mismatch")
	}
	var manifest gatewayToolManifest
	if err := strictToolJSON(data, &manifest); err != nil {
		return nil, err
	}
	if manifest.Version != 1 || len(manifest.Tools) == 0 {
		return nil, fmt.Errorf("tool descriptor manifest requires version 1 and tools")
	}
	defs, canonical, err := canonicalToolDescriptors(manifest.Tools)
	if err != nil {
		return nil, err
	}
	return &space.ToolManifestSnapshot{Version: 1, SourceSHA256: expectedSHA, SHA256: toolDigest(canonical), Tools: defs}, nil
}

// manifestPinner decorates the authenticated Registry snapshot before Space
// admits/writes a turn. The ingress already rejects user-supplied AgentConfig.
// The full selected descriptor bytes, not a mutable path, enter State's payload.
type manifestPinner struct {
	owner    space.DotConfigPinner
	manifest *space.ToolManifestSnapshot
}

func (p manifestPinner) Pin(ctx context.Context, token, key, contextID string) (*space.AgentConfigRef, error) {
	pin, err := p.owner.Pin(ctx, token, key, contextID)
	if err != nil {
		return nil, err
	}
	if pin == nil || pin.Dot == nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "missing owner Dot snapshot")
	}
	// The owner/client may cache a snapshot. Never decorate its shared instance.
	encoded, err := json.Marshal(pin)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "invalid owner Dot snapshot")
	}
	var copy space.AgentConfigRef
	if json.Unmarshal(encoded, &copy) != nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "invalid owner Dot snapshot")
	}
	allowed := make(map[string]bool, len(copy.Dot.AllowedTools))
	for _, name := range copy.Dot.AllowedTools {
		allowed[name] = true
	}
	var selected []space.GatewayToolDescriptor
	for _, def := range p.manifest.Tools {
		if allowed[def.Name] {
			selected = append(selected, def)
		}
	}
	defs, canonical, err := canonicalToolDescriptors(selected)
	if err != nil {
		return nil, err
	}
	copy.ToolManifest = &space.ToolManifestSnapshot{Version: 1, SourceSHA256: p.manifest.SourceSHA256,
		SHA256: toolDigest(canonical), Tools: defs}
	return &copy, nil
}

// gatewayToolBridge has no provider/MCP credential. It presents the CURRENT run
// task token over workload mTLS to the existing Call and GetResult owner APIs.
// There is exactly one Call attempt; uncertain effects are never auto-resubmitted.
type gatewayToolBridge struct {
	origin    string
	client    *http.Client
	authority runScopeAttester
}

func newGatewayToolBridge(origin string, client *http.Client, authority runScopeAttester) (*gatewayToolBridge, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(origin, "?#%") {
		return nil, fmt.Errorf("tool gateway requires a bare HTTPS owner origin")
	}
	if client == nil || client.Transport == nil || !authority.enabled() || authority.workload == "" {
		return nil, fmt.Errorf("tool gateway requires workload mTLS and verified run authority")
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if copy.Timeout == 0 {
		copy.Timeout = 10 * time.Minute
	}
	return &gatewayToolBridge{strings.TrimRight(origin, "/"), &copy, authority}, nil
}

func buildGatewayToolBridge(cfg config, client *http.Client) (*gatewayToolBridge, *space.ToolManifestSnapshot, error) {
	path, digest := os.Getenv("AS_TOOLS_DESCRIPTOR_FILE"), os.Getenv("AS_TOOLS_DESCRIPTOR_SHA256")
	if path == "" && digest == "" {
		return nil, nil, nil // existing deployments keep their exact tool wiring
	}
	if path == "" || cfg.ToolsURL == "" || cfg.RegistryURL == "" {
		return nil, nil, fmt.Errorf("tool descriptors require AS_TOOLS_DESCRIPTOR_FILE, AS_TOOLS_URL and AS_REGISTRY_URL")
	}
	manifest, err := readGatewayToolManifest(path, digest)
	if err != nil {
		return nil, nil, err
	}
	authority, err := buildRunScopeAttester()
	if err != nil {
		return nil, nil, fmt.Errorf("tool gateway run authority unavailable")
	}
	bridge, err := newGatewayToolBridge(cfg.ToolsURL, client, authority)
	return bridge, manifest, err
}

// profileToolExecutor routes only the definitions actually selected for this
// run. A hallucinated/disabled name cannot enter the generic gateway executor.
type profileToolExecutor struct {
	base    *core.ToolRegistry
	gateway *gatewayToolBridge
	names   map[string]bool
}

func (e profileToolExecutor) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	if e.names[name] {
		return e.gateway.Execute(ctx, name, args)
	}
	if e.base == nil {
		return nil, contracts.NewError(contracts.ErrForbidden, "tool is not in the pinned registry")
	}
	return e.base.Execute(ctx, name, args)
}

func pinnedGatewayDefinitions(snapshot *space.ToolManifestSnapshot, allowed map[string]bool) ([]space.GatewayToolDescriptor, error) {
	if snapshot.Version != 1 || !validToolDigest(snapshot.SourceSHA256) || !validToolDigest(snapshot.SHA256) {
		return nil, fmt.Errorf("invalid pinned tool manifest")
	}
	defs, canonical, err := canonicalToolDescriptors(snapshot.Tools)
	if err != nil || toolDigest(canonical) != snapshot.SHA256 {
		return nil, fmt.Errorf("pinned tool manifest digest mismatch")
	}
	for _, def := range defs {
		if !allowed[def.Name] {
			return nil, contracts.NewError(contracts.ErrForbidden, "pinned tool descriptor exceeds AllowedTools")
		}
	}
	return defs, nil
}

func (g *gatewayToolBridge) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	if _, err := contracts.Digest(args); err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "invalid canonical tool arguments")
	}
	scope, token, ok := core.RunScopeFromContext(ctx)
	binding, bound := core.RunBindingFromContext(ctx)
	if !ok || !bound || token == "" || binding.TaskID == "" || binding.OperationKey == "" {
		return nil, contracts.NewError(contracts.ErrForbidden, "tool gateway requires a journalled run binding")
	}
	claims, err := g.authority.verifier.Verify(token, g.authority.taskAudience, auth.KindTask, g.authority.workload)
	if err != nil || scope != claims.Scope() || binding.ContextID != claims.Context || claims.Context == "" {
		return nil, contracts.NewError(contracts.ErrForbidden, "tool gateway run authority mismatch")
	}
	// Do not include arguments or tool name: a changed action at the SAME run
	// coordinate must collide in the owner's ledger and be rejected, not execute
	// under a second key. All authority dimensions are included without secrets.
	coordinate, _ := json.Marshal([]string{scope.Tenant, scope.Deployment, scope.Subject,
		claims.Task, claims.Context, binding.TaskID, binding.OperationKey})
	key := "core-tool-" + toolDigest(coordinate)
	var operation struct {
		Key    string `json:"key"`
		Target string `json:"target"`
		Status string `json:"status"`
		Digest string `json:"digest"`
	}
	if err := g.post(ctx, token, "Call", map[string]any{"key": key, "tool": name, "arguments": args}, &operation, true); err != nil {
		return nil, err
	}
	if operation.Key != key || operation.Target != name {
		return nil, contracts.NewReasoned(contracts.ErrConflict, "SIDE_EFFECT_UNKNOWN", "tool gateway returned an invalid operation receipt")
	}
	if !gatewayOperationMatches(operation.Digest, name, args) {
		return nil, contracts.NewError(contracts.ErrKeyReuse, "tool gateway operation fingerprint differs from the requested action")
	}
	if operation.Status != operations.StatusSucceeded {
		reason := "TOOL_OPERATION_NOT_SUCCEEDED"
		if operation.Status == operations.StatusSideEffectUnknown || operation.Status == operations.StatusAccepted || operation.Status == operations.StatusDispatched {
			reason = "SIDE_EFFECT_UNKNOWN"
		}
		return nil, contracts.NewReasoned(contracts.ErrConflict, reason, "tool gateway operation has no successful result")
	}
	var result tools.ResultView
	if err := g.post(ctx, token, "GetResult", map[string]string{"key": key}, &result, false); err != nil {
		return nil, err
	}
	if result.Key != key || result.Target != name || result.Status != operations.StatusSucceeded || result.Digest != operation.Digest {
		return nil, contracts.NewError(contracts.ErrForbidden, "tool gateway result receipt mismatch")
	}
	var provenance struct {
		Schema  string `json:"schema"`
		Context string `json:"grant_context"`
	}
	if json.Unmarshal(result.Provenance, &provenance) != nil || provenance.Schema != tools.ProvenanceSchema || provenance.Context != claims.Context {
		return nil, contracts.NewError(contracts.ErrForbidden, "tool gateway result provenance mismatch")
	}
	switch result.Storage {
	case "inline":
		if !json.Valid(result.Payload) || bytes.Equal(bytes.TrimSpace(result.Payload), []byte("null")) {
			return nil, contracts.NewError(contracts.ErrUnavailable, "tool gateway returned invalid result JSON")
		}
		return result.Payload, nil
	case "artifact":
		if result.Ref == nil || result.Ref.ArtifactID == "" || result.Ref.Digest == "" || result.Ref.Size < 0 {
			return nil, contracts.NewError(contracts.ErrUnavailable, "tool gateway returned invalid result reference")
		}
		// Preserve the owner's immutable reference, not fabricated inline output.
		// Fetching it is a separate Artifacts operation under current authority.
		return json.Marshal(map[string]any{"result_storage": "artifact", "artifact_ref": result.Ref})
	default:
		return nil, contracts.NewError(contracts.ErrUnavailable, "unknown tool result storage")
	}
}

// The owner chooses whether to guard a side effect. Verify both legal owner
// fingerprints without copying that policy into the runner. This also rejects
// a sealed receipt for different arguments at the same logical coordinate.
func gatewayOperationMatches(digest, name string, args json.RawMessage) bool {
	guarded, err := tools.ApprovalDigest(name, args)
	if err != nil {
		return false
	}
	for _, guard := range []string{"", guarded} {
		expected, err := contracts.Digest(map[string]any{"kind": tools.EffectKind, "target": name, "guard": guard, "arguments": args})
		if err == nil && digest == expected {
			return true
		}
	}
	return false
}

func (g *gatewayToolBridge) post(ctx context.Context, token, operation string, input, output any, mutation bool) error {
	data, err := json.Marshal(input)
	if err != nil {
		return contracts.NewError(contracts.ErrInvalid, "invalid tool request")
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, g.origin+"/v1/tools/"+operation, bytes.NewReader(data))
	if err != nil {
		return contracts.NewError(contracts.ErrInvalid, "invalid tool gateway request")
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json")
	artifactTraceDecorator(r)
	response, err := g.client.Do(r)
	if err != nil {
		// Never wrap url.Error or upstream text: either may include a secret or
		// a URL. Cancellation still prevents all subsequent calls via ctx.
		if mutation {
			return contracts.NewReasoned(contracts.ErrConflict, "SIDE_EFFECT_UNKNOWN", "tool gateway transport interrupted; reconcile the same operation")
		}
		return contracts.NewError(contracts.ErrUnavailable, "tool gateway result unavailable")
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, gatewayResponseMaxBytes+1))
	if readErr != nil || len(body) > gatewayResponseMaxBytes {
		if mutation {
			return contracts.NewReasoned(contracts.ErrConflict, "SIDE_EFFECT_UNKNOWN", "tool gateway operation receipt unavailable")
		}
		return contracts.NewError(contracts.ErrUnavailable, "tool gateway result unreadable or oversized")
	}
	if response.StatusCode != http.StatusOK {
		// Preserve known public error classes, never arbitrary owner messages,
		// bodies, URLs, headers, request arguments or credentials.
		var refusal struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(body, &refusal)
		code := contracts.ErrUnavailable
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			code = contracts.ErrForbidden
		case http.StatusNotFound:
			code = contracts.ErrNotFound
		case http.StatusBadRequest:
			code = contracts.ErrInvalid
		case http.StatusConflict:
			code = contracts.ErrConflict
		}
		switch refusal.Code {
		case contracts.ErrForbidden, contracts.ErrNotFound, contracts.ErrInvalid, contracts.ErrConflict, contracts.ErrKeyReuse:
			code = refusal.Code
		}
		if mutation && response.StatusCode >= 500 {
			return contracts.NewReasoned(contracts.ErrConflict, "SIDE_EFFECT_UNKNOWN", "tool gateway operation outcome unavailable")
		}
		return contracts.NewError(code, "tool gateway rejected operation")
	}
	if json.Unmarshal(body, output) != nil {
		if mutation {
			return contracts.NewReasoned(contracts.ErrConflict, "SIDE_EFFECT_UNKNOWN", "tool gateway operation receipt is invalid")
		}
		return contracts.NewError(contracts.ErrUnavailable, "tool gateway result response is invalid")
	}
	return nil
}
