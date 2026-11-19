package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/space"
)

// Each run receives its own Core configuration. No shared runtime is mutated
// while another run is executing or recovering from a checkpoint.
type profileRuntime struct {
	cfg         core.RuntimeConfig
	selectModel func(string) (core.ModelClient, error)
	gateway     *gatewayToolBridge
}

func newProfileRuntime(cfg core.RuntimeConfig, selectModel func(string) (core.ModelClient, error)) space.CoreRunner {
	return profileRuntime{cfg: cfg, selectModel: selectModel}
}

const dotPromptMarker = "[AHP Dot configuration]\n"

func (r profileRuntime) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	if req == nil {
		return fmt.Errorf("missing Core request")
	}
	if req.AgentConfig == nil || req.AgentConfig.Dot == nil {
		return core.NewRuntime(r.cfg).Run(ctx, req, results)
	}
	dot := req.AgentConfig.Dot
	if dot.MaxSteps < 1 || dot.MaxSteps > 256 {
		return fmt.Errorf("invalid pinned Dot step limit")
	}
	cfg := r.cfg
	if dot.ModelRoute != nil {
		var route struct {
			Model string `json:"model"`
		}
		if json.Unmarshal(dot.ModelRoute.Document, &route) != nil || route.Model == "" {
			return fmt.Errorf("invalid pinned model route")
		}
		if r.selectModel == nil {
			return fmt.Errorf("pinned model selection requires Model Gateway")
		}
		model, err := r.selectModel(route.Model)
		if err != nil {
			return err
		}
		cfg.Model = model
	}
	allowed := map[string]bool{}
	for _, tool := range dot.AllowedTools {
		allowed[tool] = true
	}
	executor := profileToolExecutor{base: cfg.Tools, gateway: r.gateway, names: map[string]bool{}}
	limited := core.NewToolRegistry(executor)
	localNames := map[string]bool{}
	for _, def := range cfg.Tools.Defs() {
		localNames[def.Name] = true
		if allowed[def.Name] {
			limited.Register(def)
		}
	}
	if req.AgentConfig.ToolManifest != nil {
		defs, err := pinnedGatewayDefinitions(req.AgentConfig.ToolManifest, allowed)
		if err != nil {
			return err
		}
		if len(defs) > 0 && r.gateway == nil {
			return fmt.Errorf("pinned gateway tools require an authenticated Tool Gateway")
		}
		for _, def := range defs {
			if localNames[def.Name] {
				return fmt.Errorf("pinned gateway tool conflicts with a local tool")
			}
			executor.names[def.Name] = true
			limited.Register(core.ToolDef{Name: def.Name, Description: def.Description, Parameters: def.Parameters})
		}
	}
	cfg.Tools = limited
	cfg.ToolAllowed = func(name string) bool { return allowed[name] }
	copy := *req
	if copy.MaxSteps <= 0 || copy.MaxSteps > dot.MaxSteps {
		copy.MaxSteps = dot.MaxSteps
	}
	instructions := dot.Instructions
	for _, skill := range dot.Skills {
		var bundle struct {
			Instructions string `json:"instructions"`
		}
		if err := json.Unmarshal(skill.Bundle, &bundle); err != nil {
			return fmt.Errorf("invalid pinned skill bundle")
		}
		instructions += "\n\nSkill " + skill.SkillID + ":\n" + bundle.Instructions
	}
	var messages []core.ChatMessage
	if len(copy.Messages) > 0 {
		if err := json.Unmarshal(copy.Messages, &messages); err != nil {
			return err
		}
	}
	// A new turn replaces our preceding turn's config prompt. A recovered turn
	// uses the same pinned prompt; do not accumulate duplicates on each resume.
	filtered := make([]core.ChatMessage, 0, len(messages)+1)
	content, _ := json.Marshal(dotPromptMarker + instructions)
	filtered = append(filtered, core.ChatMessage{Role: "system", Content: content})
	for _, message := range messages {
		var text string
		if message.Role == "system" && json.Unmarshal(message.Content, &text) == nil && strings.HasPrefix(text, dotPromptMarker) {
			continue
		}
		filtered = append(filtered, message)
	}
	copy.Messages, _ = json.Marshal(filtered)
	return core.NewRuntime(cfg).Run(ctx, &copy, results)
}

// completionCap is the completion bound this runner asks the Model Gateway for.
//
// The gateway owns the number (AHP_EXEC_MAX_TOKENS, read through the same helper
// the gateway itself uses); AS_MODEL_MAX_TOKENS may only lower it for this
// workload, which is a legitimate profile choice. A stated value above the
// gateway's cap, or a stated value that is not a positive number, is refused at
// startup: the alternative is discovering the mismatch on a user's first turn.
func completionCap() (int, error) {
	limit := execbridge.MaxTokensFromEnv()
	raw := strings.TrimSpace(envDefault("AS_MODEL_MAX_TOKENS", ""))
	if raw == "" {
		return limit, nil
	}
	asked, err := strconv.Atoi(raw)
	if err != nil || asked <= 0 {
		return 0, fmt.Errorf("as-runner: AS_MODEL_MAX_TOKENS=%q is not a positive completion cap", raw)
	}
	if asked > limit {
		return 0, fmt.Errorf("as-runner: AS_MODEL_MAX_TOKENS=%d exceeds the Model Gateway completion cap %d (AHP_EXEC_MAX_TOKENS)",
			asked, limit)
	}
	return asked, nil
}

// buildProfileRuntime shares the workload HTTPS client with existing owner APIs.
func buildProfileRuntime(cfg config, base core.RuntimeConfig) (space.CoreRunner, space.DotConfigPinner, error) {
	// The completion cap is the Model Gateway's, stated explicitly here. Leaving
	// it unset let internal/core apply its own private default (4096) while the
	// gateway refused anything above AHP_EXEC_MAX_TOKENS -- so a live Dot turn
	// ended in "model stream refused: HTTP 400" over a number nobody configured
	// (DEF-DOTE2E-9). A conflict between the two is a startup failure.
	tokens, err := completionCap()
	if err != nil {
		return nil, nil, err
	}
	base.MaxTokens = tokens
	if cfg.RegistryURL == "" {
		if os.Getenv("AS_TOOLS_DESCRIPTOR_FILE") != "" || os.Getenv("AS_TOOLS_DESCRIPTOR_SHA256") != "" {
			return nil, nil, fmt.Errorf("tool descriptor snapshots require AS_REGISTRY_URL")
		}
		return core.NewRuntime(base), nil, nil
	}
	client, err := buildStateClient(cfg)
	if err != nil {
		return nil, nil, err
	}
	pinner, err := space.NewProfileClient(cfg.RegistryURL, client)
	if err != nil {
		return nil, nil, err
	}
	gateway, manifest, err := buildGatewayToolBridge(cfg, client)
	if err != nil {
		return nil, nil, err
	}
	if manifest != nil {
		for _, local := range base.Tools.Defs() {
			for _, descriptor := range manifest.Tools {
				if descriptor.Name == local.Name {
					return nil, nil, fmt.Errorf("tool descriptor conflicts with a local tool")
				}
			}
		}
		pinner = manifestPinner{owner: pinner, manifest: manifest}
	}
	selector := func(name string) (core.ModelClient, error) {
		remote, err := execbridge.NewRemoteModel(envDefault("AS_MODEL_URL", ""), name, client)
		if err != nil {
			return nil, err
		}
		authority, err := buildRunScopeAttester()
		if err != nil {
			return nil, err
		}
		if !authority.enabled() {
			return nil, fmt.Errorf("profile model route requires verified run authority")
		}
		return scopedGatewayModel{model: remote, authority: authority}, nil
	}
	return profileRuntime{cfg: base, selectModel: selector, gateway: gateway}, pinner, nil
}
