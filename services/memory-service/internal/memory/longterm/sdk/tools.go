package sdk

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
)

type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

func Tools() []Tool {
	stringField := func() map[string]any { return map[string]any{"type": "string"} }
	scope := map[string]any{"type": "string", "enum": []string{"personal", "project"}}
	return []Tool{
		{"memory_search", "Search permitted personal memory and/or the current project's knowledge.", map[string]any{"type": "object", "properties": map[string]any{"query": stringField(), "scope": map[string]any{"type": "string", "enum": []string{"personal", "project", "both"}}}, "required": []string{"query", "scope"}, "additionalProperties": false}},
		{"memory_remember", "Queue useful knowledge for one scope. Accepted means queued, not yet indexed.", map[string]any{"type": "object", "properties": map[string]any{"text": stringField(), "target": scope}, "required": []string{"text", "target"}, "additionalProperties": false}},
		{"memory_correct", "Correct a retrieved source fact. Correct observations through their source facts.", map[string]any{"type": "object", "properties": map[string]any{"memory_ref": stringField(), "scope": scope, "text": stringField(), "reason": stringField()}, "required": []string{"memory_ref", "scope", "text", "reason"}, "additionalProperties": false}},
	}
}

// Invoke binds project and source references from trusted runtime context.
// The model supplies neither user identity nor bank/project IDs.
func (c *Client) Invoke(ctx context.Context, name string, args json.RawMessage, key, project string, sourceRefs []string) (any, error) {
	switch name {
	case "memory_search":
		var a struct {
			Query string `json:"query"`
			Scope string `json:"scope"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, err
		}
		return c.Search(ctx, memory.SearchRequest{ProjectID: project, Scope: a.Scope, Query: a.Query})
	case "memory_remember":
		var a struct {
			Text   string       `json:"text"`
			Target memory.Scope `json:"target"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, err
		}
		return c.Remember(ctx, key, memory.RememberRequest{ProjectID: project, Target: a.Target, Text: a.Text, SourceRefs: sourceRefs})
	case "memory_correct":
		var a struct {
			Ref    string       `json:"memory_ref"`
			Scope  memory.Scope `json:"scope"`
			Text   string       `json:"text"`
			Reason string       `json:"reason"`
		}
		if err := json.Unmarshal(args, &a); err != nil {
			return nil, err
		}
		return c.Correct(ctx, key, a.Ref, memory.CorrectRequest{Scope: a.Scope, ProjectID: project, Text: a.Text, Reason: a.Reason})
	default:
		return nil, fmt.Errorf("unknown memory tool %q", name)
	}
}
