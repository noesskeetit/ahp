//go:build ahp_data

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	ltm "github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/internal/state"
)

func verifyMemorySource(ctx context.Context, st *state.Store, c auth.Claims, project, ref string) error {
	if strings.HasPrefix(ref, "console://memory/command/") && len(strings.TrimPrefix(ref, "console://memory/command/")) > 0 {
		if c.CtxScope == auth.CtxScopeAuthority {
			return nil
		}
		return contracts.NewError(contracts.ErrForbidden, "console source requires ingress authority")
	}
	u, err := url.Parse(ref)
	if err != nil || u.Scheme != "state" || u.Host != "runs" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return contracts.NewError(contracts.ErrInvalid, "memory source must reference State evidence")
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) < 2 || len(parts) > 3 {
		return contracts.NewError(contracts.ErrInvalid, "invalid State evidence path")
	}
	run, err := st.GetRun(ctx, c.Scope(), parts[0])
	if err != nil {
		return err
	}
	thread, err := st.GetThread(ctx, c.Scope(), run.ThreadID)
	if err != nil {
		return err
	}
	if thread.ContextID != c.Context {
		return contracts.NewError(contracts.ErrForbidden, "memory source is outside the task context")
	}
	var payload struct {
		ProjectID string `json:"project_id"`
	}
	if err := json.Unmarshal(run.Payload, &payload); err != nil {
		return err
	}
	if payload.ProjectID != project {
		return contracts.NewError(contracts.ErrForbidden, "source belongs to another project")
	}
	if len(parts) == 2 && parts[1] == "input" {
		return nil
	}
	if len(parts) != 3 || parts[1] != "steps" {
		return contracts.NewError(contracts.ErrInvalid, "invalid State evidence path")
	}
	sequence, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || sequence < 1 {
		return contracts.NewError(contracts.ErrInvalid, "invalid evidence sequence")
	}
	events, err := st.ReadStream(ctx, c.Scope(), run.ThreadID, sequence-1, 1)
	if err != nil {
		return err
	}
	if len(events) != 1 || events[0].Sequence != sequence || !strings.HasPrefix(events[0].Key, run.ID+":") {
		return contracts.NewError(contracts.ErrForbidden, "evidence does not belong to the stated run")
	}
	return nil
}

var memoryPrivateKey = regexp.MustCompile(`(?s)-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----.*?-----END (?:RSA |EC |OPENSSH )?PRIVATE KEY-----`)
var memoryBearer = regexp.MustCompile(`(?i)\bBearer\s+[a-z0-9._~+/=-]{12,}`)
var memoryCredential = regexp.MustCompile(`(?i)(api[_-]?key|password|passwd|secret|пароль|токен)\s*[:=]\s*["']?[^\s"'<>]{6,}`)

func cleanMemoryText(text string, limit int) string {
	text = memoryPrivateKey.ReplaceAllString(text, "[private key redacted]")
	text = memoryBearer.ReplaceAllString(text, "Bearer [redacted]")
	text = memoryCredential.ReplaceAllString(text, "$1=[redacted]")
	if len(text) > limit {
		text = text[:limit]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
		text += "\n[source clipped; consult its State reference]"
	}
	return strings.TrimSpace(text)
}
func memorySourceText(raw json.RawMessage) string {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var array []json.RawMessage
	if json.Unmarshal(raw, &array) == nil {
		parts := []string{}
		for _, item := range array {
			if text := memorySourceText(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	}
	var block struct {
		Type    string          `json:"type"`
		Text    string          `json:"text"`
		Content json.RawMessage `json:"content"`
		Summary json.RawMessage `json:"summary"`
		Output  json.RawMessage `json:"output"`
		Stdout  json.RawMessage `json:"stdout"`
	}
	if json.Unmarshal(raw, &block) != nil {
		return ""
	}
	if block.Type != "" && block.Type != "text" && block.Type != "output_text" && block.Type != "input_text" {
		return ""
	}
	if block.Text != "" {
		return block.Text
	}
	for _, value := range []json.RawMessage{block.Content, block.Summary, block.Output, block.Stdout} {
		if len(value) > 0 {
			return memorySourceText(value)
		}
	}
	return ""
}
func memoryEvidenceSources(e state.RunEvidence) []ltm.Source {
	var payload struct {
		Input     json.RawMessage `json:"input"`
		ProjectID string          `json:"project_id"`
	}
	if json.Unmarshal(e.Run.Payload, &payload) != nil {
		return nil
	}
	scopes := []ltm.Scope{ltm.Personal}
	if payload.ProjectID != "" {
		scopes = append(scopes, ltm.Project)
	}
	sources := []ltm.Source{}
	size := 0
	if text := cleanMemoryText(memorySourceText(payload.Input), 48<<10); text != "" {
		sources = append(sources, ltm.Source{Ref: "state://runs/" + e.Run.ID + "/input", Role: "user", Text: text, At: e.StartedAt, AllowedScopes: scopes})
		size += len(text)
	}
	// Prefer the latest conclusions when a pathological run exceeds the bounded
	// evidence budget. Frames retain State timestamps and references, not args,
	// injected memory blocks, reasoning fields or full binary tool payloads.
	tail := []ltm.Source{}
	for i := len(e.Frames) - 1; i >= 0 && len(tail)+len(sources) < 100; i-- {
		frame := e.Frames[i]
		role := "assistant"
		if frame.Kind == "tool" {
			role = "tool"
		}
		remaining := (128 << 10) - size - 128
		if remaining < 256 {
			break
		}
		limit := min(32<<10, remaining)
		text := cleanMemoryText(memorySourceText(frame.Payload), limit)
		if text == "" {
			continue
		}
		text = "Run outcome: " + e.Run.Status + ".\n" + text
		tail = append(tail, ltm.Source{Ref: fmt.Sprintf("state://runs/%s/steps/%d", e.Run.ID, frame.Sequence), Role: role, Text: text, At: frame.CreatedAt, AllowedScopes: scopes})
		size += len(text)
	}
	for i := len(tail) - 1; i >= 0; i-- {
		sources = append(sources, tail[i])
	}
	return sources
}
