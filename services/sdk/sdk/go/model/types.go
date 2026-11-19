// Package model is an API-only client for the AHP Model owner.
package model

import (
	"encoding/json"
	"time"
)

type Message struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	ToolCalls  []WireToolCall `json:"tool_calls,omitempty"`
}

type WireToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function WireFunctionCall `json:"function"`
}

type WireFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type Generation struct {
	FinishReason  string `json:"finish_reason,omitempty"`
	DeltaSequence int64  `json:"delta_sequence,omitempty"`
	ID            string `json:"id"`
	Task          string `json:"task,omitempty"`
	Key           string `json:"key"`
	Model         string `json:"model"`
	// Provider is the provider COPY this generation was admitted and executed
	// against -- the upstream whose quota window paid and whose client dialled.
	// Empty means this deployment named no provider (single-provider wiring).
	Provider        string          `json:"provider,omitempty"`
	Status          string          `json:"status"`
	StatusVersion   int64           `json:"status_version"`
	Request         json.RawMessage `json:"request"`
	Output          string          `json:"output"`
	Usage           Usage           `json:"usage"`
	Error           string          `json:"error,omitempty"`
	CancelRequested bool            `json:"cancel_requested"`
	EffectKey       string          `json:"effect_key"`
	BudgetID        string          `json:"budget_id,omitempty"`
	ReserveKey      string          `json:"reserve_key,omitempty"`
	Reserved        int64           `json:"reserved,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	digest          string
}

type Request struct {
	Key       string    `json:"key"`
	After     int64     `json:"after,omitempty"`
	Model     string    `json:"model,omitempty"`
	Messages  []Message `json:"messages"`
	Tools     []ToolDef `json:"tools,omitempty"`
	MaxTokens int       `json:"max_tokens,omitempty"`
}
type Delta struct {
	GenerationID string `json:"generation_id"`
	Sequence     int64  `json:"sequence"`
	Text         string `json:"text"`
}
type Frame struct {
	Type         string      `json:"type"`
	GenerationID string      `json:"generation_id,omitempty"`
	Delta        *Delta      `json:"delta,omitempty"`
	Generation   *Generation `json:"generation,omitempty"`
	Code         string      `json:"code,omitempty"`
}
