package evals

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
	"regexp"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Case is an operator-owned reference task. Inputs and expected output hashes
// live only in the runner configuration and are never written to Verdict rows.
// A caller can select an allowlisted suite, but cannot submit a pass assertion,
// an endpoint, an arbitrary prompt, or expected output.
var runtimeImagePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]{0,255}@sha256:[0-9a-f]{64}$`)

type Case struct {
	ID                   string          `json:"id"`
	Input                json.RawMessage `json:"input"`
	RequiredText         string          `json:"required_text,omitempty"`
	ExpectedOutputSHA256 string          `json:"expected_output_sha256"`
}
type Target struct {
	Manifest    json.RawMessage `json:"manifest"`
	Deployment  string          `json:"deployment"`
	AgentID     string          `json:"agent_id"`
	AgentDigest string          `json:"agent_digest"`
	Image       string          `json:"image"`
	Endpoint    string          `json:"endpoint"`
	ReadyURL    string          `json:"ready_url"`
}
type Suite struct {
	ID             string `json:"id"`
	Tenant         string `json:"tenant"`
	Target         Target `json:"target"`
	Cases          []Case `json:"cases"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	// PolicyID names the evaluation policy the suite is run under; every
	// verdict the runner records is bound to it. It is mandatory: a suite
	// without a policy could record verdicts no gate can age or compare.
	PolicyID string `json:"policy_id"`
}

// targetKindOf derives the verdict provenance from the endpoint itself: an
// HTTPS endpoint is a real deployment, the HTTPS-exempt loopback endpoints are
// fixtures. The operator cannot declare "real" for a loopback run -- the kind
// follows the wire, not the claim.
func targetKindOf(endpoint string) TargetKind {
	u, err := url.Parse(endpoint)
	if err == nil && u.Scheme == "https" {
		return TargetReal
	}
	return TargetFixture
}

type RunRequest struct {
	Scope       contracts.Scope
	TaskID      string
	Tenant      string
	SuiteID     string
	AgentDigest string
	Token       string
}

// Runner executes a bounded operator allowlist. The HTTP protocol is A2A
// SendMessage with blocking=true. A successful acceptance alone is insufficient:
// the task must finish completed and its text output must match the reference.
type Runner struct {
	store  *Store
	suites map[string]Suite
	client *http.Client
	slots  chan struct{}
	issuer RunTokenIssuer
}

func NewRunner(store *Store, suites []Suite, client *http.Client) (*Runner, error) {
	if store == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "evals runner requires store")
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if c.Timeout <= 0 || c.Timeout > 60*time.Second {
		c.Timeout = 60 * time.Second
	}
	r := &Runner{store: store, suites: map[string]Suite{}, client: &c, slots: make(chan struct{}, 2)}
	for _, s := range suites {
		if s.ID == "" || s.Tenant == "" || trim(s.PolicyID) == "" || !versionDigestPattern.MatchString(s.Target.AgentDigest) || s.Target.AgentID == "" || s.Target.Deployment == "" || !runtimeImagePattern.MatchString(s.Target.Image) || len(s.Cases) == 0 || len(s.Cases) > 32 || s.TimeoutSeconds < 1 || s.TimeoutSeconds > 120 {
			return nil, contracts.NewError(contracts.ErrInvalid, "evals: invalid bounded suite")
		}
		digest, err := contracts.Digest(s.Target.Manifest)
		var manifest struct {
			Components []struct {
				A2A   bool   `json:"a2a"`
				Image string `json:"image"`
			} `json:"components"`
		}
		if err != nil || digest != s.Target.AgentDigest || json.Unmarshal(s.Target.Manifest, &manifest) != nil {
			return nil, contracts.NewError(contracts.ErrInvalid, "evals: manifest digest mismatch")
		}
		images := 0
		for _, c := range manifest.Components {
			if c.A2A && c.Image == s.Target.Image {
				images++
			}
		}
		if images != 1 {
			return nil, contracts.NewError(contracts.ErrInvalid, "evals: candidate manifest must bind target image")
		}
		ep, err := evalURL(s.Target.Endpoint)
		if err != nil {
			return nil, err
		}
		ready, err := evalURL(s.Target.ReadyURL)
		if err != nil {
			return nil, err
		}
		if ep.Scheme != ready.Scheme || ep.Host != ready.Host {
			return nil, contracts.NewError(contracts.ErrInvalid, "evals: readiness origin mismatch")
		}
		key := s.Tenant + "/" + s.ID
		if _, ok := r.suites[key]; ok {
			return nil, contracts.NewError(contracts.ErrConflict, "evals: duplicate suite")
		}
		seen := map[string]bool{}
		for _, tc := range s.Cases {
			if tc.ID == "" || seen[tc.ID] || len(tc.Input) > 65536 || !json.Valid(tc.Input) || ((tc.RequiredText == "") == (tc.ExpectedOutputSHA256 == "")) || (tc.ExpectedOutputSHA256 != "" && !versionDigestPattern.MatchString(tc.ExpectedOutputSHA256)) || len(tc.RequiredText) > 1024 {
				return nil, contracts.NewError(contracts.ErrInvalid, "evals: invalid reference case")
			}
			seen[tc.ID] = true
		}
		// Deep copy isolates policy from subsequent caller mutation.
		b, _ := json.Marshal(s)
		var copy Suite
		_ = json.Unmarshal(b, &copy)
		r.suites[key] = copy
	}
	return r, nil
}

func evalURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"))) {
		return nil, contracts.NewError(contracts.ErrInvalid, "evals: invalid operator target URL")
	}
	return u, nil
}

func (r *Runner) RunEval(ctx context.Context, in RunRequest) (Verdict, error) {
	suite, ok := r.suites[in.Tenant+"/"+in.SuiteID]
	if !ok || suite.Target.AgentDigest != in.AgentDigest {
		return Verdict{}, contracts.NewError(contracts.ErrForbidden, "evals: target and suite are not allowlisted for tenant")
	}
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	default:
		return Verdict{}, contracts.NewError(contracts.ErrUnavailable, "evals: runner concurrency limit reached")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(suite.TimeoutSeconds)*time.Second)
	defer cancel()
	if err := r.ready(ctx, suite.Target); err != nil {
		return Verdict{}, err
	}
	if r.issuer == nil {
		return Verdict{}, contracts.NewError(contracts.ErrUnavailable, "evals: target credential issuer not configured")
	}
	results := make([]TaskResult, 0, len(suite.Cases))
	outcome := OutcomePass
	for _, tc := range suite.Cases {
		start := time.Now()
		passed := false
		caseReq := in
		caseReq.TaskID = contracts.NewID()
		childToken, issueErr := r.issuer.IssueEvalToken(ctx, caseReq, suite.Target)
		var output string
		terminal := TerminalUnknown
		err := issueErr
		if err == nil && childToken != "" {
			var term string
			output, term, err = r.execute(ctx, suite.Target, tc, childToken, caseReq.TaskID)
			terminal = term
		} else if err == nil {
			err = fmt.Errorf("missing target credential")
		}
		// Transport vs terminal stay separate facts (row 24 / DEF-PROF-1): a
		// clean exchange that ends in a failed or blocked task is terminal
		// evidence, not a transport error; a broken exchange claims no terminal.
		transport := err != nil && terminal == TerminalUnknown

		if err == nil {
			sum := sha256.Sum256([]byte(output))
			passed = "sha256:"+hex.EncodeToString(sum[:]) == tc.ExpectedOutputSHA256
			if tc.RequiredText != "" {
				passed = strings.Contains(output, tc.RequiredText)
			}
		}
		if !passed {
			outcome = OutcomeFail
		}
		results = append(results, TaskResult{TaskID: tc.ID, Passed: passed, TransportError: transport, TerminalState: terminal, LatencyMS: time.Since(start).Milliseconds(), Metrics: map[string]float64{"cost_reported": 0}})
	}
	// Preserve timeout/failure evidence even after the execution budget expires;
	// bounded background storage only, no detached execution or content logs.
	writeCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	return r.store.Record(writeCtx, RecordInput{AgentDigest: suite.Target.AgentDigest, TaskSetID: suite.ID, Setup: suite, Outcome: outcome, Tasks: results,
		PolicyID: suite.PolicyID, TargetKind: targetKindOf(suite.Target.Endpoint)})
}

func (r *Runner) ready(ctx context.Context, t Target) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, t.ReadyURL, nil)
	resp, err := r.client.Do(req)
	if err != nil {
		return contracts.NewError(contracts.ErrUnavailable, "evals: target readiness failed")
	}
	defer resp.Body.Close()
	var got struct {
		Ready   bool   `json:"ready"`
		AgentID string `json:"agent_id"`
		Digest  string `json:"version_digest"`
		Image   string `json:"image"`
	}
	if resp.StatusCode != 200 || json.NewDecoder(io.LimitReader(resp.Body, 16384)).Decode(&got) != nil || !got.Ready || got.AgentID != t.AgentID || got.Digest != t.AgentDigest || got.Image != t.Image {
		return contracts.NewError(contracts.ErrUnavailable, "evals: target identity mismatch")
	}
	return nil
}

func (r *Runner) execute(ctx context.Context, t Target, c Case, token, taskID string) (string, string, error) {
	var text string
	if json.Unmarshal(c.Input, &text) != nil {
		text = string(c.Input)
	}
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": contracts.NewID(), "method": "SendMessage", "params": map[string]any{"message": map[string]any{"role": "user", "kind": "message", "messageId": contracts.NewID(), "parts": []any{map[string]any{"kind": "text", "text": text}}}, "configuration": map[string]any{"blocking": true}, "metadata": map[string]any{"ahp": map[string]any{"agent_id": t.AgentID, "version_digest": t.AgentDigest, "eval_case": c.ID, "task_id": taskID}}}})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, t.Endpoint, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return "", TerminalUnknown, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", TerminalUnknown, fmt.Errorf("eval target HTTP %d", resp.StatusCode)
	}
	type part struct {
		Kind string `json:"kind"`
		Text string `json:"text"`
	}
	type task struct {
		ID     string `json:"id"`
		Status struct {
			State   string `json:"state"`
			Message struct {
				Parts []part `json:"parts"`
			} `json:"message"`
		} `json:"status"`
		Artifacts []struct {
			Parts []part `json:"parts"`
		} `json:"artifacts"`
	}
	var wire struct {
		Error  json.RawMessage `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&wire); err != nil {
		return "", TerminalUnknown, err
	}
	if len(wire.Error) > 0 && string(wire.Error) != "null" {
		return "", TerminalUnknown, fmt.Errorf("eval target RPC failed")
	}
	var taskResult task
	var wrapped struct {
		Task task `json:"task"`
	}
	_ = json.Unmarshal(wire.Result, &taskResult)
	_ = json.Unmarshal(wire.Result, &wrapped)
	if wrapped.Task.ID != "" {
		taskResult = wrapped.Task
	}
	if taskResult.ID == "" {
		return "", TerminalUnknown, fmt.Errorf("eval target did not identify the task")
	}
	if taskResult.Status.State != "completed" {
		terminal := TerminalFailed
		switch taskResult.Status.State {
		case "failed":
			terminal = TerminalFailed
		case "canceled", "rejected":
			terminal = TerminalBlocked
		default:
			terminal = TerminalUnknown
		}
		return "", terminal, fmt.Errorf("eval target did not complete task")
	}
	output := ""
	for _, a := range taskResult.Artifacts {
		for _, p := range a.Parts {
			if p.Kind == "text" {
				output += p.Text
			}
		}
	}
	if output == "" {
		for _, p := range taskResult.Status.Message.Parts {
			if p.Kind == "text" {
				output += p.Text
			}
		}
	}
	return output, TerminalCompleted, nil
}

type RunTokenIssuer interface {
	IssueEvalToken(context.Context, RunRequest, Target) (string, error)
}

func (r *Runner) SetTokenIssuer(issuer RunTokenIssuer) { r.issuer = issuer }
