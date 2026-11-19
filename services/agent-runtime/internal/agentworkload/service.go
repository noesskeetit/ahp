// Package agentworkload is a deployable custom-agent runtime. Durable tasks,
// lease fencing and checkpoints live behind State API; inference goes only
// through Model Gateway. It holds no provider keys or database credentials.
package agentworkload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/sdk"
	state "github.com/ahp-platform/ahp/sdk/go/state"
)

type Config struct {
	Tenant, Deployment, AgentID, Digest, Image, Profile, StateURL, ModelURL, Worker string
	GatewayWorkloads                                                                []string
	Client                                                                          *http.Client
	Verifier                                                                        *auth.Verifier
}
type tokenKey struct{}
type Service struct {
	cfg      Config
	state    *state.Client
	healthy  atomic.Int64
	draining atomic.Bool
	// closeRefusals counts the terminal releases this replica tried and the State door
	// refused. A refused terminal is the DEF-H22-3 hazard in the making - the run stays
	// claimable and its thread lease is held until expiry - so it is counted where an
	// operator can read it (/readyz) and logged with its code, never discarded.
	closeRefusals atomic.Int64
}
type payload struct {
	history        []map[string]string // loaded from native State, never accepted over the wire
	Type           string              `json:"type,omitempty"`
	Text           string              `json:"text"`
	Config         json.RawMessage     `json:"config"`
	ConfigRevision int64               `json:"config_revision"`
	Digest         string              `json:"digest"`
	Profile        string              `json:"profile"`
}
type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  struct {
		ID      string `json:"id"`
		Message struct {
			MessageID string `json:"messageId"`
			ContextID string `json:"contextId"`
			TaskID    string `json:"taskId"`
			Parts     []struct {
				Kind string `json:"kind"`
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"message"`
		Configuration struct {
			Blocking bool `json:"blocking"`
		} `json:"configuration"`
		Metadata struct {
			AHP struct {
				TaskID         string          `json:"task_id"`
				AgentID        string          `json:"agent_id"`
				Digest         string          `json:"version_digest"`
				Config         json.RawMessage `json:"config"`
				ConfigRevision int64           `json:"config_revision"`
			} `json:"ahp"`
		} `json:"metadata"`
	} `json:"params"`
}

func New(cfg Config) (*Service, error) {
	if cfg.Tenant == "" || cfg.Deployment == "" || cfg.AgentID == "" || cfg.Digest == "" || cfg.Image == "" || cfg.Profile == "" || cfg.Worker == "" || cfg.Verifier == nil || cfg.Client == nil || len(cfg.GatewayWorkloads) == 0 || !strings.HasPrefix(cfg.ModelURL, "https://") {
		return nil, fmt.Errorf("agent runtime configuration incomplete")
	}
	client, err := state.New(state.Config{BaseURL: cfg.StateURL, HTTPClient: cfg.Client, TokenSource: func(ctx context.Context) (string, error) {
		v, _ := ctx.Value(tokenKey{}).(string)
		if v == "" {
			return "", fmt.Errorf("missing per-task token")
		}
		return v, nil
	}})
	if err != nil {
		return nil, err
	}
	return &Service{cfg: cfg, state: client}, nil
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.URL.Path == "/readyz" {
		ready := !s.draining.Load() && time.Now().Unix()-s.healthy.Load() < 15
		code := http.StatusOK
		if !ready {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]any{"ready": ready, "agent_id": s.cfg.AgentID, "version_digest": s.cfg.Digest, "image": s.cfg.Image,
			"durable_close_refusals": s.closeRefusals.Load()})
		return
	}

	if r.URL.Path != "/a2a" || r.Method != http.MethodPost {
		http.Error(w, "not found", 404)
		return
	}
	claims, token, err := s.authenticate(r)
	if err != nil {
		http.Error(w, "forbidden", 403)
		return
	}
	var in rpcRequest
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&in) != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	ctx := context.WithValue(r.Context(), tokenKey{}, token)
	switch in.Method {
	case "SendMessage", "message/send":
		if s.draining.Load() {
			http.Error(w, "draining", 503)
			return
		}
		meta := in.Params.Metadata.AHP
		if meta.TaskID != claims.Task || meta.AgentID != s.cfg.AgentID || meta.Digest != s.cfg.Digest {
			http.Error(w, "target identity mismatch", 403)
			return
		}
		if in.Params.Message.ContextID != "" && in.Params.Message.ContextID != claims.Context || in.Params.Message.TaskID != "" && in.Params.Message.TaskID != claims.Task {
			http.Error(w, "message context/task contradicts authenticated authority", http.StatusForbidden)
			return
		}
		if len(in.Params.Message.MessageID) > 512 || strings.ContainsAny(in.Params.Message.MessageID, "\x00\r\n") {
			http.Error(w, "invalid message identity", http.StatusBadRequest)
			return
		}
		text := ""
		for _, p := range in.Params.Message.Parts {
			if p.Kind == "text" {
				text += p.Text
			}
		}
		if text == "" {
			http.Error(w, "text input required", 400)
			return
		}
		doc, _ := json.Marshal(payload{Type: nativeRunType(s.cfg.Digest), Text: text, Config: meta.Config, ConfigRevision: meta.ConfigRevision, Digest: s.cfg.Digest, Profile: s.cfg.Profile})
		thread, err := s.state.CreateThread(ctx, claims.Context, "ctx:"+claims.Context)
		if err != nil {
			s.rpcError(w, in.ID, err)
			return
		}
		run, err := s.state.CreateRun(ctx, thread.ID, "task:"+claims.Task, doc)
		if err != nil {
			s.rpcError(w, in.ID, err)
			return
		}
		if in.Params.Configuration.Blocking {
			wait, stop := context.WithTimeout(ctx, 110*time.Second)
			defer stop()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for !state.IsTerminalStatus(run.Status) {
				select {
				case <-wait.Done():
					s.rpcError(w, in.ID, wait.Err())
					return
				case <-ticker.C:
				}
				run, err = s.state.GetRun(wait, run.ID)
				if err != nil {
					s.rpcError(w, in.ID, err)
					return
				}
			}
		}
		s.taskResponse(ctx, w, in.ID, claims.Task, run)
	case "GetTask", "tasks/get":
		if in.Params.ID != claims.Task {
			http.Error(w, "task mismatch", 403)
			return
		}
		thread, err := s.state.GetThreadByContext(ctx, claims.Context)
		if err != nil {
			s.rpcError(w, in.ID, err)
			return
		}
		runs, err := s.state.ListRuns(ctx, thread.ID)
		if err != nil {
			s.rpcError(w, in.ID, err)
			return
		}
		for _, run := range runs {
			if run.TaskID == claims.Task {
				s.taskResponse(ctx, w, in.ID, claims.Task, run)
				return
			}
		}
		s.rpcError(w, in.ID, contracts.NewError(contracts.ErrNotFound, "task not found"))
	default:
		s.rpcError(w, in.ID, contracts.NewError(contracts.ErrInvalid, "unsupported A2A method"))
	}
}

func (s *Service) authenticate(r *http.Request) (auth.Claims, string, error) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return auth.Claims{}, "", errors.New("verified gateway certificate required")
	}
	uris := r.TLS.VerifiedChains[0][0].URIs
	if len(uris) != 1 {
		return auth.Claims{}, "", errors.New("one workload URI required")
	}
	allowed := false
	for _, id := range s.cfg.GatewayWorkloads {
		if uris[0].String() == id {
			allowed = true
		}
	}
	if !allowed {
		return auth.Claims{}, "", errors.New("sender is not gateway")
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return auth.Claims{}, "", errors.New("bearer missing")
	}
	claims, err := s.cfg.Verifier.Verify(token, "ahp", auth.KindTask, auth.WorkloadID(contracts.Scope{Tenant: s.cfg.Tenant, Deployment: s.cfg.Deployment}))
	return claims, token, err
}

func (s *Service) taskResponse(ctx context.Context, w http.ResponseWriter, id json.RawMessage, taskID string, run state.Run) {
	status := map[string]string{state.RunPending: "submitted", state.RunRunning: "working", state.RunDone: "completed", state.RunFailed: "failed", state.RunCanceled: "canceled"}[run.Status]
	if status == "" {
		status = "working"
	}
	task := map[string]any{"id": taskID, "kind": "task", "status": map[string]any{"state": status}, "metadata": map[string]any{"run_id": run.ID, "thread_id": run.ThreadID, "agent_id": s.cfg.AgentID, "version_digest": s.cfg.Digest, "image": s.cfg.Image}}
	if run.Status == state.RunDone {
		cp, err := s.state.GetCheckpoint(ctx, run.ThreadID, run.ID, "")
		if err != nil {
			s.rpcError(w, id, err)
			return
		}
		for _, p := range cp.Parts {
			if p.Channel == "output" {
				// GetCheckpoint returns channel/version references, never blob bytes.
				data, err := s.state.GetPart(ctx, run.ThreadID, cp.Namespace, p.Channel, p.Version)
				if err != nil {
					s.rpcError(w, id, err)
					return
				}
				task["artifacts"] = []any{map[string]any{"artifactId": cp.ID, "parts": []any{map[string]any{"kind": "text", "text": string(data)}}}}
			}
		}
	}
	writeJSON(w, 200, map[string]any{"jsonrpc": "2.0", "id": id, "result": task})
}
func (s *Service) rpcError(w http.ResponseWriter, id json.RawMessage, err error) {
	code := contracts.Code(err)
	if code == "" {
		code = contracts.ErrUnavailable
	}
	writeJSON(w, 200, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32000, "message": code}})
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Run is a durable deployment-wide claim loop. After restart it reads the
// claimed run's payload, never the last in-memory HTTP request. Model calls use
// stable operation keys; terminal output is checkpointed before lease release.
func (s *Service) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if s.draining.Load() {
			return
		}
		claim, err := s.state.ClaimRunWithFilter(ctx, s.cfg.Worker, 30000, state.ClaimFilter{PayloadType: nativeRunType(s.cfg.Digest)})
		if err == nil {
			s.healthy.Store(time.Now().Unix())
		}
		if err != nil || claim == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		s.execute(ctx, claim)
	}
}

// Compatible versions share the logical deployment and conversation, but only
// their own version may claim each accepted payload. State still enforces FIFO.
func nativeRunType(digest string) string {
	return fmt.Sprintf("agentworkload:%x", sha256.Sum256([]byte(digest)))
}
func (s *Service) execute(parent context.Context, claim *state.Claimed) {
	ctx, cancel := context.WithTimeout(context.WithValue(parent, tokenKey{}, claim.TaskToken), 100*time.Second)
	defer cancel()
	fence := state.Fence{RunID: claim.Lease.Run.ID, Worker: claim.Lease.Worker, Epoch: claim.Lease.Epoch}
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := s.state.Heartbeat(ctx, fence, 30000); err != nil {
					cancel()
					return
				}
				s.healthy.Store(time.Now().Unix())
			}
		}
	}()
	// The run ends one of two ways, and since DEF-RELEASE-REASON-1 only one of them may
	// travel without a reason: done means the durable checkpoint carries the verdict,
	// failed means nobody does unless this replica says why. So `cause` is written at
	// every exit that leaves the run failed, and the deferred close below refuses to
	// discard the door's answer. No invented verdict: each sentence names the branch that
	// actually took this run out.
	status, cause := state.RunFailed, ""
	defer func() {
		cancel()
		<-heartbeatDone
		closeCtx, done := context.WithTimeout(context.WithValue(context.Background(), tokenKey{}, claim.TaskToken), 5*time.Second)
		defer done()
		// A completed run states nothing on the wire: the durable checkpoint IS its
		// verdict, and a reason key here would change the request shape of every healthy
		// run in the fleet. Any other end owes the door a reason (DEF-RELEASE-REASON-1),
		// so a branch that forgot to say more falls back to the one sentence that is true
		// of every path that reaches the close without a checkpoint.
		stated := ""
		if status != state.RunDone {
			stated = cause
			if stated == "" {
				stated = defaultFailedCause
			}
		}
		if err := s.state.ReleaseWithReasonCause(closeCtx, fence, status, terminalCloseReason(status), stated); err != nil {
			s.observeRefusedClose(claim.Lease.Run.ID, status, err)
		}
	}()
	var p payload
	if json.Unmarshal(claim.Lease.Run.Payload, &p) != nil || p.Digest != s.cfg.Digest || p.Profile != s.cfg.Profile {
		cause = "agent workload: the claimed run's payload does not match this deployment's published digest or profile"
		return
	}
	saver := sdk.NewCheckpointSaver(s.state, fence, claim.Lease.Thread.ID)
	if _, err := saver.GetLatest(ctx, claim.Lease.Run.ID); err == nil {
		status = state.RunDone
		cause = ""
		return
	} else if contracts.Code(err) != contracts.ErrNotFound {
		cause = boundedCause("agent workload: the run's checkpoint history could not be read: ", err)
		return
	}
	var err error
	p.history, err = s.priorConversation(ctx, claim, saver)
	if err != nil {
		cause = boundedCause("agent workload: native conversation could not be restored: ", err)
		return
	}
	output, err := s.generate(ctx, claim.Lease.Run.ID, p)
	if err != nil {
		cause = boundedCause("agent workload: the model gateway call failed: ", err)
		return
	}
	meta, _ := json.Marshal(map[string]any{"format": nativeConversationFormat, "agent_id": s.cfg.AgentID, "version_digest": p.Digest, "image": s.cfg.Image, "profile": p.Profile, "config_revision": p.ConfigRevision})
	_, err = saver.PutCheckpoint(ctx, claim.Lease.Run.ID, state.Checkpoint{ID: contracts.NewID(), Namespace: claim.Lease.Run.ID, Version: 1, Metadata: meta, Parts: []state.CheckpointPart{{Channel: "input", Version: "1", Data: []byte(p.Text)}, {Channel: "output", Version: "1", Data: []byte(output)}}}, "result:"+claim.Lease.Run.ID)
	if err == nil {
		status = state.RunDone
		cause = ""
		return
	}
	cause = boundedCause("agent workload: the durable output checkpoint could not be written: ", err)
}

// defaultFailedCause is the fallback sentence for a run this replica ended without a
// checkpoint. It says which fact is missing and nothing else: it is what the deferred
// close knows, not a guess about which branch produced it.
const defaultFailedCause = "agent workload: claimed run ended without a durable output checkpoint"

// terminalCloseReason gives the door the execution plane's dictionary word for how this
// replica ended a run. It is a literal, not an import of internal/gateway, for the same
// layering reason internal/state/close_reason.go gives for the payload key. done needs
// no word: that status is lawful unstated.
func terminalCloseReason(status string) string {
	if status == state.RunDone {
		return ""
	}
	return "error"
}

// boundedCause keeps a durable cause to the plane's own sentence: prefixed, capped, and
// made of the failure's code and message rather than an unbounded upstream body.
func boundedCause(prefix string, err error) string {
	cause := prefix + err.Error()
	if len(cause) > 240 {
		cause = cause[:240]
	}
	return cause
}

// observeRefusedClose makes a terminal the State door refused observable. Discarding it
// was the defect: the run stays running, its thread lease stays held, and the next claim
// re-does work this replica already finished. The code travels in the log so a reader can
// tell a fence refusal from a policy refusal without the message being a secret.
func (s *Service) observeRefusedClose(runID, status string, err error) {
	s.closeRefusals.Add(1)
	code := contracts.Code(err)
	if code == "" {
		code = contracts.ErrUnavailable
	}
	slog.Error("agent workload: State refused the run's terminal release; the run stays claimable until its lease lapses",
		"run_id", runID, "status", status, "code", code, "refusals", s.closeRefusals.Load(), "err", err.Error())
}

type generation struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Output string `json:"output"`
}

func (s *Service) modelCall(ctx context.Context, op string, in any) (generation, error) {
	raw, _ := json.Marshal(in)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.cfg.ModelURL, "/")+"/v1/model/"+op, bytes.NewReader(raw))
	if err != nil {
		return generation{}, err
	}
	token, _ := ctx.Value(tokenKey{}).(string)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.cfg.Client.Do(req)
	if err != nil {
		return generation{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return generation{}, fmt.Errorf("model gateway status %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return generation{}, err
	}
	var g generation
	if err = json.Unmarshal(b, &g); err != nil {
		return g, err
	}
	if g.ID == "" {
		var w struct {
			Generation generation `json:"generation"`
		}
		_ = json.Unmarshal(b, &w)
		g = w.Generation
	}
	if g.ID == "" {
		return g, fmt.Errorf("model generation missing")
	}
	return g, nil
}
func (s *Service) generate(ctx context.Context, runID string, p payload) (string, error) {
	var config struct {
		Prompt    string `json:"prompt"`
		Model     string `json:"model"`
		MaxTokens int    `json:"max_tokens"`
	}
	if len(p.Config) > 0 && json.Unmarshal(p.Config, &config) != nil {
		return "", fmt.Errorf("invalid pinned config")
	}
	if config.MaxTokens == 0 {
		config.MaxTokens = 4096
	}
	if config.MaxTokens < 128 || config.MaxTokens > 4096 {
		return "", fmt.Errorf("pinned max_tokens must be between 128 and 4096")
	}
	prompt := s.cfg.Profile
	if config.Prompt != "" {
		prompt += "\n" + config.Prompt
	}
	messages := []map[string]string{{"role": "system", "content": prompt}}
	messages = append(messages, p.history...)
	messages = append(messages, map[string]string{"role": "user", "content": p.Text})
	g, err := s.modelCall(ctx, "SubmitModel", map[string]any{"key": "agent-model:" + runID, "model": config.Model, "messages": messages, "max_tokens": config.MaxTokens})
	if err != nil {
		return "", err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		switch strings.ToUpper(g.Status) {
		case "COMPLETED":
			return g.Output, nil
		case "FAILED", "CANCELED":
			return "", fmt.Errorf("model did not complete")
		}
		select {
		case <-ctx.Done():
			cleanup, stop := context.WithTimeout(context.WithValue(context.Background(), tokenKey{}, ctx.Value(tokenKey{})), 3*time.Second)
			_, _ = s.modelCall(cleanup, "CancelModel", map[string]string{"id": g.ID})
			stop()
			return "", ctx.Err()
		case <-ticker.C:
		}
		g, err = s.modelCall(ctx, "GetModel", map[string]string{"id": g.ID})
		if err != nil {
			return "", err
		}
	}
}

func (s *Service) Drain() { s.draining.Store(true) }
