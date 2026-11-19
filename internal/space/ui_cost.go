package space

// The human edge may ask what a chat COST, and read only the answer the Model Gateway
// gives (PDF rev3.2 :347 "стоимость задачи").
//
// Why this route exists and why it is not simply the machine face: /v1/model/TreeCost is
// guarded by transport.Task, i.e. it authorizes a SCOPED TASK token whose workload must
// match the mTLS certificate presenting it. A browser holds a human session token and can
// never present a workload certificate, so pointing the UI at that face is not a wiring
// choice but a category error -- measured on the live stand as HTTP 403 "token audience
// does not match". What the browser CAN do is what every other human Space call does:
// present its own verified session and let this process exchange it for the scoped task
// token. That exchange is the only authority this pipe forwards.
//
// What this file is: the same kind of pipe as ui_resolve.go. It checks the shape of the
// question, names the budget-tree task the thread's own run charged (resolved from the
// durable runs of that thread -- never taken from the request), carries the EXCHANGED task
// token to the Model Gateway over Space's own workload mTLS, and passes the gateway's
// answer through verbatim.
//
// What it never does: accept a task id, tenant or subject from the request (a browser may
// name a thread it owns and nothing else); invent or reformat cost fields (the gateway's
// JSON is the contract, so the UI cannot drift into a number the ledger never produced);
// cache (no-store); or turn a gateway refusal into a synthetic zero. Absent cost stays
// absent -- a made-up "0" on a screen is exactly the false success this surface exists to
// avoid.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	// UICostPath is the browser-visible read-only cost route.
	UICostPath = "/u-ahp/v1/model/Cost"

	// costOperation is the Model Gateway route this pipe calls.
	costOperation = "TreeCost"

	// modelAPIPath is the Model Gateway namespace on its machine surface.
	modelAPIPath = "/v1/model/"

	// ReasonThreadRequired is this surface's own refusal: a cost question with no thread.
	ReasonThreadRequired = "thread_required"

	// ReasonNoRunForThread: the thread exists (or does not -- this says nothing about
	// which), but nothing in it ever charged a budget tree, so there is no cost to read.
	ReasonNoRunForThread = "no_run_for_thread"

	costThreadParam          = "thread_id"
	maxCostBodyBytes   int64 = 1 << 20
	maxCostThreadIDLen       = 128
)

// RunTaskResolver names the budget-tree task a thread's most recent run charged. It is
// supplied by the deployment (as-runner resolves it from the thread's durable runs through
// State), because the browser cannot and must not name a task.
type RunTaskResolver func(ctx context.Context, taskToken, threadID string) (string, error)

// CostProxyConfig wires the human cost read. Everything is mandatory for the same reason
// the artifact pipes require it: without Space's own workload identity this process has no
// authority to ask about another principal's consumption.
type CostProxyConfig struct {
	// ModelCostURL is the Model Gateway ORIGIN (scheme://host[:port], no API path --
	// an origin that carries the path is refused at startup, DEF-H24-12's rule).
	ModelCostURL string
	// Client presents Space's OWN workload certificate.
	Client *http.Client
	// Tasks names the thread's run task.
	Tasks RunTaskResolver
	// CallTimeout bounds the machine hop.
	CallTimeout time.Duration
}

type costProxy struct {
	base        string
	client      *http.Client
	tasks       RunTaskResolver
	callTimeout time.Duration
}

// NewCostProxy builds the human-edge cost read.
func NewCostProxy(cfg CostProxyConfig) (http.Handler, error) {
	base, err := artifactsOrigin("the cost face", cfg.ModelCostURL)
	if err != nil {
		return nil, err
	}
	if cfg.Client == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: the cost read needs Space's own workload mTLS client")
	}
	if cfg.Tasks == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: the cost read needs a resolver for the thread's run task")
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = 10 * time.Second
	}
	return &costProxy{base: base, client: cfg.Client, tasks: cfg.Tasks, callTimeout: cfg.CallTimeout}, nil
}

type costCall struct {
	TaskID string `json:"task_id"`
}

func (p *costProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "space: unknown route"))
		return
	}
	threadID := strings.TrimSpace(r.URL.Query().Get(costThreadParam))
	if threadID == "" {
		transport.WriteError(w, r, contracts.NewReasoned(contracts.ErrInvalid, ReasonThreadRequired,
			"space: "+costThreadParam+" is required"))
		return
	}
	if len(threadID) > maxCostThreadIDLen {
		transport.WriteError(w, r, contracts.NewReasoned(contracts.ErrInvalid, ReasonThreadRequired,
			"space: "+costThreadParam+" is too long"))
		return
	}
	token, ok := ScopedTaskFromContext(r.Context())
	if !ok || token == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"space: the cost read needs an exchanged task token (no human JWT is forwarded to the Model Gateway)"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), p.callTimeout)
	defer cancel()

	taskID, err := p.tasks(ctx, token, threadID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if strings.TrimSpace(taskID) == "" {
		transport.WriteError(w, r, contracts.NewReasoned(contracts.ErrNotFound, ReasonNoRunForThread,
			"space: no run of this thread charged a budget tree"))
		return
	}

	body, err := json.Marshal(costCall{TaskID: taskID})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.base+modelAPIPath+costOperation, bytes.NewReader(body))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := p.client.Do(req)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxCostBodyBytes))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// The gateway's answer is the contract: status and body pass through untouched, so a
	// refusal upstream cannot be smoothed into a number here.
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	if r.Method != http.MethodHead {
		_, _ = w.Write(payload)
	}
}
