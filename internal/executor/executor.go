package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Executor is the canonical Qwen+tools -> a2a gateway.Executor. It is safe for
// concurrent use: per-turn state (scope, task id) travels via context, and the
// core.Runtime + artifacts store are stateless across calls.
type Executor struct {
	store    *artifacts.Store
	minio    *minio.Client
	bucket   string
	model    core.ModelClient
	tools    *core.ToolRegistry
	maxSteps int
}

// compile-time assertion that the executor satisfies the gateway seam.
var _ gateway.Executor = (*Executor)(nil)

// turnCtx carries per-attempt identity to the tool layer (which the core loop
// does not thread through) so artifacts are written under the correct tenant
// scope and idempotency key.
type turnCtx struct {
	scope  contracts.Scope
	taskID string
	// contextID is the State thread / A2A context the attempt belongs to. It is
	// carried with the task identity because an artifact written without it is
	// invisible to the context cascade -- half a deletion obligation.
	contextID string
}

type turnCtxKey struct{}

func withTurn(ctx context.Context, t turnCtx) context.Context {
	return context.WithValue(ctx, turnCtxKey{}, t)
}

func turnFromCtx(ctx context.Context) turnCtx {
	if v, ok := ctx.Value(turnCtxKey{}).(turnCtx); ok {
		return v
	}
	return turnCtx{}
}

// Execute runs one fenced attempt: a real native-Qwen model turn plus any tool
// round-trips, ending in a terminal A2A state with real, sealed artifacts.
func (e *Executor) Execute(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
	if err := in.Scope.Validate(); err != nil {
		return gateway.ExecResult{State: a2a.TaskStateFailed}, err
	}
	input := messageText(in.Input)
	if strings.TrimSpace(input) == "" {
		return gateway.ExecResult{
			State:   a2a.TaskStateFailed,
			Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("executor: no usable text input")),
		}, nil
	}

	attempt := withTurn(ctx, turnCtx{scope: in.Scope, taskID: string(in.Task.ID), contextID: in.Task.ContextID})

	// Same contract as the Model Gateway bridge: the caller's stated budget bounds
	// this attempt, the configured number applies only when none was stated, and
	// whichever was in force is reported back in Stats. Both executors must report
	// the same way, otherwise the observability depends on which wiring a
	// deployment happens to use.
	stepBudget := e.maxSteps
	if in.MaxSteps > 0 {
		stepBudget = in.MaxSteps
	}
	req := &space.CoreRequest{Scope: in.Scope, Input: jsonString(prompt(input)), MaxSteps: stepBudget}
	runtime := core.NewRuntime(core.RuntimeConfig{
		Model: e.model,
		Tools: e.tools,
	})

	results := make(chan space.StepResult, 64)
	var runErr error
	go func() {
		runErr = runtime.Run(attempt, req, results)
		close(results)
	}()

	var (
		finalText string
		produced  []artifactResult
		sawError  bool
		modelTurn int
		errText   string
	)
	for sr := range results {
		switch sr.StepType {
		case space.StepModel:
			modelTurn++
			if txt := contentText(sr.Content); txt != "" {
				finalText = txt
			}
		case space.StepTool:
			if sr.ToolCall != nil && sr.ToolCall.Name == artifactToolName {
				if ar, ok := parseArtifact(sr.Content); ok {
					produced = append(produced, ar)
				} else if contentIsError(sr.Content) {
					sawError = true
					if errText == "" {
						errText = contentText(sr.Content)
					}
				}
			}
		case space.StepError:
			sawError = true
			if errText == "" {
				errText = contentText(sr.Content)
			}
		}
	}

	if cerr := ctx.Err(); cerr != nil {
		return gateway.ExecResult{}, cerr
	}

	// A model/tool failure after the turn did not complete: report a terminal
	// Failed so the gateway retries. The artifact write is idempotent on
	// (task, sha), so a retry never duplicates a sealed artifact.
	if runErr != nil || (sawError && len(produced) == 0) {
		// The cause this process already computed must reach the operator (journal)
		// and the caller (bounded, sanitized cause in the terminal status text).
		// Neither surface claims anything about whether an effect happened: that
		// verdict belongs to the operation ledger, which stays SIDE_EFFECT_UNKNOWN.
		journalTurnFailure(string(in.Task.ID), runErr, errText)
		reason := failureCause("executor: model/tool turn failed", runErr, errText)
		return gateway.ExecResult{
			State:   a2a.TaskStateFailed,
			Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(truncate(reason, 512))),
			Stats: gateway.ExecStats{
				StepsConsumed: modelTurn,
				StepBudget:    stepBudget,
				CloseReason: gateway.ClassifyCloseReason(runErr, errText,
					sawError && len(produced) == 0, finalText, modelTurn, stepBudget),
			},
		}, nil
	}

	if finalText == "" && len(produced) == 0 {
		return gateway.ExecResult{
			State:   a2a.TaskStateFailed,
			Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("executor: turn produced no output")),
			Stats: gateway.ExecStats{
				StepsConsumed: modelTurn,
				StepBudget:    stepBudget,
				CloseReason:   gateway.CloseReasonError,
			},
		}, nil
	}

	artifactsOut := make([]*a2a.Artifact, 0, len(produced))
	for _, ar := range produced {
		artifactsOut = append(artifactsOut, &a2a.Artifact{
			ID:   a2a.ArtifactID(ar.ArtifactID),
			Name: ar.Name,
			Metadata: map[string]any{
				"sha256": ar.SHA256,
				"size":   ar.Size,
				"mime":   ar.MIME,
			},
			Parts: a2a.ContentParts{a2a.NewTextPart(
				fmt.Sprintf("sealed artifact %q sha256=%s size=%d", ar.Name, ar.SHA256, ar.Size))},
		})
	}

	return gateway.ExecResult{
		State:     a2a.TaskStateCompleted,
		Message:   a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(finalText)),
		Artifacts: artifactsOut,
		Stats: gateway.ExecStats{
			StepsConsumed: modelTurn,
			StepBudget:    stepBudget,
			CloseReason: gateway.ClassifyCloseReason(nil, errText,
				sawError && len(produced) == 0, finalText, modelTurn, stepBudget),
		},
	}, nil
}

// OpenArtifact reads the sealed bytes of an artifact under scope and returns
// them with the verified ref. A foreign tenant receives NOT_FOUND (enforced in
// internal/artifacts). Used by the durable GET-after-restart / isolation checks.
func (e *Executor) OpenArtifact(ctx context.Context, scope contracts.Scope, id string) ([]byte, artifacts.ArtifactRef, error) {
	rc, ref, err := e.store.Open(ctx, scope, id)
	if err != nil {
		return nil, artifacts.ArtifactRef{}, err
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, artifacts.ArtifactRef{}, err
	}
	return data, ref, nil
}

// ReadArtifact returns a sealed artifact ref under scope, and NOT_FOUND for a
// foreign tenant (enforced inside internal/artifacts). Exposed for the durable
// GET-after-restart and tenant-isolation checks the contract requires.
func (e *Executor) ReadArtifact(ctx context.Context, scope contracts.Scope, id string) (artifacts.ArtifactRef, error) {
	return e.store.Get(ctx, scope, id)
}

func parseArtifact(raw json.RawMessage) (artifactResult, bool) {
	if len(raw) == 0 {
		return artifactResult{}, false
	}
	var ar artifactResult
	if err := json.Unmarshal(raw, &ar); err != nil {
		return artifactResult{}, false
	}
	if ar.ArtifactID == "" || ar.SHA256 == "" {
		return artifactResult{}, false
	}
	return ar, true
}

func contentIsError(raw json.RawMessage) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return false
	}
	_, ok := m["error"]
	return ok
}

// messageText returns the concatenated text parts of a2a message.
func messageText(m *a2a.Message) string {
	if m == nil {
		return ""
	}
	var b strings.Builder
	for _, p := range m.Parts {
		if t := p.Text(); t != "" {
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(t)
		}
	}
	return b.String()
}

// contentText decodes a core model step's JSON content (a bare JSON string) to
// plain text; non-string content is rendered verbatim.
func contentText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

func prompt(user string) string {
	return "You are the AHP execution agent. You MUST call the write_artifact tool exactly once " +
		"to persist the artifact the user asks for, then reply with a short confirmation. " +
		"Never echo the user verbatim as the artifact; produce the artifact the request implies.\n\nUser: " + user
}

func jsonString(s string) json.RawMessage {
	b, err := json.Marshal(s)
	if err != nil {
		return json.RawMessage(`""`)
	}
	return b
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
