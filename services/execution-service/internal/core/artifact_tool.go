package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ToolSaveArtifact is the Core tool name that exports a generated file into the
// AHP Artifacts store — the PDF "Tool GW -> export to Artifacts -> Seal" path.
// It is AHP's OWN artifact tool: the model's tool-call arguments produce real
// bytes that are sealed as a versioned artifact under the run's task scope. It
// exists so the artifact-producing journey runs through our own Tool->Artifacts
// path under our orchestration, not solely through an external agent container.
const ToolSaveArtifact = "save_artifact"

// ArtifactHandle identifies a sealed, versioned artifact produced by
// save_artifact. It carries exactly what a downloader needs to fetch the same
// bytes back (id + version + digest) without coupling internal/core to the
// Data/artifacts package.
type ArtifactHandle struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
}

// ArtifactSink persists bytes as a sealed, versioned artifact under the run's
// scope and returns a handle. The production implementation wraps the AHP
// Data/artifacts store; the live journey wires a real store, unit tests a fake.
// The sink is expected to read the run scope from ctx (see RunScopeFromContext)
// so the Seal carries the task's labels, per the PDF.
type ArtifactSink interface {
	SaveArtifact(ctx context.Context, name, mime string, data []byte) (ArtifactHandle, error)
}

// runScope carries the per-run task scope and token so a scope-aware tool can
// act under the task's identity. PDF: "file at Seal gets the labels of the task
// that created it" — so artifact writes must be scoped to the current run, not a
// deployment-wide constant.
type runScopeKey struct{}

type runScope struct {
	Scope     contracts.Scope
	TaskToken string
}

// WithRunScope annotates ctx with the run's scope and task token for tool
// executors that must act under the task's identity (e.g. save_artifact). The
// Runtime injects this before dispatching a tool call.
func WithRunScope(ctx context.Context, scope contracts.Scope, taskToken string) context.Context {
	return context.WithValue(ctx, runScopeKey{}, runScope{Scope: scope, TaskToken: taskToken})
}

// RunScopeFromContext returns the scope/token the runtime injected, if any. The
// bool is false when no scope is present (e.g. a tool invoked outside a run).
func RunScopeFromContext(ctx context.Context) (contracts.Scope, string, bool) {
	rs, ok := ctx.Value(runScopeKey{}).(runScope)
	if !ok {
		return contracts.Scope{}, "", false
	}
	return rs.Scope, rs.TaskToken, true
}

// SaveArtifactTool is a real (non-mock) Core tool. It derives artifact bytes
// from the model's own tool-call arguments and writes them through the injected
// sink to the Artifacts store, returning the sealed reference. An empty sink or
// malformed args is an explicit error — it never fabricates an artifact.
type SaveArtifactTool struct {
	Sink ArtifactSink
}

// NewSaveArtifactTool wires the tool to a sink.
func NewSaveArtifactTool(sink ArtifactSink) *SaveArtifactTool {
	return &SaveArtifactTool{Sink: sink}
}

// Def returns the Core tool definition exposed to the model.
func (SaveArtifactTool) Def() ToolDef {
	return ToolDef{
		Name: ToolSaveArtifact,
		Description: "Save generated content as a sealed, downloadable file artifact. " +
			"Call this whenever the user asks for a report, table, or generated file. " +
			"Args: {\"name\": filename, \"content\": the full file text, \"mime\": optional media type}.",
		Parameters: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"},"content":{"type":"string"},"mime":{"type":"string"}},"required":["name","content"]}`),
	}
}

// Execute implements ToolExecutor for the save_artifact tool.
func (t *SaveArtifactTool) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	if name != ToolSaveArtifact {
		return nil, fmt.Errorf("save_artifact: unexpected tool name %q", name)
	}
	if t.Sink == nil {
		return nil, fmt.Errorf("save_artifact: no artifact sink configured")
	}
	var in struct {
		Name    string `json:"name"`
		Content string `json:"content"`
		MIME    string `json:"mime"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("save_artifact: bad args: %w", err)
	}
	if in.Name == "" || in.Content == "" {
		return nil, fmt.Errorf("save_artifact: name and content are required")
	}
	mime := in.MIME
	if mime == "" {
		mime = "text/plain"
	}
	h, err := t.Sink.SaveArtifact(ctx, in.Name, mime, []byte(in.Content))
	if err != nil {
		return nil, fmt.Errorf("save_artifact: %w", err)
	}
	// Backfill digest/size defensively so the caller/UI can verify bytes even if
	// the sink only returns an id+version.
	if h.SHA256 == "" {
		sum := sha256.Sum256([]byte(in.Content))
		h.SHA256 = hex.EncodeToString(sum[:])
	}
	if h.Size == 0 {
		h.Size = int64(len(in.Content))
	}
	return json.Marshal(h)
}

// MultiExecutor routes each tool call to a per-tool executor so one
// ToolRegistry can expose several real tools (e.g. save_artifact + an
// A2A-delegation adapter). It never fabricates a result: an unmapped tool is a
// hard error, which keeps "no executor" distinguishable from a real tool fault.
type MultiExecutor struct {
	byName map[string]ToolExecutor
}

// NewMultiExecutor creates an empty composite executor.
func NewMultiExecutor() *MultiExecutor {
	return &MultiExecutor{byName: map[string]ToolExecutor{}}
}

// Register binds a tool name to an executor. Later registrations win.
func (m *MultiExecutor) Register(name string, ex ToolExecutor) { m.byName[name] = ex }

// Execute dispatches to the executor bound to name.
func (m *MultiExecutor) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	ex, ok := m.byName[name]
	if !ok {
		return nil, fmt.Errorf("no executor registered for tool %q", name)
	}
	return ex.Execute(ctx, name, args)
}
