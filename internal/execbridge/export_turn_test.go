package execbridge

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The export is the only proof this face can offer that a completed turn exists
// outside the process. Two properties were previously asserted nowhere: that a
// failing export cannot be reported as a completion, and that a successful one is
// sealed under the OWNING scope and the task's own id (a seal written under an
// empty or borrowed scope is a leak, not a result).

type recordingExporter struct {
	calls   int
	scope   contracts.Scope
	binding artifacts.Binding
	taskID  string
	name    string
	mime    string
	content string
	err     error
}

func (r *recordingExporter) Seal(_ context.Context, scope contracts.Scope, b artifacts.Binding, name, mime, content string) (executor.SealedArtifact, error) {
	r.calls++
	taskID := b.TaskID
	r.binding = b
	r.scope, r.taskID, r.name, r.mime, r.content = scope, taskID, name, mime, content
	if r.err != nil {
		return executor.SealedArtifact{}, r.err
	}
	sha := strings.Repeat("a", 64)
	return executor.SealedArtifact{ArtifactID: "art_" + taskID, SHA256: sha, Size: int64(len(content)), MIME: mime, Name: name}, nil
}

func exportBridge(t *testing.T, exporter ArtifactExporter, text string) *Bridge {
	t.Helper()
	b, err := New(context.Background(), dbPool(t), Config{
		Inference:      &fakeInference{text: text},
		MaxSteps:       2,
		TaskTokenLimit: 20000,
		StepReserve:    4000,
		Artifacts:      exporter,
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	return b
}

// A turn whose answer cannot be sealed is FAILED with the reason. Reporting it as
// COMPLETED with an empty artifact list is the exact behaviour this face was
// admitted for failing: the budget was spent and the caller received nothing.
func TestExportFailureFailsTheTaskWithReason(t *testing.T) {
	scope := uniqueScope()
	exporter := &recordingExporter{err: errors.New("object storage unreachable (injected)")}
	b := exportBridge(t, exporter, "the answer that cannot be stored")

	taskID := "expfail-" + randHex(4)
	res, err := b.Execute(context.Background(), gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
	})
	if err != nil {
		t.Fatalf("execute must return a terminal result, not an error: %v", err)
	}
	if res.State != a2a.TaskStateFailed {
		t.Fatalf("state = %s, want FAILED: an unsealed result is not a completion", res.State)
	}
	if len(res.Artifacts) != 0 {
		t.Fatalf("a failed export must not claim artifacts: %d", len(res.Artifacts))
	}
	reason := messageText(res.Message)
	for _, want := range []string{"result export failed", "object storage unreachable"} {
		if !strings.Contains(reason, want) {
			t.Fatalf("the failure must name its cause (%q missing in %q)", want, reason)
		}
	}
	if exporter.calls != 1 {
		t.Fatalf("export attempts = %d, want exactly 1 (no retry loop over a sealed write)", exporter.calls)
	}
	if res.Stats.CloseReason != gateway.CloseReasonError {
		t.Fatalf("close_reason = %q, want %q: the model answered, the export did not",
			res.Stats.CloseReason, gateway.CloseReasonError)
	}
	if res.Stats.StepsConsumed != 1 {
		t.Fatalf("steps_consumed = %d, want 1 model turn before the export", res.Stats.StepsConsumed)
	}
}

// A successful export is bound to the owning scope, carries the task id in the
// object name, and carries exactly the turn's answer -- not a prompt echo, not a
// foreign task's bytes.
func TestExportIsSealedUnderOwningScopeAndTaskName(t *testing.T) {
	scope := uniqueScope()
	exporter := &recordingExporter{}
	answer := "один абзац про снимки"
	b := exportBridge(t, exporter, answer)

	taskID := "expok-" + randHex(4)
	res, err := b.Execute(context.Background(), gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("расскажи про снимки")),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != a2a.TaskStateCompleted {
		t.Fatalf("state = %s (%s), want COMPLETED", res.State, messageText(res.Message))
	}
	if exporter.calls != 1 {
		t.Fatalf("export attempts = %d, want 1", exporter.calls)
	}
	if exporter.scope != scope {
		t.Fatalf("sealed under %v, want the owning %v: a seal under another scope is a leak", exporter.scope, scope)
	}
	if exporter.taskID != taskID {
		t.Fatalf("sealed for task %q, want %q", exporter.taskID, taskID)
	}
	if exporter.binding.ContextID != "ctx-"+taskID || exporter.binding.TaskID != taskID {
		t.Fatalf("the export must be bound to the task's own context/task, got %+v: an unbound row leaves the deletion cascade", exporter.binding)
	}
	if !strings.Contains(exporter.name, taskID) {
		t.Fatalf("object name %q must contain the task id, or one task's answer could be read as another's", exporter.name)
	}
	if exporter.content != answer {
		t.Fatalf("sealed content = %q, want the turn's own answer %q", exporter.content, answer)
	}
	if exporter.mime != "text/plain" {
		t.Fatalf("mime = %q, want text/plain", exporter.mime)
	}
	if len(res.Artifacts) != 1 {
		t.Fatalf("artifacts = %d, want the one sealed result", len(res.Artifacts))
	}
	if res.Stats.CloseReason != gateway.CloseReasonModelFinished {
		t.Fatalf("close_reason = %q, want %q", res.Stats.CloseReason, gateway.CloseReasonModelFinished)
	}
}

// No exporter mounted (object storage not configured) is a stated impossibility,
// not a silent artifact-free completion: the bridge must still answer honestly.
func TestExportNotMountedStillCompletesWithoutArtifacts(t *testing.T) {
	scope := uniqueScope()
	b := exportBridge(t, nil, "answer with nowhere to go")
	res, err := b.Execute(context.Background(), gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: "expnone-" + randHex(4), ContextID: "ctx-expnone"},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != a2a.TaskStateCompleted || len(res.Artifacts) != 0 {
		t.Fatalf("state=%s artifacts=%d: without a mounted plane the face answers text-only", res.State, len(res.Artifacts))
	}
}
