package execbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Guard the reviewed hot caller: putting full Get/GetForStream back in this
// loop would aggregate an entire ledger every40ms while the public stream uses
// only delta pages plus metadata until it sends the terminal generation.
func TestModelStreamHotLoopUsesBoundedSnapshotReader(t *testing.T) {
	tree, err := parser.ParseFile(token.NewFileSet(), "model_stream.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, full := 0, 0
	ast.Inspect(tree, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch selector.Sel.Name {
		case "GetStreamSnapshot":
			snapshot++
		case "GetForStream", "Get":
			full++
		}
		return true
	})
	if snapshot != 1 || full != 0 {
		t.Fatalf("hot stream reader changed: snapshots=%d full=%d", snapshot, full)
	}
}

func TestDeltaLedgerWireTerminalPreservesCompleteGenerationAndRequestOmission(t *testing.T) {
	pool := dbPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	release := make(chan struct{})
	close(release)
	inference := &gatedInference{release: release, stopped: make(chan struct{}, 1)}
	bridge, err := New(ctx, pool, Config{Inference: inference, TaskTokenLimit: 1000000})
	if err != nil {
		t.Fatal(err)
	}
	scope := uniqueScope()
	input := strings.Repeat("x", 102400)
	body, _ := json.Marshal(map[string]any{"key": "terminal-projection", "max_tokens": 128, "messages": []map[string]string{{"role": "user", "content": input}}})
	request := httptest.NewRequest("POST", "/v1/model/StreamModel", bytes.NewReader(body))
	claims := auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Subject: scope.Subject, Deployment: scope.Deployment, Task: "task", Context: "ctx"}
	request = request.WithContext(transport.WithPrincipal(ctx, transport.Principal{Scope: scope, Claims: claims}))
	response := httptest.NewRecorder()
	bridge.serveModel(response, request)
	if response.Code != 200 {
		t.Fatalf("stream refused status=%d", response.Code)
	}
	decoder := json.NewDecoder(response.Body)
	var terminal *model.Generation
	var generationID string
	var fragments []model.StreamDelta
	terminalCount := 0
	for {
		var frame modelStreamFrame
		err = decoder.Decode(&frame)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch frame.Type {
		case "accepted":
			generationID = frame.GenerationID
		case "delta":
			if frame.Delta == nil {
				t.Fatal("missing delta")
			}
			fragments = append(fragments, *frame.Delta)
		case "error":
			t.Fatal("stream error frame", frame.Code)
		case "terminal":
			terminal = frame.Generation
			terminalCount++
		}
	}
	if terminalCount != 1 || terminal == nil || (len(terminal.Request) != 0 && string(terminal.Request) != "null") || terminal.Status != model.GenCompleted || terminal.Output != "first second" || terminal.DeltaSequence != 2 {
		t.Fatal("terminal lost exact fields or included request")
	}
	if len(fragments) != 2 || fragments[0].Text != "first " || fragments[1].Text != "second" || fragments[0].Sequence != 1 || fragments[1].Sequence != 2 {
		t.Fatal("wire delta boundaries/cursors changed")
	}
	stored, err := bridge.gens.Get(ctx, scope, generationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Request) < 102400 {
		t.Fatal("native stored request lost")
	}
	stored.Request = nil
	expected, _ := json.Marshal(stored)
	actual, _ := json.Marshal(terminal)
	if !bytes.Equal(actual, expected) {
		t.Fatal("terminal wire generation differs from exact scoped Get")
	}
	if inference.calls.Load() != 1 {
		t.Fatal("wire replay dispatched provider again")
	}
}
