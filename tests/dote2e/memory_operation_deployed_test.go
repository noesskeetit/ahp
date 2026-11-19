package dote2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/sdk/go/sharedmemory"
)

func TestSharedMemoryKeyFenceOnDeployedOwner(t *testing.T) {
	stand := loadStand(t)
	checks := map[string]any{}
	defer func() {
		verdict := "PASS"
		if t.Failed() {
			verdict = "FAIL"
			checks["test_failure"] = map[string]any{"status": "FAIL"}
		}
		stand.record(t, "shared-memory-operation-fence", verdict, checks)
	}()
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	otherHuman := stand.humanToken(t, stand.HTTP, mustLogin(t, "admin_email"), mustLogin(t, "admin_password"))
	stamp := strconv.FormatInt(time.Now().UnixNano(), 10)
	token, _ := stand.taskGrant(t, stand.HTTP, human, "memory-operations-"+stamp, []string{"memory", "state"})
	otherToken, _ := stand.taskGrant(t, stand.HTTP, otherHuman, "memory-operations-other-"+stamp, []string{"memory", "state"})
	newClient := func(tok string) *sharedmemory.Client {
		t.Helper()
		c, err := sharedmemory.New(sharedmemory.Config{BaseURL: stand.Origin, HTTPClient: stand.HTTP, TokenSource: func(context.Context) (string, error) { return tok, nil }})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c, foreign := newClient(token), newClient(otherToken)
	closed := "fenced-" + stamp
	op, err := c.Fence(t.Context(), closed)
	if err != nil || op.Status != "FENCED" || op.OpKey != closed {
		t.Fatalf("actual per-key fence: %+v err=%v", op, err)
	}
	op, err = c.GetOperation(t.Context(), closed)
	if err != nil || op.Status != "FENCED" {
		t.Fatalf("actual fenced receipt: %+v err=%v", op, err)
	}
	_, err = c.Propose(t.Context(), closed, sharedmemory.ProposeRequest{Kind: sharedmemory.User, Text: "this cancelled fact must not be committed"})
	var failure *sharedmemory.Error
	if !errors.As(err, &failure) || failure.Status != 409 || failure.Reason != "FENCED" {
		t.Fatalf("post-fence proposal: %v", err)
	}
	checks["precommit_fence"] = map[string]any{"status": "PASS", "post_fence_http_status": 409, "reason": "FENCED"}
	key := "committed-" + stamp
	input := sharedmemory.ProposeRequest{Kind: sharedmemory.User, Text: "keep the committed synthetic memory after Fence"}
	p, err := c.Propose(t.Context(), key, input)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []func(context.Context, string) (sharedmemory.Operation, error){c.GetOperation, c.Fence, c.GetOperation} {
		op, err := f(t.Context(), key)
		if err != nil || op.Status != "SUCCEEDED" || op.ResultID != p.Memory.ID {
			t.Fatalf("committed operation receipt: %+v err=%v", op, err)
		}
	}
	m, err := c.Read(t.Context(), p.Memory.ID)
	if err != nil || m.Text != input.Text {
		t.Fatalf("Fence erased committed content: %v", err)
	}
	replay, err := c.Propose(t.Context(), key, input)
	if err != nil || !replay.Replay || replay.Memory.ID != m.ID {
		t.Fatalf("Fence changed committed replay: %v", err)
	}
	checks["committed_result_preserved"] = map[string]any{"status": "PASS", "memory_id": m.ID, "replay_same_id": true}
	for _, f := range []func(context.Context, string) (sharedmemory.Operation, error){foreign.GetOperation, foreign.Fence} {
		_, err := f(t.Context(), key)
		if !errors.As(err, &failure) || failure.Status != 404 {
			t.Fatalf("foreign subject accessed operation: %v", err)
		}
	}
	_, err = foreign.Propose(t.Context(), key, input)
	if !errors.As(err, &failure) || failure.Status != 400 || failure.Code != "INVALID_ARGUMENT" || failure.Reason != "KEY_REUSE_MISMATCH" {
		t.Fatalf("deployment-wide key namespace: %v", err)
	}
	checks["foreign_subject"] = map[string]any{"status": "PASS", "get_fence_http_status": 404, "mutation_reuse": "KEY_REUSE_MISMATCH"}
	python := os.Getenv("AHP_DOT_E2E_PYTHON")
	if python == "" {
		t.Fatal("actual Python SDK interpreter required")
	}
	payload := map[string]any{"action": "operations", "origin": stand.Origin, "token": token, "ca": filepath.Join(stand.Dir, "certs/ca.crt"), "cert": filepath.Join(stand.Dir, "certs/operator.crt"), "key": filepath.Join(stand.Dir, "certs/operator.key"), "op_key": key, "closed_key": closed, "id": m.ID}
	wireInput, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, filepath.Join("fixtures", "shared_memory_sdk.py"))
	cmd.Stdin = bytes.NewReader(wireInput)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(cwd, "../../sdk/python"), "PYTHONDONTWRITEBYTECODE=1")
	var output, diagnostics bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &diagnostics
	if err = cmd.Run(); err != nil {
		t.Fatalf("Python actual operation owner leg failed (diagnostics withheld): %v", err)
	}
	var result map[string]any
	if json.Unmarshal(output.Bytes(), &result) != nil || result["status"] != "PASS" {
		t.Fatal("Python operation owner proof not accepted")
	}
	checks["python_same_operation_sync_async"] = result
}
