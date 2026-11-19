package stateapi

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/state"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestCommonGoSDKActualMTLSPostgresDeltaOwnersAndReceiptReplay(t *testing.T) {
	e := rev7APIEnv(t)
	state.WithCommonIncrementalAppend()(e.store)
	option, x := state.WithKVNamespacePolicies([]state.KVNamespacePolicy{{Tenant: e.tenant, Deployment: e.deployment, Namespace: []string{"app"}, Owner: "deployment"}, {Tenant: e.tenant, Deployment: e.deployment, Namespace: []string{"user"}, Owner: "subject"}})
	if x != nil {
		t.Fatal(x)
	}
	option(e.store)
	if x = e.store.Migrate(e.ctx); x != nil {
		t.Fatal(x)
	}
	oldHandler := e.handler.(*Handler)
	handler, x := New(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, oldHandler.issue)
	if x != nil {
		t.Fatal(x)
	}
	e.srv.Config.Handler = handler
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
	thread, x := e.store.CreateThread(e.ctx, sc, "ctx-1", "common-sdk-thread")
	if x != nil {
		t.Fatal(x)
	}
	run, x := e.store.CreateRun(e.ctx, sc, thread.ID, "common-sdk-run", "common-sdk-task", json.RawMessage(`{}`))
	if x != nil {
		t.Fatal(x)
	}
	lease, x := e.store.ClaimRunFor(e.ctx, e.tenant, e.deployment, "sdk-worker", run.ID, time.Minute)
	if x != nil || lease == nil {
		t.Fatal("lease", x)
	}
	token := e.signTask(t, run.TaskID)
	client, x := sdk.New(sdk.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(_ context.Context) (string, error) { return token, nil }})
	if x != nil {
		t.Fatal(x)
	}
	f := sdk.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	req := sdk.CommonCommit{Fence: f, Namespace: "native", Key: "create", Document: &sdk.CommonDocument{Format: sdk.CommonFormat, Profile: "sdk-test/1", Root: "s", Records: map[string]sdk.CommonRecord{"s": {Kind: "session", Attributes: json.RawMessage(`{"value":"hello\u0000world"}`), Relations: map[string][]string{"events": {}}}}}, KV: []sdk.KVPut{{Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment", Value: json.RawMessage(`{"version":1}`)}}}
	first, x := client.CommitRecords(e.ctx, req)
	if x != nil || first.Revision != 1 {
		t.Fatal("commit", x)
	}
	_, x = client.PutKVBatch(e.ctx, f, "newer-app", []sdk.KVPut{{Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment", ExpectedVersion: 1, Value: json.RawMessage(`{"version":2}`)}})
	if x != nil {
		t.Fatal(x)
	}
	bundle, x := client.ReadRecordsBundle(e.ctx, thread.ID, "native", []sdk.KVRead{{Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment"}}, 1)
	if x != nil || !bundle.Unchanged || bundle.Common != nil || bundle.KV[0].Version != 2 || bundle.Receipt.KV[0].Version != 1 {
		t.Fatal("coherent current KV vs original receipt", x)
	}
	zero := 0
	next, x := client.CommitRecords(e.ctx, sdk.CommonCommit{Fence: f, Namespace: "native", Key: "append", ExpectedRevision: 1, Delta: &sdk.CommonDelta{Format: sdk.CommonFormat, Profile: "sdk-test/1", Root: "s", Put: map[string]sdk.CommonRecord{"e1": {Kind: "event", Attributes: json.RawMessage(`{"text":"next"}`), Relations: map[string][]string{}}}, Relations: []sdk.CommonRelationDelta{{Source: "s", Role: "events", Mode: "append", ExpectedLength: &zero, Targets: []string{"e1"}}}}})
	if x != nil || next.Revision != 2 {
		t.Fatal("delta", x)
	}
	old, x := client.ReadRecords(e.ctx, thread.ID, "native", 1)
	if x != nil || len(old.Document.Records) != 1 {
		t.Fatal("historic cut", x)
	}
	current, x := client.ReadRecords(e.ctx, thread.ID, "native", 0)
	if x != nil || len(current.Document.Records) != 2 || current.Document.Records["s"].Relations["events"][0] != "e1" {
		t.Fatal("native restored", x)
	}
	replay, x := client.CommitRecords(e.ctx, req)
	if x != nil || replay.CutID != first.CutID || replay.Revision != 1 {
		t.Fatal("same body/key receipt", x)
	}

	goShared := sdk.SharedValues{Client: client, Namespace: []string{"app"}, Owner: "deployment", Key: "from-go"}
	big, _ := json.Marshal(strings.Repeat("g", 1200000))
	prepared, err := goShared.Prepare(e.ctx, f, map[string]json.RawMessage{"large": big, "null": json.RawMessage(`null`)})
	if err != nil {
		t.Fatal("Go shared stage", err)
	}
	goDoc := *req.Document
	if _, err = client.CommitRecords(e.ctx, sdk.CommonCommit{Fence: f, Namespace: "go-shared", Key: "go-manifest", Document: &goDoc, KV: []sdk.KVPut{prepared}}); err != nil {
		t.Fatal("Go manifest publication", err)
	}
	if python := os.Getenv("AHP_COMMON_SDK_PYTHON"); python != "" {
		dir := t.TempDir()
		write := func(name string, data []byte) string {
			p := filepath.Join(dir, name)
			if e := os.WriteFile(p, data, 0600); e != nil {
				t.Fatal(e)
			}
			return p
		}
		cert := e.srv.Client().Transport.(*http.Transport).TLSClientConfig.Certificates[0]
		keyBytes, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
		if err != nil {
			t.Fatal("fixture private key encode")
		}
		certificate := []byte{}
		for _, raw := range cert.Certificate {
			certificate = append(certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw})...)
		}
		caPath := write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: e.srv.Certificate().Raw}))
		certPath := write("client.pem", certificate)
		keyPath := write("client.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes}))
		seed, _ := json.Marshal(map[string]any{"url": e.srv.URL, "token": token, "thread": thread.ID, "fence": f, "ca": caPath, "cert": certPath, "key": keyPath})
		seedPath := write("seed.json", seed)
		_, file, _, _ := runtime.Caller(0)
		cmd := exec.CommandContext(e.ctx, python, "-B", filepath.Join(filepath.Dir(file), "testdata/common_sdk.py"), seedPath)
		cmd.Env = append(os.Environ(), "PYTHONPATH="+filepath.Clean(filepath.Join(filepath.Dir(file), "../../sdk/python")), "PYTHONDONTWRITEBYTECODE=1")
		output, err := cmd.Output()
		if err != nil {
			write("python-error.txt", output)
			t.Fatalf("Python SDK actual mTLS/PG protocol failed: %s", output)
		}
		var result map[string]any
		if json.Unmarshal(output, &result) != nil || result["status"] != "passed" {
			t.Fatal("Python common SDK result missing")
		}
		t.Log("Python SDK actual mTLS/Postgres delta/read/replay PASS")
		values, _, err := (sdk.SharedValues{Client: client, Namespace: []string{"app"}, Owner: "deployment", Key: "shared-state"}).Read(e.ctx)
		if err != nil {
			t.Fatal("Go could not read Python shared manifest after source session deletion", err)
		}
		var alpha string
		if json.Unmarshal(values["alpha"], &alpha) != nil || alpha != strings.Repeat("я", 600000) {
			t.Fatal("Go/Python shared value integrity changed")
		}

	}
}
