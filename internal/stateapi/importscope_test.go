package stateapi

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// seedThread builds a thread with one checkpoint and two stream events over the
// real mTLS surface, and returns its id, its lease token and the export bytes
// taken with the owner's own scope token. Any plane works: the lane's own tenant
// or a second tenant minted on the same server.
func seedThread(t *testing.T, p plane) (string, []byte) {
	t.Helper()
	owner := p.signTask(t, "task-"+contracts.NewID())
	status, th := p.call(t, "CreateThread", owner,
		`{"context_id":"ctx-`+contracts.NewID()+`","key":"k-`+contracts.NewID()+`"}`)
	mustStatus(t, status, http.StatusOK, th)
	threadID, _ := th["id"].(string)

	status, run := p.call(t, "CreateRun", owner,
		`{"thread_id":"`+threadID+`","key":"k-`+contracts.NewID()+`","payload":{"a":1}}`)
	mustStatus(t, status, http.StatusOK, run)

	status, claimed := p.call(t, "ClaimRun", "", `{"worker":"w1","ttl_ms":60000}`)
	mustStatus(t, status, http.StatusOK, claimed)
	leaseTok, _ := claimed["task_token"].(string)
	epoch := int64(claimed["lease"].(map[string]any)["epoch"].(float64))
	// The fence carries the run, and the run carries the thread: stream and
	// checkpoint writes are addressed by the lease, not by a thread id.
	fence := `"fence":{"run_id":"` + run["id"].(string) + `","worker":"w1","epoch":` + itoa(epoch) + `}`

	for i, payload := range []string{`{"n":1}`, `{"n":2}`} {
		body := `{` + fence + `,"key":"k-stream-` + itoa(int64(i)) +
			`","kind":"tool","payload":` + payload + `}`
		status, appended := p.call(t, "AppendStream", leaseTok, body)
		mustStatus(t, status, http.StatusOK, appended)
	}
	cpBody := `{` + fence + `,"checkpoint":{"id":"cp-1","thread_id":"` + threadID + `","namespace":"lg","version":1,` +
		`"parts":[{"channel":"values","version":"v1","data":"aW1wb3J0ZWQtYnl0ZXM="}]},"key":"cpk-` +
		contracts.NewID() + `"}`
	status, put := p.call(t, "PutCheckpoint", leaseTok, cpBody)
	mustStatus(t, status, http.StatusOK, put)

	status, exported := p.adminExport(t, threadID)
	mustStatus(t, status, http.StatusOK, exported)
	raw, err := json.Marshal(exported)
	if err != nil {
		t.Fatalf("marshal export: %v", err)
	}
	return threadID, raw
}

// signTaskAs mints a real task token for a DIFFERENT subject inside the same
// workload: the way to ask the server what it does about identity on ingress.
func (e *env) signTaskAs(t *testing.T, subject, task string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: testAudience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment,
		Subject: subject, Task: task, Root: "run-1",
		Context: "ctx-1", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// streamDigest digests the ordered content of an events array as it came off the
// wire, ignoring ids and sequence numbers: a restore has to reproduce the bytes.
func streamDigest(t *testing.T, events any) string {
	t.Helper()
	list, _ := events.([]any)
	h := sha256.New()
	for _, item := range list {
		ev, _ := item.(map[string]any)
		for _, field := range []string{"key", "kind"} {
			h.Write([]byte(field))
			h.Write([]byte("="))
			s, _ := ev[field].(string)
			h.Write([]byte(s))
			h.Write([]byte{0})
		}
		payload, _ := json.Marshal(ev["payload"])
		h.Write(payload)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// TestImportThreadRefusesForeignIdentityOnTheWire is F1 on the wire. Export bytes
// are a bearer artefact that travels by design, and they carry the scope that
// produced them in thread.scope. A foreign subject of the same tenant that
// imports them used to get 200 plus a full durable clone under its own identity,
// sharing the source context_id. It now gets a refusal with a stable reason, and
// nothing is written.
func TestImportThreadRefusesForeignIdentityOnTheWire(t *testing.T) {
	e := newEnv(t)
	threadID, export := seedThread(t, e)

	invaderTok := e.signTaskAs(t, "user-eve", "task-"+contracts.NewID())
	body := `{"export":` + string(export) + `}`
	status, resp := e.call(t, "ImportThread", invaderTok, body)
	if status != http.StatusForbidden {
		t.Fatalf("foreign subject importing another identity's export: status = %d, want 403 (body %v)", status, resp)
	}
	if resp["code"] != contracts.ErrForbidden {
		t.Errorf("code = %v, want %s", resp["code"], contracts.ErrForbidden)
	}
	if resp["reason"] != "EXPORT_SCOPE_MISMATCH" {
		t.Errorf("reason = %v, want EXPORT_SCOPE_MISMATCH", resp["reason"])
	}
	// Nothing was created: the invader owns no thread at all.
	status, listed := e.call(t, "ListThreads", invaderTok, `{"limit":10}`)
	mustStatus(t, status, http.StatusOK, listed)
	if threads, _ := listed["threads"].([]any); len(threads) != 0 {
		t.Fatalf("the refused import still wrote: the invader now owns %d threads", len(threads))
	}
	// The owner's thread is untouched and still unreadable by the invader.
	if status, got := e.call(t, "GetThread", invaderTok, `{"thread_id":"`+threadID+`"}`); status != http.StatusNotFound {
		t.Fatalf("the invader can now read the source thread: status %d, body %v", status, got)
	}
}

// TestImportThreadRestoresUnderExplicitRetarget is F2 on the wire: the owner must
// be able to restore their own export. Naming a context_id makes the new address
// deliberate; the thread id is fresh and the durable content comes back equal -
// stream bytes by digest, checkpoint parts by bytes.
func TestImportThreadRestoresUnderExplicitRetarget(t *testing.T) {
	e := newEnv(t)
	threadID, export := seedThread(t, e)
	owner := e.signTask(t, "task-"+contracts.NewID())

	newCtx := "ctx-restored-" + contracts.NewID()
	status, resp := e.call(t, "ImportThread", owner,
		`{"export":`+string(export)+`,"context_id":"`+newCtx+`"}`)
	mustStatus(t, status, http.StatusOK, resp)
	restored, _ := resp["id"].(string)
	if restored == "" || restored == threadID {
		t.Fatalf("restore must allocate a fresh thread id: got %q, source %q", restored, threadID)
	}
	if resp["context_id"] != newCtx {
		t.Fatalf("restored context_id = %v, want %q", resp["context_id"], newCtx)
	}

	status, srcStream := e.call(t, "ReadStream", owner, `{"thread_id":"`+threadID+`","after":0,"limit":100}`)
	mustStatus(t, status, http.StatusOK, srcStream)
	status, dstStream := e.call(t, "ReadStream", owner, `{"thread_id":"`+restored+`","after":0,"limit":100}`)
	mustStatus(t, status, http.StatusOK, dstStream)
	if src, dst := streamDigest(t, srcStream["events"]), streamDigest(t, dstStream["events"]); src != dst {
		t.Fatalf("stream digest differs after restore: source %s, restored %s", src, dst)
	}
	status, cps := e.call(t, "ListCheckpoints", owner,
		`{"thread_id":"`+restored+`","namespace":"lg","limit":10}`)
	mustStatus(t, status, http.StatusOK, cps)
	restoredCPs, _ := cps["checkpoints"].([]any)
	if len(restoredCPs) != 1 {
		t.Fatalf("restored checkpoints = %v, want one", cps["checkpoints"])
	}
	status, part := e.call(t, "GetPart", owner, `{"thread_id":"`+restored+
		`","namespace":"lg","channel":"values","version":"v1"}`)
	mustStatus(t, status, http.StatusOK, part)
	data, err := base64.StdEncoding.DecodeString(part["data"].(string))
	if err != nil {
		t.Fatalf("restored part is not base64: %v", err)
	}
	if string(data) != "imported-bytes" {
		t.Fatalf("restored part bytes = %q, want %q", data, "imported-bytes")
	}
}

// TestImportThreadRefusesTakenContextWithStableReason pins the other half of the
// asymmetry: the owner re-importing into the context they already own is refused
// (a silent merge into the live thread would mix two histories), and the refusal
// is machine-readable, not prose.
func TestImportThreadRefusesTakenContextWithStableReason(t *testing.T) {
	e := newEnv(t)
	_, export := seedThread(t, e)
	owner := e.signTask(t, "task-"+contracts.NewID())

	var doc map[string]any
	if err := json.Unmarshal(export, &doc); err != nil {
		t.Fatal(err)
	}
	taken, _ := doc["thread"].(map[string]any)["context_id"].(string)

	status, resp := e.call(t, "ImportThread", owner,
		`{"export":`+string(export)+`,"context_id":"`+taken+`"}`)
	if status != http.StatusConflict {
		t.Fatalf("import into an owned context_id: status = %d, want 409 (body %v)", status, resp)
	}
	if resp["code"] != contracts.ErrConflict {
		t.Errorf("code = %v, want %s", resp["code"], contracts.ErrConflict)
	}
	if resp["reason"] != "IMPORT_CONTEXT_TAKEN" {
		t.Errorf("reason = %v, want IMPORT_CONTEXT_TAKEN", resp["reason"])
	}
	// The same export under a context nobody owns restores cleanly, so the
	// refusal above is about the address and not about the export.
	if status, ok := e.call(t, "ImportThread", owner,
		`{"export":`+string(export)+`,"context_id":"ctx-free-`+contracts.NewID()+`"}`); status != http.StatusOK {
		t.Fatalf("the same export into a free context_id: status %d, body %v", status, ok)
	}
}
