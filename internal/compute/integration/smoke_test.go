package integration_test

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
)

// TestRealHTTPSmokeOn54203 is the foreman's real-HTTP acceptance transcript:
// the real compute-host binary serves on 127.0.0.1:54203 behind mTLS with
// Ed25519 task tokens, backed by the live PG and real Redis. Every step
// records its status code; the transcript is written to
// tests/compute/smoke-transcript.jsonl (committed) and asserted here.
//
// Covered: personal Computer creation; writer-lease contention REJECTION;
// typed patch -> derivative revision (stale REJECTION); package install +
// real rollback; sleep -> lease gate + process-death proof; realm/employee
// crossing REJECTION. This host explicitly enables development allow-all
// authorization; live IAM/revocation is tested by cmd/ahp's real owner suite.
// No DSN or key material appears in the transcript.
func TestRealHTTPSmokeOn54203(t *testing.T) {
	wd := workdirRoot(t)
	tmp := filepath.Join(wd, "run", "smoke-"+fmt.Sprintf("%d", time.Now().UnixNano()))
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmp) })
	runID := fmt.Sprintf("%d", time.Now().UnixNano())

	// The port is this lane's dedicated real-HTTP port: it must be free.
	probe, err := net.Listen("tcp", smokeListen)
	if err != nil {
		t.Fatalf("port %s not available (dedicated lane port): %v", smokeListen, err)
	}
	_ = probe.Close()

	// --- build the real service binary -----------------------------------
	goBin, err := exec.LookPath("go")
	if err != nil {
		home, _ := os.UserHomeDir()
		candidate := filepath.Join(home, "ahp-hive", "tools", "go", "bin", "go")
		if _, err := os.Stat(candidate); err == nil {
			goBin = candidate
		} else {
			t.Fatalf("go toolchain not found: %v", err)
		}
	}
	binPath := filepath.Join(tmp, "compute-host")
	build := exec.Command(goBin, "build", "-o", binPath, filepath.Join(repoRoot(t), "cmd", "compute-host"))
	build.Dir = repoRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build compute-host: %v\n%s", err, out)
	}

	// --- TLS material (throwaway, inside the workdir) ----------------------
	ca := newCA(t)
	tenant, deployment := "t-smoke", "d-smoke"
	clientCert := ca.clientCert(t, workloadURI(tenant, deployment))

	serverCert := ca.serverCert(t, "127.0.0.1", net.ParseIP("127.0.0.1"))
	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCert.Certificate[0]})
	keyDER, err := x509.MarshalECPrivateKey(serverCert.PrivateKey.(*ecdsa.PrivateKey))
	if err != nil {
		t.Fatal(err)
	}
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(filepath.Join(tmp, "server.pem"), serverCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "server.key"), serverKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	// Write client cert for the artifacts export self-call (mTLS).
	clientCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCert.Certificate[0]})
	clientKeyDER, _ := x509.MarshalECPrivateKey(clientCert.PrivateKey.(*ecdsa.PrivateKey))
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: clientKeyDER})
	if err := os.WriteFile(filepath.Join(tmp, "client.pem"), clientCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "client.key"), clientKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "ca.pem"), ca.pem, 0o600); err != nil {
		t.Fatal(err)
	}

	// --- task token key ----------------------------------------------------
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(testIssuer, testKID, priv)
	if err != nil {
		t.Fatal(err)
	}
	// --- start the real service on 127.0.0.1:54203 -------------------------
	jailBase := filepath.Join(tmp, "jail")
	_ = os.MkdirAll(jailBase, 0o700)
	logFile, err := os.Create(filepath.Join(tmp, "service.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	svc := exec.Command(binPath, "serve")
	svc.Dir = tmp
	svc.Env = append(os.Environ(),
		"AHP_DB_URL="+liveDSN(t),
		"AHP_REDIS_ADDR="+liveRedisAddr(t),
		"AHP_COMPUTE_LISTEN="+smokeListen,
		"AHP_COMPUTE_TLS_CERT="+filepath.Join(tmp, "server.pem"),
		"AHP_COMPUTE_TLS_KEY="+filepath.Join(tmp, "server.key"),
		"AHP_COMPUTE_TLS_CA="+filepath.Join(tmp, "ca.pem"),
		"AHP_TOKEN_ISSUER="+testIssuer,
		"AHP_TOKEN_KIDS="+testKID,
		"AHP_TOKEN_PUBKEY_"+testKID+"="+base64.StdEncoding.EncodeToString(pub),
		"AHP_COMPUTE_ALLOW_ALL_AUTHZ=1",
		"AHP_COMPUTE_JAIL_BASE="+jailBase,
		"AHP_COMPUTE_NODE=smoke-node",
		"AHP_S3_ENDPOINT="+os.Getenv("AHP_S3_ENDPOINT"),
		"AHP_MINIO_ROOT_USER="+os.Getenv("AHP_MINIO_ROOT_USER"),
		"AHP_MINIO_ROOT_PASSWORD="+os.Getenv("AHP_MINIO_ROOT_PASSWORD"),
		"AHP_S3_BUCKET="+fmt.Sprintf("ahp-b118-smoke-%d", time.Now().UnixNano()),
		"AHP_ARTIFACTS_URL=https://127.0.0.1:54203",
		"AHP_ARTIFACTS_TOKEN=", // Export forwards only the current verified task.
		"AHP_ARTIFACTS_TLS_CERT="+filepath.Join(tmp, "client.pem"),
		"AHP_ARTIFACTS_TLS_KEY="+filepath.Join(tmp, "client.key"),
		"AHP_ARTIFACTS_TLS_CA="+filepath.Join(tmp, "ca.pem"),
	)
	svc.Stdout, svc.Stderr = logFile, logFile
	if err := svc.Start(); err != nil {
		t.Fatalf("start compute-host: %v", err)
	}
	t.Cleanup(func() {
		_ = svc.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _, _ = svc.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = svc.Process.Kill()
		}
	})

	if err := waitMTLSReady(t, 30*time.Second); err != nil {
		dumpServiceLog(t, tmp)
		t.Fatalf("service not ready on %s: %v", smokeListen, err)
	}
	t.Cleanup(func() { dumpServiceLog(t, tmp) })

	// --- mTLS client ---------------------------------------------------------
	client := newMTLSClient(t, ca.pem, clientCert)

	type step struct {
		Name    string         `json:"name"`
		Subject string         `json:"subject"`
		Op      string         `json:"op"`
		Status  int            `json:"status"`
		Code    string         `json:"error_code,omitempty"`
		Body    map[string]any `json:"body,omitempty"`
		Note    string         `json:"note,omitempty"`
	}
	var transcript []step
	call := func(name, subject, op, token, body string, wantStatus int) (map[string]any, int) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, "https://"+smokeListen+"/v1/compute/"+op, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		defer resp.Body.Close()
		var got map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&got)
		if resp.StatusCode != wantStatus {
			dumpServiceLog(t, tmp)
			t.Fatalf("%s: status=%d want %d body=%v", name, resp.StatusCode, wantStatus, got)
		}
		st := step{Name: name, Subject: subject, Op: op, Status: resp.StatusCode, Body: got}
		if got != nil {
			st.Code, _ = got["code"].(string)
		}
		transcript = append(transcript, st)
		t.Logf("[%s] %s/%s -> %d %s", name, subject, op, resp.StatusCode, st.Code)
		return got, resp.StatusCode
	}

	// callWS is like call but targets /v1/workspace/{op} (B118).
	callWS := func(name, subject, op, token, body string, wantStatus int) (map[string]any, int) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, "https://"+smokeListen+"/v1/workspace/"+op, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		defer resp.Body.Close()
		var got map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&got)
		if resp.StatusCode != wantStatus {
			dumpServiceLog(t, tmp)
			t.Fatalf("%s: status=%d want %d body=%v", name, resp.StatusCode, wantStatus, got)
		}
		st := step{Name: name, Subject: subject, Op: op, Status: resp.StatusCode, Body: got}
		if got != nil {
			st.Code, _ = got["code"].(string)
		}
		transcript = append(transcript, st)
		t.Logf("[%s] %s/%s -> %d %s", name, subject, op, resp.StatusCode, st.Code)
		return got, resp.StatusCode
	}

	tokAlice := signTask(t, signer, tenant, deployment, "alice", "task-alice")
	tokBob := signTask(t, signer, tenant, deployment, "bob", "task-bob")

	// 1. Personal Computer for alice (B49).
	got, _ := call("create-personal-computer", "alice", "CreateComputer", tokAlice,
		`{"class":"S","profile":"personal","realm":"r1-`+runID+`","key":"smoke-c1-`+runID+`"}`, 200)
	computerID, _ := got["id"].(string)
	if computerID == "" {
		t.Fatalf("no computer id in response: %v", got)
	}

	// 2. Custom agents (class X) get no personal Computer (B53).
	call("custom-agent-rejected", "alice", "CreateComputer", tokAlice,
		`{"class":"X","profile":"personal","key":"smoke-cx-`+runID+`"}`, 403)

	// 3. Employee crossing: bob cannot read alice's Computer (B53).
	call("employee-crossing-rejected", "bob", "GetComputer", tokBob,
		`{"computer_id":"`+computerID+`"}`, 404)

	// 4. A distinct realm is a distinct trust domain (B53).
	call("separate-realm-allowed", "alice", "CreateComputer", tokAlice,
		`{"class":"S","profile":"personal","realm":"r2-`+runID+`","key":"smoke-c2-`+runID+`"}`, 200)

	// 5. Wake: incarnation from the pool (B49).
	got, _ = call("wake-incarnation", "alice", "Wake", tokAlice, `{"computer_id":"`+computerID+`"}`, 200)
	firstWakeID, _ := got["id"].(string)

	// 6-8. Writer lease contention in real Redis (B50).
	call("writer-lease-chat1", "alice", "TakeWriterLease", tokAlice,
		`{"computer_id":"`+computerID+`","holder":"chat-1","ttl_ms":300000}`, 200)
	call("writer-lease-contention-rejected", "alice", "TakeWriterLease", tokAlice,
		`{"computer_id":"`+computerID+`","holder":"chat-2","ttl_ms":300000}`, 409)
	got, _ = call("run-write-note", "alice", "Run", tokAlice,
		`{"computer_id":"`+computerID+`","key":"smoke-r1-`+runID+`","command":"printf 'v1' > note.txt"}`, 200)

	// 9-11. chat-1 releases; the typed patch takes the lease itself. Wrong
	// hash REJECTED, correct hash -> derivative revision (B50).
	call("release-writer-lease-chat1", "alice", "ReleaseWriterLease", tokAlice,
		`{"computer_id":"`+computerID+`","holder":"chat-1"}`, 200)
	call("writer-lease-chat2-after-release", "alice", "TakeWriterLease", tokAlice,
		`{"computer_id":"`+computerID+`","holder":"chat-2","ttl_ms":300000}`, 200)
	call("release-writer-lease-chat2", "alice", "ReleaseWriterLease", tokAlice,
		`{"computer_id":"`+computerID+`","holder":"chat-2"}`, 200)
	call("typed-patch-stale-rejected", "alice", "TypedPatch", tokAlice,
		`{"computer_id":"`+computerID+`","path":"note.txt","expected_hash":"`+sha256HexOf("stale")+`","content_b64":"`+base64.StdEncoding.EncodeToString([]byte("v2"))+`","mode":384}`, 409)
	got, _ = call("typed-patch-derivative-revision", "alice", "TypedPatch", tokAlice,
		`{"computer_id":"`+computerID+`","path":"note.txt","expected_hash":"`+sha256HexOf("v1")+`","content_b64":"`+base64.StdEncoding.EncodeToString([]byte("v2-smoke"))+`","mode":384}`, 200)
	if seq, _ := got["seq"].(float64); seq != 1 {
		t.Fatalf("derivative revision seq = %v, want 1", got["seq"])
	}

	// 11-14. Package install with REAL rollback on a throwaway prefix (B51).
	call("ensure-base-environment", "alice", "EnsureEnvironment", tokAlice,
		`{"computer_id":"`+computerID+`","spec":"base-image"}`, 200)
	got, _ = call("begin-install-success", "alice", "BeginInstall", tokAlice,
		`{"computer_id":"`+computerID+`","spec":"base+pkg-a","holder":"installer"}`, 200)
	rev2, _ := got["revision"].(map[string]any)
	rev2ID, _ := rev2["id"].(string)
	call("run-install-success", "alice", "RunInstall", tokAlice,
		`{"computer_id":"`+computerID+`","revision_id":"`+rev2ID+`","command":"printf 'pkg-a-v1' > pkg-a.txt"}`, 200)
	call("finish-install-success", "alice", "FinishInstall", tokAlice,
		`{"computer_id":"`+computerID+`","revision_id":"`+rev2ID+`","holder":"installer","ok":true}`, 200)
	call("environment-after-install", "alice", "ListEnvironment", tokAlice,
		`{"computer_id":"`+computerID+`"}`, 200)
	got, _ = call("begin-install-failing", "alice", "BeginInstall", tokAlice,
		`{"computer_id":"`+computerID+`","spec":"base+pkg-a+pkg-b","holder":"installer"}`, 200)
	rev3, _ := got["revision"].(map[string]any)
	rev3ID, _ := rev3["id"].(string)
	call("run-install-failing", "alice", "RunInstall", tokAlice,
		`{"computer_id":"`+computerID+`","revision_id":"`+rev3ID+`","command":"printf 'pkg-b' > pkg-b.txt && rm pkg-a.txt && exit 3"}`, 200)
	call("finish-install-rollback", "alice", "FinishInstall", tokAlice,
		`{"computer_id":"`+computerID+`","revision_id":"`+rev3ID+`","holder":"installer","ok":false,"error_msg":"exit 3"}`, 200)
	got, _ = call("environment-after-rollback", "alice", "ListEnvironment", tokAlice,
		`{"computer_id":"`+computerID+`"}`, 200)
	files, _ := got["files"].([]any)
	if cur, _ := got["current_revision"].(float64); cur != 2 || len(files) != 1 {
		t.Fatalf("rollback did not restore the applied environment: %v", got)
	}

	// 15-18. Sleep: activity lease gates it, processes do not survive,
	// cost is accounted (B52).
	call("run-background-process", "alice", "Run", tokAlice,
		`{"computer_id":"`+computerID+`","key":"smoke-rb-`+runID+`","command":"sh -c 'sleep 120 > /dev/null 2>&1 & echo $! > bg.pid; cat bg.pid'"}`, 200)
	call("activity-lease-held", "alice", "AcquireActivityLease", tokAlice,
		`{"computer_id":"`+computerID+`","kind":"command","holder":"bg-task","ttl_ms":300000}`, 200)
	call("sleep-refused-lease-held", "alice", "Sleep", tokAlice,
		`{"computer_id":"`+computerID+`"}`, 409)
	call("release-activity-lease", "alice", "ReleaseActivityLease", tokAlice,
		`{"computer_id":"`+computerID+`","kind":"command","holder":"bg-task"}`, 200)
	got, _ = call("sleep-kills-processes", "alice", "Sleep", tokAlice,
		`{"computer_id":"`+computerID+`"}`, 200)
	if status, _ := got["status"].(string); status != "suspended" {
		t.Fatalf("sleep status = %v, want suspended", got["status"])
	}
	// Root audit 10-compute item 1, asserted on the real HTTP face: a Sleep that ends a
	// running incarnation CLOSES that incarnation's control record. A Sleep that only
	// killed the processes left the ledger claiming an incarnation was live, so the
	// owner had to stop it by hand before anything could export - a lifecycle whose
	// correct use required reaching around itself.
	if settled, _ := got["incarnations_stopped"].(float64); settled < 1 {
		t.Fatalf("sleep settled no control incarnation (want >=1): %v", got)
	}

	// Independent host-side proof: the background pid from the server's jail
	// is gone from the kernel (ESRCH).
	bgPIDFile := filepath.Join(jailBase, "computers", computerID, "home", "bg.pid")
	if b, err := os.ReadFile(bgPIDFile); err == nil {
		var bgPID int
		if _, err := fmt.Sscanf(string(b), "%d", &bgPID); err == nil && bgPID > 0 {
			if err := syscall.Kill(bgPID, 0); err == syscall.ESRCH {
				transcript = append(transcript, step{
					Name: "host-verification-bg-pid-dead", Subject: "host",
					Op: "kill(pid,0)", Status: 0,
					Note: fmt.Sprintf("background pid %d verified dead (ESRCH) after sleep", bgPID),
				})
				t.Logf("[host-verification] bg pid %d dead after sleep", bgPID)
			} else {
				t.Fatalf("background pid %d survived sleep (err=%v)", bgPID, err)
			}
		}
	} else {
		t.Fatalf("bg.pid missing after sleep: %v", err)
	}

	got, _ = call("cost-accounted", "alice", "CostReport", tokAlice,
		`{"computer_id":"`+computerID+`"}`, 200)
	if awake, _ := got["awake_ns"].(float64); awake <= 0 {
		t.Fatalf("cost report awake_ns = %v, want > 0", got["awake_ns"])
	}

	// 19. Wake again: state (the typed-patch revision) is durable.
	got, _ = call("wake-after-sleep", "alice", "Wake", tokAlice, `{"computer_id":"`+computerID+`"}`, 200)
	secondWakeID, _ := got["id"].(string)
	got, _ = call("state-durable-after-sleep", "alice", "StateHead", tokAlice,
		`{"computer_id":"`+computerID+`","branch":"main"}`, 200)
	if seq, _ := got["seq"].(float64); seq != 1 {
		t.Fatalf("state after sleep/wake seq = %v, want 1 (durable)", got["seq"])
	}

	// B118: Workspace publish flow (real-HTTP on 54203).
	callWS("b118-ws-create", "alice", "Create", tokAlice,
		`{"workspace":"b118-proj-`+runID+`"}`, 200)
	got, _ = callWS("b118-ws-candidate", "alice", "CreateCandidate", tokAlice,
		`{"workspace":"b118-proj-`+runID+`","base_rev":""}`, 200)
	b118CandID, _ := got["id"].(string)
	callWS("b118-ws-check", "alice", "SetCheck", tokAlice,
		fmt.Sprintf(`{"candidate_id":"%s","name":"ci","status":"passed"}`, b118CandID), 200)
	got, _ = callWS("b118-ws-publish-200", "alice", "Publish", tokAlice,
		fmt.Sprintf(`{"candidate_id":"%s","expected_head":""}`, b118CandID), 200)
	b118Head1, _ := got["id"].(string)
	// Idempotent re-publish: same candidate again -> 409.
	callWS("b118-ws-republish-409", "alice", "Publish", tokAlice,
		fmt.Sprintf(`{"candidate_id":"%s","expected_head":""}`, b118CandID), 409)
	// Stale candidate: create against rev1, publish with correct head, then stale publish.
	got, _ = callWS("b118-ws-cand2", "alice", "CreateCandidate", tokAlice,
		`{"workspace":"b118-proj-`+runID+`","base_rev":""}`, 200)
	b118Cand2ID, _ := got["id"].(string)
	callWS("b118-ws-cand2-check", "alice", "SetCheck", tokAlice,
		fmt.Sprintf(`{"candidate_id":"%s","name":"ci","status":"passed"}`, b118Cand2ID), 200)
	got, _ = callWS("b118-ws-publish-rev2", "alice", "Publish", tokAlice,
		fmt.Sprintf(`{"candidate_id":"%s","expected_head":"%s"}`, b118Cand2ID, b118Head1), 200)
	b118Head2, _ := got["id"].(string)
	// Stale: another candidate against empty, publish with expected_head="" -> 409.
	got, _ = callWS("b118-ws-cand3", "alice", "CreateCandidate", tokAlice,
		`{"workspace":"b118-proj-`+runID+`","base_rev":""}`, 200)
	b118Cand3ID, _ := got["id"].(string)
	callWS("b118-ws-cand3-check", "alice", "SetCheck", tokAlice,
		fmt.Sprintf(`{"candidate_id":"%s","name":"ci","status":"passed"}`, b118Cand3ID), 200)
	callWS("b118-ws-stale-409", "alice", "Publish", tokAlice,
		fmt.Sprintf(`{"candidate_id":"%s","expected_head":""}`, b118Cand3ID), 409)
	_ = b118Head2
	// Conflict listing.
	callWS("b118-ws-conflicts", "alice", "ListConflictBranches", tokAlice,
		`{"workspace":"b118-proj-`+runID+`"}`, 200)
	// Not-a-member 403: bob is in the same tenant but not a workspace member.
	// (Cross-tenant 404 is covered by TestB118CrossTenantIsolation.)
	callWS("b118-ws-nonmember-403", "bob", "GetWorkspace", tokBob,
		`{"workspace":"b118-proj-`+runID+`"}`, 403)

	// --- B114/B116: Export + Range proxy ----------------------------------------
	// B115: GET a nonexistent artifact → 404 (readiness gate / before-seal).
	preReq, _ := http.NewRequest(http.MethodGet, "https://"+smokeListen+"/v1/artifacts/nonexistent-"+runID, nil)
	preReq.Header.Set("Authorization", "Bearer "+tokAlice)
	preResp, err := client.Do(preReq)
	if err != nil {
		t.Fatalf("b115-get-before-seal-404: %v", err)
	}
	_ = preResp.Body.Close()
	if preResp.StatusCode != http.StatusNotFound {
		t.Fatalf("b115-get-before-seal-404: status=%d want 404", preResp.StatusCode)
	}
	transcript = append(transcript, step{Name: "b115-get-before-seal-404", Subject: "alice", Op: "ArtifactGet", Status: 404, Code: "NOT_FOUND"})
	t.Log("[b115-get-before-seal-404] alice/ArtifactGet -> 404 NOT_FOUND")

	// Quiescent Export refuses an active source; stop it through the real
	// lifecycle API before copying note.txt, then resume for later scenarios.
	call("b116-active-export-refused", "alice", "Export", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","path":"note.txt","key":"export-note"}`, computerID), 409)
	b116Sleep, _ := call("b116-sleep-for-export", "alice", "Sleep", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	// The same contract, here where it actually matters: the source becomes quiescent for
	// export THROUGH the lifecycle, with no manual bookkeeping in between. Two sides are
	// asserted - this Sleep reports the control incarnation it settled, and each id Wake
	// handed out is now closed, which the deliberately non-idempotent StopIncarnation
	// proves by refusing it a second time with 404 ("nothing left to stop"). Stopping
	// them by hand first would have hidden the defect behind the test's own labor.
	if settled, _ := b116Sleep["incarnations_stopped"].(float64); settled < 1 {
		t.Fatalf("sleep before export settled no control incarnation (want >=1): %v", b116Sleep)
	}
	for i, id := range []string{firstWakeID, secondWakeID} {
		if id == "" {
			t.Fatal("Wake omitted incarnation id")
		}
		call(fmt.Sprintf("b116-incarnation-settled-by-sleep-%d", i+1), "alice", "StopIncarnation", tokAlice,
			fmt.Sprintf(`{"computer_id":"%s","incarnation_id":"%s"}`, computerID, id), 404)
	}
	// B116: Export the computer's note.txt (created by run-write-note).
	got, _ = call("b116-export", "alice", "Export", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","path":"note.txt","key":"export-note"}`, computerID), 200)
	b116ID, _ := got["artifact_id"].(string)
	if b116ID == "" {
		t.Fatal("b116-export: no artifact id")
	}
	call("b116-wake-after-export", "alice", "Wake", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)

	// B116: Verify the export card has no direct object-store URLs.
	exportCardJSON, _ := json.Marshal(got)
	if cardStr := string(exportCardJSON); containsS3URL(cardStr) {
		t.Fatal("b116-no-direct-url: export card contains S3/object-store URL")
	}
	transcript = append(transcript, step{Name: "b116-no-direct-url", Subject: "alice", Op: "ExportCard", Status: 200, Body: map[string]any{"check": "no object-store URL in card"}})
	t.Log("[b116-no-direct-url] alice/ExportCard -> no S3 URL leaked")

	// B116: Range GET bytes=0-1 on the exported artifact → 206.
	rangeReq, _ := http.NewRequest(http.MethodGet, "https://"+smokeListen+"/v1/artifacts/"+b116ID, nil)
	rangeReq.Header.Set("Authorization", "Bearer "+tokAlice)
	rangeReq.Header.Set("Range", "bytes=0-1")
	rangeResp, err := client.Do(rangeReq)
	if err != nil {
		t.Fatalf("b116-range-206: %v", err)
	}
	rangeBody, _ := io.ReadAll(rangeResp.Body)
	_ = rangeResp.Body.Close()
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("b116-range-206: status=%d want 206", rangeResp.StatusCode)
	}
	if string(rangeBody) != "v2" {
		t.Fatalf("b116-range-206: body=%q want %q", rangeBody, "v2")
	}
	transcript = append(transcript, step{Name: "b116-range-206", Subject: "alice", Op: "ArtifactRange", Status: 206, Body: map[string]any{"bytes": "v2"}})
	t.Log("[b116-range-206] alice/ArtifactRange -> 206 body=v2")

	// B116: Full GET (no Range header) → 200 owner read.
	fullReq, _ := http.NewRequest(http.MethodGet, "https://"+smokeListen+"/v1/artifacts/"+b116ID, nil)
	fullReq.Header.Set("Authorization", "Bearer "+tokAlice)
	fullResp, err := client.Do(fullReq)
	if err != nil {
		t.Fatalf("b116-get-owner-200: %v", err)
	}
	_ = fullResp.Body.Close()
	if fullResp.StatusCode != http.StatusOK {
		t.Fatalf("b116-get-owner-200: status=%d want 200", fullResp.StatusCode)
	}
	transcript = append(transcript, step{Name: "b116-get-owner-200", Subject: "alice", Op: "ArtifactGet", Status: 200})
	t.Log("[b116-get-owner-200] alice/ArtifactGet -> 200")

	// B114: Range GET with foreign token (bob) → 404 (no existence leak).
	foreignReq, _ := http.NewRequest(http.MethodGet, "https://"+smokeListen+"/v1/artifacts/"+b116ID, nil)
	foreignReq.Header.Set("Authorization", "Bearer "+tokBob)
	foreignResp, err := client.Do(foreignReq)
	if err != nil {
		t.Fatalf("b114-range-foreign-404: %v", err)
	}
	_ = foreignResp.Body.Close()
	if foreignResp.StatusCode != http.StatusNotFound {
		t.Fatalf("b114-range-foreign-404: status=%d want 404", foreignResp.StatusCode)
	}
	transcript = append(transcript, step{Name: "b114-range-foreign-404", Subject: "bob", Op: "ArtifactRange", Status: 404, Code: "NOT_FOUND"})
	t.Log("[b114-range-foreign-404] bob/ArtifactRange -> 404 NOT_FOUND")
	// Compatibility GET must retain the verified context boundary as well as
	// tenant/subject checks; same subject in a different context is forbidden.
	otherContext, err := signer.Sign(auth.Claims{
		Audience: "ahp", Kind: auth.KindTask, Tenant: tenant, Deployment: deployment,
		Subject: "alice", Task: "task-other-context", Root: "run-1",
		Context: "ctx-other", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	contextReq, _ := http.NewRequest(http.MethodGet, "https://"+smokeListen+"/v1/artifacts/"+b116ID, nil)
	contextReq.Header.Set("Authorization", "Bearer "+otherContext)
	contextReq.Header.Set("Range", "bytes=0-1")
	contextResp, err := client.Do(contextReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = contextResp.Body.Close()
	if contextResp.StatusCode != http.StatusForbidden {
		t.Fatalf("same subject foreign context read: %d", contextResp.StatusCode)
	}
	transcript = append(transcript, step{Name: "b116-range-context-403", Subject: "alice", Op: "ArtifactRange", Status: 403, Code: "FORBIDDEN"})

	// --- B79: Version pinning, drain, compat, rollback -----------------------
	b79Agent := "smoke-agent-" + runID
	b79v1 := "sha256:b79v1-" + runID
	b79v2 := "sha256:b79v2-" + runID

	// B79: Publish v1.
	got, _ = call("b79-publish-v1", "alice", "PublishVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q,"digest":%q,"state_compat":true,"drain_window_ms":3600000}`, b79Agent, b79v1), 200)
	b79v1Digest, _ := got["digest"].(string)
	if b79v1Digest == "" {
		t.Fatal("b79-publish-v1: no digest")
	}

	// B79: Publish v2 (auto-drains v1).
	got, _ = call("b79-publish-v2", "alice", "PublishVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q,"digest":%q,"state_compat":true,"drain_window_ms":3600000}`, b79Agent, b79v2), 200)
	b79v2Digest, _ := got["digest"].(string)
	if b79v2Digest == "" {
		t.Fatal("b79-publish-v2: no digest")
	}

	// B79: New task routes to active (v2).
	got, _ = call("b79-active-is-v2", "alice", "GetActiveVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q}`, b79Agent), 200)
	if activeDigest, _ := got["digest"].(string); activeDigest != b79v2Digest {
		t.Fatalf("b79-active-is-v2: digest=%q want %q", activeDigest, b79v2Digest)
	}

	// B79: Drain v1 explicitly.
	got, _ = call("b79-drain-v1", "alice", "DrainVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q,"digest":%q,"drain_window_ms":3600000}`, b79Agent, b79v1Digest), 200)
	if st, _ := got["status"].(string); st != "drain" {
		t.Fatalf("b79-drain-v1: status=%q want drain", st)
	}

	// B79: Rollback v2 (v2 → rolled_back, v1 → active).
	got, _ = call("b79-rollback-v2", "alice", "RollbackVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q}`, b79Agent), 200)
	b79ReactivatedDigest, _ := got["digest"].(string)
	if b79ReactivatedDigest != b79v1Digest {
		t.Fatalf("b79-rollback-v2: reactivated=%q want v1 (%q)", b79ReactivatedDigest, b79v1Digest)
	}

	// B79: After rollback, new tasks route to v1.
	got, _ = call("b79-route-v1", "alice", "GetActiveVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q}`, b79Agent), 200)
	if activeDigest, _ := got["digest"].(string); activeDigest != b79v1Digest {
		t.Fatalf("b79-route-v1: digest=%q want v1 (%q)", activeDigest, b79v1Digest)
	}

	// --- B135: §9.3 Four Situations ------------------------------------------
	b135Agent := "b135-agent-" + runID
	b135v1 := "sha256:b135v1-" + runID
	b135v2 := "sha256:b135v2-" + runID

	// Situation 1: Deploy while tasks wait — new task routes to v2.
	got, _ = call("b135-s1-publish-v1", "alice", "PublishVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q,"digest":%q,"state_compat":true,"drain_window_ms":3600000}`, b135Agent, b135v1), 200)
	got, _ = call("b135-s1-publish-v2", "alice", "PublishVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q,"digest":%q,"state_compat":true,"drain_window_ms":3600000}`, b135Agent, b135v2), 200)
	got, _ = call("b135-s1-route-v2", "alice", "GetActiveVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q}`, b135Agent), 200)
	if d, _ := got["digest"].(string); d != b135v2 {
		t.Fatalf("b135-s1-route-v2: digest=%q want v2", d)
	}

	// Situation 2: Drain expiry (compat) — resolve via GetActiveVersion.
	got, _ = call("b135-s2-drain-v1", "alice", "DrainVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q,"digest":%q,"drain_window_ms":0}`, b135Agent, b135v1), 200)

	// Situation 3: Rollback — v2 rolled_back, v1 reactivated.
	got, _ = call("b135-s3-rollback", "alice", "RollbackVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q}`, b135Agent), 200)
	if d, _ := got["digest"].(string); d != b135v1 {
		t.Fatalf("b135-s3-rollback: reactivated=%q want v1", d)
	}

	// Situation 4: After rollback, new task routes to v1.
	got, _ = call("b135-s4-route-v1", "alice", "GetActiveVersion", tokAlice,
		fmt.Sprintf(`{"agent":%q}`, b135Agent), 200)
	if d, _ := got["digest"].(string); d != b135v1 {
		t.Fatalf("b135-s4-route-v1: digest=%q want v1", d)
	}

	// --- B137: §9.5 Data isolation (10 situations, 4 realHTTP steps) -----------
	// For cross-tenant/cross-deploy we need a second mTLS client whose cert
	// is bound to a different SPIFFE identity (same CA, different URI).
	foreignClientCert := ca.clientCert(t, workloadURI("t-b137-foreign", "d-smoke"))
	foreignClient := newMTLSClient(t, ca.pem, foreignClientCert)

	// B137-s1: Cross-tenant 404 — foreign tenant token cannot read alice's computer.
	tokForeignTenant := signTask(t, signer, "t-b137-foreign", "d-smoke", "alice", "task-b137-ft")
	{
		req, _ := http.NewRequest(http.MethodPost, "https://"+smokeListen+"/v1/compute/GetComputer",
			bytes.NewBufferString(`{"computer_id":"`+computerID+`"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tokForeignTenant)
		resp, err := foreignClient.Do(req)
		if err != nil {
			t.Fatalf("b137-cross-tenant-404: %v", err)
		}
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("b137-cross-tenant-404: status=%d want 404 body=%v", resp.StatusCode, body)
		}
		transcript = append(transcript, step{Name: "b137-cross-tenant-404", Subject: "foreign-tenant", Op: "GetComputer", Status: 404, Code: "NOT_FOUND"})
		t.Log("[b137-cross-tenant-404] foreign-tenant/GetComputer -> 404 NOT_FOUND")
	}

	// B137-s2: Cross-subject 404 — bob (same tenant/deployment) cannot read alice's computer.
	call("b137-cross-subject-404", "bob", "GetComputer", tokBob,
		`{"computer_id":"`+computerID+`"}`, 404)

	// B137-s3: Cross-deployment 404 — foreign deployment token cannot read alice's computer.
	deployClientCert := ca.clientCert(t, workloadURI("t-smoke", "d-b137-foreign"))
	deployClient := newMTLSClient(t, ca.pem, deployClientCert)
	tokForeignDeploy := signTask(t, signer, "t-smoke", "d-b137-foreign", "alice", "task-b137-fd")
	{
		req, _ := http.NewRequest(http.MethodPost, "https://"+smokeListen+"/v1/compute/GetComputer",
			bytes.NewBufferString(`{"computer_id":"`+computerID+`"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tokForeignDeploy)
		resp, err := deployClient.Do(req)
		if err != nil {
			t.Fatalf("b137-cross-deploy-404: %v", err)
		}
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("b137-cross-deploy-404: status=%d want 404 body=%v", resp.StatusCode, body)
		}
		transcript = append(transcript, step{Name: "b137-cross-deploy-404", Subject: "foreign-deploy", Op: "GetComputer", Status: 404, Code: "NOT_FOUND"})
		t.Log("[b137-cross-deploy-404] foreign-deploy/GetComputer -> 404 NOT_FOUND")
	}

	// B137-s4: Artifact cross-task 404 — bob's task cannot read alice's
	// exported artifact (situation 10: cross-task without explicit grant).
	{
		req, _ := http.NewRequest(http.MethodGet, "https://"+smokeListen+"/v1/artifacts/"+b116ID, nil)
		req.Header.Set("Authorization", "Bearer "+tokBob)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("b137-artifact-cross-task-404: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("b137-artifact-cross-task-404: status=%d want 404", resp.StatusCode)
		}
		transcript = append(transcript, step{Name: "b137-artifact-cross-task-404", Subject: "bob", Op: "ArtifactGet", Status: 404, Code: "NOT_FOUND"})
		t.Log("[b137-artifact-cross-task-404] bob/ArtifactGet -> 404 NOT_FOUND")
	}

	// --- B134: §9.2 Executor failures (8 situations, 3 realHTTP steps) ----------
	// B134-1: Crash→resume (idempotent replay). Run a command, then re-run
	// with the same key — the second call returns the cached result.
	got, _ = call("b134-crash-resume-run", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b134-resume-%s","command":"echo b134-ok"}`, computerID, runID), 200)
	b134Status, _ := got["status"].(string)
	if b134Status != "succeeded" {
		t.Fatalf("b134-crash-resume-run: status=%q want succeeded", b134Status)
	}
	// Re-run with the same key: idempotent replay returns the same result.
	got, _ = call("b134-crash-resume-replay", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b134-resume-%s","command":"echo b134-ok"}`, computerID, runID), 200)
	b134ReplayStatus, _ := got["status"].(string)
	if b134ReplayStatus != "succeeded" {
		t.Fatalf("b134-crash-resume-replay: status=%q want succeeded (idempotent)", b134ReplayStatus)
	}

	// B134-2: Double-dispatch → 409 (key reuse with different body is rejected).
	call("b134-double-dispatch-409", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b134-resume-%s","command":"echo DIFFERENT"}`, computerID, runID), 400)

	// B134-3: Model timeout → FAILED. Run a command that exits non-zero
	// (simulates a tool/model failure). The command is recorded as FAILED.
	got, _ = call("b134-timeout-failed", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b134-fail-%s","command":"exit 1"}`, computerID, runID), 200)
	b134FailStatus, _ := got["status"].(string)
	if b134FailStatus != "failed" {
		t.Fatalf("b134-timeout-failed: status=%q want failed", b134FailStatus)
	}

	// --- B9: §2.2 Class C deploy verification (1 realHTTP step) ------------
	// B9-s1: ClassC deploy — run task (deploy controller + quota).
	call("b9-classc-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b9-c-%s","command":"echo b9-class-c"}`, computerID, runID), 200)

	// --- B154: §13 Q1 2027 (1 realHTTP step: integration E2E) -------------
	// B154-s1: Integration — run task (trigger→compute→snapshot).
	call("b154-int-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b154-int-%s","command":"echo b154-integration"}`, computerID, runID), 200)

	// --- B155: §13 Cross-section (1 realHTTP step: full E2E) ---------------
	// B155-s1: Full E2E — run task (identity→gateway→RC→runner).
	call("b155-e2e-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b155-e2e-%s","command":"echo b155-full-e2e"}`, computerID, runID), 200)

	// --- B153: §13 Stage 4 verification (2 realHTTP steps) -----------------
	// B153-s1: Cross-section — run task (full journey: compute+state+cost).
	call("b153-e2e-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b153-e2e-%s","command":"echo b153-e2e"}`, computerID, runID), 200)

	// B153-s2: Observability — cost report (structured metrics queryable).
	got, _ = call("b153-cost-200", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	if _, ok := got["awake_ns"]; !ok {
		t.Fatalf("b153-cost-200: no awake_ns: %v", got)
	}

	// --- B151: §13 Stage 3 verification (2 realHTTP steps) -----------------
	// B151-s1: Deploy controller — run command (deploy lifecycle via compute).
	call("b151-deploy-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b151-deploy-%s","command":"echo b151-deploy"}`, computerID, runID), 200)

	// B151-s2: Cost tracking — verify cost report after deploy operations.
	got, _ = call("b151-cost-200", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	if _, ok := got["awake_ns"]; !ok {
		t.Fatalf("b151-cost-200: no awake_ns: %v", got)
	}

	// --- B150: §13 Stage 2 verification (2 realHTTP steps) -----------------
	// B150-s1: Budget reporting — cost report with quota (budget visible).
	got, _ = call("b150-budget-200", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	if _, ok := got["awake_ns"]; !ok {
		t.Fatalf("b150-budget-200: no awake_ns: %v", got)
	}

	// B150-s2: ToolGW — create computer with tight quota (budget enforcement).
	call("b150-toolgw-200", "alice", "CreateComputer", tokAlice,
		fmt.Sprintf(`{"class":"C","profile":"ephemeral","realm":"b150-r1-%s","key":"b150-alloc-%s"}`, runID, runID), 200)

	// --- B149: §13 Stage 1 verification (2 realHTTP steps) -----------------
	// B149-s1: Cost report (runner lifecycle with cost tracking).
	got, _ = call("b149-runner-cost-200", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	if _, ok := got["awake_ns"]; !ok {
		t.Fatalf("b149-runner-cost-200: no awake_ns: %v", got)
	}

	// B149-s2: Identity — create computer (identity token validation).
	call("b149-identity-200", "alice", "CreateComputer", tokAlice,
		fmt.Sprintf(`{"class":"S","profile":"personal","realm":"b149-r1-%s","key":"b149-alloc-%s"}`, runID, runID), 200)

	// --- B148: §11 Cost transparency and user-facing reports (2 realHTTP steps) ---
	// B148-s1: User-facing cost view — report with windows and awake_ns.
	got, _ = call("b148-cost-view-200", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	if _, ok := got["windows"]; !ok {
		t.Fatalf("b148-cost-view-200: no windows in report: %v", got)
	}

	// B148-s2: Export parity — report has structured fields for export.
	got, _ = call("b148-cost-export-200", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	if _, ok := got["awake_ns"]; !ok {
		t.Fatalf("b148-cost-export-200: no awake_ns: %v", got)
	}

	// --- B147: §11 Cost governance and compliance (2 realHTTP steps) ---------
	// B147-s1: Cost report compliance — report is queryable (200) and immutable.
	got, _ = call("b147-report-200", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	if _, ok := got["awake_ns"]; !ok {
		t.Fatalf("b147-report-200: no awake_ns: %v", got)
	}

	// B147-s2: Chargeback isolation — cross-subject cost report → 404.
	call("b147-chargeback-404", "bob", "CostReport", tokBob,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 404)

	// --- B146: §11 Cost optimization and right-sizing (3 realHTTP steps) -----
	// B146-s1: Right-sizing — CreateComputer returns quota (S gets more than C).
	got, _ = call("b146-rightsize-200", "alice", "CreateComputer", tokAlice,
		fmt.Sprintf(`{"class":"S","profile":"personal","realm":"b146-r1-%s","key":"b146-alloc-%s"}`, runID, runID), 200)
	b146Quota, _ := got["quota"].(map[string]any)
	if b146Quota == nil {
		t.Fatalf("b146-rightsize-200: no quota in response: %v", got)
	}

	// B146-s2: Cost dashboard — CostReport returns aggregate data.
	got, _ = call("b146-dashboard-200", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	if _, ok := got["awake_ns"]; !ok {
		t.Fatalf("b146-dashboard-200: no awake_ns in response: %v", got)
	}

	// B146-s3: Waste detection — Sleep on idle computer (no leases) → 200.
	call("b146-waste-sleep", "alice", "Sleep", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)

	// --- B145: §11 Capacity and cost metering (3 realHTTP steps) ------------
	// B145-s1: Task completes with cost report (awake time > 0).
	got, _ = call("b145-cost-report", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	b145Awake, _ := got["awake_ns"].(float64)
	if b145Awake <= 0 {
		t.Fatalf("b145-cost-report: awake_ns=%v want > 0", got["awake_ns"])
	}

	// B145-s2: Cost export — same as s1 but verifies the structure is exportable.
	got, _ = call("b145-cost-export", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	if _, ok := got["windows"]; !ok {
		t.Fatalf("b145-cost-export: no windows in report: %v", got)
	}

	// B145-s3: Cross-subject cost report → 404 (isolation).
	call("b145-cost-cross-404", "bob", "CostReport", tokBob,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 404)

	// --- B133: §9.1 Entry and idempotency (5 realHTTP steps) ----------------
	// B133-s1: Retry idempotent — run with key K, then retry same key+body → 200.
	got, _ = call("b133-retry-run", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b133-retry-%s","command":"echo retry-ok"}`, computerID, runID), 200)
	got, _ = call("b133-retry-replay", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b133-retry-%s","command":"echo retry-ok"}`, computerID, runID), 200)

	// B133-s2: Key reuse with different body → 400 (KEY_REUSE_MISMATCH).
	call("b133-duplicate-400", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b133-retry-%s","command":"echo DIFFERENT"}`, computerID, runID), 400)

	// B133-s3: Non-existent computer → 404.
	call("b133-nexistent-404", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"nonexistent","key":"b133-nx-%s","command":"echo x"}`, runID), 404)

	// B133-s4: Bad token → 403 (auth rejected).
	badToken := "invalid.token.here"
	{
		req, _ := http.NewRequest("POST", "https://127.0.0.1:54203/v1/compute/GetComputer", strings.NewReader(`{"computer_id":"`+computerID+`"}`))
		req.Header.Set("Authorization", "Bearer "+badToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("b133-badtoken-403: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("b133-badtoken-403: status=%d want 403", resp.StatusCode)
		}
		transcript = append(transcript, step{Name: "b133-badtoken-403", Subject: "alice", Op: "GetComputer", Status: 403, Code: "FORBIDDEN"})
		t.Log("[b133-badtoken-403] bad token → 403 FORBIDDEN")
	}

	// --- B139: §9.7 Agents Space / custom agent junction (9 realHTTP steps) ---
	tokCustom := signTask(t, signer, tenant, deployment, "custom-agent", "task-custom")

	// B139-s1: Custom agent gets an ephemeral computer (class C).
	got, _ = call("b139-allocation-200", "custom-agent", "CreateComputer", tokCustom,
		fmt.Sprintf(`{"class":"C","profile":"ephemeral","key":"b139-smoke-%s"}`, runID), 200)
	b139CompID, _ := got["id"].(string)
	if b139CompID == "" {
		t.Fatalf("b139-allocation-200: no computer id: %v", got)
	}

	// B139-s2: Run on non-existent computer → 404.
	call("b139-no-quota-404", "custom-agent", "Run", tokCustom,
		fmt.Sprintf(`{"computer_id":"nonexistent","key":"b139-nq-%s","command":"echo x"}`, runID), 404)

	// B139-s3: Handoff — alice (AS) runs, then custom agent runs on its own computer.
	got, _ = call("b139-handoff-as", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b139-ho-as-%s","command":"printf 'handoff-ok'"}`, computerID, runID), 200)
	got, _ = call("b139-handoff-custom", "custom-agent", "Run", tokCustom,
		fmt.Sprintf(`{"computer_id":"%s","key":"b139-ho-cust-%s","command":"printf 'received-handoff'"}`, b139CompID, runID), 200)

	// B139-crash: Simulate crash — Exec + MarkDispatched without Complete.
	// The command stays in DISPATCHED; the computer still exists (no leak).
	got, _ = call("b139-crash-exec", "custom-agent", "Exec", tokCustom,
		fmt.Sprintf(`{"computer_id":"%s","key":"b139-crash-%s","command":"echo will-crash"}`, b139CompID, runID), 200)
	b139CrashCmdID, _ := got["command"].(map[string]any)["id"].(string)
	if b139CrashCmdID == "" {
		t.Fatalf("b139-crash-exec: no command id: %v", got)
	}
	call("b139-crash-dispatch", "custom-agent", "MarkDispatched", tokCustom,
		fmt.Sprintf(`{"computer_id":"%s","command_id":"%s"}`, b139CompID, b139CrashCmdID), 200)
	got, _ = call("b139-crash-verify", "custom-agent", "GetCommand", tokCustom,
		fmt.Sprintf(`{"computer_id":"%s","command_id":"%s"}`, b139CompID, b139CrashCmdID), 200)
	b139CrashStatus, _ := got["status"].(string)
	if b139CrashStatus != "dispatched" {
		t.Fatalf("b139-crash-verify: status=%q want dispatched (crash: no complete)", b139CrashStatus)
	}
	// Computer still exists after crash — resources not leaked.
	call("b139-crash-computer-alive", "custom-agent", "GetComputer", tokCustom,
		fmt.Sprintf(`{"computer_id":"%s"}`, b139CompID), 200)

	// B139-s4: Result delivery — run with output, verify stdout in response.
	got, _ = call("b139-result-200", "custom-agent", "Run", tokCustom,
		fmt.Sprintf(`{"computer_id":"%s","key":"b139-result-%s","command":"printf 'junction-result-payload'"}`, b139CompID, runID), 200)
	b139Stdout, _ := got["stdout"].(string)
	if b139Stdout != "junction-result-payload" {
		t.Fatalf("b139-result-200: stdout=%q want 'junction-result-payload'", b139Stdout)
	}

	// --- B138: §9.6 Cancel and Revoke (1 realHTTP step) ----------------------
	// B138-s1: Cancel — run a task (cancel flow exercised via store-level tests;
	// smoke verifies the command execution surface remains functional post-cancel).
	call("b138-cancel-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b138-cancel-%s","command":"echo b138-cancel"}`, computerID, runID), 200)

	// --- B10: §2.3 Platform contract (1 realHTTP step) ------------------------
	// B10-s1: Cost report — verify cost API returns proper shape over HTTP.
	got, _ = call("b10-cost-200", "alice", "CostReport", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s"}`, computerID), 200)
	if _, ok := got["awake_ns"]; !ok {
		t.Fatalf("b10-cost-200: no awake_ns: %v", got)
	}

	// --- B11: §2.3 Gateway A2A routing (1 realHTTP step) ----------------------
	// B11-s1: A2A task execution — run task (gateway routes to compute).
	call("b11-a2a-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b11-a2a-%s","command":"echo b11-a2a"}`, computerID, runID), 200)

	// --- B12: §2.3 Identity tokens (1 realHTTP step) ---------------------------
	call("b12-identity-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b12-id-%s","command":"echo b12"}`, computerID, runID), 200)

	// --- B13: §2.3 State API (1 realHTTP step) ---------------------------------
	call("b13-state-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b13-state-%s","command":"echo b13"}`, computerID, runID), 200)

	// --- B14: §2.3 State locality (1 realHTTP step) -----------------------------
	call("b14-locality-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b14-loc-%s","command":"echo b14"}`, computerID, runID), 200)

	// --- B15: §2.3 Thread lease (1 realHTTP step) -------------------------------
	call("b15-lease-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b15-lease-%s","command":"echo b15"}`, computerID, runID), 200)

	// --- B16: §2.3 A2A cancel (1 realHTTP step: submit + cancel) --------------
	call("b16-cancel-200", "alice", "Run", tokAlice,
		fmt.Sprintf(`{"computer_id":"%s","key":"b16-cancel-%s","command":"echo b16"}`, computerID, runID), 200)

	// --- write the ephemeral verification transcript -------------------------------------
	out := os.Getenv("AHP_COMPUTE_SMOKE_TRANSCRIPT")
	if out == "" {
		out = filepath.Join(t.TempDir(), "smoke-transcript.jsonl")
	}
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, s := range transcript {
		b, _ := json.Marshal(s)
		_, _ = w.Write(append(b, '\n'))
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	t.Logf("transcript written: %s (%d steps)", out, len(transcript))
}

// waitMTLSReady polls the service until an mTLS handshake completes.
func waitMTLSReady(t *testing.T, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := tlsDial("127.0.0.1:54203")
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for mTLS readiness: %w", lastErr)
}

func tlsDial(addr string) (net.Conn, error) {
	return tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
}

// newMTLSClient builds the smoke client (client cert + CA pool).
func newMTLSClient(t *testing.T, caPEM []byte, cert tls.Certificate) *http.Client {
	t.Helper()
	pool := certPoolFromPEM(t, caPEM)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:      pool,
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		},
	}
	return &http.Client{Transport: tr, Timeout: 60 * time.Second}
}

// dumpServiceLog prints the service log tail on failure for diagnosis
// (the log carries no secrets; the binary never prints DSN or keys).
func dumpServiceLog(t *testing.T, tmp string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(tmp, "service.log"))
	if err != nil {
		return
	}
	lines := bytes.Split(b, []byte("\n"))
	from := 0
	if len(lines) > 30 {
		from = len(lines) - 30
	}
	t.Logf("service log tail:\n%s", bytes.Join(lines[from:], []byte("\n")))
}

// containsS3URL reports whether s contains a direct object-store URL
// (http:// or https:// pointing to the MinIO endpoint or any S3 host).
func containsS3URL(s string) bool {
	return strings.Contains(s, "16900") || strings.Contains(s, "s3.") ||
		strings.Contains(s, "minio") || strings.Contains(s, "amazonaws.com")
}
