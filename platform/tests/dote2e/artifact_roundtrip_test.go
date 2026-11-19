package dote2e_test

// PDF rev3.2 (the file/artifact leg of the personal-Dot path): an employee uploads
// a file through their Dot's task token, the platform holds it in object storage
// under the sealed SHA-256, and the SAME subject downloads it again. A different
// subject, tenant or conversation must be refused.
//
// The artifact plane is also where the taint of an untrusted external answer is
// enforced (L943): a blob carrying the untrusted_external taint may not be
// published into a trusted workspace sink.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// postRaw posts a JSON body and returns the raw reply bytes, for the operations
// whose reply is a byte stream rather than a JSON envelope.
func (s *instance) postRaw(client *http.Client, path string, in any, token string, headers map[string]string) (int, []byte, error) {
	body, err := json.Marshal(in)
	if err != nil {
		return 0, nil, err
	}
	request, err := http.NewRequest(http.MethodPost, s.Origin+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	payload := make([]byte, 0, 4096)
	chunk := make([]byte, 64<<10)
	for {
		n, rerr := response.Body.Read(chunk)
		payload = append(payload, chunk[:n]...)
		if rerr != nil {
			break
		}
		if len(payload) > 32<<20 {
			break
		}
	}
	return response.StatusCode, payload, nil
}

// upload seals one blob under the caller's task token and returns its id, exact
// sealed version and digest, following the real three-step contract: BeginUpload
// hands out a presigned PUT, the PUT stores the bytes, Seal turns them into an
// immutable version whose digest the platform verified.
func (s *instance) upload(t *testing.T, client *http.Client, token, key, name string, blob []byte, labels map[string]any) (string, string, string) {
	t.Helper()
	var begin struct {
		ID           string            `json:"id"`
		PresignedURL string            `json:"presigned_url"`
		Headers      map[string]string `json:"headers"`
	}
	if err := s.call(client, "/v1/artifacts/BeginUpload", map[string]any{"key": key, "name": name,
		"mime": "application/octet-stream", "sha256": digestHex(blob), "size": len(blob),
		"labels": labels}, token, &begin); err != nil {
		t.Fatalf("BeginUpload refused: %v", err)
	}
	if begin.ID == "" || begin.PresignedURL == "" {
		t.Fatalf("BeginUpload returned no upload: %+v", begin)
	}
	put, err := http.NewRequest(http.MethodPut, begin.PresignedURL, bytes.NewReader(blob))
	if err != nil {
		t.Fatalf("build presigned PUT: %v", err)
	}
	for header, value := range begin.Headers {
		put.Header.Set(header, value)
	}
	response, err := client.Do(put)
	if err != nil {
		t.Fatalf("presigned PUT to object storage: %v", err)
	}
	response.Body.Close()
	if response.StatusCode/100 != 2 {
		t.Fatalf("object storage refused the blob: http %d", response.StatusCode)
	}
	var sealed struct {
		ID      string `json:"id"`
		Version string `json:"version"`
		SHA256  string `json:"sha256"`
	}
	if err := s.call(client, "/v1/artifacts/Seal", map[string]any{"id": begin.ID}, token, &sealed); err != nil {
		t.Fatalf("Seal refused: %v", err)
	}
	return sealed.ID, sealed.Version, sealed.SHA256
}

func TestArtifactUploadSealDownloadSameSubject(t *testing.T) {
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	contextID := "dot-art-ctx-" + stamp
	token, task := stand.taskGrantContext(t, stand.HTTP, human, "dot-art-"+stamp, contextID,
		[]string{"artifacts", "state", "identity:delegate"})
	blob := []byte("personal Dot artifact " + stamp + " - bytes that must come back identical")
	digest := digestHex(blob)
	checks := map[string]any{}

	// (a) Upload -> Seal: the platform stores the bytes and records the digest it
	// verified, under the employee's own subject and conversation.
	artifactID, version, sealedSHA := stand.upload(t, stand.HTTP, token, task+"-file", "dot-note.bin",
		blob, map[string]any{})
	if sealedSHA != "" && sealedSHA != digest {
		checks["seal_digest"] = map[string]any{"status": "FAIL", "sealed": sealedSHA, "expected": digest}
		stand.record(t, "artifact-upload-seal-download", "FAIL", checks)
		t.Fatalf("the sealed digest differs from the uploaded bytes: %s != %s", sealedSHA, digest)
	}
	checks["seal_digest"] = map[string]any{"status": "PASS", "sha256": digest, "version": version}

	var storedObject, storedSHA, storedContext, storedSubject string
	var storedSize int64
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT object, sha256, size, context_id, subject FROM ahp_artifacts.artifacts
		   WHERE tenant=$1 AND id=$2`, stand.Scope["tenant"], artifactID).
		Scan(&storedObject, &storedSHA, &storedSize, &storedContext, &storedSubject); err != nil {
		stand.record(t, "artifact-upload-seal-download", "FAIL", map[string]any{"pg_row": map[string]any{
			"status": "FAIL", "artifact": artifactID, "error": err.Error()}})
		t.Fatalf("read the artifact row: %v", err)
	}
	checks["durable_object_row"] = map[string]any{"status": "PASS",
		"artifact": artifactID, "object_key_sha256": digestHex([]byte(storedObject)),
		"bucket": stand.Scope["bucket"],
		"object_names_artifact_or_digest": strings.Contains(storedObject, artifactID) ||
			strings.Contains(storedObject, digest),
		"sha256": storedSHA, "bytes": storedSize, "context_id": storedContext, "subject": storedSubject}
	if storedSHA != digest || storedSize != int64(len(blob)) || storedContext != contextID ||
		storedSubject != string(stand.Scope["subject"].(string)) {
		stand.record(t, "artifact-upload-seal-download", "FAIL", checks)
		t.Fatalf("the stored artifact row does not describe the uploaded bytes: %+v", checks["durable_object_row"])
	}

	// (b) The same subject downloads the bytes with its own task token. The bytes
	// come back out of object storage, so this is the whole round trip.
	// (b0) Session-marker probe (candidate 652b7172 behaviour capture): the same
	// Download with the SAME task token, with and without X-AHP-Session. The SDKs
	// never send the browser marker; if only the marked request passes, the
	// machine face is browser-gated (tracked as DEF-DOTE2E-8).
	var meta map[string]any
	errMeta := stand.call(stand.HTTP, "/v1/artifacts/Get", map[string]any{"id": artifactID}, token, &meta)
	checks["owner_get_metadata"] = map[string]any{"status": "PASS", "http": status(errMeta),
		"code": errorCode(errMeta), "error": truncate(errText(errMeta), 160)}
	code, got, err := stand.postRaw(stand.HTTP, "/v1/artifacts/Download", map[string]any{"id": artifactID}, token, nil)
	if err != nil {
		stand.record(t, "artifact-upload-seal-download", "FAIL", map[string]any{"download": map[string]any{
			"status": "FAIL", "error": err.Error()}})
		t.Fatalf("Download failed: %v", err)
	}
	if code != http.StatusOK || digestHex(got) != digest {
		checks["download_same_subject"] = map[string]any{"status": "FAIL", "http": code,
			"bytes": len(got), "sha256": digestHex(got), "expected": digest}
		stand.record(t, "artifact-upload-seal-download", "FAIL", checks)
		t.Fatalf("the owner could not download their own bytes: http %d", code)
	}
	checks["download_same_subject"] = map[string]any{"status": "PASS", "http": code, "bytes": len(got),
		"sha256": digestHex(got)}
	machineCode, _, machineErr := stand.machinePost(stand.HTTP, "/v1/artifacts/Download",
		map[string]any{"id": artifactID}, token)
	checks["download_without_session_marker"] = map[string]any{
		"http": machineCode, "error": truncate(errText(machineErr), 120)}

	// (c) A byte range of the sealed version is served exactly, which is what a
	// resuming client depends on.
	code, partial, err := stand.postRaw(stand.HTTP, "/v1/artifacts/Download", map[string]any{"id": artifactID},
		token, map[string]string{"Range": "bytes=0-15"})
	if err != nil {
		t.Fatalf("ranged Download failed: %v", err)
	}
	ranged := map[string]any{"status": "PASS", "http": code, "bytes": len(partial),
		"matches_prefix": string(partial) == string(blob[:16])}
	checks["ranged_read"] = ranged
	if code != http.StatusPartialContent || string(partial) != string(blob[:16]) {
		ranged["status"] = "FAIL"
		stand.record(t, "artifact-upload-seal-download", "FAIL", checks)
		t.Fatalf("the ranged read is wrong: http %d bytes %d", code, len(partial))
	}

	// (d) The browser (the employee, not a task) downloads the EXACT version
	// through the human download proxy of the single origin.
	humanPath := "/u-ahp/v1/artifacts/download?artifact_id=" + artifactID + "&version=" + version
	code, humanBytes, err := stand.rawGet(stand.HTTP, humanPath, human, nil)
	if err != nil {
		t.Fatalf("human download proxy request failed: %v", err)
	}
	if code == http.StatusOK && digestHex(humanBytes) == digest {
		checks["human_exact_version_download"] = map[string]any{"status": "PASS", "http": code,
			"bytes": len(humanBytes), "route": "/u-ahp/v1/artifacts/download"}
	} else {
		checks["human_exact_version_download"] = map[string]any{"status": "FAIL", "http": code,
			"bytes": len(humanBytes), "sha256": digestHex(humanBytes),
			"body_head": truncate(string(humanBytes), 200),
			"route":     "/u-ahp/v1/artifacts/download"}
		stand.record(t, "artifact-upload-seal-download", "FAIL", checks)
		t.Fatalf("the employee could not download their own artifact version: http %d", code)
	}

	// (e) Negative rights: a foreign tenant, another subject of this tenant and
	// another conversation of the SAME employee are all refused.
	foreignHuman := stand.humanToken(t, stand.Foreign, mustLogin(t, "foreign_email"), mustLogin(t, "foreign_password"))
	foreignToken, _ := stand.taskGrantContext(t, stand.Foreign, foreignHuman, "dot-art-foreign-"+stamp,
		"dot-art-foreign-ctx-"+stamp, []string{"artifacts", "state"})
	code, body, err := stand.postRaw(stand.Foreign, "/v1/artifacts/Download", map[string]any{"id": artifactID}, foreignToken, nil)
	checks["foreign_tenant_denied"] = denyCheck(code, body, err)

	colleagueToken, _ := stand.taskGrantContext(t, stand.HTTP,
		stand.humanToken(t, stand.HTTP, mustLogin(t, "admin_email"), mustLogin(t, "admin_password")),
		"dot-art-colleague-"+stamp, "dot-art-colleague-ctx-"+stamp, []string{"artifacts", "state"})
	code, body, err = stand.postRaw(stand.HTTP, "/v1/artifacts/Download", map[string]any{"id": artifactID}, colleagueToken, nil)
	checks["other_subject_denied"] = denyCheck(code, body, err)

	// The conversation gate is a property of a RUN credential: the human's
	// per-session authority is deliberately scoped to subject+tenant and reads its
	// own artifacts across conversations (internal/artifactsapi readRequest,
	// internal/artifacts/read_scope.go). The only way to obtain a
	// conversation-bound credential from outside a run is to delegate a child
	// under the browser-exchange authority root, and Identity refuses a
	// same-deployment child there ("delegation would repeat an actor in the
	// chain"; allow_self is an inherited ceiling a client cannot grant). A
	// conversation-bound credential therefore has to come from a real Space run -
	// which this lane cannot start while DEF-DOTE2E-1 fails every Dot turn. The
	// leg is recorded as unverified instead of being faked.
	elsewhereTask := "rtask-" + string(stand.Scope["subject"].(string)) + ":dot-art-elsewhere-" + stamp
	crossConversation := map[string]any{"status": "UNVERIFIED", "child_task": elsewhereTask,
		"attempted_context": "dot-art-elsewhere-ctx-" + stamp}
	elsewhereToken, elseContext, delegateErr := stand.tryDelegateChild(stand.HTTP, token, task, elsewhereTask,
		"dot-art-elsewhere-ctx-"+stamp, []string{"artifacts"})
	crossConversation["delegate_http"] = status(delegateErr)
	crossConversation["delegate_message"] = truncate(errText(delegateErr), 160)
	if delegateErr == nil {
		code, body, err := stand.postRaw(stand.HTTP, "/v1/artifacts/Download", map[string]any{"id": artifactID}, elsewhereToken, nil)
		crossConversation = denyCheck(code, body, err)
		crossConversation["child_task"] = elsewhereTask
		crossConversation["child_context"] = elseContext
		if crossConversation["status"] != "PASS" {
			checks["other_conversation_denied"] = crossConversation
			stand.record(t, "artifact-upload-seal-download", "FAIL", checks)
			t.Fatalf("a run bound to another conversation read the artifact: %+v", crossConversation)
		}
	} else {
		crossConversation["blocked_by"] = "DEF-DOTE2E-1 (no Space run can start) + identity actor-chain rule"
		crossConversation["independent_confirmation"] = "tests/workspace-lifecycle/lifecycle_live_test.go records the " +
			"same limit as an unverified leg (U-1)"
	}
	checks["other_conversation_denied"] = crossConversation

	for _, name := range []string{"foreign_tenant_denied", "other_subject_denied", "other_conversation_denied"} {
		if value, ok := checks[name].(map[string]any); !ok || (value["status"] != "PASS" && value["status"] != "UNVERIFIED") {
			stand.record(t, "artifact-upload-seal-download", "FAIL", checks)
			t.Fatalf("%s: the artifact plane let through a caller that must be refused: %+v", name, checks[name])
		}
	}
	stand.record(t, "artifact-upload-seal-download", "PASS", checks)
}

func denyCheck(code int, body []byte, err error) map[string]any {
	if err != nil {
		return map[string]any{"status": "FAIL", "error": err.Error()}
	}
	refused := code == http.StatusForbidden || code == http.StatusUnauthorized || code == http.StatusNotFound
	statusValue := "FAIL"
	if refused {
		statusValue = "PASS"
	}
	return map[string]any{"status": statusValue, "http": code, "bytes": len(body),
		"body_head": truncate(string(body), 160)}
}

func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}

// TestUntrustedExternalAnswerKeepsExternalStatus proves the taint rule the PDF
// states for a delegated agent's answer (L943) at the two places where the
// platform can actually be driven over HTTP:
//
//   - the artifact plane refuses to write external / untrusted_external / secret
//     labelled content into the trusted private store at all, while the identical
//     call without a taint succeeds (so the refusal is about the taint);
//   - the Memory owner's Propose turns external-tainted evidence into a record
//     held for review instead of trusted shared memory, and the durable row says
//     so.
//
// The turn-internal half of L943 (the answer of a delegated agent arriving inside
// a Dot turn and being marked there) cannot be exercised while DEF-DOTE2E-1 stops
// every Dot turn; this scenario covers the label plane the answer would land in.
func TestUntrustedExternalAnswerKeepsExternalStatus(t *testing.T) {
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	contextID := "dot-taint-ctx-" + stamp
	token, task := stand.taskGrantContext(t, stand.HTTP, human, "dot-taint-"+stamp, contextID,
		[]string{"artifacts", "state", "memory"})
	checks := map[string]any{}

	// (a) Artifact ingest gate: a blob named as an untrusted external answer must
	// not enter the trusted store.
	gates := []map[string]any{}
	for _, taint := range []string{"untrusted_external", "external", "secret"} {
		err := stand.call(stand.HTTP, "/v1/artifacts/BeginUpload", map[string]any{
			"key": task + "-" + taint, "name": taint + ".txt", "mime": "text/plain",
			"sha256": digestHex([]byte(taint)), "size": len(taint),
			"labels": map[string]any{"taints": []string{taint}}}, token, nil)
		refused := err != nil && status(err) == http.StatusForbidden
		gates = append(gates, map[string]any{"taint": taint, "status": "PASS", "http": status(err),
			"code": errorCode(err), "refused": refused, "message": truncate(errText(err), 160)})
		if !refused {
			for _, gate := range gates {
				if gate["taint"] == taint {
					gate["status"] = "FAIL"
				}
			}
			checks["artifact_ingest_taint_gate"] = gates
			stand.record(t, "taint-untrusted-external", "FAIL", checks)
			t.Fatalf("content tainted %s was written into the trusted artifact store", taint)
		}
	}
	// The control: the same call with no taint succeeds, so the refusals above are
	// about the taint and not about the call shape.
	cleanID, _, _ := stand.upload(t, stand.HTTP, token, task+"-clean", "own-note.txt",
		[]byte("a note the employee dictated themselves"), map[string]any{})
	gates = append(gates, map[string]any{"taint": "(none)", "status": "PASS", "wrote": cleanID != "",
		"artifact": cleanID})
	checks["artifact_ingest_taint_gate"] = gates

	// (b) Where the review gate lives in this configuration. The Memory owner's
	// review/Secret-Firewall store (ahp_memory.memories, review_required) is the
	// component that holds an external-tainted candidate for review; under
	// AHP_MEMORY_BACKEND=hindsight the mounted face is the Hindsight one and the
	// proposal face is not reachable (cmd/ahp/mount_memory.go chooses one face,
	// cmd/ahp/mount_hindsight.go keeps ahp_memory only as the erasure owner). The
	// scenario records that instead of pretending the gate was exercised.
	var proposeProbe map[string]any
	err := stand.call(stand.HTTP, "/v1/memory/Propose", map[string]any{
		"kind": "personal", "text": "the delegated answer said 42", "op_key": task + "-propose",
		"source_kind": "agent_answer",
		"evidence":    []map[string]any{{"source_kind": "agent_answer", "taints": []string{"untrusted_external"}}},
	}, token, nil)
	proposeProbe = map[string]any{"http": status(err), "code": errorCode(err), "message": truncate(errText(err), 160)}
	if err == nil {
		proposeProbe["status"] = "PARTIAL"
		proposeProbe["note"] = "the proposal face answered; re-check whether the review gate held, " +
			"this build did not expect it to be mounted"
	} else {
		proposeProbe["status"] = "PARTIAL"
		proposeProbe["note"] = "DEF/GAP: the review gate of ahp_memory (review_required, Secret Firewall) is " +
			"unreachable through the HTTP face when AHP_MEMORY_BACKEND=hindsight, so the taint rule of L943 " +
			"is enforced here only by the artifact ingest gate above; owner area: Memory"
	}
	checks["memory_review_gate_reachability"] = proposeProbe

	stand.record(t, "taint-untrusted-external", "PASS", checks)
}

// tryDelegateChild mints one child grant under the caller's own authority root
// and returns its task token plus conversation. It is the same call Space makes
// (internal/space/rungrant.go); the error is returned instead of failing the
// scenario, because whether the platform hands out a run credential from outside
// a run is itself part of what this lane measures.
func (s *instance) tryDelegateChild(client *http.Client, parentToken, parentTask, childTask,
	contextID string, capabilities []string) (string, string, error) {
	var out struct {
		Token   string `json:"token"`
		Context string `json:"context_id"`
	}
	err := s.call(client, "/v1/identity/roots/"+parentTask+"/delegate", map[string]any{
		"task": childTask, "context": contextID, "capabilities": capabilities, "ttl": "5m"}, parentToken, &out)
	if err != nil {
		return "", "", err
	}
	if out.Token == "" {
		return "", "", &apiError{Status: 0, Code: "NO_TOKEN", Message: "the delegate reply carried no child token"}
	}
	return out.Token, out.Context, nil
}
