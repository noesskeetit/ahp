package stateapi

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ---------------------------------------------------------------------------
// Two tenants, ONE PostgreSQL database, ONE State server.
//
// The composed HTTP stand cannot express this check: its dev-PKI gives the
// human and the workload the same tenant and admits exactly one exchange
// (spacesvc), so a human of another tenant is refused at AcceptTurn and never
// reaches the tenant predicate in State. Here both tenants are real principals
// of the same running server: each gets its own SPIFFE workload certificate and
// its own Ed25519 task token, and every row is created by the product layer
// through the HTTP surface (CreateThread/CreateRun/ClaimRun/AppendStream/
// PutCheckpoint). No SQL fixtures, no stand PKI, no hand-written rows.
//
// Every checked fact prints one machine-readable ASSERT line so the leg can
// turn the run into evidence.jsonl and give a FAIL a non-zero exit code.
// ---------------------------------------------------------------------------

// plane is what both identities in this file can do: mint a real task token for
// their own scope and call one operation over the real mTLS surface.
type plane interface {
	signTask(t *testing.T, task string) string
	call(t *testing.T, op, token, body string) (int, map[string]any)
	adminExport(t *testing.T, threadID string) (int, map[string]any)
}

var (
	_ plane = (*env)(nil)
	_ plane = (*laneWorkload)(nil)
)

// laneWorkload is a second tenant on the same server and in the same database.
type laneWorkload struct {
	env        *env
	tenant     string
	deployment string
	subject    string
	client     *http.Client
}

// foreignWorkload mints a second tenant identity: a workload certificate for a
// fresh (tenant, deployment) signed by the lane's CA, used through its own
// HTTP client. The subject id is deliberately the same as the lane's own
// subject, so a leak can only come from the tenant predicate.
func (e *env) foreignWorkload(t *testing.T, subject string) *laneWorkload {
	t.Helper()
	tenant := "t-" + contracts.NewID()
	deployment := "d-" + contracts.NewID()
	cert := e.ca.clientCert(t, workloadURI(tenant, deployment))
	base, ok := e.srv.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatal("the test server must use an *http.Transport to mint a second identity")
	}
	// Transport.Clone() gives this identity its own TLSClientConfig, so the
	// pinned CA of the test server survives and only the client certificate
	// differs. Nothing about the lane's own client is mutated.
	transport := base.Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	transport.TLSClientConfig.Certificates = []tls.Certificate{cert}
	return &laneWorkload{env: e, tenant: tenant, deployment: deployment, subject: subject,
		client: &http.Client{Transport: transport, Timeout: 30 * time.Second}}
}

func (w *laneWorkload) signTask(t *testing.T, task string) string {
	t.Helper()
	tok, err := w.env.signer.Sign(auth.Claims{
		Audience: testAudience, Kind: auth.KindTask,
		Tenant: w.tenant, Deployment: w.deployment,
		Subject: w.subject, Task: task, Root: "run-1",
		Context: "ctx-1", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func (w *laneWorkload) call(t *testing.T, op, token, body string) (int, map[string]any) {
	t.Helper()
	return doCall(t, w.client, w.env.srv.URL+"/v1/state/"+op, http.MethodPost, token, body)
}

// callGet issues a non-POST request. The State plane serves no such route, which
// is itself the answer the isolation check needs: a thread id in a query string
// must not be able to read anything, and must not disclose whether it exists.
func (e *env) callGet(t *testing.T, path, token string) (int, map[string]any) {
	t.Helper()
	return doCall(t, e.srv.Client(), e.srv.URL+path, http.MethodGet, token, "")
}

func (w *laneWorkload) callGet(t *testing.T, path, token string) (int, map[string]any) {
	t.Helper()
	return doCall(t, w.client, w.env.srv.URL+path, http.MethodGet, token, "")
}

func doCall(t *testing.T, client *http.Client, endpoint, method, token, body string) (int, map[string]any) {
	t.Helper()
	var payload io.Reader
	if body != "" {
		payload = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, endpoint, payload)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

// threadContext reads back the context_id a thread carries, so a foreign probe
// uses a real foreign address instead of a guess.
func threadContext(t *testing.T, p plane, threadID string) string {
	t.Helper()
	status, body := p.call(t, "GetThread", p.signTask(t, "task-"+contracts.NewID()), `{"thread_id":"`+threadID+`"}`)
	mustStatus(t, status, http.StatusOK, body)
	context, _ := body["context_id"].(string)
	if context == "" {
		t.Fatalf("thread %s carries no context_id: %v", threadID, body)
	}
	return context
}

func countThreads(t *testing.T, p plane, token string) int {
	t.Helper()
	status, listed := p.call(t, "ListThreads", token, `{"limit":100}`)
	mustStatus(t, status, http.StatusOK, listed)
	threads, _ := listed["threads"].([]any)
	return len(threads)
}

// ---------------------------------------------------------------------------
// Machine-readable verdicts.
// ---------------------------------------------------------------------------

type verdicts struct{ total, passed, failed int }

func (v *verdicts) check(t *testing.T, name, detail string, pass bool) {
	t.Helper()
	v.total++
	outcome := "FAIL"
	if pass {
		v.passed++
		outcome = "PASS"
	} else {
		v.failed++
	}
	t.Logf("ASSERT|name=%s|%s|verdict=%s", name, detail, outcome)
	if !pass {
		t.Errorf("claim %s: %s", name, detail)
	}
}

func (v *verdicts) http(t *testing.T, name, route, principal string, status int, body map[string]any,
	wantStatus int, wantCode, wantReason string) {
	t.Helper()
	code, _ := body["code"].(string)
	reason, _ := body["reason"].(string)
	pass := status == wantStatus && (wantCode == "" || code == wantCode) &&
		(wantReason == "" || reason == wantReason)
	v.check(t, name, fmt.Sprintf("route=%s principal=%s got(%s) want(status=%d code=%s reason=%s)",
		route, principal, respFields(status, body), wantStatus, dash(wantCode), dash(wantReason)), pass)
}

// denied accepts any authentication/authorization refusal: the point is that no
// route serves data to a principal that is not its own.
func (v *verdicts) denied(t *testing.T, name, route, principal string, status int, body map[string]any) {
	t.Helper()
	v.check(t, name, fmt.Sprintf("route=%s principal=%s got(%s) want(401|403)", route, principal,
		respFields(status, body)),
		status == http.StatusUnauthorized || status == http.StatusForbidden)
}

func (v *verdicts) equal(t *testing.T, name, detail string, pass bool) {
	t.Helper()
	v.check(t, name, detail, pass)
}

func respFields(status int, body map[string]any) string {
	code, _ := body["code"].(string)
	reason, _ := body["reason"].(string)
	return fmt.Sprintf("status=%d code=%s reason=%s", status, dash(code), dash(reason))
}

func dash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

// ---------------------------------------------------------------------------
// The check itself.
// ---------------------------------------------------------------------------

func TestCrossTenantDataPlaneIsolation(t *testing.T) {
	a := newEnv(t)
	b := a.foreignWorkload(t, testSubject)
	v := &verdicts{}

	t.Logf("EVIDENCE|database=%s|tenant_a=%s|deployment_a=%s|tenant_b=%s|deployment_b=%s|subject=%s",
		laneDatabase(t), a.tenant, a.deployment, b.tenant, b.deployment, testSubject)

	// Both tenants get real content on every surface the check touches, created
	// exclusively through the product surface.
	aThread, _ := seedThread(t, a)
	bThread, bExport := seedThread(t, b)
	aContext := threadContext(t, a, aThread)
	bContext := threadContext(t, b, bThread)
	aTok := a.signTask(t, "task-"+contracts.NewID())
	bTok := b.signTask(t, "task-"+contracts.NewID())

	aSeeded, bSeeded := countThreads(t, a, aTok), countThreads(t, b, bTok)
	v.equal(t, "BASELINE_A_THREADS", fmt.Sprintf("tenant_a=%s own_threads=%d want=1", a.tenant, aSeeded), aSeeded == 1)
	v.equal(t, "BASELINE_B_THREADS", fmt.Sprintf("tenant_b=%s own_threads=%d want=1", b.tenant, bSeeded), bSeeded == 1)

	routes := []struct {
		name string
		op   string
		body func(thread, context string) string
	}{
		{"GetThread", "GetThread", func(th, _ string) string { return `{"thread_id":"` + th + `"}` }},
		{"GetThreadByContext", "GetThreadByContext", func(_, c string) string {
			return `{"context_id":"` + c + `"}`
		}},
		{"ListCheckpoints", "ListCheckpoints", func(th, _ string) string {
			return `{"thread_id":"` + th + `","namespace":"lg","limit":10}`
		}},
		{"GetPart", "GetPart", func(th, _ string) string {
			return `{"thread_id":"` + th + `","namespace":"lg","channel":"values","version":"v1"}`
		}},
		{"ReadStream", "ReadStream", func(th, _ string) string {
			return `{"thread_id":"` + th + `","after":0,"limit":100}`
		}},
	}

	// 1 + 2 + 3: every read route, both directions, plus the positive control
	// that the owner still reads its own thread, plus the transport binding.
	for _, rt := range routes {
		status, body := a.call(t, rt.op, aTok, rt.body(bThread, bContext))
		v.http(t, "DENY_A_reads_B_"+rt.name, rt.op, "tenant_A_on_tenant_B_row", status, body,
			http.StatusNotFound, contracts.ErrNotFound, "")

		ghostStatus, ghostBody := a.call(t, rt.op, aTok,
			rt.body("thr-"+contracts.NewID(), "ctx-never-created-"+contracts.NewID()))
		v.equal(t, "NO_ORACLE_A_reads_B_"+rt.name,
			fmt.Sprintf("route=%s existing_foreign(%s) nonexistent(%s)", rt.op,
				respFields(status, body), respFields(ghostStatus, ghostBody)),
			ghostStatus == status && ghostBody["code"] == body["code"] && ghostBody["reason"] == body["reason"])

		revStatus, revBody := b.call(t, rt.op, bTok, rt.body(aThread, aContext))
		v.http(t, "DENY_B_reads_A_"+rt.name, rt.op, "tenant_B_on_tenant_A_row", revStatus, revBody,
			http.StatusNotFound, contracts.ErrNotFound, "")

		revGhostStatus, revGhostBody := b.call(t, rt.op, bTok,
			rt.body("thr-"+contracts.NewID(), "ctx-never-created-"+contracts.NewID()))
		v.equal(t, "NO_ORACLE_B_reads_A_"+rt.name,
			fmt.Sprintf("route=%s existing_foreign(%s) nonexistent(%s)", rt.op,
				respFields(revStatus, revBody), respFields(revGhostStatus, revGhostBody)),
			revGhostStatus == revStatus && revGhostBody["code"] == revBody["code"] &&
				revGhostBody["reason"] == revBody["reason"])

		ownStatus, ownBody := a.call(t, rt.op, aTok, rt.body(aThread, aContext))
		v.http(t, "POSITIVE_A_owns_"+rt.name, rt.op, "tenant_A_on_tenant_A_row", ownStatus, ownBody,
			http.StatusOK, "", "")
		ownStatus, ownBody = b.call(t, rt.op, bTok, rt.body(bThread, bContext))
		v.http(t, "POSITIVE_B_owns_"+rt.name, rt.op, "tenant_B_on_tenant_B_row", ownStatus, ownBody,
			http.StatusOK, "", "")

		// A token is bound to its workload certificate: tenant A's token on
		// tenant B's connection is refused by the transport, not by a query.
		bindStatus, bindBody := b.call(t, rt.op, aTok, rt.body(bThread, bContext))
		v.denied(t, "TRANSPORT_BIND_"+rt.name, rt.op, "tenant_A_token_on_tenant_B_certificate",
			bindStatus, bindBody)
	}

	// The GET form of the stream route: the plane answers the same NOT_FOUND as
	// for any unknown route, so a foreign thread id in a query discloses nothing.
	for _, probe := range []struct {
		name    string
		caller  string
		foreign string
		call    func(t *testing.T, thread, token string) (int, map[string]any)
		token   string
	}{
		{"GET_stream_A_reads_B", "tenant_A", bThread, a.callGet, aTok},
		{"GET_stream_B_reads_A", "tenant_B", aThread, b.callGet, bTok},
	} {
		path := "/v1/state/stream?thread_id=" + url.QueryEscape(probe.foreign)
		status, body := probe.call(t, path, probe.token)
		v.http(t, "DENY_"+probe.name, "GET /v1/state/stream", probe.caller+" on a foreign thread id",
			status, body, http.StatusNotFound, contracts.ErrNotFound, "")

		ghost := "/v1/state/stream?thread_id=" + url.QueryEscape("thr-"+contracts.NewID())
		ghostStatus, ghostBody := probe.call(t, ghost, probe.token)
		v.equal(t, "NO_ORACLE_"+probe.name,
			fmt.Sprintf("route=GET /v1/state/stream existing_foreign(%s) nonexistent(%s)",
				respFields(status, body), respFields(ghostStatus, ghostBody)),
			ghostStatus == status && ghostBody["code"] == body["code"])
	}

	// 4: the F1 case across tenants. Export bytes carry the scope they came from;
	// importing them from another tenant is refused, and nothing is written.
	aBefore := countThreads(t, a, aTok)
	status, imported := a.call(t, "ImportThread", aTok, `{"export":`+string(bExport)+`}`)
	v.http(t, "IMPORT_FOREIGN_TENANT_REFUSED", "ImportThread", "tenant_A_importing_tenant_B_export",
		status, imported, http.StatusForbidden, contracts.ErrForbidden, state.ReasonImportScopeMismatch)
	aAfter := countThreads(t, a, aTok)
	v.equal(t, "IMPORT_FOREIGN_WROTE_NOTHING",
		fmt.Sprintf("route=ImportThread tenant_a_threads_before=%d after=%d", aBefore, aAfter), aAfter == aBefore)
	status, stillBlind := a.call(t, "GetThread", aTok, `{"thread_id":"`+bThread+`"}`)
	v.http(t, "IMPORT_FOREIGN_STILL_BLIND", "GetThread", "tenant_A_after_the_refused_import",
		status, stillBlind, http.StatusNotFound, contracts.ErrNotFound, "")

	// F2 semantics, stated as a fact: the owner restores its own bytes under an
	// explicit retarget, and restoring into a context it already owns is a
	// documented refusal rather than a silent merge.
	newCtx := "ctx-restored-" + contracts.NewID()
	status, restored := b.call(t, "ImportThread", bTok, `{"export":`+string(bExport)+`,"context_id":"`+newCtx+`"}`)
	v.http(t, "IMPORT_OWNER_RETARGET", "ImportThread", "tenant_B_restoring_its_own_export",
		status, restored, http.StatusOK, "", "")
	restoredID, _ := restored["id"].(string)
	v.equal(t, "IMPORT_OWNER_RETARGET_IS_NEW_THREAD",
		fmt.Sprintf("route=ImportThread restored=%s source=%s context=%v want_context=%s",
			dash(restoredID), bThread, restored["context_id"], newCtx),
		restoredID != "" && restoredID != bThread && restored["context_id"] == newCtx)
	v.equal(t, "IMPORT_OWNER_RETARGET_COPIES_CONTENT",
		fmt.Sprintf("route=ReadStream+ListCheckpoints+GetPart source_thread=%s restored_thread=%s", bThread, restoredID),
		restoredContentEquals(t, b, bTok, bThread, restoredID))

	status, taken := b.call(t, "ImportThread", bTok, `{"export":`+string(bExport)+`,"context_id":"`+bContext+`"}`)
	v.http(t, "IMPORT_OWNER_TAKEN_CONTEXT", "ImportThread", "tenant_B_restoring_into_its_own_live_context",
		status, taken, http.StatusConflict, contracts.ErrConflict, state.ReasonImportContextTaken)

	// The refused foreign attempt left the owner's namespace intact.
	status, stillOK := b.call(t, "ImportThread", bTok,
		`{"export":`+string(bExport)+`,"context_id":"ctx-free-`+contracts.NewID()+`"}`)
	v.http(t, "IMPORT_OWNER_STILL_OK_AFTER_FOREIGN_REFUSAL", "ImportThread", "tenant_B_after_tenant_As_refusal",
		status, stillOK, http.StatusOK, "", "")

	// 5: nothing depends on row order or caches. A second fresh pair of tenants
	// in the same database must give the same verdicts from scratch.
	c := a.foreignWorkload(t, testSubject)
	d := a.foreignWorkload(t, testSubject)
	cThread, _ := seedThread(t, c)
	seedThread(t, d)
	cTok := c.signTask(t, "task-"+contracts.NewID())
	dTok := d.signTask(t, "task-"+contracts.NewID())
	status, repeat := d.call(t, "GetThread", dTok, `{"thread_id":"`+cThread+`"}`)
	v.http(t, "REPEAT_DENY_SECOND_PAIR", "GetThread", "tenant_D_on_tenant_C_row", status, repeat,
		http.StatusNotFound, contracts.ErrNotFound, "")
	status, repeatOwn := c.call(t, "ReadStream", cTok, `{"thread_id":"`+cThread+`","after":0,"limit":100}`)
	v.http(t, "REPEAT_POSITIVE_SECOND_PAIR", "ReadStream", "tenant_C_on_its_own_row", status, repeatOwn,
		http.StatusOK, "", "")

	outcome := "PASS"
	if v.failed != 0 {
		outcome = "FAIL"
	}
	t.Logf("SUITE|claims=%d|pass=%d|fail=%d|database=%s|verdict=%s", v.total, v.passed, v.failed, laneDatabase(t), outcome)
}

// restoredContentEquals compares a restored thread with its source on every
// durable surface an export carries: stream bytes, checkpoint parts count and
// the blob bytes themselves.
func restoredContentEquals(t *testing.T, p plane, token, source, restored string) bool {
	t.Helper()
	if restored == "" {
		return false
	}
	_, srcStream := p.call(t, "ReadStream", token, `{"thread_id":"`+source+`","after":0,"limit":100}`)
	_, dstStream := p.call(t, "ReadStream", token, `{"thread_id":"`+restored+`","after":0,"limit":100}`)
	if streamDigest(t, srcStream["events"]) != streamDigest(t, dstStream["events"]) {
		t.Logf("detail|reason=stream digest differs: source=%v restored=%v", srcStream["events"], dstStream["events"])
		return false
	}
	_, srcCPs := p.call(t, "ListCheckpoints", token, `{"thread_id":"`+source+`","namespace":"lg","limit":10}`)
	_, dstCPs := p.call(t, "ListCheckpoints", token, `{"thread_id":"`+restored+`","namespace":"lg","limit":10}`)
	srcList, _ := srcCPs["checkpoints"].([]any)
	dstList, _ := dstCPs["checkpoints"].([]any)
	if len(srcList) != len(dstList) || len(dstList) == 0 {
		t.Logf("detail|reason=checkpoint count differs: source=%d restored=%d", len(srcList), len(dstList))
		return false
	}
	// checkpoint_parts, not only checkpoint_blobs: an export carries its channels
	// as parts, and on the stand the blob table can legitimately be empty.
	if src, dst := projectCheckpoints(srcList), projectCheckpoints(dstList); src != dst {
		t.Logf("detail|reason=checkpoint parts differ: source=%s restored=%s", src, dst)
		return false
	}
	_, srcPart := p.call(t, "GetPart", token,
		`{"thread_id":"`+source+`","namespace":"lg","channel":"values","version":"v1"}`)
	_, dstPart := p.call(t, "GetPart", token,
		`{"thread_id":"`+restored+`","namespace":"lg","channel":"values","version":"v1"}`)
	if srcPart["data"] != dstPart["data"] || srcPart["data"] == nil {
		t.Logf("detail|reason=checkpoint part bytes differ: source=%v restored=%v", srcPart["data"], dstPart["data"])
		return false
	}
	return true
}

// projectCheckpoints projects a listed checkpoint onto the fields a restore has
// to reproduce exactly: identity, namespace, version and the part references
// (channel@version). Created timestamps are deliberately excluded: they are
// assigned by the restore, not carried by the export.
func projectCheckpoints(list []any) string {
	out := make([]map[string]any, 0, len(list))
	for _, item := range list {
		cp, _ := item.(map[string]any)
		parts, _ := cp["parts"].([]any)
		refs := make([]string, 0, len(parts))
		for _, part := range parts {
			p, _ := part.(map[string]any)
			refs = append(refs, fmt.Sprintf("%v@%v", p["channel"], p["version"]))
		}
		out = append(out, map[string]any{
			"id": cp["id"], "namespace": cp["namespace"],
			"version": cp["version"], "parts": refs,
		})
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return ""
	}
	return string(raw)
}

// laneDatabase names the database this run is against, for the evidence. Only
// the name is ever returned or printed: the DSN carries credentials and must
// never reach a log.
func laneDatabase(t *testing.T) string {
	t.Helper()
	if name := os.Getenv("AHP_DB_NAME"); name != "" {
		return name
	}
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		return "unset"
	}
	if parsed, err := url.Parse(dsn); err == nil && parsed.Path != "" {
		return strings.TrimPrefix(parsed.Path, "/")
	}
	if i := strings.LastIndex(dsn, "/"); i >= 0 {
		name := dsn[i+1:]
		if j := strings.IndexAny(name, "?#"); j >= 0 {
			name = name[:j]
		}
		return name
	}
	return "unparseable"
}
