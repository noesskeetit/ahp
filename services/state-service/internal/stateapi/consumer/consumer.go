// Package consumer is the consumer-driven conformance driver for the frozen
// State candidate; command cmd/state-consumer and the historical entry point
// tests/state/httpprobe both run it. It performs every State operation over HTTPS+mTLS against a live
// candidate process and a live PostgreSQL, and appends one JSONL record per
// wire call: the operation, the authentication actually used, the HTTP status
// the server really returned, the contract error code, latency and the
// post-conditions observed in the response.
//
// Nothing is mocked. The transport is net/http with a real client certificate,
// the credentials come from the candidate handshake file, the tokens are the
// ones the candidate itself issued (ClaimRun returns a fresh task token), and
// every assertion is a property observed on the wire: idempotent replay,
// fence staleness, scope isolation, empty-bytes versus null, strict request
// shapes and honest error codes.
//
// The exit status is non-zero when any call deviated from its expectation or
// when a contract operation was not covered with a success, so this binary is
// usable as a gate, not only as a reporter.
//
// Secrets are never printed: tokens, DSNs and private keys stay in the
// handshake file; records carry only hashes of request/response bodies.
package consumer

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// contractOps is the operation list of the State HTTP contract: the 36
// operations named in the contract addendum (its heading says 35, its own
// by-name list holds 36 - verified by parsing that post) plus the control-plane
// ImportThread route the same handler serves. Coverage is reported per name, so
// a missing operation fails the gate instead of quietly passing.
var contractOps = []string{
	"CreateThread", "GetThread", "GetThreadByContext", "ListThreads",
	"CreateRun", "GetRun", "ListRuns", "ClaimRun", "Heartbeat", "Release",
	"Suspend", "PutCheckpoint", "GetCheckpoint", "ListCheckpoints", "GetPart",
	"PutWrites", "GetWrites", "PutKV", "DeleteKV", "GetKV", "SearchKV",
	"RecordIntent", "UpdateIntent", "ReconcileIntent", "GetIntent", "Enqueue",
	"ReadInbox", "AckInbox", "AppendStream", "ReadStream", "PutBatch",
	"DeleteThread", "DeleteSubject", "SetLegalhold", "RemoveLegalhold",
	"ExportThread", "ImportThread",
}

// handshake mirrors the file the candidate writes at startup.
type handshake struct {
	BaseURL          string `json:"base_url"`
	CAFile           string `json:"ca_file"`
	CertFile         string `json:"cert_file"`
	KeyFile          string `json:"key_file"`
	TaskToken        string `json:"task_token"`
	Tenant           string `json:"tenant"`
	Deployment       string `json:"deployment"`
	Subject          string `json:"subject"`
	ForeignTenant    string `json:"foreign_tenant"`
	ForeignCertFile  string `json:"foreign_cert_file"`
	ForeignKeyFile   string `json:"foreign_key_file"`
	ForeignTaskToken string `json:"foreign_task_token"`
	PID              int    `json:"pid"`
}

// assertion is one observed post-condition attached to a record.
type assertion struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// record is one evidence line.
type record struct {
	Schema      string      `json:"schema"`
	Seq         int         `json:"seq"`
	TS          string      `json:"ts"`
	Op          string      `json:"op"`
	Auth        string      `json:"auth"`
	Method      string      `json:"method"`
	Path        string      `json:"path"`
	Status      int         `json:"status"`
	Expect      []int       `json:"expect_status"`
	Code        string      `json:"code,omitempty"`
	ExpectCode  string      `json:"expect_code,omitempty"`
	OK          bool        `json:"ok"`
	Note        string      `json:"note,omitempty"`
	RequestSHA  string      `json:"request_sha256,omitempty"`
	ResponseSHA string      `json:"response_sha256,omitempty"`
	LatencyMS   int64       `json:"latency_ms"`
	Assertions  []assertion `json:"assertions,omitempty"`
}

// probe owns the three TLS clients and the evidence stream.
type probe struct {
	self         *http.Client // this workload: certificate + task token
	foreign      *http.Client // a foreign tenant: its own certificate
	anon         *http.Client // TLS without any client certificate
	base         string
	token        string
	foreignToken string
	tenant       string
	deployment   string
	subject      string

	out      *os.File
	buf      *bufio.Writer
	seq      int
	failures int
	ok       map[string]bool
}

type callSpec struct {
	op     string
	auth   string
	token  string
	body   []byte
	method string
	expect []int
	code   string
	note   string
	check  func(status int, body []byte) []assertion
}

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// contentDigest digests selected fields of a JSON array in order, skipping the
// addresses that legitimately differ between a thread and its restore. It is
// how the import leg proves durable fidelity: equal bytes, different ids.
func contentDigest(body []byte, arrayPath, containerPath string, fields ...string) string {
	var cur any
	if err := json.Unmarshal(body, &cur); err != nil {
		return "unparseable"
	}
	for _, seg := range strings.Split(arrayPath, ".") {
		if seg == "" {
			continue
		}
		m, ok := cur.(map[string]any)
		if !ok {
			return "shape"
		}
		cur = m[seg]
	}
	list, _ := cur.([]any)
	pick := func(item any) []byte {
		m, _ := item.(map[string]any)
		var out []byte
		for _, f := range fields {
			v, _ := json.Marshal(m[f])
			out = append(out, []byte(f+"=")...)
			out = append(out, v...)
			out = append(out, 0)
		}
		return out
	}
	h := sha256.New()
	for _, item := range list {
		if containerPath == "" {
			h.Write(pick(item))
			continue
		}
		m, _ := item.(map[string]any)
		nested, _ := m[containerPath].([]any)
		for _, part := range nested {
			h.Write(pick(part))
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func js(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func dig(body []byte, path ...string) (any, bool) {
	var cur any
	if err := json.Unmarshal(body, &cur); err != nil {
		return nil, false
	}
	for _, seg := range path {
		if arr, isArr := cur.([]any); isArr {
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(arr) {
				return nil, false
			}
			cur = arr[idx]
			continue
		}
		m, isObj := cur.(map[string]any)
		if !isObj {
			return nil, false
		}
		cur, isObj = m[seg]
		if !isObj {
			return nil, false
		}
	}
	return cur, true
}

func strOf(body []byte, path ...string) string {
	v, ok := dig(body, path...)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func numOf(body []byte, path ...string) (float64, bool) {
	v, ok := dig(body, path...)
	if !ok {
		return 0, false
	}
	n, ok := v.(float64)
	return n, ok
}

func lenOf(body []byte, path ...string) (int, bool) {
	v, ok := dig(body, path...)
	if !ok {
		return 0, false
	}
	arr, ok := v.([]any)
	if !ok {
		return 0, false
	}
	return len(arr), true
}

func eqStr(name, got, want string) assertion {
	return assertion{Name: name, OK: got == want, Detail: fmt.Sprintf("got=%q want=%q", got, want)}
}

func eqBool(name string, got, want bool) assertion {
	return assertion{Name: name, OK: got == want, Detail: fmt.Sprintf("got=%v want=%v", got, want)}
}

func eqNum(name string, got float64, want float64) assertion {
	return assertion{Name: name, OK: got == want, Detail: fmt.Sprintf("got=%v want=%v", got, want)}
}

func check(name string, cond bool, detail string) assertion {
	return assertion{Name: name, OK: cond, Detail: detail}
}

// present asserts that a JSON path exists in the body.
func present(body []byte, name string, path ...string) assertion {
	_, ok := dig(body, path...)
	return check(name, ok, "path="+strings.Join(path, "."))
}

// rawField returns the literal JSON token encoded for a field, so the
// difference between null and "" survives into an assertion. Path segments
// index objects by name and arrays by decimal position.
func rawField(body []byte, path ...string) (string, bool) {
	cur := body
	for i, seg := range path {
		trimmed := bytes.TrimLeft(cur, " \t\r\n")
		if len(trimmed) > 0 && trimmed[0] == '[' {
			var arr []json.RawMessage
			if err := json.Unmarshal(cur, &arr); err != nil {
				return "", false
			}
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(arr) {
				return "", false
			}
			cur = arr[idx]
		} else {
			var doc map[string]json.RawMessage
			if err := json.Unmarshal(cur, &doc); err != nil {
				return "", false
			}
			v, ok := doc[seg]
			if !ok {
				return "", false
			}
			cur = v
		}
		if i == len(path)-1 {
			return string(bytes.TrimRight(cur, " \t\r\n")), true
		}
	}
	return "", false
}

// boolOf reads a boolean field, reporting false when it is absent or not a
// boolean.
func boolOf(body []byte, path ...string) bool {
	v, ok := dig(body, path...)
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}

func newClient(caFile, certFile, keyFile string, withCert bool) (*http.Client, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("ca pem rejected")
	}
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if withCert {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: cfg, MaxIdleConnsPerHost: 4},
		Timeout:   30 * time.Second,
	}, nil
}

// Prefix labels the lines this driver prints: it is the reporting format of the
// tool, identical for the public command and for the historical entry point.
const Prefix = "state-consumer"

// spaceNS is the namespace the Space lane writes its snapshots into
// (internal/space CheckpointNamespace). The consumer scenario uses it as the
// one non-empty checkpoint namespace, to pin that the State filter is exact.
const spaceNS = "space/core"

// ErrUsage marks an unusable command line. The command maps it to exit code 2,
// every other error to exit code 1, so a caller can tell "you invoked me
// wrong" from "the candidate deviated from the contract".
var ErrUsage = errors.New("--handshake, --evidence and --worker are required")

// ErrDeviations is returned when a wire call deviated from its expectation or a
// contract operation was never covered by a successful call.
var ErrDeviations = errors.New("the run recorded deviations from the State contract")

// abort unwinds Run from any helper. A library must not terminate the process
// that imports it, which is what the os.Exit calls this replaces used to do.
type abort struct{ err error }

func fail(err error) {
	if err != nil {
		panic(abort{err})
	}
}

// Run performs the whole consumer scenario and appends the evidence file. It
// returns ErrUsage for a bad command line, ErrDeviations when the candidate
// broke the contract, and another error when the run itself could not proceed.
func Run(args []string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			ab, ok := r.(abort)
			if !ok {
				panic(r)
			}
			err = ab.err
		}
	}()
	return drive(args)
}

func drive(args []string) error {
	fs := flag.NewFlagSet(Prefix, flag.ContinueOnError)
	handshakePath := fs.String("handshake", "", "candidate handshake json (required)")
	evidencePath := fs.String("evidence", "", "jsonl evidence file to append (required)")
	worker := fs.String("worker", "", "worker identity for ClaimRun (required)")
	note := fs.String("note", "", "free-form note recorded in the summary")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *handshakePath == "" || *evidencePath == "" || *worker == "" {
		return ErrUsage
	}
	hs := readHandshake(*handshakePath)

	out, err := os.OpenFile(*evidencePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		fail(err)
	}
	defer out.Close()

	p := &probe{
		base:         hs.BaseURL,
		token:        hs.TaskToken,
		foreignToken: hs.ForeignTaskToken,
		tenant:       hs.Tenant,
		deployment:   hs.Deployment,
		subject:      hs.Subject,
		out:          out,
		buf:          bufio.NewWriter(out),
		ok:           map[string]bool{},
	}
	p.self, err = newClient(hs.CAFile, hs.CertFile, hs.KeyFile, true)
	if err != nil {
		fail(fmt.Errorf("self client: %w", err))
	}
	if hs.ForeignCertFile != "" {
		p.foreign, err = newClient(hs.CAFile, hs.ForeignCertFile, hs.ForeignKeyFile, true)
		if err != nil {
			fail(fmt.Errorf("foreign client: %w", err))
		}
	}
	p.anon, err = newClient(hs.CAFile, "", "", false)
	if err != nil {
		fail(fmt.Errorf("anon client: %w", err))
	}

	summary := p.run(*worker)

	p.writeSummary(hs, *worker, *note, summary)
	if err := p.buf.Flush(); err != nil {
		fail(fmt.Errorf("flush evidence: %w", err))
	}
	missing := missingOps(p.ok)
	fmt.Fprintf(os.Stderr, "state-consumer: calls=%d failures=%d ops_succeeded=%d/%d missing=%s\n",
		p.seq, p.failures, len(p.ok), len(contractOps), strings.Join(missing, ","))
	if p.failures > 0 || len(missing) > 0 {
		return ErrDeviations
	}
	return nil
}

func readHandshake(path string) handshake {
	raw, err := os.ReadFile(path)
	if err != nil {
		fail(fmt.Errorf("handshake: %w", err))
	}
	var hs handshake
	if err := json.Unmarshal(raw, &hs); err != nil {
		fail(fmt.Errorf("handshake: %w", err))
	}
	if hs.BaseURL == "" || hs.TaskToken == "" || hs.CertFile == "" {
		fail(errors.New("handshake is missing base_url/task_token/cert_file"))
	}
	return hs
}

func (p *probe) record(r record) {
	p.seq++
	r.Seq = p.seq
	r.Schema = "ahp.state-http-evidence.v1"
	r.TS = time.Now().UTC().Format(time.RFC3339Nano)
	line, err := json.Marshal(r)
	if err != nil {
		fail(fmt.Errorf("marshal evidence record: %w", err))
	}
	p.buf.Write(line)
	p.buf.WriteByte('\n')
	p.buf.Flush()
	if !r.OK {
		p.failures++
	}
	if r.Status >= 200 && r.Status < 300 && strings.HasPrefix(r.Path, "/v1/state/") {
		if op := strings.TrimPrefix(r.Path, "/v1/state/"); op != "" {
			p.ok[op] = true
		}
	}
	marker := "PASS"
	if !r.OK {
		marker = "FAIL"
	}
	fmt.Fprintf(os.Stderr, "%s %-16s %d %s\n", marker, r.Op, r.Status, r.Code)
}

// call performs one wire call and records the observation.
func (p *probe) call(spec callSpec) []byte {
	method := spec.method
	if method == "" {
		method = http.MethodPost
	}
	path := "/v1/state/" + spec.op
	var bodyReader io.Reader
	if spec.body != nil {
		bodyReader = bytes.NewReader(spec.body)
	}
	req, err := http.NewRequest(method, p.base+path, bodyReader)
	if err != nil {
		p.record(record{Op: spec.op, Auth: spec.auth, Method: method, Path: path,
			Expect: spec.expect, OK: false, Note: "request build: " + err.Error()})
		return nil
	}
	req.Header.Set("Content-Type", "application/json")

	client := p.self
	switch spec.auth {
	case "task-token":
		token := spec.token
		if token == "" {
			token = p.token
		}
		req.Header.Set("Authorization", "Bearer "+token)
	case "foreign-token":
		req.Header.Set("Authorization", "Bearer "+p.foreignToken)
		client = p.foreign
	case "foreign-cert+own-token":
		req.Header.Set("Authorization", "Bearer "+p.token)
		client = p.foreign
	case "workload-cert", "none":
		// ClaimRun: the certificate alone is the identity; no bearer header is
		// sent at all, exactly as the contract requires.
	case "no-client-cert":
		client = p.anon
	default:
		panic("state-consumer: unknown auth kind " + spec.auth)
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		status := 0
		note := classifyTransport(err)
		ok := contains(spec.expect, status)
		p.record(record{Op: spec.op, Auth: spec.auth, Method: method, Path: path,
			Status: status, Expect: spec.expect, OK: ok, Note: note,
			RequestSHA: sha(spec.body), LatencyMS: time.Since(start).Milliseconds()})
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	elapsed := time.Since(start).Milliseconds()

	code := strOf(body, "code")
	ok := contains(spec.expect, resp.StatusCode) && (spec.code == "" || code == spec.code)
	var assertions []assertion
	if spec.check != nil {
		assertions = spec.check(resp.StatusCode, body)
		for _, a := range assertions {
			if !a.OK {
				ok = false
			}
		}
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && code != "" {
		ok = false // a success body must not carry an error code
	}
	rec := record{
		Op: spec.op, Auth: spec.auth, Method: method, Path: path,
		Status: resp.StatusCode, Expect: spec.expect, Code: code, ExpectCode: spec.code,
		OK: ok, Note: spec.note, RequestSHA: sha(spec.body), ResponseSHA: sha(body),
		LatencyMS: elapsed, Assertions: assertions,
	}
	p.record(rec)
	return body
}

// classifyTransport names a transport-level rejection without echoing secrets.
func classifyTransport(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "certificate required"),
		strings.Contains(msg, "bad certificate"),
		strings.Contains(msg, "tls:"),
		strings.Contains(msg, "tls:"):
		return "TLS_REJECTED: " + truncate(msg, 160)
	default:
		return "TRANSPORT_ERROR: " + truncate(msg, 160)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func contains(list []int, v int) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func missingOps(done map[string]bool) []string {
	var missing []string
	for _, op := range contractOps {
		if !done[op] {
			missing = append(missing, op)
		}
	}
	sort.Strings(missing)
	return missing
}

func (p *probe) writeSummary(hs handshake, worker, note string, extra map[string]any) {
	summary := map[string]any{
		"schema":           "ahp.state-http-evidence.summary.v1",
		"ts":               time.Now().UTC().Format(time.RFC3339Nano),
		"base_url":         hs.BaseURL,
		"tenant":           hs.Tenant,
		"deployment":       hs.Deployment,
		"subject":          hs.Subject,
		"foreign_tenant":   hs.ForeignTenant,
		"server_pid":       hs.PID,
		"worker":           worker,
		"calls":            p.seq,
		"failures":         p.failures,
		"ops_total":        len(contractOps),
		"ops_succeeded":    len(p.ok),
		"ops_succeeded_at": sortedKeys(p.ok),
		"ops_missing":      missingOps(p.ok),
		"note":             note,
	}
	for k, v := range extra {
		summary[k] = v
	}
	line, err := json.Marshal(summary)
	if err != nil {
		fail(fmt.Errorf("marshal summary: %w", err))
	}
	p.buf.Write(line)
	p.buf.WriteByte('\n')
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// fenceJSON is the flat JSON shape of state.Fence, reused by every call that
// needs one (nested in "fence", flat at the top level for the lease ops).
func fenceJSON(runID, worker string, epoch float64) map[string]any {
	return map[string]any{"run_id": runID, "worker": worker, "epoch": epoch}
}

// flatFence spreads the lease token over the top level of a body, which is how
// Heartbeat/Release/Suspend take it (nested only for the mutation ops).
func flatFence(body map[string]any, runID, worker string, epoch float64) map[string]any {
	body["run_id"] = runID
	body["worker"] = worker
	body["epoch"] = epoch
	return body
}

// run drives the whole contract over one live TLS connection set. Every call
// states the authentication it really used, so the evidence shows the
// authentication split (ClaimRun on the workload certificate, everything else
// on the bearer task token) instead of asserting it in prose.
func (p *probe) run(worker string) map[string]any {
	suffix := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	ctxMain := "ctx-main-" + suffix
	ctxSecond := "ctx-second-" + suffix
	ctxHold := "ctx-hold-" + suffix
	kvNS := []string{"store", "mem-" + suffix}

	var (
		mainThread string
		mainRun    string
		epoch1     float64
		epoch2     float64
		token2     string
		inboxID    string
		digest1    string
		exportBody []byte
		imported   string
	)

	// --- threads and runs, bearer task token ------------------------------
	b := p.call(callSpec{op: "CreateThread", auth: "task-token",
		body:   js(map[string]any{"context_id": ctxMain, "key": "thread-" + suffix}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{
				present(b, "thread.id_present", "id"),
				eqStr("thread.context_id", strOf(b, "context_id"), ctxMain),
				eqStr("thread.scope_tenant", strOf(b, "scope", "tenant"), p.tenant),
				eqStr("thread.scope_subject", strOf(b, "scope", "subject"), p.subject),
			}
		}})
	mainThread = strOf(b, "id")

	p.call(callSpec{op: "CreateThread", auth: "task-token",
		body:   js(map[string]any{"context_id": ctxMain, "key": "thread-replay-" + suffix}),
		expect: []int{200},
		note:   "thread creation is idempotent per (scope, context_id): the same context replays the same thread",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("thread.replay_same_id", strOf(b, "id"), mainThread)}
		}})

	secondBody := p.call(callSpec{op: "CreateThread", auth: "task-token",
		body:   js(map[string]any{"context_id": ctxSecond, "key": "thread-second-" + suffix}),
		expect: []int{200},
		note:   "a distinct context opens a distinct thread"})
	secondThread := strOf(secondBody, "id")

	runBody := map[string]any{
		"thread_id": mainThread,
		"key":       "run-1-" + suffix,
		"payload":   map[string]any{"graph": "e2e", "step": 1},
	}
	b = p.call(callSpec{op: "CreateRun", auth: "task-token", body: js(runBody),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			seq, _ := numOf(b, "sequence")
			return []assertion{
				present(b, "run.id_present", "id"),
				eqStr("run.thread_id", strOf(b, "thread_id"), mainThread),
				eqStr("run.status_pending", strOf(b, "status"), "pending"),
				eqNum("run.sequence_first", seq, 1),
			}
		}})
	mainRun = strOf(b, "id")

	p.call(callSpec{op: "GetRun", auth: "task-token",
		body: js(map[string]any{"run_id": mainRun}), expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("run.id", strOf(b, "id"), mainRun),
				eqStr("run.status", strOf(b, "status"), "pending")}
		}})

	p.call(callSpec{op: "ListRuns", auth: "task-token",
		body: js(map[string]any{"thread_id": mainThread}), expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			n, ok := lenOf(b, "runs")
			return []assertion{check("runs.exactly_one", ok && n == 1, fmt.Sprintf("n=%d", n))}
		}})

	p.call(callSpec{op: "GetThread", auth: "task-token",
		body: js(map[string]any{"thread_id": mainThread}), expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("thread.id", strOf(b, "id"), mainThread)}
		}})

	p.call(callSpec{op: "GetThreadByContext", auth: "task-token",
		body: js(map[string]any{"context_id": ctxMain}), expect: []int{200},
		note: "the context resolves to the thread created first in this context",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("thread.id", strOf(b, "id"), mainThread)}
		}})

	p.call(callSpec{op: "ListThreads", auth: "task-token",
		body: js(map[string]any{"limit": 10}), expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			n, _ := lenOf(b, "threads")
			return []assertion{
				check("threads.at_least_two", n >= 2, fmt.Sprintf("n=%d", n)),
				present(b, "threads.has_more_present", "has_more"),
				present(b, "threads.next_cursor_present", "next_cursor"),
			}
		}})

	// --- ClaimRun on the workload certificate only ------------------------
	b = p.call(callSpec{op: "ClaimRun", auth: "workload-cert",
		body:   js(map[string]any{"worker": worker, "ttl_ms": 240000}),
		expect: []int{200},
		note:   "no Authorization header at all: the workload certificate is the identity",
		check: func(_ int, b []byte) []assertion {
			e, ok := numOf(b, "lease", "epoch")
			return []assertion{
				eqStr("lease.run_id", strOf(b, "lease", "run", "id"), mainRun),
				eqStr("lease.worker", strOf(b, "lease", "worker"), worker),
				eqStr("lease.scope_subject", strOf(b, "lease", "scope", "subject"), p.subject),
				check("lease.epoch_positive", ok && e >= 1, fmt.Sprintf("epoch=%v", e)),
				check("task_token_present", len(strOf(b, "task_token")) > 20, "issued with the lease"),
			}
		}})
	epoch1, _ = numOf(b, "lease", "epoch")
	claimToken := strOf(b, "task_token")
	if claimToken == "" {
		claimToken = p.token
	}

	// --- lease operations -------------------------------------------------
	heartbeat := func(tok, w string, epoch float64, ttl int) callSpec {
		return callSpec{op: "Heartbeat", auth: "task-token", token: tok,
			body:   js(flatFence(map[string]any{"ttl_ms": ttl}, mainRun, w, epoch)),
			expect: []int{200}}
	}
	p.call(heartbeat(claimToken, worker, epoch1, 240000))

	// A flat fence on a nested-fence operation must be rejected: the shapes are
	// not interchangeable, and a missing fence is an invalid request.
	p.call(callSpec{op: "PutKV", auth: "task-token", token: claimToken,
		body: js(flatFence(map[string]any{"namespace": kvNS, "key": "k-fence",
			"operation_key": "op-fence-" + suffix, "value": map[string]any{"a": 1}}, mainRun, worker, epoch1)),
		expect: []int{400}, code: "INVALID_ARGUMENT",
		note: "flat fence on a nested-fence operation: the 'fence' field is required"})

	// --- checkpoints ------------------------------------------------------
	cp1 := func(id, parent string, version int, parts []map[string]any, key string) map[string]any {
		return map[string]any{
			"fence": fenceJSON(mainRun, worker, epoch1),
			"checkpoint": map[string]any{
				"id": id, "thread_id": mainThread, "namespace": "", "version": version,
				"parent":   parent,
				"metadata": map[string]any{"source": "state-consumer", "step": version},
				"parts":    parts,
			},
			"key": key,
		}
	}
	partValue := []byte("state-e2e-" + suffix)
	partEmpty := []byte{}
	cpBody := cp1("cp-1", "", 1, []map[string]any{
		{"channel": "values", "version": "v1", "data": partValue},
		{"channel": "meta", "version": "v0", "data": partEmpty},
	}, "cpk-1-"+suffix)
	p.call(callSpec{op: "PutCheckpoint", auth: "task-token", token: claimToken, body: js(cpBody),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			v, _ := numOf(b, "version")
			n, _ := lenOf(b, "parts")
			return []assertion{eqStr("cp.id", strOf(b, "id"), "cp-1"),
				eqNum("cp.version", v, 1),
				check("cp.parts", n == 2, fmt.Sprintf("n=%d", n))}
		}})

	p.call(callSpec{op: "PutCheckpoint", auth: "task-token", token: claimToken, body: js(cpBody),
		expect: []int{200},
		note:   "identical replay under the same idempotency key returns the original checkpoint",
		check: func(_ int, b []byte) []assertion {
			v, _ := numOf(b, "version")
			return []assertion{eqStr("replay.id", strOf(b, "id"), "cp-1"), eqNum("replay.version_unchanged", v, 1)}
		}})

	reuseBody := cp1("cp-1", "", 1, []map[string]any{
		{"channel": "values", "version": "v9", "data": []byte("different")},
	}, "cpk-1-"+suffix)
	p.call(callSpec{op: "PutCheckpoint", auth: "task-token", token: claimToken, body: js(reuseBody),
		expect: []int{400}, code: "INVALID_ARGUMENT",
		note: "same idempotency key, different body: the class stays 400 and reason names the domain code",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("error.reason_is_key_reuse", strOf(b, "reason"), "KEY_REUSE_MISMATCH")}
		}})

	// A part with data == null references the blob stored under (channel,
	// version): no new blob, the same bytes.
	p.call(callSpec{op: "PutCheckpoint", auth: "task-token", token: claimToken,
		body: js(cp1("cp-2", "cp-1", 2, []map[string]any{
			{"channel": "values", "version": "v1", "data": nil},
		}, "cpk-2-"+suffix)),
		expect: []int{200},
		note:   "parent cp-1, part references the existing blob",
		check: func(_ int, b []byte) []assertion {
			v, _ := numOf(b, "version")
			return []assertion{eqStr("cp2.id", strOf(b, "id"), "cp-2"), eqNum("cp2.version", v, 2),
				eqStr("cp2.parent", strOf(b, "parent"), "cp-1")}
		}})

	p.call(callSpec{op: "GetCheckpoint", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "namespace": "", "id": "cp-1"}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("cp.id", strOf(b, "id"), "cp-1")}
		}})

	p.call(callSpec{op: "GetCheckpoint", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "namespace": "", "id": "cp-missing"}),
		expect: []int{404}, code: "NOT_FOUND",
		note: "a missing snapshot is NOT_FOUND, not a generic 500"})

	p.call(callSpec{op: "ListCheckpoints", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "namespace": "", "limit": 10}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			n, _ := lenOf(b, "checkpoints")
			return []assertion{check("checkpoints.at_least_two", n >= 2, fmt.Sprintf("n=%d", n))}
		}})

	// The namespace filter is an EXACT match, and "" is a namespace of its own,
	// not "all namespaces". The Space lane stores its snapshots under
	// "space/core", so a reader that omits the namespace gets an empty list and
	// concludes nothing was ever written - a measurement trap that was already
	// mistaken for a missing product feature. Pinned on the wire, with the
	// positive control in the same namespace.
	p.call(callSpec{op: "PutCheckpoint", auth: "task-token", token: claimToken,
		body: js(map[string]any{
			"fence": fenceJSON(mainRun, worker, epoch1),
			"checkpoint": map[string]any{
				"id": "cp-ns-1", "thread_id": mainThread, "namespace": spaceNS,
				"version": 1, "metadata": map[string]any{"source": "state-consumer"},
				"parts": []map[string]any{
					{"channel": "values", "version": "v9", "data": []byte("namespaced-" + suffix)},
				},
			},
			"key": "cpk-ns-" + suffix,
		}), expect: []int{200},
		note: "a snapshot written into a non-empty namespace, as the Space writer does",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("cpns.namespace", strOf(b, "namespace"), spaceNS),
				eqStr("cpns.id", strOf(b, "id"), "cp-ns-1")}
		}})
	p.call(callSpec{op: "ListCheckpoints", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "namespace": spaceNS, "limit": 10}),
		expect: []int{200},
		note:   "the exact namespace is the positive control",
		check: func(_ int, b []byte) []assertion {
			n, _ := lenOf(b, "checkpoints")
			return []assertion{check("checkpoints.in_space_ns", n == 1, fmt.Sprintf("n=%d", n)),
				eqStr("checkpoints.in_space_ns.first_id", strOf(b, "checkpoints", "0", "id"), "cp-ns-1")}
		}})
	p.call(callSpec{op: "ListCheckpoints", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "namespace": "", "limit": 10}),
		expect: []int{200},
		note:   "namespace \"\" is NOT \"all\": it still returns only the two unnamed snapshots",
		check: func(_ int, b []byte) []assertion {
			n, _ := lenOf(b, "checkpoints")
			return []assertion{check("checkpoints.empty_ns_is_not_all", n == 2, fmt.Sprintf("n=%d", n))}
		}})

	wantB64 := base64.StdEncoding.EncodeToString(partValue)
	p.call(callSpec{op: "GetPart", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "namespace": "", "channel": "values", "version": "v1"}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("part.bytes", strOf(b, "data"), wantB64)}
		}})

	p.call(callSpec{op: "GetPart", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "namespace": "", "channel": "meta", "version": "v0"}),
		expect: []int{200},
		note:   "an empty part round-trips as the empty byte string, not as null",
		check: func(_ int, b []byte) []assertion {
			raw, ok := rawField(b, "data")
			return []assertion{check("part.empty_not_null", ok && raw == `""`, "raw="+raw)}
		}})

	p.call(callSpec{op: "GetPart", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "namespace": "", "channel": "nope", "version": "v404"}),
		expect: []int{404}, code: "NOT_FOUND",
		note: "a missing part is NOT_FOUND, not a generic 500"})

	// --- pending writes ---------------------------------------------------
	p.call(callSpec{op: "PutWrites", auth: "task-token", token: claimToken,
		body: js(map[string]any{
			"fence": fenceJSON(mainRun, worker, epoch1), "namespace": "", "checkpoint_id": "cp-2",
			"writes": []map[string]any{
				{"task": "node-a", "index": 0, "channel": "out", "data": []byte("partial-a")},
				{"task": "node-b", "index": -1, "channel": "err", "data": nil},
			},
		}),
		expect: []int{200},
		note:   "negative index is legal; data null must materialize as the empty byte string"})

	p.call(callSpec{op: "GetWrites", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "namespace": "", "checkpoint_id": "cp-2"}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			n, ok := lenOf(b, "writes")
			rawNull, _ := rawField(b, "writes", "1", "data")
			return []assertion{
				check("writes.count", ok && n == 2, fmt.Sprintf("n=%d", n)),
				eqStr("writes.order_first_task", strOf(b, "writes", "0", "task"), "node-a"),
				eqStr("writes.first_channel", strOf(b, "writes", "0", "channel"), "out"),
				eqStr("writes.negative_index_kept", strOf(b, "writes", "1", "channel"), "err"),
				check("writes.nil_data_is_empty_string", rawNull == `""`,
					"data of the null-data write arrives as: "+rawNull),
			}
		}})

	// --- KV ---------------------------------------------------------------
	putKV := func(opKey string, value any, expectedVersion int) map[string]any {
		return map[string]any{
			"fence": fenceJSON(mainRun, worker, epoch1), "namespace": kvNS, "key": "k1",
			"operation_key": opKey, "expected_version": expectedVersion, "value": value,
		}
	}
	p.call(callSpec{op: "PutKV", auth: "task-token", token: claimToken,
		body: js(putKV("op-k1-"+suffix, map[string]any{"n": 1}, 0)), expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			v, _ := numOf(b, "version")
			return []assertion{eqNum("kv.version", v, 1),
				eqStr("kv.key", strOf(b, "key"), "k1")}
		}})

	p.call(callSpec{op: "GetKV", auth: "task-token", token: claimToken,
		body: js(map[string]any{"namespace": kvNS, "key": "k1"}), expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			v, _ := numOf(b, "version")
			return []assertion{eqNum("kv.version", v, 1), eqNum("kv.value_n", mustNum(digOf(b, "value", "n")), 1)}
		}})

	p.call(callSpec{op: "PutKV", auth: "task-token", token: claimToken,
		body: js(putKV("op-k1-"+suffix, map[string]any{"n": 1}, 0)), expect: []int{200},
		note: "byte-identical replay under the same operation_key must not bump the version",
		check: func(_ int, b []byte) []assertion {
			v, _ := numOf(b, "version")
			return []assertion{eqNum("kv.replay_version", v, 1)}
		}})

	p.call(callSpec{op: "PutKV", auth: "task-token", token: claimToken,
		body: js(putKV("op-k1-"+suffix, map[string]any{"n": 2}, 0)), expect: []int{400},
		code: "INVALID_ARGUMENT",
		note: "same operation_key, different digest: the 400 stays 400 and carries reason=KEY_REUSE_MISMATCH",
		check: func(_ int, b []byte) []assertion {
			return []assertion{
				eqStr("error.reason_is_key_reuse", strOf(b, "reason"), "KEY_REUSE_MISMATCH"),
				eqStr("error.code_stays_invalid_argument", strOf(b, "code"), "INVALID_ARGUMENT"),
			}
		}})

	// The receipt is addressed by (scope, operation_key) ALONE - the namespace
	// and the key are part of the request digest, not of the receipt identity.
	// The SDK snapshot carried (namespace, key) into the replay lookup, which
	// would have made this call succeed; on the pinned contract it is refused.
	p.call(callSpec{op: "PutKV", auth: "task-token", token: claimToken,
		body: js(map[string]any{
			"fence":            fenceJSON(mainRun, worker, epoch1),
			"namespace":        []string{"m2", "opkey-" + suffix},
			"key":              "k-other",
			"operation_key":    "op-k1-" + suffix,
			"expected_version": 0,
			"value":            map[string]any{"n": 1},
		}),
		expect: []int{400},
		code:   "INVALID_ARGUMENT",
		note:   "one operation_key across two different items is refused: the replay is scoped by (tenant, deployment, subject, operation_key), not by namespace/key",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("error.reason_is_key_reuse", strOf(b, "reason"), "KEY_REUSE_MISMATCH")}
		}})

	p.call(callSpec{op: "PutKV", auth: "task-token", token: claimToken,
		body:   js(putKV("op-cas-"+suffix, map[string]any{"n": 3}, 999)),
		expect: []int{409}, code: "CONFLICT",
		note: "wrong expected_version is a CAS conflict, not a 500"})

	p.call(callSpec{op: "SearchKV", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"namespace_prefix": []string{"store"}, "key_prefix": "k", "limit": 10}),
		expect: []int{200},
		note:   "SearchKV takes namespace_prefix as an array; there is no 'namespace' field here",
		check: func(_ int, b []byte) []assertion {
			n, _ := lenOf(b, "items")
			return []assertion{check("search.hits", n == 1, fmt.Sprintf("n=%d", n))}
		}})

	p.call(callSpec{op: "DeleteKV", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1), "namespace": kvNS,
			"key": "k1", "operation_key": "op-del-" + suffix, "expected_version": 1}),
		expect: []int{200}})

	p.call(callSpec{op: "GetKV", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"namespace": kvNS, "key": "k1"}),
		expect: []int{404}, code: "NOT_FOUND",
		note: "a deleted key reads as NOT_FOUND"})

	// --- intents ------------------------------------------------------------
	// The journal lifecycle is recorded -> dispatched -> terminal. Only the
	// privileged ReconcileIntent may resolve an intent that was marked unknown,
	// and only with the exact original fingerprint.
	b = p.call(callSpec{op: "RecordIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n1", "call": "c1", "kind": "tool", "target": "tool:echo",
			"args": map[string]any{"text": "hello"}}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.status", strOf(b, "status"), "recorded"),
				check("intent.digest_present", len(strOf(b, "digest")) > 10, "digest issued")}
		}})
	digest1 = strOf(b, "digest")

	p.call(callSpec{op: "RecordIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n1", "call": "c1", "kind": "tool", "target": "tool:echo",
			"args": map[string]any{"text": "hello"}}),
		expect: []int{200},
		note:   "identical replay returns the same intent, not a second journal row",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.replay_status", strOf(b, "status"), "recorded")}
		}})

	p.call(callSpec{op: "RecordIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n1", "call": "c1", "kind": "tool", "target": "tool:other",
			"args": map[string]any{"text": "different"}}),
		expect: []int{400},
		note:   "same (run,node,call) with a different fingerprint is a key reuse violation"})

	p.call(callSpec{op: "GetIntent", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"run_id": mainRun, "node": "n1", "call": "c1"}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.status", strOf(b, "status"), "recorded")}
		}})

	p.call(callSpec{op: "RecordIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n1", "call": "c1", "kind": "tool", "target": "tool:echo",
			"args": map[string]any{"text": "hello"}}),
		expect: []int{200},
		note:   "the journal replays the stored status, never a fabricated 'recorded'",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.replay_reports_stored_status", strOf(b, "status"), "recorded")}
		}})

	p.call(callSpec{op: "UpdateIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n1", "call": "c1", "status": "succeeded",
			"result": map[string]any{"ok": true}}),
		expect: []int{409}, code: "CONFLICT",
		note: "recorded jumps straight to succeeded: an illegal transition"})

	p.call(callSpec{op: "UpdateIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n1", "call": "c1", "status": "dispatched"}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.status", strOf(b, "status"), "dispatched")}
		}})

	resultOK := map[string]any{"ok": true, "exit_code": 0}
	p.call(callSpec{op: "UpdateIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n1", "call": "c1", "status": "succeeded", "result": resultOK}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.status", strOf(b, "status"), "succeeded")}
		}})

	p.call(callSpec{op: "UpdateIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n1", "call": "c1", "status": "succeeded", "result": resultOK}),
		expect: []int{200},
		note:   "a completion replays when the result is byte-identical",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.replay_status", strOf(b, "status"), "succeeded")}
		}})

	p.call(callSpec{op: "UpdateIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n1", "call": "c1", "status": "succeeded",
			"result": map[string]any{"ok": true, "exit_code": 1}}),
		expect: []int{409}, code: "CONFLICT",
		note: "the same completion with a different result is refused"})

	b = p.call(callSpec{op: "RecordIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n2", "call": "c2", "kind": "tool", "target": "tool:sum"}),
		expect: []int{200},
		note:   "args is optional in the contract: an intent without args is recorded",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.status", strOf(b, "status"), "recorded"),
				check("intent.digest_present", len(strOf(b, "digest")) > 10, "digest issued")}
		}})
	digest2 := strOf(b, "digest")

	p.call(callSpec{op: "UpdateIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n2", "call": "c2", "status": "dispatched"}),
		expect: []int{200}})

	p.call(callSpec{op: "UpdateIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n2", "call": "c2", "status": "unknown"}),
		expect: []int{200},
		note:   "a worker that lost the outcome marks the intent unknown",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.status", strOf(b, "status"), "unknown")}
		}})

	p.call(callSpec{op: "UpdateIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n2", "call": "c2", "status": "succeeded",
			"result": map[string]any{"ok": true}}),
		expect: []int{409}, code: "CONFLICT",
		note: "an unknown intent is not completable through the ordinary path"})

	p.call(callSpec{op: "ReconcileIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n2", "call": "c2", "status": "succeeded", "digest": "sha256:deadbeef",
			"result": map[string]any{"ok": true}}),
		expect: []int{400}, code: "INVALID_ARGUMENT",
		note: "reconciliation without the exact fingerprint is refused: State reports KEY_REUSE_MISMATCH " +
			"and the 400 carries it as reason",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("error.reason_is_key_reuse", strOf(b, "reason"), "KEY_REUSE_MISMATCH")}
		}})

	p.call(callSpec{op: "ReconcileIntent", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"node": "n2", "call": "c2", "status": "succeeded", "digest": digest2,
			"result": map[string]any{"ok": true, "via": "reconcile"}}),
		expect: []int{200},
		note:   "only the privileged reconciliation resolves an unknown intent",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.status", strOf(b, "status"), "succeeded")}
		}})

	p.call(callSpec{op: "GetIntent", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"run_id": mainRun, "node": "n2", "call": "c2"}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("intent.status", strOf(b, "status"), "succeeded")}
		}})

	p.call(callSpec{op: "GetIntent", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"run_id": mainRun, "node": "n9", "call": "c9"}),
		expect: []int{404}, code: "NOT_FOUND",
		note: "a missing intent is NOT_FOUND, not a generic 500"})

	// --- inbox ------------------------------------------------------------
	b = p.call(callSpec{op: "Enqueue", auth: "task-token", token: claimToken,
		body: js(map[string]any{"thread_id": mainThread, "key": "ev-1-" + suffix,
			"kind": "resume", "payload": map[string]any{"approved": true}}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			seq, _ := numOf(b, "sequence")
			return []assertion{present(b, "event.id_present", "id"), eqNum("event.sequence", seq, 1)}
		}})
	inboxID = strOf(b, "id")

	p.call(callSpec{op: "Enqueue", auth: "task-token", token: claimToken,
		body: js(map[string]any{"thread_id": mainThread, "key": "ev-1-" + suffix,
			"kind": "resume", "payload": map[string]any{"approved": true}}),
		expect: []int{200},
		note:   "identical replay under the same event key returns the original event",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("event.replay_id", strOf(b, "id"), inboxID)}
		}})

	p.call(callSpec{op: "ReadInbox", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "after": 0, "limit": 10}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			n, _ := lenOf(b, "events")
			return []assertion{check("inbox.one_event", n == 1, fmt.Sprintf("n=%d", n))}
		}})

	p.call(callSpec{op: "AckInbox", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1), "ids": []string{inboxID}}),
		expect: []int{200}})

	p.call(callSpec{op: "AckInbox", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1), "ids": []string{"evt-missing"}}),
		expect: []int{404}, code: "NOT_FOUND",
		note: "an unknown event id is NOT_FOUND, not a generic 500"})

	// --- stream -----------------------------------------------------------
	p.call(callSpec{op: "AppendStream", auth: "task-token", token: claimToken,
		body: js(map[string]any{"fence": fenceJSON(mainRun, worker, epoch1),
			"key": "s-1-" + suffix, "kind": "token", "payload": map[string]any{"text": "hel"}}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			seq, _ := numOf(b, "sequence")
			return []assertion{eqNum("stream.sequence", seq, 1)}
		}})

	p.call(callSpec{op: "ReadStream", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "after": 0, "limit": 10}),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			n, _ := lenOf(b, "events")
			return []assertion{check("stream.one_event", n == 1, fmt.Sprintf("n=%d", n))}
		}})

	// --- batch ------------------------------------------------------------
	p.call(callSpec{op: "PutBatch", auth: "task-token", token: claimToken,
		body: js(map[string]any{
			"fence": fenceJSON(mainRun, worker, epoch1),
			"batch": map[string]any{
				"key": "b-1-" + suffix,
				"checkpoint": map[string]any{
					"id": "cp-3", "thread_id": mainThread, "namespace": "", "version": 3,
					"parent":   "cp-2",
					"metadata": map[string]any{"step": 3},
					"parts": []map[string]any{
						{"channel": "values", "version": "v3", "data": []byte("batched")},
					},
				},
				"writes": []map[string]any{
					{"task": "node-c", "index": 0, "channel": "out", "data": []byte("partial-c")},
				},
				"writes_namespace": "",
				"stream": map[string]any{"key": "s-2-" + suffix, "kind": "token",
					"payload": map[string]any{"text": "lo"}},
			},
		}),
		expect: []int{200},
		note:   "one transaction: checkpoint + writes + stream",
		check: func(_ int, b []byte) []assertion {
			return []assertion{present(b, "batch.checkpoint", "checkpoint"),
				eqBool("batch.ok", boolOf(b, "ok"), true),
				present(b, "batch.stream_event", "stream_event")}
		}})

	// --- export -----------------------------------------------------------
	exportBody = p.call(callSpec{op: "ExportThread", auth: "task-token", token: claimToken,
		body: js(map[string]any{"thread_id": mainThread}), expect: []int{200},
		note: "export inlines part bytes so the snapshot is portable",
		check: func(_ int, b []byte) []assertion {
			n, _ := lenOf(b, "checkpoints")
			r, _ := lenOf(b, "runs")
			return []assertion{eqStr("export.thread", strOf(b, "thread", "id"), mainThread),
				check("export.checkpoints", n >= 3, fmt.Sprintf("n=%d", n)),
				check("export.runs", r == 1, fmt.Sprintf("n=%d", r))}
		}})

	// --- scope isolation on the wire --------------------------------------
	p.call(callSpec{op: "GetThread", auth: "foreign-token",
		body:   js(map[string]any{"thread_id": mainThread}),
		expect: []int{404}, code: "NOT_FOUND",
		note: "a foreign tenant with a valid certificate and token cannot see this scope's thread"})

	p.call(callSpec{op: "GetThread", auth: "foreign-cert+own-token",
		body:   js(map[string]any{"thread_id": mainThread}),
		expect: []int{403}, code: "FORBIDDEN",
		note: "this scope's token on a foreign certificate is refused"})

	// --- request-shape strictness -----------------------------------------
	p.call(callSpec{op: "ListThreads", auth: "task-token", method: http.MethodGet,
		body: nil, expect: []int{404}, code: "NOT_FOUND",
		note: "GET is not a route: NOT_FOUND, never 405"})

	p.call(callSpec{op: "NoSuchOperation", auth: "task-token",
		body: js(map[string]any{}), expect: []int{404}, code: "NOT_FOUND",
		note: "unknown operation is NOT_FOUND"})

	p.call(callSpec{op: "ListThreads", auth: "none",
		body: js(map[string]any{"limit": 1}), expect: []int{403}, code: "FORBIDDEN",
		note: "task routes demand a bearer token"})

	p.call(callSpec{op: "ListThreads", auth: "task-token",
		body: []byte(`{"limit":1,"tenant":"someone-else"}`), expect: []int{400}, code: "INVALID_ARGUMENT",
		note: "scope in the body is rejected as an unknown field"})

	p.call(callSpec{op: "ListThreads", auth: "task-token",
		body: []byte(``), expect: []int{400}, code: "INVALID_ARGUMENT",
		note: "an empty body is invalid"})

	p.call(callSpec{op: "ListThreads", auth: "task-token",
		body: []byte(`{"limit":1,"limit":2}`), expect: []int{400}, code: "INVALID_ARGUMENT",
		note: "duplicate JSON keys are rejected",
		check: func(_ int, b []byte) []assertion {
			_, hasReason := dig(b, "reason")
			return []assertion{check("error.malformed_body_has_no_reason", !hasReason,
				"reason must be absent for a malformed body, so only a domain code sets it")}
		}})

	p.call(callSpec{op: "ListThreads", auth: "no-client-cert",
		body: js(map[string]any{"limit": 1}), expect: []int{0},
		note: "the TLS handshake itself fails without a workload certificate"})

	p.call(callSpec{op: "ClaimRun", auth: "no-client-cert",
		body: js(map[string]any{"worker": worker, "ttl_ms": 1000}), expect: []int{0},
		note: "ClaimRun cannot be reached without the workload certificate"})

	big := make([]byte, (1<<20)+64)
	for i := range big {
		big[i] = 'a'
	}
	p.call(callSpec{op: "ListThreads", auth: "task-token",
		body:   append([]byte(`{"cursor":"`), append(big, []byte(`"}`)...)...),
		expect: []int{400}, code: "INVALID_ARGUMENT",
		note: "a body above the 1 MiB cap is rejected"})

	// --- suspend, requeue, re-claim, release ------------------------------
	p.call(callSpec{op: "Suspend", auth: "task-token", token: claimToken,
		body:   js(flatFence(map[string]any{"wait_for": "resume"}, mainRun, worker, epoch1)),
		expect: []int{200}})

	p.call(callSpec{op: "Heartbeat", auth: "task-token", token: claimToken,
		body:   js(flatFence(map[string]any{"ttl_ms": 60000}, mainRun, worker, epoch1)),
		expect: []int{409}, code: "STALE_EPOCH",
		note: "Suspend released the lease: the old fence is dead"})

	p.call(callSpec{op: "PutCheckpoint", auth: "task-token", token: claimToken,
		body:   js(cp1("cp-post-suspend", "", 4, nil, "cpk-4-"+suffix)),
		expect: []int{409}, code: "STALE_EPOCH",
		note: "writing a snapshot on a released lease is fenced off"})

	p.call(callSpec{op: "Enqueue", auth: "task-token", token: claimToken,
		body: js(map[string]any{"thread_id": mainThread, "key": "ev-2-" + suffix,
			"kind": "resume", "payload": map[string]any{"approved": true}}),
		expect: []int{200},
		note:   "an inbox event of the waited kind requeues the waiting run"})

	p.call(callSpec{op: "GetRun", auth: "task-token", token: claimToken,
		body: js(map[string]any{"run_id": mainRun}), expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("run.status_requeued", strOf(b, "status"), "pending")}
		}})

	b = p.call(callSpec{op: "ClaimRun", auth: "workload-cert",
		body: js(map[string]any{"worker": worker, "ttl_ms": 240000}), expect: []int{200},
		note: "the requeued run is claimable again with a strictly newer epoch",
		check: func(_ int, b []byte) []assertion {
			e, ok := numOf(b, "lease", "epoch")
			return []assertion{eqStr("lease.run_id", strOf(b, "lease", "run", "id"), mainRun),
				check("lease.epoch_advanced", ok && e == epoch1+1, fmt.Sprintf("epoch=%v want=%v", e, epoch1+1))}
		}})
	epoch2, _ = numOf(b, "lease", "epoch")
	token2 = strOf(b, "task_token")
	if token2 == "" {
		token2 = p.token
	}

	p.call(callSpec{op: "Heartbeat", auth: "task-token", token: token2,
		body:   js(flatFence(map[string]any{"ttl_ms": 60000}, mainRun, worker, epoch1)),
		expect: []int{409}, code: "STALE_EPOCH",
		note: "the previous epoch stays dead even with a fresh token"})

	p.call(callSpec{op: "Heartbeat", auth: "task-token", token: token2,
		body:   js(flatFence(map[string]any{"ttl_ms": 60000}, mainThread, worker, epoch2)),
		expect: []int{404}, code: "NOT_FOUND",
		note: "a run id that is not the leased run is NOT_FOUND"})

	p.call(callSpec{op: "Heartbeat", auth: "task-token", token: token2,
		body:   js(flatFence(map[string]any{"ttl_ms": 240000}, mainRun, worker, epoch2)),
		expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{present(b, "heartbeat.expires_at", "expires_at")}
		}})

	p.call(callSpec{op: "Release", auth: "task-token", token: token2,
		body:   js(flatFence(map[string]any{"status": "running"}, mainRun, worker, epoch2)),
		expect: []int{400}, code: "INVALID_ARGUMENT",
		note: "Release accepts only pending/done/failed/canceled"})

	p.call(callSpec{op: "Release", auth: "task-token", token: token2,
		body:   js(flatFence(map[string]any{"status": "done"}, mainRun, worker, epoch2)),
		expect: []int{200}})

	p.call(callSpec{op: "GetRun", auth: "task-token", token: token2,
		body: js(map[string]any{"run_id": mainRun}), expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("run.status_done", strOf(b, "status"), "done")}
		}})

	p.call(callSpec{op: "Release", auth: "task-token", token: token2,
		body:   js(flatFence(map[string]any{"status": "done"}, mainRun, worker, epoch2)),
		expect: []int{409}, code: "STALE_EPOCH",
		note: "after Release the fence is gone"})

	p.call(callSpec{op: "ClaimRun", auth: "workload-cert",
		body: js(map[string]any{"worker": worker, "ttl_ms": 60000}), expect: []int{200},
		note: "with the run finished there is nothing to claim: lease is null",
		check: func(_ int, b []byte) []assertion {
			lease, ok := dig(b, "lease")
			return []assertion{check("claim.lease_null", ok && lease == nil, "lease is null when idle")}
		}})

	// --- control plane ----------------------------------------------------
	b = p.call(callSpec{op: "CreateThread", auth: "task-token",
		body:   js(map[string]any{"context_id": ctxHold, "key": "thread-hold-" + suffix}),
		expect: []int{200}})
	heldThread := strOf(b, "id")

	p.call(callSpec{op: "SetLegalhold", auth: "task-token",
		body: js(map[string]any{"thread_id": heldThread, "reason": "e2e-legalhold"}), expect: []int{200}})

	p.call(callSpec{op: "DeleteThread", auth: "task-token",
		body:   js(map[string]any{"thread_id": heldThread}),
		expect: []int{403}, code: "FORBIDDEN",
		note: "a thread under legalhold cannot be deleted"})

	p.call(callSpec{op: "RemoveLegalhold", auth: "task-token",
		body: js(map[string]any{"thread_id": heldThread}), expect: []int{200}})

	p.call(callSpec{op: "DeleteThread", auth: "task-token",
		body: js(map[string]any{"thread_id": heldThread}), expect: []int{200}})

	p.call(callSpec{op: "GetThread", auth: "task-token",
		body:   js(map[string]any{"thread_id": heldThread}),
		expect: []int{404}, code: "NOT_FOUND",
		note: "the deleted thread is gone"})

	// Importing the snapshot back into the scope that still owns the exported
	// context must be refused: silently merging it into the live thread would
	// mix two histories under one id.
	p.call(callSpec{op: "ImportThread", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"export": json.RawMessage(exportBody)}),
		expect: []int{409}, code: "CONFLICT",
		note: "the target scope already owns this context_id",
		check: func(_ int, b []byte) []assertion {
			return []assertion{eqStr("import.refusal_reason", strOf(b, "reason"), "IMPORT_CONTEXT_TAKEN")}
		}})

	// The restore is a copy under a NEW address, so the new context_id is named
	// in the request: rewriting thread.context_id inside the export bytes would
	// mean the bytes decide the identity they get, which is exactly the defect
	// the import scope check exists to close.
	b = p.call(callSpec{op: "ImportThread", auth: "task-token", token: claimToken,
		body: js(map[string]any{"export": json.RawMessage(exportBody),
			"context_id": "ctx-imported-" + suffix}),
		expect: []int{200},
		note:   "the exported snapshot restores as a fresh thread under an explicitly retargeted context_id",
		check: func(_ int, b []byte) []assertion {
			return []assertion{present(b, "import.id_present", "id"),
				check("import.id_fresh", strOf(b, "id") != mainThread,
					"ImportThread must allocate a fresh thread id: got="+strOf(b, "id")),
				eqStr("import.context_id_retargeted", strOf(b, "context_id"), "ctx-imported-"+suffix)}
		}})
	imported = strOf(b, "id")
	// Fidelity of the restore, measured as equal bytes rather than equal rows:
	// the stream and the checkpoint parts of the source are re-read and digested
	// against the imported thread. Addresses differ on purpose, content must not.
	// checkpoint_blobs are 0 in this scenario - the channels live in
	// checkpoint_parts - so the parts are what carries the durable bytes.
	srcStream := p.call(callSpec{op: "ReadStream", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "after": 0, "limit": 100}),
		expect: []int{200}, note: "source stream, re-read to digest the restore against it"})
	srcCPs := p.call(callSpec{op: "ListCheckpoints", auth: "task-token", token: claimToken,
		body:   js(map[string]any{"thread_id": mainThread, "namespace": "", "limit": 10}),
		expect: []int{200}, note: "source checkpoints, to digest their parts against the restore"})
	if imported != "" {
		p.call(callSpec{op: "ReadStream", auth: "task-token", token: claimToken,
			body:   js(map[string]any{"thread_id": imported, "after": 0, "limit": 100}),
			expect: []int{200},
			note:   "the restored stream carries the same bytes in the same order",
			check: func(_ int, b []byte) []assertion {
				srcCount, _ := lenOf(srcStream, "events")
				dstCount, _ := lenOf(b, "events")
				return []assertion{
					check("restore.stream_event_count", srcCount == dstCount && dstCount > 0,
						fmt.Sprintf("source=%d imported=%d", srcCount, dstCount)),
					eqStr("restore.stream_digest_equal",
						contentDigest(b, "events", "", "key", "kind", "payload"),
						contentDigest(srcStream, "events", "", "key", "kind", "payload")),
				}
			}})
		p.call(callSpec{op: "GetThread", auth: "task-token", token: claimToken,
			body: js(map[string]any{"thread_id": imported}), expect: []int{200},
			check: func(_ int, b []byte) []assertion {
				return []assertion{eqStr("import.thread_readable", strOf(b, "id"), imported)}
			}})
		p.call(callSpec{op: "ListCheckpoints", auth: "task-token", token: claimToken,
			body:   js(map[string]any{"thread_id": imported, "namespace": "", "limit": 10}),
			expect: []int{200},
			note:   "the imported thread carries the exported snapshots",
			check: func(_ int, b []byte) []assertion {
				n, _ := lenOf(b, "checkpoints")
				return []assertion{
					check("import.checkpoints_moved", n >= 3, fmt.Sprintf("n=%d", n)),
					eqStr("restore.checkpoint_parts_digest_equal",
						contentDigest(b, "checkpoints", "parts", "channel", "version", "data"),
						contentDigest(srcCPs, "checkpoints", "parts", "channel", "version", "data")),
				}
			}})
		p.call(callSpec{op: "GetWrites", auth: "task-token", token: claimToken,
			body:   js(map[string]any{"thread_id": imported, "namespace": "", "checkpoint_id": "cp-2"}),
			expect: []int{200},
			note:   "pending writes travel with the snapshot",
			check: func(_ int, b []byte) []assertion {
				n, _ := lenOf(b, "writes")
				return []assertion{check("import.writes_moved", n == 2, fmt.Sprintf("n=%d", n))}
			}})
		p.call(callSpec{op: "DeleteThread", auth: "task-token", token: claimToken,
			body: js(map[string]any{"thread_id": imported}), expect: []int{200},
			note: "the imported copy is removed again so the scenario leaves no pending run"})
	}

	p.call(callSpec{op: "DeleteThread", auth: "task-token", token: claimToken,
		body: js(map[string]any{"thread_id": secondThread}), expect: []int{200},
		note: "the second thread of this scenario is removed before DeleteSubject"})

	p.call(callSpec{op: "DeleteSubject", auth: "task-token",
		body: js(map[string]any{}), expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			d, ok := numOf(b, "deleted")
			return []assertion{check("delete_subject.deleted_remaining", ok && d >= 1, fmt.Sprintf("deleted=%v", d))}
		}})

	p.call(callSpec{op: "GetThread", auth: "task-token",
		body:   js(map[string]any{"thread_id": mainThread}),
		expect: []int{404}, code: "NOT_FOUND",
		note: "DeleteSubject removed this subject's threads"})

	p.call(callSpec{op: "ListThreads", auth: "task-token",
		body: js(map[string]any{"limit": 10}), expect: []int{200},
		check: func(_ int, b []byte) []assertion {
			n, _ := lenOf(b, "threads")
			return []assertion{check("threads.empty_after_delete", n == 0, fmt.Sprintf("n=%d", n))}
		}})

	return map[string]any{
		"thread_id":       mainThread,
		"run_id":          mainRun,
		"epoch_first":     epoch1,
		"epoch_second":    epoch2,
		"intent_digest":   digest1,
		"imported_thread": imported,
	}
}

// digOf digs a value and returns it as any for ad-hoc numeric comparisons.
func digOf(body []byte, path ...string) any {
	v, _ := dig(body, path...)
	return v
}

// mustNum coerces a decoded JSON number to float64, returning 0 for anything
// else so an assertion can report the mismatch.
func mustNum(v any) float64 {
	if n, ok := v.(float64); ok {
		return n
	}
	return 0
}
