// Command sdk-live-scenario runs the Go leg of the joint public-SDK live
// scenario defined by the shared contract (scenario.json, schema
// ahp-sdk-live-scenario/v1, authored by Boyle). The contract is the ONLY source
// of expected codes: this program never decides for itself what a step returns.
//
// Standalone as root asked for: a customer-shaped process built from the public
// clients (sdk/go/tools, sdk/go/artifacts) plus plain HTTP for the human-edge
// routes; one pass; no test harness; no internal helpers.
//
// Contract rules implemented verbatim: an unset prereq_env variable is
// SKIP_PREREQ (never PASS); expect_by_env takes the first matching key with
// 'default' last; expect code is a substring match; fail_expected means the step
// passes when the platform refuses; labels and notes are copied verbatim. A step
// with no handler here is SKIP_NOT_IMPLEMENTED and is never counted as proved.
//
// Credentials come from the environment only. The verdict carries ids, statuses,
// platform codes and digests — never a token, password or DSN.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/sdk/go/artifacts"
	"github.com/ahp-platform/ahp/sdk/go/tools"
)

const (
	statusPass       = "PASS"
	statusFail       = "FAIL"
	statusSkipPrereq = "SKIP_PREREQ"
	statusSkipUnimpl = "SKIP_NOT_IMPLEMENTED"
)

// prereqError marks a variable THIS leg needs and the contract does not name.
// It becomes SKIP_PREREQ with the variable listed, never a silent FAIL.
type prereqError struct{ name string }

func (e *prereqError) Error() string { return "driver prereq missing: " + e.name }

type step struct {
	ID        string                    `json:"id"`
	Plane     string                    `json:"plane"`
	Title     string                    `json:"title"`
	Note      string                    `json:"note"`
	Labels    []string                  `json:"labels"`
	PrereqEnv []string                  `json:"prereq_env"`
	ExpectBy  map[string]expectationSet `json:"expect_by_env"`
}

// expectationSet normalises the three shapes the contract uses for "http":
// a list of {http:[...],code}, a bare list of statuses, or one such object.
type expectationSet struct {
	Statuses     []int
	Code         string
	FailExpected bool
}

func (e *expectationSet) UnmarshalJSON(raw []byte) error {
	var outer struct {
		HTTP         json.RawMessage `json:"http"`
		Code         string          `json:"code"`
		FailExpected *bool           `json:"fail_expected"`
	}
	if err := json.Unmarshal(raw, &outer); err != nil {
		return err
	}
	e.Code = outer.Code
	e.FailExpected = outer.FailExpected != nil && *outer.FailExpected
	if len(outer.HTTP) == 0 {
		return nil
	}
	var statuses []int
	if json.Unmarshal(outer.HTTP, &statuses) == nil {
		e.Statuses = statuses
		return nil
	}
	var items []struct {
		HTTP []int  `json:"http"`
		Code string `json:"code"`
	}
	if json.Unmarshal(outer.HTTP, &items) == nil {
		for _, item := range items {
			e.Statuses = append(e.Statuses, item.HTTP...)
			if item.Code != "" {
				e.Code = item.Code
			}
		}
		return nil
	}
	var one struct {
		HTTP []int  `json:"http"`
		Code string `json:"code"`
	}
	if err := json.Unmarshal(outer.HTTP, &one); err != nil {
		return err
	}
	e.Statuses = append(e.Statuses, one.HTTP...)
	if one.Code != "" {
		e.Code = one.Code
	}
	return nil
}

type contract struct {
	Schema string `json:"schema"`
	Title  string `json:"title"`
	Author string `json:"author"`
	Steps  []step `json:"steps"`
}

type record struct {
	ID            string   `json:"id"`
	Plane         string   `json:"plane"`
	Status        string   `json:"status"`
	Labels        []string `json:"labels,omitempty"`
	Note          string   `json:"note,omitempty"`
	Reason        string   `json:"reason,omitempty"`
	ExpectedHTTP  []int    `json:"expected_http,omitempty"`
	ExpectedCode  string   `json:"expected_code,omitempty"`
	FailExpected  bool     `json:"fail_expected,omitempty"`
	ActualHTTP    int      `json:"actual_http,omitempty"`
	ActualCode    string   `json:"actual_code,omitempty"`
	Actual        string   `json:"actual,omitempty"`
	Contracts     string   `json:"contracts,omitempty"`
	PrereqMissing []string `json:"prereq_missing,omitempty"`
	EffectProved  bool     `json:"effect_proved,omitempty"`
}

type outcome struct {
	status       int
	code         string
	detail       string
	effectProved bool
}

type blobState struct {
	id, version, digest string
	size                int64
	payload             []byte
}

type toolState struct{ key, target, digest string }

type leg struct {
	base, human, token string
	stamp, stampLabel  string
	http               *http.Client
	contract           contract
	recs, proposed     []record
	v                  map[string]any
	blob               blobState
	tool               toolState
}

func main() {
	l := &leg{v: map[string]any{"language": "go"}}
	os.Exit(l.finish(l.main()))
}

func (l *leg) env(name string) string { return strings.TrimSpace(os.Getenv(name)) }

func (l *leg) orEnv(name, fallback string) string {
	if v := l.env(name); v != "" {
		return v
	}
	return fallback
}

func (l *leg) bail(id, reason string) {
	l.recs = append(l.recs, record{ID: id, Plane: "driver", Status: statusFail, Reason: reason})
}

func (l *leg) main() int {
	for _, name := range []string{"AHP_API_URL", "AHP_CA_FILE", "AHP_SDK_STAMP", "AHP_SCENARIO_CONTRACT"} {
		if l.env(name) == "" {
			l.bail("P0", "required configuration missing: "+name)
			return 2
		}
	}
	// The caller's label is not the run identity. Two legs of one pass that share a
	// label would also share the tools idempotency key ("<stamp>-tools-1"), and the
	// second leg would replay the first instead of exercising the platform twice.
	l.stampLabel = l.env("AHP_SDK_STAMP")
	l.stamp = fmt.Sprintf("%s-go-%s-%04x", l.stampLabel, time.Now().UTC().Format("150405"),
		uint16(time.Now().UnixNano()))
	l.base = strings.TrimRight(l.env("AHP_API_URL"), "/")
	if parsed, err := url.Parse(l.base); err != nil {
		l.bail("P0", "AHP_API_URL is not an origin")
		return 2
	} else {
		l.v["base_host"] = parsed.Host
	}
	raw, err := os.ReadFile(l.env("AHP_SCENARIO_CONTRACT"))
	if err != nil {
		l.bail("P0", "contract unreadable")
		return 2
	}
	if err := json.Unmarshal(raw, &l.contract); err != nil || len(l.contract.Steps) == 0 {
		l.bail("P0", "contract is not a readable ahp-sdk-live-scenario/v1 document")
		return 2
	}
	sum := sha256.Sum256(raw)
	l.v["contract"] = map[string]any{"path": l.env("AHP_SCENARIO_CONTRACT"), "schema": l.contract.Schema,
		"sha256": hex.EncodeToString(sum[:]), "steps_total": len(l.contract.Steps)}
	if err := l.transport(); err != nil {
		l.bail("P0", err.Error())
		return 2
	}
	for _, s := range l.contract.Steps {
		l.run(s)
	}
	l.proposedHumanRead()
	return 0
}

func (l *leg) transport() error {
	ca, err := os.ReadFile(l.env("AHP_CA_FILE"))
	if err != nil {
		return errors.New("trust bundle unreadable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("trust bundle holds no certificate")
	}
	cfg := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	sent := false
	if c, k := l.env("AHP_CERT_FILE"), l.env("AHP_KEY_FILE"); c != "" && k != "" {
		pair, err := tls.LoadX509KeyPair(c, k)
		if err != nil {
			return errors.New("client certificate pair unusable")
		}
		cfg.Certificates = []tls.Certificate{pair}
		sent = true
	}
	l.v["client_cert_sent"] = sent
	l.http = &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}, Timeout: 90 * time.Second}
	return nil
}

func (l *leg) expectation(s step) expectationSet {
	keys := make([]string, 0, len(s.ExpectBy))
	for k := range s.ExpectBy {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		if keys[i] == "default" {
			return false
		}
		if keys[j] == "default" {
			return true
		}
		return keys[i] < keys[j]
	})
	for _, key := range keys {
		if key == "default" || strings.Count(key, "=") == 1 {
			if parts := strings.SplitN(key, "=", 2); len(parts) == 2 && l.env(parts[0]) != parts[1] {
				continue
			}
		} else {
			continue
		}
		return s.ExpectBy[key]
	}
	return expectationSet{}
}

func (l *leg) run(s step) {
	want := l.expectation(s)
	rec := record{ID: s.ID, Plane: s.Plane, Labels: s.Labels, Note: s.Note,
		ExpectedHTTP: want.Statuses, ExpectedCode: want.Code, FailExpected: want.FailExpected}
	for _, name := range s.PrereqEnv {
		if l.env(name) == "" {
			rec.Status = statusSkipPrereq
			rec.PrereqMissing = append(rec.PrereqMissing, name)
			l.recs = append(l.recs, rec)
			return
		}
	}
	handler, ok := l.handlers()[s.ID]
	if !ok {
		rec.Status = statusSkipUnimpl
		rec.Reason = "no Go handler in this leg yet (plane " + s.Plane + ")"
		l.recs = append(l.recs, rec)
		return
	}
	got, err := handler(s)
	rec.ActualHTTP, rec.ActualCode, rec.Actual, rec.EffectProved = got.status, got.code, got.detail, got.effectProved
	if err != nil {
		var pre *prereqError
		if errors.As(err, &pre) {
			rec.Status = statusSkipPrereq
			rec.PrereqMissing = append(rec.PrereqMissing, pre.name)
			rec.Reason = "driver prereq the contract does not name: " + pre.name
		} else {
			rec.Status = statusFail
			rec.Reason = err.Error()
		}
		l.recs = append(l.recs, rec)
		return
	}
	if verdict := judge(want, got); verdict != "" {
		rec.Status = statusFail
		rec.Reason = verdict
	} else {
		rec.Status = statusPass
	}
	l.recs = append(l.recs, rec)
}

// judge returns "" when the outcome satisfies the contract expectation.
func judge(want expectationSet, got outcome) string {
	if len(want.Statuses) > 0 {
		matched := false
		for _, status := range want.Statuses {
			if status == got.status {
				matched = true
			}
		}
		if !matched {
			return fmt.Sprintf("HTTP %d not in expected %v", got.status, want.Statuses)
		}
	}
	if want.FailExpected && got.status < 400 {
		return fmt.Sprintf("the contract expects this step to be refused, but HTTP %d answered", got.status)
	}
	if want.Code != "" && !strings.Contains(strings.ToLower(got.code), strings.ToLower(want.Code)) {
		return fmt.Sprintf("code %q does not contain expected %q", got.code, want.Code)
	}
	return ""
}

func (l *leg) handlers() map[string]func(step) (outcome, error) {
	return map[string]func(step) (outcome, error){
		"P1": l.p1, "P2": l.p2,
		"A1": l.a1, "A2": l.a2, "A3": l.a3, "A4": l.a4, "A5": l.a5,
		"T7": l.t7, "T9": l.t9,
	}
}

func (l *leg) p1(step) (outcome, error) {
	certPath, keyPath := l.env("AHP_CERT_FILE"), l.env("AHP_KEY_FILE")
	if certPath == "" || keyPath == "" {
		return outcome{}, &prereqError{"AHP_CERT_FILE/AHP_KEY_FILE"}
	}
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return outcome{}, fmt.Errorf("client certificate pair unusable")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return outcome{}, fmt.Errorf("client certificate unparsable")
	}
	subject := leaf.Subject
	detail := "subject CN=" + subject.CommonName
	if len(subject.OrganizationalUnit) > 0 {
		detail += " ou=" + strings.Join(subject.OrganizationalUnit, ",")
	}
	l.v["client_subject"] = subject.CommonName
	return outcome{status: http.StatusOK, detail: detail}, nil
}

// p2 walks the sanctioned chain: browser login -> scoped task token at
// /v1/identity/control/exchange, which is the face a workload may use.
func (l *leg) p2(step) (outcome, error) {
	if path := l.env("AHP_TASK_TOKEN_FILE"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return outcome{}, fmt.Errorf("task token unreadable")
		}
		l.token = strings.TrimSpace(string(raw))
		return outcome{status: http.StatusOK, detail: "task token handed in"}, nil
	}
	if path := l.env("AHP_HUMAN_TOKEN_FILE"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return outcome{}, fmt.Errorf("human token unreadable")
		}
		l.human = strings.TrimSpace(string(raw))
	} else {
		user, pass := l.env("AHP_HUMAN_USER"), l.env("AHP_HUMAN_PASSWORD")
		if user == "" || pass == "" {
			return outcome{}, &prereqError{"AHP_HUMAN_USER/AHP_HUMAN_PASSWORD or AHP_HUMAN_TOKEN_FILE"}
		}
		status, code, payload := l.post(l.base+"/u-ahp/v1/auth/login",
			map[string]string{"username": user, "password": pass}, "")
		var out struct {
			AccessToken string `json:"access_token"`
		}
		_ = json.Unmarshal(payload, &out)
		if status != http.StatusOK || out.AccessToken == "" {
			return outcome{status: status, code: code}, nil
		}
		l.human = out.AccessToken
	}
	task := l.env("AHP_TASK_ID")
	if task == "" {
		return outcome{}, &prereqError{"AHP_TASK_ID"}
	}
	request := map[string]any{"human_token": l.human, "task": task}
	if c := l.env("AHP_TASK_CONTEXT"); c != "" {
		request["context"] = c
	}
	if c := l.env("AHP_TASK_CAPABILITIES"); c != "" {
		request["capabilities"] = strings.Split(c, ",")
	}
	status, code, payload := l.post(l.base+"/v1/identity/control/exchange", request, "")
	var out struct {
		Token string `json:"token"`
		Task  string `json:"task"`
	}
	_ = json.Unmarshal(payload, &out)
	if status != http.StatusOK || out.Token == "" {
		return outcome{status: status, code: code}, nil
	}
	l.token = out.Token
	return outcome{status: http.StatusOK,
		detail: "login -> control/exchange produced a scoped task token for task " + out.Task}, nil
}

func (l *leg) ensureAuth() error {
	if l.token != "" {
		return nil
	}
	got, err := l.p2(step{ID: "P2"})
	if err != nil {
		return err
	}
	if l.token == "" {
		return fmt.Errorf("P2 produced no task token (HTTP %d)", got.status)
	}
	return nil
}

func (l *leg) tokenSource(context.Context) (string, error) {
	if l.token == "" {
		return "", errors.New("no task token yet")
	}
	return l.token, nil
}

func (l *leg) artifacts() (*artifacts.Client, error) {
	return artifacts.New(artifacts.Config{BaseURL: l.orEnv("AHP_ARTIFACTS_API_URL", l.base),
		HTTPClient: l.http, TokenSource: l.tokenSource})
}

func (l *leg) toolsClient(origin string, token string) (*tools.Client, error) {
	source := l.tokenSource
	if token != "" {
		source = func(context.Context) (string, error) { return token, nil }
	}
	return tools.New(tools.Config{BaseURL: origin, HTTPClient: l.http, TokenSource: source})
}

func (l *leg) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	time.AfterFunc(3*time.Minute, cancel)
	return ctx
}

func (l *leg) a1(step) (outcome, error) {
	if err := l.ensureAuth(); err != nil {
		return outcome{}, err
	}
	store, err := l.artifacts()
	if err != nil {
		return outcome{}, err
	}
	ctx := l.ctx()
	payload := []byte("AHP-SDK-LIVE go " + l.stamp + "\n" + strings.Repeat("q", 4096))
	sum := sha256.Sum256(payload)
	up, err := store.BeginUpload(ctx, artifacts.UploadRequest{Key: l.stamp + "-blob",
		Name: l.stamp + "-blob.txt", MIME: "text/plain", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(payload))})
	if err != nil {
		return outcome{status: statusCode(err), code: string(contracts.Code(err)),
			detail: "BeginUpload refused with " + string(contracts.Code(err))}, nil
	}
	if err := store.Upload(ctx, up, payload); err != nil {
		return outcome{status: statusCode(err), code: string(contracts.Code(err)),
			detail: "blob PUT refused with " + string(contracts.Code(err))}, nil
	}
	ref, err := store.Seal(ctx, up.ID)
	if err != nil {
		return outcome{status: statusCode(err), code: string(contracts.Code(err)),
			detail: "Seal refused with " + string(contracts.Code(err))}, nil
	}
	l.blob = blobState{id: ref.ID, version: ref.Version, digest: ref.SHA256, size: ref.Size, payload: payload}
	l.v["artifact"] = map[string]any{"artifact_id": ref.ID, "version": ref.Version,
		"sha256": ref.SHA256, "size": ref.Size}
	return outcome{status: http.StatusOK, effectProved: true,
		detail: fmt.Sprintf("sealed %s@%s size=%d digest=%s", ref.ID, ref.Version, ref.Size, ref.SHA256)}, nil
}

func (l *leg) a2(step) (outcome, error) {
	if l.blob.id == "" {
		return outcome{}, &prereqError{"A1 (this run sealed nothing, so the read has nothing to check)"}
	}
	store, err := l.artifacts()
	if err != nil {
		return outcome{}, err
	}
	body, err := store.Download(l.ctx(), l.blob.id)
	if err != nil {
		return outcome{status: statusCode(err), code: string(contracts.Code(err)),
			detail: "Download refused with " + string(contracts.Code(err))}, nil
	}
	if hex.EncodeToString(sha(body)) != l.blob.digest {
		return outcome{status: http.StatusOK}, fmt.Errorf("downloaded bytes hash differently from the sealed digest")
	}
	return outcome{status: http.StatusOK, effectProved: true,
		detail: fmt.Sprintf("%d bytes, sha256 equals the sealed digest", len(body))}, nil
}

func (l *leg) a3(step) (outcome, error) {
	if l.blob.id == "" {
		return outcome{}, &prereqError{"A1 (this run sealed nothing, so the read has nothing to check)"}
	}
	store, err := l.artifacts()
	if err != nil {
		return outcome{}, err
	}
	part, err := store.ReadRange(l.ctx(), l.blob.id, 0, 64)
	if err != nil {
		return outcome{status: statusCode(err), code: string(contracts.Code(err)),
			detail: "ReadRange refused with " + string(contracts.Code(err))}, nil
	}
	if !bytes.Equal(part, l.blob.payload[:64]) {
		return outcome{status: http.StatusOK}, fmt.Errorf("the slice is not the requested one")
	}
	return outcome{status: http.StatusOK,
		detail: fmt.Sprintf("%d bytes, equal to offset 0 length 64", len(part))}, nil
}

func (l *leg) a4(step) (outcome, error) {
	if l.blob.id == "" {
		return outcome{}, &prereqError{"A1 (this run sealed nothing, so the read has nothing to check)"}
	}
	store, err := l.artifacts()
	if err != nil {
		return outcome{}, err
	}
	body, err := store.ReadRange(l.ctx(), l.blob.id, l.blob.size+1, 16)
	if err != nil {
		return outcome{status: statusCode(err), code: string(contracts.Code(err)),
			detail: "refused with " + string(contracts.Code(err))}, nil
	}
	return outcome{status: http.StatusOK, detail: fmt.Sprintf("served %d bytes past the end", len(body))}, nil
}

func (l *leg) a5(step) (outcome, error) {
	if l.blob.id == "" {
		return outcome{}, &prereqError{"A1 (this run sealed nothing, so the read has nothing to check)"}
	}
	path := l.env("AHP_SCENARIO_FOREIGN_TASK_TOKEN_FILE")
	if path == "" {
		return outcome{}, &prereqError{"AHP_SCENARIO_FOREIGN_TASK_TOKEN_FILE (a foreign TLS client alone cannot read artifacts; the face needs its own task token)"}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return outcome{}, fmt.Errorf("foreign task token unreadable")
	}
	foreign, err := artifacts.New(artifacts.Config{BaseURL: l.orEnv("AHP_ARTIFACTS_API_URL", l.base),
		HTTPClient: l.http, TokenSource: func(context.Context) (string, error) {
			return strings.TrimSpace(string(raw)), nil
		}})
	if err != nil {
		return outcome{}, err
	}
	if _, err := foreign.Download(l.ctx(), l.blob.id); err != nil {
		return outcome{status: statusCode(err), code: string(contracts.Code(err)),
			detail: "foreign read refused with " + string(contracts.Code(err))}, nil
	}
	return outcome{status: http.StatusOK, detail: "the foreign tenant read the artifact"}, nil
}

func (l *leg) ensureTool() error {
	if err := l.ensureAuth(); err != nil {
		return err
	}
	if l.tool.target == "" {
		l.tool = toolState{key: l.stamp + "-tools-1", target: l.env("AHP_SCENARIO_TOOL")}
	}
	return nil
}

func (l *leg) toolArguments() json.RawMessage {
	if raw := l.env("AHP_TOOL_ARGUMENTS_JSON"); raw != "" {
		return json.RawMessage(raw)
	}
	encoded, _ := json.Marshal(map[string]string{"text": l.stamp})
	return encoded
}

func (l *leg) t7(step) (outcome, error) {
	if err := l.ensureTool(); err != nil {
		return outcome{}, err
	}
	caller, err := l.toolsClient(l.base, "")
	if err != nil {
		return outcome{}, err
	}
	ctx := l.ctx()
	first, err := caller.Call(ctx, l.tool.key, l.tool.target, l.toolArguments())
	if err != nil {
		return outcome{status: statusCode(err), code: string(contracts.Code(err)),
			detail: "first Call refused with " + string(contracts.Code(err))}, nil
	}
	second, err := caller.Call(ctx, l.tool.key, l.tool.target, l.toolArguments())
	if err != nil {
		return outcome{status: statusCode(err), code: string(contracts.Code(err)),
			detail: "replay refused with " + string(contracts.Code(err))}, nil
	}
	l.tool.digest = first.Digest
	if first.Digest != second.Digest || first.Key != second.Key {
		return outcome{status: http.StatusOK, effectProved: true},
			fmt.Errorf("replay did not return the journaled operation")
	}
	return outcome{status: http.StatusOK, effectProved: true,
		detail: "replay returned the journaled operation, digest " + first.Digest}, nil
}

func (l *leg) t9(step) (outcome, error) {
	if err := l.ensureTool(); err != nil {
		return outcome{}, err
	}
	caller, err := l.toolsClient(l.base, "")
	if err != nil {
		return outcome{}, err
	}
	result, err := caller.ToolResult(l.ctx(), l.tool.key)
	if err != nil {
		return outcome{status: statusCode(err), code: string(contracts.Code(err)),
			detail: "GetResult refused with " + string(contracts.Code(err))}, nil
	}
	if result.Status != "SUCCEEDED" {
		return outcome{status: http.StatusOK, code: result.Status,
			detail: "stored row is not a success: " + result.Status}, nil
	}
	if result.Storage == tools.StorageArtifact && result.Ref != nil {
		store, err := l.artifacts()
		if err != nil {
			return outcome{}, err
		}
		body, err := tools.FetchResultBytes(l.ctx(), store, result)
		if err != nil {
			return outcome{status: statusCode(err), code: string(contracts.Code(err)),
				detail: "artifact result unusable: " + err.Error()}, nil
		}
		l.v["tool_result"] = map[string]any{"storage": result.Storage, "bytes": len(body),
			"digest": result.Digest, "artifact_id": result.Ref.ArtifactID}
		return outcome{status: http.StatusOK, effectProved: true,
			detail: fmt.Sprintf("full result read through the artifact ref, %d bytes", len(body))}, nil
	}
	l.v["tool_result"] = map[string]any{"storage": result.Storage, "bytes": len(result.Payload),
		"digest": result.Digest}
	return outcome{status: http.StatusOK, effectProved: true,
		detail: fmt.Sprintf("inline result read, %d bytes", len(result.Payload))}, nil
}

// humanLeg names the three measured hops of the human byte-read route, so a
// skipped leg still says which hops were skipped (PDF rev3.2 :632).
var humanLeg = [][2]string{
	{"H1", "resolve the sealed digest to an exact version: GET /u-ahp/v1/artifacts/Resolve"},
	{"H2", "read those exact bytes with the human session: GET /u-ahp/v1/artifacts/download"},
	{"H3", "refuse latest: download with version=latest must say exact_version_required"},
}

// proposedHumanRead is NOT part of the shared contract. It measures the human
// byte-read the PDF requires (rev3.2 :632 "скачивание через прокси: текущие
// права, точная версия") and reports it in its own section, so this leg cannot
// silently widen Boyle's contract.
func (l *leg) proposedHumanRead() {
	add := func(rec record) { l.proposed = append(l.proposed, rec) }
	if l.blob.id == "" || l.human == "" {
		reason := "needs a sealed artifact from A1 and a human session from P2"
		for _, step := range humanLeg {
			add(record{ID: step[0], Plane: "human-edge", Status: statusSkipPrereq,
				Reason: reason, Contracts: step[1]})
		}
		return
	}
	status, _, payload := l.get(l.base + "/u-ahp/v1/artifacts/Resolve?sha256=" + url.QueryEscape(l.blob.digest))
	var resolved struct {
		Versions []struct {
			ArtifactID string `json:"artifact_id"`
			Version    string `json:"version"`
			SHA256     string `json:"sha256"`
		} `json:"versions"`
	}
	_ = json.Unmarshal(payload, &resolved)
	picked := ""
	for _, v := range resolved.Versions {
		if v.ArtifactID == l.blob.id {
			picked = v.Version
		}
	}
	if status != http.StatusOK || picked == "" {
		add(record{ID: "H1", Plane: "human-edge", Status: statusFail, ActualHTTP: status,
			Reason: "the human resolver did not return this artifact's sealed version"})
		return
	}
	add(record{ID: "H1", Plane: "human-edge", Status: statusPass, ActualHTTP: status,
		Actual: "digest resolved to exactly sealed version " + picked})

	status, headers, body := l.get(l.base + fmt.Sprintf("/u-ahp/v1/artifacts/download?artifact_id=%s&version=%s",
		url.QueryEscape(l.blob.id), url.QueryEscape(picked)))
	got := hex.EncodeToString(sha(body))
	l.v["human_read"] = map[string]any{"http_status": status, "downloaded_sha256": got,
		"cache_control": headers.Get("Cache-Control"), "version": picked}
	if status != http.StatusOK || got != l.blob.digest {
		add(record{ID: "H2", Plane: "human-edge", Status: statusFail, ActualHTTP: status,
			Reason: "human-read bytes are not the bytes the SDK sealed"})
	} else {
		add(record{ID: "H2", Plane: "human-edge", Status: statusPass, ActualHTTP: status,
			Actual: fmt.Sprintf("%d bytes read by the human edge, digest matches", len(body))})
	}

	status, _, payload = l.get(l.base + fmt.Sprintf("/u-ahp/v1/artifacts/download?artifact_id=%s&version=latest",
		url.QueryEscape(l.blob.id)))
	var refusal struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(payload, &refusal)
	reason := refusal.Reason
	if reason == "" {
		reason = refusal.Code
	}
	l.v["human_latest_gate"] = map[string]any{"http_status": status, "reason": reason}
	switch {
	case status >= 200 && status < 300:
		add(record{ID: "H3", Plane: "human-edge", Status: statusFail, ActualHTTP: status,
			Reason: "the proxy served 'latest': the exact-version gate is not enforced"})
	case reason != "exact_version_required":
		add(record{ID: "H3", Plane: "human-edge", Status: statusFail, ActualHTTP: status, ActualCode: reason,
			Reason: "refused for another reason than exact_version_required"})
	default:
		add(record{ID: "H3", Plane: "human-edge", Status: statusPass, ActualHTTP: status,
			Actual: "'latest' refused with exact_version_required"})
	}
}

func (l *leg) post(endpoint string, payload any, bearer string) (int, string, []byte) {
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, "", nil
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := l.http.Do(req)
	if err != nil {
		return 0, "TRANSPORT_UNREACHABLE", nil
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, codeOf(raw), raw
}

func (l *leg) get(endpoint string) (int, http.Header, []byte) {
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, nil, nil
	}
	if l.human != "" {
		req.Header.Set("Authorization", "Bearer "+l.human)
	}
	resp, err := l.http.Do(req)
	if err != nil {
		return 0, nil, nil
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return resp.StatusCode, resp.Header, raw
}

func codeOf(raw []byte) string {
	var envelope struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(raw, &envelope)
	return envelope.Code
}

func statusCode(err error) int {
	var apiErr *tools.Error
	if errors.As(err, &apiErr) && apiErr.Status != 0 {
		return apiErr.Status
	}
	return 0
}

func sha(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

func (l *leg) finish(code int) int {
	counts := map[string]int{statusPass: 0, statusFail: 0, statusSkipPrereq: 0, statusSkipUnimpl: 0}
	proved := map[string]int{}
	for _, rec := range l.recs {
		counts[rec.Status]++
		if rec.Status == statusPass && rec.EffectProved {
			proved[rec.Plane]++
		}
	}
	proposed := map[string]int{"PASS": 0, "FAIL": 0, "SKIP_PREREQ": 0}
	for _, rec := range l.proposed {
		proposed[rec.Status]++
	}
	l.v["stamp"] = l.stamp
	l.v["stamp_label"] = l.stampLabel
	l.v["steps"] = l.recs
	l.v["proposed_steps"] = l.proposed
	l.v["counts"] = counts
	l.v["proposed_counts"] = proposed
	l.v["effect_proved_by_plane"] = proved
	l.v["generated_at_utc"] = time.Now().UTC().Format(time.RFC3339)
	out, _ := json.MarshalIndent(l.v, "", "  ")
	path := l.env("AHP_VERDICT_OUT")
	if path == "" {
		path = filepath.Join(os.TempDir(), "verdict-go.json")
	}
	if err := os.WriteFile(path, append(out, '\n'), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "verdict not written:", err)
		return 2
	}
	fmt.Println(string(out))
	if code == 0 && counts[statusFail] > 0 {
		return 1
	}
	return code
}
