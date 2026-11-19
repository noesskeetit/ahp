package space_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

// The human edge has one download route, and these tests are its contract:
//
//	GET /u-ahp/v1/artifacts/download?artifact_id=<id>&version=<exact>
//
// A person gets the bytes of the EXACT sealed version they named, through Space, with
// Space's own workload identity, and every refusal Artifacts makes stays distinguishable
// on the way out. Nothing here weakens a check: the fake below only answers the way
// internal/artifacts/grant.go answers (same codes, same reason vocabulary:
// grant_version_required, grant_recipient_mismatch, grant_unknown, grant_expired,
// grant_used, artifact_deleted, artifact_legal_hold), so a proxy that swallowed a
// refusal into a 200 or a 500 fails here.

// fakeRefusal is one upstream answer the test forces, verbatim.
type fakeRefusal struct {
	status  int
	code    string
	reason  string
	message string
}

// fakeGrant is one issued ticket. Uses counts the consumptions still allowed.
type fakeGrant struct {
	token, artifactID, version string
	expiresAt                  time.Time
	uses                       int
}

// fakeCall is one machine-surface call the proxy made.
type fakeCall struct {
	operation string
	bearer    string
	body      string
}

// fakeArtifacts speaks the Artifacts wire contract for the two grant operations.
type fakeArtifacts struct {
	mu       sync.Mutex
	objects  map[string][]byte // "<id>@<version>" -> bytes
	deleted  map[string]bool
	legal    map[string]bool
	grants   map[fakeGrant]bool
	issued   []fakeGrant
	calls    []fakeCall
	nextID   int
	wantTask string
	// humanJWT is what a browser holds. A call bearing it means the proxy forwarded the
	// person's own credential to a machine surface, which is exactly what Artifacts
	// refuses - and what this route must never do.
	humanJWT string
	server   *httptest.Server
	failOpen *fakeRefusal
	// bareRangeRefusal makes the double answer an unsatisfiable Range with the status and
	// Content-Range only, with no envelope - the variant the product must still not turn
	// into an internal error. Production sends an envelope, so the default is false.
	bareRangeRefusal bool
	forcedToken      string // when set, every mint returns this same token (replay/caching)
	defaultUses      int
}

func newFakeArtifacts(t *testing.T, wantTask, humanJWT string) *fakeArtifacts {
	t.Helper()
	f := &fakeArtifacts{
		objects:     map[string][]byte{},
		deleted:     map[string]bool{},
		legal:       map[string]bool{},
		grants:      map[fakeGrant]bool{},
		wantTask:    wantTask,
		humanJWT:    humanJWT,
		defaultUses: 1,
	}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	f.server = srv
	return f
}

func (f *fakeArtifacts) put(id, version string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[id+"@"+version] = body
}

func (f *fakeArtifacts) mintCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeArtifacts) callsFor(op string) []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeCall
	for _, c := range f.calls {
		if c.operation == op {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeArtifacts) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	op := strings.TrimPrefix(r.URL.Path, "/v1/artifacts/")
	bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	f.mu.Lock()
	f.calls = append(f.calls, fakeCall{operation: op, bearer: bearer, body: string(body)})
	f.mu.Unlock()

	if r.Method != http.MethodPost {
		writeFakeError(w, http.StatusNotFound, "NOT_FOUND", "", "artifactsapi: unknown route")
		return
	}
	if bearer == f.humanJWT {
		// A human token is not a task token: the audience and the workload certificate
		// are both wrong. Refuse it the way the machine surface does.
		writeFakeError(w, http.StatusForbidden, "FORBIDDEN", "task_token_invalid",
			"identity: token audience is not this service")
		return
	}
	if bearer != f.wantTask {
		writeFakeError(w, http.StatusForbidden, "FORBIDDEN", "task_token_invalid", "unverified task token")
		return
	}
	switch op {
	case "DownloadGrant":
		f.mint(w, body)
	case "OpenDownloadGrant":
		f.open(w, r, body)
	default:
		writeFakeError(w, http.StatusNotFound, "NOT_FOUND", "", "artifactsapi: unknown route")
	}
}

func (f *fakeArtifacts) mint(w http.ResponseWriter, raw []byte) {
	// The real server reads strictly: an unknown field is INVALID_ARGUMENT, not ignored.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var in struct {
		ID         string `json:"id"`
		Version    string `json:"version"`
		Recipient  string `json:"recipient,omitempty"`
		TTLSeconds int64  `json:"ttl_seconds,omitempty"`
		Uses       int    `json:"uses,omitempty"`
	}
	if err := dec.Decode(&in); err != nil {
		writeFakeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "", err.Error())
		return
	}
	if in.Version == "" || strings.EqualFold(in.Version, "latest") {
		writeFakeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "grant_version_required",
			"an exact sealed version is required")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleted[in.ID] {
		writeFakeError(w, http.StatusNotFound, "NOT_FOUND", "artifact_deleted", "artifact not found")
		return
	}
	if f.legal[in.ID] {
		writeFakeError(w, http.StatusForbidden, "FORBIDDEN", "artifact_legal_hold",
			"artifact is under legal hold")
		return
	}
	key := in.ID + "@" + in.Version
	data, ok := f.objects[key]
	if !ok {
		writeFakeError(w, http.StatusNotFound, "NOT_FOUND", "", "artifact not found")
		return
	}
	if in.Recipient != "" && in.Recipient != "TASK-subject" {
		writeFakeError(w, http.StatusForbidden, "FORBIDDEN", "grant_recipient_mismatch",
			"recipient may only name the calling subject")
		return
	}
	token := f.forcedToken
	if token == "" {
		f.nextID++
		token = fmt.Sprintf("G%d", f.nextID)
	}
	uses := in.Uses
	if uses == 0 {
		uses = f.defaultUses
	}
	ttl := time.Duration(in.TTLSeconds) * time.Second
	if ttl == 0 {
		ttl = time.Minute
	}
	g := fakeGrant{token: token, artifactID: in.ID, version: in.Version,
		expiresAt: time.Now().Add(ttl), uses: uses}
	f.grants[g] = true
	f.issued = append(f.issued, g)
	sum := sha256.Sum256(data)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"grant":       g.token,
		"artifact_id": in.ID,
		"version":     in.Version,
		"sha256":      hex.EncodeToString(sum[:]),
		"size":        len(data),
		"recipient":   "TASK-subject",
		"one_time":    uses == 1,
		"uses_max":    uses,
		"issued_at":   time.Now().UTC(),
		"expires_at":  g.expiresAt.UTC(),
	})
}

func (f *fakeArtifacts) open(w http.ResponseWriter, r *http.Request, raw []byte) {
	if f.failOpen != nil {
		f.mu.Lock()
		fail := *f.failOpen
		f.mu.Unlock()
		writeFakeError(w, fail.status, fail.code, fail.reason, fail.message)
		return
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var in struct {
		Grant string `json:"grant"`
	}
	if err := dec.Decode(&in); err != nil {
		writeFakeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "", err.Error())
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var found *fakeGrant
	for g := range f.grants {
		if g.token == in.Grant {
			cp := g
			found = &cp
			break
		}
	}
	if found == nil {
		writeFakeError(w, http.StatusForbidden, "FORBIDDEN", "grant_unknown", "grant is unknown")
		return
	}
	if time.Now().After(found.expiresAt) {
		writeFakeError(w, http.StatusForbidden, "FORBIDDEN", "grant_expired", "grant has expired")
		return
	}
	if found.uses <= 0 {
		writeFakeError(w, http.StatusForbidden, "FORBIDDEN", "grant_used", "grant is already used")
		return
	}
	if f.deleted[found.artifactID] {
		writeFakeError(w, http.StatusNotFound, "NOT_FOUND", "artifact_deleted", "artifact not found")
		return
	}
	data := f.objects[found.artifactID+"@"+found.version]
	if data == nil {
		writeFakeError(w, http.StatusNotFound, "NOT_FOUND", "", "artifact not found")
		return
	}
	// Consume one use in place.
	// The map key IS the grant, so consuming a use means replacing the entry: delete the
	// stored key first, then insert the decremented one. Mutating the copy before the
	// delete would delete nothing and leave a reusable ticket behind.
	for g := range f.grants {
		if g.token == found.token {
			used := g
			delete(f.grants, g)
			used.uses--
			f.grants[used] = true
			break
		}
	}
	sum := sha256.Sum256(data)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	w.Header().Set("Content-Type", "application/octet-stream")
	if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
		start, end, ok := parseTestRange(rng, len(data))
		if !ok {
			// Measured on the live pin: the machine surface answers 416 with an envelope
			// whose code is "range_not_satisfiable" - a name outside the contracts
			// vocabulary - plus the RFC's Content-Range. The proxy must carry both.
			w.Header().Set("Content-Range", "bytes */"+strconv.Itoa(len(data)))
			if f.bareRangeRefusal {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			writeFakeError(w, http.StatusRequestedRangeNotSatisfiable, "range_not_satisfiable",
				"", "INVALID_ARGUMENT: range not satisfiable")
			return
		}
		w.Header().Set("Content-Range", "bytes "+strconv.Itoa(start)+"-"+strconv.Itoa(end)+
			"/"+strconv.Itoa(len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func parseTestRange(rng string, size int) (int, int, bool) {
	spec := strings.TrimPrefix(rng, "bytes=")
	bounds := strings.SplitN(spec, "-", 2)
	if len(bounds) != 2 {
		return 0, 0, false
	}
	start, err := strconv.Atoi(strings.TrimSpace(bounds[0]))
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	end := size - 1
	if b := strings.TrimSpace(bounds[1]); b != "" {
		end, err = strconv.Atoi(b)
		if err != nil || end < start || end >= size {
			return 0, 0, false
		}
	}
	return start, end, true
}

func writeFakeError(w http.ResponseWriter, status int, code, reason, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := map[string]string{"code": code, "message": message}
	if reason != "" {
		body["reason"] = reason
	}
	_ = json.NewEncoder(w).Encode(body)
}

// server makes the production wiring for this route: the proxy handler behind the SAME
// human-JWT auth + human->task exchange middleware that guards the Space API (this is
// what cmd/as-runner does in buildMux/main).
func (f *fakeArtifacts) proxyServer(t *testing.T, ex space.TaskExchanger) *httptest.Server {
	t.Helper()
	h, err := space.NewDownloadProxy(space.DownloadProxyConfig{
		ArtifactsURL: f.server.URL,
		Client:       f.server.Client(),
	})
	if err != nil {
		t.Fatalf("NewDownloadProxy: %v", err)
	}
	var wrapped http.Handler = h
	if ex != nil {
		wrapped = space.ExchangeMiddleware(ex, authPerSubject)(h)
	}
	srv := httptest.NewServer(wrapped)
	t.Cleanup(srv.Close)
	return srv
}

const testHumanJWT = "human-jwt-must-never-leave-the-edge"

func downloadAs(t *testing.T, url, subject, query, rangeHeader string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url+space.UIDownloadPath+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Test-Subject", subject)
	req.Header.Set("Authorization", "Bearer "+testHumanJWT)
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func readContractError(t *testing.T, resp *http.Response) (code, reason string, body []byte) {
	t.Helper()
	body, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var env struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("refusal %d is not a contracts envelope: %s", resp.StatusCode, body)
	}
	return env.Code, env.Reason, body
}

// TestUIDownloadStreamsExactVersionByteForByte is the positive leg: the browser gets the
// exact sealed version's bytes, identical sha256, with the version's own length/ETag, and
// exactly one use-counted grant per request (a proxy that cached a grant would issue zero
// or reuse one, and both are wrong here).
func TestUIDownloadStreamsExactVersionByteForByte(t *testing.T) {
	const body = "отчёт по активным задачам\n" // deterministic bytes, incl. non-ASCII
	want := sha256.Sum256([]byte(body))
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.put("art-1", "v1", []byte(body))
	srv := f.proxyServer(t, &spyExchanger{})

	resp := downloadAs(t, srv.URL, "alice", "?artifact_id=art-1&version=v1", "")
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("download = %d, want 200 (%s)", resp.StatusCode, raw)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte(body)) {
		t.Fatalf("bytes differ: got %q want %q", got, body)
	}
	if sum := sha256.Sum256(got); hex.EncodeToString(sum[:]) != hex.EncodeToString(want[:]) {
		t.Fatalf("sha256 = %x, want %x", sum, want)
	}
	if cl := resp.Header.Get("Content-Length"); cl != strconv.Itoa(len(body)) {
		t.Fatalf("Content-Length = %q, want %d", cl, len(body))
	}
	if et := resp.Header.Get("ETag"); !strings.Contains(et, hex.EncodeToString(want[:])) {
		t.Fatalf("ETag = %q, want the sealed digest of the exact version", et)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store (a proxy must not keep a person's file)", cc)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.Contains(cd, "art-1") {
		t.Fatalf("Content-Disposition = %q, want a filename derived from the artifact id", cd)
	}
	if ar := resp.Header.Get("Accept-Ranges"); ar != "bytes" {
		t.Fatalf("Accept-Ranges = %q, want bytes", ar)
	}
	if mints := f.callsFor("DownloadGrant"); len(mints) != 1 || mints[0].bearer != "TASK:alice" {
		t.Fatalf("mints = %+v, want exactly one issued with the EXCHANGED task token", mints)
	}
	if !strings.Contains(f.callsFor("DownloadGrant")[0].body, `"version":"v1"`) {
		t.Fatalf("mint body = %s, want the exact version named by the caller", f.callsFor("DownloadGrant")[0].body)
	}
}

// TestUIDownloadProxiesByteRange keeps Range a proxy, not a reinterpretation: the same
// bytes and the same Content-Range the machine surface would answer.
func TestUIDownloadProxiesByteRange(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789"), 4) // 40 bytes
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.put("art-2", "v7", payload)
	srv := f.proxyServer(t, &spyExchanger{})

	resp := downloadAs(t, srv.URL, "alice", "?artifact_id=art-2&version=v7", "bytes=5-9")
	if resp.StatusCode != http.StatusPartialContent {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("range download = %d, want 206 (%s)", resp.StatusCode, raw)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "56789" {
		t.Fatalf("range bytes = %q, want 56789", got)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "bytes 5-9/40" {
		t.Fatalf("Content-Range = %q, want %q", cr, "bytes 5-9/40")
	}
}

// TestUIDownloadVersionIsExact proves the frozen shape: both parameters required, and
// "latest" is refused rather than resolved. A wrong-file download is a data-integrity
// event, so this gate is in front of any call to Artifacts at all.
func TestUIDownloadVersionIsExact(t *testing.T) {
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.put("art-3", "v1", []byte("x"))
	srv := f.proxyServer(t, &spyExchanger{})

	cases := []struct {
		name       string
		query      string
		wantReason string
	}{
		{"no version at all", "?artifact_id=art-3", "exact_version_required"},
		{"empty version", "?artifact_id=art-3&version=", "exact_version_required"},
		{"blank version", "?artifact_id=art-3&version=%20", "exact_version_required"},
		{"latest is not a version", "?artifact_id=art-3&version=latest", "exact_version_required"},
		{"LATEST in caps", "?artifact_id=art-3&version=LATEST", "exact_version_required"},
		{"no artifact id", "?version=v1", "artifact_id_required"},
		{"blank artifact id", "?artifact_id=%20&version=v1", "artifact_id_required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := downloadAs(t, srv.URL, "alice", tc.query, "")
			code, reason, raw := readContractError(t, resp)
			if resp.StatusCode != http.StatusBadRequest || code != "INVALID_ARGUMENT" {
				t.Fatalf("%s = %d/%s, want 400/INVALID_ARGUMENT (%s)", tc.name, resp.StatusCode, code, raw)
			}
			if reason != tc.wantReason {
				t.Fatalf("%s reason = %q, want %q", tc.name, reason, tc.wantReason)
			}
		})
	}
	if n := len(f.calls); n != 0 {
		t.Fatalf("proxy called Artifacts %d time(s) for malformed requests, want 0", n)
	}
}

// TestUIDownloadRequiresAnExchangedTask: a request that never completed the human->task
// exchange carries no authority, and the route says so without touching Artifacts.
func TestUIDownloadRequiresAnExchangedTask(t *testing.T) {
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.put("art-4", "v1", []byte("x"))
	srv := f.proxyServer(t, nil) // auth only: the exchange stage is absent

	resp := downloadAs(t, srv.URL, "alice", "?artifact_id=art-4&version=v1", "")
	code, _, raw := readContractError(t, resp)
	if resp.StatusCode != http.StatusForbidden || code != "FORBIDDEN" {
		t.Fatalf("unexchanged request = %d/%s, want 403/FORBIDDEN (%s)", resp.StatusCode, code, raw)
	}
	if n := len(f.calls); n != 0 {
		t.Fatalf("proxy reached Artifacts %d time(s) without a task token, want 0", n)
	}
}

// TestHumanJWTIsNeverForwardedAndNeverAcceptedByArtifacts is the two-sided half of
// стр.743: the edge forwards only the exchanged task token, and a machine surface that is
// handed the human JWT answers a refusal - never 200, never 500.
func TestHumanJWTIsNeverForwardedAndNeverAcceptedByArtifacts(t *testing.T) {
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.put("art-5", "v1", []byte("payload"))
	srv := f.proxyServer(t, &spyExchanger{})

	resp := downloadAs(t, srv.URL, "alice", "?artifact_id=art-5&version=v1", "")
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("leg setup: download = %d (%s)", resp.StatusCode, raw)
	}
	_, _ = io.ReadAll(resp.Body)
	for _, c := range f.calls {
		if c.bearer == testHumanJWT {
			t.Fatalf("%s was called with the human JWT; the edge must forward only the exchanged task token", c.operation)
		}
		if strings.Contains(c.body, testHumanJWT) {
			t.Fatalf("%s body carries the human JWT: %s", c.operation, c.body)
		}
	}

	// The other side: the browser's credential aimed straight at the machine surface.
	req, err := http.NewRequest(http.MethodPost, f.server.URL+"/v1/artifacts/DownloadGrant",
		strings.NewReader(`{"id":"art-5","version":"v1"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testHumanJWT)
	direct, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = direct.Body.Close() }()
	code, reason, raw := readContractError(t, direct)
	if direct.StatusCode == http.StatusOK || direct.StatusCode >= 500 {
		t.Fatalf("human JWT straight at Artifacts = %d, want a client-side refusal (%s)", direct.StatusCode, raw)
	}
	if code != "FORBIDDEN" || reason != "task_token_invalid" {
		t.Fatalf("human JWT straight at Artifacts = %s/%s, want FORBIDDEN/task_token_invalid (%s)", code, reason, raw)
	}
}

// TestUIDownloadTransportsEveryGrantRefusal is the reason-preservation leg. Each row is
// an answer internal/artifacts/grant.go really emits; the proxy must return the same HTTP
// class, the same code and the same reason, so a browser can tell an expired ticket from
// a used one, a tombstone from a legal hold - and never sees a 500 for a refusal.
func TestUIDownloadTransportsEveryGrantRefusal(t *testing.T) {
	cases := []struct {
		name       string
		setup      func(f *fakeArtifacts)
		wantStatus int
		wantCode   string
		wantReason string
	}{
		{
			name:       "a version of this artifact that was never sealed",
			setup:      func(f *fakeArtifacts) { f.put("art-6", "v2", []byte("a different version")) },
			wantStatus: http.StatusNotFound,
			wantCode:   "NOT_FOUND",
		},
		{
			name: "tombstone answers like any deleted artifact",
			setup: func(f *fakeArtifacts) {
				f.put("art-6", "v1", []byte("gone"))
				f.mu.Lock()
				f.deleted["art-6"] = true
				f.mu.Unlock()
			},
			wantStatus: http.StatusNotFound,
			wantCode:   "NOT_FOUND",
			wantReason: "artifact_deleted",
		},
		{
			name: "legal hold stays visible",
			setup: func(f *fakeArtifacts) {
				f.put("art-6", "v1", []byte("held"))
				f.mu.Lock()
				f.legal["art-6"] = true
				f.mu.Unlock()
			},
			wantStatus: http.StatusForbidden,
			wantCode:   "FORBIDDEN",
			wantReason: "artifact_legal_hold",
		},
		{
			name:       "a grant that expired between issue and open",
			setup:      func(f *fakeArtifacts) { f.put("art-6", "v1", []byte("x")) },
			wantStatus: http.StatusForbidden,
			wantCode:   "FORBIDDEN",
			wantReason: "grant_expired",
		},
		{
			name:       "a one-time grant replayed",
			setup:      func(f *fakeArtifacts) { f.put("art-6", "v1", []byte("x")) },
			wantStatus: http.StatusForbidden,
			wantCode:   "FORBIDDEN",
			wantReason: "grant_used",
		},
		{
			name:       "a grant the store never issued",
			setup:      func(f *fakeArtifacts) { f.put("art-6", "v1", []byte("x")) },
			wantStatus: http.StatusForbidden,
			wantCode:   "FORBIDDEN",
			wantReason: "grant_unknown",
		},
	}
	refusals := map[string]*fakeRefusal{
		"a grant that expired between issue and open": {
			status: http.StatusForbidden, code: "FORBIDDEN", reason: "grant_expired", message: "grant has expired"},
		"a one-time grant replayed": {
			status: http.StatusForbidden, code: "FORBIDDEN", reason: "grant_used", message: "grant is already used"},
		"a grant the store never issued": {
			status: http.StatusForbidden, code: "FORBIDDEN", reason: "grant_unknown", message: "grant is unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
			tc.setup(f)
			if r := refusals[tc.name]; r != nil {
				f.mu.Lock()
				f.failOpen = r
				f.mu.Unlock()
			}
			srv := f.proxyServer(t, &spyExchanger{})
			resp := downloadAs(t, srv.URL, "alice", "?artifact_id=art-6&version=v1", "")
			code, reason, raw := readContractError(t, resp)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("%s = %d, want %d (%s)", tc.name, resp.StatusCode, tc.wantStatus, raw)
			}
			if resp.StatusCode == http.StatusInternalServerError {
				t.Fatalf("%s collapsed into a 500 - a refusal with a reason must stay distinguishable", tc.name)
			}
			if code != tc.wantCode {
				t.Fatalf("%s code = %s, want %s (%s)", tc.name, code, tc.wantCode, raw)
			}
			if tc.wantReason != "" && reason != tc.wantReason {
				t.Fatalf("%s reason = %q, want %q (%s)", tc.name, reason, tc.wantReason, raw)
			}
			if body := mustReadAll(t, resp); len(body) != 0 {
				t.Fatalf("%s carried a body after a refusal: %q", tc.name, body)
			}
		})
	}
}

func mustReadAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	rest, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
	return rest
}

// TestUIDownloadFakeGrantIsReallySingleUse guards the harness itself: if the fake let a
// one-time ticket be redeemed twice, the refusal leg above would prove nothing.
func TestUIDownloadFakeGrantIsReallySingleUse(t *testing.T) {
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.put("art-7", "v1", []byte("once"))
	srv := f.proxyServer(t, &spyExchanger{})

	resp := downloadAs(t, srv.URL, "alice", "?artifact_id=art-7&version=v1", "")
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("first download = %d (%s)", resp.StatusCode, raw)
	}
	_, _ = io.ReadAll(resp.Body)
	token := f.issuedGrantToken(t)

	open := func(tok string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, f.server.URL+"/v1/artifacts/OpenDownloadGrant",
			strings.NewReader(`{"grant":"`+tok+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer TASK:alice")
		resp2, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp2
	}
	// One use was already consumed by the proxy leg above.
	second := open(token)
	t.Cleanup(func() { _ = second.Body.Close() })
	code, reason, raw := readContractError(t, second)
	if second.StatusCode != http.StatusForbidden || code != "FORBIDDEN" || reason != "grant_used" {
		t.Fatalf("replaying a one-time grant = %d/%s/%s, want 403/FORBIDDEN/grant_used (%s)",
			second.StatusCode, code, reason, raw)
	}
}

// issuedGrantToken returns the most recently minted token (harness helper).
func (f *fakeArtifacts) issuedGrantToken(t *testing.T) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.issued) == 0 {
		t.Fatal("no grant was minted")
	}
	return f.issued[len(f.issued)-1].token
}

// TestUIDownloadRejectsOtherMethodsAndBadConfig: only GET/HEAD is a download, and a proxy
// that cannot be configured honestly is refused at construction rather than serving a
// weaker route.
func TestUIDownloadRejectsOtherMethodsAndBadConfig(t *testing.T) {
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.put("art-8", "v1", []byte("x"))
	srv := f.proxyServer(t, &spyExchanger{})

	post, err := http.NewRequest(http.MethodPost, srv.URL+space.UIDownloadPath+"?artifact_id=art-8&version=v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	post.Header.Set("X-Test-Subject", "alice")
	post.Header.Set("Authorization", "Bearer "+testHumanJWT)
	resp, err := http.DefaultClient.Do(post)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("POST to the download route = %d, want 404", resp.StatusCode)
	}

	if _, err := space.NewDownloadProxy(space.DownloadProxyConfig{}); err == nil {
		t.Fatal("a proxy without the Artifacts URL must fail to build, not serve")
	}
	if _, err := space.NewDownloadProxy(space.DownloadProxyConfig{ArtifactsURL: "not-a-url"}); err == nil {
		t.Fatal("a proxy with an unparsable Artifacts URL must fail to build")
	}
	if _, err := space.NewDownloadProxy(space.DownloadProxyConfig{ArtifactsURL: "https://artifacts.internal"}); err == nil {
		t.Fatal("a proxy without Space's own workload client must fail to build")
	}
}

// TestUIDownloadHeadIsHeadersOnly keeps HEAD usable for a size check without shipping the
// file twice.
func TestUIDownloadHeadIsHeadersOnly(t *testing.T) {
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.put("art-9", "v2", []byte("eleven bytes"))
	srv := f.proxyServer(t, &spyExchanger{})

	req, err := http.NewRequest(http.MethodHead, srv.URL+space.UIDownloadPath+"?artifact_id=art-9&version=v2", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Test-Subject", "alice")
	req.Header.Set("Authorization", "Bearer "+testHumanJWT)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD = %d, want 200", resp.StatusCode)
	}
	if cl := resp.Header.Get("Content-Length"); cl != "12" {
		t.Fatalf("HEAD Content-Length = %q, want 12", cl)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) != 0 {
		t.Fatalf("HEAD returned %d body byte(s), want none", len(body))
	}
}

// TestUIDownloadUnsatisfiableRangeIsNotAnInternalError is the defect measured on the live pin
// by registry-deploy (20261101T013012Z): the machine surface answers an unsatisfiable Range
// with 416 + Content-Range + the code "range_not_satisfiable", while the browser surface behind
// this proxy answered 500 INTERNAL, because that code is outside the contracts vocabulary
// transport can render and fell through to the internal-error default.
//
// The practical harm is not cosmetic: a browser resuming a interrupted download with a tail
// Range was told the SERVER failed, so neither the UI nor the user can tell "ask for bytes
// that do not exist" from "the platform is down", and the natural reaction is to retry forever.
func TestUIDownloadUnsatisfiableRangeIsNotAnInternalError(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789"), 4) // 40 bytes
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.put("art-range", "v1", payload)
	srv := f.proxyServer(t, &spyExchanger{})

	for _, rng := range []string{"bytes=40-44", "bytes=100-200"} {
		resp := downloadAs(t, srv.URL, "alice", "?artifact_id=art-range&version=v1", rng)
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if resp.StatusCode == http.StatusInternalServerError {
			t.Fatalf("Range %s: the proxy flattened Artifacts' own verdict into 500 (body=%s)", rng, body)
		}
		if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("Range %s: got %d, want 416 exactly as the machine surface answered (%s)", rng, resp.StatusCode, body)
		}
		if cr := resp.Header.Get("Content-Range"); cr != "bytes */40" {
			t.Fatalf("Range %s: Content-Range = %q, want the upstream's own %q", rng, cr, "bytes */40")
		}
		// The name Artifacts chose survives the hop; the proxy does not rename a verdict.
		if !strings.Contains(string(body), "range_not_satisfiable") {
			t.Fatalf("Range %s: the refusal code was rewritten, body=%s", rng, body)
		}
	}
}

// TestUIDownloadUnsatisfiableRangeWithoutEnvelopeStillCarriesTheVerdict covers the variant
// where the upstream states only a status. The status IS a verdict, so it is forwarded with a
// code that admits it is unclassified - the honest alternative to claiming an internal error.
func TestUIDownloadUnsatisfiableRangeWithoutEnvelopeStillCarriesTheVerdict(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789"), 4)
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.bareRangeRefusal = true
	f.put("art-bare", "v1", payload)
	srv := f.proxyServer(t, &spyExchanger{})

	resp := downloadAs(t, srv.URL, "alice", "?artifact_id=art-bare&version=v1", "bytes=100-200")
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("bare 416 became %d (%s): the upstream status is a verdict and must not be dropped", resp.StatusCode, body)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "bytes */40" {
		t.Fatalf("Content-Range = %q, want %q", cr, "bytes */40")
	}
	if strings.Contains(string(body), "INTERNAL") || strings.Contains(string(body), "internal server error") {
		t.Fatalf("a stated refusal must not surface as an internal error: %s", body)
	}
}

// TestUIDownloadCanonicalRefusalsKeepTheirShape guards the other side of the same change: the
// refusals transport CAN render must come out byte-for-byte as before, or this fix would have
// traded one broken contract for another. Codes below are the canonical vocabulary, so they
// still travel through transport.WriteError.
func TestUIDownloadCanonicalRefusalsKeepTheirShape(t *testing.T) {
	f := newFakeArtifacts(t, "TASK:alice", testHumanJWT)
	f.put("art-shape", "v1", []byte("payload"))
	f.mu.Lock()
	f.legal["art-shape"] = true // mint is refused with FORBIDDEN, a canonical code
	f.mu.Unlock()
	srv := f.proxyServer(t, &spyExchanger{})

	resp := downloadAs(t, srv.URL, "alice", "?artifact_id=art-shape&version=v1", "")
	code, _, body := readContractError(t, resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("canonical refusal = %d (%s), want 403", resp.StatusCode, body)
	}
	if code != "FORBIDDEN" {
		t.Fatalf("canonical refusal code = %q, want FORBIDDEN unchanged (%s)", code, body)
	}
}
