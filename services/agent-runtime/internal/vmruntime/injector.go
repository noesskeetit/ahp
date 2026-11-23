//go:build unix

package vmruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const injectorSessions = "/v1/state/sessions/"

type InjectorConfig struct {
	StateURL  string
	ClientFor func(context.Context, contracts.Scope) (*http.Client, error)
	MintToken func(context.Context, Execution) (string, error)
}

// Injector owns a separate listener, cancellation scope and task token for
// each physical attempt. The host still owns the execution grant.
type Injector struct {
	stateURL  *url.URL
	clientFor func(context.Context, contracts.Scope) (*http.Client, error)
	mintToken func(context.Context, Execution) (string, error)
	mu        sync.Mutex
	attempts  map[string]*injectorAttempt
}

type injectorAttempt struct {
	guest     Guest
	cancel    context.CancelFunc
	listener  *net.UnixListener
	server    *http.Server
	tokenGate chan struct{}
	token     string
	expires   time.Time
}

func NewInjector(config InjectorConfig) (*Injector, error) {
	origin, err := url.Parse(config.StateURL)
	if err != nil || origin.Scheme != "https" || origin.Hostname() == "" ||
		origin.User != nil || origin.Opaque != "" || origin.RawPath != "" ||
		(origin.Path != "" && origin.Path != "/") || origin.RawQuery != "" ||
		origin.ForceQuery || origin.Fragment != "" || config.ClientFor == nil || config.MintToken == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "injector requires an HTTPS State API origin and credential providers")
	}
	return &Injector{
		stateURL: origin, clientFor: config.ClientFor, mintToken: config.MintToken,
		attempts: make(map[string]*injectorAttempt),
	}, nil
}

// Start opens the socket before returning. The attempt remains live after
// the launch request finishes, until its owner calls Close.
func (i *Injector) Start(ctx context.Context, guest Guest, socketPath string) error {
	if err := guest.Execution.validate(); err != nil {
		return err
	}
	if guest.Attempt < 1 || !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath {
		return contracts.NewError(contracts.ErrInvalid, "invalid injector attempt or socket path")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	id := guest.ID()
	if _, exists := i.attempts[id]; exists {
		return contracts.NewError(contracts.ErrConflict, "injector attempt already has a listener")
	}
	dir, err := os.Lstat(filepath.Dir(socketPath))
	if err != nil {
		return err
	}
	stat, owned := dir.Sys().(*syscall.Stat_t)
	if !dir.IsDir() || dir.Mode().Perm() != 0o700 || !owned || int(stat.Uid) != os.Geteuid() {
		return contracts.NewError(contracts.ErrInvalid, "injector socket directory must be private and owned by the runtime user")
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return err
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		return err
	}
	if err := ctx.Err(); err != nil {
		_ = listener.Close()
		return err
	}
	attemptCtx, cancel := context.WithCancel(context.Background())
	attempt := &injectorAttempt{
		guest: guest, cancel: cancel, listener: listener, tokenGate: make(chan struct{}, 1),
	}
	attempt.server = &http.Server{
		BaseContext: func(net.Listener) context.Context { return attemptCtx },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			i.serveAttempt(attempt, w, r)
		}),
		DisableGeneralOptionsHandler: true,
		ReadHeaderTimeout:            5 * time.Second,
		ReadTimeout:                  30 * time.Second,
		IdleTimeout:                  30 * time.Second,
		MaxHeaderBytes:               64 << 10,
	}
	i.attempts[id] = attempt
	go func() {
		_ = attempt.server.Serve(listener)
		i.mu.Lock()
		defer i.mu.Unlock()
		if i.attempts[id] == attempt {
			_ = i.closeAttempt(id, attempt)
		}
	}()
	return nil
}

func (i *Injector) Listening(guest Guest) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	attempt := i.attempts[guest.ID()]
	return attempt != nil && attempt.matches(guest)
}

func (i *Injector) Close(guest Guest) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	id := guest.ID()
	attempt := i.attempts[id]
	if attempt == nil {
		return nil
	}
	if !attempt.matches(guest) {
		return contracts.NewError(contracts.ErrConflict, "injector attempt address does not match")
	}
	return i.closeAttempt(id, attempt)
}

// Fence requests address the execution without carrying launch-only fields.
func (a *injectorAttempt) matches(guest Guest) bool {
	current, requested := a.guest.Execution, guest.Execution
	return a.guest.Attempt == guest.Attempt && current.Scope == requested.Scope && current.ID == requested.ID &&
		current.SessionID == requested.SessionID && current.Generation == requested.Generation
}

func (i *Injector) closeAttempt(id string, attempt *injectorAttempt) error {
	delete(i.attempts, id)
	attempt.cancel()
	serverErr := attempt.server.Close()
	// Serve may still be starting, in which case the server has not yet
	// registered the listener and cannot close or unlink it itself.
	listenerErr := attempt.listener.Close()
	if errors.Is(listenerErr, net.ErrClosed) {
		listenerErr = nil
	}
	return errors.Join(serverErr, listenerErr)
}

func (i *Injector) serveAttempt(attempt *injectorAttempt, w http.ResponseWriter, r *http.Request) {
	abortCanceledInjectorRequest(r.Context())
	path, _, _ := strings.Cut(r.RequestURI, "?")
	if r.URL.IsAbs() || r.URL.Opaque != "" || r.URL.Host != "" || path != r.URL.EscapedPath() {
		injectorForbidden(w, r)
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		injectorForbidden(w, r)
		return
	}
	exec := attempt.guest.Execution
	if r.Method == http.MethodGet && path == "/v1/runtime/bootstrap" && len(query) == 0 && r.Body == http.NoBody {
		transport.WriteJSON(w, http.StatusOK, journal.RuntimeBootstrap{
			Scope: exec.Scope, SessionID: exec.SessionID, Harness: exec.Harness,
			LaunchTurnSeq: exec.LaunchTurnSeq, Generation: exec.Generation, ExecutionID: exec.ID,
		})
		return
	}
	var body []byte
	var allowed bool
	session := injectorSessions + url.PathEscape(exec.SessionID)
	switch r.Method {
	case http.MethodGet:
		allowed = r.Body == http.NoBody && injectorReadAllowed(path, session, exec.SessionID, query)
	case http.MethodPost:
		if len(query) == 0 {
			body, allowed = injectorWriteBody(w, r, path, session, exec)
		}
	}
	if !allowed {
		injectorForbidden(w, r)
		return
	}
	i.forward(attempt, w, r, query, body)
}

func injectorForbidden(w http.ResponseWriter, r *http.Request) {
	transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "injector request is outside the attempt policy"))
}

func injectorReadAllowed(path, session, sessionID string, query url.Values) bool {
	switch path {
	case session:
		return len(query) == 0 || injectorQueryAllowed(query, "", "view") && query.Get("view") == "resume"
	case session + "/events":
		return injectorQueryAllowed(query, "", "after_seq", "limit", "q", "branch_leaf_id", "after_ts", "active_only", "num_recent")
	case session + "/checkpoints":
		return injectorQueryAllowed(query, "", "checkpoint_ns", "checkpoint_id", "list", "before", "limit", "include_tuples", "metadata", "include_unindexed")
	case session + "/chat-history":
		return injectorQueryAllowed(query, "", "checkpoint_ns", "checkpoint_id")
	case "/v1/state/kv":
		return injectorQueryAllowed(query, "key", "scope_type", "session_id", "namespace", "key", "prefix", "limit", "offset") &&
			query.Get("scope_type") == "session" && query.Get("session_id") == sessionID
	case "/v1/state/aux:mutate":
		return injectorQueryAllowed(query, "", "session_id", "call_id", "state_namespace") &&
			query.Get("session_id") == sessionID && query.Get("state_namespace") == ""
	default:
		return false
	}
}

func injectorQueryAllowed(query url.Values, repeatable string, fields ...string) bool {
	for field, values := range query {
		if !slices.Contains(fields, field) || (field != repeatable && len(values) != 1) {
			return false
		}
	}
	return true
}

type injectorHeartbeat struct {
	Epoch   int64  `json:"lease_epoch"`
	TTL     int64  `json:"ttl_ms,omitempty"`
	Release bool   `json:"release,omitempty"`
	Status  string `json:"status,omitempty"`
}

type injectorAppend struct {
	Epoch int64 `json:"lease_epoch"`
	journal.AppendRequest
}

type injectorKV struct {
	TurnSeq     int64                          `json:"turn_seq,omitempty"`
	Catalog     *journal.OpenClawCatalogUpdate `json:"catalog,omitempty"`
	SessionID   string                         `json:"session_id"`
	Epoch       int64                          `json:"lease_epoch"`
	Key         string                         `json:"key"`
	ExpectedSeq int64                          `json:"expected_seq"`
	KV          []journal.KVMutation           `json:"kv"`
}

type injectorIntent struct {
	SessionID  string                   `json:"session_id"`
	Epoch      int64                    `json:"lease_epoch"`
	Transition journal.IntentTransition `json:"transition"`
}

func injectorWriteBody(w http.ResponseWriter, r *http.Request, path, session string, exec Execution) ([]byte, bool) {
	switch path {
	case session + "/vm:claim":
		var claim journal.VMTurnClaimRequest
		body, ok := injectorJSON(w, r, &claim, 0)
		return body, ok && claim.ExecutionID == exec.ID && claim.Generation == exec.Generation
	case session + ":heartbeat":
		var heartbeat injectorHeartbeat
		body, ok := injectorJSON(w, r, &heartbeat, 0)
		return body, ok && !heartbeat.Release && heartbeat.Status == ""
	case session + "/events:append":
		var appendReq injectorAppend
		body, ok := injectorJSON(w, r, &appendReq, 3<<20)
		return body, ok && injectorSessionKV(appendReq.KV)
	case session + "/checkpoints":
		var checkpoint struct {
			Epoch int64 `json:"lease_epoch"`
			journal.CheckpointCommit
		}
		return injectorJSON(w, r, &checkpoint, 0)
	case session + "/blobs:stage":
		var stage struct {
			Epoch int64  `json:"lease_epoch"`
			Data  []byte `json:"data"`
		}
		return injectorJSON(w, r, &stage, 6<<20)
	case session + "/blobs:read":
		return injectorJSON(w, r, &journal.BlobRef{}, 0)
	case session + "/tools:begin-standalone":
		var tool struct {
			Epoch int64 `json:"lease_epoch"`
			journal.StandaloneToolRequest
		}
		return injectorJSON(w, r, &tool, 0)
	case "/v1/state/kv:mutate":
		var kv injectorKV
		body, ok := injectorJSON(w, r, &kv, 3<<20)
		return body, ok && kv.SessionID == exec.SessionID && injectorSessionKV(kv.KV)
	case "/v1/state/aux:mutate", "/v1/state/intents:mutate":
		var intent injectorIntent
		body, ok := injectorJSON(w, r, &intent, 0)
		return body, ok && intent.SessionID == exec.SessionID
	}
	if rest, ok := strings.CutPrefix(path, session+"/checkpoints/"); ok {
		if checkpoint, ok := strings.CutSuffix(rest, "/writes"); ok && injectorCheckpointSegment(checkpoint) {
			var writes struct {
				TurnSeq   int64                  `json:"turn_seq,omitempty"`
				Epoch     int64                  `json:"lease_epoch"`
				Namespace string                 `json:"namespace"`
				Writes    []journal.PendingWrite `json:"writes"`
			}
			return injectorJSON(w, r, &writes, 0)
		}
	}
	return nil, false
}

func injectorCheckpointSegment(segment string) bool {
	part, err := url.PathUnescape(segment)
	return err == nil && part != "" &&
		!strings.ContainsFunc(part, func(r rune) bool { return r == '/' || r < ' ' || r == 127 }) &&
		url.PathEscape(part) == segment
}

func injectorSessionKV(mutations []journal.KVMutation) bool {
	for _, mutation := range mutations {
		if mutation.ScopeType != "session" {
			return false
		}
	}
	return true
}

// ReadJSON applies the State API's byte and duplicate-key limits. Checking the
// outer wire names also excludes the standard decoder's case-folded aliases
// for session, generation and release selectors.
func injectorJSON(w http.ResponseWriter, r *http.Request, request any, maxBytes int64) ([]byte, bool) {
	var raw bytes.Buffer
	original := r.Body
	r.Body = struct {
		io.Reader
		io.Closer
	}{io.TeeReader(original, &raw), original}
	var fields map[string]json.RawMessage
	if err := transport.ReadJSON(w, r, &fields, maxBytes); err != nil || fields == nil {
		return nil, false
	}
	for field := range fields {
		if strings.ToLower(field) != field {
			return nil, false
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(request); err != nil {
		return nil, false
	}
	return raw.Bytes(), true
}

func (i *Injector) forward(attempt *injectorAttempt, w http.ResponseWriter, r *http.Request, query url.Values, body []byte) {
	ctx := r.Context()
	token, err := i.attemptToken(ctx, attempt)
	abortCanceledInjectorRequest(ctx)
	if err != nil {
		transport.WriteError(w, r, unavailable("injector task credentials unavailable"))
		return
	}
	client, err := i.clientFor(ctx, attempt.guest.Execution.Scope)
	abortCanceledInjectorRequest(ctx)
	if err != nil || client == nil {
		transport.WriteError(w, r, unavailable("injector State API client unavailable"))
		return
	}
	// The fixed State origin and fresh request keep guest headers, cookies,
	// hop-by-hop fields and URL authority out of the trusted request.
	destination := *i.stateURL
	destination.Path = r.URL.Path
	destination.RawPath = injectorStatePath(r.URL.EscapedPath())
	destination.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, r.Method, destination.String(), bytes.NewReader(body))
	if err != nil {
		injectorForbidden(w, r)
		return
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	outbound := *client
	outbound.Jar = nil
	outbound.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := outbound.Do(request)
	if err != nil {
		abortCanceledInjectorRequest(ctx)
		transport.WriteError(w, r, unavailable("injector State API request failed"))
		return
	}
	defer response.Body.Close()
	abortCanceledInjectorRequest(ctx)
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		transport.WriteError(w, r, unavailable("injector State API returned an unexpected redirect"))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if contentType := response.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(response.StatusCode)
	_, _ = io.Copy(w, response.Body)
}

// Abort the HTTP stream if fencing canceled it while it was in flight.
func abortCanceledInjectorRequest(ctx context.Context) {
	if ctx.Err() != nil {
		panic(http.ErrAbortHandler)
	}
}

// ServeMux cleans literal dot segments. Escaping allowed dot-only session or
// checkpoint IDs lets the upstream mux decode the exact value.
func injectorStatePath(path string) string {
	if !strings.Contains(path, "/.") {
		return path
	}
	segments := strings.Split(path, "/")
	for index, segment := range segments {
		if segment == "." || segment == ".." {
			segments[index] = strings.ReplaceAll(segment, ".", "%2E")
		}
	}
	return strings.Join(segments, "/")
}

func (i *Injector) attemptToken(ctx context.Context, attempt *injectorAttempt) (string, error) {
	select {
	case attempt.tokenGate <- struct{}{}:
		defer func() { <-attempt.tokenGate }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if time.Until(attempt.expires) > 30*time.Second {
		return attempt.token, nil
	}
	token, err := i.mintToken(ctx, attempt.guest.Execution)
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	expiry, err := injectorTokenExpiry(token)
	if err != nil {
		return "", err
	}
	attempt.token, attempt.expires = token, expiry
	return token, nil
}

// This expiry is advisory for refreshing a token from the trusted minter.
// The State API verifies the signature, claims and live grant.
func injectorTokenExpiry(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return time.Time{}, errors.New("malformed injector task token")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, err
	}
	var claims struct {
		Expiry int64 `json:"exp"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		return time.Time{}, err
	}
	expiry := time.Unix(claims.Expiry, 0)
	if time.Until(expiry) <= 0 {
		return time.Time{}, errors.New("injector task token expired")
	}
	return expiry, nil
}
