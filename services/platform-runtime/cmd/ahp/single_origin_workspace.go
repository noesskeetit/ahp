package main

// The Workspace face reached through the single user edge.
//
// Why this file exists, measured on the acceptance deployment (14:50Z-14:57Z):
// the edge forwards to as-runner; as-runner owns /v1/space/* and the UI artifact
// download (both answer 403 unauthenticated) but answers /v1/workspace/GetHead
// and /v1/artifacts/BeginUpload with the same 404 it gives /v1/bogus. Those
// faces are mounted on the composed aggregate, which guards them with
// transport.Task: a verified workload certificate AND a KindTask token. So
// forwarding the prefix to the runner reaches a face that is not there, and
// pointing this edge's proxy at the aggregate cannot work either -- the proxy
// has no workload certificate (pinned by
// TestSingleOriginCannotFrontAnMTLSWorkspaceFace: 502, the request is never sent).
//
// The contract already names the mechanism for this boundary: the control
// exchange, with its own exchanger instance and authority prefix per boundary
// (capabilities=["workspace"]). A caller holding a workload certificate turns its
// own KindHuman token into a scoped KindTask token; the scope -- and therefore
// the workspace membership and tier checks inside internal/workspace -- stay the
// caller's, so nothing is elevated here.
//
// What this deliberately is NOT: it is not a second Data instance on the human
// edge (tools/dir-human-edge-planes.sh refuses that, correctly) and it invents no
// route -- the operation set is exactly the one internal/workspace/workspaceapi
// already serves, forwarded byte for byte with the upstream's status, headers and
// body, so a 403 from the authorization gate arrives as a 403 and the leg measures
// the gate instead of the proxy.
//
// It is OFF unless AHP_WORKSPACE_ORIGIN names an origin, and it refuses to
// half-wire: with any piece missing the prefix keeps its plain forwarding and the
// reason is logged, so the edge never starts minting tokens behind an operator's
// back.

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	envWorkspaceOrigin     = "AHP_WORKSPACE_ORIGIN"
	envWorkspaceExchange   = "AHP_WORKSPACE_EXCHANGE_URL"
	envWorkspaceClientCert = "AHP_WORKSPACE_CLIENT_CERT"
	envWorkspaceClientKey  = "AHP_WORKSPACE_CLIENT_KEY"
	envWorkspaceCA         = "AHP_WORKSPACE_CA"
	// envWorkspaceUserPassthrough selects the shape the director ruled at
	// 20261101T154234Z item 2 (erratum 20261101T154241Z): the edge presents its OWN
	// workload certificate and does NOT rewrite Authorization, so the user's own
	// credential arrives at the data plane, which is the plane contracted to verify
	// it. Unset keeps today's behavior -- exchange for a narrowed child token and
	// forward that -- which is the shape measured green since 15:40Z.
	envWorkspaceUserPassthrough = "AHP_WORKSPACE_USER_PASSTHROUGH"
	// The two authority prefixes this edge exchanges under. The contract freezes
	// the workspace boundary to capabilities=["workspace"], and the artifact face
	// declares its own capability ("artifacts", the one cmd/ahp/role_data.go
	// re-checks on that route). They are kept APART on purpose: one exchange
	// asking for both would create a single merged authority root, which the
	// contract rejects, and the durable ceiling of a workspace upload would then
	// also carry artifact authority. Distinct task names mean distinct roots in
	// the authority tree, so each boundary can be revoked on its own.
	workspaceExchangeTask = "workspace-upload"
	workspaceExchangeCap  = "workspace"
	artifactUploadTask    = "artifact-upload"
	// Upload requires the same capability that the artifact owner verifies.
	// A read-only artifact permission cannot authorize BeginUpload or Seal.
	artifactUploadCap = "artifacts"
	// headerWorkspaceContext lets a caller pin the exchange context (the exchange
	// binds a context id to one scope and refuses it from another, measured
	// 14:46Z). Absent means the edge derives one, per subject, from the caller's
	// own token, so two users can never collide on a context id.
	headerWorkspaceContext = "X-AHP-Context"
)

type workspaceEdge struct {
	origin      *url.URL
	exchangeURL string
	client      *http.Client
	logger      *slog.Logger
	// passthrough forwards the caller's credential unchanged on the workspace
	// boundary. It is per-boundary on purpose: the artifact face answers a user
	// credential 403 "token audience does not match" (measured 20261101T155238Z), so
	// forwarding the bearer there would break an upload path that is green today.
	passthrough bool
}

// exchangeBoundary is one authority prefix: a task name, the single capability it
// is contracted for, and the tag that makes its exchange context distinct.
type exchangeBoundary struct {
	name       string
	task       string
	capability string
	contextTag string
}

var (
	boundaryWorkspace      = exchangeBoundary{"workspace", workspaceExchangeTask, workspaceExchangeCap, "ws"}
	boundaryArtifactUpload = exchangeBoundary{"artifact-upload", artifactUploadTask, artifactUploadCap, "art"}
)

// forBoundary mounts this edge for exactly one boundary, so no request can be
// served under the other boundary's authority.
func (e *workspaceEdge) forBoundary(b exchangeBoundary) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { e.serve(w, r, b) })
}

// newWorkspaceEdge returns nil when the mode is off or half-wired. Every refusal
// is logged with its reason: the failure this file is designed against is an edge
// that appears to serve uploads while the pieces behind it are missing.
func newWorkspaceEdge(logger *slog.Logger) *workspaceEdge {
	raw := strings.TrimSpace(envOr(envWorkspaceOrigin, ""))
	if raw == "" {
		return nil
	}
	origin, err := url.Parse(raw)
	if err != nil || origin.Host == "" || (origin.Scheme != "http" && origin.Scheme != "https") ||
		(origin.Path != "" && origin.Path != "/") {
		logger.Warn("workspace edge NOT mounted: AHP_WORKSPACE_ORIGIN must be an origin without a path", "value", raw)
		return nil
	}
	exchange := strings.TrimSpace(envOr(envWorkspaceExchange, ""))
	if exchange == "" {
		logger.Warn("workspace edge NOT mounted: AHP_WORKSPACE_EXCHANGE_URL is required, refusing to forward a prefix whose face needs a task token this edge cannot obtain")
		return nil
	}
	tlsConf := &tls.Config{}
	if origin.Scheme == "https" {
		// An https origin is the mTLS aggregate: without a client certificate this
		// edge is not a workload, so forwarding would deliver 403s that read like
		// authorization failures. Refuse instead.
		certFile, keyFile, caFile := envOr(envWorkspaceClientCert, ""), envOr(envWorkspaceClientKey, ""), envOr(envWorkspaceCA, "")
		if certFile == "" || keyFile == "" || caFile == "" {
			logger.Warn("workspace edge NOT mounted: an https workspace origin needs AHP_WORKSPACE_CLIENT_CERT, AHP_WORKSPACE_CLIENT_KEY and AHP_WORKSPACE_CA")
			return nil
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			logger.Warn("workspace edge NOT mounted: cannot load the workload certificate", "err", err)
			return nil
		}
		pem, err := os.ReadFile(caFile)
		if err != nil {
			logger.Warn("workspace edge NOT mounted: cannot read the workspace CA", "err", err)
			return nil
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			logger.Warn("workspace edge NOT mounted: the workspace CA file holds no certificate")
			return nil
		}
		tlsConf = &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, ServerName: origin.Hostname()}
	}
	passthrough := strings.EqualFold(strings.TrimSpace(envOr(envWorkspaceUserPassthrough, "")), "1")
	if passthrough && origin.Scheme != "https" {
		// This mode puts the caller's own credential on the wire. Refusing beats
		// mounting a face that ships a session in clear text and looks, to the
		// browser, exactly like a working upload route.
		logger.Warn("workspace edge NOT mounted: " + envWorkspaceUserPassthrough +
			" forwards the caller's own credential and therefore requires an https workspace origin")
		return nil
	}
	if passthrough {
		logger.Info("single-origin edge: the Workspace prefix forwards the caller's own credential to the data plane, which verifies it; the artifact upload boundary still exchanges for a narrowed child token",
			"origin", origin.Host, "mode", "user-passthrough")
	}
	return &workspaceEdge{
		origin:      origin,
		exchangeURL: exchange,
		client:      &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConf}, Timeout: 30 * time.Second},
		logger:      logger,
		passthrough: passthrough,
	}
}

// contextFor names the exchange context for this caller and boundary when the
// caller did not pin one. It must be STABLE per (subject, boundary) and distinct
// across subjects, and neither requirement is decoration:
//
//   - The exchange binds a context id to one scope and refuses it from another
//     (measured 14:46Z), so one shared constant would make every user after the
//     first fail inside the boundary.
//   - Hashing the bearer, which is what this file did first, is wrong the other
//     way: a login mints a new token every time, so the same human gets a new
//     context per session while the child task id is derived from the root and the
//     effective capability set. Measured on a lane deployment at 15:24Z, the second
//     login therefore answered 400 KEY_REUSE_MISMATCH "child task id was already
//     used with a different canonical request or parent" -- the authority tree
//     refusing to re-delegate one child under two contexts.
//
// So the label is derived from the token's own tenant and subject claims. It is a
// LABEL, not an identity decision: the exchange re-verifies the token and derives
// the scope from that verification, so a caller that pins X-AHP-Context, or a token
// whose claims cannot be read, can never widen its own scope -- the worst case is a
// refusal. Unreadable claims fall back to hashing the token, which keeps the
// fail-closed behaviour (a stable-per-session label) instead of guessing.
func (b exchangeBoundary) contextFor(human string) string {
	tenant, subject, ok := tokenScopeLabel(human)
	if !ok {
		sum := sha256.Sum256([]byte(human))
		return b.contextTag + "-t" + hex.EncodeToString(sum[:])[:16]
	}
	sum := sha256.Sum256([]byte(tenant + "\x00" + subject))
	return b.contextTag + "-" + hex.EncodeToString(sum[:])[:16]
}

// tokenScopeLabel reads the tenant and subject claims out of a JWT payload without
// verifying anything about it. See contextFor: the value is a label, and the
// identity service re-derives the real scope from the signature it does check.
func tokenScopeLabel(token string) (tenant, subject string, ok bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", false
	}
	var claims struct {
		Tenant  string `json:"tenant"`
		Subject string `json:"sub"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil || claims.Subject == "" {
		return "", "", false
	}
	return claims.Tenant, claims.Subject, true
}

// exchangeRequest is the control-exchange body: the caller's own human token, the
// boundary's own task prefix, and exactly one capability. Capabilities is a slice
// so a request for two of them cannot be expressed by this edge at all.
type exchangeRequest struct {
	HumanToken   string   `json:"human_token"`
	Task         string   `json:"task"`
	Context      string   `json:"context"`
	Capabilities []string `json:"capabilities"`
}

// exchangeResponse also names the task the returned token is ACTUALLY bound to:
// a narrowed grant is minted under a child task, not the one sent.
type exchangeResponse struct {
	Token string `json:"token"`
	Task  string `json:"task"`
}

func (e *workspaceEdge) serve(w http.ResponseWriter, r *http.Request, boundary exchangeBoundary) {
	if e.passthrough && boundary == boundaryWorkspace {
		e.forwardUntouched(w, r)
		return
	}
	human := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	ctx := r.Header.Get(headerWorkspaceContext)
	if ctx == "" {
		ctx = boundary.contextFor(human)
	}
	payload, _ := json.Marshal(exchangeRequest{
		HumanToken:   human,
		Task:         boundary.task,
		Context:      ctx,
		Capabilities: []string{boundary.capability},
	})
	exReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, e.exchangeURL, bytes.NewReader(payload))
	if err != nil {
		e.unavailable(w)
		return
	}
	exReq.Header.Set("Content-Type", "application/json")
	exResp, err := e.client.Do(exReq)
	if err != nil {
		e.logger.Warn("workspace edge: control exchange unreachable", "err", err)
		e.unavailable(w)
		return
	}
	exBody, _ := io.ReadAll(exResp.Body)
	exResp.Body.Close()
	if exResp.StatusCode != http.StatusOK {
		// The exchange's own verdict travels unchanged: an unauthenticated or
		// unauthorized caller must see the identity plane's answer, not one this
		// edge invented.
		copyHeaders(w.Header(), exResp.Header)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(exResp.StatusCode)
		_, _ = w.Write(exBody)
		return
	}
	var minted exchangeResponse
	if err := json.Unmarshal(exBody, &minted); err != nil || minted.Token == "" {
		e.logger.Warn("workspace edge: the exchange answered 200 without a token")
		e.unavailable(w)
		return
	}

	if minted.Task != "" && minted.Task != boundary.task {
		e.logger.Info("workspace edge: grant narrowed to a child task",
			"boundary", boundary.name, "requested_task", boundary.task, "minted_task", minted.Task)
	}

	target := e.origin.Scheme + "://" + e.origin.Host + r.URL.RequestURI()
	out, err := http.NewRequestWithContext(r.Context(), r.Method, target, r.Body)
	if err != nil {
		e.unavailable(w)
		return
	}
	out.Header = r.Header.Clone()
	out.Header.Set("Authorization", "Bearer "+minted.Token)
	// The caller's session cookie is not this caller's identity at the aggregate
	// (the task token and the workload certificate are), so it does not travel.
	out.Header.Del("Cookie")
	out.Header.Del(headerWorkspaceContext)
	resp, err := e.client.Do(out)
	if err != nil {
		e.logger.Warn("workspace edge: workspace origin unreachable", "path", r.URL.Path, "err", err)
		e.unavailable(w)
		return
	}
	defer resp.Body.Close()
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// forwardUntouched is the ruled shape: same method, path, query and body, the
// caller's Authorization header exactly as it arrived, and the edge's own workload
// certificate on the connection (the client was built with it). Nothing here
// decides who the caller is; the data plane verifies the credential it receives
// and its store re-checks membership and tier per call, so the plane's verdict --
// 200 or 403 -- is carried back verbatim rather than re-narrated by this edge.
func (e *workspaceEdge) forwardUntouched(w http.ResponseWriter, r *http.Request) {
	target := e.origin.Scheme + "://" + e.origin.Host + r.URL.RequestURI()
	out, err := http.NewRequestWithContext(r.Context(), r.Method, target, r.Body)
	if err != nil {
		e.unavailable(w)
		return
	}
	out.Header = r.Header.Clone()
	// The session cookie identifies a browser session at the identity plane, not a
	// principal at the data plane, and the context header only names an exchange
	// this mode does not perform. Neither is this caller's identity downstream.
	out.Header.Del("Cookie")
	out.Header.Del(headerWorkspaceContext)
	resp, err := e.client.Do(out)
	if err != nil {
		e.logger.Warn("workspace edge: workspace origin unreachable while forwarding the user credential",
			"path", r.URL.Path, "err", err)
		e.unavailable(w)
		return
	}
	defer resp.Body.Close()
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (e *workspaceEdge) unavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadGateway)
	_, _ = w.Write([]byte(`{"code":"unavailable","message":"the workspace origin or its control exchange is not reachable"}` + "\n"))
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}
