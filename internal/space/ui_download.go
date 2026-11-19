package space

// The human edge gets exactly one download route, and this file is that route.
//
// It exists because the decision for PDF rev3.2 рис. 7 ("скачивание через прокси:
// текущие права, точная версия", decisions/2026-10-31-artifacts-download-grant-contract.md)
// puts a person's file behind two facts that cannot be traded away:
//
//   - the rights check is Artifacts', made at the moment the grant is ISSUED, from the
//     verified workload principal and its conversation gate and label ceiling;
//   - the browser never holds a workload credential, so the bytes have to travel through
//     a process that does - Space - and that process is a pipe, not a second
//     implementation of the download rules.
//
// So this handler is deliberately boring: it checks the SHAPE of the request (an exact
// version is mandatory; "latest" is not a version), exchanges nothing itself (the human
// JWT was already turned into a scoped task token by ExchangeMiddleware - the same
// boundary that keeps a browser token out of State), calls DownloadGrant and
// OpenDownloadGrant with Space's OWN workload mTLS client, and streams the answer.
//
// What it never does: cache (every response is `no-store`), buffer a whole artifact in
// memory, write an artifact, redirect the browser, hand out a presigned URL, forward the
// human JWT, or re-classify an Artifacts refusal. A refusal reaches the browser with the
// code AND the reason Artifacts gave - grant_expired and grant_used stay two different
// answers, and neither one collapses into a 500.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	// UIDownloadPath is the browser-visible route, frozen in
	// forum-outbox/20261031T181611Z-space-ui-download-route-freeze.json. as-web codes
	// against this name; changing it is a contract change, not a refactor.
	UIDownloadPath = "/u-ahp/v1/artifacts/download"

	// downloadGrantOperation and openDownloadGrantOperation are the Artifacts machine
	// routes (internal/artifactsapi: POST /v1/artifacts/{Operation}).
	downloadGrantOperation           = "DownloadGrant"
	openDownloadGrantOperation       = "OpenDownloadGrant"
	artifactsOperationPath           = "/v1/artifacts/"
	maxGrantBodyBytes          int64 = 1 << 20
	maxArtifactIDLen                 = 256
)

// Reasons this surface owns. They are SPACE's refusals - a refusal that comes back from
// Artifacts keeps Artifacts' own reason (grant_expired, grant_used, artifact_deleted,
// artifact_legal_hold, ...) unchanged, so a client reads one vocabulary for one event.
const (
	ReasonExactVersionRequired = "exact_version_required"
	ReasonArtifactIDRequired   = "artifact_id_required"
)

// DownloadProxyConfig wires the pipe. Both the URL and the client are mandatory: a
// deployment that cannot present its own workload certificate must not serve this route
// at all, because the alternative is answering a browser with something weaker.
type DownloadProxyConfig struct {
	// ArtifactsURL is the Artifacts machine surface (never a browser-reachable value).
	ArtifactsURL string
	// Client presents Space's OWN workload certificate. The browser's credential is
	// never used here, and never travels on this hop.
	Client *http.Client
	// GrantTTL asks Artifacts for a specific grant lifetime; zero takes Artifacts'
	// default. Artifacts clamps to its ceiling and refuses below its floor.
	GrantTTL time.Duration
	// MintTimeout bounds only the grant-issuing hop. The byte stream is bounded by the
	// caller's own request context, because a client downloading a large file over a
	// slow link is not an error.
	MintTimeout time.Duration
}

// downloadProxy is the http.Handler for UIDownloadPath.
type downloadProxy struct {
	base        string
	client      *http.Client
	grantTTL    time.Duration
	mintTimeout time.Duration
}

// NewDownloadProxy builds the human-edge download handler.
// artifactsOrigin validates the configured Artifacts origin and returns it without a
// trailing slash.
//
// A base that carries the API path is refused at construction instead of being carried
// along: this proxy appends /v1/artifacts/{Operation} itself, so an origin configured as
// ".../v1/artifacts" asks for ".../v1/artifacts/v1/artifacts/Resolve". Measured on the
// acceptance pair at 07:07Z with AS_ARTIFACTS_URL=https://127.0.0.1:29612/v1/artifacts:
// the human resolver answered NOT_FOUND "artifactsapi: unknown route", which is what an
// unreachable plane sounds like when it is dressed up as a missing artifact. An operator
// misconfiguration is allowed to fail the mount, loudly, at startup; it is not allowed to
// impersonate a user-facing "not found".
func artifactsOrigin(what, raw string) (string, error) {
	if raw == "" {
		return "", contracts.NewError(contracts.ErrInvalid, "space: "+what+" needs the Artifacts URL")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", contracts.NewError(contracts.ErrInvalid, "space: the Artifacts URL must be an absolute http(s) URL")
	}
	if strings.Trim(u.Path, "/") != "" {
		return "", contracts.NewError(contracts.ErrInvalid,
			"space: the Artifacts URL must name the origin only; this proxy adds the /v1/artifacts path itself")
	}
	return strings.TrimRight(raw, "/"), nil
}

func NewDownloadProxy(cfg DownloadProxyConfig) (http.Handler, error) {
	base, err := artifactsOrigin("the download proxy", cfg.ArtifactsURL)
	if err != nil {
		return nil, err
	}
	if cfg.Client == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: the download proxy needs Space's own workload mTLS client")
	}
	if cfg.MintTimeout <= 0 {
		cfg.MintTimeout = 10 * time.Second
	}
	client := *cfg.Client
	// Deliberately no client.Timeout: it would also bound reading the response BODY,
	// turning a legitimately slow large download into a spurious failure.
	return &downloadProxy{
		base:        base,
		client:      &client,
		grantTTL:    cfg.GrantTTL,
		mintTimeout: cfg.MintTimeout,
	}, nil
}

// grantMintRequest is the Artifacts DownloadGrant body. Field names are the wire
// contract and the server rejects unknown fields, so nothing optional is ever sent empty.
type grantMintRequest struct {
	ID         string `json:"id"`
	Version    string `json:"version"`
	TTLSeconds int64  `json:"ttl_seconds,omitempty"`
}

// grantOpenRequest is the OpenDownloadGrant body.
type grantOpenRequest struct {
	Grant string `json:"grant"`
}

// issuedGrant is what Artifacts answers with. Decoding is tolerant of extra fields on
// purpose: a peer adding a field must not break a person's download.
type issuedGrant struct {
	Grant      string `json:"grant"`
	ArtifactID string `json:"artifact_id"`
	Version    string `json:"version"`
	SHA256     string `json:"sha256"`
	Size       int64  `json:"size"`
	Recipient  string `json:"recipient"`
	OneTime    bool   `json:"one_time"`
	UsesMax    int    `json:"uses_max"`
}

func (p *downloadProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "space: unknown route"))
		return
	}
	id, version, err := parseDownloadQuery(r.URL.Query())
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// The scoped task token is the ONLY authority this hop may carry. Its absence means
	// the request never completed the human->task exchange, and an unexchanged browser
	// session has no right to reach a machine surface.
	token, ok := ScopedTaskFromContext(r.Context())
	if !ok || token == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"space: artifact download needs an exchanged task token (no human JWT is forwarded to Artifacts)"))
		return
	}

	grant, err := p.mint(r.Context(), token, id, version)
	if err != nil {
		writeRefusal(w, r, err)
		return
	}

	req, err := json.Marshal(grantOpenRequest{Grant: grant.Grant})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	open, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		p.base+artifactsOperationPath+openDownloadGrantOperation, bytes.NewReader(req))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	setServiceRequest(open, token, "application/json")
	// Range is proxied, not reinterpreted: the same bytes the machine surface would
	// answer for that range are the bytes the browser gets.
	if rng := r.Header.Get("Range"); rng != "" {
		open.Header.Set("Range", rng)
	}
	resp, err := p.client.Do(open)
	if err != nil {
		// A transport failure has no verdict in it. Inventing a client-visible code here
		// would be a guess, so it stays unclassified and answers 500 honestly.
		transport.WriteError(w, r, contracts.NewError("", "space: artifact stream could not be opened"))
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		writeRefusal(w, r, artifactsRefusal(resp))
		return
	}

	// The proxy is a pipe: identity headers of the exact version pass through, and
	// nothing is stored on this side of the hop.
	copyStreamHeaders(w, resp, id)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// mint asks Artifacts for one grant for this exact version. Every current right is
// consulted there and now; this function only transports the answer.
func (p *downloadProxy) mint(ctx context.Context, token, id, version string) (issuedGrant, error) {
	body, err := json.Marshal(grantMintRequest{
		ID: id, Version: version,
		TTLSeconds: int64(p.grantTTL / time.Second),
	})
	if err != nil {
		return issuedGrant{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.mintTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.base+artifactsOperationPath+downloadGrantOperation, bytes.NewReader(body))
	if err != nil {
		return issuedGrant{}, err
	}
	setServiceRequest(req, token, "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return issuedGrant{}, contracts.NewError("", "space: grant issue could not be reached")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return issuedGrant{}, artifactsRefusal(resp)
	}
	var grant issuedGrant
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxGrantBodyBytes)).Decode(&grant); err != nil {
		return issuedGrant{}, contracts.NewError("", "space: grant answer could not be read")
	}
	if grant.Grant == "" {
		return issuedGrant{}, contracts.NewError("", "space: Artifacts answered without a grant")
	}
	return grant, nil
}

// parseDownloadQuery enforces the frozen shape: both parameters, and an EXACT version.
func parseDownloadQuery(q url.Values) (string, string, error) {
	id := strings.TrimSpace(q.Get("artifact_id"))
	if id == "" || len(id) > maxArtifactIDLen {
		return "", "", contracts.NewReasoned(contracts.ErrInvalid, ReasonArtifactIDRequired,
			"space: artifact_id is required")
	}
	version := strings.TrimSpace(q.Get("version"))
	// "latest" is refused rather than resolved: the contract is the exact sealed version,
	// and silently choosing a newer one would download bytes the caller never named.
	if version == "" || strings.EqualFold(version, "latest") {
		return "", "", contracts.NewReasoned(contracts.ErrInvalid, ReasonExactVersionRequired,
			"space: version must name one exact sealed version (latest is not a version)")
	}
	return id, version, nil
}

// setServiceRequest builds the machine-surface request shape: Space's own bearer, JSON.
func setServiceRequest(req *http.Request, token, contentType string) {
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")
}

// upstreamRefusal is a refusal Artifacts itself stated, kept in the form it arrived in.
//
// It exists because transport.WriteError can only render the canonical contracts codes, and
// Artifacts names some refusals outside that vocabulary - "range_not_satisfiable" among them.
// Before this type existed, such a verdict fell through WriteError's default branch and the
// browser was told INTERNAL/internal server error: a user retrying a truncated download with a
// tail Range was told their own byte range was OUR failure, and no client could tell a
// permanent refusal from a transient one. Measured on the live pin: the machine surface answers
// 416 for bytes=100-200, the browser surface behind this proxy answered 500.
type upstreamRefusal struct {
	status       int
	code         string // exactly the name Artifacts gave; "" when it sent no envelope
	message      string
	reason       string
	body         []byte // the envelope verbatim, so nothing is re-worded on the way through
	contentRange string
}

func (e *upstreamRefusal) Error() string {
	if e.code != "" {
		return "space: Artifacts refused the download: " + e.code
	}
	return fmt.Sprintf("space: Artifacts refused the download with HTTP %d and no error envelope", e.status)
}

// artifactsRefusal turns a non-2xx Artifacts answer into the error Artifacts actually
// gave. A refusal already named in the contracts vocabulary becomes an ordinary contracts
// error, so transport.WriteError answers its code and its reason exactly as before; anything
// else becomes an *upstreamRefusal, which writeRefusal answers with the status Artifacts
// chose instead of collapsing it to INTERNAL.
func artifactsRefusal(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxGrantBodyBytes))
	var env struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Reason  string `json:"reason"`
	}
	decoded := json.Unmarshal(raw, &env) == nil && env.Code != ""
	if decoded && canonicalContractCode(env.Code) {
		msg := env.Message
		if msg == "" {
			msg = "artifacts refused the download"
		}
		if env.Reason != "" {
			return contracts.NewReasoned(env.Code, env.Reason, msg)
		}
		return contracts.NewError(env.Code, msg)
	}
	refusal := &upstreamRefusal{
		status:       resp.StatusCode,
		contentRange: resp.Header.Get("Content-Range"),
		body:         raw,
	}
	if decoded {
		refusal.code, refusal.message, refusal.reason = env.Code, env.Message, env.Reason
		return refusal
	}
	if code := codeForStatus(resp.StatusCode); code != "" {
		return contracts.NewError(code, "space: Artifacts refused the download without an error envelope")
	}
	return refusal
}

// canonicalContractCode reports whether transport.WriteError can render this code itself.
// The list mirrors its switch: keeping the two in sync is what guarantees that the 400/403/404
// refusals already measured on this surface keep their exact body and status.
func canonicalContractCode(code string) bool {
	switch code {
	case contracts.ErrInvalid, contracts.ErrKeyReuse, contracts.ErrForbidden,
		contracts.ErrNotFound, contracts.ErrConflict, contracts.ErrStaleEpoch,
		contracts.ErrLeaseLost,
		contracts.ErrUnavailable:
		// ErrUnavailable belongs here because transport.WriteError answers it (503). It was
		// missing while the switch had the case, so a dependency's own `503 UNAVAILABLE`
		// envelope -- Registry refusing PinDotConfig, measured 714 times on the live density
		// stand -- fell through to *upstreamRefusal, whose code no AcceptTurn renderer knows,
		// and the caller was told `500 INTERNAL / internal server error`. Refusing stays
		// refusing: the code and the upstream's sentence travel on, nothing becomes a success.
		return true
	default:
		return false
	}
}

// writeRefusal answers a download refusal. Canonical contracts errors go to
// transport.WriteError unchanged; a stated-but-unrenderable upstream verdict is answered with
// the upstream's own status, its own envelope and its Content-Range, because "Range is proxied,
// not reinterpreted" has to hold for refusals too or the proxy becomes the thing that lies.
// A refusal with no envelope at all still carries the upstream status, which is a real verdict;
// its code says unclassified rather than pretending to be an internal error.
func writeRefusal(w http.ResponseWriter, r *http.Request, err error) {
	var up *upstreamRefusal
	if !errors.As(err, &up) {
		transport.WriteError(w, r, err)
		return
	}
	if up.contentRange != "" {
		w.Header().Set("Content-Range", up.contentRange)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(up.status)
	if len(up.body) > 0 && json.Valid(up.body) {
		_, _ = w.Write(up.body)
		return
	}
	message := up.message
	if message == "" {
		message = fmt.Sprintf("space: Artifacts refused the download with HTTP %d and no error envelope", up.status)
	}
	code := up.code
	if code == "" {
		code = "UNCLASSIFIED_REFUSAL"
	}
	_ = json.NewEncoder(w).Encode(&contracts.Error{Code: code, Message: message})
}

// copyStreamHeaders forwards the identity of the exact version and states, loudly, that
// nothing here may be cached: a grant is use-counted and a revoked artifact must not keep
// living in an intermediary because of a stale copy.
func copyStreamHeaders(w http.ResponseWriter, resp *http.Response, artifactID string) {
	for _, h := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges", "ETag"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition", "attachment; filename="+sanitizeFilename(artifactID))
}

// sanitizeFilename derives a download name from the server-side artifact id. It never
// reflects a caller-supplied string into a response header.
func sanitizeFilename(id string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		if b.Len() > 128 {
			break
		}
	}
	b.WriteString(`"`)
	return b.String()
}
