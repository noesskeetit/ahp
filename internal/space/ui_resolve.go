package space

// The human edge is allowed to translate a digest into something downloadable, and
// nothing else.
//
// Why this route exists: a run's frame names the bytes it produced (sha256), while the
// frozen download contract demands an artifact id plus one exact sealed version
// ("текущие права, точная версия"). Person-facing clients must not guess those two
// values, and the contract must not weaken to accept a digest. So the human edge gets a
// read-only resolver and "download by digest" becomes Resolve → download.
//
// What this file is: the same kind of pipe ui_download.go is. It checks the SHAPE of the
// question, carries the exchanged scoped task token (never the browser's credential) to
// Artifacts' Resolve operation over Space's own workload mTLS, and passes the answer
// through. Rights are decided there, now, from the caller's current grant and labels.
//
// What it never does: accept a tenant or subject from the request, list artifacts,
// return object keys or labels, issue a grant, cache (answers are no-store), or turn
// Artifacts' "not yours" into anything other than Artifacts' answer. Artifacts answers
// the same NOT_FOUND for "no such digest" and "not yours to see", so this route cannot
// become an existence oracle.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	// UIResolvePath is the browser-visible read-only resolver route beside UIDownloadPath.
	UIResolvePath = "/u-ahp/v1/artifacts/Resolve"

	// resolveOperation is Artifacts' machine route this pipe calls.
	resolveOperation = "Resolve"

	// ReasonDigestRequired is this surface's own refusal: a lookup with no digest.
	ReasonDigestRequired = "digest_required"

	maxResolveBodyBytes int64 = 1 << 20
	digestParam               = "sha256"
)

// ResolveProxyConfig wires the resolver. Both fields are mandatory for the same reason
// the download proxy requires them: without its own workload identity this process has
// no business answering about artifacts at all.
type ResolveProxyConfig struct {
	// ArtifactsURL is the Artifacts machine surface (never browser-reachable).
	ArtifactsURL string
	// Client presents Space's OWN workload certificate.
	Client *http.Client
	// CallTimeout bounds the machine hop; a resolver returns a small JSON body, so a
	// per-call timeout is honest here (unlike the byte stream of a download).
	CallTimeout time.Duration
}

type resolveProxy struct {
	base        string
	client      *http.Client
	callTimeout time.Duration
}

// NewResolveProxy builds the human-edge digest resolver.
func NewResolveProxy(cfg ResolveProxyConfig) (http.Handler, error) {
	base, err := artifactsOrigin("the resolver", cfg.ArtifactsURL)
	if err != nil {
		return nil, err
	}
	if cfg.Client == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: the resolver needs Space's own workload mTLS client")
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = 10 * time.Second
	}
	return &resolveProxy{
		base:        base,
		client:      cfg.Client,
		callTimeout: cfg.CallTimeout,
	}, nil
}

type resolveCall struct {
	SHA256 string `json:"sha256"`
}

func (p *resolveProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "space: unknown route"))
		return
	}
	digest := strings.TrimSpace(r.URL.Query().Get(digestParam))
	if digest == "" {
		transport.WriteError(w, r, contracts.NewReasoned(contracts.ErrInvalid, ReasonDigestRequired,
			"space: "+digestParam+" is required"))
		return
	}
	token, ok := ScopedTaskFromContext(r.Context())
	if !ok || token == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
			"space: artifact resolve needs an exchanged task token (no human JWT is forwarded to Artifacts)"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), p.callTimeout)
	defer cancel()

	body, err := json.Marshal(resolveCall{SHA256: digest})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.base+artifactsOperationPath+resolveOperation, bytes.NewReader(body))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	setServiceRequest(req, token, "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		transport.WriteError(w, r, contracts.NewError("", "space: artifact resolve could not be reached"))
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		writeRefusal(w, r, artifactsRefusal(resp))
		return
	}
	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxResolveBodyBytes))
	if err != nil {
		transport.WriteError(w, r, contracts.NewError("", "space: artifact resolve response could not be read"))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}
