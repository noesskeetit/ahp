package main

// DEF-H24-11: name the plane that rejects Space's workload certificate.
//
// Measured before this file existed: pointing AS_WORKLOAD_* at a bundle from another CA
// while AS_STATE_URL stayed on the healthy plane produced AcceptTurn 403s and NO startup
// line that said which plane refused the certificate. The operator could see the symptom
// (every browser run dies) but not the cause (one plane's CA out of four), because the
// only certificate is presented lazily, per hop, and the transport error is swallowed by
// the hop that hits it.
//
// The probe answers that question before any task is accepted: for every plane this
// deployment is configured to reach, it performs one unauthenticated round trip with the
// SAME client the plane hops use, and reports one outcome per plane. It changes no
// authorization and grants nothing: a plane that answers 403 to an anonymous GET is
// reported as reachable, because the TLS handshake -- the thing being diagnosed --
// succeeded. It never makes the deployment fail to start either: a plane that is briefly
// down must not prevent a restart, and every real hop still fails closed on its own.
//
// Nothing about a key is logged. The detail is the transport error's first line, which
// carries the alert or the x509 reason and no key material.

import (
	"context"
	"crypto/x509"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Per-plane probe outcomes. They are enum strings on purpose: an operator greps for the
// plane name and gets one of these, rather than a stack of wrapped transport errors.
const (
	// planeProbePlainScheme is reported for an http:// plane: there is no certificate
	// exchange to diagnose, and saying so is more honest than pretending to probe one.
	planeProbePlainScheme = "not_probed_no_tls"
	planeProbeReachable   = "reachable"
	// planeProbeCAUntrusted is OUR side refusing: the plane's certificate is not signed by
	// the CA bundle this deployment was given (AHP/AS *_CA), i.e. the runner points at a
	// plane from another estate, or its CA bundle is stale.
	planeProbeCAUntrusted = "plane_certificate_not_signed_by_our_CA"
	// planeProbeNameMismatch is our side refusing for a different reason: the plane's
	// certificate does not name the host we dialed. Same family, different remedy.
	planeProbeNameMismatch = "plane_certificate_names_another_host"
	// planeProbeWorkloadRejected is the PLANE refusing the certificate we presented: it was
	// not issued by the CA the plane trusts. This is the DEF-H24-11 case.
	planeProbeWorkloadRejected = "plane_rejected_our_workload_certificate"
	planeProbeTLSFailed        = "tls_handshake_failed"
	planeProbeUnreachable      = "plane_unreachable"
)

// planeProbeTimeout bounds one probe. Startup must not stall behind a black-holed plane.
const planeProbeTimeout = 3 * time.Second

// planeTarget is one configured plane this deployment reaches with its workload certificate.
type planeTarget struct {
	// Name is what the journal and the operator see: state, identity, identity_exchange,
	// artifacts. It must stay stable so a board line can be diffed across restarts.
	Name string
	URL  string
}

// planeProbeResult is the outcome for one plane.
type planeProbeResult struct {
	Plane   string
	URL     string
	Outcome string
	Detail  string
	Status  int
}

// planeTargetsFromConfig names every plane the deployment is configured to reach.
// An unset plane is simply absent: the probe reports what the deployment claims to use,
// and a missing plane is a configuration question, not a certificate question.
func planeTargetsFromConfig(cfg config) []planeTarget {
	targets := make([]planeTarget, 0, 4)
	for _, t := range []planeTarget{
		{Name: "state", URL: strings.TrimSpace(cfg.StateURL)},
		{Name: "identity_exchange", URL: strings.TrimSpace(cfg.IdentityExchangeURL)},
		{Name: "identity", URL: strings.TrimSpace(cfg.IdentityURL)},
		{Name: "artifacts", URL: strings.TrimSpace(cfg.ArtifactsURL)},
	} {
		if t.URL == "" {
			continue
		}
		targets = append(targets, t)
	}
	return targets
}

// classifyProbeError maps a transport failure onto one of the outcome enums.
func classifyProbeError(err error) (outcome, detail string) {
	if err == nil {
		return planeProbeReachable, ""
	}
	var unknownAuthority x509.UnknownAuthorityError
	var invalidCert *x509.CertificateInvalidError
	var hostName x509.HostnameError
	switch {
	case errors.As(err, &unknownAuthority):
		return planeProbeCAUntrusted, firstLine(err.Error())
	case errors.As(err, &invalidCert):
		return planeProbeCAUntrusted, firstLine(err.Error())
	case errors.As(err, &hostName):
		return planeProbeNameMismatch, firstLine(err.Error())
	}
	msg := err.Error()
	// An alert that arrived from the peer is the plane refusing what WE presented (or a
	// TLS-level refusal of the handshake we started). Either way the plane, not the runner,
	// spoke last -- which is exactly the naming the operator needs.
	if strings.Contains(msg, "remote error: tls:") {
		return planeProbeWorkloadRejected, firstLine(msg)
	}
	if strings.Contains(msg, "certificate signed by unknown authority") {
		return planeProbeCAUntrusted, firstLine(msg)
	}
	var netErr net.Error
	var opErr *net.OpError
	if errors.As(err, &netErr) || errors.As(err, &opErr) || errors.Is(err, context.DeadlineExceeded) {
		return planeProbeUnreachable, firstLine(msg)
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return planeProbeUnreachable, firstLine(msg)
	}
	return planeProbeTLSFailed, firstLine(msg)
}

// probePlane performs one anonymous round trip against a plane with the given client.
func probePlane(ctx context.Context, target planeTarget, client *http.Client) planeProbeResult {
	res := planeProbeResult{Plane: target.Name, URL: target.URL}
	if !strings.HasPrefix(strings.ToLower(target.URL), "https://") {
		res.Outcome = planeProbePlainScheme
		res.Detail = "plane is reached over http: there is no workload certificate exchange to diagnose"
		return res
	}
	callCtx, cancel := context.WithTimeout(ctx, planeProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, target.URL, nil)
	if err != nil {
		res.Outcome = planeProbeUnreachable
		res.Detail = firstLine(err.Error())
		return res
	}
	resp, err := client.Do(req)
	if err != nil {
		res.Outcome, res.Detail = classifyProbeError(err)
		return res
	}
	defer func() { _ = resp.Body.Close() }()
	res.Status = resp.StatusCode
	res.Outcome = planeProbeReachable
	res.Detail = "handshake accepted the workload certificate; the anonymous request itself is expected to be refused by authorization"
	return res
}

// probePlanes probes every target with the deployment's own workload client.
func probePlanes(ctx context.Context, targets []planeTarget, client *http.Client) []planeProbeResult {
	out := make([]planeProbeResult, 0, len(targets))
	for _, t := range targets {
		out = append(out, probePlane(ctx, t, client))
	}
	return out
}

// logPlaneProbes writes one line per plane, plus one summary line when anything refused the
// certificate. The summary exists so the journal answers the operator's actual question --
// "is this the certificate?" -- without reading four lines.
func logPlaneProbes(logger *slog.Logger, results []planeProbeResult) {
	if logger == nil || len(results) == 0 {
		return
	}
	var refused []string
	for _, r := range results {
		attrs := []any{"plane", r.Plane, "outcome", r.Outcome}
		if r.Status != 0 {
			attrs = append(attrs, "http_status", r.Status)
		}
		if r.Detail != "" {
			attrs = append(attrs, "detail", r.Detail)
		}
		switch r.Outcome {
		case planeProbeReachable, planeProbePlainScheme:
			logger.Info("as-runner: workload certificate probe", attrs...)
		default:
			logger.Error("as-runner: workload certificate probe", attrs...)
		}
		if r.Outcome == planeProbeWorkloadRejected || r.Outcome == planeProbeCAUntrusted ||
			r.Outcome == planeProbeNameMismatch {
			refused = append(refused, r.Plane+"="+r.Outcome)
		}
	}
	if len(refused) > 0 {
		logger.Error("as-runner: workload certificate is refused by at least one configured plane; every hop to that plane will fail closed",
			"planes", strings.Join(refused, ","))
	}
}

// probeConfiguredPlanes wires the two functions above into startup. It uses the same
// certificate bundle the plane hops use, so what it reports is what a real hop will see.
func probeConfiguredPlanes(logger *slog.Logger, cfg config, client *http.Client) {
	if client == nil {
		return // no workload bundle: the planes already refuse Space loudly, nothing to name
	}
	probeConfiguredPlanesWith(client, logger, planeTargetsFromConfig(cfg))
}

func probeConfiguredPlanesWith(client *http.Client, logger *slog.Logger, targets []planeTarget) {
	logPlaneProbes(logger, probePlanes(context.Background(), targets, client))
}

// firstLine keeps a transport detail to one line, so a journal entry cannot grow with a
// certificate chain or a stack.
func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 240 {
		s = s[:240]
	}
	return s
}
