package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// ErrPermanent marks a delivery the peer definitively refused (malformed
// target, rejected payload, stale epoch). The runner settles such an attempt
// as failed instead of burning retries on a call that can never succeed.
var ErrPermanent = errors.New("gateway: permanent delivery failure")

// notificationTokenHeader carries PushConfig.Token to the receiver, following
// the A2A push-notification contract.
const notificationTokenHeader = "X-A2A-Notification-Token"

// wroteRequest traces whether a request body actually left the process. A
// transport failure after that point is ambiguous — the peer may have applied
// the notification — and must never be silently retried.
type wroteRequest struct{ written bool }

func (w *wroteRequest) clientTrace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { w.written = true }}
}

// postJSON sends body and classifies the outcome. Transport failures before
// the request was written are retryable; after the write they are ambiguous.
func postJSON(ctx context.Context, client *http.Client, target string, headers map[string]string, body []byte) (int, []byte, error) {
	trace := &wroteRequest{}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace.clientTrace()),
		http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return 0, nil, fmt.Errorf("%w: target %q: %v", ErrPermanent, target, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		if trace.written {
			return 0, nil, fmt.Errorf("%w: transport failed after request was written: %v", ErrAmbiguous, err)
		}
		return 0, nil, fmt.Errorf("gateway: request not sent: %w", err)
	}
	defer resp.Body.Close()
	raw, drainErr := netpolicy.Drain(resp, netpolicy.DefaultMaxResponseBytes)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if drainErr != nil {
			return resp.StatusCode, raw, fmt.Errorf("%w: success status with unreadable body: %v", ErrAmbiguous, drainErr)
		}
		return resp.StatusCode, raw, nil
	}
	return resp.StatusCode, raw, nil
}

// classifyStatus maps an HTTP status to a delivery decision: nil for success,
// ErrPermanent for refusals that replaying cannot fix, a plain error for
// conditions worth retrying (rate limits, server faults).
func classifyStatus(status int, raw []byte, what string) error {
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500:
		return fmt.Errorf("gateway: %s endpoint %d: %s", what, status, truncate(string(raw), 200))
	default:
		return fmt.Errorf("%w: %s endpoint %d: %s", ErrPermanent, what, status, truncate(string(raw), 200))
	}
}

// PushSink delivers terminal task statuses to the notification URLs registered
// for a task. Egress goes through a netpolicy client, so a task can only make
// the gateway POST to hosts its policy allows, and credential headers are
// dropped whenever the origin changes.
//
// A task may hold several push configs. Notifications are at-least-once by
// contract: if one endpoint fails after another succeeded, the whole entry is
// retried and the successful endpoint may see a duplicate delivery.
type PushSink struct {
	store  *Store
	client *http.Client
}

// NewPushSink builds the sink with an egress-gated client. Because loopback is
// denied unconditionally, tests must inject a resolver and dialer.
func NewPushSink(store *Store, policy netpolicy.Policy, opts ...netpolicy.Option) (*PushSink, error) {
	if store == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: push sink needs a store")
	}
	client, err := netpolicy.NewClient(policy, opts...)
	if err != nil {
		return nil, fmt.Errorf("gateway: push client: %w", err)
	}
	return &PushSink{store: store, client: client}, nil
}

type pushPayload struct {
	TaskID        string `json:"taskId"`
	StatusVersion int64  `json:"statusVersion"`
}

// Deliver implements Sink.
func (s *PushSink) Deliver(ctx context.Context, e OutboxEntry) error {
	if e.Kind != OutboxKindPush {
		return fmt.Errorf("%w: push sink received kind %q", ErrPermanent, e.Kind)
	}
	var p pushPayload
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return fmt.Errorf("%w: push payload: %v", ErrPermanent, err)
	}
	if p.TaskID == "" {
		return fmt.Errorf("%w: push payload without taskId", ErrPermanent)
	}
	configs, err := s.store.PushConfigs(ctx, e.Scope, p.TaskID)
	if err != nil {
		return err
	}
	if len(configs) == 0 {
		// Configs were deleted after the entry was queued; nothing to notify.
		return nil
	}
	event, err := s.statusEvent(ctx, e, p.StatusVersion)
	if err != nil {
		return err
	}
	var (
		permanent []error
		sent      int
	)
	for _, cfg := range configs {
		switch err := s.post(ctx, cfg, event); {
		case err == nil:
			sent++
		case errors.Is(err, ErrPermanent):
			permanent = append(permanent, err)
		default:
			return err
		}
	}
	if len(permanent) > 0 && sent == 0 {
		return errors.Join(permanent...)
	}
	return nil
}

func (s *PushSink) statusEvent(ctx context.Context, e OutboxEntry, version int64) (a2a.Event, error) {
	events, err := s.store.Events(ctx, e.Scope, e.TaskID, version)
	if err != nil {
		return nil, err
	}
	for _, ev := range events {
		if ev.Version == version && ev.Payload != nil {
			return ev.Payload, nil
		}
	}
	return nil, fmt.Errorf("%w: status event %s@%d is missing", ErrPermanent, e.TaskID, version)
}

func (s *PushSink) post(ctx context.Context, cfg *a2a.PushConfig, event a2a.Event) error {
	if cfg == nil || strings.TrimSpace(cfg.URL) == "" {
		return fmt.Errorf("%w: push config without url", ErrPermanent)
	}
	body, err := json.Marshal(a2a.StreamResponse{Event: event})
	if err != nil {
		return fmt.Errorf("%w: push body: %v", ErrPermanent, err)
	}
	headers := map[string]string{notificationTokenHeader: cfg.Token}
	if cfg.Auth != nil && cfg.Auth.Credentials != "" {
		scheme := cfg.Auth.Scheme
		if scheme == "" {
			scheme = "Bearer"
		}
		headers["Authorization"] = scheme + " " + cfg.Auth.Credentials
	}
	status, raw, err := postJSON(ctx, s.client, cfg.URL, headers, body)
	if err != nil {
		return err
	}
	return classifyStatus(status, raw, "push")
}

// TokenSource supplies the bearer token used for internal API calls. The
// platform injects a task-scoped token; a nil source sends no credential.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// TokenSourceFunc adapts a function to TokenSource.
type TokenSourceFunc func(ctx context.Context) (string, error)

// Token implements TokenSource.
func (f TokenSourceFunc) Token(ctx context.Context) (string, error) { return f(ctx) }
