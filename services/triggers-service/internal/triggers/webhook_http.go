package triggers

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5"
)

const WebhookPathPrefix = "/v1/trigger-webhooks/"
const WebhookMessageIDHeader = "X-AHP-Message-ID"
const WebhookSignatureHeader = "X-AHP-Signature"

// NewWebhookHandler accepts deliveries from explicitly allowed public-edge
// workloads. The edge forwards the raw body and signature over verified mTLS;
// webhook secrets authenticate external senders. This does not expose CRUD.
// An empty allowlist denies every request. Tenant is additionally certificate-
// bound, while the owner subject and execution deployment come only from State.
func NewWebhookHandler(svc *Service, allowedDeployments []string) (http.Handler, error) {
	if svc == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: webhook service required")
	}
	allowed := map[string]bool{}
	for _, d := range allowedDeployments {
		if d = strings.TrimSpace(d); d != "" {
			allowed[d] = true
		}
	}
	return transport.Workload()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := transport.FromContext(r.Context())
		if !ok || !allowed[p.Scope.Deployment] {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "triggers: webhook ingress workload not permitted"))
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id, ok := strings.CutPrefix(r.URL.Path, WebhookPathPrefix)
		if !ok || id == "" || strings.Contains(id, "/") {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "triggers: webhook not found"))
			return
		}
		// A verified ingress may forward only its own tenant's webhook. The
		// resulting owner scope is the persisted record, never request fields.
		trig, err := scanTrigger(svc.pool.QueryRow(r.Context(), fmt.Sprintf(`SELECT %s FROM %s.triggers WHERE trigger_id=$1 AND tenant=$2`, triggerSelect, quoteIdent(svc.schema)), id, p.Scope.Tenant))
		if errors.Is(err, pgx.ErrNoRows) {
			err = contracts.NewError(contracts.ErrNotFound, "triggers: webhook not found")
		}
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, WebhookMaxBody))
		if err != nil {
			var limit *http.MaxBytesError
			if errors.As(err, &limit) {
				http.Error(w, "webhook body exceeds limit", http.StatusRequestEntityTooLarge)
			} else {
				transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "triggers: webhook body unreadable"))
			}
			return
		}
		op, err := svc.ReceiveWebhook(r.Context(), trig.scope(), id, r.Header.Get(WebhookMessageIDHeader), r.Header.Get(WebhookSignatureHeader), body)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		// Do not expose the owner's scope, delegation reference or credential
		// to an external sender in the receipt.
		transport.WriteJSON(w, http.StatusOK, map[string]any{"operation_key": op.Key, "status": op.Status, "result": op.Result})
	})), nil
}
