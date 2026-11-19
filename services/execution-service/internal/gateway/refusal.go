// Package gateway: operator-visible diagnostics for refused requests.
//
// PDF rev3.2 puts refusal diagnosis on the operator's side of the boundary:
// the protocol answer stays a stable, content-free protocol error (:347/:277
// keep traces and metrics free of request content), while the plane that
// refused must be able to say afterwards -- to its own operator -- which guard
// decided, under which contract code, for which operation. Without that the
// accepting side of a delegation hop logs nothing but an HTTP span and the
// refusal cannot be debugged or honestly accepted.

package gateway

import (
	"errors"
	"log/slog"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Stable refusal reasons. A reason names the guard that decided the refusal, so
// two failures that share one transport class stay distinguishable in the
// operator journal without enriching the client answer. Names are part of the
// operator contract: adding one is fine, renaming or removing one is not.
const (
	ReasonPrincipalUnverified   = "principal_unverified"
	ReasonPrincipalScopePartial = "principal_scope_incomplete"
	ReasonCallerRequired        = "caller_required"
	ReasonTenantMismatch        = "tenant_echo_mismatch"
	ReasonTaskIDMissing         = "task_id_required"
	ReasonHistoryLengthInvalid  = "history_length_out_of_range"
	ReasonPushDisabled          = "push_disabled"
	ReasonPushConfigMissing     = "push_config_missing"
	ReasonPushURLMissing        = "notification_url_missing"
	ReasonTaskNotCancelable     = "task_already_terminal"
	ReasonContextForeignScope   = "context_outside_scope"
	ReasonContextNotOwned       = "context_not_owned_by_caller"
	ReasonTaskNotOwned          = "task_not_owned_by_caller"
	ReasonExtendedCardMissing   = "extended_card_not_configured"
	ReasonMessageMissing        = "message_required"
	ReasonContentEmpty          = "inbound_content_empty"
	ReasonRoleMissing           = "inbound_role_missing"
	ReasonRoleAgent             = "inbound_role_agent"
	ReasonRoleUnknown           = "inbound_role_unknown"
	ReasonBudgetInvalid         = "step_budget_invalid"
	ReasonBudgetConflict        = "step_budget_conflict"
	ReasonBudgetCeiling         = "step_budget_ceiling_exceeded"
	ReasonIdempotencyKeyPartial = "idempotency_key_incomplete"
	ReasonDigestUnusable        = "idempotency_digest_unusable"
	ReasonKeyReuseMismatch      = "idempotency_key_reuse_mismatch"
	ReasonContextIDTooLong      = "context_id_too_long"
	ReasonTaskKindMissing       = "task_kind_required"
	ReasonParentCycle           = "parent_chain_cycle"
	ReasonParentUnavailable     = "parent_task_unavailable"
	ReasonTreeDepth             = "task_tree_depth_guard"
	ReasonPageTokenUnusable     = "page_token_unusable"
	ReasonTaskStateUnknown      = "task_state_unknown"
	ReasonWorkerMissing         = "worker_id_required"
	ReasonOutboxKeyPartial      = "outbox_key_incomplete"
	ReasonOutboxStateInvalid    = "outbox_state_invalid"
	ReasonPushConfigIncomplete  = "push_config_incomplete"
	ReasonUnclassified          = "unclassified"
)

// refusal is the content-free projection of one refusal onto the operator
// journal: what was asked for (op), how it is classified (code), which guard
// decided (reason, plus the surfacing stage), and a sanitised cause line.
type refusal struct {
	Op     string
	Code   string
	Reason string
	Stage  string
	Detail string
}

// refusalIDs are the operation identifiers a refusal names in the journal.
// These are identities -- isolation scope, calling workload, task and idempotency
// key -- never payload.
type refusalIDs struct {
	scope     contracts.Scope
	caller    string
	taskID    string
	messageID string
}

// sendIDs derives the journal identifiers of a send request without touching
// its content: only the idempotency key (message id) and the addressed task
// cross over.
func sendIDs(scope contracts.Scope, caller string, req *a2a.SendMessageRequest) refusalIDs {
	ids := refusalIDs{scope: scope, caller: caller}
	if req == nil || req.Message == nil {
		return ids
	}
	ids.messageID = string(req.Message.ID)
	ids.taskID = string(req.Message.TaskID)
	return ids
}

// refusalCode classifies err for the journal. A contract code wins; a protocol
// error contributes its own stable reason name; anything else is said so
// explicitly instead of being silently filed under an internal error.
func refusalCode(err error) string {
	if err == nil {
		return ""
	}
	if code := contracts.Code(err); code != "" {
		return code
	}
	// The layers below speak protocol sentinels directly, without the typed
	// wrapper, so both shapes must classify: a bare sentinel carried up by
	// errors.Is, and an *a2a.Error that names its own reason.
	for _, sentinel := range a2aPassthrough {
		if errors.Is(err, sentinel) {
			return a2a.ErrorReason(sentinel)
		}
	}
	var typed *a2a.Error
	if errors.As(err, &typed) {
		return a2a.ErrorReason(typed.Err)
	}
	return "UNCLASSIFIED"
}

// refusalReason names the guard. A reason carried by the error itself (the
// guard that decided) wins; otherwise the surfacing stage names it; otherwise
// the refusal is honestly labelled unclassified -- which is exactly the case
// this defect was about, so it must stay visible rather than look explained.
func refusalReason(stage string, err error) string {
	if reason := contracts.ReasonOf(err); reason != "" {
		return reason
	}
	if stage != "" {
		return stage
	}
	return ReasonUnclassified
}

// classifyRefusal projects err onto the journal fields. It never mutates err:
// the client answer is produced by mapError from the untouched error.
func classifyRefusal(op, stage string, err error) refusal {
	return refusal{
		Op:     op,
		Code:   refusalCode(err),
		Reason: refusalReason(stage, err),
		Stage:  stage,
		Detail: sanitizeDetail(err),
	}
}

const (
	// refusalDetailMaxRunes caps the sanitised cause line.
	refusalDetailMaxRunes = 200
	// refusalTokenMaxRunes truncates one runaway token: a value that arrived in a
	// lower layer's message (a driver complaining about a too-long column, an
	// echo of a client-supplied token) cannot ride the journal just because it
	// happens to sit inside an error string.
	refusalTokenMaxRunes = 40
)

// redactQuoted replaces every quoted literal with a marker. Quoting is how a
// cause line quotes back a value it choked on, and that value can be request
// content.
func redactQuoted(s string) string {
	if !strings.ContainsAny(s, `"'`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		q := s[i]
		if q != '\'' && q != '"' {
			b.WriteByte(q)
			i++
			continue
		}
		end := strings.IndexByte(s[i+1:], q)
		if end < 0 {
			// Unterminated: whatever follows is suspect to the end of the line.
			b.WriteString("<redacted>")
			return b.String()
		}
		b.WriteString(`"<redacted>"`)
		i += 1 + end + 1
	}
	return b.String()
}

// sanitizeDetail turns a cause line into a single journal-safe line. Control
// characters are separators, not data (a cause line must not forge journal
// lines or terminal escapes); quoted literals are redacted; oversized tokens
// are truncated; the whole line is bounded.
func sanitizeDetail(err error) string {
	if err == nil {
		return ""
	}
	fields := strings.FieldsFunc(redactQuoted(err.Error()), func(r rune) bool {
		return !unicode.IsPrint(r) || unicode.IsSpace(r)
	})
	tokens := make([]string, 0, len(fields))
	for _, f := range fields {
		if utf8.RuneCountInString(f) > refusalTokenMaxRunes {
			f = string([]rune(f)[:refusalTokenMaxRunes]) + "…"
		}
		tokens = append(tokens, f)
	}
	out := strings.Join(tokens, " ")
	if utf8.RuneCountInString(out) > refusalDetailMaxRunes {
		out = string([]rune(out)[:refusalDetailMaxRunes]) + "…"
	}
	return out
}

// refuse records one refused request in the operator journal and returns the
// protocol error the client is allowed to see, which is exactly what mapError
// produced before this file existed. The client answer is not enriched with the
// reason, the guard name or the cause line: those are for the operator, and the
// boundary rule is that content and internals stay inside.
func (h *Handler) refuse(op, stage string, ids refusalIDs, err error) error {
	if err == nil {
		return nil
	}
	writeRefusal(h.logger(), op, stage, ids, err)
	return mapError(err)
}

// writeRefusal writes the operator journal line for one refusal. Both the
// protocol face and the acceptance decision inside the service use it, so a
// refusal is described the same way whoever produced it.
func writeRefusal(log *slog.Logger, op, stage string, ids refusalIDs, err error) {
	if err == nil || log == nil {
		return
	}
	r := classifyRefusal(op, stage, err)
	attrs := []any{
		slog.String("op", r.Op),
		slog.String("code", r.Code),
		slog.String("reason", r.Reason),
		slog.String("tenant", ids.scope.Tenant),
		slog.String("deployment", ids.scope.Deployment),
		slog.String("subject", ids.scope.Subject),
		slog.String("caller", ids.caller),
		slog.String("detail", r.Detail),
	}
	if r.Stage != "" && r.Stage != r.Reason {
		attrs = append(attrs, slog.String("stage", r.Stage))
	}
	if ids.taskID != "" {
		attrs = append(attrs, slog.String("task_id", ids.taskID))
	}
	if ids.messageID != "" {
		attrs = append(attrs, slog.String("message_id", ids.messageID))
	}
	log.Warn("gateway: request refused", attrs...)
}

// logger returns the operator journal for this face. A Handler built without a
// logger writes to the process journal, which is the same destination the rest
// of the plane already uses.
func (h *Handler) logger() *slog.Logger {
	if h == nil || h.cfg.Logger == nil {
		return slog.Default()
	}
	return h.cfg.Logger
}
