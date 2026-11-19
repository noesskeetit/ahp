package gateway

import (
	"context"
	"errors"
	"iter"
	"log/slog"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// HandlerConfig tunes the A2A request surface.
type HandlerConfig struct {
	// AllowPush enables the push-notification configuration methods. When false
	// every push method answers PUSH_NOTIFICATION_NOT_SUPPORTED, which is the
	// protocol-conformant way of declaring the feature off.
	AllowPush bool
	// MaxHistory caps how many history messages a task response may carry.
	MaxHistory int
	// ExtendedCard is returned by GetExtendedAgentCard. When nil the handler
	// answers EXTENDED_CARD_NOT_CONFIGURED.
	ExtendedCard *a2a.AgentCard
	// Logger receives the operator journal of this face: refused requests with
	// the guard that decided them. Defaults to the process journal. It carries
	// identifiers and a sanitised cause line only -- never request content.
	Logger *slog.Logger
}

func (c *HandlerConfig) withDefaults() {
	if c.MaxHistory <= 0 || c.MaxHistory > maxHistoryHardCap {
		c.MaxHistory = maxHistoryHardCap
	}
}

const maxHistoryHardCap = 200

// Handler is the A2A ingress surface of the execution plane. It implements the
// official a2a-go RequestHandler on top of the durable Service and Store.
//
// Identity is taken exclusively from the verified principal that the
// transport middleware installed; a client-supplied tenant is only allowed as
// a redundant echo and must match the principal.
type Handler struct {
	svc *Service
	cfg HandlerConfig
}

// NewHandler binds h to svc.
func NewHandler(svc *Service, cfg HandlerConfig) *Handler {
	cfg.withDefaults()
	return &Handler{svc: svc, cfg: cfg}
}

// principal resolves the isolation scope and the caller key from the verified
// principal. Task principals key their messages by task ID so a parent and its
// delegations never collide on a message ID, and vice versa.
func (h *Handler) principal(ctx context.Context) (contracts.Scope, string, error) {
	p, ok := transport.FromContext(ctx)
	if !ok {
		return contracts.Scope{}, "", a2a.NewError(a2a.ErrUnauthenticated, "gateway: no verified principal")
	}
	scope := p.Scope
	if err := scope.Validate(); err != nil {
		return contracts.Scope{}, "", a2a.NewError(a2a.ErrUnauthenticated, "gateway: principal scope is incomplete")
	}
	caller := p.Claims.Task
	if caller == "" && p.Claims.Kind == auth.KindHuman {
		caller = p.Claims.Subject
	}
	if caller == "" {
		caller = "workload:" + scope.Deployment
	}
	return scope, caller, nil
}

// checkTenant rejects a request body that names a different tenant than the
// authenticated principal.
func checkTenant(requestTenant string, scope contracts.Scope) error {
	if requestTenant != "" && requestTenant != scope.Tenant {
		return a2a.NewError(a2a.ErrUnauthorized, "gateway: tenant does not match the authenticated principal")
	}
	return nil
}

// a2aPassthrough lists the protocol sentinels the layers below already speak.
var a2aPassthrough = []error{
	a2a.ErrTaskNotFound,
	a2a.ErrTaskNotCancelable,
	a2a.ErrPushNotificationNotSupported,
	a2a.ErrUnsupportedOperation,
	a2a.ErrUnauthenticated,
	a2a.ErrUnauthorized,
	a2a.ErrInvalidParams,
	a2a.ErrInternalError,
}

// mapError projects internal errors onto protocol errors. Only protocol
// sentinels and stable AHP codes cross the boundary; anything else collapses
// to INTERNAL_ERROR so storage details never leak to callers.
//
// The collapse is deliberate and is not where diagnosis ends: callers must not
// see which guard refused, the operator must. Handler.refuse (refusal.go) writes
// the guard name, the contract code and the operation identifiers to the journal
// before handing this projection over, so a refusal is never only an HTTP span.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	for _, sentinel := range a2aPassthrough {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	var typed *a2a.Error
	if errors.As(err, &typed) {
		return err
	}
	switch contracts.Code(err) {
	case contracts.ErrNotFound:
		return a2a.NewError(a2a.ErrTaskNotFound, "gateway: not found")
	case contracts.ErrForbidden, contracts.ErrStaleEpoch:
		return a2a.NewError(a2a.ErrUnauthorized, "gateway: operation not permitted for this principal")
	case contracts.ErrInvalid, contracts.ErrKeyReuse:
		return a2a.NewError(a2a.ErrInvalidParams, "gateway: request rejected")
	case contracts.ErrConflict, contracts.ErrLeaseLost:
		return a2a.NewError(a2a.ErrInvalidRequest, "gateway: request conflicts with current state")
	}
	return a2a.NewError(a2a.ErrInternalError, "gateway: internal error")
}

// history loads a task history bounded by the handler cap. A nil limit means
// "no history requested"; a negative limit is a client error. op and ids name
// the refusal in the operator journal: this guard is reached from several
// methods, so only the caller knows which one is being answered.
func (h *Handler) history(ctx context.Context, op string, ids refusalIDs, taskID string, length *int) ([]*a2a.Message, error) {
	if length == nil {
		return nil, nil
	}
	if *length < 0 || *length > h.cfg.MaxHistory {
		return nil, h.refuse(op, ReasonHistoryLengthInvalid, ids,
			a2a.NewError(a2a.ErrInvalidParams, "gateway: historyLength out of range"))
	}
	msgs, err := h.svc.Store().History(ctx, taskID, *length)
	if err != nil {
		return nil, h.refuse(op, "history_read", ids, err)
	}
	return msgs, nil
}

// trimHistory keeps the last n messages of an already built task snapshot.
func trimHistory(task *a2a.Task, n *int) error {
	if n == nil {
		return nil
	}
	if *n < 0 {
		return a2a.NewError(a2a.ErrInvalidParams, "gateway: historyLength out of range")
	}
	if *n == 0 {
		task.History = nil
		return nil
	}
	if len(task.History) > *n {
		task.History = task.History[len(task.History)-*n:]
	}
	return nil
}

// GetTask implements a2asrv.RequestHandler.
func (h *Handler) GetTask(ctx context.Context, req *a2a.GetTaskRequest) (*a2a.Task, error) {
	scope, _, err := h.principal(ctx)
	if err != nil {
		return nil, h.refuse(MethodGetTask, "principal", refusalIDs{}, err)
	}
	ids := refusalIDs{scope: scope, taskID: string(req.ID)}
	if err := checkTenant(req.Tenant, scope); err != nil {
		return nil, h.refuse(MethodGetTask, ReasonTenantMismatch, ids, err)
	}
	if req.ID == "" {
		return nil, h.refuse(MethodGetTask, ReasonTaskIDMissing, ids,
			a2a.NewError(a2a.ErrInvalidParams, "gateway: taskId is required"))
	}
	row, err := h.svc.Store().Task(ctx, scope, string(req.ID))
	if err != nil {
		return nil, h.refuse(MethodGetTask, "task_read", ids, err)
	}
	hist, err := h.history(ctx, MethodGetTask, ids, row.ID, req.HistoryLength)
	if err != nil {
		return nil, err
	}
	return toTask(row, hist), nil
}

// ListTasks implements a2asrv.RequestHandler. Paging is offset based; the
// returned token is opaque to clients and stable for the caller's scope only.
func (h *Handler) ListTasks(ctx context.Context, req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	scope, _, err := h.principal(ctx)
	if err != nil {
		return nil, h.refuse(MethodListTasks, "principal", refusalIDs{}, err)
	}
	ids := refusalIDs{scope: scope}
	if err := checkTenant(req.Tenant, scope); err != nil {
		return nil, h.refuse(MethodListTasks, ReasonTenantMismatch, ids, err)
	}
	rows, total, next, err := h.svc.Store().ListTasks(ctx, scope, req.ContextID, req.Status, req.PageSize, req.PageToken)
	if err != nil {
		return nil, h.refuse(MethodListTasks, "task_list", ids, err)
	}
	pageSize := req.PageSize
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 50
	}
	tasks := make([]*a2a.Task, 0, len(rows))
	for _, row := range rows {
		var hist []*a2a.Message
		if req.HistoryLength != nil {
			hist, err = h.history(ctx, MethodListTasks, ids, row.ID, req.HistoryLength)
			if err != nil {
				return nil, err
			}
		}
		tasks = append(tasks, toTask(row, hist))
	}
	return &a2a.ListTasksResponse{Tasks: tasks, TotalSize: total, PageSize: pageSize, NextPageToken: next}, nil
}

// CancelTask implements a2asrv.RequestHandler. Cancellation is cooperative:
// the flag is set durably and the holding worker observes it at its next
// checkpoint, so an in-flight side effect is never torn down mid-dispatch.
func (h *Handler) CancelTask(ctx context.Context, req *a2a.CancelTaskRequest) (*a2a.Task, error) {
	scope, _, err := h.principal(ctx)
	if err != nil {
		return nil, h.refuse(MethodCancelTask, "principal", refusalIDs{}, err)
	}
	ids := refusalIDs{scope: scope, taskID: string(req.ID)}
	if err := checkTenant(req.Tenant, scope); err != nil {
		return nil, h.refuse(MethodCancelTask, ReasonTenantMismatch, ids, err)
	}
	if req.ID == "" {
		return nil, h.refuse(MethodCancelTask, ReasonTaskIDMissing, ids,
			a2a.NewError(a2a.ErrInvalidParams, "gateway: taskId is required"))
	}
	row, err := h.svc.Store().Task(ctx, scope, string(req.ID))
	if err != nil {
		return nil, h.refuse(MethodCancelTask, "task_read", ids, err)
	}
	if row.Terminal() {
		return nil, h.refuse(MethodCancelTask, ReasonTaskNotCancelable, ids,
			a2a.NewError(a2a.ErrTaskNotCancelable, "gateway: task already reached a terminal state"))
	}
	updated, err := h.svc.Store().RequestCancel(ctx, scope, string(req.ID))
	if err != nil {
		return nil, h.refuse(MethodCancelTask, "cancel_request", ids, err)
	}
	return toTask(updated, nil), nil
}

// SendMessage implements a2asrv.RequestHandler.
func (h *Handler) SendMessage(ctx context.Context, req *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	scope, caller, err := h.principal(ctx)
	if err != nil {
		return nil, h.refuse(MethodSendMessage, "principal", refusalIDs{}, err)
	}
	ids := sendIDs(scope, caller, req)
	if err := h.acceptSend(scope, req); err != nil {
		return nil, h.refuse(MethodSendMessage, "accept_send", ids, err)
	}
	task, err := h.svc.SendMessage(ctx, scope, caller, req)
	if err != nil {
		return nil, h.refuse(MethodSendMessage, "accept_task", ids, err)
	}
	if err := trimHistory(task, historyLength(req)); err != nil {
		return nil, h.refuse(MethodSendMessage, ReasonHistoryLengthInvalid, ids, err)
	}
	return task, nil
}

func historyLength(req *a2a.SendMessageRequest) *int {
	if req == nil || req.Config == nil {
		return nil
	}
	return req.Config.HistoryLength
}

// acceptSend validates the parts of a send request the handler owns.
func (h *Handler) acceptSend(scope contracts.Scope, req *a2a.SendMessageRequest) error {
	if req == nil {
		return a2a.NewError(a2a.ErrInvalidParams, "gateway: request is required")
	}
	if err := checkTenant(req.Tenant, scope); err != nil {
		return err
	}
	if pushConfigOf(req) != nil && !h.cfg.AllowPush {
		return a2a.NewError(a2a.ErrPushNotificationNotSupported, "gateway: push notifications are disabled")
	}
	return nil
}

// SendStreamingMessage implements a2asrv.RequestHandler. The task is accepted
// durably first and returned as the first stream event, so a client that
// disconnects immediately still leaves an observable task behind.
func (h *Handler) SendStreamingMessage(ctx context.Context, req *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		scope, caller, err := h.principal(ctx)
		if err != nil {
			yield(nil, h.refuse(MethodSendStreamingMessage, "principal", refusalIDs{}, err))
			return
		}
		if err := h.acceptSend(scope, req); err != nil {
			yield(nil, h.refuse(MethodSendStreamingMessage, "accept_send", sendIDs(scope, caller, req), err))
			return
		}
		row, err := h.startTask(ctx, MethodSendStreamingMessage, scope, caller, req)
		if err != nil {
			yield(nil, err)
			return
		}
		for event, err := range h.streamTask(ctx, MethodSendStreamingMessage, scope, row) {
			if !yield(event, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
}

// startTask accepts a message without blocking on completion and reloads the
// row so the stream can anchor on a durable status version.
func (h *Handler) startTask(ctx context.Context, op string, scope contracts.Scope, caller string, req *a2a.SendMessageRequest) (TaskRow, error) {
	clone := *req
	if clone.Config == nil {
		clone.Config = &a2a.SendMessageConfig{}
	}
	cfg := *clone.Config
	cfg.ReturnImmediately = true
	clone.Config = &cfg
	task, err := h.svc.SendMessage(ctx, scope, caller, &clone)
	if err != nil {
		return TaskRow{}, h.refuse(op, "accept_task", sendIDs(scope, caller, req), err)
	}
	row, err := h.svc.Store().Task(ctx, scope, string(task.ID))
	if err != nil {
		return TaskRow{}, h.refuse(op, "task_read", sendIDs(scope, caller, req), err)
	}
	return row, nil
}

// streamTask replays the journalled transitions of row and then follows the
// live journal until a terminal event. The task snapshot itself is the first
// event, which is what the protocol requires of a subscription.
func (h *Handler) streamTask(ctx context.Context, op string, scope contracts.Scope, row TaskRow) iter.Seq2[a2a.Event, error] {
	ids := refusalIDs{scope: scope, taskID: row.ID}
	return func(yield func(a2a.Event, error) bool) {
		hist, err := h.svc.Store().History(ctx, row.ID, 0)
		if err != nil {
			yield(nil, h.refuse(op, "history_read", ids, err))
			return
		}
		if !yield(toTask(row, hist), nil) {
			return
		}
		if row.Terminal() {
			// The task object handed over above already carries the durable
			// artifacts, so a reconnecting subscriber sees its result at once.
			return
		}
		for event, err := range h.svc.Events(ctx, scope, row.ID, row.StatusVersion) {
			if err != nil {
				yield(nil, h.refuse(op, "event_stream", ids, err))
				return
			}
			// A client picked SSE precisely because the Agent Card declares
			// capabilities.streaming, and this plane's advertised skill is "run a
			// turn and export its result as an artifact". Closing the stream with a
			// terminal status while the artifacts sit unseen forces that client to
			// poll GetTask for the thing it streamed for, so the artifacts the task
			// really carries are announced here -- read back from the durable row,
			// never asserted from the executor's return value -- and only then does
			// the terminal status close the stream.
			if terminalEvent(event) {
				latest, err := h.svc.Store().Task(ctx, scope, row.ID)
				if err != nil {
					yield(nil, h.refuse(op, "task_read", ids, err))
					return
				}
				for _, art := range latest.Artifacts {
					if art == nil {
						continue
					}
					update := &a2a.TaskArtifactUpdateEvent{
						TaskID:    a2a.TaskID(latest.ID),
						ContextID: latest.ContextID,
						Artifact:  art,
						LastChunk: true,
					}
					if !yield(update, nil) {
						return
					}
				}
			}
			if !yield(event, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
}

// SubscribeToTask implements a2asrv.RequestHandler. It is the SSE replay
// entrypoint: a subscriber that reconnects after a restart sees the full
// journalled history again before any live transition.
func (h *Handler) SubscribeToTask(ctx context.Context, req *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		scope, _, err := h.principal(ctx)
		if err != nil {
			yield(nil, h.refuse(MethodSubscribeToTask, "principal", refusalIDs{}, err))
			return
		}
		ids := refusalIDs{scope: scope, taskID: string(req.ID)}
		if err := checkTenant(req.Tenant, scope); err != nil {
			yield(nil, h.refuse(MethodSubscribeToTask, ReasonTenantMismatch, ids, err))
			return
		}
		if req.ID == "" {
			yield(nil, h.refuse(MethodSubscribeToTask, ReasonTaskIDMissing, ids,
				a2a.NewError(a2a.ErrInvalidParams, "gateway: taskId is required")))
			return
		}
		row, err := h.svc.Store().Task(ctx, scope, string(req.ID))
		if err != nil {
			yield(nil, h.refuse(MethodSubscribeToTask, "task_read", ids, err))
			return
		}
		for event, err := range h.streamTask(ctx, MethodSubscribeToTask, scope, row) {
			if !yield(event, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
}

// CreateTaskPushConfig implements a2asrv.RequestHandler.
func (h *Handler) CreateTaskPushConfig(ctx context.Context, cfg *a2a.PushConfig) (*a2a.PushConfig, error) {
	if !h.cfg.AllowPush {
		return nil, h.refuse(MethodCreatePushConfig, ReasonPushDisabled, refusalIDs{},
			a2a.NewError(a2a.ErrPushNotificationNotSupported, "gateway: push notifications are disabled"))
	}
	scope, _, err := h.principal(ctx)
	if err != nil {
		return nil, h.refuse(MethodCreatePushConfig, "principal", refusalIDs{}, err)
	}
	ids := refusalIDs{scope: scope}
	if cfg == nil {
		return nil, h.refuse(MethodCreatePushConfig, ReasonPushConfigMissing, ids,
			a2a.NewError(a2a.ErrInvalidParams, "gateway: push notification configuration is required"))
	}
	ids.taskID = string(cfg.TaskID)
	if err := checkTenant(cfg.Tenant, scope); err != nil {
		return nil, h.refuse(MethodCreatePushConfig, ReasonTenantMismatch, ids, err)
	}
	if cfg.TaskID == "" {
		return nil, h.refuse(MethodCreatePushConfig, ReasonTaskIDMissing, ids,
			a2a.NewError(a2a.ErrInvalidParams, "gateway: taskId is required"))
	}
	if cfg.URL == "" {
		return nil, h.refuse(MethodCreatePushConfig, ReasonPushURLMissing, ids,
			a2a.NewError(a2a.ErrInvalidParams, "gateway: notification url is required"))
	}
	if _, err := h.svc.Store().Task(ctx, scope, string(cfg.TaskID)); err != nil {
		return nil, h.refuse(MethodCreatePushConfig, "task_read", ids, err)
	}
	out, err := h.svc.Store().PutPushConfig(ctx, cfg, scope)
	if err != nil {
		return nil, h.refuse(MethodCreatePushConfig, "push_config_write", ids, err)
	}
	return out, nil
}

// GetTaskPushConfig implements a2asrv.RequestHandler.
func (h *Handler) GetTaskPushConfig(ctx context.Context, req *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	if !h.cfg.AllowPush {
		return nil, h.refuse(MethodGetPushConfig, ReasonPushDisabled, refusalIDs{},
			a2a.NewError(a2a.ErrPushNotificationNotSupported, "gateway: push notifications are disabled"))
	}
	scope, _, err := h.principal(ctx)
	if err != nil {
		return nil, h.refuse(MethodGetPushConfig, "principal", refusalIDs{}, err)
	}
	ids := refusalIDs{scope: scope, taskID: string(req.TaskID)}
	if err := checkTenant(req.Tenant, scope); err != nil {
		return nil, h.refuse(MethodGetPushConfig, ReasonTenantMismatch, ids, err)
	}
	if req.TaskID == "" || req.ID == "" {
		return nil, h.refuse(MethodGetPushConfig, ReasonTaskIDMissing, ids,
			a2a.NewError(a2a.ErrInvalidParams, "gateway: taskId and configId are required"))
	}
	configs, err := h.svc.Store().PushConfigs(ctx, scope, string(req.TaskID))
	if err != nil {
		return nil, h.refuse(MethodGetPushConfig, "push_config_read", ids, err)
	}
	for _, cfg := range configs {
		if cfg.ID == req.ID {
			return cfg, nil
		}
	}
	return nil, h.refuse(MethodGetPushConfig, "push_config_not_found", ids,
		a2a.NewError(a2a.ErrTaskNotFound, "gateway: push notification configuration not found"))
}

// ListTaskPushConfigs implements a2asrv.RequestHandler. The number of
// configurations per task is bounded by the caller itself, so the whole set is
// returned and no paging token is issued.
func (h *Handler) ListTaskPushConfigs(ctx context.Context, req *a2a.ListTaskPushConfigRequest) (*a2a.ListTaskPushConfigResponse, error) {
	if !h.cfg.AllowPush {
		return nil, h.refuse(MethodListPushConfigs, ReasonPushDisabled, refusalIDs{},
			a2a.NewError(a2a.ErrPushNotificationNotSupported, "gateway: push notifications are disabled"))
	}
	scope, _, err := h.principal(ctx)
	if err != nil {
		return nil, h.refuse(MethodListPushConfigs, "principal", refusalIDs{}, err)
	}
	ids := refusalIDs{scope: scope, taskID: string(req.TaskID)}
	if err := checkTenant(req.Tenant, scope); err != nil {
		return nil, h.refuse(MethodListPushConfigs, ReasonTenantMismatch, ids, err)
	}
	if req.TaskID == "" {
		return nil, h.refuse(MethodListPushConfigs, ReasonTaskIDMissing, ids,
			a2a.NewError(a2a.ErrInvalidParams, "gateway: taskId is required"))
	}
	configs, err := h.svc.Store().PushConfigs(ctx, scope, string(req.TaskID))
	if err != nil {
		return nil, h.refuse(MethodListPushConfigs, "push_config_read", ids, err)
	}
	return &a2a.ListTaskPushConfigResponse{Configs: configs}, nil
}

// DeleteTaskPushConfig implements a2asrv.RequestHandler.
func (h *Handler) DeleteTaskPushConfig(ctx context.Context, req *a2a.DeleteTaskPushConfigRequest) error {
	if !h.cfg.AllowPush {
		return h.refuse(MethodDeletePushConfig, ReasonPushDisabled, refusalIDs{},
			a2a.NewError(a2a.ErrPushNotificationNotSupported, "gateway: push notifications are disabled"))
	}
	scope, _, err := h.principal(ctx)
	if err != nil {
		return h.refuse(MethodDeletePushConfig, "principal", refusalIDs{}, err)
	}
	ids := refusalIDs{scope: scope, taskID: string(req.TaskID)}
	if err := checkTenant(req.Tenant, scope); err != nil {
		return h.refuse(MethodDeletePushConfig, ReasonTenantMismatch, ids, err)
	}
	if req.TaskID == "" || req.ID == "" {
		return h.refuse(MethodDeletePushConfig, ReasonTaskIDMissing, ids,
			a2a.NewError(a2a.ErrInvalidParams, "gateway: taskId and configId are required"))
	}
	if err := h.svc.Store().DeletePushConfig(ctx, scope, string(req.TaskID), req.ID); err != nil {
		return h.refuse(MethodDeletePushConfig, "push_config_delete", ids, err)
	}
	return nil
}

// GetExtendedAgentCard implements a2asrv.RequestHandler.
func (h *Handler) GetExtendedAgentCard(ctx context.Context, req *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error) {
	scope, _, err := h.principal(ctx)
	if err != nil {
		return nil, h.refuse(MethodGetExtendedAgentCard, "principal", refusalIDs{}, err)
	}
	ids := refusalIDs{scope: scope}
	if req != nil {
		if err := checkTenant(req.Tenant, scope); err != nil {
			return nil, h.refuse(MethodGetExtendedAgentCard, ReasonTenantMismatch, ids, err)
		}
	}
	if h.cfg.ExtendedCard == nil {
		return nil, h.refuse(MethodGetExtendedAgentCard, ReasonExtendedCardMissing, ids,
			a2a.NewError(a2a.ErrExtendedCardNotConfigured, "gateway: extended agent card is not configured"))
	}
	return h.cfg.ExtendedCard, nil
}
