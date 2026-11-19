package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// EffectKind is the ledger journal kind for a model generation side effect.
const EffectKind = "model.generation"

// SubmitArgs describes one durable generation request. Reservation is the
// admission bound: the service reserves ReserveAmount against BudgetID before
// any provider call. A provider overrun is recorded in full and blocks further
// admission; the estimate never replaces the actual bill.
type SubmitArgs struct {
	Scope         contracts.Scope
	Task          string
	Key           string // generation idempotency key (caller-scoped)
	Model         string
	Request       ChatRequest
	BudgetID      string
	ReserveKey    string // budget reservation idempotency key
	ReserveAmount int64  // upper bound of tokens this generation may consume
	// Provider overrides which provider copy the RPM/TPM gate charges for this
	// generation. Label-aware routing can send two turns of one tenant to two
	// copies of one upstream with different limits, so the window that pays is the
	// copy that served -- a per-request fact that no deployment-wide name can
	// express. Empty means the service's own configured provider, which is exactly
	// the behaviour before routing existed: an unset field changes nothing.
	Provider string
}

// Service drives durable generations: it reserves budget before dispatch,
// journals the provider call in the service-owned effect ledger (so a crash
// yields SIDE_EFFECT_UNKNOWN rather than an automatic replay), streams deltas,
// honours cancellation and settles the budget from real usage.
type Service struct {
	files    *FileService
	gens     *GenerationStore
	budget   *BudgetStore
	ledger   *operations.Store
	inf      Inference
	checkN   int           // deltas between cancellation checks
	checkDur time.Duration // fallback interval for cancellation checks
	// quota and provider are the provider-quota gate (see quota.go). Both are
	// zero unless a deployment attaches them with WithQuota, and with no gate
	// Submit behaves exactly as it did before quotas existed.
	quota    *QuotaStore
	provider string
	// providers picks the PHYSICAL client of the provider copy a generation was
	// admitted against. Nil keeps today's wiring exactly: one configured
	// inference for every generation. With it, a generation recorded as provider
	// "modelrun-in-tenant" is executed by that copy's client or the attempt is
	// refused -- never by whichever client happens to be mounted.
	providers ProviderClients
}

// ProviderClients maps a provider copy to the client that can reach it. The
// composition root (internal/execbridge) builds it from the operator's provider
// table, one secret resolution per copy; the model plane stays free of secret
// stores and routing tables.
type ProviderClients interface {
	InferenceFor(ctx context.Context, provider string) (Inference, error)
}

// ReasonProviderClientUnavailable names the refusal of a generation whose
// recorded copy cannot be served here. Stable: operators script on it.
const ReasonProviderClientUnavailable = "provider_client_unavailable"

// ReasonModelEgressRefusedBeforeSend names the terminal cause of a generation
// the network-policy guard refused before any request byte reached a provider
// (scheme/host/method/address denial, or a failed resolution). It is a definite
// failure, not an unknown side effect: the reservation is returned and the
// operation is journalled FAILED, never SIDE_EFFECT_UNKNOWN.
const ReasonModelEgressRefusedBeforeSend = "model_egress_refused_before_send"

// WithProviderClients attaches the per-copy client selector.
func (s *Service) WithProviderClients(p ProviderClients) *Service {
	s.providers = p
	return s
}

// clientFor answers which physical client runs one generation.
//
// The rule is deliberately narrow. A generation with no recorded provider runs on
// the configured client (the single-provider deployment, unchanged). A generation
// that names a copy MUST run on that copy: no selector mounted, no client
// resolved, or a resolution error all refuse the attempt rather than substituting
// the default client. Substituting is the defect -- a request routed "in-tenant"
// and dialled at the external upstream reports an upstream that never saw it.
func (s *Service) clientFor(ctx context.Context, g Generation) (Inference, error) {
	if g.Provider == "" {
		return s.inf, nil
	}
	if s.providers == nil {
		if g.Provider == s.provider && s.inf != nil {
			return s.inf, nil
		}
		return nil, contracts.NewReasoned(contracts.ErrConflict, ReasonProviderClientUnavailable,
			"model: generation "+g.ID+" was admitted against provider "+g.Provider+
				" but this service mounts no provider clients")
	}
	inf, err := s.providers.InferenceFor(ctx, g.Provider)
	if err != nil {
		// A refused base_url is a configuration fact with its own reason class:
		// folding it into provider_client_unavailable would erase why the copy is
		// unservable, and the reason is what an operator scripts on. The message
		// carries no credentials by construction.
		if cfg := new(BaseURLConfigError); errors.As(err, &cfg) {
			return nil, contracts.NewReasoned(contracts.ErrConflict, cfg.Reason, cfg.Error())
		}
		return nil, contracts.NewReasoned(contracts.ErrConflict, ReasonProviderClientUnavailable,
			"model: provider "+g.Provider+" cannot be served here: "+err.Error())
	}
	if inf == nil {
		return nil, contracts.NewReasoned(contracts.ErrConflict, ReasonProviderClientUnavailable,
			"model: provider "+g.Provider+" resolved no client")
	}
	return inf, nil
}

// NewService wires a model Service. ledger may be nil for tests that do not
// exercise crash semantics, but production wiring always supplies it.
func NewService(gens *GenerationStore, budget *BudgetStore, ledger *operations.Store, inf Inference) *Service {
	return &Service{gens: gens, budget: budget, ledger: ledger, inf: inf, checkN: 8, checkDur: 250 * time.Millisecond}
}

// WithQuota attaches the provider quota gate to this service and names the
// upstream the limits are keyed by.
//
// The provider label is a deployment-chosen name ("qwen-ds-core"), never a
// credential or an endpoint: an operator has to be able to write a limit row
// against something that survives a provider host change. Attaching a store
// without a name is refused at Submit time rather than silently keying every
// tenant's window under an empty provider.
func (s *Service) WithQuota(q *QuotaStore, provider string) *Service {
	s.quota = q
	s.provider = strings.TrimSpace(provider)
	return s
}

// ProviderQuotas reports the mounted quota gate (nil when this deployment runs
// without one). It is the operator/verifier handle: limits, current window and
// the admission journal all read through it.
func (s *Service) ProviderQuotas() *QuotaStore { return s.quota }

// quotaTokenEstimate is the token figure a generation is admitted against. The
// budget's own reserve is the authoritative upper bound when the caller set one;
// a raw request carries its own completion cap otherwise. Estimating at all is
// the point: a window cannot refuse a request later for tokens it never reserved,
// and Refund converges the estimate to what the provider really charged.
func quotaTokenEstimate(args SubmitArgs) int64 {
	if args.ReserveAmount > 0 {
		return args.ReserveAmount
	}
	if args.Request.MaxTokens > 0 {
		return int64(args.Request.MaxTokens)
	}
	return 1
}

// Submit creates (or replays) a generation and reserves its budget before any
// spend. Reservation failure (insufficient capacity) aborts the submit, so an
// over-budget generation is never accepted.
//
// The provider quota gate runs first, so a tenant whose minute is spent produces
// no generation row and no budget hold: nothing has to be unwound, and the
// refusal is the operation's answer rather than a statistic written after it.
func (s *Service) Submit(ctx context.Context, args SubmitArgs) (Generation, error) {
	if err := args.Scope.Validate(); err != nil {
		return Generation{}, err
	}
	if args.Key == "" {
		return Generation{}, contracts.NewError(contracts.ErrInvalid, "generation key required")
	}
	// Resolve the physical destination before booking quota or budget.
	if args.Provider == "" {
		args.Provider = s.provider
	}
	if _, err := s.clientFor(ctx, Generation{Provider: args.Provider}); err != nil {
		return Generation{}, err
	}
	model := args.Model
	if model == "" {
		model = s.inf.Model()
	}
	if model == "" {
		return Generation{}, contracts.NewError(contracts.ErrInvalid, "model required")
	}
	// A completed retry must not re-open a sealed ledger key, and a changed
	// request must not book a different provider before discovering the mismatch.
	if old, err := s.gens.lookup(ctx, s.gens.pool, args.Scope, args.Key); err == nil {
		digest, err := requestDigest(args.Scope, args.Task, args.Key, model, args.Request)
		if err != nil {
			return Generation{}, err
		}
		if old.digest != digest || old.Provider != args.Provider || old.BudgetID != args.BudgetID || old.ReserveKey != args.ReserveKey || old.Reserved != args.ReserveAmount {
			return Generation{}, contracts.NewError(contracts.ErrKeyReuse, "generation key reused with different request")
		}
		if terminal(old.Status) || s.ledger == nil {
			return old, nil
		}
		if _, err := s.ledger.Get(ctx, args.Scope, old.EffectKey); err == nil {
			return old, nil
		} else if contracts.Code(err) != contracts.ErrNotFound {
			return Generation{}, err
		}
	} else if contracts.Code(err) != contracts.ErrNotFound {
		return Generation{}, err
	}

	// Provider quota: accepted-at-admission, not observed-after-the-fact. The
	// generation key is the attempt's idempotency key, so a retry of an admitted
	// attempt replays its booking instead of paying for it twice.
	// The copy that serves is the copy that pays. Falling back to the service's
	// configured provider when the caller named none keeps every existing path on
	// the same window it used before, including the error an operator gets when a
	// quota store is mounted with nothing to key it by.
	if s.quota != nil {
		quotaProvider := args.Provider
		if quotaProvider == "" {
			quotaProvider = s.provider
		}
		if quotaProvider == "" {
			return Generation{}, contracts.NewError(contracts.ErrInvalid,
				"model: a quota store is attached but no provider is named (Service.WithQuota)")
		}
		if _, err := s.quota.Admit(ctx, quotaProvider, args.Scope.Tenant, args.Key, 1,
			quotaTokenEstimate(args)); err != nil {
			return Generation{}, err
		}
	}

	// Admission: reserve before creating spendable work. Idempotent on
	// ReserveKey, so a replay of Submit does not double-hold.
	if args.BudgetID != "" && args.ReserveAmount > 0 {
		if args.ReserveKey == "" {
			return Generation{}, contracts.NewError(contracts.ErrInvalid, "reserve key required with budget")
		}
		if _, err := s.budget.Reserve(ctx, args.Scope, args.BudgetID, args.ReserveKey, args.ReserveAmount); err != nil {
			return Generation{}, fmt.Errorf("model: budget admission: %w", err)
		}
	}

	g, created, err := s.gens.Submit(ctx, args.Scope, args.Task, args.Key, model, args.Request,
		args.BudgetID, args.ReserveKey, args.ReserveAmount, args.Provider)
	if err != nil {
		return Generation{}, err
	}
	_ = created

	if s.ledger != nil {
		argsJSON, _ := json.Marshal(map[string]any{"generation": g.ID, "model": model})
		if _, err := s.ledger.Accept(ctx, args.Scope, operations.Request{
			Key:       g.EffectKey,
			Kind:      EffectKind,
			Target:    model,
			Guard:     "generation:" + g.ID,
			Arguments: argsJSON,
		}); err != nil {
			return Generation{}, fmt.Errorf("model: ledger accept: %w", err)
		}
	}
	return g, nil
}

// Execute drives a generation to a terminal state. It is idempotent: a replay
// after completion returns the stored result, and a crash between dispatch and
// completion is surfaced as SIDE_EFFECT_UNKNOWN instead of a silent replay.
func (s *Service) Execute(ctx context.Context, scope contracts.Scope, id string) (Generation, error) {
	ctx = beginExecutionDiagnostics(ctx)
	// Free any volatile checkpoint-boundary tail this execution opened once the
	// generation is terminal. No-op in the default chunk mode; on a mid-stream
	// abandon the buffered-but-unflushed fragments are declared volatile losses.
	defer s.gens.DiscardGeneration(id)
	// A session lock distinguishes a live execution from a crashed dispatch.
	// PostgreSQL releases it when the worker's connection dies. Concurrent
	// retries observe the row; only a new owner may reconcile a lost dispatch.
	conn, err := pgx.ConnectConfig(ctx, s.gens.pool.Config().ConnConfig.Copy())
	if err != nil {
		return Generation{}, executionError(ctx, "owner_connect", "execute", err)
	}
	lockKey := contracts.OperationKey(scope, id, "model.execute", "owner")
	var owned bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, lockKey).Scan(&owned); err != nil {
		err = executionError(ctx, "owner_lock", "execute", err)
		_ = conn.Close(ctx)
		return Generation{}, err
	}
	if !owned {
		_ = conn.Close(ctx)
		current, err := s.gens.Get(ctx, scope, id)
		return current, executionError(ctx, "observer_generation_get", "execute", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := conn.Exec(cleanup, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, lockKey); err != nil {
			_ = conn.Close(cleanup)
		}
		_ = conn.Close(cleanup)
	}()
	g, err := s.gens.Get(ctx, scope, id)
	if err != nil {
		return Generation{}, executionError(ctx, "generation_get", "execute", err)
	}
	if terminal(g.Status) {
		if g.Status == GenCanceled && g.Error == "" {
			if err = s.cancelUnsentReservation(ctx, scope, g); err != nil {
				return Generation{}, executionError(ctx, "cancel_unsent_reservation", "execute", err)
			}
		}
		return g, nil
	}
	if g.CancelRequested {
		return s.cancelled(ctx, scope, g, "", g.Usage)
	}

	// Fencing before dispatch: only the holder of the ledger dispatch right may
	// call the provider.
	if s.ledger != nil {
		if _, derr := s.ledger.Dispatch(ctx, scope, g.EffectKey); derr != nil {
			if errors.Is(derr, operations.ErrAlreadyDispatched) {
				return s.recoverDispatched(ctx, scope, g)
			}
			return Generation{}, executionError(ctx, "ledger_dispatch", "execute", fmt.Errorf("model: ledger dispatch: %w", derr))
		}
	}
	return s.run(ctx, scope, g)
}

// recoverDispatched resolves the ambiguous case where another attempt already
// grabbed the dispatch right (possibly the same process before a crash).
func (s *Service) recoverDispatched(ctx context.Context, scope contracts.Scope, g Generation) (Generation, error) {
	op, err := s.ledger.Get(ctx, scope, g.EffectKey)
	if err != nil {
		return Generation{}, executionError(ctx, "recovery_ledger_get", "execute", err)
	}
	switch op.Status {
	case operations.StatusSucceeded, operations.StatusFailed:
		var res resultPayload
		_ = json.Unmarshal(op.Result, &res)
		return s.settleAndFinish(ctx, scope, g, res.Status, res.Error, res.Usage, op.Status == operations.StatusFailed, res)
	case operations.StatusSideEffectUnknown, operations.StatusFenced:
		return s.ambiguous(ctx, scope, g)
	default: // DISPATCHED: a prior attempt may have reached the provider
		if _, merr := s.ledger.MarkUnknown(ctx, scope, g.EffectKey); merr != nil {
			return Generation{}, executionError(ctx, "recovery_mark_unknown", "execute", merr)
		}
		return s.ambiguous(ctx, scope, g)
	}
}

// ambiguous records that a provider call may have happened but its outcome is
// unknown: the budget is charged the full reservation (never silently replayed)
// and the generation fails rather than re-invoking the provider.
func (s *Service) ambiguous(ctx context.Context, scope contracts.Scope, g Generation) (Generation, error) {
	if err := s.settle(ctx, scope, g, max(g.Reserved, int64(g.Usage.TotalTokens))); err != nil {
		return Generation{}, err
	}
	final, err := s.gens.Finalize(ctx, scope, g.ID, GenFailed, "ambiguous side effect: provider outcome unknown", g.Usage)
	return final, executionError(ctx, "ambiguous_finalize", "provider", err)
}

// run performs the actual streaming generation under the held dispatch right.
func (s *Service) run(ctx context.Context, scope contracts.Scope, g Generation) (Generation, error) {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	// A stalled provider must still observe cancellation; polling only between
	// deltas cannot stop a stream that has stopped producing them.
	diagnosticGeneration := g // the worker below later changes its local g.Status
	go func(watchCtx context.Context, id string) {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				probe, cancel := context.WithTimeout(watchCtx, time.Second)
				requested, err := s.gens.ShouldCancel(probe, scope, id)
				if requested || err != nil {
					cause := "cancel_requested"
					if err != nil {
						cause = "cancel_probe_error"
					}
					logExecutionCancellation(probe, diagnosticGeneration, cause, "cancel_probe", err)
				}
				cancel()
				if requested || err != nil {
					stop()
					return
				}
			}
		}
	}(ctx, g.ID)
	var req ChatRequest
	if len(g.Request) > 0 {
		if err := json.Unmarshal(g.Request, &req); err != nil {
			return s.failCommitted(ctx, scope, g, "corrupt request", true)
		}
	}
	req.Model = g.Model

	if g.Status == GenPending {
		if _, err := s.gens.Transition(ctx, scope, g.ID, []string{GenPending}, GenRunning); err != nil {
			return Generation{}, executionError(ctx, "generation_running", "provider", err)
		}
		g.Status = GenRunning
	}

	client, err := s.clientFor(ctx, g)
	if err != nil {
		// Nothing has been sent to any provider, so nothing is unknown to the
		// ledger: the attempt is refused with its reason before dispatch. The
		// stable reason and the copy name go into the terminal record, so a
		// verifier reading the row can tell WHY it died -- and can see that the
		// recorded provider survived the refusal rather than being rewritten to
		// whatever client this deployment happened to have.
		cause := ReasonProviderClientUnavailable + ": provider " + g.Provider + " refused before send"
		if IsBaseURLConfigReason(contracts.ReasonOf(err)) {
			// Keep the configuration cause verbatim: it names the branch (path,
			// port, credentials, query) that made this copy unservable.
			cause = err.Error()
		}
		return s.failCommitted(ctx, scope, g, cause, true)
	}
	stream, err := client.Stream(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			logExecutionCancellation(ctx, g, "provider_open_context_done", "provider", err)
			return s.cancelled(ctx, scope, g, "", Usage{})
		}
		if IsPreSendRefusal(err) {
			// The guard refused before any byte left the process, so nothing is
			// unknown to the ledger: fail definitively, return the reservation
			// (settle charges zero actual), and never journal an unknown.
			return s.failCommitted(ctx, scope, g, ReasonModelEgressRefusedBeforeSend+": refused by network policy before send", true)
		}
		if IsTransportError(err) {
			// Request may have reached the provider: never assume, journal unknown.
			if s.ledger != nil {
				_, _ = s.ledger.MarkUnknown(ctx, scope, g.EffectKey)
			}
			return s.ambiguous(ctx, scope, g)
		}
		return s.failCommitted(ctx, scope, g, err.Error(), true)
	}
	defer func() { _ = stream.Close() }()

	if _, err := s.gens.Transition(ctx, scope, g.ID, []string{GenPending, GenRunning}, GenStreaming); err != nil {
		return Generation{}, executionError(ctx, "generation_streaming", "provider", err)
	}

	var collected string
	var toolCalls []ToolCall
	sawToolFinish := false
	// finishReason is the provider's own last stated terminal reason. It is kept
	// for every value, not just "tool_calls": when a turn ends with nothing to hand
	// the caller, the difference between "the provider hit the token limit" and "the
	// provider stopped for some other reason" is the whole diagnosis (DEF-A7-1,
	// measured live as finish="length", no content, 128 completion tokens).
	finishReason := ""
	checkEvery := s.checkN
	if checkEvery <= 0 {
		checkEvery = 8
	}
	for i := 0; ; i++ {
		d, err := stream.Next(ctx)
		if errors.Is(err, ErrStreamDone) {
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				logExecutionCancellation(ctx, g, "provider_next_context_done", "provider", err)
				return s.cancelled(ctx, scope, g, collected, stream.Usage())
			}
			if IsTransportError(err) {
				if s.ledger != nil {
					_, _ = s.ledger.MarkUnknown(ctx, scope, g.EffectKey)
				}
				g.Usage = stream.Usage()
				return s.ambiguous(ctx, scope, g)
			}
			return s.failWithUsage(ctx, scope, g, err.Error(), true, stream.Usage())
		}
		if d.Text != "" {
			collected += d.Text
			if err := s.gens.AppendOutput(ctx, scope, g.ID, d.Text, stream.Usage()); err != nil {
				logExecutionCancellation(ctx, g, "append_output_error", "provider", err)
				return s.cancelled(ctx, scope, g, collected, stream.Usage())
			}
		}
		if d.FinishReason == "tool_calls" {
			sawToolFinish = true
			finishReason = "tool_calls"
			toolCalls = d.ToolCalls
			continue
		}
		if d.FinishReason != "" {
			finishReason = d.FinishReason
		}
		if i%checkEvery == 0 {
			if cancel, _ := s.gens.ShouldCancel(ctx, scope, g.ID); cancel {
				logExecutionCancellation(ctx, g, "cancel_requested", "provider", nil)
				return s.cancelled(ctx, scope, g, collected, stream.Usage())
			}
		}
	}

	// The provider stream is finished. Persist its outcome even if the caller
	// disconnects exactly with the final usage/EOF frame.
	if ctx.Err() != nil {
		logExecutionCancellation(ctx, g, "completion_context_done", "provider", ctx.Err())
		return s.cancelled(ctx, scope, g, collected, stream.Usage())
	}
	settlement, cancelSettlement := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelSettlement()
	ctx = settlement

	if sawToolFinish && len(toolCalls) == 0 {
		// The provider said "tool_calls" and delivered none. Completing this
		// generation would hand the caller a terminal success with no text and
		// no calls -- the run loop could only stall or fabricate. Fail loudly;
		// the reservation settles through the same path as any other failure.
		return s.failWithUsage(ctx, scope, g, "provider finished with tool_calls but emitted no calls", true, stream.Usage())
	}
	finalOutput := collected
	if sawToolFinish {
		// The assistant's real message IS the calls: persist them in the
		// provider's own shape, wrapped with any accompanying text, so any
		// consumer of the durable generation can replay the turn faithfully.
		wire := make([]WireToolCall, 0, len(toolCalls))
		for _, tc := range toolCalls {
			args := string(tc.Arguments)
			if args == "" {
				args = "{}"
			}
			wire = append(wire, WireToolCall{ID: tc.ID, Type: "function",
				Function: WireFunctionCall{Name: tc.Name, Arguments: args}})
		}
		env, err := json.Marshal(struct {
			Text      string         `json:"text"`
			ToolCalls []WireToolCall `json:"tool_calls"`
		}{Text: collected, ToolCalls: wire})
		if err != nil {
			return s.failWithUsage(ctx, scope, g, "tool-call envelope: "+err.Error(), true, stream.Usage())
		}
		finalOutput = string(env)
	}
	usage := stream.Usage()
	if strings.TrimSpace(finalOutput) == "" {
		// A turn that handed back neither text nor tool calls is not a completed
		// generation, however the provider labelled its stop. Completing it would
		// report success on empty bytes: the run loop gets an empty assistant
		// message, can neither act on it nor stop honestly, and an artifact sealed
		// from it is fabricated content. This is the case measured live --
		// finish="length" with an empty completion -- and it ends FAILED WITH A
		// REASON, the way every corner-case row in PDF rev3.2 section 9 ends
		// (:3177 "failed with a reason", :3213; section 9 preamble makes each row a
		// negative-test candidate). Deciding this here, and not in the caller, is
		// the Model Gateway's own line of ownership: "operations and attempts of
		// generation" (:1626-1627).
		//
		// It is a refusal, not a loop: the tokens the provider really consumed are
		// charged, the reservation settles, and the caller sees a terminal FAILED
		// generation whose cause it can name. Re-asking the provider is the caller's
		// bounded attempt, never an unbounded spin here.
		return s.refuseEmptyCompletion(ctx, scope, g, finishReason, usage)
	}
	encodedOutput, err := json.Marshal(finalOutput)
	if err != nil || len(encodedOutput) > maxEncodedCompletionBytes {
		return s.failWithUsage(ctx, scope, g, "provider output exceeds encoded stream bound", true, usage)
	}
	// Boundary flush persists the volatile provider-output tail BEFORE the terminal
	// completion write. prepareCompletion validates output recomputed from the
	// durable delta ledger, so the fragments must be committed first; a failed flush
	// means no fragment set is durable and the completion cannot be acknowledged
	// (fail flush => no successful terminal). No-op in the default chunk mode.
	if err := s.gens.FlushGeneration(ctx, scope, g.ID); err != nil {
		logExecutionCancellation(ctx, g, "boundary_flush_error", "settlement", err)
		if contracts.Code(err) == contracts.ErrConflict || contracts.Code(err) == contracts.ErrForbidden {
			return s.cancelled(ctx, scope, g, collected, usage)
		}
		return Generation{}, executionError(ctx, "boundary_flush", "settlement", err)
	}
	if err = s.gens.prepareCompletion(ctx, scope, g.ID, collected, finalOutput, finishReason, usage); err != nil {
		if contracts.Code(err) == contracts.ErrForbidden || contracts.Code(err) == contracts.ErrConflict {
			logExecutionCancellation(ctx, g, "prepare_completion_refused", "settlement", err)
			return s.cancelled(ctx, scope, g, collected, usage)
		}
		return Generation{}, executionError(ctx, "prepare_completion", "settlement", err)
	}
	if err := s.settle(ctx, scope, g, int64(usage.TotalTokens)); err != nil {
		return Generation{}, err
	}
	res, _ := json.Marshal(resultPayload{Status: GenCompleted, Usage: usage, OutputSHA256: outputDigest(finalOutput), FinishReason: finishReason})
	if s.ledger != nil {
		if _, err := s.completeOwned(ctx, scope, g.EffectKey, res, false); err != nil {
			return Generation{}, executionError(ctx, "ledger_complete", "settlement", fmt.Errorf("model: ledger complete: %w", err))
		}
	}
	final, err := s.gens.FinalizeWithOutput(ctx, scope, g.ID, GenCompleted, "", finalOutput, usage, finishReason)
	return final, executionError(ctx, "generation_finalize", "settlement", err)
}

// cancelled stops a live generation, preserves observed usage or retains its
// reservation when billing is unknown, and records the terminal CANCELED state.
func (s *Service) cancelled(ctx context.Context, scope contracts.Scope, g Generation, collected string, usage Usage) (Generation, error) {
	// Disconnect stops inference, but cannot erase the usage already observed.
	settlement, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	ctx = settlement
	if usage.TotalTokens == 0 {
		if s.ledger != nil {
			if _, err := s.ledger.MarkUnknown(ctx, scope, g.EffectKey); err != nil {
				return Generation{}, executionError(ctx, "cancel_mark_unknown", "settlement", err)
			}
		}
		final, err := s.gens.Finalize(ctx, scope, g.ID, GenCanceled, "canceled mid-stream: provider billing unknown; reservation retained", usage)
		return final, executionError(ctx, "cancel_finalize", "settlement", err)
	}
	if err := s.settle(ctx, scope, g, int64(usage.TotalTokens)); err != nil {
		return Generation{}, err
	}
	res, _ := json.Marshal(resultPayload{Status: GenCanceled, Usage: usage})
	if s.ledger != nil {
		_, _ = s.completeOwned(ctx, scope, g.EffectKey, res, true)
	}
	final, err := s.gens.Finalize(ctx, scope, g.ID, GenCanceled, "canceled mid-stream", usage)
	return final, executionError(ctx, "cancel_finalize", "settlement", err)
}

// failCommitted records a deterministic provider failure and settles the budget
// to the tokens actually consumed (zero on immediate failures).
func (s *Service) failCommitted(ctx context.Context, scope contracts.Scope, g Generation, msg string, failed bool) (Generation, error) {
	return s.failWithUsage(ctx, scope, g, msg, failed, Usage{})
}

func (s *Service) failWithUsage(ctx context.Context, scope contracts.Scope, g Generation, msg string, failed bool, usage Usage) (Generation, error) {
	if err := s.settle(ctx, scope, g, int64(usage.TotalTokens)); err != nil {
		return Generation{}, err
	}
	res, _ := json.Marshal(resultPayload{Status: GenFailed, Error: msg, Usage: usage})
	if s.ledger != nil {
		if _, err := s.completeOwned(ctx, scope, g.EffectKey, res, failed); err != nil {
			return Generation{}, executionError(ctx, "failure_ledger_complete", "settlement", err)
		}
	}
	final, err := s.gens.Finalize(ctx, scope, g.ID, GenFailed, msg, usage)
	return final, executionError(ctx, "failure_finalize", "settlement", err)
}

// EmptyCompletionCause is the stable, greppable cause written to the generation
// row, the operations ledger and the operator journal when a provider turn ends
// with nothing to hand over. The words after it name the provider's own stop
// reason, which is the part an operator needs and the part a live flake has.
const EmptyCompletionCause = "provider_empty_completion"

// emptyCompletionMessage builds the terminal cause line. No request content and no
// provider payload appear in it: the finish reason is a label, not data.
func emptyCompletionMessage(finishReason string) string {
	finish := finishReason
	if finish == "" {
		finish = "unreported"
	}
	return EmptyCompletionCause + ": the provider ended the turn with no text and no tool calls (finish=" + finish + ")"
}

// IsEmptyCompletionRefusal reports whether a generation ended on that cause. A
// consumer that wants to re-ask -- under its own attempt bound -- asks through
// this, instead of pattern-matching prose.
func IsEmptyCompletionRefusal(g Generation) bool {
	return g.Status == GenFailed && strings.HasPrefix(g.Error, EmptyCompletionCause)
}

// refuseEmptyCompletion ends a contentless turn FAILED, and charges the tokens the
// provider actually consumed: the spend happened whether or not anything usable
// came back, and hiding it here is how a tree's cost stops matching the provider's
// usage. The reservation still settles through the same path, and the ledger keeps
// the usage so a replayed attempt converges on the same numbers.
func (s *Service) refuseEmptyCompletion(ctx context.Context, scope contracts.Scope, g Generation, finishReason string, usage Usage) (Generation, error) {
	msg := emptyCompletionMessage(finishReason)
	if err := s.settle(ctx, scope, g, int64(usage.TotalTokens)); err != nil {
		return Generation{}, err
	}
	res, _ := json.Marshal(resultPayload{Status: GenFailed, Error: msg, Usage: usage})
	if s.ledger != nil {
		_, _ = s.completeOwned(ctx, scope, g.EffectKey, res, true)
	}
	final, err := s.gens.Finalize(ctx, scope, g.ID, GenFailed, msg, usage)
	return final, executionError(ctx, "empty_completion_finalize", "settlement", err)
}

// settleAndFinish finalises from a ledger-stored result (recovery path).
func (s *Service) settleAndFinish(ctx context.Context, scope contracts.Scope, g Generation, status, errText string, usage Usage, failed bool, receipts ...resultPayload) (Generation, error) {
	if len(receipts) > 0 && receipts[0].OutputSHA256 != "" {
		// Load through the same generation scope; the compact receipt points to
		// owner data, never a caller-supplied URL or a different generation.
		current, err := s.gens.Get(ctx, scope, g.ID)
		if err != nil {
			return Generation{}, executionError(ctx, "recovery_generation_get", "settlement", err)
		}
		if outputDigest(current.Output) != receipts[0].OutputSHA256 {
			return Generation{}, executionError(ctx, "recovery_output_digest", "settlement", contracts.NewError(contracts.ErrUnavailable, "model: completion receipt output mismatch"))
		}
		receipts[0].Output = current.Output
	}
	charge := int64(usage.TotalTokens)
	if status == GenFailed && failed && charge == 0 {
		charge = 0
	}
	if err := s.settle(ctx, scope, g, charge); err != nil {
		return Generation{}, err
	}
	if status == "" {
		status = GenFailed
	}
	if len(receipts) > 0 {
		final, err := s.gens.FinalizeWithOutput(ctx, scope, g.ID, status, errText, receipts[0].Output, usage, receipts[0].FinishReason)
		return final, executionError(ctx, "recovery_finalize", "settlement", err)
	}
	final, err := s.gens.Finalize(ctx, scope, g.ID, status, errText, usage)
	return final, executionError(ctx, "recovery_finalize", "settlement", err)
}

// settle records actual usage before converging provider quota. Missing/deleted
// ledger facts never authorize refunds, and a crash replay adopts the durable
// settlement rather than refunding it with an inferred zero.
func (s *Service) settle(ctx context.Context, scope contracts.Scope, g Generation, actual int64) error {
	if actual < 0 {
		return contracts.NewError(contracts.ErrInvalid, "negative provider usage")
	}
	if s.budget != nil && g.BudgetID != "" && g.ReserveKey != "" {
		rec, err := s.budget.Settle(ctx, scope, g.BudgetID, g.ReserveKey, actual)
		if contracts.Code(err) == contracts.ErrConflict {
			rec, err = s.budget.GetReservation(ctx, scope, g.BudgetID, g.ReserveKey)
			if err == nil && !rec.Settled {
				err = contracts.NewError(contracts.ErrConflict, "reservation is not settled")
			}
		}
		if err != nil {
			return executionError(ctx, "budget_settle", "settlement", fmt.Errorf("model: budget settle: %w", err))
		}
		actual = rec.Actual
	}
	if s.quota != nil {
		if err := s.quota.RefundByOpKey(ctx, scope.Tenant, g.Key, actual); err != nil {
			slog.Warn("model: provider quota reconciliation failed", "tenant", scope.Tenant, "generation_key", g.Key, "err", err)
		}
	}
	return nil
}

// Cancel requests cancellation of a live generation and returns its state.
func (s *Service) Cancel(ctx context.Context, scope contracts.Scope, id string) (Generation, error) {
	g, err := s.gens.RequestCancel(ctx, scope, id)
	if err == nil && g.Status == GenCanceled && g.Error == "" {
		err = s.cancelUnsentReservation(ctx, scope, g)
	}
	return g, err
}

// RequestCancel can terminate PENDING work immediately. Only a fenced, never
// dispatched effect proves that its entire reservation is unused.
func (s *Service) cancelUnsentReservation(ctx context.Context, scope contracts.Scope, g Generation) error {
	if s.ledger != nil {
		op, err := s.ledger.Fence(ctx, scope, g.EffectKey)
		if err != nil {
			return err
		}
		if op.Status != operations.StatusFenced {
			return nil
		}
	}
	return s.settle(ctx, scope, g, 0)
}

// Usage returns the persisted usage of a generation.
func (s *Service) Usage(ctx context.Context, scope contracts.Scope, id string) (Generation, error) {
	return s.gens.Get(ctx, scope, id)
}

type resultPayload struct {
	OutputSHA256 string `json:"output_sha256,omitempty"`
	// Output is read only for backward-compatible inline receipts. New receipts reference generation-owned output.
	Output       string `json:"output,omitempty"`
	FinishReason string `json:"finish_reason,omitempty"`
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
	Usage        Usage  `json:"usage"`
}

// completeOwned accepts a late known outcome only from the executor that held
// the dispatch right. A control fence can turn that attempt UNKNOWN in flight;
// its positive receipt remains valid evidence for trusted reconciliation.
func (s *Service) completeOwned(ctx context.Context, scope contracts.Scope, key string, result json.RawMessage, failed bool) (operations.Operation, error) {
	op, err := s.ledger.Complete(ctx, scope, key, result, failed)
	if err == nil {
		return op, nil
	}
	current, e := s.ledger.Get(ctx, scope, key)
	if e != nil || current.Status != operations.StatusSideEffectUnknown {
		return op, err
	}
	return s.ledger.Reconcile(ctx, scope, key, result, failed)
}
