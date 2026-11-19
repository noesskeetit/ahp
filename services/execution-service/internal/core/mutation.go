package core

// mutation.go -- the built-in runner's share of the State mutation contract.
//
// The published contract is unambiguous (docs/state.md, internal/state/intents.go,
// and the same shape the compute plane has always enforced in internal/compute.Exec's
// mayDispatch):
//
//   - an effect is JOURNALED before it is attempted, under the run's live lease;
//   - replaying the same journal key returns the ORIGINAL outcome instead of applying
//     the effect a second time;
//   - an outcome that cannot be confirmed becomes UNKNOWN, and UNKNOWN is never
//     auto-replayed -- it is reconciled explicitly or not at all.
//
// Before this file the Core loop journalled a tool call and then ignored the answer:
// it keyed the row on the model's own tool-call id (a value the model re-mints on
// every physical attempt, so a re-delivered run could never find the row an earlier
// life wrote), never advanced the row past `recorded`, and discarded the journal's
// error. A run delivered twice therefore applied the same external effect twice, and a
// run that died between an effect and its outcome left behind a row saying "effect not
// yet attempted" -- the one status that invites a replay. The journal was a log line
// beside the effect, not a guard on it.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// intentNode namespaces the Core loop's journal rows. State keys an intent by
// (run_id, node, call), so the node is this loop's identity within a run.
const intentNode = "core"

const (
	// mutationWriteAttempts bounds how often one journal write is repeated while the
	// plane has not confirmed it. Repeats are safe by construction: they resend the
	// identical (fence, node, call, status, result), and State answers a repeat of an
	// already-applied transition with the stored row instead of a second effect.
	mutationWriteAttempts = 3
	// mutationWriteTimeout bounds ONE journal write. The budget belongs to the write,
	// never to the run: a shared run-scoped deadline turns a healthy long turn into a
	// journal that refuses every effect the moment it expires.
	mutationWriteTimeout = 5 * time.Second
)

// mutationJournal guards the external effects of ONE life of one run. A nil journal is
// inert: a loop with no State client, or a caller carrying no run token
// (internal/executor, internal/execbridge), behaves exactly as it did before.
type mutationJournal struct {
	state  *space.StateClient
	token  string
	runID  string
	worker string
	epoch  int64
	log    *slog.Logger
	// next is the run's mutation coordinate: a POSITION in the run's sequence of
	// effects, not a per-process counter and not the model's tool-call id. A
	// re-delivery of the same request produces the same effects in the same order, so
	// it lands on the same coordinates and meets the recorded outcome instead of
	// performing the effect again. That is the PDF's (task, node, call_index) effect
	// key (see pkg/sdk.OperationKey), and it is the rule the durable step stream
	// already applies to step bodies with space.StepKey.
	next int
}

// newMutationJournal builds the journal for one life. A life that resumes from a
// restored checkpoint continues the coordinate sequence where the restored history
// leaves it: every effect an earlier life completed put exactly one tool message into
// that history, so counting them is durable evidence of how many coordinates are
// taken. It can only UNDER-count (a life that died before its tool message was
// checkpointed is not counted) and under-counting is the safe direction: the earlier
// row is then met at its coordinate, where its outcome is replayed or refused --
// never silently re-run.
func (r *Runtime) newMutationJournal(req *space.CoreRequest) *mutationJournal {
	if r.cfg.State == nil || req.TaskToken == "" {
		return nil
	}
	return &mutationJournal{
		state:  r.cfg.State,
		token:  req.TaskToken,
		runID:  req.RunID,
		worker: req.Worker,
		epoch:  req.Epoch,
		log:    r.log(),
		next:   priorMutations(req),
	}
}

// priorMutations counts the effects an earlier life of this run already carried out,
// read out of the history the coordinator restored.
func priorMutations(req *space.CoreRequest) int {
	if req.Checkpoint == nil || len(req.Messages) == 0 {
		return 0
	}
	var restored []ChatMessage
	if err := json.Unmarshal(req.Messages, &restored); err != nil {
		return 0
	}
	n := 0
	for _, m := range restored {
		if m.Role == string(space.RoleTool) {
			n++
		}
	}
	return n
}

// callKey returns the next mutation coordinate, or "" when the journal is inert. It
// advances once per effect ATTEMPT, so two tool calls in one model turn are two
// coordinates.
func (j *mutationJournal) callKey() string {
	if j == nil {
		return ""
	}
	key := strconv.Itoa(j.next)
	j.next++
	return key
}

// begin journals one effect before it happens and decides whether the effect may run.
//
// It returns (result, true, nil) when the mutation is already resolved -- the caller
// must serve that recorded result and must NOT touch the world again -- and
// (nil, false, refusal) when the effect must not run. Only a row still in `recorded`
// (journal-confirmed, never dispatched by anyone) authorises the effect.
func (j *mutationJournal) begin(ctx context.Context, call, tool string, args json.RawMessage) (json.RawMessage, bool, error) {
	if j == nil || call == "" {
		return nil, false, nil
	}
	var status string
	err := j.repeat(ctx, "RecordIntent", func(ctx context.Context) error {
		intent, err := j.state.RecordIntent(ctx, j.token, j.runID, j.worker, j.epoch,
			intentNode, call, "tool", tool, args)
		if err != nil {
			return err
		}
		status = intent.Status
		return nil
	})
	if err != nil {
		return nil, false, j.refuse(tool, call, "could not be recorded by the mutation journal", err)
	}

	switch status {
	case string(space.IntentRecorded):
		// Never dispatched, in this life or any other: performing it now is not a
		// replay. Marking it dispatched first is what makes a later crash here an
		// AMBIGUITY instead of a blank -- see resolve.
		if err := j.update(ctx, call, string(space.IntentDispatched), nil); err != nil {
			return nil, false, j.refuse(tool, call,
				"could not be marked dispatched before the effect", err)
		}
		return nil, false, nil
	case string(space.IntentSucceeded), string(space.IntentFailed):
		// The outcome is durable. Replay it; the effect is not performed again.
		result, err := j.readResult(ctx, call)
		if err != nil {
			return nil, false, j.refuse(tool, call,
				"is already resolved but its recorded result could not be read back", err)
		}
		return result, true, nil
	default:
		// dispatched / unknown / fenced: the effect may or may not have reached the
		// world. Nothing here may guess, and nothing here may replay it.
		return nil, false, j.refuse(tool, call,
			"is left in status '"+status+"', which is never auto-replayed; reconcile it with ReconcileIntent", nil)
	}
}

// resolve closes a mutation with the outcome of its effect. A terminal write that
// cannot be confirmed after its retries becomes UNKNOWN, because from here the honest
// answer about the world is "it happened, and this run cannot prove what it produced".
func (j *mutationJournal) resolve(ctx context.Context, call string, result json.RawMessage, effectErr error) {
	if j == nil || call == "" {
		return
	}
	status := string(space.IntentSucceeded)
	switch {
	case unconfirmedOutcome(ctx, effectErr):
		status = string(space.IntentUnknown)
	case effectErr != nil:
		status = string(space.IntentFailed)
	}
	payload := result
	if status != string(space.IntentSucceeded) {
		payload = nil
	}
	if err := j.update(ctx, call, status, payload); err != nil {
		if status == string(space.IntentUnknown) {
			j.warn("core: mutation outcome is unrecordable and marking it unknown failed too", call, err)
			return
		}
		if uerr := j.update(ctx, call, string(space.IntentUnknown), nil); uerr != nil {
			j.warn("core: mutation outcome could not be recorded and marking it unknown failed too", call, err)
			return
		}
		j.note("core: mutation outcome could not be recorded, the journal holds it as unknown", call, err)
	}
}

// unconfirmedOutcome reports the one case where the tool layer cannot answer for the
// world: the attempt was cut short (cancelled, or its deadline passed) while the
// effect was in flight, so it may or may not have landed. An error the tool ANSWERED
// with is an answered failure, not an ambiguity.
func unconfirmedOutcome(ctx context.Context, effectErr error) bool {
	if ctx.Err() != nil || contracts.ReasonOf(effectErr) == "SIDE_EFFECT_UNKNOWN" {
		return true
	}
	return errors.Is(effectErr, context.Canceled) || errors.Is(effectErr, context.DeadlineExceeded)
}

func (j *mutationJournal) readResult(ctx context.Context, call string) (json.RawMessage, error) {
	intent, err := j.state.GetIntent(ctx, j.token, j.runID, intentNode, call)
	if err != nil {
		return nil, err
	}
	return intent.Result, nil
}

func (j *mutationJournal) update(ctx context.Context, call, status string, result json.RawMessage) error {
	return j.repeat(ctx, "UpdateIntent:"+status, func(ctx context.Context) error {
		_, err := j.state.UpdateIntent(ctx, j.token, j.runID, j.worker, j.epoch,
			intentNode, call, status, result)
		return err
	})
}

// repeat runs one journal write, repeating the IDENTICAL write while the failure
// carries no contract code.
//
// An EMPTY code means the failure never arrived as a refusal: the connection died, the
// deadline passed, or State answered 5xx. The write may therefore already be durable
// and only its ACK was lost -- which is exactly why resending the identical write is
// safe and why refusing here would cost a legitimate effect. A DEFINITIVE code says
// the opposite: a fence (STALE_EPOCH), a permission, an argument or a key reuse refused
// it, and a fence does not loosen because the loser asks again.
//
// Every write runs on a context detached from the loop's, bounded per write: a
// cancellation that arrives while an effect is in flight must not also delete the only
// record that the effect was attempted.
func (j *mutationJournal) repeat(ctx context.Context, op string, write func(context.Context) error) error {
	var last error
	for attempt := 1; attempt <= mutationWriteAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(time.Duration(attempt*attempt) * 50 * time.Millisecond)
		}
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationWriteTimeout)
		err := write(actx)
		cancel()
		if err == nil {
			return nil
		}
		last = err
		if contracts.Code(err) != "" {
			return err
		}
		j.warn("core: mutation journal write unconfirmed, repeating the identical write", op, err)
	}
	return last
}

func (j *mutationJournal) refuse(tool, call, why string, cause error) error {
	msg := "tool \"" + tool + "\" refused: the run's mutation \"" + call + "\" " + why
	if cause != nil {
		msg += ": " + cause.Error()
	}
	j.warn(msg, call, cause)
	return errRefusal{msg: msg}
}

func (j *mutationJournal) warn(msg, call string, cause error) {
	if j == nil || j.log == nil {
		return
	}
	if cause != nil {
		j.log.Warn(msg, "run_id", j.runID, "node", intentNode, "call", call, "err", cause.Error())
		return
	}
	j.log.Warn(msg, "run_id", j.runID, "node", intentNode, "call", call)
}

func (j *mutationJournal) note(msg, call string, cause error) {
	if j == nil || j.log == nil {
		return
	}
	if cause != nil {
		j.log.Info(msg, "run_id", j.runID, "node", intentNode, "call", call, "err", cause.Error())
		return
	}
	j.log.Info(msg, "run_id", j.runID, "node", intentNode, "call", call)
}

// errRefusal is a refused effect: an ordinary error to the loop (the model is told and
// the step is durable) rather than a run abort, because refusing one mutation must not
// throw away the work a turn has already done.
type errRefusal struct{ msg string }

func (e errRefusal) Error() string { return e.msg }

// EffectRefused reports whether an error is the mutation journal refusing an effect
// rather than a tool answering with a failure.
func EffectRefused(err error) bool {
	var target errRefusal
	return errors.As(err, &target)
}
