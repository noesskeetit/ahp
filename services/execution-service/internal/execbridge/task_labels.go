package execbridge

// The ceiling of the DATA, not only of the worker.
//
// A routing decision checked against the operator's deployment record answers
// "what is this worker allowed to talk to". It does not answer "what is THIS task
// carrying right now", which is the ceiling the platform's label journal keeps:
// labels accumulate per context and per grant, and a child can be widened by
// AddLabels without its deployment record changing at all (identity/journal.go,
// identity/labels.go). Checking only the deployment record therefore lets a task
// that has picked up a taint keep the route it had before it did -- the gate is
// real but stale, and staleness in a confidentiality gate is the failure.
//
// A missing or unreadable journal fails a production generation closed.
// A deployment record alone cannot describe labels acquired by this task.

import (
	"context"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// ErrTaskLabelJournalAbsent says the journal holds no row for this task under this
// scope. Production routing refuses a generation without its verified journal.
var ErrTaskLabelJournalAbsent = errors.New("execbridge: no label journal row for this context")

// Refusal reasons of the journal half of the routing ceiling. A refusal an
// operator cannot name is a refusal nobody can triage: the bare 500 these
// reasons replaced hid a bricked default mount for two measurement legs
// (DEF-DOTE2E-2). Reasons and identifiers only, never request content
// (PDF rev3.2 :347/:277).
const (
	// ReasonTaskLabelJournalAbsent: production routing is mounted and this
	// conversation has no journal row, so the labels this task carries right now
	// are unknowable. Fail closed.
	ReasonTaskLabelJournalAbsent = "task_label_journal_absent"
	// ReasonTaskLabelJournalUnreadable: the journal could not be read at all
	// (dependency failure). Also fail closed, and say which dependency is blind.
	ReasonTaskLabelJournalUnreadable = "task_label_journal_unreadable"
	// ReasonDeploymentLabelRecordUnreadable: the operator's deployment record, the
	// other half of the ceiling, is unreadable.
	ReasonDeploymentLabelRecordUnreadable = "deployment_label_record_unreadable"
	// ReasonGrantBindingMissing: no verified grant context/task reached the router,
	// so there is nothing to authorize and nothing to route.
	ReasonGrantBindingMissing = "grant_binding_missing"
	// ReasonCompletionCapExceeded: a step asked for more completion tokens than
	// this deployment's cap (AHP_EXEC_MAX_TOKENS). Refused, never clamped: a
	// silent clamp turns a misconfigured runner into a truncated answer nobody
	// asked for (DEF-DOTE2E-9).
	ReasonCompletionCapExceeded = "completion_cap_exceeded"
)

// TaskLabelJournal reads the current label set of one task from the platform's
// journal. The composition root implements it over Identity's store; the execution
// bridge stays free of the identity schema.
type TaskLabelJournal interface {
	// LabelsForContext returns the label journal accumulated for one conversation
	// context under this scope. ErrTaskLabelJournalAbsent says the journal holds no
	// row for it. The routed generation must then be refused.
	LabelsForContext(ctx context.Context, scope contracts.Scope, contextID string) (labels.Set, error)
}

// TaskGrantAuthority is implemented by the production journal adapter. Recheck
// the live task at every model step, including attempts after a worker restart.
type TaskGrantAuthority interface {
	AuthorizeModelTask(context.Context, contracts.Scope, string) error
}
