package model

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"reflect"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgconn"
)

// ExecutionDiagnostics contains content-free facts about a failed operation.
// It deliberately excludes error text, queries, connection details and payloads.
type ExecutionDiagnostics struct {
	Phase, AttemptID, ErrorClass, ErrorType, Code, Reason, SQLState string
	ContextRole, ContextState                                       string
	HasDeadline, IsCanceled, IsDeadline, NetTimeout                 bool
	TransportError, PreSendRefusal                                  bool
	DeadlineUnixMS, DeadlineRemainingMS, ElapsedMS                  int64
}

type executionAttemptKey struct{}
type executionAttempt struct {
	id      string
	started time.Time
}

func beginExecutionDiagnostics(ctx context.Context) context.Context {
	return context.WithValue(ctx, executionAttemptKey{}, executionAttempt{contracts.NewID(), time.Now()})
}

// Embedding preserves the existing Error method without formatting the cause.
type executionDiagnosticError struct {
	error
	details ExecutionDiagnostics
}

func (e *executionDiagnosticError) Unwrap() error { return e.error }

func executionError(ctx context.Context, phase, role string, err error) error {
	if err == nil {
		return nil
	}
	var annotated *executionDiagnosticError
	if errors.As(err, &annotated) {
		return err // keep the deepest operation and its pre-cleanup snapshot
	}
	return &executionDiagnosticError{error: err, details: InspectExecutionError(ctx, phase, role, err)}
}

// InspectExecutionError extracts typed facts without evaluating error messages.
// An annotated error retains the context snapshot taken before cleanup canceled it.
func InspectExecutionError(ctx context.Context, phase, role string, err error) ExecutionDiagnostics {
	var annotated *executionDiagnosticError
	if errors.As(err, &annotated) && annotated != nil {
		return annotated.details
	}
	now := time.Now()
	d := ExecutionDiagnostics{Phase: phase, ContextRole: role, ContextState: "active", ErrorClass: "none"}
	if attempt, ok := ctx.Value(executionAttemptKey{}).(executionAttempt); ok {
		d.AttemptID, d.ElapsedMS = attempt.id, now.Sub(attempt.started).Milliseconds()
	}
	switch ctx.Err() {
	case context.Canceled:
		d.ContextState = "canceled"
	case context.DeadlineExceeded:
		d.ContextState = "deadline"
	}
	if deadline, ok := ctx.Deadline(); ok {
		d.HasDeadline, d.DeadlineUnixMS, d.DeadlineRemainingMS = true, deadline.UnixMilli(), deadline.Sub(now).Milliseconds()
	}
	if err == nil {
		return d
	}
	d.ErrorType = reflect.TypeOf(err).String()
	switch code := contracts.Code(err); code {
	case contracts.ErrNotFound, contracts.ErrConflict, contracts.ErrStaleEpoch, contracts.ErrForbidden,
		contracts.ErrInvalid, contracts.ErrKeyReuse, contracts.ErrLeaseLost, contracts.ErrUnavailable:
		d.Code = code
	}
	switch reason := contracts.ReasonOf(err); reason {
	case ReasonProviderClientUnavailable, ReasonModelEgressRefusedBeforeSend:
		d.Reason = reason
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) && pg != nil && safeSQLState(pg.Code) {
		d.SQLState = pg.Code
	}
	d.IsCanceled = errors.Is(err, context.Canceled)
	d.IsDeadline = errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
	var network net.Error
	isNetwork := errors.As(err, &network)
	d.NetTimeout = isNetwork && network.Timeout()
	d.TransportError, d.PreSendRefusal = IsTransportError(err), IsPreSendRefusal(err)
	switch {
	case d.SQLState != "":
		d.ErrorClass = "postgres"
	case d.IsDeadline:
		d.ErrorClass = "deadline"
	case d.IsCanceled:
		d.ErrorClass = "canceled"
	case d.PreSendRefusal:
		d.ErrorClass = "pre_send_refusal"
	case d.TransportError:
		d.ErrorClass = "transport"
	case d.NetTimeout:
		d.ErrorClass = "network_timeout"
	case isNetwork:
		d.ErrorClass = "network"
	case errors.Is(err, io.ErrUnexpectedEOF):
		d.ErrorClass = "unexpected_eof"
	case errors.Is(err, io.EOF):
		d.ErrorClass = "eof"
	case d.Code != "":
		d.ErrorClass = "contract"
	default:
		d.ErrorClass = "unclassified"
	}
	return d
}

func safeSQLState(code string) bool {
	if len(code) != 5 {
		return false
	}
	for _, c := range code {
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z') {
			return false
		}
	}
	return true
}

// LogAttrs returns only scalar diagnostics, never an error or its contents.
func (d ExecutionDiagnostics) LogAttrs() []slog.Attr {
	return []slog.Attr{
		slog.String("phase", d.Phase), slog.String("execution_attempt_id", d.AttemptID),
		slog.String("error_class", d.ErrorClass), slog.String("error_type", d.ErrorType),
		slog.String("contract_code", d.Code), slog.String("reason", d.Reason), slog.String("sqlstate", d.SQLState),
		slog.Bool("error_canceled", d.IsCanceled), slog.Bool("error_deadline", d.IsDeadline), slog.Bool("net_timeout", d.NetTimeout),
		slog.Bool("transport_error", d.TransportError), slog.Bool("pre_send_refusal", d.PreSendRefusal),
		slog.String("ctx_role", d.ContextRole), slog.String("ctx_state", d.ContextState),
		slog.Bool("has_deadline", d.HasDeadline), slog.Int64("deadline_unix_ms", d.DeadlineUnixMS),
		slog.Int64("deadline_remaining_ms", d.DeadlineRemainingMS), slog.Int64("elapsed_ms", d.ElapsedMS),
	}
}

func logExecutionCancellation(ctx context.Context, g Generation, cause, role string, err error) {
	attrs := InspectExecutionError(ctx, cause, role, err).LogAttrs()
	attrs = append(attrs, slog.String("generation_id", g.ID), slog.String("task_id", g.Task), slog.String("cause", cause))
	slog.LogAttrs(ctx, slog.LevelWarn, "model.execution_cancelled", attrs...)
}
