package model

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgconn"
)

type diagnosticPanicError struct{}

func (diagnosticPanicError) Error() string { panic("diagnostics must not format errors") }

// Losing the original chain would break the existing recovery/error contracts.
func TestExecutionDiagnosticsPreserveErrorChainAndSnapshot(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	ctx = beginExecutionDiagnostics(ctx)
	pg := &pgconn.PgError{Code: "53300", Message: "private-driver-message", Detail: "private-query"}
	err := executionError(ctx, "owner_connect", "execute", fmt.Errorf("connect: %w", pg))
	cancel()
	err = executionError(ctx, "run", "execute", err)
	d := InspectExecutionError(ctx, "fallback", "owner", err)
	var got *pgconn.PgError
	if !errors.As(err, &got) || got != pg || !errors.Is(err, pg) {
		t.Fatal("original driver error was lost")
	}
	if d.Phase != "owner_connect" || d.ContextRole != "execute" || d.ContextState != "active" || d.DeadlineRemainingMS <= 0 || !d.HasDeadline || d.AttemptID == "" || d.SQLState != "53300" {
		t.Fatalf("snapshot not preserved: %+v", d)
	}
	base := contracts.NewReasoned(contracts.ErrConflict, ReasonProviderClientUnavailable, "private-provider-detail")
	wrapped := executionError(context.Background(), "client_select", "provider", base)
	if contracts.Code(wrapped) != contracts.ErrConflict || contracts.ReasonOf(wrapped) != ReasonProviderClientUnavailable {
		t.Fatal("contract code/reason lost")
	}
	if wrapped.Error() != base.Error() {
		t.Fatal("client-visible error text changed")
	}
	if executionError(ctx, "unused", "execute", nil) != nil {
		t.Fatal("nil error became a refusal")
	}
}

func TestExecutionDiagnosticsSafeClassification(t *testing.T) {
	cases := []struct {
		name, class, code, sqlstate string
		err                         error
	}{
		{"postgres_capacity", "postgres", "", "53300", &pgconn.PgError{Code: "53300", Message: "private"}},
		{"postgres_serialization", "postgres", "", "40001", fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "40001"})},
		{"deadline", "deadline", "", "", context.DeadlineExceeded},
		{"canceled", "canceled", "", "", context.Canceled},
		{"network_timeout", "network_timeout", "", "", &net.DNSError{Name: "private-host", IsTimeout: true}},
		{"eof", "eof", "", "", io.EOF},
		{"truncated", "unexpected_eof", "", "", io.ErrUnexpectedEOF},
		{"transport", "transport", "", "", &transportError{diagnosticPanicError{}}},
		{"presend", "pre_send_refusal", "", "", &preSendRefusalError{diagnosticPanicError{}}},
		{"contract", "contract", contracts.ErrForbidden, "", contracts.NewError(contracts.ErrForbidden, "private-policy")},
		{"unknown", "unclassified", "", "", diagnosticPanicError{}},
		{"none", "none", "", "", nil},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			d := InspectExecutionError(context.Background(), "provider_stream", "provider", tt.err)
			if d.ErrorClass != tt.class || d.Code != tt.code || d.SQLState != tt.sqlstate {
				t.Fatalf("classification: %+v", d)
			}
		})
	}
}

func TestExecutionDiagnosticsLogsContainOnlySafeFacts(t *testing.T) {
	const secret = "DO_NOT_LOG_SENTINEL"
	errs := []error{
		&pgconn.PgError{Code: "53300", Message: secret, Detail: secret, Where: secret, InternalQuery: secret},
		&net.DNSError{Name: secret, Server: secret, Err: secret, IsTimeout: true},
		fmt.Errorf("provider returned 500: %s", secret),
		contracts.NewReasoned(secret, secret, secret),
		&pgconn.PgError{Code: secret, Message: secret},
		diagnosticPanicError{},
	}
	for _, err := range errs {
		var output bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&output, nil))
		d := InspectExecutionError(context.Background(), "provider_stream", "provider", err)
		logger.LogAttrs(context.Background(), slog.LevelError, "safe-test", d.LogAttrs()...)
		if strings.Contains(output.String(), secret) {
			t.Fatal("diagnostic leaked driver/provider/body/URL/code/reason content")
		}
		var event map[string]any
		if json.Unmarshal(output.Bytes(), &event) != nil || event["phase"] != "provider_stream" {
			t.Fatal("structured safe event missing")
		}
	}
}

func TestExecutionDiagnosticsDistinguishAttemptsAndCanceledContext(t *testing.T) {
	first := beginExecutionDiagnostics(context.Background())
	second := beginExecutionDiagnostics(first)
	a := InspectExecutionError(first, "owner_connect", "execute", context.Canceled)
	b := InspectExecutionError(second, "owner_connect", "execute", context.Canceled)
	if a.AttemptID == "" || b.AttemptID == "" || a.AttemptID == b.AttemptID || !a.IsCanceled || a.ContextState != "active" {
		t.Fatal("execution invocation or error/context distinction lost")
	}
	ctx, cancel := context.WithCancel(second)
	cancel()
	d := InspectExecutionError(ctx, "cancel_probe", "cancel_probe", nil)
	if d.ContextState != "canceled" || d.IsCanceled || d.ErrorClass != "none" {
		t.Fatalf("context and error were conflated: %+v", d)
	}
}
