package execbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgconn"
)

// A reader can leave before Execute fails; only the producer still sees it.
func TestObservedExecutionLogsAfterReaderDisconnect(t *testing.T) {
	var output bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	reader, disconnect := context.WithCancel(context.Background())
	disconnect()
	owner := context.WithoutCancel(reader)
	pg := &pgconn.PgError{Code: "53300", Message: "PRIVATE_DSN_AND_PROVIDER_CONTENT"}
	failures := make(chan error, 1)
	scope := contracts.Scope{Tenant: "t", Subject: "s", Deployment: "d"}
	runObservedModelExecution(owner, scope, "generation-test", "subscription-test", func(ctx context.Context, got contracts.Scope, id string) (model.Generation, error) {
		if ctx.Err() != nil || got != scope || id != "generation-test" {
			t.Fatal("owner context/scope/generation changed")
		}
		return model.Generation{}, pg
	}, failures)
	if len(failures) != 1 || !errors.Is(<-failures, pg) {
		t.Fatal("producer changed or dropped the original error")
	}
	if strings.Contains(output.String(), "PRIVATE_DSN_AND_PROVIDER_CONTENT") {
		t.Fatal("raw lower cause leaked")
	}
	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatal("expected one structured event after reader left")
	}
	if event["msg"] != "model.execute_failed" || event["generation_id"] != "generation-test" || event["stream_attempt_id"] != "subscription-test" || event["sqlstate"] != "53300" {
		t.Fatalf("missing producer facts: %+v", event)
	}
}

func TestObservedExecutionSuccessDoesNotLogFailure(t *testing.T) {
	var output bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	failures := make(chan error, 1)
	runObservedModelExecution(context.Background(), contracts.Scope{}, "generation", "subscription", func(context.Context, contracts.Scope, string) (model.Generation, error) {
		return model.Generation{}, nil
	}, failures)
	if len(failures) != 0 || output.Len() != 0 {
		t.Fatal("successful Execute gained error behavior")
	}
}
