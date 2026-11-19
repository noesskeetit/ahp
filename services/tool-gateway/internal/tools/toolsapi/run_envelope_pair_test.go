package toolsapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

type runEnvelopeStand struct {
	database   string
	handshake  string
	fixture    *mcpFixture
	toolSchema string
}

func envelopeInt(t *testing.T, variable string, fallback int) int {
	t.Helper()
	value := os.Getenv(variable)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		t.Fatalf("%s must be a non-negative integer", variable)
	}
	return n
}

func newRunEnvelopeStand(t *testing.T, mode, dsn string) runEnvelopeStand {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || cfg.ConnConfig.Host != "127.0.0.1" ||
		!strings.HasPrefix(cfg.ConnConfig.Database, "ahp_carbon_compact") {
		t.Fatal("benchmark requires an isolated local ahp_carbon_compact database")
	}
	e := newEnvForDatabase(t, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	benchScope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject}
	if err := e.svc.GrantOperatorPolicy(ctx, benchScope, "c", "write", "benchmark-operator"); err != nil {
		t.Fatal(err)
	}
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	issue := func(_ context.Context, scope contracts.Scope, task string) (string, error) {
		return e.signer.Sign(auth.Claims{
			Audience: tAudience, Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment,
			Subject: scope.Subject, Task: task, Root: "root-" + task, Context: "ctx-" + task,
			Grant: "grant-" + task, Depth: 1,
		}, 15*time.Minute)
	}
	routes := http.NewServeMux()
	if mode == "dispatch" {
		journal := statejournal.New(e.pool)
		if err := journal.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		signer, err := auth.NewSigner("benchmark-state", "benchmark-state-key", private)
		if err != nil {
			t.Fatal(err)
		}
		handler, err := statejournal.NewHTTPWithToolLeases(journal, e.verifier, authorize, signer)
		if err != nil {
			t.Fatal(err)
		}
		leaseVerifier := auth.NewVerifier("benchmark-state", map[string]ed25519.PublicKey{"benchmark-state-key": public})
		gateway, err := NewHandlerWithToolLeases(ctx, e.svc, e.verifier, authorize, leaseVerifier)
		if err != nil {
			t.Fatal(err)
		}
		routes.Handle("/v1/state/", handler)
		routes.Handle("/v1/tools/", gateway)
	} else {
		store := state.New(e.pool)
		if err := store.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if err := store.MigrateControl(ctx); err != nil {
			t.Fatal(err)
		}
		legacy, err := stateapi.New(store, e.verifier, authorize, issue)
		if err != nil {
			t.Fatal(err)
		}
		routes.Handle("/v1/state/", legacy)
		routes.Handle("/v1/tools/", e.srv.Config.Handler)
	}
	if mode == "compact" {
		journal := statejournal.New(e.pool)
		if err := journal.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		handler, err := statejournal.NewHTTP(journal, e.verifier, authorize)
		if err != nil {
			t.Fatal(err)
		}
		routes.Handle("/compact/v1/state/", http.StripPrefix("/compact", handler))
	}
	server := httptest.NewUnstartedServer(routes)
	server.TLS = e.srv.TLS.Clone()
	server.StartTLS()
	t.Cleanup(server.Close)
	token, err := issue(ctx, benchScope, "benchmark")
	if err != nil {
		t.Fatal(err)
	}
	handshake := writeProbeHandshake(t, t.TempDir(), server, e, token, "")
	if mode == "dispatch" {
		var config map[string]any
		data, err := os.ReadFile(handshake)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &config); err != nil {
			t.Fatal(err)
		}
		workers := make([]string, 10)
		for index := range workers {
			workers[index], err = issue(ctx, benchScope, "benchmark-worker-"+strconv.Itoa(index))
			if err != nil {
				t.Fatal(err)
			}
		}
		config["worker_tokens"] = workers
		data, err = json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(handshake, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return runEnvelopeStand{
		database: cfg.ConnConfig.Database, fixture: e.fixture,
		handshake: handshake, toolSchema: e.schema,
	}
}

// Compact mode can either use the legacy coordinator or dispatch turns from the
// journal. Both comparisons exercise the same native LangGraph and MCP server.
func TestLangGraphPairedRunEnvelope(t *testing.T) {
	python := os.Getenv("AHP_JOURNAL_BENCH_PYTHON")
	if python == "" {
		t.Skip("requires the pinned LangGraph environment and two local databases")
	}
	legacyDSN, compactDSN := os.Getenv("AHP_JOURNAL_BENCH_LEGACY_DSN"), os.Getenv("AHP_JOURNAL_BENCH_COMPACT_DSN")
	if legacyDSN == "" || compactDSN == "" || legacyDSN == compactDSN {
		t.Fatal("provide two distinct isolated database DSNs")
	}
	output := os.Getenv("AHP_JOURNAL_BENCH_OUTPUT")
	if output == "" {
		t.Fatal("set AHP_JOURNAL_BENCH_OUTPUT to an existing result directory")
	}
	repeats := envelopeInt(t, "AHP_JOURNAL_BENCH_REPEATS", 3)
	if repeats < 1 {
		t.Fatal("benchmark needs at least one repetition")
	}
	seed := envelopeInt(t, "AHP_JOURNAL_BENCH_SEED", 10000)
	history := envelopeInt(t, "AHP_JOURNAL_BENCH_HISTORY", 25)
	compactMode := os.Getenv("AHP_JOURNAL_BENCH_COMPACT_MODE")
	if compactMode == "" {
		compactMode = "compact"
	}
	if compactMode != "compact" && compactMode != "dispatch" {
		t.Fatal("benchmark compact mode must be compact or dispatch")
	}
	stands := map[string]runEnvelopeStand{
		"legacy": newRunEnvelopeStand(t, "legacy", legacyDSN), compactMode: newRunEnvelopeStand(t, compactMode, compactDSN),
	}
	if stands["legacy"].database == stands[compactMode].database {
		t.Fatal("benchmark must use separate database names")
	}
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= repeats; round++ {
		modes := []string{"legacy", compactMode}
		if round%2 == 0 {
			modes = []string{compactMode, "legacy"}
		}
		for _, concurrency := range []int{1, 10} {
			for _, mode := range modes {
				name := mode + "-r" + strconv.Itoa(round) + "-n" + strconv.Itoa(concurrency)
				ok := t.Run(name, func(t *testing.T) {
					stand := stands[mode]
					seedCount := 0
					if round == 1 && concurrency == 1 {
						seedCount = seed
					}
					args := []string{"profiling/state-journal/run_envelope_pair.py", mode, strconv.Itoa(concurrency),
						"--seed", strconv.Itoa(seedCount), "--history", strconv.Itoa(history)}
					if psql := os.Getenv("AHP_JOURNAL_BENCH_PSQL"); psql != "" {
						args = append(args, "--psql", psql, "--sql-target-db", stand.database)
					}
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
					defer cancel()
					cmd := exec.CommandContext(ctx, python, args...)
					cmd.Dir = repo
					cmd.Env = append(os.Environ(),
						"AHP_STATE_HANDSHAKE="+stand.handshake,
						"PYTHONPATH="+filepath.Join(repo, "sdk", "python"), "PYTHONDONTWRITEBYTECODE=1",
					)
					var stderr bytes.Buffer
					cmd.Stderr = &stderr
					before := atomic.LoadInt32(&stand.fixture.effCount)
					raw, err := cmd.Output()
					if err != nil || !json.Valid(raw) {
						t.Fatalf("invalid benchmark result: %v\n%s%s", err, raw, stderr.Bytes())
					}
					if got := atomic.LoadInt32(&stand.fixture.effCount) - before; int(got) != 100+history*concurrency {
						t.Fatalf("MCP upstream effects = %d, want %d", got, 100+history*concurrency)
					}
					if err := os.WriteFile(filepath.Join(output, name+".json"), raw, 0600); err != nil {
						t.Fatal(err)
					}
					t.Logf("turns=100 effects=100 verified_history=%d", history*concurrency)
				})
				if !ok {
					return
				}
			}
		}
	}
	if dump := os.Getenv("AHP_JOURNAL_BENCH_PGDUMP"); dump != "" {
		for _, item := range []struct {
			mode, dsn, stateSchema string
		}{
			{"legacy", legacyDSN, "ahp_state"},
			{compactMode, compactDSN, "ahp_journal"},
		} {
			schemas := []string{"--schema=" + item.stateSchema, "--schema=" + stands[item.mode].toolSchema}
			if item.mode == "compact" {
				schemas = append(schemas, "--schema=ahp_state")
			}
			args := append([]string{"--schema-only", "--no-owner", "--no-privileges", "--dbname=" + item.dsn}, schemas...)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			cmd := exec.CommandContext(ctx, dump, args...)
			raw, err := cmd.Output()
			cancel()
			if err != nil {
				t.Fatalf("schema dump for %s: %v", item.mode, err)
			}
			if err := os.WriteFile(filepath.Join(output, item.mode+"-schema.sql"), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
