package model

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This first RED uses existing APIs/columns. It must fail on d69 because the
// generation row itself grows after the first delta, not because a new API or
// migration column is missing. Every test uses only the approved isolated DB.
func TestDeltaLedgerStreamingStorageRemainsUnmaterialized(t *testing.T) {
	e := newModelEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	request := chatReq(strings.Repeat("x", 102400))
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "immutable-4000", "fixture", request, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	var expected strings.Builder
	for i := 0; i < 4000; i++ {
		fragment := fmt.Sprintf("result%04d ", i)
		expected.WriteString(fragment)
		usage := Usage{PromptTokens: 11, CompletionTokens: i + 1, TotalTokens: i + 12}
		if err = e.gens.AppendOutput(ctx, e.scope, g.ID, fragment, usage); err != nil {
			t.Fatal(err)
		}
		if i != 0 && (i+1)%256 != 0 && i != 3999 {
			continue
		}
		full, err := e.gens.Get(ctx, e.scope, g.ID)
		if err != nil {
			t.Fatal(err)
		}
		if full.Output != expected.String() || full.DeltaSequence != int64(i+1) || full.Usage != usage {
			t.Fatalf("public Get lost durable content: seq=%d bytes=%d", full.DeltaSequence, len(full.Output))
		}
		projected, err := e.gens.GetForStream(ctx, e.scope, g.ID)
		if err != nil {
			t.Fatal(err)
		}
		copy := full
		copy.Request = nil
		if !reflect.DeepEqual(copy, projected) {
			t.Fatal("existing GetForStream semantics changed")
		}
		replay, created, err := e.gens.Submit(ctx, e.scope, "task", "immutable-4000", "fixture", request, "", "", 0)
		if err != nil || created || replay.Output != full.Output || replay.DeltaSequence != full.DeltaSequence {
			t.Fatal("idempotent lookup lost current output")
		}
		var storedBytes int
		if err = e.pool.QueryRow(ctx, `SELECT octet_length(output) FROM ahp_model.generations WHERE id=$1`, g.ID).Scan(&storedBytes); err != nil {
			t.Fatal(err)
		}
		if storedBytes != 0 {
			t.Fatalf("growing generation output duplicates durable ledger: bytes=%d after %d fragments; expected 0 before preparation", storedBytes, i+1)
		}
	}
	if len(expected.String()) != 44000 || outputDigest(expected.String()) != "7769e72b2888273b62509ce0855b9f70ed16924c2035d6becfa367c7b0416d61" {
		t.Fatal("independent output fixture drift")
	}
	after := int64(0)
	for after < 4000 {
		page, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, after, 256)
		if err != nil || len(page) == 0 {
			t.Fatal("missing durable delta page", err)
		}
		for _, delta := range page {
			after++
			if delta.Sequence != after || delta.Text != fmt.Sprintf("result%04d ", after-1) {
				t.Fatal("delta identity/boundary changed")
			}
		}
	}
	if err = e.gens.prepareCompletion(ctx, e.scope, g.ID, expected.String(), expected.String(), "stop", Usage{TotalTokens: 4011}); err != nil {
		t.Fatal(err)
	}
	var stored string
	var fromDeltas bool
	if err = e.pool.QueryRow(ctx, `SELECT output,output_from_deltas FROM ahp_model.generations WHERE id=$1`, g.ID).Scan(&stored, &fromDeltas); err != nil {
		t.Fatal(err)
	}
	if stored != expected.String() || fromDeltas {
		t.Fatal("prepared output not durably materialized")
	}
	final, err := e.gens.FinalizeWithOutput(ctx, e.scope, g.ID, GenCompleted, "", expected.String(), Usage{TotalTokens: 4011}, "stop")
	if err != nil || final.Output != expected.String() || final.DeltaSequence != 4000 || final.FinishReason != "stop" {
		t.Fatal("terminal output changed", err)
	}
}

func TestDeltaLedgerInsertFailureRollsBackParentMetadata(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "rollback", "fixture", chatReq("test"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	usage := Usage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "a", usage); err != nil {
		t.Fatal(err)
	}
	before, err := e.gens.Get(ctx, e.scope, g.ID)
	if err != nil {
		t.Fatal(err)
	}
	// A deliberate fixture PK collision makes the INSERT fail after parent UPDATE.
	if _, err = e.pool.Exec(ctx, `INSERT INTO ahp_model.generation_deltas(generation_id,sequence,text) VALUES($1,2,'collision')`, g.ID); err != nil {
		t.Fatal(err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "b", Usage{TotalTokens: 99}); err == nil {
		t.Fatal("delta insert collision succeeded")
	}
	if _, err = e.pool.Exec(ctx, `DELETE FROM ahp_model.generation_deltas WHERE generation_id=$1 AND sequence=2`, g.ID); err != nil {
		t.Fatal(err)
	}
	after, err := e.gens.Get(ctx, e.scope, g.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("failed transaction changed sequence/usage/output/time", err)
	}
	var bytes int64
	if err = e.pool.QueryRow(ctx, `SELECT streamed_bytes FROM ahp_model.generations WHERE id=$1`, g.ID).Scan(&bytes); err != nil || bytes != 1 {
		t.Fatal("byte counter escaped failed transaction", err)
	}
}

func TestDeltaLedgerConcurrentAppendAndReadRemainSnapshotConsistent(t *testing.T) {
	e := newModelEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "concurrent-ledger", "fixture", chatReq("test"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	errors := make(chan error, 32)
	finished := make(chan struct{})
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			errors <- e.gens.AppendOutput(ctx, e.scope, g.ID, fmt.Sprintf("result%04d ", i), Usage{})
		}(i)
	}
	go func() { workers.Wait(); close(errors); close(finished) }()
	for {
		current, err := e.gens.Get(ctx, e.scope, g.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(current.Output) != int(current.DeltaSequence)*11 {
			t.Fatal("Get mixed different ledger/parent snapshots")
		}
		select {
		case <-finished:
			goto joined
		case <-ctx.Done():
			t.Fatal("concurrent append/read timeout")
		case <-time.After(time.Millisecond):
		}
	}
joined:
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	page, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 0, 256)
	if err != nil || len(page) != 32 {
		t.Fatal("concurrent delta count", err)
	}
	unique := map[string]bool{}
	var joined strings.Builder
	for i, d := range page {
		if d.Sequence != int64(i+1) || unique[d.Text] {
			t.Fatal("non-contiguous or duplicate concurrent delta")
		}
		unique[d.Text] = true
		joined.WriteString(d.Text)
	}
	for i := 0; i < 32; i++ {
		if !unique[fmt.Sprintf("result%04d ", i)] {
			t.Fatal("lost concurrent fragment")
		}
	}
	full, err := e.gens.Get(ctx, e.scope, g.ID)
	if err != nil || full.Output != joined.String() {
		t.Fatal("concurrent ordered aggregate mismatch", err)
	}
}

func TestDeltaLedgerMigrationPreservesLegacyAndNewRows(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	statuses := []string{GenPending, GenRunning, GenStreaming, GenCompleted, GenFailed, GenCanceled}
	for i, status := range statuses {
		g, _, err := e.gens.Submit(ctx, e.scope, "task", fmt.Sprintf("legacy-%d", i), "fixture", chatReq("stored input"), "", "", 0)
		if err != nil {
			t.Fatal(err)
		}
		legacy := fmt.Sprintf("legacy output %d", i)
		// This models a pre-v8 row: authoritative nonempty output, no delta ledger.
		if _, err = e.pool.Exec(ctx, `UPDATE ahp_model.generations SET output_from_deltas=false,output=$2,status=$3 WHERE id=$1`, g.ID, legacy, status); err != nil {
			t.Fatal(err)
		}
		before, err := e.gens.Get(ctx, e.scope, g.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err = e.gens.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		after, err := e.gens.Get(ctx, e.scope, g.ID)
		if err != nil || !reflect.DeepEqual(before, after) || after.Output != legacy {
			t.Fatal("migration lost legacy content", err)
		}
		if !terminal(status) {
			if err = e.gens.AppendOutput(ctx, e.scope, g.ID, " tail", Usage{}); err != nil {
				t.Fatal(err)
			}
			after, err = e.gens.Get(ctx, e.scope, g.ID)
			if err != nil || after.Output != legacy+" tail" {
				t.Fatal("legacy append lost prefix", err)
			}
		}
	}
	fresh, _, err := e.gens.Submit(ctx, e.scope, "task", "still-ledger", "fixture", chatReq("test"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, fresh.ID, "new content", Usage{}); err != nil {
		t.Fatal(err)
	}
	if err = e.gens.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var flag bool
	var stored int
	if err = e.pool.QueryRow(ctx, `SELECT output_from_deltas,octet_length(output) FROM ahp_model.generations WHERE id=$1`, fresh.ID).Scan(&flag, &stored); err != nil || !flag || stored != 0 {
		t.Fatal("repeat migration reset new representation", err)
	}
	full, err := e.gens.Get(ctx, e.scope, fresh.ID)
	if err != nil || full.Output != "new content" {
		t.Fatal("repeat migration lost ledger content", err)
	}
}

func TestDeltaLedgerReadsRejectGapAndByteCounterMismatch(t *testing.T) {
	for _, kind := range []string{"gap", "bytes", "sequence", "future"} {
		t.Run(kind, func(t *testing.T) {
			e := newModelEnv(t)
			ctx := context.Background()
			request := chatReq("test")
			g, _, err := e.gens.Submit(ctx, e.scope, "task", "corruption", "fixture", request, "", "", 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, fragment := range []string{"a", "b"} {
				if err = e.gens.AppendOutput(ctx, e.scope, g.ID, fragment, Usage{}); err != nil {
					t.Fatal(err)
				}
			}
			query := map[string]string{"gap": `DELETE FROM ahp_model.generation_deltas WHERE generation_id=$1 AND sequence=1`, "bytes": `UPDATE ahp_model.generations SET streamed_bytes=streamed_bytes+1 WHERE id=$1`, "sequence": `UPDATE ahp_model.generations SET delta_sequence=delta_sequence+1 WHERE id=$1`, "future": `INSERT INTO ahp_model.generation_deltas(generation_id,sequence,text) VALUES($1,3,'future')`}[kind]
			if _, err = e.pool.Exec(ctx, query, g.ID); err != nil {
				t.Fatal(err)
			}
			if _, err = e.gens.Get(ctx, e.scope, g.ID); contracts.Code(err) != contracts.ErrUnavailable {
				t.Fatal("Get silently accepted incomplete ledger", err)
			}
			if _, err = e.gens.GetForStream(ctx, e.scope, g.ID); contracts.Code(err) != contracts.ErrUnavailable {
				t.Fatal("GetForStream silently accepted incomplete ledger", err)
			}
			if _, _, err = e.gens.Submit(ctx, e.scope, "task", "corruption", "fixture", request, "", "", 0); contracts.Code(err) != contracts.ErrUnavailable {
				t.Fatal("lookup silently accepted incomplete ledger", err)
			}
		})
	}
}

type deltaStreamSnapshotter interface {
	GetStreamSnapshot(context.Context, contracts.Scope, string) (Generation, error)
}

func TestDeltaLedgerStreamSnapshotIsMetadataOnlyUntilTerminal(t *testing.T) {
	for _, status := range []string{GenPending, GenRunning, GenStreaming, GenCompleted, GenFailed, GenCanceled} {
		t.Run(status, func(t *testing.T) {
			e := newModelEnv(t)
			ctx := context.Background()
			snapshot, ok := any(e.gens).(deltaStreamSnapshotter)
			if !ok {
				t.Fatal("bounded hot stream snapshot API missing")
			}
			g, _, err := e.gens.Submit(ctx, e.scope, "task", "snapshot", "fixture", chatReq(strings.Repeat("x", 102400)), "", "", 0)
			if err != nil {
				t.Fatal(err)
			}
			if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "durable text", Usage{TotalTokens: 7}); err != nil {
				t.Fatal(err)
			}
			if _, err = e.pool.Exec(ctx, `UPDATE ahp_model.generations SET status=$2 WHERE id=$1`, g.ID, status); err != nil {
				t.Fatal(err)
			}
			got, err := snapshot.GetStreamSnapshot(ctx, e.scope, g.ID)
			if err != nil || got.Request != nil {
				t.Fatal("snapshot loaded request", err)
			}
			full, err := e.gens.Get(ctx, e.scope, g.ID)
			if err != nil {
				t.Fatal(err)
			}
			full.Request = nil
			if !terminal(status) {
				if got.Output != "" {
					t.Fatal("active snapshot materialized output")
				}
				full.Output = ""
			}
			if !reflect.DeepEqual(got, full) {
				t.Fatal("snapshot changed metadata/terminal output")
			}
			foreign := e.scope
			foreign.Subject = "foreign"
			if _, err = snapshot.GetStreamSnapshot(ctx, foreign, g.ID); contracts.Code(err) != contracts.ErrNotFound {
				t.Fatal("snapshot crossed owner scope", err)
			}
		})
	}
}

func TestDeltaLedgerCompletionPreservesContentFenceAndFirstTerminalWriter(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "content-fence", "fixture", chatReq("test"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"a", "b"} {
		if err = e.gens.AppendOutput(ctx, e.scope, g.ID, fragment, Usage{}); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.gens.prepareCompletion(ctx, e.scope, g.ID, "stale", "final", "stop", Usage{}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatal("stale content accepted", err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "c", Usage{}); err != nil {
		t.Fatal(err)
	}
	if err = e.gens.prepareCompletion(ctx, e.scope, g.ID, "ab", "final", "stop", Usage{}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatal("append escaped expected content fence", err)
	}
	if err = e.gens.prepareCompletion(ctx, e.scope, g.ID, "abc", "final", "stop", Usage{TotalTokens: 9}); err != nil {
		t.Fatal(err)
	}
	if err = e.gens.prepareCompletion(ctx, e.scope, g.ID, "old retry", "final", "stop", Usage{TotalTokens: 9}); err != nil {
		t.Fatal("prepared result idempotency changed", err)
	}
	final, err := e.gens.FinalizeWithOutput(ctx, e.scope, g.ID, GenCompleted, "", "final", Usage{TotalTokens: 9}, "stop")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := e.gens.FinalizeWithOutput(ctx, e.scope, g.ID, GenCanceled, "late", "wrong", Usage{TotalTokens: 1}, "length")
	if err != nil || !reflect.DeepEqual(final, replay) {
		t.Fatal("later terminal writer replaced content", err)
	}
}

func TestDeltaLedgerByteAndSequenceLimitsRemainAtomic(t *testing.T) {
	e := newModelEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "utf8-limit", "fixture", chatReq("test"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	content := strings.Repeat("я", 1<<20) //2MiB UTF8, not2MiB Unicode code points.
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, content, Usage{TotalTokens: 2}); err != nil {
		t.Fatal(err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "x", Usage{TotalTokens: 99}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatal("stream byte limit changed", err)
	}
	full, err := e.gens.Get(ctx, e.scope, g.ID)
	if err != nil || full.Output != content || full.DeltaSequence != 1 || full.Usage.TotalTokens != 2 {
		t.Fatal("rejected append changed bytes/usage", err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, strings.Repeat("x", (2<<20)+1), Usage{}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatal("oversized single delta accepted", err)
	}
	g, _, err = e.gens.Submit(ctx, e.scope, "task", "sequence-limit", "fixture", chatReq("test"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Bounded synthetic setup only, not a provider batching behavior.
	if _, err = e.pool.Exec(ctx, `INSERT INTO ahp_model.generation_deltas(generation_id,sequence,text) SELECT $1,n,'x' FROM generate_series(1,65535) n`, g.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = e.pool.Exec(ctx, `UPDATE ahp_model.generations SET delta_sequence=65535,streamed_bytes=65535 WHERE id=$1`, g.ID); err != nil {
		t.Fatal(err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "x", Usage{}); err != nil {
		t.Fatal("last permitted sequence refused", err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "x", Usage{TotalTokens: 99}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatal("delta ceiling changed", err)
	}
	full, err = e.gens.Get(ctx, e.scope, g.ID)
	if err != nil || full.DeltaSequence != 65536 || len(full.Output) != 65536 {
		t.Fatal("sequence limit changed durable content", err)
	}
}

func TestDeltaLedgerCompletionAfterLockWaitUsesFreshLedgerSnapshot(t *testing.T) {
	e := newModelEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "completion-lock-wait", "fixture", chatReq("test"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "a", Usage{}); err != nil {
		t.Fatal(err)
	}
	cfg := e.pool.Config()
	cfg.MaxConns = 1
	cfg.ConnConfig.RuntimeParams["application_name"] = "v8-prepare-" + e.u
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var locked string
	if err = tx.QueryRow(ctx, `SELECT id FROM ahp_model.generations WHERE id=$1 FOR UPDATE`, g.ID).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- NewGenerationStore(pool).prepareCompletion(ctx, e.scope, g.ID, "ab", "final", "stop", Usage{TotalTokens: 3})
	}()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err = e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')`, "v8-prepare-"+e.u).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatal("prepare did not wait for append parent lock", err)
		case <-ctx.Done():
			t.Fatal("prepare lock wait not observed")
		case <-ticker.C:
		}
	}
	// Commit the next fragment while prepare's first SQL snapshot is waiting.
	if _, err = tx.Exec(ctx, `UPDATE ahp_model.generations SET delta_sequence=2,streamed_bytes=2 WHERE id=$1`, g.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ahp_model.generation_deltas(generation_id,sequence,text) VALUES($1,2,'b')`, g.ID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("completion used stale pre-lock ledger snapshot", err)
		}
	case <-ctx.Done():
		t.Fatal("completion did not finish after parent lock released")
	}
	final, err := e.gens.Get(ctx, e.scope, g.ID)
	if err != nil || final.Output != "final" || final.DeltaSequence != 2 {
		t.Fatal("prepared result lost post-wait content", err)
	}
}

func TestDeltaLedgerByteCounterConstraintsRejectInvalidStorage(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "counter-constraint", "fixture", chatReq("test"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []int64{-1, (2 << 20) + 1} {
		if _, err = e.pool.Exec(ctx, `UPDATE ahp_model.generations SET streamed_bytes=$2 WHERE id=$1`, g.ID, bad); err == nil {
			t.Fatal("invalid durable byte counter accepted")
		}
	}
	got, err := e.gens.Get(ctx, e.scope, g.ID)
	if err != nil || got.Output != "" || got.DeltaSequence != 0 {
		t.Fatal("rejected counter mutation changed generation", err)
	}
}

func TestDeltaLedgerCanceledOrFencedPartialStaysExactWithoutLateContent(t *testing.T) {
	for _, kind := range []string{"cancel", "subject-fence"} {
		t.Run(kind, func(t *testing.T) {
			e := newModelEnv(t)
			ctx := context.Background()
			g, _, err := e.gens.Submit(ctx, e.scope, "task", "partial-terminal", "fixture", chatReq("test"), "", "", 0)
			if err != nil {
				t.Fatal(err)
			}
			if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "durable partial", Usage{TotalTokens: 3}); err != nil {
				t.Fatal(err)
			}
			if _, err = e.gens.Transition(ctx, e.scope, g.ID, []string{GenPending}, GenRunning); err != nil {
				t.Fatal(err)
			}
			want := contracts.ErrConflict
			if kind == "cancel" {
				if _, err = e.gens.RequestCancel(ctx, e.scope, g.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				want = contracts.ErrForbidden
				if err = e.ledger.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
					t.Fatal(err)
				}
			}
			if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "late", Usage{}); contracts.Code(err) != want {
				t.Fatal("late content crossed cancellation/erasure fence", err)
			}
			if err = e.gens.prepareCompletion(ctx, e.scope, g.ID, "durable partial", "late final", "stop", Usage{}); contracts.Code(err) != want {
				t.Fatal("late final content crossed fence", err)
			}
			got, err := e.gens.Finalize(ctx, e.scope, g.ID, GenCanceled, "billing reconciliation", Usage{TotalTokens: 3})
			if err != nil || got.Output != "durable partial" || got.Status != GenCanceled || got.Usage.TotalTokens != 3 {
				t.Fatal("metadata finalization lost partial/usage", err)
			}
			var stored int
			if err = e.pool.QueryRow(ctx, `SELECT octet_length(output) FROM ahp_model.generations WHERE id=$1`, g.ID).Scan(&stored); err != nil || stored != 0 {
				t.Fatal("metadata-only finalization copied content after fence", err)
			}
			projection, err := e.gens.GetForStream(ctx, e.scope, g.ID)
			if err != nil || projection.Output != "durable partial" || projection.Request != nil {
				t.Fatal("canceled partial no longer observable", err)
			}
		})
	}
}

// An explicit finish with empty replacement must keep the ledger-backed output;
// unconditionally clearing output_from_deltas would lose a partial generation.
func TestDeltaLedgerEmptyReplacementPreservesOutputAndExplicitFinish(t *testing.T) {
	for _, partial := range []string{"", "durable partial"} {
		t.Run(fmt.Sprintf("bytes-%d", len(partial)), func(t *testing.T) {
			e := newModelEnv(t)
			ctx := context.Background()
			g, _, err := e.gens.Submit(ctx, e.scope, "task", "empty-replacement", "fixture", chatReq("test"), "", "", 0)
			if err != nil {
				t.Fatal(err)
			}
			if partial != "" {
				if err = e.gens.AppendOutput(ctx, e.scope, g.ID, partial, Usage{TotalTokens: 3}); err != nil {
					t.Fatal(err)
				}
			}
			final, err := e.gens.FinalizeWithOutput(ctx, e.scope, g.ID, GenCompleted, "", "", Usage{TotalTokens: 3}, "stop")
			if err != nil || final.Output != partial || final.Status != GenCompleted || final.FinishReason != "stop" || final.Usage.TotalTokens != 3 {
				t.Fatal("empty replacement lost output or explicit finish", err)
			}
			projected, err := e.gens.GetForStream(ctx, e.scope, g.ID)
			if err != nil || projected.Output != partial || projected.FinishReason != "stop" || projected.Request != nil {
				t.Fatal("public projection lost finalized output/finish", err)
			}
			var stored int
			if err = e.pool.QueryRow(ctx, `SELECT octet_length(output) FROM ahp_model.generations WHERE id=$1`, g.ID).Scan(&stored); err != nil || stored != 0 {
				t.Fatal("empty replacement copied content", err)
			}
		})
	}
}
