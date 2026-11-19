//go:build compute_live3

// CONTRACT item 4 in the wording the earlier evidence did NOT cover: the executor is
// killed in the middle of a TURN (not of one command), and what has to be shown is
// that after recovery the external effect is not repeated, the artifacts stay
// consistent, and the TURN reaches a terminal state.
//
// The turn is a real internal/core.Runtime loop; its per-step durability goes through
// the published port (space.DurabilitySink + core.DurableHighWater) implemented here
// over the same live PostgreSQL, so the step history that survives the SIGKILL is read
// back from the database rather than remembered by the test. What is NOT claimed here:
// the Space Coordinator's own terminal frame for the turn - that is internal/space, a
// different owner; this file proves the compute half and the Core loop's terminal step.
//
// Two crash points, because they have different right answers:
//
//	TestB3ExecutorCrashMidTurnResumesTerminalOnce      - killed while the sandboxed
//	  command runs: the command is reaped, the same position-derived key is refused, the
//	  resumed turn ends with a terminal step, the effect never lands, no artifact exists.
//	TestB3ExecutorCrashAfterExportResumesOneArtifact   - killed after the file was
//	  exported but before the turn finished: the durable tool result is read back, the
//	  resumed turn finishes WITHOUT touching the tool again, and the run still owns
//	  exactly one artifact.
//
// Gated behind //go:build compute_live3 like the rest of the live compute suite.
package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	turnChildEnv   = "AHP_B3T_CHILD"
	turnModeEnv    = "AHP_B3T_MODE"
	turnJailEnv    = "AHP_B3T_JAIL"
	turnCompEnv    = "AHP_B3T_COMPUTER"
	turnRunEnv     = "AHP_B3T_RUN"
	turnBucketEnv  = "AHP_B3T_BUCKET"
	turnCtxEnv     = "AHP_B3T_CTX"
	turnTokenEnv   = "AHP_B3T_TOKEN"
	turnTenanEnv   = "AHP_B3T_TENANT"
	turnDeploEnv   = "AHP_B3T_DEPLOYMENT"
	turnSubjEnv    = "AHP_B3T_SUBJECT"
	turnChildRun   = "^TestTurnCrashChild$"
	turnModeMid    = "midcommand"
	turnModeExport = "afterexport"
	turnHold       = 12 * time.Second
)

// TestTurnCrashChild is the executor process the two tests below start and kill. It
// runs one real turn (real core.Runtime, real compute runner, real artifacts store,
// durable steps in real PostgreSQL) and prints its progress so the parent can pick a
// crash point. Reaching its last line means nobody killed it, which the parent treats
// as the test proving nothing.
func TestTurnCrashChild(t *testing.T) {
	if os.Getenv(turnChildEnv) != "1" {
		t.Skip("helper process for the executor-crash-mid-turn tests; it only runs when the parent starts it")
	}
	mode := turnEnvValue(t, turnModeEnv)
	jail := turnEnvValue(t, turnJailEnv)
	compID := turnEnvValue(t, turnCompEnv)
	runID := turnEnvValue(t, turnRunEnv)
	sc := contracts.Scope{
		Tenant: turnEnvValue(t, turnTenanEnv), Deployment: turnEnvValue(t, turnDeploEnv),
		Subject: turnEnvValue(t, turnSubjEnv),
	}
	runner, _ := restartedExecutor(t, jail, "test-node-turn")
	astore := sharedArtifactsStore(t, turnEnvValue(t, turnBucketEnv))
	tool := &turnTool{
		runner: runner, exporter: &storeExporter{st: astore, bindCtx: turnEnvValue(t, turnCtxEnv)},
		homeBase: filepath.Join(jail, "computers", compID, "home"), computerID: compID, runID: runID,
	}
	tools := core.NewToolRegistry(tool)
	tools.Register(core.ToolDef{
		Name: computeToolName, Description: "Run one shell command in the sandbox and export the file it writes.",
		Parameters: json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`),
	})
	runtime := core.NewRuntime(core.RuntimeConfig{
		Model: turnModel{mode: mode, hold: turnHold, token: turnEnvValue(t, turnTokenEnv)},
		Tools: tools, MaxTokens: 256, Durable: newTurnSink(t),
	})
	req := &space.CoreRequest{
		RunID: runID, ThreadID: "th-" + runID, ContextID: turnEnvValue(t, turnCtxEnv),
		Worker: "turn-crash-node", Epoch: 1, MaxSteps: 4, Scope: sc,
		TaskToken: turnEnvValue(t, turnTokenEnv),
		Input:     json.RawMessage(`"write the file"`),
	}
	fmt.Printf("EXECUTOR_PID %d\n", os.Getpid())
	fmt.Println("TURN_BEGIN")
	results := make(chan space.StepResult, 32)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(context.Background(), req, results) }()
	for {
		select {
		case sr := <-results:
			fmt.Printf("STEP %s\n", sr.StepType)
		case err := <-done:
			fmt.Printf("TURN_RETURNED err=%v\n", err)
			return
		}
	}
}

func turnEnvValue(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Fatalf("%s must be set for the turn-crash child process", name)
	}
	return v
}

// turnModel is message-driven rather than step-counter-driven, which is what a real
// provider is: it reads the conversation it was handed. No tool result yet means the
// tool call is still owed; a tool result present means the work is done and only the
// answer is left. In "afterexport" mode the answer is deliberately slow, so the parent
// can kill the executor between the export and the end of the turn.
//
// The command text is a pure function of the run's own token, so a life that resumes
// the run re-issues the BYTE-IDENTICAL command under the same position key. That is the
// only honest way to exercise the ledger: a different body would be refused as
// KEY_REUSE_MISMATCH, which is a different guarantee (and this test fails loudly with
// that error in the tool result if it ever stops holding).
type turnModel struct {
	mode  string
	hold  time.Duration
	token string
}

func (m turnModel) Generate(ctx context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	toolResults := 0
	for _, msg := range req.Messages {
		if msg.Role == "tool" {
			toolResults++
		}
	}
	if toolResults == 0 {
		command := "printf " + m.token + " > out.txt"
		if m.mode == turnModeMid {
			command = "sleep 6; printf " + m.token + " > out.txt"
		}
		args, _ := json.Marshal(map[string]any{"command": command})
		return &core.ModelResponse{
			Content:   json.RawMessage("null"),
			ToolCalls: []core.ToolCall{{ID: "call-1", Name: computeToolName, Args: args}},
		}, nil
	}
	if m.mode == turnModeExport {
		select {
		case <-time.After(m.hold):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &core.ModelResponse{Content: json.RawMessage(`"file written"`)}, nil
}

// turnBucketName is a bucket name that one turn-crash test owns for its whole
// lifetime, so the executor process and the parent can be pointed at the same
// real Artifacts bucket.
func turnBucketName(t *testing.T) string {
	t.Helper()
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatalf("bucket entropy: %v", err)
	}
	return "ahp-b3t-" + hex.EncodeToString(rnd[:])
}

// sharedArtifactsStore opens the REAL Artifacts store (live PostgreSQL + live
// MinIO) on a bucket both the executor process and the parent can reach: an
// artifact exported by the crashed life has to be visible to the recovering one,
// which is the situation under test. The bucket is created by whoever migrates
// first and removed by the PARENT's cleanup only - a child that was SIGKILLed
// never gets to clean, and a child that was not must not empty the bucket out
// from under the assertions that still have to run.
func sharedArtifactsStore(t *testing.T, bucket string) *artifacts.Store {
	t.Helper()
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if raw == "" || os.Getenv("AHP_MINIO_ROOT_USER") == "" || os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Skip("artifact substrate env not set")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatal("bad AHP_S3_ENDPOINT")
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(c, liveDSN(t))
	if err != nil {
		t.Fatalf("artifacts pool: %v", err)
	}
	t.Cleanup(pool.Close)
	st, err := artifacts.New(pool, client, bucket, func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatalf("artifacts.New: %v", err)
	}
	if err := st.Migrate(c); err != nil {
		t.Fatalf("artifacts.Migrate: %v", err)
	}
	if os.Getenv(turnChildEnv) != "1" {
		t.Cleanup(func() {
			cc, cn := context.WithTimeout(context.Background(), 20*time.Second)
			defer cn()
			for o := range client.ListObjects(cc, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
				if o.Err != nil {
					return
				}
				_ = client.RemoveObject(cc, bucket, o.Key, minio.RemoveObjectOptions{VersionID: o.VersionID})
			}
			_ = client.RemoveBucket(cc, bucket)
		})
	}
	return st
}

// readCloserBytes adapts an in-memory body to the io.ReadCloser the export seam takes.
func readCloserBytes(b []byte) io.ReadCloser { return io.NopCloser(bytes.NewReader(b)) }

// turnTool runs the sandbox command under the key derived from its POSITION in the run
// and exports the produced file. When the ledger refuses the key - the case a restart
// produces - it says so instead of pretending the work happened: that answer is what
// lets the resumed turn terminate without a second touch of the world.
type turnTool struct {
	runner     *compute.Runner
	exporter   *storeExporter
	homeBase   string
	computerID string
	runID      string
	calls      int
}

func (tt *turnTool) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	if name != computeToolName {
		return nil, fmt.Errorf("turnTool: unknown tool %q", name)
	}
	var in struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, err
	}
	sc, _, ok := core.RunScopeFromContext(ctx)
	if !ok {
		return nil, errNoScope
	}
	index := tt.calls
	tt.calls++
	key := compute.CommandKeyForStep(tt.runID, index)
	fmt.Printf("TOOL_CALL %d key %s\n", index, key)
	rec, err := tt.runner.RunCommand(ctx, sc, tt.computerID, key, in.Command, "")
	if err != nil {
		return nil, err
	}
	if rec.Status != compute.CommandSucceeded {
		// The key was already dispatched by the life that died: the outcome is
		// unknown, and repeating it is the thing this test exists to forbid.
		out := map[string]any{
			"status": string(rec.Status), "command_id": rec.ID,
			"detail": "the command of this position was already dispatched by an earlier life; not re-executed",
		}
		fmt.Printf("TOOL_REFUSED %s status=%s\n", key, rec.Status)
		return json.Marshal(out)
	}
	body, err := os.ReadFile(filepath.Join(tt.homeBase, "out.txt"))
	if err != nil {
		return nil, err
	}
	ref, err := tt.exporter.Export(ctx, compute.ArtifactExportRequest{
		Scope: sc, ComputerID: tt.computerID, TaskID: "task-" + tt.runID,
		Filename: "out.txt", ContentType: "application/octet-stream",
		Body: readCloserBytes(body), SizeHint: int64(len(body)),
	})
	if err != nil {
		return nil, err
	}
	fmt.Printf("ARTIFACT_EXPORTED %s\n", ref.ArtifactID)
	return json.Marshal(map[string]any{
		"artifact_id": ref.ArtifactID, "sha256": ref.Digest, "size": ref.SizeBytes,
	})
}

// turnSink is the published durability port over real PostgreSQL: it is what makes the
// step history outlive the executor process, so recovery can read it back instead of
// remembering it. Key-reuse is reported, never papered over, and the high-water is
// answered, which is how a continuing life avoids writing foreign bodies onto keys an
// earlier life already used.
type turnSink struct {
	pool    *pgxpool.Pool
	reuses  int
	appends int
}

func newTurnSink(t *testing.T) *turnSink {
	t.Helper()
	s := &turnSink{pool: turnPool(t)}
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := s.pool.Exec(c, `CREATE SCHEMA IF NOT EXISTS ahp_b3turn`); err != nil {
		t.Fatalf("create the turn-step schema: %v", err)
	}
	if _, err := s.pool.Exec(c, `
		CREATE TABLE IF NOT EXISTS ahp_b3turn.steps (
			key         text PRIMARY KEY,
			kind        text NOT NULL,
			fingerprint text NOT NULL,
			payload     text NOT NULL,
			written_at  timestamptz NOT NULL DEFAULT clock_timestamp()
		)`); err != nil {
		t.Fatalf("create the turn-step table: %v", err)
	}
	return s
}

func turnPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), liveDSN(t))
	if err != nil {
		t.Fatalf("turn sink pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func (s *turnSink) AppendDurable(ctx context.Context, key, kind, fingerprint string, payload json.RawMessage) (bool, error) {
	s.appends++
	var stored string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO ahp_b3turn.steps (key, kind, fingerprint, payload)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (key) DO NOTHING
		RETURNING fingerprint`, key, kind, fingerprint, string(payload)).Scan(&stored)
	if err == pgx.ErrNoRows {
		if err := s.pool.QueryRow(ctx, `SELECT fingerprint FROM ahp_b3turn.steps WHERE key = $1`, key).Scan(&stored); err != nil {
			return false, err
		}
		if stored != fingerprint {
			s.reuses++
			return false, space.ErrKeyReuse
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

// StepHighWater answers max(n)+1 over "<loopID>:<n>", the port core.DurableHighWater
// asks for on a continuing life.
func (s *turnSink) StepHighWater(ctx context.Context, loopID string) (int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT key FROM ahp_b3turn.steps WHERE key LIKE $1`, loopID+`:%`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	next := 0
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return 0, err
		}
		n, cerr := strconv.Atoi(strings.TrimPrefix(k, loopID+":"))
		if cerr != nil {
			continue
		}
		if n+1 > next {
			next = n + 1
		}
	}
	return next, rows.Err()
}

func (s *turnSink) rowCount(ctx context.Context, loopID string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_b3turn.steps WHERE key LIKE $1`, loopID+`:%`).Scan(&n)
	return n, err
}

// durableHistory reads the steps the dead process made durable and rebuilds the
// conversation from them. The user prompt is the run's own input; everything after it
// comes out of PostgreSQL, which is the point: the recovery does not remember the turn,
// it reads it.
func durableHistory(t *testing.T, pool *pgxpool.Pool, runID, prompt string) []core.ChatMessage {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT key, payload FROM ahp_b3turn.steps WHERE key LIKE $1
		ORDER BY (split_part(key, ':', 2))::int`, runID+`:%`)
	if err != nil {
		t.Fatalf("read the durable steps back: %v", err)
	}
	defer rows.Close()
	msgs := []core.ChatMessage{{Role: "user", Content: json.RawMessage(`"` + prompt + `"`)}}
	for rows.Next() {
		var key, payload string
		if err := rows.Scan(&key, &payload); err != nil {
			t.Fatalf("scan a durable step: %v", err)
		}
		var sr space.StepResult
		if err := json.Unmarshal([]byte(payload), &sr); err != nil {
			t.Fatalf("decode the durable step %s: %v", key, err)
		}
		switch sr.StepType {
		case space.StepTool:
			if sr.ToolCall == nil {
				t.Fatalf("the durable tool step %s carries no tool call, so its result cannot be attributed to a call", key)
			}
			// The emitted model step carries only the completion text, so the
			// assistant message that owed this result is rebuilt from the durable
			// tool step, which freezes the call's id, name and arguments. It is
			// marshaled as []core.ToolCall because that is the shape the loop
			// itself puts into ChatMessage.ToolCalls: a history in any other shape
			// is a history the live loop never produced, and a tool message with no
			// assistant call before it is a conversation no real provider accepts.
			calls := mustMarshal(t, []core.ToolCall{{ID: sr.ToolCall.ID, Name: sr.ToolCall.Name, Args: sr.ToolCall.Args}})
			msgs = append(msgs, core.ChatMessage{Role: "assistant", Content: json.RawMessage("null"), ToolCalls: calls})
			content := sr.Content
			if len(content) == 0 {
				content = json.RawMessage("null")
			}
			msgs = append(msgs, core.ChatMessage{Role: "tool", ToolCallID: sr.ToolCall.ID, Content: content})
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate the durable steps: %v", err)
	}
	return msgs
}

// runTurnToTerminal drives one life of the real loop and returns every emitted step.
func runTurnToTerminal(t *testing.T, runtime *core.Runtime, req *space.CoreRequest) ([]space.StepResult, error) {
	t.Helper()
	results := make(chan space.StepResult, 64)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(context.Background(), req, results) }()
	var steps []space.StepResult
	var runErr error
	for {
		select {
		case sr := <-results:
			steps = append(steps, sr)
		case err := <-done:
			runErr = err
			for {
				select {
				case sr := <-results:
					steps = append(steps, sr)
				default:
					return steps, runErr
				}
			}
		}
	}
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// waitForDurableStepKind counts the durable steps of one kind a run has, polling up to
// d. A crash point chosen by watching the executor's stdout can still land a moment
// before the step it follows becomes durable; killing there would recover a history
// that is missing work that really happened, which is a different situation from the
// one under test - so the parent waits for durability, not for a printed line alone.
func waitForDurableStepKind(t *testing.T, pool *pgxpool.Pool, runID, kind string, d time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		var n int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM ahp_b3turn.steps WHERE key LIKE $1 AND kind = $2`, runID+`:%`, kind).Scan(&n); err != nil {
			t.Fatalf("count the durable %q steps: %v", kind, err)
		}
		if n > 0 || time.Now().After(deadline) {
			return n
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// turnChild starts the executor process for one crash mode and returns it with the
// line reader the parent uses to choose the crash point.
type turnChild struct {
	cmd   *exec.Cmd
	lines chan string
	token string
}

func startTurnChild(t *testing.T, e *liveEnv, sc contracts.Scope, compID, runID, bucket, ctxID, token, mode string) *turnChild {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	childTmp := filepath.Join(e.jailBase, "child-tmp")
	if err := os.MkdirAll(childTmp, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run", turnChildRun, "-test.v", "-test.timeout", "300s")
	cmd.Dir = wd
	cmd.Env = append(os.Environ(),
		turnChildEnv+"=1", turnModeEnv+"="+mode, turnJailEnv+"="+e.jailBase,
		turnCompEnv+"="+compID, turnRunEnv+"="+runID, turnBucketEnv+"="+bucket,
		turnCtxEnv+"="+ctxID, turnTokenEnv+"="+token,
		turnTenanEnv+"="+sc.Tenant, turnDeploEnv+"="+sc.Deployment, turnSubjEnv+"="+sc.Subject,
		// The child must not queue on the package flock its parent holds.
		"TMPDIR="+childTmp,
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	lines := make(chan string, 128)
	go func() {
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			lines <- scan.Text()
		}
		close(lines)
	}()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the executor process: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return &turnChild{cmd: cmd, lines: lines, token: token}
}

// waitForLine returns the first child line containing want, failing the test on
// timeout or on the child exiting first - a crash point that was never reached proves
// nothing, so it must stop the run rather than be papered over.
func (c *turnChild) waitForLine(t *testing.T, want string, d time.Duration) string {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				t.Fatalf("the executor process exited before printing %q", want)
			}
			if strings.Contains(line, want) {
				return line
			}
			t.Logf("executor process: %s", line)
		case <-deadline:
			t.Fatalf("the executor process never printed %q within %s", want, d)
		}
	}
}

// kill stops the executor process only. The sandboxed command, if any, is left alone:
// that is the crash, not a cleanup.
func (c *turnChild) kill(t *testing.T) int {
	t.Helper()
	pid := c.cmd.Process.Pid
	if err := c.cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL the executor process %d: %v", pid, err)
	}
	_ = c.cmd.Wait()
	if err := syscall.Kill(pid, 0); err != syscall.ESRCH {
		t.Fatalf("the executor process %d is still alive after SIGKILL: %v", pid, err)
	}
	return pid
}

func TestB3ExecutorCrashMidTurnResumesTerminalOnce(t *testing.T) {
	e := newLiveEnv(t)
	sc := scope("b3-turn-crash")
	bucket := turnBucketName(t)
	astore := sharedArtifactsStore(t, bucket)
	ctxID := "ctx-" + contracts.NewID()
	token := "AHPTURN-" + contracts.NewID()
	runID := "run-" + contracts.NewID()
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Quota: compute.Quota{MaxSessionTTL: 60 * time.Second},
	}, "b3-turn-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	home := e.home(comp.ID)
	out := filepath.Join(home, "out.txt")

	child := startTurnChild(t, e, sc, comp.ID, runID, bucket, ctxID, token, turnModeMid)
	child.waitForLine(t, "TURN_BEGIN", 40*time.Second)
	key := compute.CommandKeyForStep(runID, 0)
	storedKey, status := waitForLedgerRow(t, e.pool, comp.ID, key, string(compute.CommandDispatched), 25*time.Second)
	if storedKey != key {
		t.Fatalf("the turn recorded its command under %q, not the position key %q", storedKey, key)
	}
	sandboxPID := waitForRegisteredProcess(t, e.jailBase, comp.ID, 20*time.Second)
	t.Logf("crash point: turn dispatched (key %s), sandboxed command pid %d under executor %d", key[:16], sandboxPID, child.cmd.Process.Pid)

	executorPID := child.kill(t)
	if gone, _ := procNotRunning(sandboxPID); !gone {
		if ppid := procPPid(sandboxPID); ppid == executorPID {
			t.Fatalf("the command neither survived its executor nor was reaped")
		}
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("the file appeared before the crash: the crash point is not mid-command")
	}

	// RESTART: what a taking-over executor does first, then what it knows.
	reaped, err := compute.ReapProcessesAtStartup(e.jailBase)
	if err != nil {
		t.Fatalf("startup reaper of the restarted executor: %v", err)
	}
	if reaped.Survivors() != nil {
		t.Fatalf("the restarted executor left processes running: %+v", reaped.Survivors())
	}
	t.Logf("reaped at takeover: %d process(es) proven dead", reaped.ReapedProcs())

	sink := newTurnSink(t)
	before, err := sink.rowCount(ctx(t), runID)
	if err != nil {
		t.Fatalf("count the durable steps: %v", err)
	}
	if before == 0 {
		t.Fatal("the dead executor left no durable steps at all: recovery would be reading nothing")
	}
	msgs := durableHistory(t, sink.pool, runID, "write the file")
	toolResults := 0
	for _, m := range msgs {
		if m.Role == "tool" {
			toolResults++
		}
	}
	if toolResults != 0 {
		t.Fatalf("the durable history claims a tool result that never happened (%d): recovery would invent an outcome", toolResults)
	}
	t.Logf("recovered history from PostgreSQL: %d message(s), %d durable step(s), no tool result", len(msgs), before)

	restarted, _ := restartedExecutor(t, e.jailBase, "test-node-takeover")
	tool := &turnTool{
		runner: restarted, exporter: &storeExporter{st: astore, bindCtx: ctxID},
		homeBase: home, computerID: comp.ID, runID: runID,
	}
	tools := core.NewToolRegistry(tool)
	tools.Register(core.ToolDef{Name: computeToolName, Description: "run a sandbox command", Parameters: json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`)})
	runtime := core.NewRuntime(core.RuntimeConfig{
		Model: turnModel{mode: turnModeMid, token: token}, Tools: tools, MaxTokens: 256, Durable: sink,
	})
	req := &space.CoreRequest{
		RunID: runID, ThreadID: "th-" + runID, ContextID: ctxID,
		Worker: "test-node-takeover", Epoch: 2, MaxSteps: 4, Scope: sc,
		TaskToken: "task-" + runID, Input: json.RawMessage(`"write the file"`),
		Messages:   mustMarshal(t, msgs),
		Checkpoint: &space.Checkpoint{ID: "ckpt-" + runID, Namespace: "compute-turn"},
	}
	steps, runErr := runTurnToTerminal(t, runtime, req)
	if runErr != nil {
		t.Fatalf("the resumed turn did not run to an end: %v", runErr)
	}
	if !containsStep(steps, space.StepDone) {
		t.Fatalf("the resumed turn never emitted a terminal step: %v", stepTypes(steps))
	}
	if tool.calls != 1 {
		t.Fatalf("the resumed turn consulted the tool %d times, want 1 (the refusal)", tool.calls)
	}
	if !containsToolRefusal(steps) {
		t.Fatalf("the resumed turn did not report the refused dispatch to the model: %s", strings.Join(toolStepContents(steps), " | "))
	}
	if sink.reuses != 0 {
		t.Fatalf("the continuing life collided with durable keys %d time(s)", sink.reuses)
	}
	after, err := sink.rowCount(ctx(t), runID)
	if err != nil {
		t.Fatalf("count the durable steps after the resume: %v", err)
	}
	if after <= before {
		t.Fatalf("the resumed turn added no durable steps (%d -> %d): the high-water seeding is not being used", before, after)
	}

	// The world: unchanged.
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("the resumed turn produced the effect the crashed command never reached: %v", err)
	}
	owned, err := astore.ListByContext(ctx(t), sc, ctxID)
	if err != nil {
		t.Fatalf("list this run's artifacts: %v", err)
	}
	if len(owned) != 0 {
		t.Fatalf("the run owns %d artifact(s) although no command ever completed: %+v", len(owned), owned)
	}
	_, status = waitForLedgerRow(t, e.pool, comp.ID, key, string(compute.CommandDispatched), 2*time.Second)
	t.Logf("after recovery: terminal step reached, key %s still %s, 0 effects, 0 artifacts, durable steps %d -> %d (%d written by the recovering life)",
		key[:16], status, before, after, sink.appends)
}

func TestB3ExecutorCrashAfterExportResumesOneArtifact(t *testing.T) {
	e := newLiveEnv(t)
	sc := scope("b3-turn-export")
	bucket := turnBucketName(t)
	astore := sharedArtifactsStore(t, bucket)
	ctxID := "ctx-" + contracts.NewID()
	token := "AHPTURN-" + contracts.NewID()
	runID := "run-" + contracts.NewID()
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Quota: compute.Quota{MaxSessionTTL: 60 * time.Second},
	}, "b3-turn-x-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	home := e.home(comp.ID)
	out := filepath.Join(home, "out.txt")

	sink := newTurnSink(t)
	child := startTurnChild(t, e, sc, comp.ID, runID, bucket, ctxID, token, turnModeExport)
	child.waitForLine(t, "TURN_BEGIN", 40*time.Second)
	line := child.waitForLine(t, "ARTIFACT_EXPORTED", 40*time.Second)
	artifactID := strings.TrimSpace(strings.TrimPrefix(line, "ARTIFACT_EXPORTED"))
	t.Logf("the crashed life exported %s and was still inside the turn", artifactID)
	if n := waitForDurableStepKind(t, sink.pool, runID, string(space.StepTool), 5*time.Second); n != 1 {
		t.Fatalf("the crashed life left %d durable tool step(s) by the export point, want the 1 it really produced", n)
	}

	child.kill(t)

	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("the exported file vanished with the executor: %v", err)
	}
	if strings.TrimSpace(string(body)) != token {
		t.Fatalf("the exported file changed after the crash: %q", string(body))
	}
	msgs := durableHistory(t, sink.pool, runID, "write the file")
	toolResults := 0
	for _, m := range msgs {
		if m.Role == "tool" {
			toolResults++
		}
	}
	if toolResults != 1 {
		t.Fatalf("the durable history holds %d tool result(s), want the 1 the dead process really produced", toolResults)
	}

	restarted, _ := restartedExecutor(t, e.jailBase, "test-node-takeover-2")
	tool := &turnTool{
		runner: restarted, exporter: &storeExporter{st: astore, bindCtx: ctxID},
		homeBase: home, computerID: comp.ID, runID: runID,
	}
	tools := core.NewToolRegistry(tool)
	tools.Register(core.ToolDef{Name: computeToolName, Description: "run a sandbox command", Parameters: json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`)})
	runtime := core.NewRuntime(core.RuntimeConfig{
		Model: turnModel{mode: turnModeMid, token: token}, Tools: tools, MaxTokens: 256, Durable: sink,
	})
	req := &space.CoreRequest{
		RunID: runID, ThreadID: "th-" + runID, ContextID: ctxID,
		Worker: "test-node-takeover-2", Epoch: 2, MaxSteps: 4, Scope: sc,
		TaskToken: "task-" + runID, Input: json.RawMessage(`"write the file"`),
		Messages:   mustMarshal(t, msgs),
		Checkpoint: &space.Checkpoint{ID: "ckpt-" + runID, Namespace: "compute-turn"},
	}
	steps, runErr := runTurnToTerminal(t, runtime, req)
	if runErr != nil {
		t.Fatalf("the resumed turn did not run to an end: %v", runErr)
	}
	if !containsStep(steps, space.StepDone) {
		t.Fatalf("the resumed turn never reached a terminal step: %v", stepTypes(steps))
	}
	if tool.calls != 0 {
		t.Fatalf("the resumed turn ran the tool %d time(s) although its result was already durable", tool.calls)
	}
	if sink.reuses != 0 {
		t.Fatalf("the continuing life collided with durable keys %d time(s)", sink.reuses)
	}
	owned, err := astore.ListByContext(ctx(t), sc, ctxID)
	if err != nil {
		t.Fatalf("list this run's artifacts: %v", err)
	}
	if len(owned) != 1 {
		t.Fatalf("the run owns %d artifact(s) after recovery, want exactly the 1 the crashed life exported", len(owned))
	}
	body2, err := os.ReadFile(out)
	if err != nil || string(body2) != string(body) {
		t.Fatalf("the produced file changed across the recovery: %v", err)
	}
	t.Logf("after recovery: terminal step reached without touching the tool, exactly 1 artifact (%s), file unchanged, %d durable step(s) written by the recovering life",
		owned[0].ID, sink.appends)
}

func containsStep(steps []space.StepResult, kind space.StepType) bool {
	for _, s := range steps {
		if s.StepType == kind {
			return true
		}
	}
	return false
}

func stepTypes(steps []space.StepResult) []space.StepType {
	out := make([]space.StepType, 0, len(steps))
	for _, s := range steps {
		out = append(out, s.StepType)
	}
	return out
}

func containsToolRefusal(steps []space.StepResult) bool {
	for _, s := range steps {
		if s.StepType == space.StepTool && strings.Contains(string(s.Content), "not re-executed") {
			return true
		}
	}
	return false
}

// toolStepContents quotes what the model was actually told about each tool call - the
// answer a resumed turn is judged on, so a failure has to name it.
func toolStepContents(steps []space.StepResult) []string {
	out := make([]string, 0, len(steps))
	for _, s := range steps {
		if s.StepType == space.StepTool {
			out = append(out, string(s.Content))
		}
	}
	return out
}
