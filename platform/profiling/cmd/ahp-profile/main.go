// Command ahp-profile runs the reproducible AHP baseline profiling phases.
//
// Every phase spawns an ISOLATED platform instance: its own mTLS material,
// its own listener port, its own tenant/deployment identity, and the lane
// database from the injected lane environment. The live stand is never
// touched; PostgreSQL/Redis/MinIO containers are only sampled (read-only)
// or reached through harness-owned proxies for fault injection.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ahp-platform/ahp/internal/model/routing"
	"github.com/ahp-platform/ahp/profiling/harness"
)

const liveStandPorts = "front :29612 (pid 46371) and runner :29690 (pid 46295) were never contacted by this harness"

var (
	phase          = flag.String("phase", "smoke", "overhead|recovery|soak|depfail|llm|smoke")
	outDir         = flag.String("out", "../evidence", "evidence directory for reports")
	stepSeconds    = flag.Int("step-s", 30, "closed-loop duration per ladder step")
	ladder         = flag.String("steps", "1,5,10,20,30", "core concurrency ladder")
	extended       = flag.String("steps-ext", "45,60,80,100,128", "extension ladder until degradation boundary")
	soakMinutes    = flag.Int("soak-minutes", 30, "soak duration")
	sampleSeconds  = flag.Int("interval-s", 5, "sampler interval seconds")
	minSamples     = flag.Int("min-samples", 20, "data-quality gate: minimum samples/requests per counted step")
	runIDFlag      = flag.String("run-id", envDefault("AHP_RUN_ID", ""), "run id handed down by the run wrapper (AHP_RUN_ID); the artifacts, the run log and the live sidecar must carry one identical id or a publication cannot be reconciled")
	productRev     = flag.String("product-rev", envDefault("AHP_PRODUCT_REVISION", ""), "product revision the MEASURED binary was built from (declared, not inferred from the vcs stamp)")
	sessionsMode   = flag.String("sessions", "hot-thread", "hot-thread (one thread/grant shared by all workers: hot-path throughput) | per-worker (own thread+grant+fence each: user capacity, the only mode a 'capacity for users' claim may be signed against)")
	loadPattern    = flag.String("pattern", "closed", "closed (as many as finish: throughput) | open (fixed arrival rate: offered load, queueing, shedded arrivals)")
	openRate       = flag.Float64("open-rate", 200, "open-loop offered rate in requests/second (total, not per worker)")
	repeats        = flag.Int("repeats", 1, "repetitions per rung; publish each repeat separately and compare, cold/warm cache state is recorded per step")
	workloadFilter = flag.String("workloads", "", "overhead ladder: comma-separated workload names to run (empty = all)")
	llmCountC1     = flag.Int("llm-c1", 10, "real LLM requests at concurrency 1")
	llmCountC5     = flag.Int("llm-c5", 25, "real LLM requests at concurrency 5")
	llmCountC10    = flag.Int("llm-c10", 0, "real LLM requests at concurrency 10 (0 = skip this rung)")
	llmCountC20    = flag.Int("llm-c20", 0, "real LLM requests at concurrency 20 (0 = skip this rung)")
	llmCountC30    = flag.Int("llm-c30", 0, "real LLM requests at concurrency 30 (0 = skip this rung)")
	llmStreamN     = flag.Int("llm-stream", 10, "real LLM streaming requests for TTFB (concurrency 1)")
	// llmMaxTokens is a MEASUREMENT setting of this harness for this model and
	// this benchmark -- not a platform or SDK limit. The default used to be 64,
	// and that single number produced 261 FAILED generations against 6
	// COMPLETED in the profiling database (durable cause
	// provider_empty_completion, finish_reason=length, usage_completion == the
	// cap): the workload was measuring its own budget, not the product. 1024
	// matches the platform's own compiled per-response default, leaves the
	// model room to answer, and keeps token volume bounded; the single-shot
	// proof at 22:00:49Z completed with completion=133/1024.
	llmMaxTokens = flag.Int("llm-max-tokens", 1024, "max_tokens cap for real LLM calls (harness measurement config; platform default is 1024/response)")
	repoRoot     = flag.String("repo", ".", "platform repository root")
	// binaryPath measures an EXISTING immutable copy instead of building.
	// A measurement window must not compile the product (builds serialize and
	// pollute timings), and the artifact must name bytes the run cannot change
	// -- the lane bin/ is overwritten by the next build, which is how
	// acceptance twice found published numbers bound to the wrong binary.
	probesPolicy  = flag.String("probes", envDefault("AHP_PROBE_POLICY", "in-window"), "where observer probes may run: in-window | outside-only (protocol v1.1 rule 2: docker stats etc. strictly outside the measured window)")
	manifestPath  = flag.String("manifest", envDefault("AHP_ACCEPTANCE_MANIFEST", ""), "path of the acceptance manifest (source/binary/image attestation of the accepted candidate) this profile belongs to")
	binaryPath    = flag.String("binary", envDefault("AHP_MEASURE_BINARY", ""), "measure this immutable per-run binary instead of building (chmod a-w, runs/<stamp>/bin/ or bin-<stamp>/)")
	burstSecs     = flag.Int("burst-s", 120, "recovery phase: burst duration seconds")
	burstConc     = flag.Int("burst-conc", 60, "recovery phase: burst concurrency")
	idleSecs      = flag.Int("idle-s", 60, "recovery phase: idle duration seconds")
	steadySecs    = flag.Int("steady-s", 45, "depfail phase: steady phase duration seconds")
	outageSecs    = flag.Int("outage-s", 20, "depfail phase: outage duration seconds")
	llmTaskLimit  = flag.String("llm-task-limit", "", "llm phase: override AHP_EXEC_TASK_TOKEN_LIMIT (empty = compiled default 20000/task; the budget gate answers 409 CONFLICT once reservations exceed capacity)")
	llmChildLimit = flag.String("llm-child-limit", "", "llm phase: override AHP_EXEC_CHILD_TOKEN_LIMIT (empty = half the task limit; the scoped token is a child budget)")
)

func main() {
	flag.Parse()
	log.SetPrefix("[ahp-profile] ")
	// The harness no longer reads a dev signing seed: every State/model credential
	// comes from the product exchange boundary, so there is nothing here to seed
	// and no self-signed token that could carry an empty actor chain.
	var err error
	machine := harness.CollectMachine()
	log.Printf("machine: %s %s (%s), cores %d/%d, %d bytes RAM", machine.Hostname, machine.OSVersion, machine.Arch, machine.PhysicalCores, machine.LogicalCores, machine.MemoryBytes)
	var ver harness.VersionInfo
	if *binaryPath != "" {
		ver, err = harness.UsePinnedBinary(*repoRoot, *binaryPath)
		if err != nil {
			log.Fatalf("pinned binary: %v", err)
		}
	} else {
		ver, err = harness.BuildPlatform(*repoRoot)
		if err != nil {
			log.Fatalf("build: %v", err)
		}
	}
	// Ask the bytes what they were built from. The lane tree describes the
	// harness; the subject may be a binary nobody here compiled (the canonical
	// candidate build), and a profile must be able to say the two agree.
	ver.MeasuredBinaryBuild = harness.ReadBinaryBuildInfo(ver.BinaryPath)
	if mb := ver.MeasuredBinaryBuild; mb != nil {
		if mb.Revision != nil {
			mod := "clean"
			if mb.Modified != nil && *mb.Modified {
				mod = "DIRTY"
			}
			log.Printf("measured binary build stamp: vcs.revision=%s (%s) tags=%q go=%s (%s)", *mb.Revision, mod, derefStr(mb.BuildTags), derefStr(mb.GoVersion), mb.Source)
			if *mb.Revision != ver.SourceRevision {
				log.Printf("PROVENANCE: the measured binary is stamped %s while this tree is %s -- product_revision declares which one the numbers describe", shortHash(*mb.Revision), shortHash(ver.SourceRevision))
			}
		} else {
			log.Printf("measured binary build stamp: none (%s)", mb.UnavailableReason)
		}
	}
	log.Printf("binary %s rev %s sha256 %s pinned=%t (%s)", ver.BinaryPath, ver.SourceRevision[:8], ver.BinarySHA256[:12], ver.BinaryPinned, ver.BinaryPinReason)

	tmp, err := os.MkdirTemp("", "ahp-prof-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	cm, err := harness.MintCerts(tmp)
	if err != nil {
		log.Fatal(err)
	}
	runID, runIDSource := harness.ResolveRunID(*runIDFlag, time.Now().UTC().Format("20060102T150405Z")+"-"+randSuffix())
	log.Printf("run_id=%s source=%s", runID, runIDSource)
	if runIDSource == harness.RunIDGenerated {
		log.Printf("WARNING: run id was generated here, not handed down by the wrapper: the run log will carry a DIFFERENT id and numbers from these artifacts cannot be reconciled with it")
	}
	dbName := os.Getenv("AHP_DB_NAME")
	if dbName == "" {
		dbName = "unknown-lane-db"
	}

	ver.ImageDigests = pinnedImages(os.Getenv("AHP_PINNED_IMAGES"))
	// Explicit pins win; otherwise record what the live stack actually runs.
	if len(ver.ImageDigests) == 0 {
		ver.ImageDigests = harness.ContainerImageDigests([]string{
			"ahp-handbook-acceptance-pg", "ahp-handbook-go-minio-1", "ahp-pdf-redis-16379", "ahp-handbook-memory-hindsight-api-1",
		})
	}
	ver.ProductRevision = strings.TrimSpace(*productRev)
	// Root discipline: every artifact must carry a non-empty product_revision so its
	// numbers cannot be attributed to the wrong candidate. If -product-rev was not
	// passed, fall back to the revision the measured binary itself is stamped with
	// (the canonical pinned binary carries vcs.revision), so the field is never left
	// empty for acceptance to reconcile by hand -- the failure that happened with the
	// n=5 artifact. If neither the flag nor the stamp is present, EnforceProductRevision
	// demotes the artifact to FAIL rather than publishing an anonymous profile.
	if ver.ProductRevision == "" && ver.MeasuredBinaryBuild != nil && ver.MeasuredBinaryBuild.Revision != nil && *ver.MeasuredBinaryBuild.Revision != "" {
		ver.ProductRevision = *ver.MeasuredBinaryBuild.Revision
		log.Printf("product_revision: -product-rev unset; derived from the measured binary's own vcs stamp %s", ver.ProductRevision[:min(12, len(ver.ProductRevision))])
	}
	if ver.ProductRevision != "" {
		absRepo, rerr := filepath.Abs(*repoRoot)
		if rerr != nil {
			absRepo = *repoRoot
		}
		ver.ProductDeltaOutsideProfiling = harness.ProductDeltaOutsideProfiling(absRepo, ver.ProductRevision)
		log.Printf("product_revision %s: %d changed files outside profiling/ (-1 = unverifiable)", ver.ProductRevision[:min(12, len(ver.ProductRevision))], ver.ProductDeltaOutsideProfiling)
	} else {
		ver.ProductDeltaOutsideProfiling = -1
	}

	if *manifestPath != "" {
		ref, merr := harness.LoadManifestRef(*manifestPath)
		if merr != nil {
			// Claiming an attestation the harness cannot read would print a
			// provenance field that means nothing.
			log.Fatalf("acceptance manifest: %v", merr)
		}
		ver.AcceptanceManifest = ref
		log.Printf("acceptance manifest %s sha256 %s source %s tree %s subject_binary %s quota %s", ref.Path, shortHash(ref.SHA256), ref.SourceRevision, shortHash(ref.SourceTree), shortHash(ref.SubjectBinarySHA256), ref.Quota)
	} else {
		log.Printf("acceptance manifest: NOT REFERENCED (-manifest unset): a measurement phase will be refused by the manifest discipline rule")
	}

	base := harness.Report{
		Kind: *phase, Label: "ahp-baseline-profiling",
		Methodology:    loadPatternLabel() + ", isolated instance, own ports/certs/identity, lane DB; latency recorded per request (TTFB via httptrace / first stream byte); samplers read-only; no secrets recorded",
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339), RunID: runID, RunIDSource: runIDSource,
		Machine: machine, Version: ver,
		Instance: harness.InstanceInfo{Tenant: "prof-" + runID, TLS: "mTLS SPIFFE (run-scoped CA)", LiveStand: liveStandPorts, Database: dbName},
		Notes:    []string{},
	}

	ctx := &runCtx{cm: cm, tmp: tmp, runID: runID, db: dbName, ver: ver}
	switch *phase {
	case "smoke":
		phaseSmoke(ctx, &base)
	case "overhead":
		phaseOverhead(ctx, &base)
	case "recovery":
		phaseRecovery(ctx, &base)
	case "soak":
		phaseSoak(ctx, &base)
	case "depfail":
		phaseDepFail(ctx, &base)
	case "idle":
		phaseIdle(ctx, &base)
	case "llm":
		phaseLLM(ctx, &base)
	default:
		log.Fatalf("unknown phase %q", *phase)
	}
	switch *phase {
	case "overhead", "recovery", "soak", "llm":
		q := harness.DefaultFinalRequirements()
		if *minSamples > q.MinSamplesPerStep {
			q.MinSamplesPerStep = *minSamples
		}
		if h := os.Getenv("AHP_PROFILE_EXPECTED_HOST"); h != "" {
			q.ExpectedHostname = h
		}
		if c := os.Getenv("AHP_PROFILE_EXPECTED_CPU"); c != "" {
			q.ExpectedCPUModelSub = c
		}
		base.ApplyQualityGuard(q)
		// Manifest discipline: a measurement phase must say which attested
		// candidate it measured, or it is not a profile of anything.
		mv, mn := base.ManifestDiscipline()
		base.Notes = append(base.Notes, mn...)
		for _, x := range mn {
			log.Printf("acceptance manifest: %s", x)
		}
		if len(mv) > 0 {
			base.Verdict = "FAIL"
			base.VerdictReasons = append(base.VerdictReasons, mv...)
		}
		log.Printf("%s run verdict after data-quality guard: %s %v", *phase, base.Verdict, base.VerdictReasons)
	}
	stem := "profiling-" + *phase + "-" + runID
	paths, err := base.WriteOutputs(*outDir, stem)
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range paths {
		log.Printf("wrote %s", p)
	}
}

func randSuffix() string { return strconv.FormatInt(int64(rand.Intn(1<<31)), 36) }

// tailPlatformLog prints the platform's own log tail for diagnostics when a
// phase sees errors (structured server log; the platform already redacts
// credentials).
func tailPlatformLog(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("platform log unavailable: %v", err)
		return
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) > 60 {
		lines = lines[len(lines)-60:]
	}
	for _, l := range lines {
		if true {
			if len(l) > 300 {
				l = l[:300]
			}
			log.Printf("platform: %s", l)
		}
	}
}

type runCtx struct {
	cm    harness.CertMaterial
	tmp   string
	runID string
	db    string
	ver   harness.VersionInfo
}

// spawn starts one isolated instance with a unique workload identity.
func (c *runCtx) spawn(tag string, pgProxyPort int) (*harness.Platform, harness.ClientCertFile, string, string) {
	tenant := fmt.Sprintf("prof-%s", c.runID)
	deployment := fmt.Sprintf("%s-%s", tag, randSuffix())
	cc, err := harness.MintClientCert(c.cm, tenant, deployment)
	if err != nil {
		log.Fatalf("mint client cert: %v", err)
	}
	port, err := harness.FreePort()
	if err != nil {
		log.Fatal(err)
	}
	p, err := harness.Start(harness.PlatformConfig{
		Binary: c.ver.BinaryPath, Certs: c.cm, GatewayPort: port, PGProxyPort: pgProxyPort,
		LogPath: filepath.Join(c.tmp, "ahp-"+tag+".log"),
		// The state plane is authorized through the product exchange boundary, which
		// only exists when the browser face is mounted and this instance's own
		// deployment is an allowlisted exchanger. Without these the harness could
		// only fall back to a self-signed token, which is what this fix removes.
		ExtraEnv: []string{"AHP_ENABLE_BROWSER=1", "AHP_IDENTITY_EXCHANGER_DEPLOYMENTS=" + deployment},
	})
	if err != nil {
		log.Fatalf("start instance %s: %v", tag, err)
	}
	return p, cc, tenant, deployment
}

// session authorizes one isolated instance through the product boundary and
// bootstraps a worker thread on it. The State credential is a scoped task token
// obtained from /v1/identity/control/exchange with capability "state": a
// human account provisioned through the documented operator seam (see
// seedProfilerUser) is exchanged for a token whose actor chain the product copies
// from the live grant. The harness signs nothing itself.
//
// This replaces a self-signed dev token that left the actor chain empty, which
// made the platform reject CreateThread with 403 "identity: token carries an
// empty actor chain" (internal/identity/tokens.go). If the live exchange cannot be
// reached the run fails closed here rather than minting a token that would only
// fail downstream -- so a State number can never be produced by a forged grant.
func (c *runCtx) session(rep *harness.Report, p *harness.Platform, cc harness.ClientCertFile, tag string) (*harness.Caller, *harness.Session) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		log.Fatalf("state authorization for %s needs AHP_TEST_DATABASE_URL to provision the exchange account through the documented operator seam; the harness refuses to fall back to a self-signed token", tag)
	}
	username := "profiler-" + tag + "-" + randSuffix()
	subject := "prof-state-" + tag + "-" + c.runID
	taskName := "prof-state-" + tag + "-" + c.runID
	ctx0, cancel0 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel0()
	password, err := seedProfilerUser(ctx0, dsn, cc.Tenant, subject, username, []string{"space:access", "state"})
	if err != nil {
		log.Fatalf("seed state account %s: %v", tag, err)
	}
	token, boundTask, err := scopedTaskToken(ctx0, p.Port, c.cm, cc, username, password, taskName, []string{"state"})
	if err != nil {
		log.Fatalf("state scoped task token %s: %v", tag, err)
	}
	if err := harness.RequireNonEmptyActorChain(token, "state"); err != nil {
		log.Fatalf("state authorization %s: %v", tag, err)
	}
	log.Printf("state scoped task token acquired (tag %s task %s)", tag, boundTask)
	caller, err := harness.NewCaller(p.Port, c.cm, cc, 512)
	if err != nil {
		log.Fatal(err)
	}
	// The state token TTL is short (product runTokenTTL = 10m) against a ladder or a
	// soak; re-exchange it off the measured path so a >TTL phase stays authorized
	// without the refresh ever entering a latency sample. username/password live in
	// this closure only, for re-login -- never logged or serialized; authFailure keeps
	// error bodies (and any echoed bearer) out of the log.
	provider := func(ctx context.Context) (string, error) {
		t, _, e := scopedTaskToken(ctx, p.Port, c.cm, cc, username, password, taskName, []string{"state"})
		return t, e
	}
	if err := caller.UseLiveToken(token, provider, 2*time.Minute); err != nil {
		log.Fatalf("state live-token setup %s: %v", tag, err)
	}
	if rep != nil {
		if rep.Authorization == nil {
			rep.Authorization = harness.NewAuthorizationLedger()
		}
		rep.Authorization.RecordPlane(harness.PlaneMechanism("state", []string{"state"}))
		rep.Authorization.RecordSession(
			harness.SessionAuthorization{Tag: tag, Plane: "state", Task: boundTask, ExchangerDeployment: cc.Deployme, Capabilities: []string{"state"}},
			caller.RotationsFunc(), caller.RotationError, caller.RotationOutsideMeasurementWindow,
		)
	}
	sess, err := harness.Bootstrap(caller, "ctx-"+tag, "prof-worker-"+tag)
	if err != nil {
		log.Fatalf("bootstrap %s: %v", tag, err)
	}
	return caller, sess
}

// ---------------------------------------------------------------------------
// Phase: smoke — explicitly NOT a load test
// ---------------------------------------------------------------------------

func phaseSmoke(c *runCtx, rep *harness.Report) {
	rep.Kind = "smoke"
	rep.Methodology += "; SMOKE ONLY: single requests, no concurrency, NOT a load measurement"
	p, cc, tenant, dep := c.spawn("smoke", 0)
	defer p.Stop()
	rep.Instance.GatewayPort = p.Port
	rep.Instance.MeasuredProcess = harness.DescribeProcess(p.PID(), harness.PlatformRole)
	rep.Instance.Tenant, rep.Instance.Deployment = tenant, dep
	caller, sess := c.session(rep, p, cc, "smoke")
	defer sess.StopHeartbeat()
	ctx := context.Background()
	calls := []harness.Call{
		caller.Get(ctx, "/healthz", "healthz", false),
		caller.Get(ctx, "/readyz", "readyz", false),
		caller.Get(ctx, "/debug/stats", "debug_stats", false),
		caller.Do(ctx, "/v1/state/GetThread", "GetThread", map[string]any{"thread_id": sess.ThreadID}),
	}
	var steps []harness.StepResult
	for _, cl := range calls {
		s := harness.StepResult{Workload: cl.Op, Concurrency: 1, Requests: 1, DurationSec: cl.Totalms / 1000}
		if cl.OK {
			// One call is one observation: the harness tail rule decides which
			// quantiles that can support (p50 yes, p95/p99 no). Hand-quoting
			// the same number as p50=p95=p99 here is what produced
			// "ttfb_p99_ms" on a step with requests=1.
			tails := harness.Tails([]float64{cl.Totalms}, &s.PercentileNote)
			tm := cl.Totalms
			s.P50, s.P95, s.P99, s.Mean, s.Max = tails.P50, tails.P95, tails.P99, &tm, &tm
			// TTFB stays null unless a first byte was actually observed
			// (DEF-PROF-1: no 0-ms placeholders).
			if cl.TTFBms > 0 {
				tt := harness.Tails([]float64{cl.TTFBms}, &s.TTFBPercentileNote)
				s.TTFBP50, s.TTFBP95, s.TTFBP99 = tt.P50, tt.P95, tt.P99
			}
		} else {
			s.Errors = 1
			s.ErrorClasses = map[string]int{cl.ErrClass: 1}
		}
		steps = append(steps, s)
	}
	rep.Steps = steps
	if failed := countErr(steps); failed > 0 {
		log.Fatalf("smoke failures: %d", failed)
	}
	log.Printf("smoke ok: %d checks", len(calls))
}

func countErr(steps []harness.StepResult) int {
	n := 0
	for _, s := range steps {
		n += s.Errors
	}
	return n
}

// ---------------------------------------------------------------------------
// Phase: overhead — deterministic platform overhead, concurrency ladder
// ---------------------------------------------------------------------------

type workload struct {
	name  string
	setup func(caller *harness.Caller, sess *harness.Session) error
	op    func(caller *harness.Caller, sess *harness.Session, worker int, seq *atomic.Int64) harness.Call
}

func (c *runCtx) workloads() []workload {
	var kvKeys []string
	all := []workload{
		{
			name: "edge_health",
			op: func(caller *harness.Caller, sess *harness.Session, worker int, seq *atomic.Int64) harness.Call {
				return caller.Get(context.Background(), "/healthz", "healthz", false)
			},
		},
		{
			name: "state_read_thread",
			op: func(caller *harness.Caller, sess *harness.Session, worker int, seq *atomic.Int64) harness.Call {
				return caller.Do(context.Background(), "/v1/state/GetThread", "GetThread", map[string]any{"thread_id": sess.ThreadID})
			},
		},
		{
			name: "kv_write",
			op: func(caller *harness.Caller, sess *harness.Session, worker int, seq *atomic.Int64) harness.Call {
				n := seq.Add(1)
				key := fmt.Sprintf("k-%d", n)
				return caller.Do(context.Background(), "/v1/state/PutKV", "PutKV", map[string]any{
					"fence": sess.Fence, "namespace": []string{"prof"}, "key": key,
					"operation_key": fmt.Sprintf("op-%s-%d", c.runID, n), "expected_version": 0,
					"value": json.RawMessage(`{"blob":"` + strings.Repeat("x", 512) + `"}`),
				})
			},
		},
		{
			name: "kv_read",
			setup: func(caller *harness.Caller, sess *harness.Session) error {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				n := int64(0)
				for i := 0; i < 50; i++ {
					n++
					key := fmt.Sprintf("rk-%d", n)
					cr := caller.Do(ctx, "/v1/state/PutKV", "PutKV", map[string]any{
						"fence": sess.Fence, "namespace": []string{"prof"}, "key": key,
						"operation_key": fmt.Sprintf("rop-%s-%d", c.runID, n), "expected_version": 0,
						"value": json.RawMessage(`{"v":1}`),
					})
					if !cr.OK {
						return fmt.Errorf("seed PutKV: %+v", cr)
					}
					kvKeys = append(kvKeys, key)
				}
				return nil
			},
			op: func(caller *harness.Caller, sess *harness.Session, worker int, seq *atomic.Int64) harness.Call {
				n := seq.Add(1)
				key := kvKeys[int((n-1)%int64(len(kvKeys)))]
				return caller.Do(context.Background(), "/v1/state/GetKV", "GetKV", map[string]any{"namespace": []string{"prof"}, "key": key})
			},
		},
		{
			name: "ckpt_write",
			op: func(caller *harness.Caller, sess *harness.Session, worker int, seq *atomic.Int64) harness.Call {
				n := seq.Add(1)
				part := json.RawMessage(`"` + strings.Repeat("y", 512) + `"`)
				return caller.Do(context.Background(), "/v1/state/PutCheckpoint", "PutCheckpoint", map[string]any{
					"fence": sess.Fence,
					"checkpoint": map[string]any{
						"id": fmt.Sprintf("cp-%s-%d", c.runID, n), "thread_id": sess.ThreadID,
						"namespace": "prof", "version": 1, "parts": []any{map[string]any{"channel": "messages", "version": "v1", "data": part}},
					},
					"key": fmt.Sprintf("ck-%s-%d", c.runID, n),
				})
			},
		},
	}
	if f := strings.TrimSpace(*workloadFilter); f != "" {
		want := map[string]bool{}
		for _, w := range strings.Split(f, ",") {
			if w = strings.TrimSpace(w); w != "" {
				want[w] = true
			}
		}
		var out []workload
		for _, w := range all {
			if want[w.name] {
				out = append(out, w)
			}
		}
		if len(out) == 0 {
			log.Fatalf("-workloads %q matches nothing in %v", f, workloadNames(all))
		}
		return out
	}
	return all
}

func workloadNames(ws []workload) []string {
	var out []string
	for _, w := range ws {
		out = append(out, w.name)
	}
	return out
}

func parseInts(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		if v, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
			out = append(out, v)
		}
	}
	return out
}

func (c *runCtx) ladder(rep *harness.Report, wl workload, tag string) {
	steps := parseInts(*ladder)
	ext := parseInts(*extended)
	all := append(append([]int{}, steps...), ext...)
	p, cc, tenant, dep := c.spawn(tag, 0)
	defer p.Stop()
	rep.Instance.GatewayPort = p.Port
	rep.Instance.MeasuredProcess = harness.DescribeProcess(p.PID(), harness.PlatformRole)
	rep.Instance.Tenant, rep.Instance.Deployment = tenant, dep
	caller, sess := c.session(rep, p, cc, tag)
	defer sess.StopHeartbeat()
	if wl.setup != nil {
		if err := wl.setup(caller, sess); err != nil {
			log.Fatalf("workload setup %s: %v", wl.name, err)
		}
	}
	pool, err := lanePool()
	if err != nil {
		log.Printf("warn: PG sampler disabled: %v", err)
	} else {
		defer pool.Close()
	}
	scfg := harness.SamplerConfig{Platform: p, Pool: pool, RedisAddr: redisAddr(), MinioHealth: minioHealth(), PGContainer: "ahp-handbook-acceptance-pg", MinioContainer: "aoh-not-present", Interval: time.Duration(*sampleSeconds) * time.Second, SuppressContainerProbes: probesOutsideWindow(), DockerEvery: 2}
	scfg.MinioContainer = "ahp-handbook-go-minio-1"
	stream := harness.StartSampling(scfg)
	defer stream.Stop()
	lastCut := time.Now() // attribution cuts start with the stream

	ph := harness.SubPhase{Name: "overhead:" + wl.name}
	var baseP95 *float64
	var seq atomic.Int64
	boundary := ""
	// Session topology is part of what a number means: a shared hot thread
	// measures the hot path, per-worker threads measure how many USERS the
	// platform can serve (each with its own thread, task grant and fence).
	perWorker := *sessionsMode == "per-worker"
	var wsess []*workerSession
	ensure := func(n int) {
		for len(wsess) < n {
			ws := c.workerSession(rep, p, cc, fmt.Sprintf("%s-w%d", tag, len(wsess)))
			wsess = append(wsess, ws)
		}
	}
	if perWorker {
		ensure(1)
		defer func() {
			for _, w := range wsess {
				w.sess.StopHeartbeat()
			}
		}()
	}
	forEachTarget := func(worker int) (*harness.Caller, *harness.Session) {
		if !perWorker {
			return caller, sess
		}
		w := wsess[worker%len(wsess)]
		return w.caller, w.sess
	}
	for i, conc := range all {
		if perWorker {
			ensure(conc)
		}
		for r := 1; r <= *repeats; r++ {
			var rec *harness.LatencyRecorder
			var wall time.Duration
			var offered, dropped, maxq int
			call := func(worker int) harness.Call {
				cl, ts := forEachTarget(worker)
				return wl.op(cl, ts, worker, &seq)
			}
			if *loadPattern == "open" {
				res := harness.RunOpenLoop(harness.OpenLoopOptions{RatePerSec: *openRate, Workers: conc, Duration: time.Duration(*stepSeconds) * time.Second}, call)
				rec, wall = res.Rec, res.Wall
				offered, dropped, maxq = res.Offered, res.Dropped, res.MaxQueue
			} else {
				rec, wall = harness.RunClosedLoop(conc, time.Duration(*stepSeconds)*time.Second, call)
			}
			step := tagStep(rec.Summarize(wl.name, conc, wall))
			step.Repeat = r
			// offered/shedded/queue arrive through the recorder itself; the
			// locals exist only for the progress line.
			_ = offered
			_ = dropped
			_ = maxq
			// Contiguous attribution: from the previous cut to now; samples that
			// complete during the settle gap belong to the step they ran under.
			now := time.Now()
			subs := windowSamples(stream.Samples(), lastCut, now)
			lastCut = now
			if len(subs) > 0 {
				summary := harness.SummarizeSamples(subs)
				summary.TickIntervalMS = harness.I64ptr(sampleTick().Milliseconds())
				step.Samplers = &summary
				step.CacheState = harness.ProbeCache(subs)
			}
			ph.Samples = append(ph.Samples, subs...)
			if conc == all[0] && r == 1 {
				baseP95 = step.P95
			}
			// EXPLORATION THRESHOLDS, not SLOs and not capacity limits: >5%
			// errors, p95 > 10x the c=1 baseline, or p99 above 10s. They say
			// "the ladder stopped being interesting here", nothing more.
			if i >= len(steps) {
				switch {
				case step.ErrorRatePct > 5:
					boundary = fmt.Sprintf("exploration threshold reached at concurrency %d (not an SLO): error rate %.2f%% > 5%%", conc, step.ErrorRatePct)
				case baseP95 != nil && *baseP95 > 0 && step.P95 != nil && *step.P95 > 10**baseP95:
					boundary = fmt.Sprintf("exploration threshold reached at concurrency %d (not an SLO): p95 %.1fms > 10x baseline %.1fms", conc, *step.P95, *baseP95)
				case step.P99 != nil && *step.P99 > 10000:
					boundary = fmt.Sprintf("exploration threshold reached at concurrency %d (not an SLO): p99 %.1fms > 10000ms", conc, *step.P99)
				}
			}
			log.Printf("rung done %s c=%d r=%d ok=%d err=%d p50=%sms p95=%sms p99=%sms err_p95=%sms rps=%.1f samples=%s cache=%s offered=%d shedded=%d load1=%s",
				wl.name, conc, r, step.Requests-step.Errors, step.Errors, ms(step.P50), ms(step.P95), ms(step.P99), ms(step.ErrP95), step.ThroughputRPS, samplesLabelOf(step), cacheLabelOf(step), offered, dropped, ms1(harness.Load1()))
			harness.AppendLiveStep(*outDir, fmt.Sprintf("overhead-%s", c.runID), "overhead:"+wl.name, step, nil)
			ph.Steps = append(ph.Steps, step)
			if boundary != "" {
				break
			}
			time.Sleep(3 * time.Second) // settle between steps
		}
		if boundary != "" {
			break
		}
	}
	if boundary == "" {
		boundary = "no degradation boundary reached within configured ladder"
	}
	rep.Notes = append(rep.Notes, wl.name+": "+boundary)
	rep.Phases = append(rep.Phases, ph)
}

func phaseOverhead(c *runCtx, rep *harness.Report) {
	rep.LoadProfile = map[string]any{"step_seconds": *stepSeconds, "ladder": *ladder, "ladder_extended": *extended,
		"pattern": *loadPattern, "open_rate_rps": *openRate, "repeats": *repeats, "sessions_mode": *sessionsMode}
	if *loadPattern == "closed" {
		rep.Methodology += "; CLOSED-LOOP: measures what the system finished, not what arrived -- offered load falls with latency, so saturation shows as lower RPS rather than as queueing"
	} else {
		rep.Methodology += fmt.Sprintf("; OPEN-LOOP at %.1f req/s offered: queue depth and shedded arrivals are reported, so saturation is visible", *openRate)
	}
	if *sessionsMode == "per-worker" {
		rep.Methodology += "; sessions=per-worker: every worker owns thread+task-grant+fence, so these numbers may be signed as capacity for users"
	} else {
		rep.Methodology += "; sessions=hot-thread: one thread/grant shared by all workers -- hot-path throughput ONLY, NOT user capacity"
	}
	rep.Methodology += "; degradation rules (5% errors, 10x p95, 10s p99) are EXPLORATION THRESHOLDS for where to stop the ladder, not SLOs and not capacity limits"
	for _, wl := range c.workloads() {
		c.ladder(rep, wl, "oh-"+sanitizeTag(wl.name))
	}
}

func sanitizeTag(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// ---------------------------------------------------------------------------
// Phase: recovery — same step before and after a heavy burst
// ---------------------------------------------------------------------------

func phaseRecovery(c *runCtx, rep *harness.Report) {
	rep.LoadProfile = map[string]any{"baseline_concurrency": 10, "burst_concurrency": *burstConc, "burst_seconds": *burstSecs, "idle_seconds": *idleSecs}
	wl := c.workloads()[2] // kv_write
	p, cc, tenant, dep := c.spawn("rec", 0)
	defer p.Stop()
	rep.Instance.GatewayPort = p.Port
	rep.Instance.MeasuredProcess = harness.DescribeProcess(p.PID(), harness.PlatformRole)
	rep.Instance.Tenant, rep.Instance.Deployment = tenant, dep
	caller, sess := c.session(rep, p, cc, "rec")
	defer sess.StopHeartbeat()
	pool, err := lanePool()
	if err == nil {
		defer pool.Close()
	}
	scfg := harness.SamplerConfig{Platform: p, Pool: pool, RedisAddr: redisAddr(), MinioHealth: minioHealth(), PGContainer: "ahp-handbook-acceptance-pg", MinioContainer: "ahp-handbook-go-minio-1", Interval: time.Duration(*sampleSeconds) * time.Second, SuppressContainerProbes: probesOutsideWindow(), DockerEvery: 2}
	stream := harness.StartSampling(scfg)
	lastCut := time.Now()
	var seq atomic.Int64
	run := func(name string, conc, secs int) (harness.StepResult, []harness.Sample) {
		rec, wall := harness.RunClosedLoop(conc, time.Duration(secs)*time.Second, func(worker int) harness.Call {
			return wl.op(caller, sess, worker, &seq)
		})
		step := tagStep(rec.Summarize("kv_write", conc, wall))
		// Contiguous attribution: from the previous cut to now; samples that
		// complete during the settle gap belong to the step they ran under.
		now := time.Now()
		subs := windowSamples(stream.Samples(), lastCut, now)
		lastCut = now
		if len(subs) > 0 {
			sm := harness.SummarizeSamples(subs)
			sm.TickIntervalMS = harness.I64ptr(sampleTick().Milliseconds())
			step.Samplers = &sm
		}
		return step, subs
	}
	base, sBase := run("baseline", 10, 30)
	rep.Notes = append(rep.Notes, "baseline before burst")
	rep.Phases = append(rep.Phases, harness.SubPhase{Name: "baseline", Steps: []harness.StepResult{base}, Samples: sBase})
	burst, sBurst := run("burst", *burstConc, *burstSecs)
	rep.Phases = append(rep.Phases, harness.SubPhase{Name: "burst", Steps: []harness.StepResult{burst}, Samples: sBurst})
	time.Sleep(time.Duration(*idleSecs) * time.Second)
	after, sAfter := run("post-idle", 10, 30)
	rep.Phases = append(rep.Phases, harness.SubPhase{Name: "post_idle", Steps: []harness.StepResult{after}, Samples: sAfter})
	samples := stream.Stop()
	_ = samples
	summary := harness.SummarizeSamples(append(append(append([]harness.Sample{}, sBase...), sBurst...), sAfter...))
	summary.TickIntervalMS = harness.I64ptr(sampleTick().Milliseconds())
	rep.Summary = &summary
	rep.Notes = append(rep.Notes,
		fmt.Sprintf("recovery ratio p95 post/baseline = %s (post %sms vs baseline %sms)", ratioOrNA(after.P95, base.P95), ms(after.P95), ms(base.P95)),
		fmt.Sprintf("goroutines start %s end %s (max %s); RSS start %s end %s (peak %s)", ni(summary.GoroutinesStart), ni(summary.GoroutinesEnd), ni(summary.GoroutinesMax), nb(summary.RSSStartBytes), nb(summary.RSSEndBytes), nb(summary.RSSMaxBytes)),
		"recovery measured on the same instance without restart",
	)
}

// ---------------------------------------------------------------------------
// Phase: soak — long steady run, leak analysis
// ---------------------------------------------------------------------------

func phaseSoak(c *runCtx, rep *harness.Report) {
	rep.LoadProfile = map[string]any{"concurrency": 10, "minutes": *soakMinutes, "pattern": "closed-loop mixed kv_write/kv_read/state_read/edge"}
	wls := c.workloads()
	pick := []workload{wls[0], wls[1], wls[2], wls[3]}
	if len(wls) > 4 {
		pick = []workload{wls[0], wls[1], wls[2], wls[3]}
	}
	p, cc, tenant, dep := c.spawn("soak", 0)
	defer p.Stop()
	rep.Instance.GatewayPort = p.Port
	rep.Instance.MeasuredProcess = harness.DescribeProcess(p.PID(), harness.PlatformRole)
	rep.Instance.Tenant, rep.Instance.Deployment = tenant, dep
	caller, sess := c.session(rep, p, cc, "soak")
	defer sess.StopHeartbeat()
	if err := pick[3].setup(caller, sess); err != nil {
		log.Fatalf("kv seed: %v", err)
	}
	pool, err := lanePool()
	if err == nil {
		defer pool.Close()
	}
	scfg := harness.SamplerConfig{Platform: p, Pool: pool, RedisAddr: redisAddr(), MinioHealth: minioHealth(), PGContainer: "ahp-handbook-acceptance-pg", MinioContainer: "ahp-handbook-go-minio-1", Interval: time.Duration(*sampleSeconds) * time.Second, SuppressContainerProbes: probesOutsideWindow(), DockerEvery: 3}
	stream := harness.StartSampling(scfg)
	var seq atomic.Int64
	var variants []*harness.LatencyRecorder
	names := []string{"edge", "read", "kv_write", "kv_read"}
	per := 10
	workers := 10
	var recMu []*harness.LatencyRecorder
	_ = recMu
	recs := make([]*harness.LatencyRecorder, len(pick))
	for i := range recs {
		recs[i] = harness.NewLatencyRecorder()
		_ = per
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*soakMinutes)*time.Minute)
	defer cancel()
	done := make(chan struct{})
	for w := 0; w < workers; w++ {
		go func(worker int) {
			for {
				select {
				case <-ctx.Done():
					done <- struct{}{}
					return
				default:
				}
				i := worker % len(pick)
				recs[i].Record(pick[i].op(caller, sess, worker, &seq))
			}
		}(w)
	}
	for i := 0; i < workers; i++ {
		<-done
	}
	wall := time.Since(start)
	_ = variants
	samples := stream.Stop()
	rep.Samples = samples
	var steps []harness.StepResult
	for i, r := range recs {
		steps = append(steps, tagStep(r.Summarize(names[i], workers, wall)))
	}
	rep.Steps = steps
	summary := harness.SummarizeSamples(samples)
	summary.TickIntervalMS = harness.I64ptr(sampleTick().Milliseconds())
	rep.Summary = &summary
	rep.Notes = append(rep.Notes,
		fmt.Sprintf("soak %v: RSS slope %s bytes/min; goroutines slope %s/min; RSS %s -> %s (max %s); goroutines %s -> %s",
			wall.Round(time.Second), slopeOrNA(summary.RSSSlopeBytesPerMin, 0), slopeOrNA(summary.GoroutinesSlopePerMin, 2), nb(summary.RSSStartBytes), nb(summary.RSSEndBytes), nb(summary.RSSMaxBytes), ni(summary.GoroutinesStart), ni(summary.GoroutinesEnd)),
		"PG/MinIO container counters and Redis deltas summarized in model field; PG disk IO attributed via container block counters",
	)
}

// ---------------------------------------------------------------------------
// Phase: depfail — dependency loss via harness-owned PG proxy
// ---------------------------------------------------------------------------

func phaseDepFail(c *runCtx, rep *harness.Report) {
	rep.LoadProfile = map[string]any{"concurrency": 10, "phase_a_s": *steadySecs, "outage_s": *outageSecs, "phase_c_s": *steadySecs, "fault": "harness TCP proxy cut/restore on port owned by harness; PG container never restarted"}
	proxyPort, err := harness.FreePort()
	if err != nil {
		log.Fatal(err)
	}
	upstream := fmt.Sprintf("127.0.0.1:%s", envDefault("AHP_PG_PORT", "25495"))
	proxy, err := harness.StartProxy(proxyPort, upstream)
	if err != nil {
		log.Fatal(err)
	}
	defer proxy.Stop()
	startedAt := pgContainerUptime()
	p, cc, tenant, dep := c.spawn("df", proxyPort)
	defer p.Stop()
	rep.Instance.GatewayPort = p.Port
	rep.Instance.MeasuredProcess = harness.DescribeProcess(p.PID(), harness.PlatformRole)
	rep.Instance.Tenant, rep.Instance.Deployment = tenant, dep
	caller, sess := c.session(rep, p, cc, "df")
	defer sess.StopHeartbeat()
	wl := c.workloads()[2]
	var seq atomic.Int64
	dpool, derr := lanePool()
	if derr == nil {
		defer dpool.Close()
	}
	scfg := harness.SamplerConfig{Platform: p, Pool: dpool, RedisAddr: redisAddr(), MinioHealth: minioHealth(), PGContainer: "ahp-handbook-acceptance-pg", MinioContainer: "ahp-handbook-go-minio-1", Interval: time.Duration(*sampleSeconds) * time.Second, SuppressContainerProbes: probesOutsideWindow(), DockerEvery: 2}
	stream := harness.StartSampling(scfg)
	lastCut := time.Now()
	run := func(name string, secs int) (harness.StepResult, []harness.Sample) {
		rec, wall := harness.RunClosedLoop(10, time.Duration(secs)*time.Second, func(worker int) harness.Call {
			return wl.op(caller, sess, worker, &seq)
		})
		step := tagStep(rec.Summarize("kv_write_behind_proxy", 10, wall))
		// Contiguous attribution: from the previous cut to now; samples that
		// complete during the settle gap belong to the step they ran under.
		now := time.Now()
		subs := windowSamples(stream.Samples(), lastCut, now)
		lastCut = now
		if len(subs) > 0 {
			sm := harness.SummarizeSamples(subs)
			sm.TickIntervalMS = harness.I64ptr(sampleTick().Milliseconds())
			step.Samplers = &sm
		}
		return step, subs
	}
	a, sa := run("phase_a", *steadySecs)
	rep.Phases = append(rep.Phases, harness.SubPhase{Name: "steady", Steps: []harness.StepResult{a}, Samples: sa})
	proxy.Cut()
	b, sb := run("outage", *outageSecs)
	rep.Phases = append(rep.Phases, harness.SubPhase{Name: "outage", Steps: []harness.StepResult{b}, Samples: sb})
	if err := proxy.Restore(); err != nil {
		log.Fatalf("proxy restore: %v", err)
	}
	time.Sleep(10 * time.Second) // let pool reconnect
	cStep, sc := run("phase_c", *steadySecs)
	rep.Phases = append(rep.Phases, harness.SubPhase{Name: "restored", Steps: []harness.StepResult{cStep}, Samples: sc})
	rep.Samples = append(append(append([]harness.Sample{}, sa...), sb...), sc...)
	stream.Stop()
	endUptime := pgContainerUptime()
	rep.Notes = append(rep.Notes,
		fmt.Sprintf("outage error rate %.2f%% (expected: errors while dependency down)", b.ErrorRatePct),
		fmt.Sprintf("post-restore error rate %.2f%% without platform restart (recovery %s)", cStep.ErrorRatePct, map[bool]string{true: "confirmed", false: "NOT confirmed"}[cStep.ErrorRatePct == 0]),
		fmt.Sprintf("PG container uptime unchanged: %v (container never restarted by harness)", startUptimeEqual(startedAt, endUptime)),
		fmt.Sprintf("proxy counters: conns=%d up_bytes=%d down_bytes=%d", proxy.ConnTotal.Load(), proxy.BytesUp.Load(), proxy.BytesDown.Load()),
	)
	health := caller.Get(context.Background(), "/healthz", "healthz", false)
	rep.Notes = append(rep.Notes, fmt.Sprintf("platform /healthz after restore: %d", health.Status))
}

func startUptimeEqual(a, b string) bool { return a == b && a != "" }

func pgContainerUptime() string {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.StartedAt}}", "ahp-handbook-acceptance-pg").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// ---------------------------------------------------------------------------
// Phase: llm — REAL Qwen end-to-end (limited volume; tokens, not currency)
// ---------------------------------------------------------------------------

// llmPhaseStats reports one LLM phase with the transport plane and the
// terminal plane counted apart (DEF-PROF-1): transport_errors are failed HTTP
// calls; terminal_* count durable generation outcomes; the embedded
// PhaseAccounting carries logical_errors (= requests - terminal_completed,
// broken down by reason) and the phase verdict. The old single `errors`
// counter hid a FAILED generation and three unresolved streams behind
// "errors=0" (evidence profiling-llm-20261109T161709Z-y9f8mc.json).
type llmPhaseStats struct {
	Phase        string           `json:"phase"`
	Requests     int              `json:"requests"`
	ErrorClasses map[string]int   `json:"error_classes,omitempty"`
	UsageTotals  map[string]int64 `json:"usage_tokens_totals"`
	P50ms        *float64         `json:"p50_ms"`
	P95ms        *float64         `json:"p95_ms"`
	P99ms        *float64         `json:"p99_ms"`
	TTFBP50ms    *float64         `json:"ttfb_p50_ms"`
	TTFBP95ms    *float64         `json:"ttfb_p95_ms"`
	TTFBP99ms    *float64         `json:"ttfb_p99_ms"`
	// ModelName is null for a phase whose calls never yielded a model-bearing
	// durable row (the stream phase collected none): "" here read as "a model
	// named nothing served these requests".
	ModelName    *string        `json:"model"`
	StatusCounts map[string]int `json:"generation_status_counts"`
	harness.PhaseAccounting
}

// profilingProvidersSource locates the provider configuration the deployment
// actually routes by: the product config point infra/model/providers.json.
// AHP_PROFILING_PROVIDERS_SOURCE overrides it (CI/lane runs may pin a copy);
// otherwise the file is searched upward from the working directory, the way
// module tools find a repository root. The profiler refuses to run its LLM
// phase when the source is missing: measuring an identity that cannot be
// named by the config point is worthless, so a silent default identity is
// exactly what is NOT allowed here.
func profilingProvidersSource() (string, error) {
	if v := strings.TrimSpace(os.Getenv("AHP_PROFILING_PROVIDERS_SOURCE")); v != "" {
		return v, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(dir, "infra", "model", "providers.json")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("infra/model/providers.json not found from %s; set AHP_PROFILING_PROVIDERS_SOURCE", dir)
		}
		dir = parent
	}
}

// profiledIdentity derives the (provider, model) the profiling LLM phase will
// measure from the SAME config point production routing loads, through the
// same loader: the default ("*") route's provider; the model is that
// provider's routed model. AHP_LLM_MODEL must agree with the routed
// configuration -- when it names something the default route does not serve,
// the profiler stops rather than silently measuring a different model than
// the one the deployment would answer with.
func profiledIdentity(table *routing.Table) (routing.Provider, string, error) {
	providerName := ""
	for _, rule := range table.Routes {
		for _, tenant := range rule.When.Tenants {
			if tenant == "*" {
				providerName = rule.Then.Provider
			}
		}
	}
	if providerName == "" && len(table.Routes) > 0 {
		providerName = table.Routes[0].Then.Provider
	}
	if providerName == "" {
		return routing.Provider{}, "", fmt.Errorf("provider config point declares no routes")
	}
	var chosen *routing.Provider
	for i := range table.Providers {
		if table.Providers[i].Name == providerName {
			chosen = &table.Providers[i]
		}
	}
	if chosen == nil {
		return routing.Provider{}, "", fmt.Errorf("default route points at provider %q that the config point does not declare", providerName)
	}
	model := os.Getenv("AHP_LLM_MODEL")
	if model == "" {
		if len(chosen.Models) != 1 {
			return routing.Provider{}, "", fmt.Errorf("provider %q routes %d models; name the profiled one with AHP_LLM_MODEL", chosen.Name, len(chosen.Models))
		}
		model = chosen.Models[0]
	} else {
		found := false
		for _, m := range chosen.Models {
			if m == model {
				found = true
			}
		}
		if !found {
			return routing.Provider{}, "", fmt.Errorf("AHP_LLM_MODEL %q is not routed through provider %q by the config point: the profile would measure a different configuration than the deployment uses", model, chosen.Name)
		}
	}
	return *chosen, model, nil
}

// writeProfilingProviders writes the smallest honest label-aware routing table a
// Model Gateway face needs, DERIVED from the product config point
// (infra/model/providers.json): a single copy of the provider the default route
// already points at, carrying only the routed model. No identity is hardcoded
// here -- provider name, class, audience and secret_ref come from the config
// point, so the profile measures the identity the live stand would use, and the
// tree-wide identity ledger sees no unaccounted copy. With exactly one declared
// copy the bound provider is inferred from it, so the per-tenant quota gate
// stays unmounted (a throwaway profiling tenant has no quota rows, and an
// unmounted gate means unlimited rather than a fail-closed zero).
func writeProfilingProviders(path string) error {
	source, err := profilingProvidersSource()
	if err != nil {
		return err
	}
	table, err := routing.LoadFile(source)
	if err != nil {
		return err
	}
	provider, model, err := profiledIdentity(table)
	if err != nil {
		return err
	}
	fixture := map[string]any{
		"version":   table.Version,
		"providers": []any{provider},
		"routes": []any{map[string]any{
			"name": "default",
			"when": map[string]any{"tenants": []string{"*"}},
			"then": map[string]any{"provider": provider.Name},
		}},
	}
	// The single copy carries exactly the routed model; a profiling run must not
	// inherit unrelated models the source copy may declare.
	fixtureProviders := fixture["providers"].([]any)
	fp := fixtureProviders[0].(routing.Provider)
	fp.Quota = nil
	fp.Models = []string{model}
	fixtureProviders[0] = fp
	raw, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}

// outcomeFromDurable maps one durable generation row (ahp_model.generations)
// onto the accounting outcome. This is the boundary where the platform's CAUSE
// either reaches the artifact or is lost -- the old polling parser read only
// id/status/model/usage, so a phase with 200+ failed generations could only
// say "generation_failed" (Erdos's RED, cause_attribution_red_test.go.txt,
// sha256 2ccdf48863d007d698c51aeb0d3121cc44cdecef251c7c4a0a203a463c710820).
//
// Rules, and why:
//   - the durable error travels verbatim; CauseToken keys the breakdown and
//     nothing is invented when the row carries no error (cause_unavailable);
//   - COMPLETED with an empty answer is an empty answer, not a success;
//   - FAILED with no output says nothing about length: a length of 0 there
//     would be an inference, not an observation.
func outcomeFromDurable(status, errText, output string) harness.Outcome {
	oc := harness.Outcome{Status: status, Error: errText}
	if output != "" {
		n := len(output)
		oc.OutputLen = &n
	} else if status == "COMPLETED" {
		zero := 0
		oc.OutputLen = &zero
	}
	return oc
}

func phaseLLM(c *runCtx, rep *harness.Report) {
	rep.Methodology += "; REAL LLM calls to the configured Qwen endpoint ONLY, volume-bounded; authorization via the product path Human login -> control exchange -> scoped task token; platform reports TOKENS (prompt/completion/total), never currency"
	rep.LoadProfile = map[string]any{"submit_c1": *llmCountC1, "submit_c5": *llmCountC5, "submit_c10": *llmCountC10, "submit_c20": *llmCountC20, "submit_c30": *llmCountC30, "stream_c1": *llmStreamN, "max_tokens": *llmMaxTokens,
		// Whose knob this is, stated in the artifact: a cap too small for the
		// workload produces provider_empty_completion failures that look like
		// product defects. It is the harness's choice, and acceptance reads it
		// here rather than inferring it.
		"max_tokens_owner":         "harness measurement config (not platform, not SDK)",
		"max_tokens_floor_warning": *llmMaxTokens < 256,
		"configured_limits": map[string]any{"AHP_EXEC_TASK_TOKEN_LIMIT": envDefault("AHP_EXEC_TASK_TOKEN_LIMIT", "unset -> compiled default 20000 tokens/task"),
			"AHP_EXEC_MAX_TOKENS":      envDefault("AHP_EXEC_MAX_TOKENS", "unset -> compiled default 1024 tokens/response"),
			"AHP_EXEC_STEP_RESERVE":    envDefault("AHP_EXEC_STEP_RESERVE", "unset -> compiled default 4000 tokens"),
			"AHP_MODEL_QUOTA_WINDOW_S": envDefault("AHP_MODEL_QUOTA_WINDOW_S", "unset -> compiled default 60s")}}
	if *llmMaxTokens < 256 {
		rep.Notes = append(rep.Notes, fmt.Sprintf("measurement cap max_tokens=%d is below 256: expect provider_empty_completion failures that describe the harness budget, not the product (evidence: cap=64 gave 261 FAILED / 6 COMPLETED on qwen-38-27b-fp8)", *llmMaxTokens))
	}
	// A model-bearing instance: the same composition the acceptance stand uses
	// (browser surface + exchanger allowlist + model gateway), on this run's
	// own port and identity.
	tag := "llm-" + randSuffix()
	tenant := fmt.Sprintf("prof-%s", c.runID)
	deployment := tag
	cc, err := harness.MintClientCert(c.cm, tenant, deployment)
	if err != nil {
		log.Fatal(err)
	}
	providersFile := filepath.Join(c.tmp, "providers.json")
	if err := writeProfilingProviders(providersFile); err != nil {
		log.Fatalf("write providers file: %v", err)
	}
	extra := []string{
		"AHP_ENABLE_BROWSER=1",
		"AHP_ENABLE_MODEL=1",
		"AHP_ENABLE_MODEL_COST=1",
		"AHP_IDENTITY_EXCHANGER_DEPLOYMENTS=" + deployment,
		"AHP_MODEL_PROVIDERS_FILE=" + providersFile,
		"AHP_VERBOSE=1",
	}
	if *llmTaskLimit != "" {
		extra = append(extra, "AHP_EXEC_TASK_TOKEN_LIMIT="+*llmTaskLimit)
	}
	if *llmChildLimit != "" {
		extra = append(extra, "AHP_EXEC_CHILD_TOKEN_LIMIT="+*llmChildLimit)
	}
	port, err := harness.FreePort()
	if err != nil {
		log.Fatal(err)
	}
	p, err := harness.Start(harness.PlatformConfig{
		Binary: c.ver.BinaryPath, Certs: c.cm, GatewayPort: port,
		LogPath:  filepath.Join(c.tmp, "ahp-llm.log"),
		ExtraEnv: extra,
	})
	if err != nil {
		log.Fatalf("start llm instance: %v", err)
	}
	defer p.Stop()
	rep.Instance.GatewayPort = port
	rep.Instance.MeasuredProcess = harness.DescribeProcess(p.PID(), harness.PlatformRole)
	rep.Instance.Tenant, rep.Instance.Deployment = tenant, deployment

	// Operator-provisioned human account (documented provisioning seam), then
	// the product mints every credential from here on.
	username := "profiler"
	subject := "profiler-" + c.runID
	ctx0, cancel0 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel0()
	password, err := seedProfilerUser(ctx0, os.Getenv("AHP_TEST_DATABASE_URL"), tenant, subject, username, []string{"space:access", "model"})
	if err != nil {
		log.Fatalf("seed profiler user: %v", err)
	}
	taskName := "prof-llm-" + c.runID
	token, boundTask, err := scopedTaskToken(ctx0, port, c.cm, cc, username, password, taskName, []string{"model"})
	if err != nil {
		log.Fatalf("scoped task token: %v", err)
	}
	log.Printf("scoped task token acquired (task %s)", boundTask)

	if err := harness.RequireNonEmptyActorChain(token, "model"); err != nil {
		log.Fatalf("model authorization: %v", err)
	}
	// Model calls can run past the token TTL too; re-exchange off the measured path
	// (same mechanism as the state plane). username/password stay in this closure
	// only, for re-login -- never logged or serialized.
	modelProvider := func(ctx context.Context) (string, error) {
		t, _, e := scopedTaskToken(ctx, port, c.cm, cc, username, password, taskName, []string{"model"})
		return t, e
	}

	caller, err := harness.NewCaller(port, c.cm, cc, 64)
	if err != nil {
		log.Fatal(err)
	}
	defer caller.StopRotation()
	if err := caller.UseLiveToken(token, modelProvider, 2*time.Minute); err != nil {
		log.Fatalf("model live-token setup: %v", err)
	}
	if rep.Authorization == nil {
		rep.Authorization = harness.NewAuthorizationLedger()
	}
	rep.Authorization.RecordPlane(harness.PlaneMechanism("model", []string{"model"}))
	rep.Authorization.RecordSession(
		harness.SessionAuthorization{Tag: tag, Plane: "model", Task: boundTask, ExchangerDeployment: deployment, Capabilities: []string{"model"}},
		caller.RotationsFunc(), caller.RotationError, caller.RotationOutsideMeasurementWindow,
	)

	lpool, lperr := lanePool()
	if lperr == nil {
		defer lpool.Close()
	}
	lstream := harness.StartSampling(harness.SamplerConfig{Platform: p, Pool: lpool, RedisAddr: redisAddr(), MinioHealth: minioHealth(), PGContainer: "ahp-handbook-acceptance-pg", MinioContainer: "ahp-handbook-go-minio-1", Interval: time.Duration(*sampleSeconds) * time.Second, SuppressContainerProbes: probesOutsideWindow(), DockerEvery: 2})
	llmLastCut := time.Now()

	var phases []llmPhaseStats
	var phaseAccounts []harness.PhaseAccounting
	runSubmit := func(name string, n, conc int) {
		type genResp struct {
			ID           string `json:"id"`
			Status       string `json:"status"`
			Model        string `json:"model"`
			Error        string `json:"error"`
			Output       string `json:"output"`
			FinishReason string `json:"finish_reason"`
			Usage        struct {
				Prompt     int64 `json:"prompt_tokens"`
				Completion int64 `json:"completion_tokens"`
				Total      int64 `json:"total_tokens"`
			} `json:"usage"`
		}
		rec := harness.NewLatencyRecorder()
		totals := map[string]int64{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
		var outcomes []harness.Outcome
		statusCounts := map[string]int{}
		var modelName *string
		var mu = make(chan struct{}, 1)
		mu <- struct{}{}
		firstErr := ""
		firstCause := ""
		_, wall := harness.RunFixedWork(n, conc, func(i int) harness.Call {
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()
			body := map[string]any{"key": fmt.Sprintf("%s-%s-%d", name, c.runID, i), "max_tokens": *llmMaxTokens,
				"messages": []any{map[string]any{"role": "user", "content": fmt.Sprintf("Profiling probe %d: reply with a 10-word sentence.", i)}}}
			cl, bodyBytes := caller.DoJSON(ctx, "/v1/model/SubmitModel", "SubmitModel", body)
			if !cl.OK {
				<-mu
				if len(firstErr) == 0 && len(bodyBytes) > 0 {
					firstErr = string(bodyBytes)
					if len(firstErr) > 300 {
						firstErr = firstErr[:300]
					}
					log.Printf("llm %s first error body: %s", name, firstErr)
				}
				mu <- struct{}{}
			}
			if cl.OK {
				var g genResp
				if err := json.Unmarshal(bodyBytes, &g); err == nil {
					// An in-flight generation is not an outcome: follow the
					// durable row to its terminal state before retiring the
					// request (DEF-PROF-1). Bounded polling; a row that never
					// resolves is accounted as no_terminal_status, never as a
					// silent success.
					for attempt := 0; g.ID != "" && !harness.IsTerminalGenerationStatus(g.Status); attempt++ {
						if attempt == 600 {
							break
						}
						time.Sleep(200 * time.Millisecond)
						pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
						pcl, pbody := caller.DoJSON(pctx, "/v1/model/GetModel", "GetModel", map[string]any{"id": g.ID})
						pcancel()
						if !pcl.OK {
							continue
						}
						var pg genResp
						if json.Unmarshal(pbody, &pg) == nil && pg.ID != "" {
							g = pg
						}
					}
					<-mu
					totals["prompt_tokens"] += g.Usage.Prompt
					totals["completion_tokens"] += g.Usage.Completion
					totals["total_tokens"] += g.Usage.Total
					if g.Status != "" {
						statusCounts[g.Status]++
					}
					if g.Model != "" {
						m := g.Model
						modelName = &m
					}
					// The durable row carries the CAUSE (DEF-PROF-2): a FAILED
					// generation without its error text is a count, not a
					// finding, and losing it turns every phase into a manual
					// database autopsy.
					outcomes = append(outcomes, outcomeFromDurable(g.Status, g.Error, g.Output))
					if g.Status == "FAILED" && g.Error != "" && firstCause == "" {
						firstCause = harness.CauseToken(g.Error)
						log.Printf("llm %s first durable cause: %s (row %s finish_reason=%q usage prompt=%d completion=%d)",
							name, firstCause, g.ID, g.FinishReason, g.Usage.Prompt, g.Usage.Completion)
					}
					mu <- struct{}{}
				} else {
					<-mu
					outcomes = append(outcomes, harness.Outcome{})
					mu <- struct{}{}
				}
			}
			rec.Record(cl)
			return cl
		})
		s := tagStep(rec.Summarize(name, conc, wall))
		// LLM rungs are load steps too: attach the platform stream slice that
		// ran under them and publish them as report steps, so the quality
		// gates see the ladder and no LLM step can reach disk streamless.
		lcut := time.Now()
		if subs := windowSamples(lstream.Samples(), llmLastCut, lcut); len(subs) > 0 {
			sm := harness.SummarizeSamples(subs)
			sm.TickIntervalMS = harness.I64ptr(sampleTick().Milliseconds())
			s.Samplers = &sm
		}
		llmLastCut = lcut
		rep.Steps = append(rep.Steps, s)
		// Transport errors and terminal generation outcomes are different facts;
		// the live view shows both so a "0 transport errors" line can never be
		// read as "0 failed generations" (DEF-PROF-1).
		log.Printf("rung done %s c=%d ok=%d transport_err=%d p50=%sms p95=%sms p99=%sms rps=%.1f samples=%s terminal=%v load1=%s",
			name, conc, s.Requests-s.Errors, s.Errors, ms(s.P50), ms(s.P95), ms(s.P99), s.ThroughputRPS, samplesLabelOf(s), statusCounts, ms1(harness.Load1()))
		account := harness.AccountPhaseOutcomes(s.Requests, s.Errors, outcomes)
		harness.AppendLiveStep(*outDir, fmt.Sprintf("llm-%s", c.runID), "llm:"+name, s, map[string]any{"terminal_status_counts": statusCounts, "failure_causes": account.FailureCauses, "percentile_note": s.PercentileNote})
		phases = append(phases, llmPhaseStats{Phase: name, Requests: s.Requests, ErrorClasses: s.ErrorClasses, UsageTotals: totals, P50ms: s.P50, P95ms: s.P95, P99ms: s.P99, ModelName: modelName, StatusCounts: statusCounts, PhaseAccounting: account})
		phaseAccounts = append(phaseAccounts, account)
		log.Printf("llm %s: req=%d transport_err=%d completed=%d failed=%d blocked=%d unknown=%d logical_err=%d verdict=%s total_tokens=%d p95=%sms",
			name, s.Requests, account.TransportErrors, account.TerminalCompleted, account.TerminalFailed, account.TerminalBlocked, account.TerminalUnknown, account.LogicalErrors, account.Verdict, totals["total_tokens"], ms(s.P95))
		if account.TransportErrors > 0 || account.TerminalFailed > 0 || account.TerminalBlocked > 0 {
			tailPlatformLog(filepath.Join(c.tmp, "ahp-llm.log"))
		}
	}
	runSubmit("submit_c1", *llmCountC1, 1)
	if *llmCountC5 > 0 {
		runSubmit("submit_c5", *llmCountC5, 5)
	}
	if *llmCountC10 > 0 {
		runSubmit("submit_c10", *llmCountC10, 10)
	}
	if *llmCountC20 > 0 {
		runSubmit("submit_c20", *llmCountC20, 20)
	}
	if *llmCountC30 > 0 {
		runSubmit("submit_c30", *llmCountC30, 30)
	}

	// Streaming TTFB (first delta), concurrency 1.
	// The terminal outcome of a stream request is the DURABLE generation row
	// carried by the terminal frame -- bytes arriving is transport progress,
	// not completion, and "streamed" is deliberately not a status anyone may
	// count (DEF-PROF-1). Concurrency here is 1, so the slice append below is
	// race-free by construction.
	var streamOutcomes []harness.Outcome
	srec, wall := harness.RunFixedWork(*llmStreamN, 1, func(i int) harness.Call {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		body := map[string]any{"key": fmt.Sprintf("stream-%s-%d", c.runID, i), "max_tokens": *llmMaxTokens,
			"messages": []any{map[string]any{"role": "user", "content": fmt.Sprintf("Streaming probe %d: name three primary colors.", i)}}}
		cl, bodyBytes, err := caller.StreamCall(ctx, "/v1/model/StreamModel", "StreamModel", body)
		if err == nil && cl.OK {
			streamOutcomes = append(streamOutcomes, harness.StreamOutcome(bodyBytes))
		}
		return cl
	})
	s := tagStep(srec.Summarize("stream_c1", 1, wall))
	lcut := time.Now()
	if subs := windowSamples(lstream.Samples(), llmLastCut, lcut); len(subs) > 0 {
		sm := harness.SummarizeSamples(subs)
		sm.TickIntervalMS = harness.I64ptr(sampleTick().Milliseconds())
		s.Samplers = &sm
	}
	llmLastCut = lcut
	rep.Steps = append(rep.Steps, s)
	harness.AppendLiveStep(*outDir, fmt.Sprintf("llm-%s", c.runID), "llm:stream_c1", s, map[string]any{"ttfb_p50_ms": s.TTFBP50, "ttfb_p95_ms": s.TTFBP95})
	streamCounts := map[string]int{}
	for _, oc := range streamOutcomes {
		if oc.Status != "" {
			streamCounts[oc.Status]++
		}
	}
	streamAccount := harness.AccountPhaseOutcomes(s.Requests, s.Errors, streamOutcomes)
	// UsageTotals is nil, not {0,0,0}: stream usage was never collected, and a
	// zero map reads as "this model consumed no tokens" (DEF-PROF-1).
	phases = append(phases, llmPhaseStats{Phase: "stream_c1", Requests: s.Requests, ErrorClasses: s.ErrorClasses, UsageTotals: nil, P50ms: s.P50, P95ms: s.P95, P99ms: s.P99, TTFBP50ms: s.TTFBP50, TTFBP95ms: s.TTFBP95, TTFBP99ms: s.TTFBP99, StatusCounts: streamCounts, PhaseAccounting: streamAccount})
	phaseAccounts = append(phaseAccounts, streamAccount)
	log.Printf("llm stream_c1: req=%d transport_err=%d completed=%d failed=%d blocked=%d unknown=%d logical_err=%d verdict=%s",
		s.Requests, streamAccount.TransportErrors, streamAccount.TerminalCompleted, streamAccount.TerminalFailed, streamAccount.TerminalBlocked, streamAccount.TerminalUnknown, streamAccount.LogicalErrors, streamAccount.Verdict)
	lsum := lstream.Stop()
	rep.Samples = lsum
	ls := harness.SummarizeSamples(lsum)
	ls.TickIntervalMS = harness.I64ptr(sampleTick().Milliseconds())
	rep.Summary = &ls
	aggregate := harness.AggregateAccounting(phaseAccounts)
	rep.Model = map[string]any{"phases": phases, "accounting": aggregate,
		"note": "Token counts come from the platform generation API (durable usage). The platform does not expose currency prices; no token figure in this report may be interpreted as money."}
	rep.Verdict, rep.VerdictReasons = aggregate.Verdict, aggregate.VerdictReasons
	rep.Notes = append(rep.Notes,
		"accounting (DEF-PROF-1): transport_errors = failed HTTP calls; terminal_completed/failed/blocked/unknown = durable generation outcomes; logical_errors = requests - terminal_completed with logical_breakdown by reason; a phase or run with terminal_failed/blocked>0 or logical_errors>0 can never report PASS; a stream request is terminal only via the durable row of its terminal frame",
		"real LLM E2E only against the Qwen endpoint configured on this instance (lane env vault); volume bounded by --llm-c1/--llm-c5/--llm-stream",
		"authorization: human account provisioned via identity.SeedBrowserUser (documented operator seam); credentials minted by the product: /u-ahp/v1/auth/login -> /u-ahp/v1/auth/token -> /v1/identity/control/exchange (scoped task token, capabilities=[model])",
		"platform API returns TOKENS (prompt/completion/total) and token budgets; NO currency values are produced or implied",
	)
	log.Printf("llm run verdict: %s %v", aggregate.Verdict, aggregate.VerdictReasons)
}

// ---------------------------------------------------------------------------
// Phase: idle - the paired no-load reference of measurement protocol v1.1
// ---------------------------------------------------------------------------

// phaseIdle is the no-load reference of measurement protocol v1.1: the same
// instance, the same samplers, the same wall-clock duration and ZERO product
// requests. On this shared machine the background spikes on its own (VM
// PSI-cpu reaches 15 with no AHP load at all, host loadavg sits at 4.7-5.9 per
// density-baseline-ops/VM-CONTENTION-20261110T0019Z.md), so a rung can only be
// blamed on the subject when it exceeds a reference measured in the same window
// on the same machine. The artifact says in its own text that it is a reference
// and not a profile: it carries no latency distribution, because no request was
// ever issued - every percentile is null, never 0.
func phaseIdle(c *runCtx, rep *harness.Report) {
	rep.Methodology += "; NO-LOAD REFERENCE (protocol v1.1 rule 3): same instance, same samplers, same duration, zero product requests; this artifact is a background reference, NOT a profile"
	p, _, tenant, dep := c.spawn("idle", 0)
	defer p.Stop()
	rep.Instance.GatewayPort = p.Port
	rep.Instance.MeasuredProcess = harness.DescribeProcess(p.PID(), harness.PlatformRole)
	rep.Instance.Tenant, rep.Instance.Deployment = tenant, dep
	pool, err := lanePool()
	if err == nil {
		defer pool.Close()
	} else {
		// The reference still measures host/process/CPU with no database; the
		// PG columns then report null with the dial error as their reason,
		// which is honest, unlike a zero.
		log.Printf("idle reference: lane DB pool unavailable (%v): PG columns report null with a reason", err)
	}
	scfg := harness.SamplerConfig{Platform: p, Pool: pool, RedisAddr: redisAddr(), MinioHealth: minioHealth(), PGContainer: "ahp-handbook-acceptance-pg", MinioContainer: "ahp-handbook-go-minio-1", Interval: sampleTick(), DockerEvery: 3, SuppressContainerProbes: probesOutsideWindow()}
	stream := harness.StartSampling(scfg)
	warm := 10 * time.Second
	log.Printf("idle reference: warming %s, then sampling %ds with zero requests (probe_policy=%s)", warm, *stepSeconds, *probesPolicy)
	time.Sleep(warm)
	start := time.Now()
	time.Sleep(time.Duration(*stepSeconds) * time.Second)
	samples := stream.Stop()
	span := time.Since(start)
	sm := harness.SummarizeSamples(samples)
	sm.TickIntervalMS = harness.I64ptr(sampleTick().Milliseconds())
	step := harness.StepResult{Workload: "idle_reference", Concurrency: 0, DurationSec: span.Seconds(), Requests: 0, Samplers: &sm,
		Sessions:       "n/a (no request was issued on a no-load reference)",
		LoadPattern:    "reference (no load)",
		PercentileNote: "a no-load reference issues zero requests, so no latency distribution exists: every percentile is null, never 0"}
	rep.Steps = append(rep.Steps, step)
	rep.Summary = &sm
	if sm.SampleCount < *minSamples {
		// The paired verdict is only as strong as the baseline under it. Say so
		// here, where a reader of this artifact cannot miss it, instead of
		// letting a thin reference silently underwrite a rung verdict.
		rep.Notes = append(rep.Notes, fmt.Sprintf("reference density shortfall: %d samples in %.0fs, below --min-samples %d: paired verdicts built on this reference are weak evidence, not a clean baseline", sm.SampleCount, span.Seconds(), *minSamples))
	}
	rep.LoadProfile = map[string]any{"phase": "idle", "seconds": *stepSeconds, "requests": 0,
		"warmup": warm.String(), "probe_policy": *probesPolicy,
		"cache_state": cacheLabelOf(step),
		"purpose":     "paired background reference for verdict_rule_v1_1_paired (protocol v1.1 rule 3)"}
	log.Printf("idle reference done: %d samples in %.1fs (probe_policy=%s), zero requests", sm.SampleCount, span.Seconds(), *probesPolicy)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// ms renders a possibly-unmeasured millisecond figure; nil is an explicit
// "n/a", never a number invented from absence (DEF-PROF-1).
// shortHash keeps 64-hex fingerprints readable in the live log without
// dropping them: a truncated hash is still a fingerprint, an elided one is not.
// probesOutsideWindow reports whether container-level observer probes must be
// kept out of the measured window. The shared Docker VM spikes PSI-cpu to 15
// with no AHP load at all, and docker stats itself is part of that background,
// so a profile taken under v1.1 keeps probes outside and lets the columns say
// "suppressed" instead of lying with zeros.
func probesOutsideWindow() bool {
	switch *probesPolicy {
	case "outside-only":
		return true
	case "", "in-window":
		return false
	default:
		log.Fatalf("-probes %q is not one of in-window|outside-only", *probesPolicy)
		return false
	}
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func shortHash(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

func ms(v *float64) string {
	if v == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.1f", *v)
}

func ratioOrNA(after, base *float64) string {
	if after == nil || base == nil || *base == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2fx", *after / *base)
}

func slopeOrNA(v *float64, prec int) string {
	if v == nil {
		return "not enough samples"
	}
	return strconv.FormatFloat(*v, 'f', prec, 64)
}

func windowSamples(all []harness.Sample, from, to time.Time) []harness.Sample {
	var out []harness.Sample
	for _, s := range all {
		t := parseUTC(s.AtUTC)
		if !t.Before(from) && !t.After(to) {
			out = append(out, s)
		}
	}
	return out
}

func parseUTC(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err == nil {
		return t
	}
	t, err = time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// samplesLabelOf renders the sampler density of a step for the live log line.
// A missing sampler stream is a MISSING observation, so it prints as
// "not_measured". It used to print "-1", which a reader (and any script
// scraping the live lines) takes for a number -- the same class of lie as the
// 0 placeholders cut by DEF-PROF-3, one layer earlier: in the log instead of
// the artifact.
func samplesLabelOf(s harness.StepResult) string {
	if s.Samplers == nil {
		return "not_measured"
	}
	return strconv.Itoa(s.Samplers.SampleCount)
}

func cacheLabelOf(s harness.StepResult) string {
	if s.CacheState == nil {
		return "not_measured"
	}
	return s.CacheState.Label
}

// nb / ni render possibly-unobserved byte and count figures in notes. "0" in a
// note reads as a measurement; these say not_measured instead (DEF-PROF-3).
func nb(v *uint64) string {
	if v == nil {
		return "not_measured"
	}
	return fmt.Sprintf("%d", *v)
}

func ni(v *int) string {
	if v == nil {
		return "not_measured"
	}
	return fmt.Sprintf("%d", *v)
}

func ms1(v *float64) string {
	if v == nil {
		return "not_measured"
	}
	return fmt.Sprintf("%.2f", *v)
}

// workerSession is one end-to-end user identity: its own thread, task grant
// and fence, driven by its own rotating token. per-worker load composes these.
type workerSession struct {
	caller *harness.Caller
	sess   *harness.Session
}

func (c *runCtx) workerSession(rep *harness.Report, p *harness.Platform, cc harness.ClientCertFile, tag string) *workerSession {
	cl, ss := c.session(rep, p, cc, tag)
	return &workerSession{caller: cl, sess: ss}
}

// sampleTick reports the configured sampler interval as a duration; every
// SamplerSummary carries it so sample_count can be checked against the tick
// (density rule) rather than trusted.
func sampleTick() time.Duration { return time.Duration(*sampleSeconds) * time.Second }

func redisAddr() string {
	v := os.Getenv("AHP_REDIS_ADDR")
	if v == "" {
		return ""
	}
	return v
}

func minioHealth() string {
	ep := os.Getenv("AHP_S3_ENDPOINT")
	if ep == "" {
		return ""
	}
	if !strings.HasPrefix(ep, "http") {
		ep = "http://" + ep
	}
	return ep + "/minio/health/live"
}

func envDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// pinnedImages parses AHP_PINNED_IMAGES ("name=sha256:...,name2=sha256:...").
func pinnedImages(raw string) map[string]string {
	out := map[string]string{}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, digest, ok := strings.Cut(item, "=")
		if !ok || name == "" || digest == "" {
			continue
		}
		out[strings.TrimSpace(name)] = strings.TrimSpace(digest)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// loadPatternLabel names the arrival pattern of this run for the artifact.
// Closed loop reports what finished; open loop reports what was offered, and
// only open loop can show a queue or shedded arrivals.
func loadPatternLabel() string {
	if *loadPattern == "open" {
		return fmt.Sprintf("open-loop(offered %.0f rps)", *openRate)
	}
	return "closed-loop"
}

// tagStep stamps the methodology that produced a step onto the step itself, so
// a CSV row can be read without the command line that made it.
func tagStep(s harness.StepResult) harness.StepResult {
	s.Sessions = *sessionsMode
	s.LoadPattern = loadPatternLabel()
	return s
}
