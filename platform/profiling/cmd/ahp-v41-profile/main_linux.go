package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var productRevision = "557090861c48f8b18f1881d907d85f52d8a023a0"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := mainRun(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "AHP_PROFILE_ERROR", err)
		os.Exit(1)
	}
}

func mainRun(ctx context.Context, args []string) error {
	// ProcessDriver drops parent arguments and supplies only the socket.
	if os.Getenv("AHP_INJECTOR_SOCKET") != "" {
		return runWorker(ctx)
	}
	if len(args) == 0 {
		return errors.New("select run, worker, gateway or cgexec")
	}
	switch args[0] {
	case "worker":
		return runWorker(ctx)
	case "gateway":
		return runGateway(ctx)
	case "cgexec":
		return runCgroupExec(args[1:])
	case "run":
		return runProfile(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

type sessionConfig struct {
	phase, out   string
	quick        time.Duration
	rate         float64
	seed         int
	maxLanes     int
	fcLanes      int
	enforce      bool
	injectFaults bool
}

func runProfile(ctx context.Context, args []string) (err error) {
	flagset := flag.NewFlagSet("run", flag.ContinueOnError)
	cfg := sessionConfig{}
	flagset.StringVar(&cfg.phase, "phase", "smoke", "smoke, p0, p1, p2, p3, p4, fc1, fc2, all-process or all-fc")
	flagset.StringVar(&cfg.out, "out", envDefault("AHP_PROFILE_OUT", "./evidence"), "private evidence directory")
	var mcpTransport string
	flagset.StringVar(&mcpTransport, "mcp-transport", envDefault(profileMCPTransportEnv, "per-call"), "per-call baseline or pooled A/B candidate")
	flagset.DurationVar(&cfg.quick, "quick", 0, "shortened diagnostic step duration; zero runs the protocol")
	flagset.Float64Var(&cfg.rate, "rate", 0, "healthy P2 throughput used to schedule a standalone P4")
	flagset.IntVar(&cfg.seed, "seed", 10000, "sleeping State sessions; 25 events and one checkpoint each")
	flagset.IntVar(&cfg.maxLanes, "max-lanes", 192, "upper bound on concurrency ladder")
	flagset.IntVar(&cfg.fcLanes, "fc-lanes", 0, "run only this FC2 density step (1, 4, 8, or 16)")
	flagset.BoolVar(&cfg.injectFaults, "inject-faults", true, "inject worker failures during full P4 or FC2")
	flagset.BoolVar(&cfg.enforce, "enforce-budget", true, "require a kernel-enforced 4 vCPU / 4 GiB SUT")
	if err := flagset.Parse(args); err != nil {
		return err
	}
	if mcpTransport != "per-call" && mcpTransport != "pooled" {
		return errors.New("invalid MCP transport mode")
	}
	if err := os.Setenv(profileMCPTransportEnv, mcpTransport); err != nil {
		return err
	}
	if cfg.quick < 0 || cfg.seed < 0 || cfg.seed > 10000 || cfg.maxLanes < 1 || cfg.maxLanes > 192 || cfg.rate < 0 {
		return errors.New("invalid profiling argument")
	}
	switch cfg.phase {
	case "smoke", "p0", "p1", "p2", "p3", "p4", "fc1", "fc2", "all-process", "all-fc":
	default:
		return fmt.Errorf("unknown profile phase %q", cfg.phase)
	}
	if cfg.phase == "p4" && cfg.rate == 0 {
		return errors.New("standalone P4 requires the completed/s rate from a healthy P2 step")
	}
	if cfg.fcLanes != 0 && (cfg.phase != "fc2" || cfg.fcLanes > cfg.maxLanes ||
		(cfg.fcLanes != 1 && cfg.fcLanes != 4 && cfg.fcLanes != 8 && cfg.fcLanes != 16)) {
		return errors.New("--fc-lanes requires an FC2 density step within --max-lanes")
	}
	faults := cfg.injectFaults && cfg.quick == 0 &&
		(cfg.phase == "p4" || cfg.phase == "all-process" || cfg.phase == "fc2" || cfg.phase == "all-fc")

	executable, err := os.Executable()
	if err != nil {
		return err
	}
	cfg.out, err = filepath.Abs(cfg.out)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.out, 0700); err != nil {
		return err
	}
	sc := standConfig{
		BinDir: envDefault("AHP_PROFILE_BIN_DIR", filepath.Dir(executable)), OutputDir: cfg.out,
		PostgresDSN:    os.Getenv("AHP_PROFILE_PG_DSN"),
		MinIOEndpoint:  os.Getenv("AHP_PROFILE_MINIO_ENDPOINT"),
		MinIOAccessKey: os.Getenv("AHP_PROFILE_MINIO_ACCESS_KEY"), MinIOSecretKey: os.Getenv("AHP_PROFILE_MINIO_SECRET_KEY"),
		SUTCgroup: os.Getenv("AHP_PROFILE_SUT_CGROUP"), GuestCgroup: os.Getenv("AHP_PROFILE_WORKER_CGROUP"),
		Driver: "process", WorkerPath: executable, Faults: faults || cfg.phase == "p4" || cfg.phase == "all-process",
		FirecrackerPath: os.Getenv("AHP_PROFILE_FC_BINARY"), KernelPath: os.Getenv("AHP_PROFILE_FC_KERNEL"),
		RootFSPath: os.Getenv("AHP_PROFILE_FC_ROOTFS"), FirecrackerCgroup: os.Getenv("AHP_PROFILE_FC_CGROUP"),
	}
	fc := strings.HasPrefix(cfg.phase, "fc") || cfg.phase == "all-fc"
	if fc {
		sc.Driver = "firecracker"
		if fi, e := os.Stat("/dev/kvm"); e != nil || fi.Mode()&os.ModeDevice == 0 {
			return errors.New("Firecracker profiles require /dev/kvm")
		}
	}
	manifest := map[string]any{
		"product_sha": productRevision, "phase": cfg.phase, "go_version": runtime.Version(),
		"goarch": runtime.GOARCH, "goos": runtime.GOOS, "visible_cpus": runtime.NumCPU(),
		"started_at": time.Now().UTC(), "step_override": cfg.quick.String(), "dormant_sessions": cfg.seed, "max_lanes": cfg.maxLanes,
		"fc_lanes": cfg.fcLanes, "inject_faults": faults, "mcp_transport": mcpTransport,
		"diagnostic": cfg.quick > 0 || cfg.seed < 10000 || !cfg.enforce || cfg.phase == "smoke",
		"driver":     sc.Driver, "sut_cgroup": describeBudget(sc.SUTCgroup), "worker_cgroup": describeBudget(sc.GuestCgroup),
		"storage":                              os.Getenv("AHP_PROFILE_STORAGE"),
		"process_worker_soft_heap_limit_bytes": processWorkerMemoryLimit,
		"process_worker_gomaxprocs":            processWorkerMaxProcs,
	}
	if cfg.enforce {
		if e := checkSUT(sc.SUTCgroup); e != nil {
			return e
		}
	}
	if e := writeJSONFile(filepath.Join(cfg.out, "manifest.json"), manifest); e != nil {
		return e
	}
	s, err := startStand(ctx, sc)
	if err != nil {
		return fmt.Errorf("start live profiling owners: %w", err)
	}
	defer func() { err = errors.Join(err, s.Close()) }()
	defer func() {
		effects := s.Effects.snapshot()
		_ = writeJSONFile(filepath.Join(cfg.out, "mcp.json"), effects)
		if effects.Duplicates > 0 || effects.Unauthorized > 0 {
			err = errors.Join(err, errors.New("MCP duplicate or unauthorized request"))
		}
		_ = writeJSONFile(filepath.Join(cfg.out, "cleanup.json"), resourceCleanup(s))
	}()
	if _, err := s.PG.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"); err != nil {
		fmt.Fprintln(os.Stderr, "AHP_PROFILE_NOTE pg_stat_statements unavailable")
	}
	if cfg.seed > 0 && cfg.phase != "smoke" {
		if err := seedSessions(ctx, s, cfg.seed); err != nil {
			return fmt.Errorf("preseed PostgreSQL: %w", err)
		}
	}

	switch cfg.phase {
	case "smoke":
		summary, err := runClosedStage(ctx, s, cfg, closedStep{
			name: "smoke", work: profile{tools: 2, mcpMS: 1, warm: true, largeEvery: 2}, lanes: 2, countPerLane: 4,
		}, false)
		if err != nil {
			return err
		}
		if summary.Completed != 8 || summary.Warm != 8 || summary.Effects != 16 || summary.Large != 4 {
			return fmt.Errorf("smoke expected eight successful warm turns, sixteen MCP effects and four large results, observed %+v", summary)
		}
		return nil
	case "fc1", "fc2", "all-fc":
		return runFirecracker(ctx, s, cfg)
	default:
		return runProcessProfiles(ctx, s, cfg, sc.SUTCgroup)
	}
}

func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func describeBudget(path string) map[string]string {
	out := make(map[string]string)
	if path == "" {
		return out
	}
	for _, name := range []string{"cpu.max", "cpuset.cpus.effective", "memory.max", "memory.swap.max", "pids.max"} {
		if b, err := os.ReadFile(filepath.Join(path, name)); err == nil {
			out[name] = strings.TrimSpace(string(b))
		}
	}
	return out
}

func checkSUT(path string) error {
	budget := describeBudget(path)
	cpu := strings.Fields(budget["cpu.max"])
	if len(cpu) != 2 {
		return errors.New("create a Linux cgroup v2 with an explicit SUT CPU quota")
	}
	q, e1 := strconv.ParseInt(cpu[0], 10, 64)
	period, e2 := strconv.ParseInt(cpu[1], 10, 64)
	mem, e3 := strconv.ParseUint(budget["memory.max"], 10, 64)
	if e1 != nil || e2 != nil || e3 != nil || period <= 0 || q != 4*period || mem != 4<<30 || budget["memory.swap.max"] != "0" {
		return fmt.Errorf("SUT budget must be 4 vCPU / 4 GiB with zero swap: %v", budget)
	}
	return nil
}
