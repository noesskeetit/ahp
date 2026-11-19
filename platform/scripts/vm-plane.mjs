#!/usr/bin/env node
// scripts/vm-plane.mjs — VM-plane / L4 acceptance driver for the composed checkout.
//
// WHY THIS EXISTS (delivery acceptance #2/#3/#4, director 2026-10-30):
//   The composed journey (tests/e2e/live-journey.mjs) is verified on the Mac
//   host. But whole-platform acceptance additionally requires L4 gates that a
//   disk/egress-gated Mac MUST NOT fake:
//     * G0 native-Qwen provider-identity assertion (run BEFORE any bin starts)
//     * whole-tree `go test -race`            (swap headroom on host ~1.3 GiB)
//     * target-runtime load p95               (local != target runtime)
//     * compose app-image build               (docker build of the real role bin)
//     * OTLP collector target                 (offline exporter is component-only)
//     * MinIO scoped-IAM                      (root creds are NOT an IAM proof)
//     * Tier0/1/2A/2B server-dry-run + L4 rollout
//   This driver gives the VM-plane / integration-validation ONE reproducible
//   command that runs whichever gates the current environment can honestly
//   prove, marks the rest PENDING, and NEVER reports a fabricated GREEN.
//
// EVIDENCE DISCIPLINE (this file is the contract):
//   PASS    = the command ran in THIS environment and exited 0; its own
//             sub-tests are already fail-closed and secret-free.
//   RED     = the capability was present and the gate genuinely FAILED.
//   PENDING = the capability is absent HERE (Mac) so the gate is deferred to
//             the VM-plane; never counted as a pass. The verdict is then at
//             best PARTIAL, never GREEN.
//   AVM/VM-only heavy gates are gated on AHP_VM_PLANE_HEAVY=1 so a tight host
//   never trips the disk/OOM gate. Target-runtime & IAM & collector additionally
//   require their own explicit operator-provided config; absent => PENDING.
//
// Secret hygiene: env comes from buildEnv (git-ignored .env, isolated DB);
// every captured stderr line is run through the long-token redactor before it
// is echoed or stored. No credential, DSN, or signed-URL value is ever printed.
//
// Usage:
//   node scripts/vm-plane.mjs [--report <path>] [--kubectl-context <ctx>]
//   Env: AHP_VM_PLANE_HEAVY=1  enable -race / compose-image heavy gates
//        AHP_TARGET_RUNTIME=1 + --kubectl-context  enable tier server-dry-run
//        AHP_OTLP_ENDPOINT=<collector>             enable OTLP target gate
//        AHP_S3_ENDPOINT + AHP_S3_IAM_USER/PASSWORD + AHP_S3_BUCKET  enable IAM
//        AHP_LOAD_TARGET=1                         run target-runtime load legs
import { spawn, spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildEnv } from './ahp-env.mjs';

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const RED = (s) => String(s).replace(/[A-Za-z0-9._+/=-]{40,}/g, '<REDACTED>');

const argv = process.argv.slice(2);
let reportPath = process.env.AHP_VM_PLANE_REPORT || join(projectRoot, 'acceptance', 'vm-plane-report.json');
let kctlCtx = '';
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === '--report') reportPath = argv[++i] || reportPath;
  else if (argv[i] === '--kubectl-context') kctlCtx = argv[++i] || '';
  else { process.stderr.write(`unknown arg: ${argv[i]}\n`); process.exit(2); }
}

const env = await buildEnv(projectRoot);
const heavy = env.AHP_VM_PLANE_HEAVY === '1' || env.AHP_VM_PLANE_HEAVY === 'true';
const targetRuntime = env.AHP_TARGET_RUNTIME === '1' || env.AHP_TARGET_RUNTIME === 'true';
const has = (k) => !!(env[k] && String(env[k]).trim());
const have = (bin) => spawnSync('sh', ['-c', `command -v ${bin}`], { env, encoding: 'utf8' }).status === 0;

// Run a child command; redact + tail its combined output; return {status, detail}.
function run(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { cwd: projectRoot, env, encoding: 'utf8', ...opts });
  const out = RED(((r.stdout || '') + '\n' + (r.stderr || '')).trim());
  const tail = out.split('\n').filter(Boolean).slice(-8).join(' | ');
  return { rc: r.status, tail, err: r.error ? String(r.error.message) : '' };
}

const gates = [];
function record(id, layer, proves, status, detail) {
  gates.push({ id, layer, proves, status, detail: RED(detail) });
  const mark = { PASS: 'ok  ', RED: 'FAIL', PENDING: 'PEND', SKIP: 'skip' }[status] || '????';
  console.log(`  [${mark}] ${id} (${layer}) :: ${proves}`);
  console.log(`         ${status}${detail ? '  ' + detail : ''}`);
}

// ---- G0 native-Qwen provider identity (MUST run before any checkout bin) ----
// CRITERION #3 fidelity: production openai_compat.go carries NO host allowlist, so
// "native Qwen only / no OpenAI-Astra fallback" is enforced ONLY at this external gate.
// Therefore NO checkout bin that drives the model may start until G0 is GREEN — the same
// g0BlocksBin invariant iv-verify enforces (legB/legC). g0State is module-scope so the
// downstream bin-starting legs (load/data/otlp) can refuse to start a bin on RED/PENDING.
let g0State = 'PENDING';
{
  const assert = env.AHP_NATIVE_QWEN_ASSERT
    || '/Users/aogabbasov/AI Agents/ahp/company-brain/acceptance/integration-validation/scripts/native-qwen-assert.sh';
  // The composed serve maps Qwen creds into the LLM slot at serve time; a VM may
  // export either AHP_LLM_* (canonical) or AHP_QWEN_* (runtime). Accept both so the
  // gate is faithful, feeding the assert's canonical AHP_LLM_* contract.
  const g0base = env.AHP_LLM_BASE_URL || env.AHP_QWEN_BASE_URL || '';
  const g0model = env.AHP_LLM_MODEL || env.AHP_QWEN_MODEL || '';
  const g0ready = existsSync(assert) && g0base && g0model && has('AHP_NATIVE_QWEN_HOSTS');
  if (!g0ready) {
    record('G0-native-qwen', 'G0',
      'runtime native-Qwen-only (no OpenAI/Astra fallback), secret-free classifier',
      'PENDING', 'needs assert script + (AHP_LLM_BASE_URL|AHP_QWEN_BASE_URL)+model + AHP_NATIVE_QWEN_HOSTS (injected at VM-plane)');
  } else {
    const r = run('bash', [assert], { env: { ...env, AHP_LLM_BASE_URL: g0base, AHP_LLM_MODEL: g0model } });
    g0State = r.rc === 0 ? 'PASS' : 'RED';
    record('G0-native-qwen', 'G0',
      'runtime native-Qwen-only (no OpenAI/Astra fallback), secret-free classifier',
      r.rc === 0 ? 'PASS' : 'RED', `rc=${r.rc} :: ${r.tail}`);
  }
}
// Any leg that execs a composed checkout bin (model-serving) MUST be gated on G0 GREEN.
const g0BlocksBin = () => g0State !== 'PASS';
const g0BlockNote = () => `G0 not GREEN (${g0State}); criterion #3 forbids starting a checkout bin before native-Qwen-only (no OpenAI/Astra) is proven`;

// ---- Tier manifests: STATIC (always, non-mutating) ----
{
  const r = run('bash', ['scripts/verify-tier-target.sh']);
  record('tier-static', 'L1/L2', 'Tier0/1/2A/2B manifests well-formed + promised kinds/names',
    r.rc === 0 ? 'PASS' : 'RED', `rc=${r.rc} :: ${r.tail}`);
}

// ---- Tier manifests: SERVER-DRY-RUN (L3 admission) + L4 rollout PENDING ----
{
  if (!targetRuntime || !kctlCtx) {
    record('tier-server-dryrun', 'L3', 'admission against real API server (NOT L4 rollout)',
      'PENDING', 'needs AHP_TARGET_RUNTIME=1 --kubectl-context <ctx>; L4 rollout always separate');
  } else if (!have('kubectl')) {
    record('tier-server-dryrun', 'L3', 'admission against real API server (NOT L4 rollout)',
      'PENDING', 'kubectl absent in this environment');
  } else {
    const r = run('bash', ['scripts/verify-tier-target.sh', '--target', '--context', kctlCtx]);
    record('tier-server-dryrun', 'L3', 'admission against real API server (NOT L4 rollout)',
      r.rc === 0 ? 'PASS' : 'RED',
      `rc=${r.rc} ctx-given; STILL not L4 rollout proof :: ${r.tail}`);
  }
}

// ---- L4 whole-tree -race (heavy: host swap gate) ----
if (heavy) {
  const r = run('node', ['scripts/gotest.mjs', '-race', '-p=1', '-count=1']);
  record('go-race-whole-tree', 'L4', 'data-race free across ./... under -race on acceptance host',
    r.rc === 0 ? 'PASS' : 'RED', `rc=${r.rc} :: ${r.tail}`);
} else {
  record('go-race-whole-tree', 'L4', 'data-race free across ./... under -race on acceptance host',
    'PENDING', 'heavy gate: host swap/disk gate -> set AHP_VM_PLANE_HEAVY=1 on the VM');
}

// ---- compose app image build (heavy) ----
if (heavy && have('docker')) {
  const r = run('npm', ['run', 'app:build']);
  record('compose-image-build', 'L4', 'docker build of the real all-role ahp image',
    r.rc === 0 ? 'PASS' : 'RED', `rc=${r.rc} :: ${r.tail}`);
} else {
  record('compose-image-build', 'L4', 'docker build of the real all-role ahp image',
    'PENDING', 'needs AHP_VM_PLANE_HEAVY=1 + docker (VM-plane)');
}

// ---- OTLP collector target (component exporter already covered by check) ----
if (g0BlocksBin()) {
  record('otlp-collector-target', 'L4', 'spans delivered to a REAL OTLP collector endpoint',
    'PENDING', g0BlockNote());
} else if (has('AHP_OTLP_ENDPOINT')) {
  const r = run('node', ['tests/e2e/otel-trace.mjs']);
  record('otlp-collector-target', 'L4', 'spans delivered to a REAL OTLP collector endpoint',
    r.rc === 0 ? 'PASS' : 'RED', `rc=${r.rc} :: ${r.tail}`);
} else {
  record('otlp-collector-target', 'L4', 'spans delivered to a REAL OTLP collector endpoint',
    'PENDING', 'needs AHP_OTLP_ENDPOINT=<collector> (offline exporter = component only)');
}

// ---- MinIO scoped access / tenant-scope (root creds alone are NOT an IAM proof) ----
// The scoped-access proof belongs to the data plane (RFC-002: MinIO Store = data
// owner). It is ALREADY encoded in tests/e2e/data-plane.mjs: presigned PUT of real
// bytes -> Seal(version) -> Download sha256 match -> foreign-TENANT deny (403/404)
// -> Delete -> 404, against a REAL MinIO + isolated PG. delivery does not re-implement
// the data plane's signing/scoping; it runs the data owner's proven primitive. Needs
// the data plane built (heavy) + MinIO reachable => VM-plane.
if (heavy && have('node') && (env.AHP_ENABLE_DATA === '1' || env.AHP_ENABLE_DATA === 'true') && has('AHP_MINIO_ROOT_USER')) {
  if (g0BlocksBin()) {
    record('data-plane-tenant-scope', 'L4', 'real presigned bytes + foreign-tenant deny (RFC-002 data-owner primitive)',
      'PENDING', g0BlockNote());
  } else {
    const r = run('node', ['tests/e2e/data-plane.mjs']);
    record('data-plane-tenant-scope', 'L4', 'real presigned bytes + foreign-tenant deny (RFC-002 data-owner primitive)',
      r.rc === 0 ? 'PASS' : 'RED', `rc=${r.rc} :: ${r.tail}`);
  }
} else {
  record('data-plane-tenant-scope', 'L4', 'real presigned bytes + foreign-tenant deny (RFC-002 data-owner primitive)',
    'PENDING', 'delegated to data-plane: needs AHP_VM_PLANE_HEAVY=1 + AHP_ENABLE_DATA=1 + MinIO (data-owner primitive, not re-implemented)');
}

// ---- target-runtime load p95 (local != target) ----
if (env.AHP_LOAD_TARGET === '1') {
  if (g0BlocksBin()) {
    record('load-p95-target', 'L4', 'control-API p95 on TARGET runtime (PDF §11 SLO)',
      'PENDING', g0BlockNote());
  } else {
    const r = run('node', ['tests/load/latency.mjs']);
    record('load-p95-target', 'L4', 'control-API p95 on TARGET runtime (PDF §11 SLO)',
      r.rc === 0 ? 'PASS' : 'RED', `rc=${r.rc} target-leg :: ${r.tail}`);
  }
} else {
  record('load-p95-target', 'L4', 'control-API p95 on TARGET runtime (PDF §11 SLO)',
    'PENDING', 'local latency is NOT target proof; set AHP_LOAD_TARGET=1 on the VM-plane runtime');
}

// ---- verdict ----
const fails = gates.filter((g) => g.status === 'RED');
const pend = gates.filter((g) => g.status === 'PENDING');
const passed = gates.filter((g) => g.status === 'PASS');
let verdict = 'GREEN';
if (fails.length) verdict = 'RED';
else if (pend.length) verdict = 'PARTIAL';
const summary = {
  ts: new Date().toISOString(), verdict,
  counts: { pass: passed.length, red: fails.length, pending: pend.length, total: gates.length },
  gates,
};
console.log(`\nVM_PLANE=${verdict}  (pass=${passed.length} red=${fails.length} pending=${pend.length})`);
if (verdict !== 'GREEN') {
  console.log('NOTE: PENDING gates are external VM-plane requirements, not failures;');
  console.log('      source != deployment, host != target runtime. Never fabricated.');
}
await writeFile(reportPath, JSON.stringify(summary, null, 2) + '\n', 'utf8');
console.log(`report: ${reportPath}`);
process.exit(verdict === 'RED' ? 1 : 0);
