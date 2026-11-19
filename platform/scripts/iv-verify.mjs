#!/usr/bin/env node
// scripts/iv-verify.mjs — deterministic verification kit for integration-validation (iv).
//
// iv's r2 ask (director 20261030T031552Z): one reproducible entry that runs the three
// acceptance legs INDEPENDENTLY of the author (delivery):
//   LEG-A  fail-closed integrity  -- prove the live-journey harness CANNOT be green-washed:
//                                    a forbidden Emil target / mock mode must exit non-zero.
//   LEG-B  journey:live           -- the real whole journey (Qwen->Emil-core->MinIO->PG).
//   LEG-C  pg-restart-recovery    -- G-PG/#74 durable state survives a PostgreSQL restart.
//
// Honesty model (identical to scripts/vm-plane.mjs):
//   PASS    = ran HERE and exited 0 (its own assertions already gate the claim).
//   RED     = capability present and the leg GENUINELY FAILED.
//   PENDING = the capability is absent in THIS environment; deferred to the operator/VM.
//             Never a fabricated GREEN.
//
// Boundary: iv is the INDEPENDENT verifier (author!=verifier). This driver makes the
// legs reproducible; it does NOT pre-run iv's verdict. LEG-B is opt-in so iv owns it.
// LEG-C takes an OPERATOR-SUPPLIED PG restart command (AHP_IV_PG_RESTART_CMD); delivery
// never bounces a shared PostgreSQL container. Secrets/env are never printed.
//
// Usage:
//   node scripts/iv-verify.mjs [--report <path>] [--only A|B|C]
// Env:
//   AHP_IV_RUN_JOURNEY=1        enable LEG-B (iv runs it on the VM with real deps)
//   AHP_IV_PG_RESTART_CMD=<sh>  enable LEG-C: shell command that restarts iv's isolated PG
//                               (e.g. "docker restart iv_ahp_pg"); delivery has no such host.
import { spawn, spawnSync } from 'node:child_process';
import http from 'node:http';
import { existsSync } from 'node:fs';
import { mkdirSync } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildEnv } from './ahp-env.mjs';

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const RED = (s) => String(s).replace(/[A-Za-z0-9._+/=-]{40,}/g, '<REDACTED>');
const argv = process.argv.slice(2);
let reportPath = process.env.AHP_IV_REPORT || join(projectRoot, 'acceptance', 'iv-verify-report.json');
let only = '';
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === '--report') reportPath = argv[++i] || reportPath;
  else if (argv[i] === '--only') only = (argv[++i] || '').toUpperCase();
  else { process.stderr.write(`unknown arg: ${argv[i]}\n`); process.exit(2); }
}
const env = await buildEnv(projectRoot);
const has = (k) => !!(env[k] && String(env[k]).trim());
const run = (cmd, args, opts = {}) => {
  const r = spawnSync(cmd, args, { cwd: projectRoot, env: { ...env, ...(opts.env || {}) }, encoding: 'utf8' });
  const out = RED(((r.stdout || '') + '\n' + (r.stderr || '')).trim());
  return { rc: r.status, out, tail: out.split('\n').filter(Boolean).slice(-6).join(' | ') };
};
const legs = [];
function record(id, proves, status, detail) {
  legs.push({ id, proves, status, detail: RED(detail) });
  const mark = { PASS: 'ok  ', RED: 'FAIL', PENDING: 'PEND' }[status] || '????';
  console.log(`[${mark}] ${id} :: ${proves}`);
}

// ---- LEG-G0: native-Qwen provider identity (criterion #3: MUST gate ANY bin start) ----
// Production openai_compat.go carries NO host allowlist; "native Qwen only / no
// OpenAI/Astra fallback" is enforced ONLY at this external gate. Therefore no checkout
// bin may start (LEG-B drives the model; LEG-C execs bin/ahp) until G0 is GREEN.
// The wrapper (scripts/native-qwen-gate.sh) locates the iv-owned assert and never reads
// or prints any secret value; we mirror vm-plane's env shape (AHP_LLM_* / AHP_QWEN_*).
let g0State = 'PENDING';
function legG0() {
  const gate = join(projectRoot, 'scripts', 'native-qwen-gate.sh');
  const g0base = env.AHP_LLM_BASE_URL || env.AHP_QWEN_BASE_URL || '';
  const g0model = env.AHP_LLM_MODEL || env.AHP_QWEN_MODEL || '';
  const ready = existsSync(gate) && g0base && g0model && has('AHP_NATIVE_QWEN_HOSTS');
  if (!ready) {
    record('G0 native-qwen-gate', 'G0-before-bin: native-Qwen-only (no OpenAI/Astra), secret-free',
      'PENDING', 'needs scripts/native-qwen-gate.sh + (AHP_LLM_BASE_URL|AHP_QWEN_BASE_URL)+model + AHP_NATIVE_QWEN_HOSTS (VM-injected); bin NOT started');
    return;
  }
  const r = run('bash', ['scripts/native-qwen-gate.sh'], { env: { AHP_LLM_BASE_URL: g0base, AHP_LLM_MODEL: g0model } });
  g0State = r.rc === 0 ? 'PASS' : 'RED';
  record('G0 native-qwen-gate', 'G0-before-bin: native-Qwen-only (no OpenAI/Astra), secret-free',
    r.rc === 0 ? 'PASS' : 'RED', `rc=${r.rc} :: ${r.tail}`);
}
const g0BlocksBin = () => g0State !== 'PASS';

// ---- LEG-A: fail-closed integrity (runnable anywhere; non-destructive) ----------------
// Prove the harness refuses to fake a journey. Each forbidden configuration MUST exit
// non-zero with its specific guard message. If any is wrongly accepted -> harness RED.
function legA() {
  const probe = (id, envOverride, needle) => {
    const r = run('node', ['tests/e2e/live-journey.mjs'], { env: envOverride });
    const refused = r.rc !== 0 && r.out.includes(needle);
    const detail = `rc=${r.rc} expect!=0 && contains("${needle}") :: ${r.tail}`;
    if (refused) { record(id, 'harness cannot green-wash a forbidden condition', 'PASS', detail); return true; }
    record(id, 'harness cannot green-wash a forbidden condition', 'RED',
      r.rc === 0 ? `UNSAFE: forbidden condition exited 0 (forgeable) :: ${r.tail}` : detail);
    return false;
  };
  const a1 = probe('A1 anti-stand-in(:19081)', { AHP_EMIL_URL: 'http://127.0.0.1:19081' }, 'STAND-IN');
  const a2 = probe('A2 anti-mock(AHP_E2E_ALLOW_MOCK)', { AHP_E2E_ALLOW_MOCK: '1' }, 'mock mode must not be enabled');
  void a1; void a2;
}

// A3 — the D2 de-gate (ruling b9f9b174) replaced the author's container-NAME gate with a
// structural "valid A2A agent-card" check. That de-gate must NOT become a rubber stamp:
// a 200 response that is NOT a valid A2A agent-card (empty/garbage JSON) MUST still be
// rejected. Runs an in-process HTTP server returning HTTP 200 + `{}` and proves the
// harness fails closed. Non-destructive, no build, no network. The container-NAME check is
// gone BY DESIGN; this proves the SUBSTITUTED gate is real, not weakened to always-pass.
async function probeCard(body) {
  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(body);
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const port = server.address().port;
  const child = spawn('node', ['tests/e2e/live-journey.mjs'], {
    cwd: projectRoot,
    env: { ...env, AHP_TOOL_RUNTIME_URL: `http://127.0.0.1:${port}` },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let out = '';
  child.stdout.on('data', (d) => { out += d; });
  child.stderr.on('data', (d) => { out += d; });
  const rc = await new Promise((r) => child.on('close', (code) => r(code ?? 1)));
  await new Promise((r) => server.close(r));
  return { rc, out };
}

async function legA3() {
  const needle = 'not a valid A2A agent-card';

  // A3 (reject): HTTP 200 + `{}` is NOT a valid A2A agent-card -> harness MUST reject.
  {
    const { rc, out } = await probeCard('{}');
    const detail = `rc=${rc} expect!=0 && contains("${needle}") :: ${RED(out.trim()).split('\n').filter(Boolean).slice(-6).join(' | ')}`;
    if (rc !== 0 && out.includes(needle)) {
      record('A3 anti-empty-agentcard(de-gate fail-closed)', 'de-gate still rejects HTTP 200 non-A2A (not a rubber stamp)', 'PASS', detail);
    } else {
      record('A3 anti-empty-agentcard(de-gate fail-closed)', 'de-gate still rejects HTTP 200 non-A2A (not a rubber stamp)', 'RED',
        rc === 0 ? `UNSAFE: HTTP 200 empty agent-card was ACCEPTED (de-gate = green-wash) :: ${detail}` : detail);
    }
  }

  // A4 (accept): a VALID A2A agent-card whose name is NOT the author's 'core-agent' MUST pass
  // the guard — proving the de-gate truly retired the container-NAME requirement, not
  // silently kept it. The harness should advance PAST the card check (then fail on a later
  // stage like MinIO/model), so the card-rejection needle MUST be ABSENT.
  {
    const card = JSON.stringify({ name: 'ahp-tool-runtime', version: '1.0.0', url: 'http://127.0.0.1', capabilities: {}, skills: [] });
    const { rc, out } = await probeCard(card);
    const detail = `rc=${rc} expect NOT contains("${needle}") (valid non-core-agent card accepted) :: ${RED(out.trim()).split('\n').filter(Boolean).slice(-6).join(' | ')}`;
    if (!out.includes(needle)) {
      record('A4 accepts-valid-non-core-agent-card', 'de-gate retired the author container-NAME requirement (valid A2A card of any name passes the guard)', 'PASS', detail);
    } else {
      record('A4 accepts-valid-non-core-agent-card', 'de-gate retired the author container-NAME requirement (valid A2A card of any name passes the guard)', 'RED',
        `UNSAFE: valid non-'core-agent' A2A card was REJECTED (name gate still enforced) :: ${detail}`);
    }
  }
}

// ---- LEG-B: journey:live (iv-independent; opt-in) --------------------------------------
function legB() {
  if (env.AHP_IV_RUN_JOURNEY !== '1') {
    record('B journey:live', 'real Qwen->Emil-core tool-call->MinIO sha PUT==GET->PG', 'PENDING',
      'iv-independent leg: set AHP_IV_RUN_JOURNEY=1 on the VM with real Emil(:18000)+MinIO+PG; delivery does not pre-run iv\'s verdict');
    return;
  }
  if (g0BlocksBin()) {
    record('B journey:live', 'real Qwen->Emil-core tool-call->MinIO sha PUT==GET->PG', 'PENDING',
      `G0 not GREEN (${g0State}); criterion #3 refuses to start the journey (model) before native-Qwen-only is proven`);
    return;
  }
  const r = run('node', ['tests/e2e/live-journey.mjs']);
  record('B journey:live', 'real Qwen->Emil-core tool-call->MinIO sha PUT==GET->PG',
    r.rc === 0 ? 'PASS' : 'RED', `rc=${r.rc} :: ${r.tail}`);
}

// ---- LEG-C: pg-restart-recovery (G-PG/#74) ---------------------------------------------
// Durable State must survive a PostgreSQL restart. Reuses the verified spawnService /
// mState / pollReady helpers (same code paths the journey proved). Restart is driven by an
// OPERATOR command so delivery never bounces a shared container.
async function legC() {
  const restartCmd = env.AHP_IV_PG_RESTART_CMD || '';
  const ahpBin = join(projectRoot, 'bin', 'ahp');
  if (!restartCmd) {
    record('C pg-restart-recovery(#74)', 'durable PG State survives a PostgreSQL restart', 'PENDING',
      'needs AHP_IV_PG_RESTART_CMD=<iv isolated-PG restart cmd> + bin/ahp; delivery never bounces a shared PG');
    return;
  }
  if (!existsSync(ahpBin)) {
    record('C pg-restart-recovery(#74)', 'durable PG State survives a PostgreSQL restart', 'PENDING',
      'bin/ahp absent (heavy build gate); rebuild on the VM then rerun with AHP_IV_PG_RESTART_CMD');
    return;
  }
  if (g0BlocksBin()) {
    record('C pg-restart-recovery(#74)', 'durable PG State survives a PostgreSQL restart', 'PENDING',
      `G0 not GREEN (${g0State}); criterion #3 refuses to start bin/ahp before native-Qwen-only is proven`);
    return;
  }
  try {
    const h = await import('../tests/helpers/ahp-client.mjs');
    const nonce = 'AHP-PG74-' + Date.now().toString(36);
    const port = 19600 + Math.floor(Math.random() * 400);
    const svc = await h.spawnService({ role: 'all', port });
    try {
      await h.pollReady(port, { tls: true, timeoutMs: 30000 });
      const tenant = 'ivpg', dep = h.uniqueDeployment('pg74');
      const cdir = h.genClient(tenant, dep);
      const tok = h.tokengen({ tenant, deployment: dep, audience: 'ahp', kind: 'task' });
      const thr = await h.mState('CreateThread', { context_id: nonce, key: 'pg74/thread' }, { port, certDir: cdir, token: tok });
      const threadID = (thr.json && thr.json.id) || '';
      if (!(thr.status === 200 && threadID)) throw new Error(`CreateThread status=${thr.status}`);
      const runRes = await h.mState('CreateRun', { thread_id: threadID, key: 'pg74/run', payload: { nonce, sentinel: nonce } }, { port, certDir: cdir, token: tok });
      const runID = (runRes.json && runRes.json.id) || '';
      if (!(runRes.status === 200 && runID)) throw new Error(`CreateRun status=${runRes.status}`);
      // Drop the service (its PG pool is stale after restart), then bounce PG.
      // svc2 binds a DISTINCT port: the ahp role is a stateless PostgreSQL client, so only
      // the PG instance must persist across the restart, not the HTTP listener. Reusing the
      // same port after an async SIGTERM risks EADDRINUSE while the old socket drains.
      const port2 = 19600 + (((port - 19600) + 200) % 400);
      svc.kill();
      const rr = run('sh', ['-c', restartCmd]);
      if (rr.rc !== 0) throw new Error(`pg restart cmd rc=${rr.rc} :: ${rr.tail}`);
      // Fresh service (new pool, new port) must read the SAME durable run back from PG.
      const svc2 = await h.spawnService({ role: 'all', port: port2 });
      try {
        await h.pollReady(port2, { tls: true, timeoutMs: 30000 });
        const back = await h.mState('GetRun', { run_id: runID }, { port: port2, certDir: cdir, token: tok });
        const blob = JSON.stringify(back.json || {});
        const survived = back.status === 200 && blob.includes(nonce);
        record('C pg-restart-recovery(#74)', 'durable PG State survives a PostgreSQL restart',
          survived ? 'PASS' : 'RED', `GetRun after restart status=${back.status} persisted=${survived}`);
      } finally { svc2.kill(); }
    } finally { try { svc.kill(); } catch { /* gone */ } }
  } catch (e) {
    record('C pg-restart-recovery(#74)', 'durable PG State survives a PostgreSQL restart', 'RED', RED(String(e && e.message || e)));
  }
}

const want = (x) => !only || only === x;
if (want('A')) { legA(); await legA3(); }
if (want('G0')) legG0();
if (want('B')) legB();
if (want('C')) await legC();

// ---- verdict ---------------------------------------------------------------------------
const fails = legs.filter((l) => l.status === 'RED');
const pend = legs.filter((l) => l.status === 'PENDING');
const pass = legs.filter((l) => l.status === 'PASS');
let verdict = 'GREEN';
if (fails.length) verdict = 'RED';
else if (pend.length) verdict = 'PARTIAL';
const summary = {
  ts: new Date().toISOString(), verdict,
  counts: { pass: pass.length, red: fails.length, pending: pend.length, total: legs.length },
  legs,
};
console.log(`\nIV_VERIFY=${verdict}  (pass=${pass.length} red=${fails.length} pending=${pend.length})`);
if (verdict !== 'GREEN') {
  console.log('NOTE: PENDING legs are iv/VM-plane responsibilities (independent journey,');
  console.log('      operator-driven isolated-PG restart). host != target runtime; never fabricated.');
}
try { mkdirSync(dirname(reportPath), { recursive: true }); } catch { /* exists */ }
await writeFile(reportPath, JSON.stringify(summary, null, 2) + '\n', 'utf8');
console.log(`report: ${reportPath}`);
process.exit(verdict === 'RED' ? 1 : 0);
