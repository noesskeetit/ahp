#!/usr/bin/env node
// tests/e2e/whole-system.mjs — Whole-system process-composition gate.
//
// Proves that, in one process group, the State service role (bin/ahp role=state,
// block-isolated DB) and the Agents Space service (bin/as-runner, owner space
// block, imported unmodified) BOOT, become ready/healthy, that Space is actually
// WIRED to the live State endpoint, that the Space HTTP surface routes
// /v1/space/{op}, and that State emits a per-role OpenTelemetry span — i.e. the
// integrated snapshot is runnable end-to-end, not merely compilable.
//
// Scope discipline (proof, not claims):
//   * This gate proves composition + readiness + wiring + routing + OTel. It does
//     NOT assert the authenticated Space->State data round-trip; that wire/auth
//     proof is the Go suite `go test -tags state_integration ./tests/space/...`
//     (npm run test:space-state), which drives the real stateapi handler
//     (stateapi.New(store, verifier, authorize, issue)) in-process against the
//     same isolated DB. The two gates are complementary, not duplicative.
//   * Imported owner code (as-runner / space) is run, never edited here.
//   * Secrets are never printed; every DB-facing child uses the isolated DB only.

import { spawnService, pollReady, httpGet, assert, ensureAhp, PROJECT_ROOT } from '../helpers/ahp-client.mjs';
import { spawn } from 'node:child_process';
import http from 'node:http';
import { execFileSync } from 'node:child_process';
import { join } from 'node:path';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const STATE_PORT = 29741; // distinct from separated-roles (29731-3) / smoke / load
const SPACE_PORT = 29742;

let failures = 0;
const record = (ok, msg) => {
  if (ok) console.log(`PASS: ${msg}`);
  else { console.error(`FAIL: ${msg}`); failures++; }
};

function ensureAsRunner() {
  execFileSync('go', ['build', '-o', 'bin/as-runner', './cmd/as-runner'], {
    cwd: PROJECT_ROOT, stdio: 'inherit', env: process.env,
  });
  return join(PROJECT_ROOT, 'bin', 'as-runner');
}

function spawnAsRunner({ listen, stateURL }) {
  const bin = join(PROJECT_ROOT, 'bin', 'as-runner');
  // as-runner's artifact store fail-closes on a PARTIALLY configured Data env
  // (cmd/as-runner/wiring.go buildArtifactSink): it requires DSN + endpoint + creds
  // + AS_ARTIFACT_BUCKET together, or none at all. This composition gate proves
  // boot/wiring/routing/OTel, NOT the artifact round-trip (that is the Go suite
  // `test:space-state`). So normalize the inherited env: when the artifact backing
  // (S3 creds + a PG DSN) is present, add a valid S3-legal bucket (no '_') to make
  // the config COMPLETE; otherwise strip every artifact var so the tool is cleanly
  // omitted and as-runner boots. This prevents a partial-env composed run from
  // failing A2 (space /healthz) and A4 (route) purely for a missing bucket.
  const env = { ...process.env, AS_LISTEN: listen, AS_STATE_URL: stateURL, AS_WORKER_ID: `whole-system-${process.pid}` };
  const hasS3 = env.AHP_S3_ENDPOINT && env.AHP_MINIO_ROOT_USER && env.AHP_MINIO_ROOT_PASSWORD;
  const hasDsn = env.AS_DATA_DATABASE_URL || env.DATABASE_URL;
  if (hasS3 && hasDsn) {
    env.AS_ARTIFACT_BUCKET = env.AS_ARTIFACT_BUCKET || 'ahp-wholesys-live'; // valid S3 name, no '_'
  } else {
    for (const k of ['AS_ARTIFACT_BUCKET', 'AHP_S3_ENDPOINT', 'AHP_MINIO_ROOT_USER', 'AHP_MINIO_ROOT_PASSWORD', 'AS_DATA_DATABASE_URL']) {
      delete env[k];
    }
    delete env.DATABASE_URL; // only feeds the artifact store in as-runner; strip to keep config unambiguously "omitted"
  }
  const proc = spawn(bin, [], {
    cwd: PROJECT_ROOT,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const logs = [];
  proc.stderr.on('data', (d) => logs.push(String(d)));
  proc.stdout.on('data', (d) => logs.push(String(d)));
  return { proc, kill: () => { try { proc.kill('SIGTERM'); } catch {} }, logs: () => logs.join('') };
}

async function pollHealthz(port, { timeoutMs = 20000 } = {}) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    try {
      const r = await httpGet(port, '/healthz', { tls: false });
      if (r.status === 200) return true;
    } catch {}
    await sleep(200);
  }
  return false;
}

const state = await spawnService({ port: STATE_PORT, role: 'state', tls: false });
let space = null;
try {
  // A1: State role readiness over the isolated DB (readyz performs a DB ping).
  const stateReady = await pollReady(STATE_PORT, { tls: false, timeoutMs: 25000 });
  record(stateReady, `state role /readyz=200 over isolated DB (:${STATE_PORT})`);
  const stateHealth = await httpGet(STATE_PORT, '/healthz', { tls: false });
  record(stateHealth.status === 200, `state role /healthz=200 (got ${stateHealth.status})`);

  // A2: Space service boots and serves /healthz in the same process group.
  ensureAsRunner();
  const stateURL = `http://127.0.0.1:${STATE_PORT}`;
  space = spawnAsRunner({ listen: `127.0.0.1:${SPACE_PORT}`, stateURL });
  const spaceHealth = await pollHealthz(SPACE_PORT, { timeoutMs: 25000 });
  record(spaceHealth, `space service (as-runner) /healthz=200 (:${SPACE_PORT})`);

  // A3: Space is wired to the LIVE state role URL (composition, not a default).
  await sleep(300);
  const slog = space.logs();
  record(slog.includes(stateURL), `space wired to live state URL (${stateURL})`);

  // A4: Space routes POST /v1/space/{Operation} (routing engaged). Operation keys
  // in the imported handler are PascalCase; ListTurns is a read op that needs no
  // auth body and, with the passthrough auth used here, deterministically returns
  // 200 {"turns":[...]}. A 200 proves the route resolved (not 404/405); the
  // authenticated Space->State data round-trip is the Go suite test:space-state.
  const routed = await new Promise((res) => {
    const body = Buffer.from('{}');
    const req = http.request(
      { host: '127.0.0.1', port: SPACE_PORT, path: '/v1/space/ListTurns', method: 'POST',
        headers: { 'content-type': 'application/json', 'content-length': body.length }, timeout: 4000 },
      (r) => { r.resume(); r.on('end', () => res(r.statusCode)); });
    req.on('error', () => res(-1));
    req.on('timeout', () => { req.destroy(); res(-2); });
    req.end(body);
  });
  record(routed === 200, `space routes POST /v1/space/ListTurns (status=${routed}, expect 200)`);

  // A5: State emits a per-role OTel span across the whole-system process group.
  state.kill();
  await sleep(300); // allow SIGTERM span flush
  const stlogs = state.logs();
  const hasSpan = /"name":"GET \/healthz"/.test(stlogs) && /"service":"state"/.test(stlogs);
  record(hasSpan, `state role OTel server-span (name=GET /healthz, service=state) flushed`);
} catch (e) {
  record(false, `unexpected error: ${e && e.message ? e.message : String(e)}`);
} finally {
  try { if (space) space.kill(); } catch {}
  try { state.kill(); } catch {}
  await sleep(300);
}

if (failures > 0) {
  console.error(`\nwhole-system: ${failures} assertion(s) FAILED`);
  process.exit(1);
}
console.log('\nwhole-system: ALL ASSERTIONS PASSED');
