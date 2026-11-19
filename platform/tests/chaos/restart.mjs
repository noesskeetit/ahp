#!/usr/bin/env node
// Process liveness smoke only: starts ahp, SIGKILLs it, checks health/readiness
// after restart. Does not write application state or prove execution recovery.
// SDK/effect recovery: node scripts/recovery-process.mjs (real PG + mTLS).
// Requires: compiled ahp binary at ./bin/ahp or AHP_BIN env.
// Requires: AHP_TEST_DATABASE_URL (isolated test database).

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const projectRoot = resolve(scriptDir, '../..');

function parseEnv(text) {
  const out = {};
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq <= 0) continue;
    out[line.slice(0, eq).trim()] = line.slice(eq + 1).trim().replace(/^["']|["']$/g, '');
  }
  return out;
}

async function loadEnv() {
  const env = { ...process.env };
  const p = resolve(projectRoot, '.env');
  if (existsSync(p)) {
    const parsed = parseEnv(await readFile(p, 'utf8'));
    for (const [k, v] of Object.entries(parsed)) if (!env[k]) env[k] = v;
  }
  return env;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForHealth(url, timeoutMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const r = await fetch(`${url}/healthz`, { signal: AbortSignal.timeout(2000) });
      if (r.ok) return true;
    } catch {}
    await sleep(500);
  }
  return false;
}

const env = await loadEnv();
const binPath = env.AHP_BIN || resolve(projectRoot, 'bin/ahp');
if (!existsSync(binPath)) {
  console.error(`FAIL: ahp binary not found at ${binPath}. Run: bash scripts/build-product.sh ahp (builds the ignored bin/ahp from source and attests its vcs stamp)`);
  process.exit(1);
}

const PORT = 19080 + Math.floor(Math.random() * 100);
const base = `http://127.0.0.1:${PORT}`;
const dbUrl = env.AHP_TEST_DATABASE_URL;
if (!dbUrl) { console.error('FAIL: AHP_TEST_DATABASE_URL not set'); process.exit(1); }

let child = null;
function killChild() { if (child && !child.killed) child.kill('SIGKILL'); }
process.on('exit', killChild);

async function startService() {
  child = spawn(binPath, ['--role', 'all', '--listen', `127.0.0.1:${PORT}`], {
    env: { ...env, AHP_ROLE: 'all', AHP_LISTEN: `127.0.0.1:${PORT}`, AHP_DATABASE_URL: dbUrl },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stderr.on('data', () => {});
  const healthy = await waitForHealth(base);
  if (!healthy) { console.error('FAIL: service did not become healthy'); process.exit(1); }
}

let pass = 0, fail = 0;
function assert(cond, msg) {
  if (cond) { pass++; console.log(`  PASS: ${msg}`); }
  else { fail++; console.error(`  FAIL: ${msg}`); }
}

// Phase 1: Start and verify health
console.log('\n[Phase 1] Start service, verify health');
await startService();
const health1 = await fetch(`${base}/healthz`).then(r => r.json());
assert(health1.status === 'ok', 'Health check after start');

// Phase 2: Kill with SIGKILL (simulates crash)
console.log('\n[Phase 2] SIGKILL (crash simulation)');
child.kill('SIGKILL');
await new Promise(r => child.on('exit', r));
await sleep(1000);
try {
  await fetch(`${base}/healthz`, { signal: AbortSignal.timeout(2000) });
  assert(false, 'Port should be closed after SIGKILL');
} catch {
  assert(true, 'Port closed after SIGKILL (expected)');
}

// Phase 3: Restart and verify liveness/readiness (same DB)
console.log('\n[Phase 3] Restart same DB, verify readiness');
await startService();
const health2 = await fetch(`${base}/healthz`).then(r => r.json());
assert(health2.status === 'ok', 'Health check after restart');

// Verify readiness includes DB ping
const ready = await fetch(`${base}/readyz`);
const readyBody = await ready.json();
assert(ready.status === 200 && readyBody.status === 'ok', 'Readiness (DB ping) after restart');

killChild();
await sleep(500);

console.log(`\nResults: ${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
console.log('Chaos/restart liveness smoke: PASSED (no data-recovery assertion)');
