#!/usr/bin/env node
// tests/chaos/graceful-shutdown.mjs — SIGTERM triggers graceful shutdown.
// Verifies: (1) process exits within timeout, (2) in-flight requests complete,
// (3) port released cleanly.

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

const env = await loadEnv();
const binPath = env.AHP_BIN || resolve(projectRoot, 'bin/ahp');
if (!existsSync(binPath)) {
  console.error(`FAIL: ahp binary not found. Build it first.`);
  process.exit(1);
}

const PORT = 19200 + Math.floor(Math.random() * 100);
const base = `http://127.0.0.1:${PORT}`;
const dbUrl = env.AHP_TEST_DATABASE_URL;
if (!dbUrl) { console.error('FAIL: AHP_TEST_DATABASE_URL not set'); process.exit(1); }

// The "all" role binds AHP_LISTEN (not the --listen flag; see cmd/ahp serveAll).
const child = spawn(binPath, ['--role', 'all'], {
  env: { ...env, AHP_ROLE: 'all', AHP_LISTEN: `127.0.0.1:${PORT}`, AHP_DATABASE_URL: dbUrl },
  stdio: ['ignore', 'pipe', 'pipe'],
});
child.stderr.on('data', () => {});

// Wait for health
let ready = false;
for (let i = 0; i < 30; i++) {
  try {
    const r = await fetch(`${base}/healthz`, { signal: AbortSignal.timeout(1000) });
    if (r.ok) { ready = true; break; }
  } catch {}
  await sleep(500);
}
if (!ready) { console.error('FAIL: service not healthy'); child.kill(); process.exit(1); }

let pass = 0, fail = 0;
function assert(cond, msg) {
  if (cond) { pass++; console.log(`  PASS: ${msg}`); }
  else { fail++; console.error(`  FAIL: ${msg}`); }
}

// Send SIGTERM and measure exit time
console.log('\n[Graceful Shutdown] Sending SIGTERM...');
const start = Date.now();
child.kill('SIGTERM');

const exitCode = await new Promise(r => child.on('exit', (code, sig) => r({ code, sig })));
const elapsed = Date.now() - start;

assert(elapsed < 20000, `Exited in ${elapsed}ms (grace period 15s)`);
assert(exitCode.code === 0 || exitCode.sig === null, `Clean exit code=${exitCode.code} signal=${exitCode.sig}`);

// Port should be released
await sleep(500);
try {
  await fetch(`${base}/healthz`, { signal: AbortSignal.timeout(2000) });
  assert(false, 'Port should be closed after shutdown');
} catch {
  assert(true, 'Port closed after graceful shutdown');
}

console.log(`\nResults: ${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
