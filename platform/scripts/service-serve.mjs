#!/usr/bin/env node
// npm-run helper: run bin/ahp as a background daemon for HTTP e2e/load tests.
//
// It loads the ignored .env, forces the isolated block database, then spawns
// the already-built ./bin/ahp in a NEW process group (detached) so the service
// outlives the shell that started it. The child PID is written to
// .ahp-evidence/service.pid; logs go to .ahp-evidence/service.log. Secrets from
// the DB URL are never printed.
//
// Usage: node scripts/service-serve.mjs <start|stop|status> [role] [listen]
//   role:   all (default) | state | identity | model
//   listen: 127.0.0.1:<port> (default derived from AHP_API_URL, else :8080)

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildEnv } from './ahp-env.mjs';

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const stateDir = resolve(projectRoot, '.ahp-evidence');
const pidFile = resolve(stateDir, 'service.pid');
const logFile = resolve(stateDir, 'service.log');
const bin = resolve(projectRoot, 'bin', 'ahp');

const cmd = process.argv[2] || 'start';

async function readPid() {
  try {
    return parseInt((await readFile(pidFile, 'utf8')).trim(), 10) || 0;
  } catch {
    return 0;
  }
}

function alive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function stop() {
  const pid = await readPid();
  if (!alive(pid)) {
    console.log('[service-serve] not running');
    await rm(pidFile, { force: true });
    return 0;
  }
  process.kill(pid, 'SIGTERM');
  for (let i = 0; i < 40; i++) {
    await new Promise((r) => setTimeout(r, 250));
    if (!alive(pid)) break;
  }
  if (alive(pid)) {
    console.log(`[service-serve] pid=${pid} still alive after SIGTERM, sending SIGKILL`);
    process.kill(pid, 'SIGKILL');
  }
  console.log(`[service-serve] stopped pid=${pid}`);
  await rm(pidFile, { force: true });
  return 0;
}

async function status() {
  const pid = await readPid();
  if (alive(pid)) {
    console.log(`[service-serve] running pid=${pid}`);
    return 0;
  }
  console.log('[service-serve] not running');
  return 1;
}

async function start() {
  const role = process.argv[3] || 'all';
  if (!existsSync(bin)) {
    console.error('[service-serve] bin/ahp not built. Run: npm run check && node -e ...  (go build -o bin/ahp ./cmd/ahp)');
    return 1;
  }
  await mkdir(stateDir, { recursive: true });
  const env = await buildEnv(projectRoot);
  if (!env.AHP_DATABASE_URL) {
    console.error('[service-serve] AHP_DATABASE_URL not set');
    return 1;
  }
  let listen = process.argv[4] || '';
  if (!listen) {
    const api = env.AHP_API_URL || 'http://127.0.0.1:8080';
    const u = new URL(api);
    const host = ['localhost', ''].includes(u.hostname) ? '127.0.0.1' : u.hostname;
    listen = `${host}:${u.port || (u.protocol === 'https:' ? '443' : '80')}`;
  }
  // "all" role reads AHP_LISTEN; single roles use --listen (also pass AHP_LISTEN).
  env.AHP_LISTEN = listen;
  env.AHP_ROLE = role;
  if (alive(await readPid())) {
    console.log('[service-serve] already running, stopping first');
    await stop();
  }
  const args = role === 'all' ? ['--role', 'all', '--verbose'] : ['--role', role, '--listen', listen, '--verbose'];
  const out = await open(logFile, 'a');
  const child = spawn(bin, args, { cwd: projectRoot, env, detached: true, stdio: ['ignore', out.fd, out.fd] });
  await out.close();
  child.unref();
  await writeFile(pidFile, String(child.pid));
  console.log(`[service-serve] started pid=${child.pid} role=${role} listen=${listen} db=${env.AHP_DB_NAME}`);
  return 0;
}

import { open } from 'node:fs/promises';

let code = 1;
if (cmd === 'start') code = await start();
else if (cmd === 'stop') code = await stop();
else if (cmd === 'status') code = await status();
else {
  console.error('[service-serve] usage: start|stop|status [role] [listen]');
  code = 2;
}
process.exit(code);
