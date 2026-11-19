#!/usr/bin/env node
// scripts/space-state.mjs — Whole-system State<->Space wire gate.
//
// Runs the Space block's own State integration suite (build tag
// state_integration) with the environment forced to the block-isolated
// database. Those tests construct the REAL accepted State handler
// (stateapi.New(store, verifier, authorize, issue)) in-process and drive the
// Space client against it: durable/restart, UNKNOWN reconcile, stale-epoch
// fencing, private-context cross-subject isolation, checkpoint fencing, and the
// Space HTTP journey + dispatch/stream. This is the authenticated wire/auth
// proof that complements tests/e2e/whole-system.mjs (composition/readiness).
//
// Children inherit ONLY the isolated DB env (ahp-env.isolateDatabase); the
// shared ahp_go database is unreachable from here. Secrets are never printed.

import { spawn } from 'node:child_process';
import { buildEnv } from './ahp-env.mjs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(HERE, '..');
const env = await buildEnv(PROJECT_ROOT);
if (env.AHP_DB_NAME && !/^ahp_hive_[a-z][a-z0-9_]{0,23}$/.test(env.AHP_DB_NAME)) {
  console.error('refusing: non-allowlisted DB name'); process.exit(2);
}
console.error(`[space-state] DB=${env.AHP_DB_NAME} (creds masked)`);

const args = ['-tags', 'state_integration', '-count=1', '-timeout', '240s', './tests/space/...', ...process.argv.slice(2)];
const child = spawn('go', ['test', ...args], { cwd: PROJECT_ROOT, env, stdio: 'inherit' });
child.on('exit', (code, sig) => process.exit(sig ? 1 : (code ?? 1)));
