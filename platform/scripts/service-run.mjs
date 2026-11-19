#!/usr/bin/env node
// npm run service:run — start ahp in a chosen role for local development.
// Loads the ignored .env, forces the isolated block database via ahp-env, then
// runs `go run ./cmd/ahp`. Requires Go 1.26+ on PATH.
//
// Usage: node scripts/service-run.mjs [role] [listen]
//   role:   all (default) | state | identity | model
//   listen: ":8080" (default for all) or ":18081" etc.

import { spawn } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildEnv } from './ahp-env.mjs';

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const env = await buildEnv(projectRoot);

if (!env.AHP_DATABASE_URL) {
  console.error('[service:run] FAIL: AHP_DATABASE_URL not set in environment or .env');
  process.exit(1);
}

const role = process.argv[2] || 'all';
const listen = process.argv[3] || (role === 'all' ? ':8080' : '');
const args = ['run', './cmd/ahp', '--role', role, '--verbose'];
if (listen) args.splice(4, 0, '--listen', listen);

console.log(`[service:run] go ${args.join(' ')}  (db=${env.AHP_DB_NAME})`);
const child = spawn('go', args, { cwd: projectRoot, env, stdio: 'inherit' });
child.on('close', (code) => process.exit(code ?? 1));
