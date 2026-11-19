#!/usr/bin/env node
// npm run test:go — run the Go test suite against the block-isolated database.
//
// This is delivery's reproducible scoped gate: it loads the git-ignored .env,
// forces every database-facing variable to the block DB (default
// ahp_hive_delivery) via ahp-env, then runs `go test` over ./... . A package
// whose *neighbour-owned* code does not compile (e.g. an unfixed upstream test
// file) can be excluded with --exclude <path-fragment> so delivery's own scope
// is still measured; every exclusion is printed and must be justified by an
// open RFC — silent skipping is never a pass. Secret values are never printed.
//
// Usage: node scripts/gotest.mjs [--exclude <frag>]... [-- <extra go test args>]

import { spawn, spawnSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildEnv } from './ahp-env.mjs';

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const argv = process.argv.slice(2);
const excludes = [];
const goArgs = [];
let afterSep = false;
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (a === '--') { afterSep = true; continue; }
  if (afterSep) { goArgs.push(a); continue; }
  if (a === '--exclude') { excludes.push(argv[++i] ?? ''); continue; }
  goArgs.push(a);
}

const env = await buildEnv(projectRoot);

// Enumerate packages so we can drop only the explicitly excluded ones.
const list = spawnSync('go', ['list', './...'], { cwd: projectRoot, env, encoding: 'utf8' });
if (list.status !== 0) {
  process.stderr.write(list.stderr || list.stdout || 'go list failed\n');
  process.exit(1);
}
const all = list.stdout.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
const dropped = all.filter((p) => excludes.some((f) => f && p.includes(f)));
const pkgs = all.filter((p) => !dropped.includes(p));

if (dropped.length > 0) {
  process.stdout.write(`[gotest] EXCLUDED (open RFC, not a pass): ${dropped.join(', ')}\n`);
}
const testArgs = goArgs.length > 0 ? goArgs : ['-race', '-p=1', '-count=1'];
process.stdout.write(`[gotest] db=${env.AHP_DB_NAME} packages=${pkgs.length} args=${testArgs.join(' ')}\n`);

const code = await new Promise((res) => {
  const child = spawn('go', ['test', ...testArgs, ...pkgs], { cwd: projectRoot, env, stdio: 'inherit' });
  child.on('error', (e) => { process.stderr.write(`[gotest] spawn failed: ${e.message}\n`); res(1); });
  child.on('close', (c) => res(c ?? 1));
});
process.stdout.write(`[gotest] go test exited ${code}\n`);
process.exit(code);
