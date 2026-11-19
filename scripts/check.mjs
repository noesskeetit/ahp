#!/usr/bin/env node
// npm run check — go vet + go test -race over delivery's whole-system scope.
//
// Delivery integrates every contractor's code, so this gate runs the full tree,
// not just delivery-owned packages. It is NEVER a silent skip: every package is
// enumerated with `go list ./...` and the ONLY packages dropped are the ones
// listed in OPEN_RFC_EXCLUSIONS below, each of which is an open RFC posted to
// the forum and owned by another contractor. Dropped packages and their reasons
// are printed on stdout and the run still exits 1 if the in-scope packages fail.
// An exclusion is therefore a documented known-open-defect, never a pass.
//
// Integration is never skipped either: without AHP_TEST_DATABASE_URL and
// AHP_TEST_REDIS_URL the script fails loudly. All databases are forced to the
// block-isolated database (default ahp_hive_delivery) via ahp-env.mjs so this
// never touches a neighbour's database. Tests are serialized (-p=1) because
// sibling test binaries migrate the same isolated database and otherwise
// deadlock on concurrent DDL. Only Node builtins; no shell interpolation — all
// arguments reach the child as an array. Secret values are never printed.

import { spawn, spawnSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildEnv } from './ahp-env.mjs';

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const REQUIRED_ENV = ['AHP_TEST_DATABASE_URL', 'AHP_TEST_REDIS_URL'];

// Neighbour-owned packages that fail at the integration baseline and are NOT
// delivery's to fix ("прораб не чинит чужой код"). Each entry MUST correspond to
// an open RFC on the forum; if the owner fixes the package it must be removed
// here so the gate tightens again. fragment matches a `go list` package path.
//
// History: `internal/identity` was excluded at the dirty baseline 8b14a59 where
// TestCancelSubtreeChildThenIdempotent / TestCancelSubtreeRootAndForeignScope
// failed with "identity: task id is required". After integrating the ACCEPTED
// identity snapshot (commit a65ebc17d56f41822d12a7ac4d102aa40fb7a492) those tests
// pass, so the exclusion was removed to tighten the gate again. Verified with
// `go test -race ./internal/identity/` against the isolated DB. If the gate
// reopens, re-add the entry and post the RFC.
const OPEN_RFC_EXCLUSIONS = [];

// `go vet` builds whole package graphs at once; over the full ~27-package tree
// this exhausts memory (SIGKILL on shared protobuf/internal/filetype compiles on
// this host). We bound the build/link parallelism with -p AND feed vet in small
// chunks so each compiler graph stays small; GOCACHE is shared so each chunk
// reuses what earlier chunks compiled. This NEVER drops a package: every in-scope
// package is vetted exactly once across all chunks.
const VET_CHUNK_SIZE = 4;
const VET_BUILD_PARALLELISM = 2;

function run(cmd, args, env) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn(cmd, args, { cwd: projectRoot, env, stdio: 'inherit' });
    child.on('error', (err) => rejectPromise(new Error(`failed to start ${cmd}: ${err.message}`)));
    child.on('close', (code, signal) => {
      if (signal) return rejectPromise(new Error(`${cmd} ${args.join(' ')} terminated by signal ${signal}`));
      resolvePromise(code ?? 1);
    });
  });
}

function fail(message) {
  process.stderr.write(`\n[check] FAIL: ${message}\n`);
  process.exit(1);
}

const env = await buildEnv(projectRoot);
// Build against the CANONICAL host module cache where root pre-seeded the full
// dependency set (go-sdk v1.8.0, OTel v1.47.0, ...). buildEnv()/ignored .env may
// redirect GOMODCACHE to a fresh empty scratch which lacks that pre-seed; a plain
// offline `go list`/`vet` there falsely reports "module lookup disabled by GOPROXY=off".
// Reading the canonical cache changes no dependency semantics (no replace/downgrade,
// sum verification intact). Override with GOMODCACHE explicitly if you know better.
if (!process.env.AHP_KEEP_ISOLATED_MODCACHE) {
  const host = (env.HOME || process.env.HOME || '') + '/go/pkg/mod';
  if (host) env.GOMODCACHE = host;
}

const missing = REQUIRED_ENV.filter((name) => {
  const value = env[name];
  return typeof value !== 'string' || value.trim() === '';
});
if (missing.length > 0) {
  fail(
    `required environment is not set: ${missing.join(', ')}. ` +
      'Set them in the ignored .env (see .env.example) or the process environment. ' +
      'Integration checks run against real isolated services and are never reported as skipped-pass.',
  );
}

// Enumerate packages so we can drop only the explicitly RFC-tracked ones, and
// so an exclusion that no longer exists is caught rather than silently masking.
const list = spawnSync('go', ['list', './...'], { cwd: projectRoot, env, encoding: 'utf8' });
if (list.status !== 0) fail(`go list failed: ${list.stderr || list.stdout || 'unknown error'}`);
const all = list.stdout.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
if (all.length === 0) fail('go list returned no packages');

const excludedPackages = all.filter((p) => OPEN_RFC_EXCLUSIONS.some((e) => p.includes(e.fragment)));
const pkgs = all.filter((p) => !excludedPackages.includes(p));

// Warn if an exclusion fragment matched nothing (owner may have removed the
// package) so this list cannot rot into dead silent config.
for (const e of OPEN_RFC_EXCLUSIONS) {
  if (!all.some((p) => p.includes(e.fragment))) {
    process.stdout.write(`[check] WARN: exclusion fragment "${e.fragment}" matches no package (remove it?)\n`);
  }
}
if (excludedPackages.length > 0) {
  process.stdout.write(`[check] EXCLUDED (open RFC on forum, NOT a pass): ${excludedPackages.join(', ')}\n`);
  for (const e of OPEN_RFC_EXCLUSIONS) {
    if (excludedPackages.some((p) => p.includes(e.fragment))) {
      process.stdout.write(`[check]   - ${e.fragment}: ${e.reason}\n`);
    }
  }
}

process.stdout.write(`[check] database target: ${env.AHP_DB_NAME}\n`);

// --- go vet (chunked + bounded parallelism to avoid host memory exhaustion) ---
process.stdout.write(
  `[check] go vet -p=${VET_BUILD_PARALLELISM} (${pkgs.length}/${all.length} in-scope packages, ` +
    `chunks of ${VET_CHUNK_SIZE})\n`,
);
let vetFailures = 0;
for (let i = 0; i < pkgs.length; i += VET_CHUNK_SIZE) {
  const chunk = pkgs.slice(i, i + VET_CHUNK_SIZE);
  const chunkNo = Math.floor(i / VET_CHUNK_SIZE) + 1;
  const totalChunks = Math.ceil(pkgs.length / VET_CHUNK_SIZE);
  process.stdout.write(`[check]   vet chunk ${chunkNo}/${totalChunks}: ${chunk.length} pkg(s)\n`);
  const code = await run('go', ['vet', `-p=${VET_BUILD_PARALLELISM}`, ...chunk], env);
  if (code !== 0) {
    vetFailures += 1;
    process.stderr.write(`[check] go vet failed on chunk ${chunkNo}: ${chunk.join(' ')}\n`);
  }
}
if (vetFailures !== 0) fail(`go vet failed on ${vetFailures} chunk(s)`);

// --- go test (serialized to avoid isolated-DB DDL deadlock) ---
// `-race` whole-tree is the director's VM gate (swap headroom on this host is ~1.3GiB).
// On a resource-tight host set AHP_CHECK_SKIP_RACE=1 to run the REAL test suite without
// the race detector locally; this is a loud, explicit mode, NEVER a silent pass — the
// race detector still runs for the whole tree on the VM acceptance run.
const skipRace = process.env.AHP_CHECK_SKIP_RACE === '1' || process.env.AHP_CHECK_SKIP_RACE === 'true';
const raceArgs = skipRace ? [] : ['-race'];
const raceTag = skipRace ? '(race SKIPPED -> VM gate)' : '-race';
if (skipRace) process.stdout.write('[check] NOTICE: AHP_CHECK_SKIP_RACE set — running WITHOUT -race locally; full -race is a VM acceptance gate.\n');
process.stdout.write(`[check] go test ${raceArgs.length ? '-race ' : ''}-p=1 -count=1 ${raceTag} (${pkgs.length}/${all.length} in-scope packages)\n`);
const jsonArgs = process.env.AHP_CHECK_JSON === '1' ? ['-json'] : [];
const testCode = await run('go', ['test', ...jsonArgs, ...raceArgs, '-p=1', '-count=1', ...pkgs], env);
if (testCode !== 0) fail(`go test ${raceArgs.join(' ')} -p=1 -count=1 exited with code ${testCode}`);

process.stdout.write(
  `[check] OK: go vet and go test ${raceTag} passed for ${pkgs.length} in-scope packages; ` +
    `${excludedPackages.length} excluded as open RFCs.` +
    (skipRace ? ' (run full -race on the VM acceptance host)\n' : '\n'),
);
