#!/usr/bin/env node
// Builds the MinIO image from the ignored .local/minio-src git checkout.
// Verifies HEAD and a clean tracked tree, then runs docker build directly (no shell).

import { spawn } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const EXPECTED_HEAD = '9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a';
const SRC = '.local/minio-src';
const IMAGE = 'ahp-handbook-minio:9e49d5e7';
const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const env = { ...process.env };
for (const k of ['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy']) {
  delete env[k];
}

function run(cmd, args, cwd) {
  return new Promise((res, rej) => {
    const child = spawn(cmd, args, { cwd, env, stdio: ['ignore', 'pipe', 'inherit'] });
    let out = '';
    child.stdout.on('data', (d) => (out += d));
    child.on('error', rej);
    child.on('close', (code, signal) => res({ code: signal ? 1 : code ?? 1, out: out.trim() }));
  });
}

function fail(msg) {
  process.stderr.write(`[build-minio] FAIL: ${msg}\n`);
  process.exit(1);
}

const head = await run('git', ['rev-parse', 'HEAD'], resolve(projectRoot, SRC));
if (head.code !== 0) fail(`git rev-parse HEAD failed in ${SRC}`);
if (head.out !== EXPECTED_HEAD) {
  fail(`${SRC} HEAD is ${head.out}, expected ${EXPECTED_HEAD}`);
}

const dirty = await run('git', ['diff', '--quiet'], resolve(projectRoot, SRC));
if (dirty.code !== 0) fail(`${SRC} tracked tree is dirty (git diff --quiet)`);

const staged = await run('git', ['diff', '--cached', '--quiet'], resolve(projectRoot, SRC));
if (staged.code !== 0) fail(`${SRC} index is dirty (git diff --cached --quiet)`);

const build = await run(
  'docker',
  ['build', '-f', 'infra/minio.Dockerfile', '-t', IMAGE, SRC],
  projectRoot,
);
process.exit(build.code);
