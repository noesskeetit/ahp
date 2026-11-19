#!/usr/bin/env node
// npm run test:e2e|chaos|load — run all .mjs scripts in tests/<category>/.
// Children inherit the isolated environment (block DB) from ahp-env, never the
// shared default database. Each test exits 0 on pass, non-zero on failure.
//
// Usage: node scripts/run-tests.mjs <e2e|chaos|load> [filter]

import { spawn } from 'node:child_process';
import { readdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildEnv } from './ahp-env.mjs';

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const category = process.argv[2];
const filter = process.argv[3] || '';

if (!category || !['e2e', 'chaos', 'load'].includes(category)) {
  console.error('[run-tests] Usage: node scripts/run-tests.mjs <e2e|chaos|load> [filter]');
  process.exit(1);
}

const testDir = resolve(projectRoot, 'tests', category);
if (!existsSync(testDir)) {
  console.error(`[run-tests] Directory not found: ${testDir}`);
  process.exit(1);
}

const entries = await readdir(testDir, { withFileTypes: true });
const scripts = entries
  .filter((e) => e.isFile() && e.name.endsWith('.mjs'))
  .map((e) => e.name)
  .filter((n) => !filter || n.includes(filter))
  .sort();

if (scripts.length === 0) {
  console.error(`[run-tests] No ${category} scripts found in ${testDir}`);
  process.exit(1);
}

const env = await buildEnv(projectRoot);
console.log(`[run-tests] ${category}: ${scripts.length} script(s)  (db=${env.AHP_DB_NAME}, api=${env.AHP_API_URL || 'default'})`);
let passed = 0;
let failed = 0;

for (const script of scripts) {
  const full = join(testDir, script);
  console.log(`\n--- ${script} ---`);
  const code = await new Promise((res) => {
    const child = spawn(process.execPath, [full], {
      cwd: projectRoot,
      env,
      stdio: 'inherit',
    });
    child.on('close', (c) => res(c ?? 1));
  });
  if (code === 0) passed++;
  else {
    failed++;
    console.error(`  x ${script} exited ${code}`);
  }
}

console.log(`\n[run-tests] ${category}: ${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
