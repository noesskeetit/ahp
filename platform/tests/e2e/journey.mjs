#!/usr/bin/env node
// tests/e2e/journey.mjs -- Journey orchestrator (delivery-owned, UNACCEPTED).
//
// Runs the delivery legs against COMBAT dependencies in one entrypoint and prints
// ONE honest verdict. Nothing is synthesized, mocked or re-asserted.
//
//   LEG-1 tests/e2e/model-to-artifact.mjs  -> REAL native-Qwen -> REAL MinIO versioned
//          artifact -> REAL PG State.            (component proof)
//   LEG-2 tests/e2e/emil-tool-hop.mjs      -> Emil A2A wiring/component hop.
//   LEG-3 tests/e2e/live-journey.mjs       -> CANONICAL WHOLE single chain:
//          REAL Qwen -> REAL Emil-core :18000 tool-call -> Emil's REAL tool bytes ->
//          REAL MinIO (presigned PUT/Seal/Download, sha256 PUT==GET) -> REAL PG.
//
// WHOLE-journey completion is decided ONLY by LEG-3 (the single chained run), per
// HQ decisions/2026-10-30-live-journey-contract.md. Component legs green never imply
// the whole journey is green. Exit 0 only when the whole journey is GREEN.
// Secrets are never printed.

import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const EMIL = (process.env.AHP_EMIL_URL || 'http://127.0.0.1:18000').replace(/\/$/, '');
const RED = (s) => String(s).replace(/[A-Za-z0-9._+/=-]{40,}/g, '<REDACTED>');

function run(label, script, env = {}) {
  const p = join(HERE, script);
  if (!existsSync(p)) { console.log(`# MISSING ${script}`); return { status: null, text: '' }; }
  const r = spawnSync('node', [p], { encoding: 'utf8', env: { ...process.env, ...env }, cwd: process.cwd() });
  const text = (r.stdout || '') + (r.stderr || '');
  console.log(`\n===== ${label} (${script}) rc=${r.status} =====`);
  console.log(RED(text).trimEnd());
  return { status: r.status, text };
}

console.log('# AHP delivery JOURNEY orchestrator  (unaccepted; combat deps; no mocks)');

const m2a = run('LEG-1 model->artifact->PG-state (component)', 'model-to-artifact.mjs');
const mm = m2a.text.match(/(\d+)\s+passed,\s*(\d+)\s+failed/);
const leg1Passed = mm ? parseInt(mm[1], 10) : 0;
const leg1Failed = mm ? parseInt(mm[2], 10) : 999;
const leg1Green = leg1Failed === 0 && leg1Passed > 0;

const hop = run('LEG-2 Emil A2A hop (component)', 'emil-tool-hop.mjs', { AHP_EMIL_URL: EMIL });
const hopVerdict = (hop.text.match(/EMIL_TOOL_HOP=(\S+)/) || [])[1] || 'unknown';

const live = run('LEG-3 CANONICAL whole live journey (single chain)', 'live-journey.mjs', { AHP_EMIL_URL: EMIL });
const wholeGreen = /LIVE_JOURNEY=GREEN/.test(live.text) && live.status === 0;
const putSha = (live.text.match(/LIVE_PUT_SHA256=([0-9a-f]+)/) || [])[1] || '';
const getSha = (live.text.match(/LIVE_GET_SHA256=([0-9a-f]+)/) || [])[1] || '';
const emilTask = (live.text.match(/LIVE_EMIL_TASK=(\S+)/) || [])[1] || '';
const shaMatch = !!putSha && putSha === getSha;

let verdict, exitCode;
if (wholeGreen && shaMatch) { verdict = 'WHOLE-GREEN'; exitCode = 0; }
else if (wholeGreen && !shaMatch) { verdict = 'RED (whole leg claims green but PUT/GET sha mismatch)'; exitCode = 1; }
else { verdict = 'NOT-COMPLETE (LEG-3 whole journey not green)'; exitCode = 1; }

console.log('\n===== JOURNEY VERDICT =====');
console.log(`  LEG-1 model->artifact->PG (component) : ${leg1Green ? 'GREEN' : 'RED'} (passed=${leg1Passed} failed=${leg1Failed})`);
console.log(`  LEG-2 Emil A2A hop (component)        : ${hopVerdict}`);
console.log(`  LEG-3 WHOLE live journey (canonical)  : ${wholeGreen ? 'GREEN' : 'NOT GREEN'}` + (emilTask ? ` emilTask=${emilTask}` : ''));
console.log(`  sha256 PUT==GET on real Emil bytes    : ${shaMatch ? 'YES' : 'NO'} ${putSha ? putSha.slice(0, 16) + '..' : ''}`);
console.log(`  combat deps                           : MinIO=${process.env.AHP_S3_ENDPOINT || 'env'} Emil=${EMIL}`);
console.log(`JOURNEY=${verdict}`);
console.log(`  JOURNEY_WHOLE=${wholeGreen ? 'COMPLETE' : 'NOT_COMPLETE'}`);
if (!wholeGreen) console.log('  NOTE: whole-journey completion requires LEG-3 green; component legs never substitute for it.');
process.exit(exitCode);
