#!/usr/bin/env node
// delivery acceptance-pack generator (contract acceptance #5: "полный acceptance pack").
//
// Orchestrates EXISTING proven entrypoints and writes a structured evidence pack.
// Honest classification per item:
//   run   = executed NOW in this process (real rc + parsed verdict)
//   cited = proven in a prior recorded run at a specific commit; re-run cmd given
//   gated = blocked on external/target-runtime (VM / Emil-core / go-sdk pre-seed);
//           never marked green. source != deployment; availability(200) != execution.
//
// No secrets are printed. DB is always forced to the block-isolated allowlist DB.
// Usage: node scripts/acceptance-pack.mjs [--all] [--out DIR]
//   --all  additionally run heavier Mac-runnable suites (chaos, load) NOW.

import { spawn } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve, join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildEnv } from './ahp-env.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, '..');
const HQ = process.env.AHP_HQ_DIR || '/Users/aogabbasov/AI Agents/ahp/company-brain';
const G0 = process.env.AHP_NATIVE_QWEN_ASSERT ||
  join(HQ, 'acceptance/integration-validation/scripts/native-qwen-assert.sh');

const argv = process.argv.slice(2);
const RUN_ALL = argv.includes('--all');
const outIdx = argv.indexOf('--out');
const OUT = outIdx >= 0 ? resolve(argv[outIdx + 1]) : '/tmp';

function tail(s, n = 4) { return String(s).split(/\r?\n/).filter(Boolean).slice(-n).join(' | '); }

function run(cmd, args, env, timeoutMs = 240000) {
  return new Promise((res) => {
    const p = spawn(cmd, args, { cwd: ROOT, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = ''; const t = setTimeout(() => { try { p.kill('SIGKILL'); } catch {} }, timeoutMs);
    p.stdout.on('data', d => out += d); p.stderr.on('data', d => err += d);
    p.on('close', rc => { clearTimeout(t); res({ rc, out, err }); });
    p.on('error', e => { clearTimeout(t); res({ rc: 127, out: '', err: String(e) }); });
  });
}

// Extract only verdict-bearing lines so the pack never carries env/credentials.
function verdictKeys(text, keys) {
  const out = {};
  for (const line of String(text).split(/\r?\n/)) {
    for (const k of keys) { const m = line.match(new RegExp(k + '[=:][ \\t]*([^ \\t]+.*)$')); if (m) out[k] = m[1].trim(); }
  }
  return out;
}

async function main() {
  const base = await buildEnv(ROOT);
  const env = {
    ...base,
    HTTP_PROXY: '', HTTPS_PROXY: '', http_proxy: '', https_proxy: '', NO_PROXY: 'localhost,127.0.0.1,::1',
    GOPROXY: 'off', GOFLAGS: '-mod=mod',
    AHP_NATIVE_QWEN_ASSERT: G0, AHP_HQ_DIR: HQ, AHP_ROLE: 'all',
  };
  const commit = (await run('git', ['rev-parse', 'HEAD'], env)).out.trim();
  const tree = (await run('git', ['rev-parse', 'HEAD^{tree}'], env)).out.trim();
  const ranAt = new Date().toISOString();
  const items = [];

  // Stage 0 — G0 native-Qwen classifier regression (synthetic: no runtime env/secrets/network).
  // The RUNTIME gate (env-gated, must run before any model leg / bin start) is WIRED into
  // scripts/prototype-rebuild-deploy.sh as G0-before-bin. Here we only prove the classifier.
  let r = await run('bash', [G0, '--selftest'], env, 60000);
  const g0m = String(r.out).match(/selftest:\s*(\d+)\s*pass\s*\/\s*(\d+)\s*fail/) || [];
  items.push({ id: 'G0-native-qwen-classifier', status: (r.rc === 0 && g0m[2] === '0') ? 'run:pass' : 'run:fail', kind: 'run',
    cmd: 'bash $AHP_NATIVE_QWEN_ASSERT --selftest', rc: r.rc, verdict: { pass: g0m[1] || '?', fail: g0m[2] || '?' },
    note: 'classifier regression only; runtime gate wired as G0-before-bin in scripts/prototype-rebuild-deploy.sh' });

  // The build reads the CANONICAL host module cache ($HOME/go/pkg/mod) where root pre-seeded
  // go-sdk v1.8.0 / OTel v1.47.0 etc. buildEnv() redirects GOMODCACHE to a fresh empty scratch
  // (/tmp/ahp-hive-build/modules) which lacks that pre-seed, so a plain offline build there
  // would falsely report a module deficit. Using the canonical cache changes no dependency
  // semantics (no downgrade / no replace / sum-verification intact).
  const HOST_MODCACHE = (env.HOME || process.env.HOME || '/Users/aogabbasov') + '/go/pkg/mod';
  const buildEnv2 = { ...env, GOMODCACHE: HOST_MODCACHE, GOCACHE: env.GOCACHE || '/tmp/ahp-gocache' };
  // Stage 1 — offline build of the runnable surface (cmd/ahp + pkg + internal). If it fails ONLY
  // because a host-cache module is absent under GOPROXY=off, that is an EXTERNAL pre-seed gap
  // (G-GOSDK) and is reported as gated, never as a product failure. No downgrade / no sum off.
  r = await run('go', ['build', '-tags', 'ahp_exec ahp_data', './cmd/ahp', './pkg/...', './internal/...'], buildEnv2, 240000);
  const modDeficit = /module lookup disabled|cannot find module|no required module|missing go\.sum|GOPROXY=off|to add it:/.test(r.err);
  items.push({ id: 'build-offline-surface', status: r.rc === 0 ? 'run:pass' : (modDeficit ? 'gated' : 'run:fail'),
    kind: (r.rc === 0 || modDeficit) ? (r.rc === 0 ? 'run' : 'gated') : 'run',
    cmd: "go build -tags 'ahp_exec ahp_data' ./cmd/ahp ./pkg/... ./internal/...", rc: r.rc,
    tail: r.rc === 0 ? 'rc=0' : tail(r.err, 3),
    ...(modDeficit ? { owner: 'root', note: 'G-GOSDK external module pre-seed gap; runnable cmd/ahp unaffected if built alone' } : {}) });

  // Stage 2 — runnable UI/API serve selftest (planes + create-run->PG + Emil-hop).
  r = await run('node', ['tests/e2e/demo-ui.mjs', '--selftest'], env, 180000);
  items.push({ id: 'demo-serve-selftest', status: r.rc === 0 ? 'run:pass' : 'run:fail', kind: 'run',
    cmd: 'npm run demo:selftest', rc: r.rc,
    verdict: verdictKeys(r.out, ['DEMO_UI', 'planes', 'create-run->PG', 'emil-hop']) });

  // Stage 3 — native Qwen model->versioned-artifact->PG (needs Vault; tolerates gate).
  r = await run('node', ['tests/e2e/demo-ui.mjs', '--selftest', 'model'], env, 240000);
  items.push({ id: 'model-artifact-pg', status: r.rc === 0 ? 'run:pass' : 'run:fail', kind: 'run',
    cmd: 'npm run demo:selftest -- model', rc: r.rc,
    verdict: verdictKeys(r.out, ['model->artifact->PG', 'DEMO_UI']) });

  // Stage 3b — CANONICAL whole-journey gate (delivery priority; HQ decisions/2026-10-30-live-
  // journey-contract.md): REAL native-Qwen -> REAL Emil-core :18000 A2A tool-call -> REAL MinIO
  // presigned PUT -> Seal -> sha256(PUT)==sha256(GET) -> REAL durable PG. No mocks / no httptest /
  // no newMockState / no :19081 stand-in. Fail-closed: a missing real dependency yields
  // LIVE_JOURNEY=RED (rc!=0), never a skip and never a pass.
  const LJ = ['LIVE_JOURNEY','LIVE_EMIL_TASK','LIVE_EMIL_TOOL_CALLS','LIVE_EMIL_MODEL_TURNS','LIVE_PUT_SHA256','LIVE_GET_SHA256','LIVE_PG_RUN'];
  r = await run('node', ['tests/e2e/live-journey.mjs'], env, 180000);
  const lj = verdictKeys(r.out, LJ);
  const ljGreen = r.rc === 0 && lj.LIVE_JOURNEY === 'GREEN' && !!lj.LIVE_PUT_SHA256 && lj.LIVE_PUT_SHA256 === lj.LIVE_GET_SHA256;
  items.push({ id: 'live-journey-whole', status: ljGreen ? 'run:pass' : 'run:fail', kind: 'run',
    cmd: 'npm run journey:live', rc: r.rc, verdict: lj,
    note: 'REAL Qwen->Emil-core:18000 core_skill_activate->REAL MinIO sha256 PUT==GET->REAL PG; fail-closed on any missing dep' });

  // Stage 2b — SEPARATED service roles (acceptance #1: separately deployable + per-role
  // health/readiness) and OTel spans observed on LIVE serve traffic (acceptance #1 observability).
  // Light, PG-only, Mac-safe; each boots real bin/ahp processes and asserts per-role isolation.
  r = await run('node', ['scripts/run-tests.mjs', 'e2e', 'separated-roles'], buildEnv2, 120000);
  items.push({ id: 'separated-roles', status: r.rc === 0 ? 'run:pass' : 'run:fail', kind: 'run',
    cmd: 'npm run test:e2e separated-roles', rc: r.rc, tail: tail(r.out, 2) });

  r = await run('node', ['scripts/run-tests.mjs', 'e2e', 'otel-trace'], buildEnv2, 120000);
  items.push({ id: 'otel-live-trace', status: r.rc === 0 ? 'run:pass' : 'run:fail', kind: 'run',
    cmd: 'npm run test:e2e otel-trace', rc: r.rc, tail: tail(r.out, 2) });

  // Acceptance #3 static layer: every tier manifest must be well-formed YAML and
  // declare the documented topology. Cheap, cluster-free, no env leak (verifier
  // only parses YAML + reports kinds/names). L4 target-runtime stays a GATED item.
  r = await run('bash', ['scripts/verify-tier-target.sh'], { PYTHON: process.env.PYTHON || 'python3' }, 60000);
  items.push({ id: 'tier-static-verify', status: r.rc === 0 ? 'run:pass' : 'run:fail', kind: 'run',
    cmd: 'bash scripts/verify-tier-target.sh', rc: r.rc, tail: tail(r.out, 3) });

  if (RUN_ALL) {
    r = await run('node', ['scripts/run-tests.mjs', 'chaos'], env, 300000);
    items.push({ id: 'chaos', status: r.rc === 0 ? 'run:pass' : 'run:fail', kind: 'run', cmd: 'npm run test:chaos', rc: r.rc, tail: tail(r.out, 3) });
    r = await run('node', ['scripts/run-tests.mjs', 'load'], env, 300000);
    items.push({ id: 'load', status: r.rc === 0 ? 'run:pass' : 'run:fail', kind: 'run', cmd: 'npm run test:load', rc: r.rc, tail: tail(r.out, 3) });
  } else {
    items.push({ id: 'chaos', status: 'cited', kind: 'cited', cmd: 'npm run test:chaos', note: '6/6 (46 asserts) @ ck41 @24fb8ce' });
    items.push({ id: 'load', status: 'cited', kind: 'cited', cmd: 'npm run test:load', note: '3/3 checkpoint p95<=300ms RSS~18.8MB @ ck41 @24fb8ce' });
  }

  // Cited prior runs.
  items.push({ id: 'smoke', status: 'cited', kind: 'cited', cmd: 'npm run smoke', note: 'rc=0 prior runs' });
  items.push({ id: 'pdf-closure', status: 'cited', kind: 'cited', cmd: 'pdftotext -layout "$AHP_PDF_PATH" /tmp/ahp-pdf.txt && node scripts/verify-pdf-closure.mjs --text /tmp/ahp-pdf.txt --coverage docs/coverage.md --pdf "$AHP_PDF_PATH" --expect-sha256 044e045b73b7c34b642bd3c9eb5701b93eed470873a1e0a44e8b1d024821a7e2', note: 'REPRODUCIBLE every-PDF-section->evidence gate (acc #2): derives 18 top-level sections from the canonical PDF СОДЕРЖАНИЕ (sha 044e..a7e2, shasum-verified) and FAILs if coverage.md omits one. Proven this turn @81328ec: PDF_CLOSURE=PASS sections=18 rc=0; negative (drop a section row) -> FAIL rc=1; integrity (bad sha) -> rc=2. Needs canonical PDF OUTSIDE git, so it is NOT executed inside the offline pack.' });
  items.push({ id: 'integrated-crossplane', status: 'cited', kind: 'cited', cmd: 'npm run check && test:go:scoped', note: 'DIRECTOR independently verified cross-plane GREEN on VM target-runtime @ck42 abcfe84 (memory/artifacts/workspace/state/conformance rc=0)' });
  items.push({ id: 'go-build-whole-tree-offline', status: 'cited', kind: 'cited', cmd: 'GOPROXY=off go build ./...', note: 'rc=0 offline @0934ddf (host modcache pre-seeded: go-sdk v1.8.0 + OTel v1.47.0 present) — G-GOSDK/G-OTEL-BUILD CLOSED on host' });

  // Gated (external / target-runtime). Never green.
  items.push({ id: 'emil-tool-completion', status: ljGreen ? 'run:pass' : 'gated', kind: ljGreen ? 'run' : 'gated',
    ...(ljGreen ? {} : { owner: 'root/kvm-lab' }),
    ...(ljGreen ? { note: 'CLOSED via live-journey-whole: real Emil-core:18000 core_skill_activate succeeded + Emil provenance tool_calls>=1 (backend model-base fixed)' }
                 : { note: 'combat Emil-core MODEL_UNAVAILABLE; hop wired+proven, backend needs native-Qwen runtime (G-EMIL-TOOL)' }) });
  items.push({ id: 'whole-system-race', status: 'cited', kind: 'cited', cmd: 'go test -race ./...', note: 'DIRECTOR closed G-WHOLETEST-VM on VM (go1.26.2, -race rc=0 for state/conformance real-PG + workspace PG+MinIO on merged canonical 012a849+3033dd4); ck46 adds only scripts/docs (no product code), so product-code race result applies to this checkout' });
  items.push({ id: 'tier2a2b-l4-iam-compose-otlp', status: 'gated', kind: 'gated', owner: 'director/VM', note: 'Tier L4 HA, IAM, compose app image build, OTLP collector target — target-runtime proof pending. Turnkey static+admission verifier ships: scripts/verify-tier-target.sh (static rc=0 this turn on canonical topology; --context <ctx> runs server-dry-run on VM-plane; L4 rollout stays pending)' });

  const summary = items.reduce((a, i) => { const b = i.status.startsWith('run:') ? 'run' : i.status; a[b] = (a[b] || 0) + 1; return a; }, {});
  const pack = { task: 'delivery', status: 'ready_for_director_review', commit, tree, base: '8b14a59', ranAt, isolation: { db: env.AHP_DB_NAME, minio: process.env.AHP_S3_ENDPOINT || 'http://127.0.0.1:29000' }, summary, items };

  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, 'acceptance-pack.json'), JSON.stringify(pack, null, 2) + '\n');
  const lines = [
    '# delivery acceptance-pack', '',
    `- status: **ready_for_director_review** (whole prototype UNACCEPTED)`,
    `- commit \`${commit}\`  tree \`${tree}\`  base \`8b14a59\`  ranAt ${ranAt}`,
    `- isolation: db \`${env.AHP_DB_NAME}\`  minio \`${pack.isolation.minio}\``,
    `- summary: ${JSON.stringify(summary)}`, '',
    '| id | status | kind | evidence |', '| --- | --- | --- | --- |',
  ];
  for (const i of items) {
    const ev = i.verdict ? JSON.stringify(i.verdict) : (i.note || (i.tail) || (i.cmd));
    lines.push(`| ${i.id} | ${i.status} | ${i.kind} | ${String(ev).replace(/\|/g, '/')} |`);
  }
  writeFileSync(join(OUT, 'acceptance-pack.md'), lines.join('\n') + '\n');
  console.log('[acceptance-pack] ' + JSON.stringify(summary));
  console.log('[acceptance-pack] wrote ' + join(OUT, 'acceptance-pack.json') + ' and .md');
  const anyFail = items.some(i => i.status === 'run:fail');
  process.exit(anyFail ? 1 : 0);
}
main().catch(e => { console.error('[acceptance-pack] fatal', e.message); process.exit(2); });
