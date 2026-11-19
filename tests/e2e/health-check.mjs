#!/usr/bin/env node
// tests/e2e/health-check.mjs — Verifies /healthz and /readyz over the real
// mTLS server (self-spawned on an isolated port + isolated DB). No external
// AHP_API_URL dependency: the test is self-contained and deterministic.
// Maps to PDF §11 "доступность control API" readiness/liveness surface.

import { spawnService, pollReady, httpGet, assert } from '../helpers/ahp-client.mjs';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const PORT = 19616;

const svc = await spawnService({ port: PORT, role: 'all' });
let failures = 0;
try {
  await pollReady(PORT, { tls: true });

  // /readyz must be 200 {"status":"ok"} (pollReady already proved it, but
  // re-fetch to capture the body explicitly).
  const endpoints = [
    { path: '/healthz', expectCode: 200, expectStatus: 'ok' },
    { path: '/readyz', expectCode: 200, expectStatus: 'ok' },
  ];
  for (const ep of endpoints) {
    const res = await httpGet(PORT, ep.path, { tls: true });
    let body = null;
    try {
      body = JSON.parse(res.text);
    } catch {
      /* leave null */
    }
    if (res.status !== ep.expectCode) {
      console.error(`FAIL: ${ep.path} → HTTP ${res.status}, expected ${ep.expectCode}`);
      failures++;
    } else if (!body || body.status !== ep.expectStatus) {
      console.error(`FAIL: ${ep.path} → body=${JSON.stringify(body)}, expected status="${ep.expectStatus}"`);
      failures++;
    } else {
      console.log(`PASS: ${ep.path} → 200 {"status":"ok"}`);
    }
  }

  // /debug/stats must report a live numeric rss_bytes (used by load/rss.mjs).
  const stats = await httpGet(PORT, '/debug/stats', { tls: true });
  const sj = JSON.parse(stats.text);
  assert(stats.status === 200, `/debug/stats status ${stats.status}`);
  assert(typeof sj.rss_bytes === 'number' && sj.rss_bytes > 0, '/debug/stats rss_bytes');
  assert(typeof sj.goroutines === 'number', '/debug/stats goroutines');
  console.log(`PASS: /debug/stats → rss_bytes=${sj.rss_bytes} goroutines=${sj.goroutines} go=${sj.go_version}`);
} finally {
  svc.kill();
  await sleep(500);
}

if (failures > 0) {
  console.error(`\n${failures} endpoint(s) failed.`);
  process.exit(1);
}
console.log('\nAll health endpoints OK (self-spawn mTLS).');
