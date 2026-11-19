#!/usr/bin/env node
// tests/load/latency.mjs — Measure /healthz control-API latency against a
// self-spawned mTLS server (isolated port + isolated DB). Reports p50/p95/p99.
// Maps to PDF §11 SLO: "доступность control API — 99,9 %" and the ≤50 мс p95
// transition budget (PoC). Latency is MEASURED and reported; the hard gate is
// availability (error-rate), because local Mac != target runtime (no L4 claim).
//
// Optional env: AHP_LOAD_RPS (default 50), AHP_LOAD_DURATION_S (default 5).

import https from 'node:https';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import {
  spawnService,
  pollReady,
  CERT_DIR,
} from '../helpers/ahp-client.mjs';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const PORT = 19617;
const RPS = parseInt(process.env.AHP_LOAD_RPS || '50', 10);
const DURATION_S = parseInt(process.env.AHP_LOAD_DURATION_S || '5', 10);
const TOTAL = RPS * DURATION_S;

const tlsOpts = {
  host: '127.0.0.1',
  port: PORT,
  path: '/healthz',
  ca: readFileSync(join(CERT_DIR, 'ca', 'ca.crt')),
  cert: readFileSync(join(CERT_DIR, 'client', 'client.crt')),
  key: readFileSync(join(CERT_DIR, 'client', 'client.key')),
  servername: 'localhost',
  rejectUnauthorized: true,
  agent: new https.Agent({ keepAlive: true, maxSockets: 32 }),
};

function hit() {
  return new Promise((resolve) => {
    const t0 = performance.now();
    const req = https.get(tlsOpts, (r) => {
      r.resume();
      r.on('end', () => resolve({ ok: r.statusCode === 200, ms: performance.now() - t0 }));
    });
    req.on('error', () => resolve({ ok: false, ms: performance.now() - t0 }));
    req.on('timeout', () => {
      req.destroy();
      resolve({ ok: false, ms: performance.now() - t0 });
    });
    req.setTimeout(5000);
  });
}

const svc = await spawnService({ port: PORT, role: 'all' });
try {
  await pollReady(PORT, { tls: true });
  console.log(`[load] self-spawn mTLS :${PORT}, ${RPS} RPS × ${DURATION_S}s = ${TOTAL} requests`);

  const latencies = [];
  let errors = 0;
  const start = Date.now();
  const tasks = [];
  for (let i = 0; i < TOTAL; i++) {
    tasks.push(
      hit().then((r) => {
        if (r.ok) latencies.push(r.ms);
        else errors++;
      }),
    );
    // pace: one full second's worth of requests, then sleep out the second
    if ((i + 1) % RPS === 0) {
      const targetElapsed = ((i + 1) / RPS) * 1000;
      const wait = targetElapsed - (Date.now() - start);
      if (wait > 0) await sleep(wait);
    }
  }
  await Promise.all(tasks);

  const elapsed = (Date.now() - start) / 1000;
  latencies.sort((a, b) => a - b);
  const pct = (p) =>
    latencies.length === 0
      ? 'N/A'
      : latencies[Math.max(0, Math.ceil((p / 100) * latencies.length) - 1)].toFixed(2);

  console.log(`\n[load] /healthz latency (${elapsed.toFixed(1)}s elapsed):`);
  console.log(`  Total: ${TOTAL}, Success: ${latencies.length}, Errors: ${errors}`);
  console.log(`  p50: ${pct(50)} ms  p95: ${pct(95)} ms  p99: ${pct(99)} ms  max: ${latencies.length ? latencies[latencies.length - 1].toFixed(2) : 'N/A'} ms`);
  console.log(`  RPS achieved: ${(latencies.length / elapsed).toFixed(1)}`);
  console.log(`  PDF §11 SLO: control-API p95 ≤ 50 ms (PoC) — REPORTED (local != target runtime)`);

  if (errors > TOTAL * 0.01) {
    console.error(`\nFAIL: error rate ${((errors / TOTAL) * 100).toFixed(1)}% exceeds 1% (availability gate)`);
    process.exit(1);
  }
  if (tlsOpts.agent) tlsOpts.agent.destroy();
  console.log('\n[load] PASSED (availability ≥99%)');
} finally {
  svc.kill();
  await sleep(500);
}
