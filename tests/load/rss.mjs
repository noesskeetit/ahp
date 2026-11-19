#!/usr/bin/env node
// tests/load/rss.mjs — Measure resident memory of a self-spawned ahp service
// via its own /debug/stats (peak RSS via getrusage). Works in hardened
// sandboxes where `ps` is not executable. Maps to PDF §11 capacity/footprint
// measurement; local Mac value is REPORTED (not a target-runtime L4 claim).
//
// Threshold via AHP_RSS_THRESHOLD_MB (default 256).

import { spawnService, pollReady, httpGet } from '../helpers/ahp-client.mjs';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const PORT = 19618;
const THRESHOLD_MB = parseInt(process.env.AHP_RSS_THRESHOLD_MB || '256', 10);

const svc = await spawnService({ port: PORT, role: 'all' });
try {
  await pollReady(PORT, { tls: true });
  console.log(`[rss] self-spawn mTLS :${PORT}, sampling /debug/stats`);

  let peak = 0;
  let meta = null;
  for (let i = 0; i < 3; i++) {
    const res = await httpGet(PORT, '/debug/stats', { tls: true });
    if (res.status === 200) {
      const j = JSON.parse(res.text);
      if (typeof j.rss_bytes === 'number') {
        peak = Math.max(peak, j.rss_bytes);
        meta = j;
      }
    }
    await sleep(150);
  }
  if (peak <= 0) {
    console.error('FAIL: /debug/stats never returned rss_bytes');
    process.exit(1);
  }
  const rssMB = peak / (1024 * 1024);
  console.log(`  RSS peak: ${rssMB.toFixed(1)} MB (${peak} bytes) via /debug/stats (service=${meta?.service}, goroutines=${meta?.goroutines}, go=${meta?.go_version})`);
  console.log(`  Threshold: ${THRESHOLD_MB} MB`);
  console.log(`  PDF §11 footprint measurement — REPORTED (local != target runtime)`);
  if (rssMB > THRESHOLD_MB) {
    console.error(`FAIL: RSS ${rssMB.toFixed(1)} MB exceeds threshold ${THRESHOLD_MB} MB`);
    process.exit(1);
  }
  console.log('\n[rss] PASSED');
} finally {
  svc.kill();
  await sleep(500);
}
