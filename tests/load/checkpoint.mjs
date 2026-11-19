#!/usr/bin/env node
// tests/load/checkpoint.mjs — PDF §11 (SLO/нагрузка): задержка durable-записи
// снимка (PutCheckpoint) под конкурентной нагрузкой.
//
// One leased run supplies a single valid epoch fence; N=OPS PutCheckpoint calls
// (distinct public ids + idempotency keys) are driven at CONCURRENCY parallelism
// against the self-spawned service. We measure per-op latency p50/p95/p99/max and
// throughput. PDF §11 target: "Durable ACK — p95 ≤ 300 мс". Correctness (every
// write returns 200) is a HARD gate; the latency target is REPORTED, not asserted,
// because this is a single-process local measurement on the block-isolated DB —
// it is NOT target-runtime proof (G-CAPACITY remains open).

import {
  spawnService, pollReady, tokengen, mState, genClient, uniqueDeployment,
} from '../helpers/ahp-client.mjs';

const PORT = 19615;
const OPS = parseInt(process.env.AHP_CKPT_OPS || '50', 10);
const CONCURRENCY = parseInt(process.env.AHP_CKPT_CONCURRENCY || '10', 10);
const tenant = 'dev';
const dep = uniqueDeployment('ckpt');
const certDir = genClient(tenant, dep);

const partPayload = Buffer.from(JSON.stringify({ role: 'assistant', content: 'x'.repeat(512) })).toString('base64');

let errors = 0;
const lat = [];

async function putOne(i, fence, wTok, threadId) {
  const body = {
    fence,
    checkpoint: {
      id: `cp-${i}`,
      thread_id: threadId,
      namespace: 'load',
      version: 1,
      parts: [{ channel: 'messages', version: 'v1', data: partPayload }],
    },
    key: `k-${i}`,
  };
  const t0 = performance.now();
  const r = await mState('PutCheckpoint', body, { port: PORT, certDir, token: wTok });
  const dt = performance.now() - t0;
  if (r.status === 200) lat.push(dt);
  else { errors++; console.error(`  op ${i} -> ${r.status} ${r.json && r.json.code}`); }
}

const svc = await spawnService({ port: PORT, role: 'all' });
try {
  await pollReady(PORT, { tls: true });
  console.log(`[checkpoint] service ready (deployment=${dep}) — ${CONCURRENCY} concurrency × ${OPS} PutCheckpoint`);
  const tok = tokengen({ tenant, deployment: dep, subject: 'load', task: 'task-' + dep, ctx: 'ctx-1', grant: 'g-1' });

  const th = await mState('CreateThread', { context_id: 'ctx-1', key: 'tk-ck' }, { port: PORT, certDir, token: tok });
  const threadId = th.json && th.json.id;
  await mState('CreateRun', { thread_id: threadId, key: 'rk-ck', payload: {} }, { port: PORT, certDir, token: tok });
  // Long lease so the fence stays valid for the whole run (no heartbeats needed).
  const claim = await mState('ClaimRun', { worker: 'load', ttl_ms: 120000 }, { port: PORT, certDir });
  const lease = claim.json.lease;
  const wTok = claim.json.task_token;
  const fence = { run_id: lease.run.id, worker: lease.worker, epoch: lease.epoch };

  // Simple worker pool at fixed concurrency.
  let idx = 0;
  const t0 = performance.now();
  async function worker() {
    while (true) {
      const i = idx++;
      if (i >= OPS) return;
      await putOne(i, fence, wTok, threadId);
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));
  const wall = (performance.now() - t0) / 1000;

  // Release the lease.
  await mState('Release', { run_id: fence.run_id, worker: fence.worker, epoch: fence.epoch, status: 'done' }, { port: PORT, certDir, token: wTok });

  lat.sort((a, b) => a - b);
  const pct = (p) => (lat.length ? lat[Math.max(0, Math.ceil((p / 100) * lat.length) - 1)].toFixed(2) : 'N/A');
  const p95 = lat.length ? lat[Math.max(0, Math.ceil(0.95 * lat.length) - 1)] : null;
  const rps = lat.length ? (lat.length / wall).toFixed(1) : '0';

  console.log('\n[checkpoint] Results:');
  console.log(`  ok=${lat.length}  errors=${errors}  wall=${wall.toFixed(2)}s  throughput=${rps} writes/s`);
  console.log(`  latency ms — p50=${pct(50)}  p95=${pct(95)}  p99=${pct(99)}  max=${lat.length ? lat[lat.length - 1].toFixed(2) : 'N/A'}`);
  console.log('\n[checkpoint] PDF §11 SLO: "Durable ACK — p95 ≤ 300 мс"');
  if (p95 !== null) {
    console.log(`  measured PutCheckpoint p95=${p95.toFixed(2)} ms → ${p95 <= 300 ? 'WITHIN (local, informational)' : 'ABOVE local target (informational; not a gate)'} (local single-node, NOT target runtime)`);
  }

  if (errors > 0) {
    console.error(`\n[checkpoint] FAILED: ${errors}/${OPS} writes did not return 200`);
    process.exit(1);
  }
} finally {
  svc.kill();
  await new Promise((r) => setTimeout(r, 500));
}

console.log('\n[checkpoint] all writes durable (200); PASSED');
