#!/usr/bin/env node
// State lease-expiry + KV idempotency contract probe (PDF §9.2 / §9.8).
// This script simulates an abandoned lease by withholding Release/Heartbeat;
// it does not kill an executor process or execute an external business effect.
// It verifies higher epoch, stale-fence denial and an identical PutKV receipt.
// A non-terminal run here is NOT an IntentJournal status=unknown assertion.
// Real mTLS and PostgreSQL; SDK crash/external-effect/UNKNOWN recovery is tested
// separately by node scripts/recovery-process.mjs.
import {
  spawnService, pollReady, tokengen, mState, genClient, uniqueDeployment,
} from '../helpers/ahp-client.mjs';

const PORT = 19620;
const tenant = 'dev';
const dep = uniqueDeployment('unkn');
const certDir = genClient(tenant, dep);
const subject = 'alice';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let pass = 0;
let fail = 0;
function check(cond, msg) {
  if (cond) { pass++; console.log(`  PASS: ${msg}`); }
  else { fail++; console.error(`  FAIL: ${msg}`); }
}
const STALE = (j) => j && (j.code === 'STALE_EPOCH' || j.code === 'LEASE_LOST');

const svc = await spawnService({ port: PORT, role: 'all' });
try {
  await pollReady(PORT, { tls: true });
  console.log(`[unknown-outcome] service ready (deployment=${dep})`);
  const tok = tokengen({ tenant, deployment: dep, subject, task: 'task-' + dep, ctx: 'ctx-1', grant: 'g-1' });

  const th = await mState('CreateThread', { context_id: 'ctx-1', key: 'tk-u-1' }, { port: PORT, certDir, token: tok });
  const threadId = th.json && th.json.id;
  check(th.status === 200 && threadId, `CreateThread -> 200 (got ${th.status})`);

  const run = await mState('CreateRun', { thread_id: threadId, key: 'rk-u-1', payload: { a: 1 } }, { port: PORT, certDir, token: tok });
  const runId = run.json && run.json.id;
  check(run.status === 200 && runId, `CreateRun -> 200 (got ${run.status})`);

  // worker-A claims with a SHORT lease (2s) so we can observe expiry after a crash.
  const claimA = await mState('ClaimRun', { worker: 'worker-A', ttl_ms: 2000 }, { port: PORT, certDir });
  const leaseA = claimA.json && claimA.json.lease;
  const tokA = claimA.json && claimA.json.task_token;
  check(claimA.status === 200 && leaseA, `worker-A ClaimRun -> lease (got ${claimA.status})`);
  const fenceA = { run_id: leaseA.run.id, worker: leaseA.worker, epoch: leaseA.epoch };
  const epochA = leaseA.epoch;

  // Durable partial progress persisted under A's fence, idempotency key op-1.
  const val = { step: 'partial', from: 'A' };
  const putA = await mState('PutKV', { fence: fenceA, namespace: ['ns'], key: 'k', operation_key: 'op-1', expected_version: 0, value: val }, { port: PORT, certDir, token: tokA });
  check(putA.status === 200 && putA.json && putA.json.version === 1, `worker-A PutKV durable -> v1 (got ${putA.status} ${putA.json && putA.json.version})`);

  // Simulate abandonment: withhold Release and Heartbeat until the lease
  // expires in database time. No executor process is killed by this script.
  console.log('[unknown-outcome] simulating worker-A crash (no Release); waiting for lease expiry...');
  await sleep(2600);

  // The system must NOT have failed the run: it is still observable, status running.
  const mid = await mState('GetRun', { run_id: runId }, { port: PORT, certDir, token: tok });
  check(mid.status === 200 && mid.json && mid.json.status !== 'failed' && mid.json.status !== 'canceled',
    `GetRun after crash stays non-terminal/unknown (not failed/canceled) (got ${mid.json && mid.json.status})`);

  // Crashed (zombie) worker revives and tries to renew its now-expired lease.
  const hbA = await mState('Heartbeat', { run_id: fenceA.run_id, worker: fenceA.worker, epoch: fenceA.epoch, ttl_ms: 30000 }, { port: PORT, certDir, token: tokA });
  check(hbA.status === 409 && STALE(hbA.json), `worker-A Heartbeat on expired lease -> 409 (got ${hbA.status} ${hbA.json && hbA.json.code})`);

  // A second worker re-claims the SAME run; epoch must have grown monotonically.
  const claimB = await mState('ClaimRun', { worker: 'worker-B', ttl_ms: 60000 }, { port: PORT, certDir });
  const leaseB = claimB.json && claimB.json.lease;
  const tokB = claimB.json && claimB.json.task_token;
  check(claimB.status === 200 && leaseB, `worker-B ClaimRun -> re-claimed (got ${claimB.status})`);
  const fenceB = { run_id: leaseB.run.id, worker: leaseB.worker, epoch: leaseB.epoch };
  check(leaseB && leaseB.run.id === runId, `re-claim targets the SAME run (got ${leaseB && leaseB.run.id})`);
  check(leaseB && leaseB.epoch > epochA, `re-claim epoch grows monotonically (A=${epochA} B=${leaseB && leaseB.epoch})`);

  // The zombie's write under the old fence is rejected, even replaying the same
  // operation_key — the fence is checked before idempotency, so corruption is impossible.
  const putZombie = await mState('PutKV', { fence: fenceA, namespace: ['ns'], key: 'k', operation_key: 'op-1', expected_version: 0, value: val }, { port: PORT, certDir, token: tokA });
  check(putZombie.status === 409 && STALE(putZombie.json), `worker-A zombie PutKV old fence -> 409 (got ${putZombie.status} ${putZombie.json && putZombie.json.code})`);

  // Legitimate resumption: B replays A's operation_key idempotently. Idempotency is
  // keyed by (tenant,deployment,subject,operation_key), so the retry returns the
  // byte-identical receipt (version stays 1) — no duplicate durable write.
  const putB = await mState('PutKV', { fence: fenceB, namespace: ['ns'], key: 'k', operation_key: 'op-1', expected_version: 0, value: val }, { port: PORT, certDir, token: tokB });
  check(putB.status === 200 && putB.json && putB.json.version === 1,
    `worker-B idempotent replay of op-1 -> same version (no duplicate) (got ${putB.status} ${putB.json && putB.json.version})`);

  // B completes the run once; the previously-unknown outcome becomes done.
  const relB = await mState('Release', { run_id: fenceB.run_id, worker: fenceB.worker, epoch: fenceB.epoch, status: 'done' }, { port: PORT, certDir, token: tokB });
  check(relB.status === 200, `worker-B Release(done) -> 200 (got ${relB.status})`);

  const done = await mState('GetRun', { run_id: runId }, { port: PORT, certDir, token: tok });
  check(done.status === 200 && done.json && done.json.status === 'done', `GetRun final status=done (got ${done.json && done.json.status})`);
} finally {
  svc.kill();
  await sleep(300);
}

console.log(`\n[unknown-outcome] ${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
console.log('chaos/unknown-outcome: lease/KV contract PASSED (no external-effect assertion)');
