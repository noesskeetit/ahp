#!/usr/bin/env node
// tests/chaos/epoch-fence.mjs — PDF §9.2 (Сбои исполнителя) + §9.8 (lease/эпоха).
//
// "Зависший под ожил после перехвата: State API отклоняет его записи и намерения
//  со старой эпохой." A stalled worker that revives after its lease was taken
// over must have its writes rejected with STALE_EPOCH (HTTP 409). After Release
// the lease is gone, so the old fence can never write again.
//
// Real mTLS, real lease, block-isolated DB only. Self-spawns the service.

import {
  spawnService, pollReady, tokengen, mState, genClient, uniqueDeployment,
} from '../helpers/ahp-client.mjs';

const PORT = 19612;
const tenant = 'dev';
const dep = uniqueDeployment('fence');
const certDir = genClient(tenant, dep);
const subject = 'alice';
const task = 'task-' + dep;

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
  console.log(`[epoch-fence] service ready (deployment=${dep})`);
  const tok = tokengen({ tenant, deployment: dep, subject, task, ctx: 'ctx-1', grant: 'g-1' });

  const th = await mState('CreateThread', { context_id: 'ctx-1', key: 'tk-1' }, { port: PORT, certDir, token: tok });
  const threadId = th.json && th.json.id;
  check(th.status === 200 && threadId, `CreateThread -> 200 (got ${th.status})`);

  const run = await mState('CreateRun', { thread_id: threadId, key: 'rk-1', payload: { a: 1 } }, { port: PORT, certDir, token: tok });
  check(run.status === 200 && run.json && run.json.id, `CreateRun -> 200 (got ${run.status})`);

  // ClaimRun (workload cert only) yields the current epoch fence + bound token.
  const claim = await mState('ClaimRun', { worker: 'w-1', ttl_ms: 60000 }, { port: PORT, certDir });
  const lease = claim.json && claim.json.lease;
  check(claim.status === 200 && lease && lease.epoch >= 1, `ClaimRun -> lease epoch>=1 (got ${claim.status})`);
  const wTok = claim.json && claim.json.task_token;
  const fence = { run_id: lease.run.id, worker: lease.worker, epoch: lease.epoch };

  // Correct fence: write succeeds.
  const ok = await mState('PutKV', { fence, namespace: ['ns'], key: 'k', operation_key: 'op-1', expected_version: 0, value: { v: 1 } }, { port: PORT, certDir, token: wTok });
  check(ok.status === 200 && ok.json && ok.json.version === 1, `PutKV current fence -> 200 version=1 (got ${ok.status})`);

  // Zombie worker revives with a FUTURE (stale relative to a newer claim) epoch: rejected.
  const zombie = { run_id: fence.run_id, worker: fence.worker, epoch: fence.epoch + 1000 };
  const z1 = await mState('PutKV', { fence: zombie, namespace: ['ns'], key: 'k', operation_key: 'op-2', expected_version: 1, value: { v: 2 } }, { port: PORT, certDir, token: wTok });
  check(z1.status === 409 && STALE(z1.json), `PutKV stale epoch -> 409 STALE_EPOCH/LEASE_LOST (got ${z1.status} ${z1.json && z1.json.code})`);

  const z2 = await mState('PutCheckpoint', {
    fence: zombie,
    checkpoint: { id: 'cpz', thread_id: threadId, namespace: 'ns', version: 1, parts: [{ channel: 'ch', version: 'v1', data: 'aGk=' }] },
    key: 'cpzk',
  }, { port: PORT, certDir, token: wTok });
  check(z2.status === 409 && STALE(z2.json), `PutCheckpoint stale epoch -> 409 (got ${z2.status} ${z2.json && z2.json.code})`);

  // Legitimate worker releases the run.
  const rel = await mState('Release', { run_id: fence.run_id, worker: fence.worker, epoch: fence.epoch, status: 'done' }, { port: PORT, certDir, token: wTok });
  check(rel.status === 200, `Release(done) with current fence -> 200 (got ${rel.status})`);

  // After release the lease is gone: the OLD fence can never write again.
  const after = await mState('PutKV', { fence, namespace: ['ns'], key: 'k', operation_key: 'op-3', expected_version: 1, value: { v: 3 } }, { port: PORT, certDir, token: wTok });
  check(after.status === 409 && STALE(after.json), `PutKV old fence after Release -> 409 (got ${after.status} ${after.json && after.json.code})`);
} finally {
  svc.kill();
  await new Promise((r) => setTimeout(r, 500));
}

console.log(`\n[epoch-fence] ${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
console.log('chaos/epoch-fence: PASSED');
