#!/usr/bin/env node
// tests/chaos/cancel-delete.mjs — PDF §9.6 (Отмена работы) + §9.8 (lease/эпоха)
// + §9.x (Right-to-be-forgotten vs legalhold).
//
// Covered behaviours:
//  * A worker cancels a run via Release(status=canceled); the run is observably
//    canceled and its lease is dropped (a late Heartbeat on the old fence fails).
//  * DeleteThread removes a thread (hard delete); a subsequent read is 404.
//  * A thread under legalhold cannot be deleted (403) until the hold is removed.
//
// Real mTLS, real lease, block-isolated DB only. Self-spawns the service.

import {
  spawnService, pollReady, tokengen, mState, genClient, uniqueDeployment,
} from '../helpers/ahp-client.mjs';

const PORT = 19613;
const tenant = 'dev';
const dep = uniqueDeployment('cdel');
const certDir = genClient(tenant, dep);
const subject = 'alice';

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
  console.log(`[cancel-delete] service ready (deployment=${dep})`);
  const tok = tokengen({ tenant, deployment: dep, subject, task: 'task-' + dep, ctx: 'ctx-1', grant: 'g-1' });

  // --- Cancel path -------------------------------------------------------
  const th = await mState('CreateThread', { context_id: 'ctx-1', key: 'tk-c-1' }, { port: PORT, certDir, token: tok });
  const threadId = th.json && th.json.id;
  check(th.status === 200 && threadId, `CreateThread -> 200 (got ${th.status})`);

  const run = await mState('CreateRun', { thread_id: threadId, key: 'rk-c-1', payload: { a: 1 } }, { port: PORT, certDir, token: tok });
  const runId = run.json && run.json.id;
  check(run.status === 200 && runId, `CreateRun -> 200 (got ${run.status})`);

  const claim = await mState('ClaimRun', { worker: 'w-1', ttl_ms: 60000 }, { port: PORT, certDir });
  const lease = claim.json && claim.json.lease;
  const wTok = claim.json && claim.json.task_token;
  check(claim.status === 200 && lease, `ClaimRun -> lease (got ${claim.status})`);
  const fence = { run_id: lease.run.id, worker: lease.worker, epoch: lease.epoch };

  const rel = await mState('Release', { run_id: fence.run_id, worker: fence.worker, epoch: fence.epoch, status: 'canceled' }, { port: PORT, certDir, token: wTok });
  check(rel.status === 200, `Release(canceled) -> 200 (got ${rel.status})`);

  const after = await mState('GetRun', { run_id: runId }, { port: PORT, certDir, token: tok });
  check(after.status === 200 && after.json && after.json.status === 'canceled', `GetRun -> status=canceled (got ${after.status} ${after.json && after.json.status})`);

  const hb = await mState('Heartbeat', { run_id: fence.run_id, worker: fence.worker, epoch: fence.epoch, ttl_ms: 30000 }, { port: PORT, certDir, token: wTok });
  check(hb.status === 409 && STALE(hb.json), `Heartbeat on released lease -> 409 (got ${hb.status} ${hb.json && hb.json.code})`);

  // --- Hard delete path --------------------------------------------------
  const del = await mState('DeleteThread', { thread_id: threadId }, { port: PORT, certDir, token: tok });
  check(del.status === 200 && del.json && del.json.ok === true, `DeleteThread -> 200 {ok:true} (got ${del.status})`);

  const gone = await mState('GetThread', { thread_id: threadId }, { port: PORT, certDir, token: tok });
  check(gone.status === 404, `GetThread after delete -> 404 (got ${gone.status})`);

  // --- Legalhold blocks deletion until lifted ----------------------------
  const th2 = await mState('CreateThread', { context_id: 'ctx-1', key: 'tk-c-2' }, { port: PORT, certDir, token: tok });
  const thread2 = th2.json && th2.json.id;
  check(th2.status === 200 && thread2, `CreateThread #2 -> 200 (got ${th2.status})`);

  const hold = await mState('SetLegalhold', { thread_id: thread2, reason: 'litigation' }, { port: PORT, certDir, token: tok });
  check(hold.status === 200, `SetLegalhold -> 200 (got ${hold.status} ${hold.text})`);

  const blocked = await mState('DeleteThread', { thread_id: thread2 }, { port: PORT, certDir, token: tok });
  check(blocked.status === 403, `DeleteThread under legalhold -> 403 (got ${blocked.status} ${blocked.json && blocked.json.code})`);

  const unhold = await mState('RemoveLegalhold', { thread_id: thread2 }, { port: PORT, certDir, token: tok });
  check(unhold.status === 200, `RemoveLegalhold -> 200 (got ${unhold.status})`);

  const del2 = await mState('DeleteThread', { thread_id: thread2 }, { port: PORT, certDir, token: tok });
  check(del2.status === 200, `DeleteThread after removing hold -> 200 (got ${del2.status})`);
} finally {
  svc.kill();
  await new Promise((r) => setTimeout(r, 500));
}

console.log(`\n[cancel-delete] ${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
console.log('chaos/cancel-delete: PASSED');
