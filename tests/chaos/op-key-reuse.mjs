#!/usr/bin/env node
// tests/chaos/op-key-reuse.mjs — PDF §2.4 (ключ операции: digest, KEY_REUSE_MISMATCH,
// Get/Fence) + §9 (детерминированная отмена повтора).
//
// Идемпотентный ключ операции (operation_key) привязан к digest запроса. Повтор
// с ТЕМ ЖЕ ключом и ТЕМ ЖЕ payload = идемпотентный replay (без дубля версии).
// Повтор с ТЕМ ЖЕ ключом и ДРУГИМ payload = KEY_REUSE_MISMATCH (отклонён), и
// original-значение НЕ портится (corruption impossible). Фence проверяется раньше
// идемпотентности, поэтому «зомби»-раннер со старой эпохой получает STALE_EPOCH,
// а не может зондировать чужой op-key.
//
// Real mTLS, real lease, block-isolated DB only. Self-spawns the service.
//
// NOTE (cross-cutting finding, NOT delivery-owned): pkg/transport/json.go maps
// contracts.ErrKeyReuse to HTTP 400 with code=INVALID (message still carries
// KEY_REUSE_MISMATCH). We assert on the wire-observed behaviour (400 + the code
// text in the body), so this test stays truthful to the real transport layer.

import {
  spawnService, pollReady, tokengen, mState, genClient, uniqueDeployment,
} from '../helpers/ahp-client.mjs';

const PORT = 19617;
const tenant = 'dev';
const dep = uniqueDeployment('opkey');
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
const KEY_REUSE = (r) => r.status === 400 && /KEY_REUSE_MISMATCH/.test(r.text || '');

const svc = await spawnService({ port: PORT, role: 'all' });
try {
  await pollReady(PORT, { tls: true });
  console.log(`[op-key-reuse] service ready (deployment=${dep})`);
  const tok = tokengen({ tenant, deployment: dep, subject, task, ctx: 'ctx-1', grant: 'g-1' });

  const th = await mState('CreateThread', { context_id: 'ctx-1', key: 'tk-1' }, { port: PORT, certDir, token: tok });
  const threadId = th.json && th.json.id;
  check(th.status === 200 && threadId, `CreateThread -> 200 (got ${th.status})`);

  const run = await mState('CreateRun', { thread_id: threadId, key: 'rk-1', payload: { a: 1 } }, { port: PORT, certDir, token: tok });
  check(run.status === 200 && run.json && run.json.id, `CreateRun -> 200 (got ${run.status})`);

  const claim = await mState('ClaimRun', { worker: 'w-1', ttl_ms: 60000 }, { port: PORT, certDir });
  const lease = claim.json && claim.json.lease;
  const wTok = claim.json && claim.json.task_token;
  const fence = { run_id: lease.run.id, worker: lease.worker, epoch: lease.epoch };
  check(claim.status === 200 && lease && lease.epoch >= 1, `ClaimRun -> lease epoch>=1 (got ${claim.status})`);

  // (1) First write under op-1.
  const v1 = { v: 1 };
  const put1 = await mState('PutKV', { fence, namespace: ['ns'], key: 'k', operation_key: 'op-1', expected_version: 0, value: v1 }, { port: PORT, certDir, token: wTok });
  check(put1.status === 200 && put1.json && put1.json.version === 1, `PutKV op-1 (first) -> 200 version=1 (got ${put1.status})`);

  // (2) Idempotent replay: SAME op-key, SAME payload -> same version, no duplicate write.
  const replay = await mState('PutKV', { fence, namespace: ['ns'], key: 'k', operation_key: 'op-1', expected_version: 0, value: v1 }, { port: PORT, certDir, token: wTok });
  check(replay.status === 200 && replay.json && replay.json.version === 1, `PutKV op-1 idempotent replay (same digest) -> 200 version=1 (no dup) (got ${replay.status} ${replay.json && replay.json.version})`);

  // (3) KEY REUSE: SAME op-key, DIFFERENT payload -> rejected (digest mismatch).
  const reuse = await mState('PutKV', { fence, namespace: ['ns'], key: 'k', operation_key: 'op-1', expected_version: 0, value: { v: 999 } }, { port: PORT, certDir, token: wTok });
  check(KEY_REUSE(reuse), `PutKV op-1 with DIFFERENT payload -> KEY_REUSE_MISMATCH (400, body carries code) (got ${reuse.status} ${reuse.json && reuse.json.code})`);

  // (4) Corruption impossible: original value still {v:1}; the reused 999 never landed.
  const get = await mState('GetKV', { namespace: ['ns'], key: 'k' }, { port: PORT, certDir, token: wTok });
  const gt = get.text || '';
  check(get.status === 200 && /"v":\s*1/.test(gt) && !/999/.test(gt), `GetKV returns original {v:1}, reused {v:999} NOT persisted (got ${get.status})`);

  // (5) A DISTINCT op-key still writes (reuse guard is per-key, not global).
  const put2 = await mState('PutKV', { fence, namespace: ['ns'], key: 'k', operation_key: 'op-2', expected_version: 1, value: { v: 2 } }, { port: PORT, certDir, token: wTok });
  check(put2.status === 200 && put2.json && put2.json.version === 2, `PutKV distinct op-2 -> 200 version=2 (reuse guard is per-key) (got ${put2.status} ${put2.json && put2.json.version})`);

  // (6) Fence is checked BEFORE idempotency: a stale-epoch writer probing a reused
  // op-key gets STALE_EPOCH (409), so it cannot probe/detect another writer's key.
  const zombie = { run_id: fence.run_id, worker: fence.worker, epoch: fence.epoch + 1000 };
  const probe = await mState('PutKV', { fence: zombie, namespace: ['ns'], key: 'k', operation_key: 'op-1', expected_version: 2, value: { v: 777 } }, { port: PORT, certDir, token: wTok });
  check(probe.status === 409 && STALE(probe.json), `stale-epoch probe of reused op-1 -> 409 STALE_EPOCH (fence before idempotency) (got ${probe.status} ${probe.json && probe.json.code})`);

  const rel = await mState('Release', { run_id: fence.run_id, worker: fence.worker, epoch: fence.epoch, status: 'done' }, { port: PORT, certDir, token: wTok });
  check(rel.status === 200, `Release(done) with current fence -> 200 (got ${rel.status})`);
} finally {
  svc.kill();
  await new Promise((r) => setTimeout(r, 500));
}

console.log(`\n[op-key-reuse] ${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
console.log('chaos/op-key-reuse: PASSED');
