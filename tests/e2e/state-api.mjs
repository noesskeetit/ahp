#!/usr/bin/env node
// tests/e2e/state-api.mjs — State API end-to-end conformance over REAL mTLS.
//
// Covers PDF §6 (common contracts) and §9.1 (idempotency): a full task lifecycle
// CreateThread -> CreateRun -> ClaimRun (workload identity) -> Heartbeat ->
// PutKV -> GetKV -> idempotent replay -> checkpoint round-trip -> Release, plus
// transport negatives (missing token -> 403, unknown op -> 404, idempotency-key
// reuse with a different body -> 400).
//
// Isolation: every run uses a unique SPIFFE deployment + a freshly minted client
// cert, so ClaimRun (scoped by tenant+deployment) can never grab a neighbour
// test's run in the shared block database. Writes go only to the block DB.

import {
  spawnService, pollReady, tokengen, mState, genClient, uniqueDeployment,
} from '../helpers/ahp-client.mjs';

const PORT = 19611;
const tenant = 'dev';
const dep = uniqueDeployment('e2e');
const certDir = genClient(tenant, dep);
const subject = 'alice';
const task = 'task-' + dep;

let pass = 0;
let fail = 0;
function check(cond, msg) {
  if (cond) {
    pass++;
    console.log(`  PASS: ${msg}`);
  } else {
    fail++;
    console.error(`  FAIL: ${msg}`);
  }
}

const svc = await spawnService({ port: PORT, role: 'all' });
try {
  await pollReady(PORT, { tls: true });
  console.log(`[state-api] service ready (deployment=${dep})`);

  const tok = tokengen({ tenant, deployment: dep, subject, task, ctx: 'ctx-1', grant: 'g-1' });

  // --- Transport negatives -------------------------------------------------
  const noTok = await mState('CreateThread', { context_id: 'c1', key: 'k1' }, { port: PORT, certDir });
  check(noTok.status === 403, `CreateThread without bearer token -> 403 (got ${noTok.status})`);

  const badOp = await mState('NoSuchOp', {}, { port: PORT, certDir, token: tok });
  check(badOp.status === 404, `Unknown op -> 404 (got ${badOp.status})`);

  // GET is not an allowed verb on the state router -> 404 (POST-only).
  // (covered by the unknown-op branch; the router never registers non-POST.)

  // --- CreateThread --------------------------------------------------------
  const th = await mState('CreateThread', { context_id: 'ctx-1', key: 'tkey-1' }, { port: PORT, certDir, token: tok });
  check(th.status === 200 && th.json && th.json.id, `CreateThread -> 200 with id (got ${th.status})`);
  const threadId = th.json && th.json.id;

  // Idempotent thread replay: same (scope, key) returns the same thread id.
  const th2 = await mState('CreateThread', { context_id: 'ctx-1', key: 'tkey-1' }, { port: PORT, certDir, token: tok });
  check(th2.status === 200 && th2.json && th2.json.id === threadId, `CreateThread replay same key -> same id (got ${th2.status})`);

  // --- CreateRun -----------------------------------------------------------
  const run = await mState('CreateRun', { thread_id: threadId, key: 'rkey-1', payload: { hello: 'world' } }, { port: PORT, certDir, token: tok });
  check(run.status === 200 && run.json && run.json.id, `CreateRun -> 200 with id (got ${run.status})`);
  const runId = run.json && run.json.id;

  // --- ClaimRun (workload identity: cert only, NO bearer token) ------------
  const claim = await mState('ClaimRun', { worker: 'w-1', ttl_ms: 60000 }, { port: PORT, certDir });
  const lease = claim.json && claim.json.lease;
  check(claim.status === 200 && lease && lease.run.id === runId, `ClaimRun (workload cert, no token) -> 200 with matching run (got ${claim.status})`);
  check(lease && typeof lease.epoch === 'number' && lease.epoch >= 1, `Lease carries an epoch (epoch=${lease && lease.epoch})`);
  const wTok = claim.json && claim.json.task_token;
  check(!!wTok, `ClaimRun returns a bound task_token (len=${wTok ? wTok.length : 0})`);

  const fence = { run_id: lease.run.id, worker: lease.worker, epoch: lease.epoch };

  // --- Heartbeat -----------------------------------------------------------
  const hb = await mState('Heartbeat', { run_id: fence.run_id, worker: fence.worker, epoch: fence.epoch, ttl_ms: 60000 }, { port: PORT, certDir, token: wTok });
  check(hb.status === 200 && hb.json && hb.json.expires_at, `Heartbeat -> 200 with expires_at (got ${hb.status})`);

  // --- PutKV / GetKV -------------------------------------------------------
  const pkv = await mState('PutKV', { fence, namespace: ['ns1'], key: 'k1', operation_key: 'op-1', expected_version: 0, value: { v: 1 } }, { port: PORT, certDir, token: wTok });
  check(pkv.status === 200 && pkv.json && pkv.json.version === 1 && pkv.json.value && pkv.json.value.v === 1, `PutKV create -> 200 version=1 (got ${pkv.status})`);

  const gkv = await mState('GetKV', { namespace: ['ns1'], key: 'k1' }, { port: PORT, certDir, token: wTok });
  check(gkv.status === 200 && gkv.json && gkv.json.value && gkv.json.value.v === 1, `GetKV -> 200 returns stored value (got ${gkv.status})`);

  // --- §9.1 Idempotency: replay of the identical request -------------------
  const replay = await mState('PutKV', { fence, namespace: ['ns1'], key: 'k1', operation_key: 'op-1', expected_version: 0, value: { v: 1 } }, { port: PORT, certDir, token: wTok });
  check(replay.status === 200 && replay.json && replay.json.version === 1, `PutKV replay same operation_key -> 200 same version, no new version (got ${replay.status} v=${replay.json && replay.json.version})`);

  // Reusing the operation_key with a DIFFERENT body must be rejected (§9.1).
  const reuse = await mState('PutKV', { fence, namespace: ['ns1'], key: 'k1', operation_key: 'op-1', expected_version: 0, value: { v: 999 } }, { port: PORT, certDir, token: wTok });
  check(reuse.status === 400, `PutKV reuse operation_key with different body -> 400 KEY_REUSE (got ${reuse.status})`);

  // --- Checkpoint round-trip ----------------------------------------------
  const cp = { id: 'cp-1', thread_id: threadId, namespace: 'ns1', version: 1, parts: [{ channel: 'ch', version: 'v1', data: 'aGVsbG8=' }] };
  const pcp = await mState('PutCheckpoint', { fence, checkpoint: cp, key: 'cpk-1' }, { port: PORT, certDir, token: wTok });
  check(pcp.status === 200 && pcp.json && pcp.json.id === 'cp-1', `PutCheckpoint -> 200 (got ${pcp.status})`);

  // Checkpoint part DATA is stored out-of-line; GetCheckpoint returns the part
  // descriptor (channel/version) and GetPart fetches the payload by channel+version.
  const gcp = await mState('GetCheckpoint', { thread_id: threadId, namespace: 'ns1', id: 'cp-1' }, { port: PORT, certDir, token: wTok });
  const desc = gcp.json && gcp.json.parts && gcp.json.parts[0];
  check(gcp.status === 200 && desc && desc.channel === 'ch' && desc.version === 'v1', `GetCheckpoint -> 200 part descriptor round-trips (got ${gcp.status})`);

  const gpart = await mState('GetPart', { thread_id: threadId, namespace: 'ns1', channel: 'ch', version: 'v1' }, { port: PORT, certDir, token: wTok });
  check(gpart.status === 200 && gpart.json && gpart.json.data === 'aGVsbG8=', `GetPart -> 200 part data round-trips (got ${gpart.status})`);

  // --- Release -------------------------------------------------------------
  const rel = await mState('Release', { run_id: fence.run_id, worker: fence.worker, epoch: fence.epoch, status: 'done' }, { port: PORT, certDir, token: wTok });
  check(rel.status === 200, `Release(done) -> 200 (got ${rel.status})`);
} finally {
  svc.kill();
  await new Promise((r) => setTimeout(r, 500));
}

console.log(`\n[state-api] ${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
console.log('e2e/state-api: PASSED');
