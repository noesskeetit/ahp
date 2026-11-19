#!/usr/bin/env node
// tests/chaos/data-isolation.mjs — PDF §9.5 (Изоляция данных).
//
// State KV is partitioned by (tenant, deployment, subject, namespace, key).
// A value written under one subject MUST be invisible (404, not 403) to:
//  * a different subject inside the SAME deployment/tenant; and
//  * the same subject name inside a DIFFERENT deployment.
// 404 (rather than 403) is required so a cross-tenant reader cannot even
//  probe for existence.
//
// Real mTLS with per-deployment SPIFFE certs, real task tokens, isolated DB.

import {
  spawnService, pollReady, tokengen, mState, genClient, uniqueDeployment,
} from '../helpers/ahp-client.mjs';

const PORT = 19614;
const tenant = 'dev';
const dep = uniqueDeployment('iso');
const certDir = genClient(tenant, dep);

let pass = 0;
let fail = 0;
function check(cond, msg) {
  if (cond) { pass++; console.log(`  PASS: ${msg}`); }
  else { fail++; console.error(`  FAIL: ${msg}`); }
}

const svc = await spawnService({ port: PORT, role: 'all' });
try {
  await pollReady(PORT, { tls: true });
  console.log(`[data-isolation] service ready (deployment=${dep})`);
  const aliceTok = tokengen({ tenant, deployment: dep, subject: 'alice', task: 'task-' + dep, ctx: 'ctx-1', grant: 'g-1' });

  // Alice creates a run, claims it (to obtain a write fence bound to her subject).
  const th = await mState('CreateThread', { context_id: 'ctx-1', key: 'tk-iso-1' }, { port: PORT, certDir, token: aliceTok });
  const threadId = th.json && th.json.id;
  check(th.status === 200 && threadId, `CreateThread -> 200 (got ${th.status})`);
  await mState('CreateRun', { thread_id: threadId, key: 'rk-iso-1', payload: { a: 1 } }, { port: PORT, certDir, token: aliceTok });
  const claim = await mState('ClaimRun', { worker: 'w-1', ttl_ms: 60000 }, { port: PORT, certDir });
  const lease = claim.json && claim.json.lease;
  const wTok = claim.json && claim.json.task_token;
  const fence = { run_id: lease.run.id, worker: lease.worker, epoch: lease.epoch };

  const put = await mState('PutKV', { fence, namespace: ['ns'], key: 'secret', operation_key: 'op-iso', expected_version: 0, value: { top: 'secret' } }, { port: PORT, certDir, token: wTok });
  check(put.status === 200 && put.json && put.json.version === 1, `alice PutKV -> 200 version=1 (got ${put.status})`);

  // Alice can read her own value (sanity).
  const self = await mState('GetKV', { namespace: ['ns'], key: 'secret' }, { port: PORT, certDir, token: aliceTok });
  check(self.status === 200, `alice GetKV own value -> 200 (got ${self.status})`);

  // Bob, same tenant+deployment (same workload cert), different subject: invisible.
  const bobTok = tokengen({ tenant, deployment: dep, subject: 'bob', task: 'task-bob-' + dep, ctx: 'ctx-1', grant: 'g-1' });
  const bob = await mState('GetKV', { namespace: ['ns'], key: 'secret' }, { port: PORT, certDir, token: bobTok });
  check(bob.status === 404, `bob (same deploy, other subject) GetKV -> 404 invisible (got ${bob.status} ${bob.json && bob.json.code})`);

  // Same subject name, different deployment (different SPIFFE cert): invisible.
  const otherDep = uniqueDeployment('other');
  const otherCert = genClient(tenant, otherDep);
  const otherTok = tokengen({ tenant, deployment: otherDep, subject: 'alice', task: 'task-' + otherDep, ctx: 'ctx-1', grant: 'g-1' });
  const other = await mState('GetKV', { namespace: ['ns'], key: 'secret' }, { port: PORT, certDir: otherCert, token: otherTok });
  check(other.status === 404, `alice in other deployment GetKV -> 404 invisible (got ${other.status} ${other.json && other.json.code})`);
} finally {
  svc.kill();
  await new Promise((r) => setTimeout(r, 500));
}

console.log(`\n[data-isolation] ${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
console.log('chaos/data-isolation: PASSED');
