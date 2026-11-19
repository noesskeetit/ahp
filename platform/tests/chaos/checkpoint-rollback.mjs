#!/usr/bin/env node
// tests/chaos/checkpoint-rollback.mjs — PDF §9.3 «Откат после записи чекпоинтов»
// + §2 durability/immutability guarantee (PDF: «неизменившиеся части не
// переписываются»; «подтверждённая запись переживает отказ узла хранилища»).
//
// Model of an AHPCheckpointSaver snapshot over the State API (PDF 5.x / 1770-1772):
// a checkpoint is an APPEND-ONLY immutable snapshot — a header (id, parent,
// version, metadata) plus versioned part references; part BLOBS are stored
// out-of-line and addressed by (channel, version). Writing a newer checkpoint
// that reuses the same channel MUST NOT overwrite the older part version, so a
// rollback to an earlier checkpoint reads back the earlier bytes verbatim, and
// the whole history survives a hard process crash in the durable store.
//
// This is a HONEST test: it drives the ACCEPTED State API over real mTLS against
// the block-isolated PostgreSQL (ahp_hive_delivery). No mocks, no stand-ins. The
// crash is a real SIGKILL; the re-read is against a freshly re-spawned process on
// the SAME database, proving the checkpoint + its part blobs are durable and
// immutable — the precondition for a verified rollback.
//
// Self-spawns the service; uses a deterministic dev token seed so a read token
// survives the crash. Secrets are never printed.

import {
  spawnService, pollReady, tokengen, mState, genClient, uniqueDeployment,
} from '../helpers/ahp-client.mjs';

const PORT = 19621;
const NS = 'rb'; // checkpoint namespace for this test

// base64 of single-byte payloads — distinct, so an overwrite would be obvious.
const A = Buffer.from('CHECKPOINT-A').toString('base64');
const B = Buffer.from('CHECKPOINT-B').toString('base64');
const C = Buffer.from('CHECKPOINT-C').toString('base64');

const tenant = 'dev';
const dep = uniqueDeployment('rollback');
const certDir = genClient(tenant, dep);
const subject = 'alice';
const task = 'task-' + dep;

let pass = 0;
let fail = 0;
function check(cond, msg) {
  if (cond) { pass++; console.log(`  PASS: ${msg}`); }
  else { console.error(`  FAIL: ${msg}`); fail++; }
}

async function putCheckpoint(fence, wTok, id, parent, version, partVersion, data, key) {
  const cp = {
    id,
    thread_id: threadId,
    namespace: NS,
    ...(parent ? { parent } : {}),
    version,
    parts: [{ channel: 'state', version: partVersion, data }],
  };
  return mState('PutCheckpoint', { fence, checkpoint: cp, key }, { port: PORT, certDir, token: wTok });
}

let threadId = null;
let svc = await spawnService({ port: PORT, role: 'all' });

try {
  await pollReady(PORT, { tls: true });
  console.log(`[checkpoint-rollback] service ready (deployment=${dep})`);

  const tok = tokengen({ tenant, deployment: dep, subject, task, ctx: 'ctx-1', grant: 'g-1' });

  const th = await mState('CreateThread', { context_id: 'ctx-1', key: 'tk-1' }, { port: PORT, certDir, token: tok });
  threadId = th.json && th.json.id;
  check(th.status === 200 && !!threadId, `CreateThread -> 200 (got ${th.status})`);

  const run = await mState('CreateRun', { thread_id: threadId, key: 'rk-1', payload: { a: 1 } }, { port: PORT, certDir, token: tok });
  check(run.status === 200 && run.json && run.json.id, `CreateRun -> 200 (got ${run.status})`);

  // ClaimRun (workload cert) yields the current epoch fence + a bound task token.
  const claim = await mState('ClaimRun', { worker: 'w-1', ttl_ms: 60000 }, { port: PORT, certDir });
  const lease = claim.json && claim.json.lease;
  check(claim.status === 200 && lease && lease.epoch >= 1, `ClaimRun -> lease epoch>=1 (got ${claim.status})`);
  const wTok = claim.json && claim.json.task_token;
  const fence = { run_id: lease.run.id, worker: lease.worker, epoch: lease.epoch };

  // --- cp-a: first immutable snapshot, part state/vA = A -----------------------
  const pa = await putCheckpoint(fence, wTok, 'cp-a', null, 1, 'vA', A, 'rb-a');
  check(pa.status === 200 && pa.json && pa.json.id === 'cp-a', `PutCheckpoint cp-a -> 200 (got ${pa.status})`);

  const ga = await mState('GetCheckpoint', { thread_id: threadId, namespace: NS, id: 'cp-a' }, { port: PORT, certDir, token: wTok });
  const gaPart = ga.json && ga.json.parts && ga.json.parts[0];
  check(ga.status === 200 && ga.json.version === 1 && gaPart && gaPart.channel === 'state' && gaPart.version === 'vA',
    `GetCheckpoint cp-a -> v1, part state/vA descriptor (got ${ga.status})`);

  const pga = await mState('GetPart', { thread_id: threadId, namespace: NS, channel: 'state', version: 'vA' }, { port: PORT, certDir, token: wTok });
  check(pga.status === 200 && pga.json && pga.json.data === A, `GetPart state/vA == A (got ${pga.status})`);

  // --- cp-b: newer snapshot reusing channel 'state' with part version vB -------
  const pb = await putCheckpoint(fence, wTok, 'cp-b', 'cp-a', 2, 'vB', B, 'rb-b');
  check(pb.status === 200 && pb.json && pb.json.id === 'cp-b', `PutCheckpoint cp-b(parent cp-a) -> 200 (got ${pb.status})`);

  const gb = await mState('GetCheckpoint', { thread_id: threadId, namespace: NS, id: 'cp-b' }, { port: PORT, certDir, token: wTok });
  check(gb.status === 200 && gb.json.version === 2 && gb.json.parent === 'cp-a',
    `GetCheckpoint cp-b -> v2, parent cp-a (got ${gb.status} v=${gb.json && gb.json.version} parent=${gb.json && gb.json.parent})`);

  // --- ROLLBACK proof: writing cp-b must NOT touch cp-a's history or part vA ---
  const lst = await mState('ListCheckpoints', { thread_id: threadId, namespace: NS, limit: 50 }, { port: PORT, certDir, token: wTok });
  const items = (lst.json && lst.json.checkpoints) || [];
  const ids = items.map((c) => c.id);
  check(lst.status === 200 && ids.includes('cp-a') && ids.includes('cp-b'),
    `ListCheckpoints preserves both cp-a and cp-b (history not overwritten) (got ${lst.status} ids=${ids.join(',')})`);

  const ga2 = await mState('GetCheckpoint', { thread_id: threadId, namespace: NS, id: 'cp-a' }, { port: PORT, certDir, token: wTok });
  check(ga2.status === 200 && ga2.json && ga2.json.version === 1,
    `ROLLBACK: GetCheckpoint cp-a STILL v1 after cp-b write (got v=${ga2.json && ga2.json.version})`);

  const pga2 = await mState('GetPart', { thread_id: threadId, namespace: NS, channel: 'state', version: 'vA' }, { port: PORT, certDir, token: wTok });
  check(pga2.status === 200 && pga2.json && pga2.json.data === A,
    `ROLLBACK: GetPart state/vA STILL A (unchanged part not rewritten by cp-b) (got ${pga2.status})`);

  const pgb = await mState('GetPart', { thread_id: threadId, namespace: NS, channel: 'state', version: 'vB' }, { port: PORT, certDir, token: wTok });
  check(pgb.status === 200 && pgb.json && pgb.json.data === B, `GetPart state/vB == B (coexists with vA) (got ${pgb.status})`);

  // --- forward continuity: cp-c on the same current fence (version 3) ----------
  const pc = await putCheckpoint(fence, wTok, 'cp-c', 'cp-b', 3, 'vC', C, 'rb-c');
  check(pc.status === 200 && pc.json && pc.json.id === 'cp-c', `PutCheckpoint cp-c(version 3) forward -> 200 (got ${pc.status})`);

  // ================= CRASH: SIGKILL, then durable re-read on SAME DB ===========
  console.log('\n[checkpoint-rollback] Phase: SIGKILL (hard crash), re-read durability');
  svc.proc.kill('SIGKILL');
  await new Promise((r) => (svc.proc.exitCode !== null ? r() : svc.proc.on('exit', r)));
  await new Promise((r) => setTimeout(r, 800));

  // Port must be closed after SIGKILL.
  const dead = await mState('GetCheckpoint', { thread_id: threadId, namespace: NS, id: 'cp-a' }, { port: PORT, certDir, token: tok })
    .then(() => false).catch(() => true);
  check(dead, 'Port closed after SIGKILL (connection refused)');

  // Re-spawn on the SAME isolated database, re-poll readiness.
  svc = await spawnService({ port: PORT, role: 'all' });
  await pollReady(PORT, { tls: true });
  console.log('[checkpoint-rollback] service re-spawned on same DB, ready');

  // Deterministic seed => an equivalent read token across the crash.
  const tok2 = tokengen({ tenant, deployment: dep, subject, task, ctx: 'ctx-1', grant: 'g-1' });

  const lst2 = await mState('ListCheckpoints', { thread_id: threadId, namespace: NS, limit: 50 }, { port: PORT, certDir, token: tok2 });
  const ids2 = ((lst2.json && lst2.json.checkpoints) || []).map((c) => c.id);
  check(lst2.status === 200 && ids2.includes('cp-a') && ids2.includes('cp-b') && ids2.includes('cp-c'),
    `DURABILITY: all 3 checkpoints survived SIGKILL (got ${lst2.status} ids=${ids2.join(',')})`);

  const ga3 = await mState('GetCheckpoint', { thread_id: threadId, namespace: NS, id: 'cp-a' }, { port: PORT, certDir, token: tok2 });
  check(ga3.status === 200 && ga3.json && ga3.json.version === 1 && ga3.json.parent === undefined,
    `DURABILITY: cp-a intact after crash (v1, no parent) (got ${ga3.status})`);

  const pga3 = await mState('GetPart', { thread_id: threadId, namespace: NS, channel: 'state', version: 'vA' }, { port: PORT, certDir, token: tok2 });
  check(pga3.status === 200 && pga3.json && pga3.json.data === A,
    `DURABILITY: GetPart state/vA == A after crash — rollback target bytes durable (got ${pga3.status})`);

  const pgc3 = await mState('GetPart', { thread_id: threadId, namespace: NS, channel: 'state', version: 'vC' }, { port: PORT, certDir, token: tok2 });
  check(pgc3.status === 200 && pgc3.json && pgc3.json.data === C, `DURABILITY: GetPart state/vC == C after crash (got ${pgc3.status})`);
} catch (err) {
  check(false, `unexpected error: ${err && err.message}`);
} finally {
  try { svc.kill(); } catch { /* gone */ }
  await new Promise((r) => setTimeout(r, 500));
}

console.log(`\n[checkpoint-rollback] ${pass} passed, ${fail} failed`);
if (fail > 0) process.exit(1);
console.log('chaos/checkpoint-rollback: PASSED');
