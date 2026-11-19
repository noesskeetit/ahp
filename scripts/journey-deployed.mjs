#!/usr/bin/env node
// scripts/journey-deployed.mjs — AUTHORIZED end-to-end user journey against the
// PERSISTENT composed all-planes backend (delivery-owned deployment), NOT a
// self-spawned harness. Single script path a customer/root/iv runs against the
// durable URL to prove the WHOLE journey on ONE deployed revision:
//   mint client-cert + Bearer task token (shared DEV seed) ->
//   POST /a2a/public a2a message/send (REAL native-Qwen executor emits write_artifact
//     with model-generated nonce+marker bytes) -> poll tasks/get ->
//   /v1/artifacts Download {id} same principal -> sha256(GET)==sha256 recorded, bytes
//     SEALED into REAL MinIO carry the marker -> foreign tenant cannot see task/artifact.
// Guards (fail-closed, no fabrication): targets DEPLOYED front only (no spawn); loopback;
//   rejects stand-in :19081 / Emil-ref :18000/:19080; forbids mock; canned fixture (no
//   nonce/marker) FAILS (iv finding #2). Secrets silent via buildEnv; redacted.
// author != verifier: AUTHORITATIVE green is iv's independent run of this script.
// Usage: AHP_DEPLOYED_URL=https://127.0.0.1:29612 node scripts/journey-deployed.mjs
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync, existsSync } from 'node:fs';
import https from 'node:https';
import { buildEnv } from './ahp-env.mjs';
import { genClient, uniqueDeployment, certPaths, clientIdentity, canMint, CLIENT_CERT_DIR } from '../tests/helpers/ahp-client.mjs';

const ROOT = process.cwd();
const RED = (s) => String(s).replace(/[A-Za-z0-9._+/=-]{24,}/g, '<redacted>');
const HEX64 = /^[0-9a-f]{64}$/;
// A2A proto enum names and the short spellings both occur on the wire; accept either.
const DONE_STATES = ['completed', 'failed', 'canceled', 'rejected', 'TASK_STATE_COMPLETED', 'TASK_STATE_FAILED', 'TASK_STATE_CANCELED', 'TASK_STATE_REJECTED'];
const sha256hex = (b) => createHash('sha256').update(b).digest('hex');
const URL_ = (process.env.AHP_DEPLOYED_URL || 'https://127.0.0.1:29612').replace(/\/$/, '');
const u = new URL(URL_);
const PORT = Number(u.port || (u.protocol === 'https:' ? 443 : 80));
const HOST = u.hostname;
if (u.protocol !== 'https:') { console.error('journey front must be mTLS https'); process.exit(2); }
if (PORT === 19081 || PORT === 18000 || PORT === 19080) { console.error(`refusing stand-in/Emil-ref port ${PORT}`); process.exit(2); }
if (process.env.AHP_E2E_ALLOW_MOCK) { console.error('mock mode forbidden'); process.exit(2); }
// A pinned acceptance certificate carries one identity; the run must speak AS that
// identity (tokens minted to match) instead of inventing a per-run deployment it cannot
// certify. Without a pinned cert we mint per-run as before.
const PINNED = CLIENT_CERT_DIR ? clientIdentity(CLIENT_CERT_DIR) : null;
const TENANT = process.env.AHP_DEPLOYED_TENANT || (PINNED ? PINNED.tenant : 'djourney');
const env0 = await buildEnv(ROOT);
const seed = process.env.AHP_DEV_JWT_SEED || createHash('sha256').update('ahp-hive-delivery-dev-jwt-seed-v1').digest('hex');
function tokengen(o) {
  return execFileSync('go', ['run', './scripts/tokengen', '-kid', 'dev-1', '-tenant', o.tenant,
    '-deployment', o.deployment, '-subject', o.subject || 'journey', '-audience', 'ahp', '-kind', (o.kind || 'task'),
    ...(o.ctx ? ['-ctx', o.ctx] : []), '-ttl', '900'],
    { cwd: ROOT, encoding: 'utf8', env: { ...process.env, GOCACHE: '/tmp/ahp-gocache', GOMODCACHE: '/tmp/ahp-modcache', GOPATH: '/tmp/ahp-gopath', GOPROXY: 'off', GOFLAGS: '-mod=mod', AHP_DEV_JWT_SEED: seed } }).trim();
}
function req(method, path, body, o = {}, timeoutMs = 120000) {
  const p = certPaths(o.certDir ?? 'client');
  const headers = { 'content-type': 'application/json' };
  if (o.token) headers.authorization = `Bearer ${o.token}`;
  const payload = body === undefined ? '' : (typeof body === 'string' ? body : JSON.stringify(body));
  if (payload) headers['content-length'] = Buffer.byteLength(payload);
  const options = { host: HOST, port: PORT, method, path, servername: 'localhost', rejectUnauthorized: true, headers, timeout: timeoutMs,
    cert: readFileSync(p.cert), key: readFileSync(p.key) };
  if (existsSync(p.ca)) options.ca = readFileSync(p.ca);
  return new Promise((res, rej) => {
    const r = https.request(options, (rs) => { const chunks = []; rs.on('data', (d) => chunks.push(d));
      rs.on('end', () => { const buf = Buffer.concat(chunks); let json = null; try { json = buf.length ? JSON.parse(buf.toString('utf8')) : null; } catch {} res({ status: rs.statusCode, buf, buffer: buf, text: buf.toString('utf8'), json }); }); });
    r.on('error', rej); r.on('timeout', () => r.destroy(new Error('timeout ' + path)));
    if (payload) r.write(payload); r.end();
  });
}
const a2a = (method, params, o) => req('POST', '/a2a/public', { jsonrpc: '2.0', id: Date.now() % 1e9, method, params }, o);
const mArtifacts = (op, body, o) => req('POST', `/v1/artifacts/${op}`, body, o);
function harvest(task) {
  const hexes = new Set(); let artifactID = ''; let toolCalls = 0;
  let artifactCount = 0; let steps = 0; let closeReason = ''; let recordedSha = '';
  // DEF-H9-4: strings must be examined BEFORE the non-object bail, otherwise no hex is ever seen.
  (function walk(x) { if (x === null || x === undefined) return;
    if (typeof x === 'string') { if (HEX64.test(x)) hexes.add(x); return; }
    if (typeof x !== 'object') return;
    for (const [k, v] of Object.entries(x)) { if ((k === 'artifact_id' || k === 'artifactId') && typeof v === 'string' && !artifactID) artifactID = v;
      if (k === 'adk_type' && v === 'function_call') toolCalls++;
      if (k === 'sha256' && typeof v === 'string' && HEX64.test(v) && !recordedSha) recordedSha = v;
      if (k === 'artifacts' && Array.isArray(v) && v.length > artifactCount) artifactCount = v.length;
      if (k === 'steps_consumed' && typeof v === 'number' && v > steps) steps = v;
      if (k === 'close_reason' && typeof v === 'string' && v && !closeReason) closeReason = v;
      walk(v); } })(task);
  return { state: (task && task.status && task.status.state) || '', hexes: [...hexes], artifactID, toolCalls,
    artifactCount, steps, closeReason, recordedSha };
}
let pass = 0, fail = 0, skipped = 0; const ok = (n, x = '') => { pass++; console.log(`  \u2713 ${n}${x ? '  ' + x : ''}`); }; const bad = (n, x = '') => { fail++; console.log(`  \u2717 ${n}${x ? '  ' + x : ''}`); };
// A skipped leg is NOT verified behaviour: it is reported, counted and named.
const skip = (n, x = '') => { skipped++; console.log(`  \u2298 ${n} SKIPPED (not verified)${x ? '  ' + x : ''}`); };
console.log(`journey-deployed -> ${u.protocol}//${HOST}:${PORT} (UNACCEPTED prototype revision; iv is authoritative verifier)`);
try {
  const dep = PINNED ? PINNED.deployment : uniqueDeployment('dj');
  const dir = genClient(TENANT, dep);
  ok(PINNED
    ? 'pinned acceptance client cert reused, identity taken from its SAN (no minting)'
    : 'issued mTLS client cert bound to tenant/deployment', `spiffe=${TENANT}/${dep}`);
  const tokA2A = tokengen({ tenant: TENANT, deployment: dep, kind: 'task', subject: 'journey' }); ok('minted Bearer TASK token for /a2a/public (deployment transport mounts transport.Task)', `len=${tokA2A.length}`);
  const tok = tokengen({ tenant: TENANT, deployment: dep, kind: 'task' }); ok('minted Bearer TASK token for state/artifacts', `len=${tok.length}`);
  const nonce = 'n' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  const marker = 'AHP-DEPLOYED-JOURNEY-' + nonce;
  const prompt = 'You must use a tool. Call write_artifact exactly once with name="journey.txt", mime="text/plain" and content set EXACTLY to this single line (do not invent, do not add text):\n' + marker + '\nThen reply with the word ok.';
  const send = await a2a('SendMessage', { message: { role: 'user', kind: 'message', messageId: 'j-' + nonce, parts: [{ kind: 'text', text: prompt }] }, configuration: { blocking: true } }, { certDir: dir, token: tokA2A });
  if (send.json && send.json.error) { bad('message/send RPC error', RED(JSON.stringify(send.json.error)).slice(0, 220)); throw new Error('rpc'); }
  // DEF-H9-1: SendMessage/GetTask return the A2A oneof envelope SendMessageResponse{task|message}.
  const sendRes = (send.json && send.json.result) || null;
  let task = (sendRes && (sendRes.task || sendRes)) || null; const taskID = task && task.id;
  (send.status === 200 && taskID ? ok : bad)('POST /a2a/public message/send reached the deployed executor', `status=${send.status} taskId=${taskID ? 'present' : 'ABSENT'}`);
  if (!taskID) throw new Error('no task id');
  let state = (task.status && task.status.state) || ''; let polls = 0;
  while (!DONE_STATES.includes(state) && polls < 10) { await new Promise((z) => setTimeout(z, 3000));
    const g = await a2a('GetTask', { id: taskID }, { certDir: dir, token: tokA2A });
    if (g.json && g.json.result) { const gr = g.json.result; task = gr.task || gr; state = (task.status && task.status.state) || state; } polls++; }
  const h = harvest(task);
  const ctxId = (task && (task.contextId || task.context_id)) || '';
  const tokCtx = ctxId ? tokengen({ tenant: TENANT, deployment: dep, kind: 'task', ctx: ctxId }) : tok;
  if (ctxId) ok('re-minted TASK token bound to the task contextId', `ctx=${ctxId}`);
  (['completed', 'TASK_STATE_COMPLETED'].includes(state) ? ok : bad)('task reached TASK_STATE_COMPLETED via the REAL executor', `state=${state} polls=${polls}`);
  // DEF-H9-3: the composed front carries no ADK history; tool-call evidence is the sealed artifact
  // plus the executor's own step accounting on the task metadata.
  (h.artifactID && h.artifactCount >= 1 && h.steps >= 1 && h.closeReason === 'model_finished' ? ok : bad)('model emitted the write_artifact tool call (sealed artifact + executor step accounting + model_finished)',
    `artifacts=${h.artifactCount} steps_consumed=${h.steps} close_reason=${h.closeReason || 'n/a'} legacy_adk_toolCalls=${h.toolCalls}`);
  const recordedSha = h.recordedSha || (h.artifactID && h.hexes.length ? h.hexes[0] : '');
  const dl = h.artifactID ? await mArtifacts('Download', { id: h.artifactID }, { certDir: dir, token: tokCtx }) : { status: 0, buf: Buffer.alloc(0) };
  const gotSha = sha256hex(dl.buf); const gotText = dl.buf.toString('utf8');
  (dl.status === 200 ? ok : bad)('/v1/artifacts Download of the sealed artifact (same principal)', `status=${dl.status} bytes=${dl.buf.length}`);
  (gotText.includes(marker) ? ok : bad)('downloaded bytes are the MODEL-GENERATED nonce+marker (not a canned fixture)', `len=${gotText.length}`);
  (recordedSha && gotSha === recordedSha ? ok : bad)('sha256(GET real MinIO bytes) == sha256 recorded by the executor', `put=${(recordedSha || '').slice(0, 16)}.. get=${gotSha.slice(0, 16)}..`);
  // Foreign-tenant legs need a SECOND identity, which only minting can give. Reusing the
  // pinned victim-adjacent cert here would prove nothing, so without a CA key these legs
  // are reported SKIPPED - explicitly unverified - instead of quietly passing.
  if (PINNED && !canMint()) {
    skip('foreign tenant tasks/get -> TASK_NOT_FOUND', 'needs a minted foreign identity; this checkout has no CA key');
    if (h.artifactID) skip('foreign tenant Download -> NOT_FOUND/403', 'same reason');
  } else {
    const fdep = uniqueDeployment('dxf'); const ften = 'dxtenant' + Math.random().toString(36).slice(2, 6);
    const fdir = genClient(ften, fdep); const ftok = tokengen({ tenant: ften, deployment: fdep, kind: 'task' });
    // DEF-H9-5: /a2a/public mounts transport.Task, so a human-kind foreign token proves nothing but
    // token kind. Mint the foreign TASK token bound to the VICTIM context so the denial is tenant scope.
    const fgetTok = ctxId ? tokengen({ tenant: ften, deployment: fdep, kind: 'task', ctx: ctxId }) : ftok;
    const fget = await a2a('GetTask', { id: taskID }, { certDir: fdir, token: fgetTok });
    const notFound = fget.status === 404 || (fget.json && fget.json.error && (fget.json.error.code === -32001 || /not.?found/i.test(JSON.stringify(fget.json.error))));
    (notFound ? ok : bad)('foreign tenant tasks/get -> TASK_NOT_FOUND (isolation on deployed front)', `status=${fget.status}`);
    if (h.artifactID) { const fdl = await mArtifacts('Download', { id: h.artifactID }, { certDir: fdir, token: ftok });
      (fdl.status === 404 || fdl.status === 403 ? ok : bad)('foreign tenant Download -> NOT_FOUND/403', `status=${fdl.status}`); }
  }
  const tail = skipped ? ` + ${skipped} SKIPPED (NOT verified)` : '';
  console.log(fail === 0 ? `\nJOURNEY-DEPLOYED: PASS (${pass})${tail} — real native-Qwen -> write_artifact -> sealed REAL MinIO sha PUT==GET + durable state on the deployed front (iv re-run is authoritative)` : `\nJOURNEY-DEPLOYED: FAIL (${fail}/${pass + fail}) — NOT fabricated`);
  process.exitCode = fail === 0 ? 0 : 1;
} catch (e) { bad('exception', RED(e.message).slice(0, 200)); process.exitCode = 1; }
