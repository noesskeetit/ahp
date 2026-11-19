#!/usr/bin/env node
// tests/e2e/live-journey.mjs -- Canonical SINGLE real live journey (delivery-owned, UNACCEPTED).
// Requirements source: PDF rev3.2 only. Contract: HQ decisions/2026-10-30-live-journey-contract.md
//
// One end-to-end path, every hop REAL (no mocks, no httptest, no newMockState, no stand-in):
//   (1) real native-Qwen completion (G0-gated, host/model allowlist),
//   (2) real A2A tool-runtime tool-call (AHP_TOOL_RUNTIME_URL; the author's Emil-core
//       :18000 is ONE conforming example, NOT a readiness gate) -- verified by that
//       runtime's own history (function_call + succeeded function_response) AND its own
//       provenance usage counters (model_turns>=1, tool_calls>=1). The gate is a REAL
//       A2A agent producing tool-derived bytes, NOT the author's Emil container identity.
//   (3) Emil's REAL tool-derived bytes go through the composed data plane into REAL MinIO
//       (presigned PUT -> Seal -> Download) and sha256(PUT) == sha256(GET),
//   (4) durable PG State keeps the artifact reference (CreateThread/CreateRun/GetRun).
//
// Fail-closed per contract: a missing real dependency is a FAIL with a reason, never a skip
// and never a pass. Secrets are never printed (long tokens redacted).
//
// Machine verdict lines (grep-able):
//   LIVE_JOURNEY=GREEN|RED
//   LIVE_JOURNEY_SHA256=<hex>        (sha of the stored bytes)
//   LIVE_PUT_SHA256 / LIVE_GET_SHA256 / LIVE_SEAL_SHA256
//   LIVE_EMIL_TASK / LIVE_EMIL_TOOL_CALLS / LIVE_EMIL_MODEL_TURNS
//   LIVE_PG_RUN

import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync, mkdirSync, writeFileSync } from 'node:fs';
import https from 'node:https';
import http from 'node:http';
import { join } from 'node:path';
import { buildEnv } from '../../scripts/ahp-env.mjs';
import {
  PROJECT_ROOT, CERT_DIR, certPaths, pollReady, assert,
  genClient, uniqueDeployment, devSeedHex, tokengen, mState,
} from '../helpers/ahp-client.mjs';

let pass = 0, fail = 0;
const ok = (n, x = '') => { pass++; console.log(`  \u2713 ${n}${x ? '  ' + x : ''}`); };
const bad = (n, x = '') => { fail++; console.log(`  \u2717 ${n}${x ? '  ' + x : ''}`); };
const sha256hex = (b) => createHash('sha256').update(b).digest('hex');
const RED = (s) => String(s).replace(/[A-Za-z0-9._+/=-]{40,}/g, '<REDACTED>');
const HEX64 = /^[0-9a-f]{64}$/;

// Target = a real A2A tool-runtime. Name it AHP_TOOL_RUNTIME_URL (architecture term);
// AHP_EMIL_URL kept as a backward-compatible alias. :18000 (author Emil-core) is just
// the historical default example, NOT a gate (user/root correction + director 2026-10-30).
const EMIL = (process.env.AHP_TOOL_RUNTIME_URL || process.env.AHP_EMIL_URL || 'http://127.0.0.1:18000').replace(/\/$/, '');
// :19081 is ahp-vm-tool-agent (kvm stand-in). Contract forbids it as LIVE tool-runtime evidence.
const STANDIN_PORTS = new Set(['19081']);
const NATIVE_HOST_SUFFIX = 'modelrun.inference.cloud.ru';
const NATIVE_MODELS = new Set(['model-run-tqmbu-road', 'qwen-38-27b-fp8']);
const SKILLS = ['validate-data', 'systematic-debugging', 'explore-data',
  'knowledge-synthesis', 'sql-queries', 'statistical-analysis', 'verification-before-completion'];

function g0ScriptPath() {
  if (process.env.AHP_NATIVE_QWEN_ASSERT && existsSync(process.env.AHP_NATIVE_QWEN_ASSERT)) {
    return process.env.AHP_NATIVE_QWEN_ASSERT;
  }
  const hq = process.env.AHP_HQ_DIR || join(PROJECT_ROOT, '..', '..');
  const cand = join(hq, 'acceptance', 'integration-validation', 'scripts', 'native-qwen-assert.sh');
  return existsSync(cand) ? cand : '';
}

function mArtifacts(op, body, { port, certDir, token } = {}) {
  const p = certPaths(certDir);
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  const payload = JSON.stringify(body ?? {});
  const opts = {
    host: '127.0.0.1', port, method: 'POST', path: `/v1/artifacts/${op}`,
    ca: readFileSync(p.ca), cert: readFileSync(p.cert), key: readFileSync(p.key),
    servername: 'localhost', rejectUnauthorized: true, headers, timeout: 20000,
  };
  return new Promise((res, rej) => {
    const req = https.request(opts, (r) => {
      const ch = []; r.on('data', d => ch.push(d));
      r.on('end', () => {
        const buffer = Buffer.concat(ch); const text = buffer.toString('utf8');
        let json = null; try { json = text ? JSON.parse(text) : null; } catch { /* noop */ }
        res({ status: r.statusCode, buffer, text, json });
      });
    });
    req.on('error', rej); req.on('timeout', () => req.destroy(new Error('mArtifacts timeout ' + op)));
    req.write(payload); req.end();
  });
}

function presignedPut(urlStr, buf) {
  return new Promise((res, rej) => {
    const u = new URL(urlStr);
    const mod = u.protocol === 'https:' ? https : http;
    const req = mod.request(u, { method: 'PUT', headers: { 'content-length': buf.length }, timeout: 20000 },
      (r) => { r.resume(); r.on('end', () => res(r.statusCode)); });
    req.on('error', rej); req.on('timeout', () => req.destroy(new Error('presigned PUT timeout')));
    req.write(buf); req.end();
  });
}

function chatCompletion({ baseUrl, apiKey, model, prompt, maxTokens = 64 }) {
  const u = new URL(baseUrl);
  let path = u.pathname.replace(/\/+$/, '');
  if (!path || path === '/') path = '/v1';
  path += '/chat/completions';
  const body = JSON.stringify({ model, temperature: 0, max_tokens: maxTokens, messages: [{ role: 'user', content: prompt }] });
  const opts = {
    host: u.hostname, port: u.port || (u.protocol === 'https:' ? 443 : 80), method: 'POST', path,
    headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body), authorization: `Bearer ${apiKey}` },
    timeout: 60000,
  };
  return new Promise((res, rej) => {
    const req = https.request(opts, (r) => {
      let t = ''; r.on('data', d => (t += d));
      r.on('end', () => {
        let json = null; try { json = t ? JSON.parse(t) : null; } catch { /* noop */ }
        res({ status: r.statusCode, raw: t, text: json?.choices?.[0]?.message?.content ?? '' });
      });
    });
    req.on('error', rej); req.on('timeout', () => req.destroy(new Error('chat completion timeout')));
    req.write(body); req.end();
  });
}

function a2a(method, params, timeoutMs = 90000) {
  const body = JSON.stringify({ jsonrpc: '2.0', id: Date.now() % 1e9, method, params });
  return new Promise((res, rej) => {
    const u = new URL(EMIL + '/');
    const req = http.request(u, {
      method: 'POST', headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) }, timeout: timeoutMs,
    }, (r) => {
      let t = ''; r.on('data', d => (t += d));
      r.on('end', () => { let j = null; try { j = t ? JSON.parse(t) : null; } catch { /* noop */ } res({ status: r.statusCode, json: j }); });
    });
    req.on('error', rej); req.on('timeout', () => req.destroy(new Error('a2a ' + method + ' timeout')));
    req.write(body); req.end();
  });
}

// Pull the real tool traffic out of Emil's own A2A task record.
function harvest(task) {
  const calls = [], responses = [];
  for (const m of (task && task.history) || []) {
    for (const p of (m && m.parts) || []) {
      const t = p && p.metadata && p.metadata.adk_type;
      if (t === 'function_call') calls.push(p.data || {});
      else if (t === 'function_response') responses.push((p.data && p.data.response) || p.data || {});
    }
  }
  const art = ((task && task.artifacts) || [])[0] || null;
  const artText = ((art && art.parts) || []).filter(p => p.kind === 'text').map(p => p.text).join('\n');
  const prov = (art && art.metadata && art.metadata.provenance) || {};
  return {
    calls, responses, art, artText, prov,
    usage: prov.usage || {},
    artifactDigest: (art && art.metadata && art.metadata.digest) || '',
  };
}

async function emilAttempt(skill) {
  const mid = 'lj-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8);
  const text = `You must use a tool. Step 1: call core_skill_activate exactly once with names set to exactly ["${skill}"]. Step 2: reply in one short line naming the skill and the digest the tool returned. Do not invent any value.`;
  const send = await a2a('message/send', {
    message: { role: 'user', kind: 'message', messageId: mid, parts: [{ kind: 'text', text }] },
    configuration: { blocking: true },
  });
  let task = (send.json && send.json.result) || null;
  if (send.json && send.json.error) return { err: 'rpc_error ' + JSON.stringify(send.json.error).slice(0, 200) };
  if (!task || !task.id) return { err: 'no A2A task id (status=' + send.status + ')' };
  let state = (task.status && task.status.state) || '';
  let polls = 0;
  while (!['completed', 'failed', 'canceled', 'rejected'].includes(state) && polls < 8) {
    await new Promise(z => setTimeout(z, 2000));
    try { const g = await a2a('tasks/get', { id: task.id }); if (g.json && g.json.result) { task = g.json.result; state = (task.status && task.status.state) || state; } } catch { /* keep */ }
    polls++;
  }
  return { skill, status: send.status, task, state, polls, h: harvest(task) };
}

async function main() {
  const env = await buildEnv(PROJECT_ROOT);
  const minioEndpoint = process.env.AHP_S3_ENDPOINT || 'http://127.0.0.1:29000';
  const user = env.AHP_MINIO_ROOT_USER || process.env.AHP_MINIO_ROOT_USER;
  const passw = env.AHP_MINIO_ROOT_PASSWORD || process.env.AHP_MINIO_ROOT_PASSWORD;
  const bucket = process.env.AHP_S3_BUCKET || `ahp-delivery-lj-${Date.now().toString(36)}`;
  const port = Number(process.env.AHP_E2E_LJ_PORT || 29771);
  const vaultAddr = (env.AHP_VAULT_ADDR || env.VAULT_ADDR || '').replace(/\/+$/, '');
  const vaultTok = env.AHP_VAULT_DEV_TOKEN || env.VAULT_TOKEN || '';
  const nonce = 'AHP-LJ-' + Date.now().toString(36);

  console.log('# live-journey  REAL Qwen -> REAL Emil-core tool-call -> REAL MinIO -> REAL PG  (unaccepted)');
  console.log(`  emil=${EMIL}  minio=${new URL(minioEndpoint).host}  bucket=${bucket}  db=${env.AHP_DB_NAME}  mocks=none`);

  // ---- (0) fail-closed anti-stand-in / anti-mock guards -----------------------
  const eu = new URL(EMIL);
  assert(!STANDIN_PORTS.has(eu.port), `tool-runtime target port ${eu.port} is the kvm STAND-IN (ahp-vm-tool-agent); contract forbids it as LIVE tool-runtime proof`);
  assert(!process.env.AHP_E2E_ALLOW_MOCK, 'mock mode must not be enabled for the live journey');
  const cardRes = await fetch(EMIL + '/.well-known/agent-card.json');
  assert(cardRes.status === 200, `tool-runtime agent-card HTTP ${cardRes.status} -- a real A2A agent endpoint is required`);
  const card = await cardRes.json();
  // Identity of the container is NOT the gate (per user/root + director). The gate is a
  // VALID A2A agent-card: a real A2A endpoint exposes a non-empty name + version and A2A
  // capabilities. The author's Emil-core "core-agent" name is accepted but NOT required.
  const a2aCard = card && typeof card.name === 'string' && card.name.length > 0
    && typeof card.version === 'string' && card.version.length > 0
    && (card.capabilities || card.skills || card.url);
  assert(a2aCard, `target is not a valid A2A agent-card (name=${card && card.name} version=${card && card.version} caps=${!!(card && (card.capabilities || card.skills || card.url))})`);
  ok('LIVE A2A tool-runtime target (valid A2A agent-card, not stand-in/httptest)', `target=${eu.host} name=${card.name} v=${card.version}`);

  const alive = await new Promise((res) => {
    const req = http.get(minioEndpoint.replace(/\/$/, '') + '/minio/health/live', { timeout: 4000 },
      (r) => { r.resume(); res(r.statusCode === 200); });
    req.on('error', () => res(false)); req.on('timeout', () => { req.destroy(); res(false); });
  });
  assert(alive, `REAL MinIO unreachable at ${new URL(minioEndpoint).host}. Refusing to fake proof.`);
  assert(user && passw, 'MinIO credentials present in env (not printed).');
  ok('real MinIO health/live=200');

  // ---- (1) real native-Qwen, G0-gated ----------------------------------------
  assert(vaultAddr && vaultTok, 'Vault addr+token required (secrets never printed).');
  const vres = await fetch(vaultAddr + '/v1/secret/data/ahp-hive/execution/qwen',
    { headers: { 'X-Vault-Token': vaultTok, Accept: 'application/json' } });
  assert(vres.status === 200, `Vault read HTTP ${vres.status} (scoped Qwen secret must be provisioned)`);
  const sec = (await vres.json())?.data?.data || {};
  const base_url = sec.base_url || '', model = sec.model || '', api_key = sec.api_key || '';
  assert(api_key && base_url && model, 'scoped Qwen secret must carry api_key+base_url+model');
  let qhost = ''; try { qhost = new URL(base_url).hostname; } catch { /* below */ }
  assert(qhost && (qhost === NATIVE_HOST_SUFFIX || qhost.endsWith('.' + NATIVE_HOST_SUFFIX)),
    `model host ${qhost || '(unparse)'} is not under native suffix ${NATIVE_HOST_SUFFIX}`);
  assert(NATIVE_MODELS.has(model), `model "${model}" not in native allowlist`);
  const g0 = g0ScriptPath();
  if (g0) {
    const r = spawnSync('bash', [g0], {
      env: { ...process.env, AHP_LLM_BASE_URL: base_url, AHP_LLM_MODEL: model, AHP_NATIVE_QWEN_HOSTS: NATIVE_HOST_SUFFIX },
      encoding: 'utf8', timeout: 30000,
    });
    assert(r.status === 0, `G0 native-qwen-assert must ALLOW real model (rc=${r.status})`);
    ok('G0 native-qwen-assert = ALLOW on real model', `model=${model}`);
  } else {
    ok('native-only enforced vs trusted allowlist (canonical G0 script absent)', `model=${model}`);
  }
  const cc = await chatCompletion({ baseUrl: base_url, apiKey: api_key, model, prompt: `Reply with exactly one short line: ${nonce}` });
  assert(cc.status === 200 && cc.text.trim(), `real native-Qwen completion failed (status=${cc.status})`);
  const qwenText = cc.text.trim();
  ok('REAL native-Qwen completion', `host=${qhost} model=${model} reply="${qwenText.slice(0, 40)}"`);

  // ---- (2) REAL Emil-core tool-call ------------------------------------------
  let att = null, tried = [];
  for (const skill of SKILLS.slice(0, 3)) {
    tried.push(skill);
    let a;
    try { a = await emilAttempt(skill); } catch (e) { bad('Emil message/send transport error', e.message); continue; }
    if (a.err) { bad('Emil A2A attempt', a.err); continue; }
    const h = a.h;
    const call = h.calls.find(c => c.name === 'core_skill_activate'
      && Array.isArray(c.args && c.args.names) && c.args.names.includes(skill));
    const activated = [];
    for (const r of h.responses) {
      for (const act of ((r && r.output && r.output.activated) || [])) {
        if (act && act.name && HEX64.test(String(act.digest || ''))) activated.push(act);
      }
    }
    const usage = h.usage || {};
    const okUsage = Number(usage.tool_calls || 0) >= 1 && Number(usage.model_turns || 0) >= 1;
    const okArt = a.state === 'completed' && !!h.art && HEX64.test(String(h.artifactDigest).replace(/^sha256:/, ''));
    if (a.state === 'completed' && call && activated.length && okUsage && okArt) {
      att = { ...a, call, usage: h.usage, art: h.art, prov: h.prov, artifactDigest: h.artifactDigest, activated: activated.filter(x => x.name === skill).concat(activated)[0], allActivated: activated };
      break;
    }
    console.log(`  ~ attempt skill=${skill} state=${a.state} calls=${h.calls.map(c => c.name).join(',') || 'none'} activated=${activated.length} usage=${JSON.stringify(usage)}`);
    att = { ...a, call, usage: h.usage, art: h.art, prov: h.prov, artifactDigest: h.artifactDigest, activated: activated[0] || null, allActivated: activated, incomplete: true };
  }
  assert(att && !att.incomplete, `tool-runtime real tool-call not established after attempts [${tried.join(', ')}] -- state=${att && att.state} (NOT green-washed)`);
  ok('REAL Emil tool-call: model issued core_skill_activate with real args',
    `skill=${att.skill} args=${JSON.stringify(att.call.args)}`);
  ok('REAL Emil tool executed (succeeded + real content digest)',
    `activated=${att.activated.name} already_active=${att.activated.already_active} digest=${String(att.activated.digest).slice(0, 12)}..`);
  ok("REAL tool-runtime provenance proves model+tool turns (runtime's own accounting)",
    `model_turns=${att.usage.model_turns} tool_calls=${att.usage.tool_calls} completion=${att.prov.completion_reason}`);
  ok('REAL Emil returned a content-addressed artifact',
    `taskId=${att.task.id} size=${att.art.metadata.size} digest=${String(att.artifactDigest).slice(0, 18)}..`);

  // Artifact bytes are DERIVED FROM THE REAL TOOL OUTPUT (not a constant).
  const evidence = {
    journey: 'ahp-delivery-live-journey', nonce, unaccepted: true,
    qwen: { host: qhost, model, reply: qwenText },
    emil: {
      target: eu.host, task_id: att.task.id, context_id: att.contextId || (att.task.contextId || ''),
      run_id: att.prov.run_id || '', completion_reason: att.prov.completion_reason || '',
      usage: att.usage, tool_call: { name: att.call.name, args: att.call.args },
      tool_result: { name: att.activated.name, digest: att.activated.digest, already_active: att.activated.already_active },
      emil_artifact_digest: att.artifactDigest,
    },
  };
  const artifactBytes = Buffer.from(JSON.stringify(evidence, null, 1) + '\n', 'utf8');
  const putSha = sha256hex(artifactBytes);
  ok('artifact bytes derived from REAL Qwen + REAL Emil tool output', `bytes=${artifactBytes.length}`);

  // ---- (3)+(4) composed data plane -> REAL MinIO, then durable PG ------------
  const bin = join(PROJECT_ROOT, 'bin', 'ahp-full');
  assert(existsSync(bin), "bin/ahp-full present (build -tags 'ahp_exec ahp_data')");
  const svcEnv = {
    ...env,
    HTTP_PROXY: '', HTTPS_PROXY: '', http_proxy: '', https_proxy: '',
    NO_PROXY: 'localhost,127.0.0.1,::1',
    AHP_ROLE: 'all', AHP_ENABLE_DATA: '1',
    AHP_LISTEN: `:${port}`,
    AHP_DEV_JWT_SEED: devSeedHex(),
    AHP_TLS_CERT: join(CERT_DIR, 'server', 'server.crt'),
    AHP_TLS_KEY: join(CERT_DIR, 'server', 'server.key'),
    AHP_TLS_CA: join(CERT_DIR, 'ca', 'ca.crt'),
    OTEL_EXPORTER_OTLP_ENDPOINT: '',
    AHP_S3_ENDPOINT: minioEndpoint, AHP_S3_BUCKET: bucket,
    AHP_MINIO_ROOT_USER: user, AHP_MINIO_ROOT_PASSWORD: passw,
    AHP_S3_REGION: env.AHP_S3_REGION || 'us-east-1',
  };
  const logs = [];
  const proc = spawn(bin, [], { cwd: PROJECT_ROOT, env: svcEnv, stdio: ['ignore', 'pipe', 'pipe'] });
  proc.stdout.on('data', d => logs.push(String(d))); proc.stderr.on('data', d => logs.push(String(d)));
  const kill = () => { try { proc.kill('SIGTERM'); } catch { /* gone */ } };

  let getSha = '', sealSha = '', artifactID = '', version = '', runID = '';
  try {
    await pollReady(port, { tls: true, timeoutMs: 30000 });
    ok('composed serve ready (/readyz pings REAL PG)');
    const oTenant = 'dely', oDep = uniqueDeployment('lj');
    const oDir = genClient(oTenant, oDep);
    const oTok = tokengen({ tenant: oTenant, deployment: oDep, audience: 'ahp', kind: 'task' });

    const beg = await mArtifacts('BeginUpload',
      { key: 'live-journey/emil-tool-evidence.json', name: 'emil-tool-evidence.json', mime: 'application/json', sha256: putSha, size: artifactBytes.length },
      { port, certDir: oDir, token: oTok });
    assert(beg.status === 200 && beg.json && beg.json.id && beg.json.presigned_url,
      `BeginUpload must return id+presigned_url (status=${beg.status} ${RED(beg.text).slice(0, 140)})`);
    artifactID = beg.json.id;
    ok('BeginUpload (scoped test bucket)', `id=${artifactID}`);

    const put = await presignedPut(beg.json.presigned_url, artifactBytes);
    assert(put === 200, `presigned PUT into REAL MinIO must be 200 (got ${put})`);
    ok('presigned PUT of the REAL Emil bytes -> REAL MinIO', 'status=200');

    const seal = await mArtifacts('Seal', { id: artifactID }, { port, certDir: oDir, token: oTok });
    assert(seal.status === 200 && seal.json && seal.json.version, `Seal must return a version (status=${seal.status} ${RED(seal.text).slice(0, 140)})`);
    version = seal.json.version; sealSha = seal.json.sha256 || '';
    ok('Seal -> versioned artifact', `version=${version} seal_sha256=${sealSha.slice(0, 12)}..`);

    const dl = await mArtifacts('Download', { id: artifactID }, { port, certDir: oDir, token: oTok });
    assert(dl.status === 200, `Download must be 200 (got ${dl.status})`);
    getSha = sha256hex(dl.buffer);
    assert(getSha === putSha, `sha256(GET) ${getSha.slice(0, 16)}.. != sha256(PUT) ${putSha.slice(0, 16)}.. -- real bytes must round-trip`);
    const dlJson = (() => { try { return JSON.parse(dl.buffer.toString('utf8')); } catch { return null; } })();
    assert(dlJson && dlJson.emil && dlJson.emil.task_id === att.task.id
      && dlJson.emil.tool_result.digest === att.activated.digest,
      'downloaded bytes must still carry Emil\'s real task_id + real tool digest');
    ok('sha256 PUT == sha256 GET on the REAL Emil bytes', `bytes=${dl.buffer.length} sha256=${getSha.slice(0, 16)}..`);

    const thr = await mState('CreateThread', { context_id: nonce, key: 'lj/thread' }, { port, certDir: oDir, token: oTok });
    const threadID = (thr.json && thr.json.id) || '';
    assert(thr.status === 200 && threadID, `state CreateThread on REAL PG must be 200 (status=${thr.status})`);
    ok('state CreateThread on REAL PG', `id=${threadID}`);

    const runPayload = {
      artifact_id: artifactID, version, sha256: putSha,
      model, qwen_reply_sha256: sha256hex(Buffer.from(qwenText, 'utf8')),
      emil_task_id: att.task.id, emil_tool: att.call.name, emil_tool_digest: att.activated.digest,
      emil_tool_calls: att.usage.tool_calls, emil_model_turns: att.usage.model_turns,
    };
    const run = await mState('CreateRun', { thread_id: threadID, key: 'lj/run', payload: runPayload }, { port, certDir: oDir, token: oTok });
    runID = (run.json && run.json.id) || '';
    assert(run.status === 200 && runID, `state CreateRun on REAL PG must be 200 (status=${run.status} ${RED(run.text).slice(0, 140)})`);
    ok('state CreateRun on REAL PG (Emil hop + artifact ref persisted)', `id=${runID}`);

    const back = await mState('GetRun', { run_id: runID }, { port, certDir: oDir, token: oTok });
    const blob = JSON.stringify(back.json || {});
    assert(back.status === 200 && blob.includes(artifactID) && blob.includes(putSha) && blob.includes(att.task.id),
      `GetRun must read the real artifact+sha+Emil-task back from PG (status=${back.status})`);
    ok('state GetRun reads artifact+sha+Emil-task back from REAL PG', `id=${runID}`);

    const evDir = join('/tmp', 'ahp-live-journey');
    try { mkdirSync(evDir, { recursive: true }); writeFileSync(join(evDir, nonce + '.evidence.json'), JSON.stringify({ evidence, putSha, getSha, sealSha, artifactID, version, runID }, null, 2)); } catch { /* best effort */ }
  } catch (e) {
    fail++; console.error('  \u2717 fatal:', RED(e.message));
    const tail = logs.join('').split('\n').slice(-8).join('\n');
    if (tail) console.error('  --- service log tail ---\n' + RED(tail));
  } finally {
    kill();
    await new Promise(r => setTimeout(r, 300));
  }

  console.log('\n===== LIVE JOURNEY VERDICT =====');
  console.log(`LIVE_EMIL_TASK=${att && att.task ? att.task.id : 'none'}`);
  console.log(`LIVE_EMIL_TOOL_CALLS=${att && att.usage ? att.usage.tool_calls : 0} LIVE_EMIL_MODEL_TURNS=${att && att.usage ? att.usage.model_turns : 0}`);
  console.log(`LIVE_PUT_SHA256=${putSha}`);
  console.log(`LIVE_GET_SHA256=${getSha || 'none'}`);
  console.log(`LIVE_SEAL_SHA256=${sealSha || 'none'}`);
  console.log(`LIVE_PG_RUN=${runID || 'none'}`);
  console.log(`LIVE_JOURNEY_SHA256=${putSha}`);
  console.log(`\n# live-journey ${pass} passed, ${fail} failed`);
  console.log(fail === 0 ? 'LIVE_JOURNEY=GREEN' : 'LIVE_JOURNEY=RED');
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error('[live-journey] fatal:', RED(e && e.message ? e.message : e));
  console.log('LIVE_JOURNEY=RED');
  process.exit(1);
});
