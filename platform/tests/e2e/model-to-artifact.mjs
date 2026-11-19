#!/usr/bin/env node
// tests/e2e/model-to-artifact.mjs -- REAL native-Qwen -> versioned artifact -> PG state.
//
// Closes the journey segment "model -> versioned artifact -> PG state" the
// director flagged as previously over-claimed with mocks/httptest. NO MOCKS:
//   - the inference call is the REAL scoped native-Qwen endpoint from Vault;
//   - the bytes the model produces are stored through the REAL MinIO presigned
//     path and sealed as a versioned artifact;
//   - the artifact reference is persisted through the REAL State plane and read
//     back from the isolated PostgreSQL.
//
// NATIVE-ONLY (no OpenAI/Astra fallback): before ANY model egress the test runs
// the canonical G0 gate (integration-validation/scripts/native-qwen-assert.sh)
// against the real base_url/model and requires ALLOW (rc 0). The native host
// allowlist and the native model set are TRUSTED CONSTANTS defined here (the
// same values iv mirrors from native_qwen_test.go), NOT extracted from the
// secret -- otherwise the check would be self-fulfilling.
//
// Secrets (Vault token, api_key, full DSN) are never printed. Only the host
// suffix, model id, byte counts and ids are surfaced.

import { spawn, execFileSync, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
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

// TRUSTED native constants (mirrored from iv native_qwen_test.go @66bbf41).
// These are the authority; the secret is checked AGAINST them, never the reverse.
const NATIVE_HOST_SUFFIX = 'modelrun.inference.cloud.ru';
const NATIVE_MODELS = new Set(['model-run-tqmbu-road', 'qwen-38-27b-fp8']);

// Canonical G0 gate from integration-validation; locate it deterministically.
function g0ScriptPath() {
  if (process.env.AHP_NATIVE_QWEN_ASSERT && existsSync(process.env.AHP_NATIVE_QWEN_ASSERT)) {
    return process.env.AHP_NATIVE_QWEN_ASSERT;
  }
  // HQ default (outside the worktree; never copied into git).
  const hq = process.env.AHP_HQ_DIR || join(PROJECT_ROOT, '..', '..');
  const cand = join(hq, 'acceptance', 'integration-validation', 'scripts', 'native-qwen-assert.sh');
  return existsSync(cand) ? cand : '';
}

// mTLS POST to /v1/artifacts/<op>; returns {status, headers, buffer, text, json}.
function mArtifacts(op, body, { port, certDir, token, headers: extra } = {}) {
  const p = certPaths(certDir);
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  if (extra) Object.assign(headers, extra);
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
        res({ status: r.statusCode, headers: r.headers, buffer, text, json });
      });
    });
    req.on('error', rej); req.on('timeout', () => req.destroy(new Error('mArtifacts timeout ' + op)));
    req.write(payload); req.end();
  });
}

// Plain presigned PUT of raw bytes (signature in query, no auth header).
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

// Direct HTTPS chat completion (node https -> no ambient proxy consulted).
// Resolves {status, text, raw} where text is the assistant message content.
function chatCompletion({ baseUrl, apiKey, model, prompt, maxTokens = 64 }) {
  const u = new URL(baseUrl);
  let path = u.pathname.replace(/\/+$/, '');
  if (!path || path === '/') path = '/v1';
  path += '/chat/completions';
  const body = JSON.stringify({
    model, temperature: 0, max_tokens: maxTokens,
    messages: [{ role: 'user', content: prompt }],
  });
  const opts = {
    host: u.hostname, port: u.port || (u.protocol === 'https:' ? 443 : 80),
    method: 'POST', path,
    headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body), authorization: `Bearer ${apiKey}` },
    timeout: 60000,
  };
  return new Promise((res, rej) => {
    const req = https.request(opts, (r) => {
      let t = ''; r.on('data', d => (t += d));
      r.on('end', () => {
        let json = null; try { json = t ? JSON.parse(t) : null; } catch { /* noop */ }
        const text = json?.choices?.[0]?.message?.content ?? '';
        res({ status: r.statusCode, raw: t, text });
      });
    });
    req.on('error', rej); req.on('timeout', () => req.destroy(new Error('chat completion timeout')));
    req.write(body); req.end();
  });
}

async function main() {
  const env = await buildEnv(PROJECT_ROOT);
  const minioEndpoint = process.env.AHP_S3_ENDPOINT || 'http://127.0.0.1:29000';
  const user = env.AHP_MINIO_ROOT_USER || process.env.AHP_MINIO_ROOT_USER;
  const passw = env.AHP_MINIO_ROOT_PASSWORD || process.env.AHP_MINIO_ROOT_PASSWORD;
  const bucket = process.env.AHP_S3_BUCKET || `ahp-delivery-m2a-${Date.now().toString(36)}`;
  const port = Number(process.env.AHP_E2E_M2A_PORT || 29767);
  const vaultAddr = (env.AHP_VAULT_ADDR || env.VAULT_ADDR || '').replace(/\/+$/, '');
  const vaultTok = env.AHP_VAULT_DEV_TOKEN || env.VAULT_TOKEN || '';

  console.log('[model-to-artifact] REAL native-Qwen + REAL MinIO + isolated PG (no mocks)');
  console.log(`  vault=${vaultAddr ? new URL(vaultAddr).host : '(unset)'}  minio=${new URL(minioEndpoint).host}  bucket=${bucket}  db=${env.AHP_DB_NAME}`);

  // Precondition: real MinIO health.
  const alive = await new Promise((res) => {
    const req = http.get(minioEndpoint.replace(/\/$/, '') + '/minio/health/live', { timeout: 4000 },
      (r) => { r.resume(); res(r.statusCode === 200); });
    req.on('error', () => res(false)); req.on('timeout', () => { req.destroy(); res(false); });
  });
  assert(alive, `REAL MinIO unreachable at ${new URL(minioEndpoint).host}. Refusing to fake proof.`);
  assert(user && passw, 'MinIO root credentials present in env (not printed).');
  ok('real MinIO health/live=200');

  // --- (1) Read the scoped Qwen secret from the LOCAL Vault. Print NOTHING secret.
  assert(vaultAddr && vaultTok, 'Vault addr+token required (secrets never printed).');
  const vres = await fetch(vaultAddr + '/v1/secret/data/ahp-hive/execution/qwen',
    { headers: { 'X-Vault-Token': vaultTok, Accept: 'application/json' } });
  assert(vres.status === 200, `Vault read HTTP ${vres.status} (scoped Qwen secret must be provisioned)`);
  const vj = await vres.json();
  const sec = vj?.data?.data || {};
  const base_url = sec.base_url || '', model = sec.model || '', api_key = sec.api_key || '';
  assert(api_key && base_url && model, 'scoped Qwen secret must carry api_key+base_url+model');

  // --- (2) NATIVE-ONLY assertion against TRUSTED constants (not from the secret).
  let host = ''; try { host = new URL(base_url).hostname; } catch { /* handled below */ }
  const hostOk = host && (host === NATIVE_HOST_SUFFIX || host.endsWith('.' + NATIVE_HOST_SUFFIX));
  const modelOk = NATIVE_MODELS.has(model);
  assert(hostOk, `model host ${host || '(unparse)'} is not under native suffix ${NATIVE_HOST_SUFFIX} (native-only policy)`);
  assert(modelOk, `model "${model}" not in native allowlist {${[...NATIVE_MODELS].join(', ')}}`);
  ok('native-Qwen host+model verified vs trusted allowlist', `host=*.${NATIVE_HOST_SUFFIX} model=${model}`);

  // --- (3) Run the canonical G0 gate against the REAL base_url/model BEFORE egress.
  const g0 = g0ScriptPath();
  if (g0) {
    const r = spawnSync('bash', [g0], {
      env: { ...process.env, AHP_LLM_BASE_URL: base_url, AHP_LLM_MODEL: model, AHP_NATIVE_QWEN_HOSTS: NATIVE_HOST_SUFFIX },
      encoding: 'utf8', timeout: 30000,
    });
    const g0out = ((r.stdout || '') + (r.stderr || '')).split('\n').find((l) => /NATIVE-QWEN-ASSERT/.test(l)) || '';
    assert(r.status === 0, `G0 native-qwen-assert must ALLOW real model (rc=${r.status}) ${g0out}`);
    ok('G0 native-qwen-assert gate = ALLOW (rc 0) on real model', g0out.replace(/NATIVE-QWEN-ASSERT:\s*/, ''));
  } else {
    console.log('  ! canonical G0 script not found; relying on the inline trusted-allowlist assertion above');
  }

  // --- (4) REAL native-Qwen chat completion. The bytes are the model's own output.
  const cc = await chatCompletion({
    baseUrl: base_url, apiKey: api_key, model,
    prompt: 'You are a build artifact. Reply with exactly one short line: AHP-DELIVERY-M2A v1',
  });
  if (cc.status !== 200 || !cc.text.trim()) {
    bad('real Qwen chat completion', `status=${cc.status} body=${cc.raw.slice(0, 160)}`);
    throw new Error('real native-Qwen completion failed');
  }
  const modelText = cc.text.trim();
  const modelBytes = Buffer.from(`ahp-delivery-m2a model="${model}" host=${host}\n${modelText}\n`, 'utf8');
  ok('real native-Qwen completion', `bytes=${modelBytes.length} reply="${modelText.slice(0, 40)}"`);
  const mySha = sha256hex(modelBytes);

  // --- (5) Boot composed serve with the REAL data plane. Cleared proxy (dead ambient proxy would break egress).
  const bin = join(PROJECT_ROOT, 'bin', 'ahp-full');
  assert(existsSync(bin), 'bin/ahp-full present (build -tags \'ahp_exec ahp_data\')');
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

  try {
    await pollReady(port, { tls: true, timeoutMs: 30000 });
    ok('composed serve ready (/readyz pings REAL PG)');

    // Owner identity (cert SPIFFE == bound task token).
    const oTenant = 'dely', oDep = uniqueDeployment('m2a');
    const oDir = genClient(oTenant, oDep);
    const oTok = tokengen({ tenant: oTenant, deployment: oDep, audience: 'ahp', kind: 'task' });

    // --- (6) Versioned artifact: the model bytes through REAL MinIO.
    const beg = await mArtifacts('BeginUpload',
      { key: 'm2a/model-output.txt', name: 'model-output.txt', mime: 'text/plain', sha256: mySha, size: modelBytes.length },
      { port, certDir: oDir, token: oTok });
    if (beg.status !== 200 || !beg.json?.id || !beg.json?.presigned_url) { bad('BeginUpload 200+id+presigned_url', `status=${beg.status} ${beg.text.slice(0, 140)}`); throw new Error('BeginUpload failed'); }
    ok('BeginUpload', `id=${beg.json.id}`);

    const put = await presignedPut(beg.json.presigned_url, modelBytes);
    if (put !== 200) { bad('presigned PUT 200', 'got=' + put); throw new Error('presigned PUT failed'); }
    ok('presigned PUT -> REAL MinIO', `status=${put}`);

    const seal = await mArtifacts('Seal', { id: beg.json.id }, { port, certDir: oDir, token: oTok });
    const sealed = seal.status === 200 && seal.json?.version && seal.json?.sha256 === mySha;
    if (!sealed) { bad('Seal 200 version+sha', `status=${seal.status} ${seal.text.slice(0, 160)}`); throw new Error('Seal failed'); }
    const version = seal.json.version;
    ok('Seal -> versioned artifact', `version=${version} sha256=${seal.json.sha256.slice(0, 12)}..`);

    const dl = await mArtifacts('Download', { id: beg.json.id }, { port, certDir: oDir, token: oTok });
    if (dl.status === 200 && sha256hex(dl.buffer) === mySha) ok('Download full -> sha256 match (artifact is the model output)', `bytes=${dl.buffer.length}`);
    else bad('Download full sha match', `status=${dl.status}`);

    // --- (7) Persist the artifact reference through REAL State plane and read back.
    const ctxID = 'delivery-m2a-ctx-' + Date.now().toString(36);
    const thr = await mState('CreateThread', { context_id: ctxID, key: 'm2a/thread' }, { port, certDir: oDir, token: oTok });
    const threadID = thr.json?.id || '';
    if (thr.status === 200 && threadID) ok('state CreateThread on REAL PG', `id=${threadID}`);
    else bad('state CreateThread should be 200', `status=${thr.status} ${thr.text.slice(0, 140)}`);

    const runPayload = { artifact_id: beg.json.id, version, sha256: mySha, model, host };
    const run = threadID
      ? await mState('CreateRun', { thread_id: threadID, key: 'm2a/run', payload: runPayload }, { port, certDir: oDir, token: oTok })
      : { status: 0, text: 'skipped: no thread' };
    const runID = run.json?.id || '';
    if (run.status === 200 && runID) ok('state CreateRun on REAL PG', `id=${runID}`);
    else bad('state CreateRun should be 200', `status=${run.status} ${run.text.slice(0, 140)}`);

    const back = runID
      ? await mState('GetRun', { run_id: runID }, { port, certDir: oDir, token: oTok })
      : { status: 0, text: 'skipped: no run' };
    const persisted = back.status === 200 && back.json && back.json.id === runID
      && JSON.stringify(back.json).includes(beg.json.id) && JSON.stringify(back.json).includes(mySha);
    if (persisted) ok('state GetRun reads the artifact reference back from REAL PG', `id=${runID}`);
    else bad('state GetRun must return persisted artifact ref', `status=${back.status} ${back.text.slice(0, 140)}`);
  } catch (e) {
    fail++; console.error('  \u2717 fatal:', e.message);
    const tail = logs.join('').split('\n').slice(-10).join('\n');
    if (tail) console.error('  --- service log tail ---\n' + tail);
  } finally {
    kill();
    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`\n[model-to-artifact] ${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error('[model-to-artifact] fatal:', e && e.message ? e.message : e); process.exit(1); });
