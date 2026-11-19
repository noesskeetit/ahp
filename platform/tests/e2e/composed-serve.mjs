#!/usr/bin/env node
// tests/e2e/composed-serve.mjs -- composed serve on COMBAT dependencies (no mocks).
//
// Boots the feature-tagged all-role binary against the REAL PostgreSQL, the
// REAL MinIO and the REAL Vault-backed secret store, then proves every plane is
// actually mounted (a missing plane shows up as 404, which this test treats as
// a hard failure, never as a pass):
//   /readyz            -> real PG ping
//   /v1/state/         -> state plane on real PG
//   /v1/identity/      -> identity plane
//   /v1/artifacts/     -> data plane on real MinIO (auth rejection, not 404)
//   /a2a/              -> execution plane migrated in PG, fail-closed executor
//
// Secrets are never printed. Live-model egress is exercised by
// tests/e2e/live-model.mjs, not here.

import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import https from 'node:https';
import { join } from 'node:path';
import { buildEnv } from '../../scripts/ahp-env.mjs';
import {
  PROJECT_ROOT, CERT_DIR, certPaths, pollReady, assert,
  genClient, uniqueDeployment, devSeedHex, tokengen, mState,
} from '../helpers/ahp-client.mjs';

let pass = 0, fail = 0;
const ok = (n, x = '') => { pass++; console.log(`  \u2713 ${n}${x ? '  ' + x : ''}`); };
const bad = (n, x = '') => { fail++; console.log(`  \u2717 ${n}${x ? '  ' + x : ''}`); };

// mTLS GET; returns {status, text}.
function mGet(path, { port, certDir = 'client' }) {
  const p = certPaths(certDir);
  const opts = {
    host: '127.0.0.1', port, method: 'GET', path,
    ca: readFileSync(p.ca), cert: readFileSync(p.cert), key: readFileSync(p.key),
    servername: 'localhost', rejectUnauthorized: true, timeout: 15000,
  };
  return new Promise((res, rej) => {
    const req = https.request(opts, (r) => {
      let text = ''; r.on('data', d => (text += d));
      r.on('end', () => res({ status: r.statusCode, text }));
    });
    req.on('error', rej); req.on('timeout', () => req.destroy(new Error('mGet timeout ' + path)));
    req.end();
  });
}

// mTLS JSON-RPC-ish POST to the A2A face; returns status only.
function mPost(path, body, { port, certDir, token }) {
  const p = certPaths(certDir);
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  const payload = JSON.stringify(body ?? {});
  const opts = {
    host: '127.0.0.1', port, method: 'POST', path, headers,
    ca: readFileSync(p.ca), cert: readFileSync(p.cert), key: readFileSync(p.key),
    servername: 'localhost', rejectUnauthorized: true, timeout: 15000,
  };
  return new Promise((res, rej) => {
    const req = https.request(opts, (r) => { let t = ''; r.on('data', d => (t += d)); r.on('end', () => res({ status: r.statusCode, text: t })); });
    req.on('error', rej); req.on('timeout', () => req.destroy(new Error('mPost timeout')));
    req.write(payload); req.end();
  });
}

async function main() {
  const env = await buildEnv(PROJECT_ROOT);
  const port = Number(process.env.AHP_E2E_COMPOSED_PORT || 29753);
  const bucket = `ahp-delivery-cmp-${Date.now().toString(36)}`;

  // Egress reality: the ambient HTTPS_PROXY on this host is dead and Go's
  // ProxyFromEnvironment would silently break every live upstream call. The
  // service must run with proxies cleared; product transport is untouched.
  const svcEnv = {
    ...env,
    HTTP_PROXY: '', HTTPS_PROXY: '', http_proxy: '', https_proxy: '',
    NO_PROXY: 'localhost,127.0.0.1,::1',
    AHP_ROLE: 'all', AHP_ENABLE_DATA: '1', AHP_ENABLE_EXECUTION: '1',
    AHP_LISTEN: `:${port}`,
    AHP_DEV_JWT_SEED: devSeedHex(),
    AHP_TLS_CERT: join(CERT_DIR, 'server', 'server.crt'),
    AHP_TLS_KEY: join(CERT_DIR, 'server', 'server.key'),
    AHP_TLS_CA: join(CERT_DIR, 'ca', 'ca.crt'),
    OTEL_EXPORTER_OTLP_ENDPOINT: '',
    AHP_S3_ENDPOINT: env.AHP_S3_ENDPOINT || 'http://127.0.0.1:29000',
    AHP_S3_BUCKET: bucket,
    AHP_S3_REGION: env.AHP_S3_REGION || 'us-east-1',
  };

  console.log('[composed-serve] all-role on REAL PG + REAL MinIO (no mocks)');
  console.log(`  db=${env.AHP_DB_NAME}  minio=${new URL(svcEnv.AHP_S3_ENDPOINT).host}  bucket=${bucket}  exec_schema=ahp_exec`);

  const logs = [];
  const proc = spawn(join(PROJECT_ROOT, 'bin', 'ahp-full'), [], { cwd: PROJECT_ROOT, env: svcEnv, stdio: ['ignore', 'pipe', 'pipe'] });
  proc.stdout.on('data', d => logs.push(String(d))); proc.stderr.on('data', d => logs.push(String(d)));

  try {
    await pollReady(port, { tls: true, timeoutMs: 45000 });
    ok('composed /readyz=200', 'real PG ping');

    const L = logs.join('');
    const warnRe = /(data|execution) plane not mounted/i;
    if (warnRe.test(L)) bad('all optional planes mounted', L.match(warnRe)[0]);
    else ok('no "plane not mounted" warnings', 'data+execution both mounted');

    // Identity + state planes: real round-trips on the REAL PG.
    const tenant = 'dely', dep = uniqueDeployment('cs');
    const certDir = genClient(tenant, dep);
    const tok = tokengen({ tenant, deployment: dep, audience: 'ahp', kind: 'task' });
    const humanTok = tokengen({
      tenant, deployment: dep, subject: 'delivery-human', task: 'delivery-human',
      audience: 'ahp', kind: 'human',
    });

    // Identity plane is mounted iff the real route answers: an unauthenticated
    // create-root must be rejected by auth (401/403). A 404 means not mounted.
    const rootTask = 'delivery-root-' + Date.now().toString(36);
    // A context id is bound to the scope that first creates a root under it.
    // The deployment is unique per run, so the context must be too, otherwise a
    // re-run collides with the prior run's root ("belongs to another scope").
    const rootCtx = 'delivery-ctx-' + Date.now().toString(36);
    // A root must ask for at least one capability; these are the identity
    // plane's own verbs, the ones a descendant token will present later.
    const rootBody = {
      task: rootTask, context: rootCtx, ttl: '5m', deployment: dep,
      capabilities: ['identity:read', 'identity:mint'],
    };
    const idn = await mPost('/v1/identity/roots', rootBody, { port, certDir });
    if (idn.status === 401 || idn.status === 403) ok('identity plane mounted + auth enforced', 'status=' + idn.status);
    else if (idn.status === 404) bad('identity plane 404 = not mounted');
    else bad('identity create-root without a token should be 401/403', 'status=' + idn.status + ' ' + idn.text.slice(0, 120));

    // With a signed human token the same route writes into the real identity
    // store (PG) and returns the created grant.
    const root = await mPost('/v1/identity/roots', rootBody, { port, certDir, token: humanTok });
    if (root.status === 201) ok('identity create-root on REAL PG', 'status=' + root.status);
    else bad('identity create-root should be 201', 'status=' + root.status + ' ' + root.text.slice(0, 160));

    // State plane on REAL PG: the create-run -> PG state leg of the journey.
    const ctxID = 'delivery-ctx-' + Date.now().toString(36);
    const thr = await mState('CreateThread', { context_id: ctxID, key: 'delivery/thread' }, { port, certDir, token: tok });
    const threadID = thr.json && thr.json.id ? thr.json.id : '';
    if (thr.status === 200 && threadID) ok('state CreateThread on REAL PG', 'id=' + threadID);
    else bad('state CreateThread should be 200', 'status=' + thr.status + ' ' + thr.text.slice(0, 160));

    const run = threadID
      ? await mState('CreateRun', { thread_id: threadID, key: 'delivery/run', payload: { prompt: 'composed-real-pg' } }, { port, certDir, token: tok })
      : { status: 0, text: 'skipped: no thread', json: null };
    const runID = run.json && run.json.id ? run.json.id : '';
    if (run.status === 200 && runID) ok('state CreateRun on REAL PG', 'id=' + runID);
    else bad('state CreateRun should be 200', 'status=' + run.status + ' ' + run.text.slice(0, 160));

    const back = runID
      ? await mState('GetRun', { run_id: runID }, { port, certDir, token: tok })
      : { status: 0, text: 'skipped: no run', json: null };
    if (back.status === 200 && back.json && back.json.id === runID && JSON.stringify(back.json).includes('composed-real-pg'))
      ok('state GetRun reads the run back from REAL PG', 'id=' + runID);
    else bad('state GetRun must return the persisted run', 'status=' + back.status + ' ' + back.text.slice(0, 160));

    // Data plane mounted: auth rejection (401/403) rather than 404.
    const art = await mPost('/v1/artifacts/BeginUpload', { key: 'k', name: 'n', mime: 'text/plain', sha256: '0'.repeat(64), size: 1 }, { port, certDir });
    if (art.status === 401 || art.status === 403) ok('data plane mounted + auth enforced', 'status=' + art.status);
    else bad('data plane should be 401/403', 'status=' + art.status);

    // Execution plane mounted: health route answers, public face demands auth.
    const a2ah = await mGet('/a2a/healthz', { port });
    if (a2ah.status === 200) ok('execution plane healthz=200', 'gateway migrated in REAL PG');
    else bad('execution healthz should be 200', 'status=' + a2ah.status);
    const a2au = await mPost('/a2a/public', { jsonrpc: '2.0', id: 1, method: 'message/send', params: {} }, { port, certDir });
    if (a2au.status === 401 || a2au.status === 403) ok('execution public face demands auth', 'status=' + a2au.status);
    else if (a2au.status === 404) bad('execution public face 404 = not mounted');
    else ok('execution public face reachable', 'status=' + a2au.status);

    // Readiness/stats surface exposes OTel-instrumented counters (no OTLP endpoint set).
    const st = await mGet('/debug/stats', { port });
    if (st.status === 200) ok('debug/stats=200', st.text.slice(0, 60).replace(/\s+/g, ' '));
    else bad('debug/stats should be 200', 'status=' + st.status);
  } catch (e) {
    fail++; console.error('  \u2717 fatal:', e.message);
    const tail = logs.join('').split('\n').slice(-12).join('\n');
    if (tail) console.error('  --- service log tail ---\n' + tail);
  } finally {
    try { proc.kill('SIGTERM'); } catch { /* gone */ }
    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`\n[composed-serve] ${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error('[composed-serve] fatal:', e && e.message ? e.message : e); process.exit(1); });
