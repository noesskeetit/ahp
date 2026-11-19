#!/usr/bin/env node
// tests/e2e/data-plane.mjs -- REAL Data-plane versioned-artifact lifecycle (O1/L4).
//
// Proves the versioned-artifact segment of the AHP journey against a REAL MinIO
// object store and the block-isolated PostgreSQL metadata plane:
//   BeginUpload -> presigned PUT (real bytes) -> Seal (version) ->
//   Download (sha256 + Range) -> Get -> foreign-tenant deny -> Delete -> 404.
//
// NO MOCKS: the bytes travel through the real MinIO presigned-PUT/GET path and
// the artifact rows live in the isolated PG. If a required REAL service is
// absent the test FAILS (it never skips to a false pass). This is the segment
// the architect flagged as previously over-claimed with httptest/mock state.
//
// DEV-only: real mTLS + bound KindTask tokens; secrets are never printed.

import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import https from 'node:https';
import http from 'node:http';
import { join } from 'node:path';
import { buildEnv } from '../../scripts/ahp-env.mjs';
import {
  PROJECT_ROOT, CERT_DIR, certPaths, pollReady, assert,
  genClient, uniqueDeployment, devSeedHex, tokengen,
} from '../helpers/ahp-client.mjs';

let pass = 0, fail = 0;
const ok = (n, x = '') => { pass++; console.log(`  \u2713 ${n}${x ? '  ' + x : ''}`); };
const bad = (n, x = '') => { fail++; console.log(`  \u2717 ${n}${x ? '  ' + x : ''}`); };
const sha256hex = (b) => createHash('sha256').update(b).digest('hex');

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

async function main() {
  const env = await buildEnv(PROJECT_ROOT);
  const minioEndpoint = process.env.AHP_S3_ENDPOINT || 'http://127.0.0.1:29000';
  const user = env.AHP_MINIO_ROOT_USER || process.env.AHP_MINIO_ROOT_USER;
  const passw = env.AHP_MINIO_ROOT_PASSWORD || process.env.AHP_MINIO_ROOT_PASSWORD;
  const bucket = process.env.AHP_S3_BUCKET || `ahp-delivery-o1-${Date.now().toString(36)}`;
  const port = Number(process.env.AHP_E2E_DATA_PORT || 29741);
  const mhost = new URL(minioEndpoint).host;

  console.log('[data-plane] REAL MinIO + isolated PG (no mocks; secrets hidden)');
  console.log(`  minio host=${mhost}  bucket=${bucket}  role=all  ahp_data tag  db=${env.AHP_DB_NAME}`);

  // Precondition 1: the REAL MinIO must answer health/live (fail, never skip).
  const alive = await new Promise((res) => {
    const req = http.get(minioEndpoint.replace(/\/$/, '') + '/minio/health/live', { timeout: 4000 },
      (r) => { r.resume(); res(r.statusCode === 200); });
    req.on('error', () => res(false)); req.on('timeout', () => { req.destroy(); res(false); });
  });
  assert(alive, `REAL MinIO unreachable at ${mhost} (health/live != 200). Refusing to fake data-plane proof.`);
  assert(user && passw, 'MinIO root credentials must be present in env (not printed).');
  ok('real MinIO health/live=200', mhost);

  // Build the data-tagged binary if absent (ambient build env; warm cache on Mac).
  const bin = join(PROJECT_ROOT, 'bin', 'ahp-dataplane');
  if (!existsSync(bin)) {
    console.log('[data-plane] building bin/ahp-dataplane -tags ahp_data');
    execFileSync('go', ['build', '-tags', 'ahp_data', '-o', 'bin/ahp-dataplane', './cmd/ahp'],
      { cwd: PROJECT_ROOT, env: process.env, stdio: 'inherit' });
  }
  assert(existsSync(bin), 'bin/ahp-dataplane present');

  // Spawn the composed serve (role=all) with the REAL data plane mounted.
  const svcEnv = {
    ...env,
    AHP_ROLE: 'all', AHP_ENABLE_DATA: '1', AHP_LISTEN: `:${port}`,
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

    // Owner identity (cert SPIFFE tenant/deployment == bound token).
    const oTenant = 'dely', oDep = uniqueDeployment('o1');
    const oDir = genClient(oTenant, oDep);
    const oTok = tokengen({ tenant: oTenant, deployment: oDep, audience: 'ahp', kind: 'task' });
    // Foreign identity (different tenant/deployment).
    const fTenant = 'forgn', fDep = uniqueDeployment('fx');
    const fDir = genClient(fTenant, fDep);
    const fTok = tokengen({ tenant: fTenant, deployment: fDep, audience: 'ahp', kind: 'task' });

    // (1) no-token BeginUpload: must be an AUTH rejection (401/403), NOT 404
    //     (404 would mean the data plane is not mounted -> not a pass).
    const noTok = await mArtifacts('BeginUpload', { key: 'k0', name: 'n', mime: 'text/plain', sha256: '0'.repeat(64), size: 1 }, { port, certDir: oDir });
    if (noTok.status === 401 || noTok.status === 403) ok('no-token BeginUpload rejected', 'status=' + noTok.status + ' (plane is mounted, auth enforced)');
    else bad('no-token BeginUpload should be 401/403', 'got=' + noTok.status);

    // My OWN payload (not any fixture artifact).
    const myBytes = Buffer.concat([
      Buffer.from('ahp-delivery-O1-artifact-v1:'),
      Buffer.from(Array.from({ length: 97 }, (_, i) => (i * 7 + 3) & 0xff)),
    ]);
    const mySha = sha256hex(myBytes);
    const mySize = myBytes.length;

    // (2) BeginUpload
    const beg = await mArtifacts('BeginUpload', { key: 'o1/report.bin', name: 'report.bin', mime: 'application/octet-stream', sha256: mySha, size: mySize }, { port, certDir: oDir, token: oTok });
    if (beg.status !== 200 || !beg.json?.id || !beg.json?.presigned_url) { bad('BeginUpload 200+id+presigned_url', 'got=' + beg.status + ' ' + beg.text.slice(0, 160)); throw new Error('BeginUpload failed'); }
    ok('BeginUpload', 'id=' + beg.json.id + ' presigned_url host=' + new URL(beg.json.presigned_url).host);

    // (3) presigned PUT of REAL bytes to REAL MinIO
    const put = await presignedPut(beg.json.presigned_url, myBytes);
    if (put !== 200) bad('presigned PUT 200', 'got=' + put); else ok('presigned PUT -> REAL MinIO', 'status=' + put);

    // (4) Seal -> versioned Ref
    const seal = await mArtifacts('Seal', { id: beg.json.id }, { port, certDir: oDir, token: oTok });
    const sealed = seal.status === 200 && seal.json?.version && seal.json?.sha256 === mySha;
    if (!sealed) bad('Seal 200 version+sha', 'got=' + seal.status + ' ' + seal.text.slice(0, 200));
    else ok('Seal -> versioned artifact', 'version=' + seal.json.version + ' tenant=' + seal.json.tenant + ' sha256=' + seal.json.sha256.slice(0, 12) + '..');

    // (5) Download full -> sha match + Accept-Ranges
    const dl = await mArtifacts('Download', { id: beg.json.id }, { port, certDir: oDir, token: oTok });
    if (dl.status === 200 && sha256hex(dl.buffer) === mySha && dl.headers['accept-ranges'] === 'bytes') ok('Download full -> sha256 match + Accept-Ranges', 'bytes=' + dl.buffer.length);
    else bad('Download full sha/ranges', 'status=' + dl.status + ' accept-ranges=' + dl.headers['accept-ranges'] + ' sha=' + (dl.buffer.length ? sha256hex(dl.buffer).slice(0, 12) : 'empty'));

    // (6) Download Range bytes=0-4 -> 206 + Content-Range + 5 bytes
    const dr = await mArtifacts('Download', { id: beg.json.id }, { port, certDir: oDir, token: oTok, headers: { Range: 'bytes=0-4' } });
    if (dr.status === 206 && dr.buffer.length === 5 && dr.buffer.equals(myBytes.subarray(0, 5)) && String(dr.headers['content-range'] || '').startsWith('bytes 0-4/')) ok('Download Range 0-4 -> 206 + 5 bytes', 'content-range=' + dr.headers['content-range']);
    else bad('Download Range 0-4', 'status=' + dr.status + ' len=' + dr.buffer.length + ' content-range=' + dr.headers['content-range']);

    // (7) Get metadata
    const get = await mArtifacts('Get', { id: beg.json.id }, { port, certDir: oDir, token: oTok });
    if (get.status === 200 && get.json?.id === beg.json.id && get.json?.size === mySize) ok('Get metadata', 'size=' + get.json.size);
    else bad('Get metadata', 'status=' + get.status + ' ' + get.text.slice(0, 160));

    // (8) foreign-tenant Download -> deny (403/404)
    const frn = await mArtifacts('Download', { id: beg.json.id }, { port, certDir: fDir, token: fTok });
    if (frn.status === 403 || frn.status === 404) ok('foreign tenant denied', 'status=' + frn.status);
    else bad('foreign tenant should be 403/404', 'got=' + frn.status);

    // (9) Delete (operator requires a 1..128 byte idempotency key)
    const del = await mArtifacts('Delete', { id: beg.json.id, key: 'o1-delete-' + Date.now().toString(36) }, { port, certDir: oDir, token: oTok });
    if (del.status === 200 && del.json?.ok === true) ok('Delete (soft)');
    else bad('Delete', 'status=' + del.status + ' ' + del.text.slice(0, 160));

    // (10) Download after Delete -> 404
    const gone = await mArtifacts('Download', { id: beg.json.id }, { port, certDir: oDir, token: oTok });
    if (gone.status === 404) ok('Download after Delete -> 404');
    else bad('Download after Delete should be 404', 'got=' + gone.status);
  } catch (e) {
    fail++; console.error('  \u2717 fatal:', e.message);
    const L = logs.join('');
    if (/not mounted|feature/i.test(L)) console.error('  note: data plane mount warning present in service logs');
  } finally {
    kill();
    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`\n[data-plane] ${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error('[data-plane] fatal:', e); process.exit(1); });
