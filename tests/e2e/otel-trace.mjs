#!/usr/bin/env node
// tests/e2e/otel-trace.mjs -- OpenTelemetry tracing observed on LIVE serve traffic.
//
// Acceptance link: PDF rev3.2 observability requirement -> every service role
// emits OTel traces (server spans) with cardinality-bounded names and HTTP
// attributes. This test does NOT trust the unit test alone: it boots the real
// all-role binary with the offline OTLP exporter (no endpoint => one JSON
// object per span on stderr), drives real requests (readiness on real PG, a
// real state CreateThread write on real PG), then SIGTERMs the process so the
// batch processor flushes pending spans, and parses the stderr span stream.
//
// It proves: (1) tracing wraps actual served requests, not just a synthetic
// handler; (2) span names are cardinality-bounded (no raw ids); (3) HTTP
// status/method attributes are recorded. Secrets/DSN values are never printed.

import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { buildEnv } from '../../scripts/ahp-env.mjs';
import {
  PROJECT_ROOT, CERT_DIR, pollReady, assert,
  genClient, uniqueDeployment, devSeedHex, tokengen, mState,
} from '../helpers/ahp-client.mjs';

let pass = 0, fail = 0;
const ok = (n, x = '') => { pass++; console.log(`  \u2713 ${n}${x ? '  ' + x : ''}`); };
const bad = (n, x = '') => { fail++; console.log(`  \u2717 ${n}${x ? '  ' + x : ''}`); };

// Parse the stderr stream into OTel span objects: lines that are a JSON object
// carrying a trace_id. slog log lines fail JSON.parse or lack trace_id and drop.
function extractSpans(raw) {
  const spans = [];
  for (const line of raw.split('\n')) {
    const t = line.trim();
    if (!t.startsWith('{')) continue;
    try {
      const o = JSON.parse(t);
      if (o && typeof o === 'object' && typeof o.trace_id === 'string' && o.trace_id) spans.push(o);
    } catch { /* not a span line */ }
  }
  return spans;
}

const HEX = (s, n) => typeof s === 'string' && new RegExp(`^[0-9a-f]{${n}}$`).test(s);

async function main() {
  const env = await buildEnv(PROJECT_ROOT);
  const port = Number(process.env.AHP_E2E_OTEL_PORT || 29755);
  const bucket = `ahp-delivery-otel-${Date.now().toString(36)}`;

  // Same hardening as composed-serve: clear the dead ambient proxy; offline OTLP
  // (empty endpoint) makes the in-house exporter emit span JSON to stderr.
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

  console.log('[otel-trace] all-role serve, offline OTLP -> stderr span JSON');
  console.log(`  db=${env.AHP_DB_NAME}  role=all  otlp=offline(stderr)`);

  let stderr = '';
  const logs = [];
  const proc = spawn(join(PROJECT_ROOT, 'bin', 'ahp-full'), [], { cwd: PROJECT_ROOT, env: svcEnv, stdio: ['ignore', 'pipe', 'pipe'] });
  proc.stdout.on('data', d => logs.push(String(d)));
  proc.stderr.on('data', d => { stderr += String(d); });
  let closed = false;
  proc.on('close', () => { closed = true; });

  try {
    await pollReady(port, { tls: true, timeoutMs: 45000 });
    ok('serve /readyz=200', 'real PG ping');

    // Drive real traced traffic: readiness + a real state write on real PG.
    const tenant = 'dely', dep = uniqueDeployment('ot');
    const certDir = genClient(tenant, dep);
    const tok = tokengen({ tenant, deployment: dep, audience: 'ahp', kind: 'task' });

    const ctxID = 'delivery-otel-ctx-' + Date.now().toString(36);
    const thr = await mState('CreateThread', { context_id: ctxID, key: 'delivery/otel' }, { port, certDir, token: tok });
    const threadID = thr.json && thr.json.id ? thr.json.id : '';
    if (thr.status === 200 && threadID) ok('state CreateThread traced write on REAL PG', 'id=' + threadID);
    else bad('state CreateThread should be 200', 'status=' + thr.status + ' ' + String(thr.text).slice(0, 160));

    // Give the spans a moment to be End()-ed (they already are once responses
    // returned), then SIGTERM so tp.Shutdown flushes the pending batch to stderr.
    proc.kill('SIGTERM');
    await new Promise((res) => { const done = () => res(); if (closed) return done(); proc.on('close', done); setTimeout(done, 4000); });

    const spans = extractSpans(stderr);
    const server = spans.filter(s => s.kind === 'server');

    assert(spans.length >= 1, 'no span JSON objects found on stderr (offline exporter silent?)');
    ok('offline OTLP exporter emitted span JSON', `spans=${spans.length}`);

    assert(server.length >= 1, 'no server-kind spans (middleware not tracing served requests)');
    ok('server spans present', 'server=' + server.length);

    // Tracing observed the readiness probe as a real server span.
    const ready = server.find(s => s.name === 'GET /readyz');
    assert(ready, 'no server span named "GET /readyz" (readiness not traced)');
    assert(ready.attributes && ready.attributes['http.status_code'] === 200, 'readyz span missing 200 status attribute');
    ok('readyz traced', 'status=' + ready.attributes['http.status_code'] + ' service=' + ready.attributes.service);

    // Tracing observed the real state write; name is cardinality-bounded.
    const st = server.find(s => s.name === 'POST /v1/state/*');
    assert(st, 'no server span named "POST /v1/state/*" (state write not traced/wildcarded)');
    assert(st.attributes && st.attributes['http.method'] === 'POST', 'state span missing method attribute');
    assert(st.attributes && typeof st.attributes['http.status_code'] === 'number', 'state span missing status attribute');
    ok('state write traced + wildcarded', 'name=' + st.name + ' status=' + st.attributes['http.status_code']);

    // Every server span carries method + status + service and valid ids.
    let attrBad = 0, idBad = 0;
    for (const s of server) {
      const a = s.attributes || {};
      if (!a['http.method'] || typeof a['http.status_code'] !== 'number' || !a.service) attrBad++;
      if (!HEX(s.trace_id, 32) || !HEX(s.span_id, 16)) idBad++;
    }
    assert(attrBad === 0, `${attrBad} server spans missing http.method/http.status_code/service`);
    ok('every server span has method+status+service attrs');
    assert(idBad === 0, `${idBad} server spans with malformed trace_id/span_id`);
    ok('trace_id(32hex)/span_id(16hex) well-formed');

    // Cardinality bound: no server span name embeds a raw resource id (UUID).
    const leak = server.filter(s => /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}/.test(s.name));
    assert(leak.length === 0, `${leak.length} server span names leak a high-cardinality id: ` + leak.map(s => s.name).join(', '));
    ok('span names cardinality-bounded (no raw ids)');
  } catch (e) {
    fail++; console.error('  \u2717 fatal:', e.message);
    const tail = (stderr || logs.join('')).split('\n').filter(l => l.trim()).slice(-12).join('\n');
    if (tail) console.error('  --- service stderr tail ---\n' + tail);
  } finally {
    try { proc.kill('SIGTERM'); } catch { /* gone */ }
    await new Promise(r => setTimeout(r, 200));
  }

  console.log(`\n[otel-trace] ${pass} passed, ${fail} failed`);
  process.exit(fail === 0 ? 0 : 1);
}

main().catch((e) => { console.error('[otel-trace] fatal:', e && e.message ? e.message : e); process.exit(1); });
