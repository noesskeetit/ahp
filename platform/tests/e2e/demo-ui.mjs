#!/usr/bin/env node
// tests/e2e/demo-ui.mjs -- Runnable UI/API serve for the user (delivery-owned).
//
// Boots the REAL feature-tagged all-role binary (bin/ahp-full) on the COMBAT
// dependencies (isolated PG, local combat MinIO :29000, real Emil :18000, real
// native-Qwen), then exposes a plain-HTTP loopback front (no mTLS ceremony for
// the user) whose endpoints drive the REAL pipeline. Nothing is mocked: a leg
// that cannot run reports a real failure / external-gate, never a synthesized
// pass. Per the root rule, combat-dep availability (HTTP 200) is surfaced as
// *availability*, never as task-execution proof.
//
// Modes:
//   npm run demo            -> serve dashboard + JSON API until SIGINT; print URL
//   node tests/e2e/demo-ui.mjs --selftest [model]
//                           -> boot, probe every plane, do a real create-run->PG
//                              round-trip, run the real Emil hop, print ONE
//                              DEMO_UI verdict, exit (0 iff planes mounted +
//                              run persisted + Emil surfaced). Pass `model` to
//                              also run the (slower) model->artifact->PG leg.
//
// Secrets/DSN are NEVER printed: only hosts, ports, ids, plane statuses.

import { spawn } from 'node:child_process';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { buildEnv } from '../../scripts/ahp-env.mjs';
import {
  PROJECT_ROOT, CERT_DIR, certPaths, pollReady, devSeedHex, uniqueDeployment,
  genClient, tokengen, mState,
} from '../helpers/ahp-client.mjs';

const SELFTEST = process.argv.includes('--selftest');
const WANT_MODEL = process.argv.includes('model') || process.env.AHP_DEMO_MODEL === '1';

const COMPOSED_PORT = Number(process.env.AHP_DEMO_COMPOSED_PORT || 29780);
const FRONT_PORT = Number(process.env.AHP_DEMO_FRONT_PORT || 29790);
const MINIO = (process.env.AHP_S3_ENDPOINT || 'http://127.0.0.1:29000').replace(/\/$/, '');
const EMIL = (process.env.AHP_EMIL_URL || 'http://127.0.0.1:18000').replace(/\/$/, '');
const PG_HOST = '127.0.0.1', PG_PORT = Number(process.env.AHP_DB_PORT || 25432);
const DB_NAME = process.env.AHP_DB_NAME || 'ahp_hive_delivery';

// Pinned composition (frozen this round per director). Plane -> source commit.
const PLAN = [
  ['Identity', 'a65ebc1', 'accepted'],
  ['Compute', '27cd4382', 'accepted'],
  ['State', '461aa2ce', 'accepted'],
  ['Space', 'cdfc20c', 'canonical (state-plane byte-identical to State)'],
  ['Execution', 'ecd5854', 'candidate (live Qwen 3/3)'],
  ['Data', '6da53cb', 'candidate'],
];
const GATES = [
  ['G-EMIL-TOOL', 'external/root', 'combat Emil-core returns MODEL_UNAVAILABLE; hop is wired, not mocked'],
  ['G-GOSDK', 'root', 'pre-seed go-sdk v1.8.0 leaf (internal/tools orphan) for whole-tree go build'],
  ['VM-plane', 'director', 'whole-system -race / Tier2A/2B L4 / OTLP target (Mac disk<20 gate)'],
];

// ---- small mTLS helpers (server-side; the front holds the client cert) ----
function mreq(method, path, { body, token } = {}) {
  const p = certPaths(CLIENT_DIR);
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  const opts = {
    host: '127.0.0.1', port: COMPOSED_PORT, method, path, headers,
    ca: readFileSync(p.ca), cert: readFileSync(p.cert), key: readFileSync(p.key),
    servername: 'localhost', rejectUnauthorized: true, timeout: 20000,
  };
  return new Promise((res, rej) => {
    const req = https.request(opts, (r) => {
      let t = ''; r.on('data', d => (t += d));
      r.on('end', () => { let j = null; try { j = t ? JSON.parse(t) : null; } catch {} res({ status: r.statusCode, text: t, json: j }); });
    });
    req.on('error', rej); req.on('timeout', () => req.destroy(new Error('timeout ' + path)));
    if (body !== undefined) req.write(JSON.stringify(body));
    req.end();
  });
}

// ---- combat-dep availability probe (NOT task-execution proof) ----
async function probe(name, fn) {
  try { return { name, ...(await fn()) }; }
  catch (e) { return { name, ok: false, error: e && e.message ? e.message : String(e) }; }
}
function tcpOpen(host, port, ms = 2500) {
  return new Promise((res, rej) => {
    const s = net.connect({ host, port }); const t = setTimeout(() => { s.destroy(); rej(new Error(`tcp ${host}:${port} timeout`)); }, ms);
    s.on('connect', () => { clearTimeout(t); s.destroy(); res(true); });
    s.on('error', (e) => { clearTimeout(t); rej(e); });
  });
}
async function http200(url) {
  const r = await fetch(url, { signal: AbortSignal.timeout(5000) });
  return { ok: r.status === 200, status: r.status };
}

async function probeDeps() {
  return await Promise.all([
    probe('PostgreSQL(isolated)', async () => ({ ok: await tcpOpen(PG_HOST, PG_PORT), note: `db=${DB_NAME}` })),
    probe('MinIO(local combat)', () => http200(MINIO + '/minio/health/live')),
    probe('Emil(combat A2A)', async () => { const r = await http200(EMIL + '/.well-known/agent-card.json'); return { ...r, note: EMIL }; }),
    probe('native-Qwen-assert(G0)', async () => ({ ok: existsSync(process.env.AHP_NATIVE_QWEN_ASSERT || join(PROJECT_ROOT, 'scripts/native-qwen-gate.sh')) })),
    probe('bin/ahp-full', async () => ({ ok: existsSync(join(PROJECT_ROOT, 'bin', 'ahp-full')) })),
  ]);
}

// ---- plane mount probe on the LIVE composed serve ----
async function probePlanes() {
  const out = [];
  const push = async (plane, fn) => { try { out.push(await fn()); } catch (e) { out.push({ plane, mounted: false, error: e.message }); } };
  await push('readiness', async () => { const r = await mreq('GET', '/readyz'); return { plane: 'readiness', mounted: r.status === 200, status: r.status }; });
  await push('identity', async () => { const r = await mreq('POST', '/v1/identity/roots', { body: { task: 'x', context: 'x' } }); return { plane: 'identity', mounted: r.status !== 404, status: r.status }; });
  await push('state', async () => { const r = await mState('GetRun', { run_id: 'noop' }, { port: COMPOSED_PORT, certDir: CLIENT_DIR, token: TOK_TASK }); return { plane: 'state', mounted: true, status: r.status }; });
  await push('data(artifacts)', async () => { const r = await mreq('POST', '/v1/artifacts/BeginUpload', { body: { key: 'k', name: 'n', mime: 'text/plain', sha256: '0'.repeat(64), size: 1 } }); return { plane: 'data(artifacts)', mounted: r.status !== 404, status: r.status }; });
  await push('execution(a2a)', async () => { const r = await mreq('GET', '/a2a/healthz'); return { plane: 'execution(a2a)', mounted: r.status === 200, status: r.status }; });
  return out;
}

// ---- real create-run -> PG -> read back (no mocks) ----
async function realRun() {
  const ctx = 'demo-ctx-' + Date.now().toString(36);
  const dep = uniqueDeployment('demo');
  const cd = genClient('dely', dep);
  const tok = tokengen({ tenant: 'dely', deployment: dep, audience: 'ahp', kind: 'task', seed: SEED });
  const thr = await mState('CreateThread', { context_id: ctx, key: 'demo/thread' }, { port: COMPOSED_PORT, certDir: cd, token: tok });
  const threadID = thr.json && thr.json.id ? thr.json.id : '';
  if (!threadID) return { ok: false, stage: 'CreateThread', status: thr.status, text: thr.text.slice(0, 160) };
  const payload = { prompt: 'demo-run-' + Date.now().toString(36) };
  const run = await mState('CreateRun', { thread_id: threadID, key: 'demo/run', payload }, { port: COMPOSED_PORT, certDir: cd, token: tok });
  const runID = run.json && run.json.id ? run.json.id : '';
  if (run.status !== 200 || !runID) return { ok: false, stage: 'CreateRun', status: run.status, text: run.text.slice(0, 160) };
  const back = await mState('GetRun', { run_id: runID }, { port: COMPOSED_PORT, certDir: cd, token: tok });
  const persisted = back.status === 200 && back.json && JSON.stringify(back.json).includes(payload.prompt);
  return { ok: persisted, runID, threadID, persisted, readStatus: back.status };
}

// ---- spawn a proven e2e leg (serialized), parse its honest verdict ----
let queue = Promise.resolve();
function serialize(fn) { const r = queue.then(fn, fn); queue = r.then(() => {}, () => {}); return r; }
function runLeg(script, env = {}) {
  return serialize(() => new Promise((res) => {
    const proc = spawn(process.execPath, [join(PROJECT_ROOT, 'tests', 'e2e', script)], {
      cwd: PROJECT_ROOT, env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'],
    });
    let text = ''; proc.stdout.on('data', d => (text += d)); proc.stderr.on('data', d => (text += d));
    proc.on('close', (code) => res({ script, exit: code, text, tail: text.split('\n').slice(-15).join('\n') }));
  }));
}
function parseEmil(text) { return { verdict: (text.match(/EMIL_TOOL_HOP=(\S+)/) || [])[1] || 'unknown', taskId: (text.match(/taskId=([0-9a-fA-F-]+)/) || [])[1] || '' }; }
function parseCounts(text) { const m = text.match(/(\d+)\s+passed,\s*(\d+)\s+failed/); return m ? { passed: +m[1], failed: +m[2] } : { passed: 0, failed: 999 }; }

// ---- front (plain HTTP) ----
let CLIENT_DIR = 'client', TOK_TASK = '', SEED = '';
async function handle(req, res) {
  const u = new URL(req.url, 'http://x'); const p = u.pathname;
  const send = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json', 'access-control-allow-origin': '*' }); res.end(JSON.stringify(obj)); };
  try {
    if (req.method === 'OPTIONS') { res.writeHead(204, { 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type', 'access-control-allow-methods': 'GET,POST,OPTIONS' }); return res.end(); }
    if (p === '/' ) { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' }); return res.end(HTML); }
    if (p === '/api/plan') return send(200, { status: 'unaccepted-prototype', planes: PLAN, gates: GATES, composed: 'bin/ahp-full role=all :' + COMPOSED_PORT });
    if (p === '/api/deps') return send(200, { note: 'availability only, NOT task-execution proof', deps: await probeDeps() });
    if (p === '/api/planes') return send(200, { planes: await probePlanes() });
    if (p === '/api/run' && req.method === 'POST') return send(200, await realRun());
    if (p === '/api/emil-hop' && req.method === 'POST') { const r = await runLeg('emil-tool-hop.mjs', { AHP_EMIL_URL: EMIL }); return send(200, { ...parseEmil(r.text), exit: r.exit, tail: r.tail }); }
    if (p === '/api/model-artifact' && req.method === 'POST') { const r = await runLeg('model-to-artifact.mjs'); return send(200, { ...parseCounts(r.text), exit: r.exit, tail: r.tail }); }
    if (p === '/api/journey' && req.method === 'POST') { const r = await runLeg('journey.mjs'); return send(200, { verdict: (r.text.match(/JOURNEY=(\S+)/) || [])[1] || 'unknown', exit: r.exit, tail: r.tail }); }
    if (p === '/api/composed-serve' && req.method === 'POST') { const r = await runLeg('composed-serve.mjs'); return send(200, { ...parseCounts(r.text), exit: r.exit, tail: r.tail }); }
    res.writeHead(404); res.end('not found');
  } catch (e) { send(500, { error: e && e.message ? e.message : String(e) }); }
}

const HTML = `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>AHP prototype demo (UNACCEPTED)</title>
<style>body{font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif;margin:24px;color:#15202b;background:#f6f8fa}
.banner{background:#b45309;color:#fff;padding:10px 14px;border-radius:8px;margin-bottom:16px;font-weight:600}
h1{font-size:20px;margin:0 0 6px}h2{font-size:15px;margin:22px 0 8px;color:#374151}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px}
.card{background:#fff;border:1px solid #d0d7de;border-radius:8px;padding:12px}
.card h3{margin:0 0 6px;font-size:13px}.ok{color:#15803d}.bad{color:#b91c1c}.gate{color:#b45309}
button{cursor:pointer;border:1px solid #0969da;background:#0969da;color:#fff;border-radius:6px;padding:6px 10px;margin-top:8px}
pre{background:#0d1117;color:#e6edf3;padding:10px;border-radius:6px;white-space:pre-wrap;font-size:12px;overflow:auto;max-height:260px}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}</style></head><body>
<div class="banner">UNACCEPTED prototype (root has NOT accepted). Real combat deps, mocks=NONE. Whole-journey completion is gated by Emil-core backend (external).</div>
<h1>AHP Agents Space — runnable prototype UI/API</h1>
<div>composed all-role serve (bin/ahp-full) on isolated PG + local combat MinIO + combat Emil + real native-Qwen. Plain-HTTP front proxying the mTLS pipeline.</div>
<h2>Composition (frozen this round)</h2><div id="plan" class="grid"></div>
<h2>Combat dependency liveness <span class="gate">(availability only — NOT task-execution proof)</span></h2>
<button onclick="load('deps','/api/deps',renderDeps)">Probe deps</button><div id="deps" class="grid"></div>
<h2>Live plane mount (on the running composed serve)</h2>
<button onclick="load('planes','/api/planes',renderPlanes)">Probe planes</button><div id="planes" class="grid"></div>
<h2>Real pipeline actions (no mocks)</h2>
<div class="card"><h3>create-run &rarr; PG state</h3>Real CreateThread+CreateRun+GetRun round-trip on isolated PostgreSQL.<button onclick="run('/api/run',renderOut)">Run create-run</button><div id="out"></div></div>
<div class="card"><h3>model &rarr; Emil-tool hop (A2A)</h3>Real Emil-core A2A message/send. Completed=&gt;green; MODEL_UNAVAILABLE=&gt;external-gate (never faked).<button onclick="run('/api/emil-hop',renderOut)">Run Emil hop</button><div id="emil"></div></div>
<div class="card"><h3>model &rarr; artifact &rarr; PG state</h3>Real native-Qwen &rarr; versioned artifact in MinIO &rarr; ref in PG (slower).<button onclick="run('/api/model-artifact',renderOut)">Run model leg</button><div id="ma"></div></div>
<div class="card"><h3>Whole journey orchestrator</h3>Runs proven legs, prints one honest composed verdict.<button onclick="run('/api/journey',renderOut)">Run journey</button><div id="jn"></div></div>
<div id="raw"></div>
<script>
async function get(u){var r=await fetch(u);return await r.json()}
function el(id){return document.getElementById(id)}
function load(id,u,fn){el(id).innerHTML='<i>...</i>';get(u).then(function(j){fn(el(id),j)}).catch(function(e){el(id).innerHTML='<span class=bad>'+e+'</span>'})}
function run(u,fn){el('raw').innerHTML='<pre>running '+u+' ...</pre>';fetch(u,{method:'POST'}).then(function(r){return r.json()}).then(function(j){var k={'/api/run':'out','/api/emil-hop':'emil','/api/model-artifact':'ma','/api/journey':'jn'}[u];fn(el(k||'out'),j);el('raw').innerHTML='<pre>'+JSON.stringify(j,null,1).replace(/[<>&]/g,'')+'</pre>'}).catch(function(e){el('raw').innerHTML='<pre class=bad>'+e+'</pre>'})}
get('/api/plan').then(function(j){el('plan').innerHTML=j.planes.map(function(p){return '<div class=card><h3>'+p[0]+' <span class=mono>'+p[1]+'</span></h3>'+p[2]+'</div>'}).join('')+'<div class=card><h3>Open gates</h3>'+j.gates.map(function(g){return '<div class=gate><b>'+g[0]+'</b> ['+g[1]+'] '+g[2]+'</div>'}).join('')+'</div>'})
function renderDeps(node,j){node.innerHTML=j.deps.map(function(d){var cls=d.ok?'ok':'bad';return '<div class=card><h3>'+d.name+'</h3><span class='+cls+'>'+(d.ok?'UP':'DOWN')+'</span> '+(d.status||'')+(d.note?' <span class=mono>'+d.note+'</span>':'')+'</div>'}).join('')}
function renderPlanes(node,j){node.innerHTML=j.planes.map(function(p){var cls=p.mounted?'ok':'bad';return '<div class=card><h3>'+p.plane+'</h3><span class='+cls+'>'+(p.mounted?'MOUNTED':'NOT MOUNTED')+'</span> '+p.status+(p.error?' '+p.error:'')+'</div>'}).join('')}
function renderOut(node,j){node.innerHTML='<pre>'+JSON.stringify(j).replace(/[<>&]/g,'')+'</pre>'}
load('deps','/api/deps',renderDeps);load('planes','/api/planes',renderPlanes);
</script></body></html>`;

// ---- boot + run ----
async function main() {
  const env = await buildEnv(PROJECT_ROOT);
  SEED = devSeedHex();
  const bucket = `ahp-delivery-demo-${Date.now().toString(36)}`;
  const svcEnv = {
    ...env,
    HTTP_PROXY: '', HTTPS_PROXY: '', http_proxy: '', https_proxy: '', NO_PROXY: 'localhost,127.0.0.1,::1',
    AHP_ROLE: 'all', AHP_ENABLE_DATA: '1', AHP_ENABLE_EXECUTION: '1',
    AHP_LISTEN: ':' + COMPOSED_PORT, AHP_DEV_JWT_SEED: SEED,
    AHP_TLS_CERT: join(CERT_DIR, 'server', 'server.crt'), AHP_TLS_KEY: join(CERT_DIR, 'server', 'server.key'), AHP_TLS_CA: join(CERT_DIR, 'ca', 'ca.crt'),
    OTEL_EXPORTER_OTLP_ENDPOINT: '', AHP_S3_ENDPOINT: MINIO, AHP_S3_BUCKET: bucket, AHP_S3_REGION: env.AHP_S3_REGION || 'us-east-1',
  };
  const bin = join(PROJECT_ROOT, 'bin', 'ahp-full');
  console.log('[demo-ui] booting composed all-role on combat deps  db=' + env.AHP_DB_NAME + '  minio=' + new URL(MINIO).host + '  emil=' + EMIL);
  const proc = spawn(bin, [], { cwd: PROJECT_ROOT, env: svcEnv, stdio: ['ignore', 'pipe', 'pipe'] });
  const logs = []; proc.stdout.on('data', d => logs.push(String(d))); proc.stderr.on('data', d => logs.push(String(d)));
  const shutdown = () => { try { proc.kill('SIGTERM'); } catch {} };
  process.on('SIGINT', () => { console.log('\n[demo-ui] SIGINT'); shutdown(); process.exit(130); });

  try {
    await pollReady(COMPOSED_PORT, { tls: true, timeoutMs: 45000 });
  } catch (e) {
    console.error('[demo-ui] composed serve did not become ready:', e.message);
    console.error(logs.join('').split('\n').slice(-15).join('\n'));
    shutdown(); process.exit(1);
  }
  const dep = uniqueDeployment('demofront');
  CLIENT_DIR = genClient('dely', dep);
  TOK_TASK = tokengen({ tenant: 'dely', deployment: dep, audience: 'ahp', kind: 'task', seed: SEED });
  const front = http.createServer(handle);
  await new Promise(r => front.listen(FRONT_PORT, '127.0.0.1', r));

  const url = `http://127.0.0.1:${FRONT_PORT}/`;
  console.log('[demo-ui] SERVE  dashboard+API -> ' + url);
  console.log('[demo-ui]   run:      npm run demo        verify: node tests/e2e/demo-ui.mjs --selftest');

  if (!SELFTEST) { return; /* keep alive */ }

  // selftest: plane mount + real create-run->PG + real Emil hop (+ optional model leg)
  const planes = await probePlanes();
  const allMounted = planes.every(p => p.mounted);
  const runRes = await realRun();
  const emil = parseEmil((await runLeg('emil-tool-hop.mjs', { AHP_EMIL_URL: EMIL })).text);
  let model = null;
  if (WANT_MODEL) model = parseCounts((await runLeg('model-to-artifact.mjs')).text);

  console.log('\n===== DEMO_UI SELFTEST =====');
  console.log('  planes: ' + planes.map(p => `${p.plane}=${p.mounted ? 'MOUNTED' : 'DOWN'}`).join(' '));
  console.log(`  create-run->PG: ${runRes.ok ? 'PERSISTED' : 'FAIL'} run=${runRes.runID || '-'}`);
  console.log(`  emil-hop: ${emil.verdict}${emil.taskId ? ` task=${emil.taskId}` : ''}`);
  if (model) console.log(`  model->artifact->PG: passed=${model.passed} failed=${model.failed}`);

  let verdict, exit;
  const emilOk = emil.verdict === 'green' || emil.verdict.startsWith('external-gate');
  const modelOk = !model || model.failed === 0;
  if (!allMounted || !runRes.ok || !emilOk || !modelOk) { verdict = 'red'; exit = 1; }
  else if (emil.verdict !== 'green') { verdict = 'green-except-emil-tool'; exit = 0; }
  else { verdict = 'green'; exit = 0; }
  console.log('DEMO_UI=' + verdict);
  shutdown(); front.close(); process.exit(exit);
}

main().catch(e => { console.error('[demo-ui] fatal:', e && e.message ? e.message : e); process.exit(1); });
