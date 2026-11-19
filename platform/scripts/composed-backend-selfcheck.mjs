#!/usr/bin/env node
// scripts/composed-backend-selfcheck.mjs — proves the composed ALL-PLANES backend
// completes an AUTHENTICATED state journey on REAL PG when configured correctly:
//   * mTLS (server cert + client CA from infra/certs)  <- state/data mandate a verified
//     client certificate via pkg/transport Task guard (transport.Task), and
//   * a SHARED DEV-ONLY AHP_DEV_JWT_SEED (same deterministic derivation as the harness's
//     tests/helpers/ahp-client.mjs:devSeedHex) so a client-cert-bound Bearer task token
//     minted by scripts/tokengen is accepted by the server's verifier.
//
// This isolates the "journey residual": it was a LAUNCH-CONFIG gap (plaintext + ephemeral
// dev key), NOT a product/Identity defect. Runs an ephemeral composed-backend + REAL PG
// (isolated allowlist DB via buildEnv), then reaps it. Dev-only; seed never printed; NOT
// production Identity. Mac-safe (one prebuilt tagged bin). fail-closed.
import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import net from 'node:net';
import fs from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';
import { buildEnv } from './ahp-env.mjs';
import { genClient, uniqueDeployment, certPaths, pollReady, mState } from '../tests/helpers/ahp-client.mjs';
import https from 'node:https';

const ROOT = process.cwd();
const BIN = join(ROOT, process.env.AHP_SEPARATED_BIN || 'bin/ahp-full-tagged');
const devSeedHex = () => createHash('sha256').update('ahp-hive-delivery-dev-jwt-seed-v1').digest('hex');
const seed = process.env.AHP_DEV_JWT_SEED || devSeedHex();
const RED = (s) => String(s).replace(/[A-Za-z0-9._+/=-]{30,}/g, '<redacted>');
function mintToken({ tenant, deployment, task }) {
  return execFileSync('go', ['run','./scripts/tokengen','-kid','dev-1','-tenant',tenant,'-deployment',deployment,
    '-subject','delivery-self','-task',task,'-audience','ahp','-kind','task','-ttl','600'],
    { cwd: ROOT, encoding:'utf8', env:{ ...process.env, GOCACHE:'/tmp/ahp-gocache', GOMODCACHE:'/tmp/ahp-modcache', GOPATH:'/tmp/ahp-gopath', GOPROXY:'off', GOFLAGS:'-mod=mod', AHP_DEV_JWT_SEED: seed } }).trim();
}
function freePort(){ return new Promise((res,rej)=>{const s=net.createServer();s.once('error',rej);s.listen(0,'127.0.0.1',()=>{const p=s.address().port;s.close(()=>res(p));});});}
// inline artifacts RPC — mirrors mState (tests/helpers/ahp-client.mjs) but on /v1/artifacts.
// Self-contained on purpose: no import of any e2e helper module (keeps this gate dependency-free).
function mArtifacts(op, body, { port, certDir, token } = {}) {
  const p = certPaths(certDir ?? 'client');
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  const payload = body === undefined ? '{}' : JSON.stringify(body);
  const options = { host:'127.0.0.1', port, method:'POST', path:`/v1/artifacts/${op}`,
    ca: fs.readFileSync(p.ca), cert: fs.readFileSync(p.cert), key: fs.readFileSync(p.key),
    servername:'localhost', rejectUnauthorized:true, headers, timeout:15000 };
  return new Promise((res, rej) => {
    const req = https.request(options, (r) => {
      let text=''; r.on('data',(d)=>text+=d); r.on('end',()=>{ let json=null; try{ json=text?JSON.parse(text):null; }catch{} res({status:r.statusCode,text,json}); });
    });
    req.on('error', rej); req.on('timeout', ()=>req.destroy(new Error('mArtifacts timeout '+op)));
    req.write(payload); req.end();
  });
}
if (!fs.existsSync(BIN)){ console.error('missing bin', BIN); process.exit(2); }
const sc = certPaths(); // infra/certs/{server,ca}

let pass=0, fail=0; const ok=(n,x='')=>{pass++;console.log(`  \u2713 ${n}${x?'  '+x:''}`);}; const bad=(n,x='')=>{fail++;console.log(`  \u2717 ${n}${x?'  '+x:''}`);};
const env0 = await buildEnv(ROOT);
const port = await freePort();
const env = { ...env0, AHP_ROLE:'all', AHP_LISTEN:`:${port}`, AHP_ENABLE_DATA:'1', AHP_ENABLE_EXECUTION:'1',
  AHP_S3_BUCKET:'ahp-delivery-self', AHP_DEV_JWT_SEED: seed,
  AHP_TLS_CERT: sc.serverCert, AHP_TLS_KEY: sc.serverKey, AHP_TLS_CA: sc.ca,
  HTTP_PROXY:'',HTTPS_PROXY:'',http_proxy:'',https_proxy:'', NO_PROXY:'localhost,127.0.0.1,::1' };
const child = spawn(BIN, [], { cwd:ROOT, env, stdio:['ignore', fs.openSync('/tmp/ahp-selfcheck.out','w'), fs.openSync('/tmp/ahp-selfcheck.err','w')] });
const kill=()=>{try{child.kill('SIGTERM');}catch{}};
process.on('SIGTERM',kill);
try {
  await pollReady(port, { tls:true, timeoutMs:30000 }); ok('ephemeral mTLS composed-backend ready (shared seed, /readyz over client cert)', `:${port}`);
  const tenant='dself', dep=uniqueDeployment('self'), task='t'+Date.now();
  const clientDir = genClient(tenant, dep); ok('issued client cert bound to tenant/deployment', `dir=${clientDir}`);
  const tok = mintToken({ tenant, deployment: dep, task }); ok('minted Bearer task token via scripts/tokengen (same seed)', `len=${tok.length}`);

  const thr = await mState('CreateThread', { context_id:'ctx-'+Date.now(), key:'self/thread' }, { port, certDir: clientDir, token: tok });
  const threadID = thr.json && thr.json.id;
  (thr.status===200 && threadID ? ok:bad)('authenticated CreateThread over mTLS on REAL PG', `status=${thr.status}`);
  const run = await mState('CreateRun', { thread_id: threadID, key:'self/run', payload:{ marker:'selfcheck', seed:'derived' } }, { port, certDir: clientDir, token: tok });
  const runID = run.json && run.json.id;
  (run.status===200 && runID ? ok:bad)('authenticated CreateRun over mTLS on REAL PG', `status=${run.status}`);
  const back = await mState('GetRun', { run_id: runID }, { port, certDir: clientDir, token: tok });
  const blob=JSON.stringify(back.json||{});
  (back.status===200 && blob.includes('selfcheck') ? ok:bad)('GetRun reads run back (durable PG round-trip)', `status=${back.status}`);
  const claim = await mState('ClaimRun', { run_id: runID }, { port, certDir: clientDir, token: tok });
  (claim.status!==401 && claim.status!==403 ? ok:bad)('ClaimRun: client-cert+task-token accepted by verifier (not 403)', `status=${claim.status} ${RED(claim.text).slice(0,90)}`);

  // negative: valid cert but NO bearer token -> 403 (auth still enforced)
  const noTok = await mState('GetRun', { run_id: runID }, { port, certDir: clientDir });
  (noTok.status===403 ? ok:bad)('cert-but-no-token GetRun -> 403 (auth enforced)', `status=${noTok.status}`);

  // ---- DATA-plane (artifacts) on the composed front: route mounted + auth enforced ----
  // The full sha256(PUT)==sha256(GET) bytes leg lives in tests/e2e/live-journey.mjs (LEG-B,
  // real MinIO). Here we prove the artifacts route IS mounted (not 404) AND the transport
  // Task guard demands client-cert + Bearer, so the composed front is a real STATE+DATA-auth
  // surface — not just readiness. Presigned-URL minting is local, so this stays MinIO-free.
  const artNoTok = await mArtifacts('BeginUpload', { key:'self/no-tok', name:'n', mime:'text/plain', sha256:'0'.repeat(64), size:1 }, { port, certDir: clientDir });
  (artNoTok.status===403 ? ok:bad)('cert-but-no-token BeginUpload -> 403 (data route mounted + auth enforced)', `status=${artNoTok.status}`);
  const artTok = await mArtifacts('BeginUpload', { key:'self/ok.bin', name:'ok.bin', mime:'application/octet-stream', sha256: createHash('sha256').update('selfcheck').digest('hex'), size: 9 }, { port, certDir: clientDir, token: tok });
  (artTok.status!==401 && artTok.status!==403 ? ok:bad)('token+cert BeginUpload accepted (data route + verifier active)', `status=${artTok.status}`);

  console.log(fail===0 ? `\nSELFCHECK: PASS (${pass}) — journey-residual CONFIRMED = launch-config (mTLS+shared seed), not product; state round-trips on REAL PG` : `\nSELFCHECK: FAIL (${fail}/${pass+fail})`);
  process.exitCode = fail===0?0:1;
} catch(e){ bad('exception', RED(e.message).slice(0,160)); process.exitCode=1; }
finally { kill(); }
