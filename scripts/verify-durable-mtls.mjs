#!/usr/bin/env node
// scripts/verify-durable-mtls.mjs — handoff proof for the durable composed ALL-PLANES front
// (the EXACT LaunchAgent entrypoint private/composed-backend/scripts/composed-backend.mjs)
// under AHP_COMPOSED_BACKEND_TLS=1: authenticated mTLS + Bearer (shared DEV seed ==
// tests/helpers/ahp-client.mjs:devSeedHex == scripts/tokengen) journey on REAL PG + REAL
// MinIO + durable-after-restart + foreign-tenant isolation. De-risks iv's independent
// whole-block act (bundle 6f50d5b). This is DELIVERY HANDOFF evidence, NOT the independent
// verdict (author!=verifier) and NOT a whole-prototype claim. The a2a/model/tool leg is
// intentionally NOT on the composed front (role=all has no injected executor per ruling
// b9f9b174); model/tool/artifact bytes are proven in product-runtime (live-journey LEG-B).
// Spawns ephemerally on a free port (sandbox blocks launchctl and reaps detached); reaps.
// Mac-safe (isolated allowlist DB via buildEnv). Seed value never printed; fail-closed.
// Override durable dir with AHP_COMPOSED_BACKEND_DIR; iv: set AHP_COMPOSED_BACKEND_CERTDIR.
import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import net from 'node:net';
import fs from 'node:fs';
import https from 'node:https';
import http from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { join } from 'node:path';
import { genClient, uniqueDeployment, certPaths, mState } from '../tests/helpers/ahp-client.mjs';

const ROOT = process.cwd();
const DUR = process.env.AHP_COMPOSED_BACKEND_DIR
  || '/Users/aogabbasov/AI Agents/ahp/company-brain/private/composed-backend';
const LAUNCH = join(DUR, 'scripts', 'composed-backend.mjs');
const NODE_BIN = process.env.NODE_BIN || '/opt/homebrew/bin/node';
const CERTDIR = process.env.AHP_COMPOSED_BACKEND_CERTDIR || join(ROOT, 'infra', 'certs');
const devSeed = () => createHash('sha256').update('ahp-hive-delivery-dev-jwt-seed-v1').digest('hex');
const sha256hex = (b) => createHash('sha256').update(b).digest('hex');
const RED = (s) => String(s).replace(/[A-Za-z0-9._+/=-]{40,}/g, '<REDACTED>');
function freePort(){ return new Promise((res,rej)=>{const s=net.createServer();s.once('error',rej);s.listen(0,'127.0.0.1',()=>{const p=s.address().port;s.close(()=>res(p));});});}
function portOpen(port){ return new Promise((res)=>{const s=net.connect({host:'127.0.0.1',port,timeout:400}); s.on('connect',()=>{s.destroy();res(true)}); s.on('error',()=>res(false)); s.on('timeout',()=>{s.destroy();res(false)});});}
// inline artifacts RPC (mirrors mState on /v1/artifacts; Download returns buffer)
function mArtifactsRPC(op, body, { port, certDir, token } = {}) {
  const p = certPaths(certDir);
  const headers = { 'content-type': 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  const payload = body === undefined ? '{}' : JSON.stringify(body);
  const options = { host:'127.0.0.1', port, method:'POST', path:`/v1/artifacts/${op}`,
    ca: fs.readFileSync(p.ca), cert: fs.readFileSync(p.cert), key: fs.readFileSync(p.key),
    servername:'localhost', rejectUnauthorized:true, headers, timeout:20000 };
  return new Promise((res, rej) => {
    const req = https.request(options, (r) => {
      const bufs=[]; r.on('data',(d)=>bufs.push(Buffer.from(d))); r.on('end',()=>{ const buffer=Buffer.concat(bufs); let json=null; try{ const t=buffer.toString('utf8'); json=t?JSON.parse(t):null; }catch{} res({status:r.statusCode,json,buffer,text:buffer.toString('utf8')}); });
    });
    req.on('error', rej); req.on('timeout', ()=>req.destroy(new Error('mArtifactsRPC timeout '+op)));
    req.write(payload); req.end();
  });
}
function readyWithCert(port, clientDir, timeoutMs=30000){
  const p = certPaths(clientDir);
  const opts = { host:'127.0.0.1', port, path:'/healthz', ca: fs.readFileSync(p.ca), cert: fs.readFileSync(p.cert), key: fs.readFileSync(p.key), servername:'localhost', rejectUnauthorized:true, timeout:1500 };
  return new Promise((res,rej)=>{ const dl=Date.now()+timeoutMs;
    (function tick(){ const r = https.get(opts,(resp)=>{ resp.resume(); res(true); });
      r.on('error',()=>{ if(Date.now()>dl) return rej(new Error('not ready '+timeoutMs+'ms')); setTimeout(tick,250); });
      r.on('timeout',function(){ this.destroy(); }); r.end(); })(); });
}
function tok(tenant,dep,task){ return execFileSync('go', ['run','./scripts/tokengen','-kid','dev-1','-tenant',tenant,'-deployment',dep,'-task',task,'-kind','task'],
  { cwd: ROOT, encoding:'utf8', env:{...process.env, AHP_DEV_JWT_SEED:devSeed(), GOPROXY:'off', GOFLAGS:'-mod=mod', GOCACHE: process.env.GOCACHE||'/tmp/ahp-gocache'} }).trim(); }
let pass=0, fail=0; const ok=(n,x='')=>{pass++;console.log('  OK '+n+(x?'  '+x:''))}; const bad=(n,x='')=>{fail++;console.log('  FAIL '+n+(x?'  '+x:''))};
if (!fs.existsSync(LAUNCH)) { console.log('SKIP: durable entrypoint not present at '+DUR+' (iv: set AHP_COMPOSED_BACKEND_DIR)'); process.exit(3); }
const env = { ...process.env, AHP_COMPOSED_BACKEND_TLS:'1', AHP_COMPOSED_BACKEND_CERTDIR: CERTDIR,
  HTTP_PROXY:'',HTTPS_PROXY:'',http_proxy:'',https_proxy:'', NO_PROXY:'localhost,127.0.0.1,::1' };
let child=null; const kill=()=>{ if(child){try{child.kill('SIGTERM')}catch{}} };
const spawnBackend=(port)=>{ child = spawn(NODE_BIN,[LAUNCH],{cwd:DUR,env:{...env,AHP_COMPOSED_BACKEND_PORT:String(port)},stdio:['ignore',fs.openSync('/tmp/durable-mtls.out','w'),fs.openSync('/tmp/durable-mtls.err','w')]}); };
process.on('SIGTERM',kill);
try {
  const port = await freePort();
  const tenant='dverify', dep=uniqueDeployment('dv'), task='t'+Date.now();
  const clientDir = genClient(tenant, dep); ok('client cert issued (tenant A)', dep);
  spawnBackend(port);
  await readyWithCert(port, clientDir); ok('durable composed-backend.mjs (TLS=1) up: mTLS handshake OK',':'+port);
  const tA = tok(tenant,dep,task); ok('Bearer task token A minted (same dev seed)','len='+tA.length);
  const thr = await mState('CreateThread',{context_id:'ctx-'+Date.now(),key:'dv/thread'},{port,certDir:clientDir,token:tA});
  const tid = thr.json&&thr.json.id; (thr.status===200&&tid?ok:bad)('mTLS CreateThread 200 (REAL PG)','status='+thr.status);
  const run = await mState('CreateRun',{thread_id:tid,key:'dv/run',payload:{marker:'durable-mtls'}},{port,certDir:clientDir,token:tA});
  const rid = run.json&&run.json.id; (run.status===200&&rid?ok:bad)('mTLS CreateRun 200','status='+run.status);
  const noTok = await mState('GetRun',{run_id:rid},{port,certDir:clientDir});
  (noTok.status===403?ok:bad)('cert-but-no-token GetRun -> 403 (auth enforced)','status='+noTok.status);
  // DATA leg (real MinIO sha PUT==GET) on the composed front, if the RPC helper is available
  try {
    const bytes = Buffer.from('durable-mtls-artifact '+Date.now());
    const putSha = sha256hex(bytes);
    const beg = await mArtifactsRPC('BeginUpload',{key:'dv/ok.bin',name:'ok.bin',mime:'application/octet-stream',sha256:putSha,size:bytes.length},{port,certDir:clientDir,token:tA});
    const aid = beg.json&&beg.json.id;
    if (beg.status===200 && beg.json && beg.json.presigned_url){
      const pu = new URL(beg.json.presigned_url);
      const putCode = await new Promise((res,rej)=>{ const m=pu.protocol==='https:'?https:http; const r=m.request({host:pu.hostname,port:pu.port||(pu.protocol==='https:'?443:80),path:pu.pathname+pu.search,method:'PUT',headers:{'content-length':bytes.length},timeout:15000},x=>{x.resume();res(x.statusCode)}); r.on('error',rej); r.on('timeout',()=>r.destroy(new Error('PUT timeout'))); r.end(bytes); });
      const seal = await mArtifactsRPC('Seal',{id:aid},{port,certDir:clientDir,token:tA});
      const dl = await mArtifactsRPC('Download',{id:aid},{port,certDir:clientDir,token:tA});
      const gotSha = dl.buffer?sha256hex(dl.buffer):'';
      (putCode===200 && seal.status===200 && dl.status===200 && gotSha===putSha ? ok:bad)('DATA leg: presigned PUT->Seal->Download sha256(PUT)==sha256(GET) on REAL MinIO',`put=${putCode} seal=${seal.status} v=${seal.json&&seal.json.version} get=${dl.status} sha=${gotSha.slice(0,12)}..`);
    } else { bad('DATA leg BeginUpload did not return presigned_url','status='+beg.status); }
  } catch(e){ bad('DATA leg exception', RED(e.message).slice(0,140)); }
  // FOREIGN-TENANT isolation: a DIFFERENT tenant (client cert B + token B) must NOT read tenant A's run
  const tdepB='dvmv', tenantB='dforeign'; const dirB=genClient(tenantB,tdepB);
  const tB = tok(tenantB,tdepB,'t'+Date.now());
  const cross = await mState('GetRun',{run_id:rid},{port,certDir:dirB,token:tB});
  const body=JSON.stringify(cross.json||cross.text||{});
  ((cross.status===404||cross.status===403||/NOT_FOUND|not found/i.test(body)) ? ok:bad)('foreign-tenant GetRun(A) -> NOT_FOUND/forbidden (isolation on composed front)','status='+cross.status+' '+RED(body).slice(0,60));
  // DURABLE-AFTER-RESTART: kill backend, re-spawn same port/DB, re-read the run
  const ridBefore = rid;
  kill();
  for(let i=0;i<80 && await portOpen(port);i++) await sleep(250);
  spawnBackend(port);
  await readyWithCert(port, clientDir); ok('backend re-spawned on same port (fresh process)',':'+port);
  const after = await mState('GetRun',{run_id:ridBefore},{port,certDir:clientDir,token:tA});
  const markerAfter = JSON.stringify(after.json||{}).includes('durable-mtls');
  (after.status===200 && markerAfter ? ok:bad)('DURABLE-AFTER-RESTART: run survives process restart (REAL PG)','status='+after.status);
  console.log(fail===0?`\nDURABLE-MTLS VERIFY: PASS (${pass}) — composed all-planes front: authenticated mTLS state+data (sha PUT==GET) + foreign-tenant isolation + durable-after-restart on REAL PG/MinIO. iv HANDOFF (not the independent verdict).`:`\nDURABLE-MTLS VERIFY: FAIL (${fail}/${pass+fail})`);
  process.exitCode = fail===0?0:1;
} catch(e){ bad('exception', RED(e.message).slice(0,200)); process.exitCode=1; }
finally { kill(); }
