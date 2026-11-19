#!/usr/bin/env node
// scripts/separated-roles-smoke.mjs — repeatable gate for acceptance criterion #1
// ("Runnable separated service roles" + "OTel" + "health/readiness"), delivery-owned.
//
// Launches each service role (state|identity|data|execution) as its OWN process on a
// distinct ephemeral port from the composed binary, then asserts, per role:
//   * /readyz == 200                          (health/readiness)
//   * its own plane route responds (401/403)  (plane MOUNTED — auth-reject != 404)
//   * a foreign plane route returns 404        (roles are SEPARATED, not co-located)
// and asserts OTel emits >=1 server span per role (offline JSON exporter to stderr).
//
// Uses buildEnv so every child DB points at the isolated allowlist DB. Mac-safe
// (single prebuilt tagged bin; no whole-tree build). Fail-closed: exit 1 on any miss.
// Children are killed on exit (node process.kill of its OWN spawned children).
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import net from 'node:net';
import fs from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';
import { buildEnv } from './ahp-env.mjs';

const ROOT = process.cwd();
const BIN = join(ROOT, process.env.AHP_SEPARATED_BIN || 'bin/ahp-full-tagged');
if (!fs.existsSync(BIN)) { console.error(`[separated-roles] missing bin ${BIN}; build: go build -tags 'ahp_exec ahp_data' -o bin/ahp-full-tagged ./cmd/ahp`); process.exit(2); }

// (role) -> { own:[method,path]..., foreign:[method,path]... }
const SPEC = {
  state:     { own: [['POST','/v1/state/CreateRun']],          foreign: [['POST','/v1/artifacts/Seal'],['POST','/a2a/public']] },
  identity:  { own: [['POST','/v1/identity/roots']],           foreign: [['POST','/v1/state/CreateRun']] },
  data:      { own: [['POST','/v1/artifacts/Seal']],           foreign: [['POST','/v1/state/CreateRun']] },
  execution: { own: [['POST','/a2a/public'],['POST','/a2a/internal']], foreign: [['POST','/v1/state/CreateRun']] },
};

async function freePort(){ return new Promise((res,rej)=>{ const s=net.createServer(); s.once('error',rej); s.listen(0,'127.0.0.1',()=>{ const p=s.address().port; s.close(()=>res(p)); }); }); }
async function code(url, method='GET'){ try { const r=await fetch(url,{method, headers:{'content-type':'application/json'}, body: method==='POST'?'{}':undefined, signal: AbortSignal.timeout(5000)}); return r.status; } catch(e){ return 0; } }

const env0 = await buildEnv(ROOT);
const children=[]; const logs={}; const ports={};
try {
  for (const role of Object.keys(SPEC)){
    const port = await freePort(); ports[role]=port;
    const env = { ...env0, AHP_ROLE: role, AHP_LISTEN: `:${port}`, AHP_ENABLE_DATA:'1', AHP_ENABLE_EXECUTION:'1',
      AHP_S3_BUCKET:'ahp-delivery-roles', HTTP_PROXY:'',HTTPS_PROXY:'',http_proxy:'',https_proxy:'', NO_PROXY:'localhost,127.0.0.1,::1' };
    delete env.AHP_TLS_CERT; delete env.AHP_TLS_KEY; delete env.AHP_TLS_CA;
    const err = `/tmp/ahp-sep-${role}-${process.pid}.err`; logs[role]=err;
    const c = spawn(BIN,[],{cwd:ROOT,env,stdio:['ignore', fs.openSync(`/tmp/ahp-sep-${role}-${process.pid}.out`,'w'), fs.openSync(err,'w')]});
    children.push(c);
  }
  await sleep(3000);
  let fail=0; const rows=[];
  for (const role of Object.keys(SPEC)){
    const p=ports[role];
    const ready = await code(`http://127.0.0.1:${p}/readyz`);
    const own = []; for (const [m,path] of SPEC[role].own){ own.push([path, await code(`http://127.0.0.1:${p}${path}`, m)]); }
    const foreign = []; for (const [m,path] of SPEC[role].foreign){ foreign.push([path, await code(`http://127.0.0.1:${p}${path}`, m)]); }
    // OTel: trigger traced request, then flush wait handled after loop
    await code(`http://127.0.0.1:${p}/healthz`);
    const ownOK = own.every(([,c])=> c===401 || c===403);
    const foreignOK = foreign.every(([,c])=> c===404);
    const ok = ready===200 && ownOK && foreignOK;
    if(!ok) fail++;
    rows.push({role, ready, own, foreign, ok});
  }
  await sleep(6000); // OTLP batch flush (5s) -> stderr JSON exporter
  for (const r of rows){
    const spans = (fs.readFileSync(logs[r.role],'utf8').match(/"trace_id":"[0-9a-f]{16,}"/g)||[]).length;
    r.spans=spans; if(spans<1){ r.ok=false; fail++; }
    console.log(`role=${r.role.padEnd(9)} readyz=${r.ready} own=${JSON.stringify(r.own)} foreign=${JSON.stringify(r.foreign)} server_spans=${spans} -> ${r.ok?'PASS':'FAIL'}`);
  }
  console.log(fail===0 ? '\nSEPARATED-ROLES+OTEL: PASS (roles separated, planes mounted, OTel spans emitted per role)' : `\nSEPARATED-ROLES+OTEL: FAIL (${fail})`);
  process.exitCode = fail===0?0:1;
} finally {
  for (const c of children){ try{ c.kill('SIGTERM'); }catch{} }
}
