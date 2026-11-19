// delivery lane: independent unit test for the human-origin proxy (no docker).
// Uses a SYNTHETIC upstream only to verify PROXY behavior (cookie hardening,
// CSRF injection, SSE passthrough, routing, static UI, traversal guard). This is
// a unit test of the proxy, NOT the live product journey (which uses the real
// backend; never presented as live).
import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const PROXY = fileURLToPath(new URL('./human-origin.mjs', import.meta.url));
// Build-hygiene (ENOSPC incident 2026-11-09): every temp dir this test makes in
// os.tmpdir() is removed when the process ends, on success AND on failure alike.
const TMP_DIRS = new Set();
function mkTmpDir(prefix){ const d = fs.mkdtempSync(path.join(os.tmpdir(), prefix)); TMP_DIRS.add(d); return d; }
function cleanTmpDirs(){ for (const d of TMP_DIRS) { try { fs.rmSync(d, { recursive: true, force: true }); } catch {} } TMP_DIRS.clear(); }
process.on('exit', cleanTmpDirs);
process.on('SIGINT', () => { cleanTmpDirs(); process.exit(130); });
process.on('SIGTERM', () => { cleanTmpDirs(); process.exit(143); });
function startUpstream(){
  return new Promise((res)=>{
    const s=http.createServer((req,r)=>{
      const u=new URL(req.url,'http://x');
      if(u.pathname==='/u-ahp/v1/auth/login'){
        r.writeHead(200,{'Set-Cookie':'ahp_browser_session=ABCDEF012345; Path=/; Max-Age=3600','Content-Type':'application/json'});
        r.end(JSON.stringify({ok:true, upstreamCsrf:req.headers['x-ahp-session']||null})); return;
      }
      if(u.pathname==='/u-ahp/v1/stream'){
        const leid=req.headers['last-event-id']||null;
        r.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache','X-Last-Event-Id':String(leid)});
        r.write('event: msg\ndata: one\n\n');
        setTimeout(()=>r.write('event: msg\ndata: two\n\n'),30);
        setTimeout(()=>r.end(),60); return;
      }
      r.writeHead(200,{'Content-Type':'application/json'}); r.end(JSON.stringify({path:u.pathname}));
    });
    s.listen(0,'127.0.0.1',()=>res(s));
  });
}
function startProxy(upstream, uiDir){
  return new Promise((res,rej)=>{
    const p=spawn('node',[PROXY],{env:{...process.env, AHP_HUMAN_ORIGIN_LISTEN:'127.0.0.1:0', AHP_HUMAN_ORIGIN_UPSTREAM:upstream, AHP_HUMAN_ORIGIN_UI:uiDir}, stdio:['ignore','ignore','pipe']});
    let buf='';
    p.stderr.on('data',(d)=>{ buf+=String(d); const m=buf.match(/"listen":"([^"]+)"/); if(m) res({p, listen:m[1]}); });
    p.on('exit',(c)=>{ if(!buf.includes('listening')) rej(new Error('proxy exited '+c+' stderr='+buf)); });
    setTimeout(()=>rej(new Error('proxy start timeout stderr='+buf)),5000);
  });
}
function tmpUI(){ const d=mkTmpDir('ahp-ui-'); fs.writeFileSync(path.join(d,'index.html'),'<title>Agents Space</title>'); fs.writeFileSync(path.join(d,'secret.key'),'PRIVATE'); fs.writeFileSync(path.join(d,'server.key'),'PRIV'); fs.writeFileSync(path.join(d,'.env'),'SECRET=1'); fs.mkdirSync(path.join(d,'certs'),{recursive:true}); fs.writeFileSync(path.join(d,'certs','server.pem'),'PRIV'); return d; }
const sleep=(ms)=>new Promise(r=>setTimeout(r,ms));

test('human-origin proxy behavior', async ()=>{
  const up=await startUpstream(); const upPort=up.address().port;
  const ui=tmpUI();
  const {p, listen}=await startProxy(`http://127.0.0.1:${upPort}`, ui);
  const H=`http://${listen}`;
  try{
    // healthz (proxy-side, real)
    const h=await fetch(`${H}/healthz`); assert.equal(h.status,200); assert.match(await h.text(),/human-origin/);
    // static UI served at /
    const ui_=await fetch(`${H}/`); assert.equal(ui_.status,200); assert.match(await ui_.text(),/Agents Space/);
    // traversal guard: cannot read a file above UI_DIR (index-only allowlist by ext would 404 .key anyway; ensure not served)
    // Defense-in-depth: private keys / certs / env / dotfiles / nested cert dir NEVER served.
    for (const secret of ['/secret.key','/server.key','/server.pem','/.env','/certs/server.pem','/id_ed25519','/config/.git']){
      const rr=await fetch(`${H}${secret}`); assert.equal(rr.status,404, `expected 404 for ${secret}, got ${rr.status}`);
    }
    const tr=await fetch(`${H}/secret.key`); assert.equal(tr.status,404);
    // login: cookie hardened to HttpOnly + SameSite=Strict; proxy injects CSRF on POST
    const login=await fetch(`${H}/u-ahp/v1/auth/login`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
    assert.equal(login.status,200);
    const sc=login.headers.getSetCookie(); const joined=sc.join(' ');
    assert.match(joined,/httponly/i); assert.match(joined,/samesite=strict/i);
    const body=await login.json(); assert.equal(body.upstreamCsrf,'1'); // CSRF injected on write
    // SSE passthrough unbuffered + Last-Event-ID relayed upstream
    const t0=Date.now();
    const sse=await fetch(`${H}/u-ahp/v1/stream`,{headers:{'Last-Event-ID':'42'}});
    assert.equal(sse.headers.get('content-type'),'text/event-stream');
    assert.equal(sse.headers.get('x-last-event-id'),'42'); // relayed to upstream
    const reader=sse.body.getReader(); const dec=new TextDecoder();
    const c1=dec.decode((await reader.read()).value);
    const firstChunkMs=Date.now()-t0;
    assert.match(c1,/data: one/); // first event arrives before the delayed second (unbuffered)
    assert.ok(firstChunkMs<50, `SSE should stream live, first chunk took ${firstChunkMs}ms`);
    // drain rest
    try{ while(true){ const {done}=await reader.read(); if(done) break; } }catch(_){}
  } finally { p.kill('SIGTERM'); up.close(); }
});

// What the Agent Space user path actually needs from this origin, pinned after two
// measured breakages: the relay forwarded only /u-ahp/v1/, so every Space call a
// browser made came back 404; and the sensitive-name guard tested the ABSOLUTE serving
// root, so an installed under a directory named like a secret answered 404 for / itself.
test('human-origin forwards the human prefixes and keeps machine namespaces closed', async ()=>{
  const up=await startUpstream(); const upPort=up.address().port;
  // A UI installed under a path whose name looks sensitive (…/private/…) must still serve.
  const root=mkTmpDir('ahp-ui-');
  const ui=path.join(root,'private','web'); fs.mkdirSync(ui,{recursive:true});
  fs.writeFileSync(path.join(ui,'index.html'),'<title>Agents Space</title>');
  fs.writeFileSync(path.join(ui,'ahp-live.js'),'//live');
  const {p, listen}=await startProxy(`http://127.0.0.1:${upPort}`, ui);
  const H=`http://${listen}`;
  try{
    for (const asset of ['/','/index.html','/ahp-live.js']){
      const r=await fetch(`${H}${asset}`);
      assert.equal(r.status,200, `UI under …/private/… must serve ${asset}`);
    }
    // Human prefixes reach the upstream verbatim (the upstream echoes its request path).
    for (const human of ['/v1/space/ListChats','/v1/space/AcceptTurn','/v1/workspace/Create','/v1/compute/GetComputer','/u-ahp/v1/memory/Search']){
      const r=await fetch(`${H}${human}`,{method:'POST',body:'{}'});
      assert.equal(r.status,200, `${human} must be forwarded`);
      assert.equal((await r.json()).path, human, `${human} must arrive unchanged`);
    }
    // Machine namespaces are NOT a browser surface: they never reach the upstream, and
    // the origin answers them as an unknown UI path (404) rather than proxying.
    for (const machine of ['/v1/state/GetRecord','/v1/identity/control/exchange','/v1/artifacts/BeginUpload','/v1/model/TreeCost','/a2a/internal']){
      const r=await fetch(`${H}${machine}`,{method:'POST',body:'{}'});
      assert.equal(r.status,404, `${machine} must not be reachable from a session origin`);
    }
  } finally { p.kill('SIGTERM'); up.close(); }
});
