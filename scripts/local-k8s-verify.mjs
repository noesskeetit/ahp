#!/usr/bin/env node
// Product verification through the public origin, using only two synthetic users.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import{fileURLToPath}from'node:url';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const runtime=process.env.AHP_K8S_RUNTIME||path.join(root,'private/deployment-complete');
const origin=process.env.AHP_K8S_ORIGIN||'http://127.0.0.1:30222';
const output=process.env.AHP_K8S_VERIFY_OUTPUT||path.join(runtime,'product-report.json');
const cr=JSON.parse(fs.readFileSync(path.join(runtime,'credentials.json')));
const browser=JSON.parse(fs.readFileSync(path.join(runtime,'browser/browser-report.json')));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const report={origin,at:new Date().toISOString(),kind:'real HTTP/Identity/PG/MinIO and actual model',checks:[],microvm:'MOCK explicitly'};
async function login(user){
 let cookie='',token='';
 async function call(route,body){const r=await fetch(origin+route,{method:'POST',headers:{'content-type':'application/json','x-ahp-session':'1',...(cookie?{cookie}:{}),...(token?{authorization:'Bearer '+token}:{})},body:JSON.stringify(body),signal:AbortSignal.timeout(150000)});const cookies=r.headers.getSetCookie();if(cookies.length)cookie=cookies.map(x=>x.split(';')[0]).join('; ');const bytes=Buffer.from(await r.arrayBuffer());let json;try{json=JSON.parse(bytes)}catch{}return{status:r.status,bytes,json};}
 let r=await call('/u-ahp/v1/auth/login',{username:user.username,password:user.password});if(r.status!==200)throw Error('login returned '+r.status);
 r=await call('/u-ahp/v1/auth/token',{});if(r.status!==200)throw Error('token exchange failed');token=r.json.access_token;
 return call;
}
try{
 const api=await Promise.all(cr.users.map(login));
 for(let i=0;i<2;i++){
  const own=browser.users[i],foreign=browser.users[1-i];
  const body={workspace:own.workspace,revision:own.revision,path:'checked.txt'};
  const file=await api[i]('/u-ahp/v1/workspace/ReadFile',body);
  if(file.status!==200||sha(file.bytes)!==own.saved.sha256||file.bytes.length!==own.saved.bytes)throw Error('owner file byte/version mismatch');
  const other=await api[i]('/u-ahp/v1/workspace/ReadFile',{workspace:foreign.workspace,revision:foreign.revision,path:'checked.txt'});
  if(![403,404].includes(other.status))throw Error('foreign owner read not denied');
  report.checks.push({subject:own.subject,ownerStatus:file.status,bytes:file.bytes.length,sha256:sha(file.bytes),foreignStatus:other.status});
 }
 if(process.env.AHP_K8S_PREVIOUS_REPORT){
  const prior=JSON.parse(fs.readFileSync(process.env.AHP_K8S_PREVIOUS_REPORT));
  const priorTurn=await api[0]('/v1/space/GetTurn',{turn_id:prior.model.turn});
  const priorStream=await api[0]('/v1/space/ReadStream',{thread_id:prior.model.thread,after:0,limit:200});
  const texts=priorStream.json?.events?.filter(x=>x.kind==='model').map(x=>JSON.stringify(x.payload)).join('\n');
  if(priorTurn.status!==200||(priorTurn.json.status??priorTurn.json.turn?.status)!=='done'||!texts||sha(texts)!==prior.model.outputSHA256)throw Error('previous completed turn/stream was not preserved');
  const foreign=await api[1]('/v1/space/GetTurn',{turn_id:prior.model.turn});
  if(![403,404].includes(foreign.status))throw Error('previous turn lost owner isolation');
  report.previousTurn={turn:prior.model.turn,status:'done',streamSHA256:sha(texts),foreignStatus:foreign.status};
 }
 const key='k8s-product-'+Date.now(),marker='K8S-ACTUAL-'+Date.now();
 let r=await api[0]('/v1/space/AcceptTurn',{key,context_id:key,max_steps:2,input:{role:'user',content:'Ответь ровно '+marker+'. Не используй инструменты.'}});if(r.status!==200)throw Error('AcceptTurn status '+r.status);
 const turn=r.json,started=Date.now();r=await api[0]('/v1/space/DispatchTurn',{turn_id:turn.turn_id});if(r.status!==200)throw Error('DispatchTurn status '+r.status);
 let status;for(let i=0;i<120;i++){r=await api[0]('/v1/space/GetTurn',{turn_id:turn.turn_id});status=r.json?.status??r.json?.turn?.status;if(['done','failed','canceled','timeout'].includes(status))break;await new Promise(r=>setTimeout(r,500));}
 if(status!=='done')throw Error('actual model turn ended '+status);
 r=await api[0]('/v1/space/ReadStream',{thread_id:turn.thread_id,after:0,limit:200});
 const texts=r.json.events.filter(x=>x.kind==='model').map(x=>JSON.stringify(x.payload)).join('\n');if(!texts.includes(marker))throw Error('actual model output absent');
 report.model={status,turn:turn.turn_id,thread:turn.thread_id,elapsedMs:Date.now()-started,outputSHA256:sha(texts)};report.status='passed';
}catch(e){report.status='failed';report.error=e.message;process.exitCode=1;}
fs.writeFileSync(output,JSON.stringify(report,null,2)+'\n',{mode:0o600});console.log(JSON.stringify(report));
