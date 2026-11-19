// Browser transport/rendering fixture only. Real Model/State/mTLS is tested in
// tests/space; actual provider acceptance is a separate live-stand probe.
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {resolve,extname} from 'node:path';
import {pathToFileURL} from 'node:url';
const {chromium}=await import(process.env.AHP_PLAYWRIGHT_MODULE ? pathToFileURL(process.env.AHP_PLAYWRIGHT_MODULE) : 'playwright');
const root=resolve('web/aifactory');let generation=0,status='idle',events=[],subscribers=new Set(),cursors=[],input={};
const push=(kind,payload,key)=>{const e={id:'e'+(events.length+1),thread_id:'thread',key,kind,sequence:events.length+1,payload};events.push(e);for(const s of subscribers)send(s,e);};
const send=(s,e)=>s.write(`id: ${e.sequence}\nevent: ${e.kind}\ndata: ${JSON.stringify({type:e.kind,cursor:e.sequence,event_id:e.id,thread_id:e.thread_id,key:e.key,payload:e.payload})}\n\n`);
const server=createServer(async(req,res)=>{
 const url=new URL(req.url,'http://localhost'),op=url.pathname.split('/').at(-1);
 if(url.pathname==='/v1/space/stream/thread'){
  res.writeHead(200,{'content-type':'text/event-stream','cache-control':'no-store'});res.flushHeaders();const after=Number(url.searchParams.get('after')||0);cursors.push(after);
  for(const e of events.filter(e=>e.sequence>after))send(res,e);
  if(after===0){res.end();return;}subscribers.add(res);req.on('close',()=>subscribers.delete(res));return;
 }
 if(url.pathname.startsWith('/v1/')||url.pathname.startsWith('/u-ahp/')){
  let raw='';for await(const b of req)raw+=b;const body=raw?JSON.parse(raw):{};let data={};
  if(op==='token')data={access_token:'fixture-browser',expires_in:600,subject:'owner',tenant:'tenant'};
  else if(op==='AcceptTurn'){generation++;status='running';events=[];cursors=[];input=body.input;data={turn_id:'run'+generation,thread_id:'thread',status:'accepted'};}
  else if(op==='DispatchTurn'){push('model_delta',{generation_id:'g'+generation,delta_sequence:1,content:'Первая часть ',done:false},`run${generation}:delta:g${generation}:1`);}
  else if(op==='GetTurn')data={status};
  else if(op==='ReadStream')data={events:events.filter(e=>e.sequence>(body.after||0)).slice(0,body.limit||50)};
  else if(op==='ListTurns')data={turns:generation?[{id:'run'+generation,run_id:'run'+generation,thread_id:'thread',status,input,created_at:new Date().toISOString()}]:[]};
  else if(op==='ListChats')data={threads:[]};
  else if(op==='CancelTurn'){status='canceled';push('status',{status},`run${generation}:cancel`);data={terminal_frame_durable:true};}
  else if(op==='Search')data={hits:[]};else if(op==='List')data={triggers:[]};else if(op==='Catalog')data={cards:[]};
  res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify(data));return;
 }
 try{const path=resolve(root,'.'+(url.pathname==='/'?'/index.html':url.pathname));if(!path.startsWith(root+'/'))throw Error('path');res.setHeader('content-type',({'.html':'text/html','.js':'text/javascript','.css':'text/css'})[extname(path)]||'application/octet-stream');res.end(await readFile(path));}catch{res.writeHead(404);res.end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;const browser=await chromium.launch({headless:true});
try{
 await test('browser displays partial before terminal, resumes cursor, and replaces final once',async()=>{
  const page=await browser.newPage();try{await page.goto(origin+'/#/chats');await page.waitForFunction(()=>window.AHP_LIVE?.state.S.online);await page.evaluate(()=>AHP_LIVE.api.runTurn('fixture', {context_id:'browser-stream'}));
   await page.waitForFunction(()=>document.querySelector('.ahp-message:not(.user)')?.textContent.includes('Первая часть'));
   assert.equal(status,'running');assert.match(await page.locator('.ahp-message:not(.user)').innerText(),/Частичный ответ/);
   await page.waitForTimeout(600);assert.ok(cursors.includes(1),'reconnect must use durable cursor');
   push('model_delta',{generation_id:'g1',delta_sequence:2,content:'завершена',done:false},'run1:delta:g1:2');
   push('model',{generation_id:'g1',content:'Первая часть завершена',done:false},'run1:0');push('done',{done:true},'run1:1');status='done';
   await page.waitForFunction(()=>AHP_LIVE.state.L.streamState==='terminal:done');assert.equal(await page.locator('.ahp-message:not(.user)').count(),1);assert.match(await page.locator('.ahp-message:not(.user)').innerText(),/Первая часть завершена/);assert.doesNotMatch(await page.locator('.ahp-message:not(.user)').innerText(),/Частичный/);
  }finally{await page.close();}
 });
 await test('cancelled partial is retained and never marked final',async()=>{
  const page=await browser.newPage();try{await page.goto(origin+'/#/chats');await page.waitForFunction(()=>window.AHP_LIVE?.state.S.online);await page.evaluate(()=>AHP_LIVE.api.runTurn('fixture cancel',{context_id:'browser-cancel'}));
   await page.waitForFunction(()=>AHP_LIVE.state.L.frames.some(e=>e.kind==='model_delta'));
   await page.evaluate(()=>AHP_LIVE.api.cancelTurn());await page.waitForFunction(()=>AHP_LIVE.state.L.turn?.status==='canceled');assert.match(await page.locator('.ahp-message:not(.user)').innerText(),/Частичный ответ/);
  }finally{await page.close();}
 });
}finally{await browser.close();for(const res of subscribers)res.end();await new Promise(r=>server.close(r));}
