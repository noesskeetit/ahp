// Writable, isolated browser acceptance. No fixtures or service credentials in the browser.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
const {chromium}=await import(pathToFileURL(process.env.AHP_PLAYWRIGHT_MODULE));
const origin=process.env.AHP_UI_LIVE_ORIGIN, output=process.env.AHP_UI_WORKSPACE_OUTPUT;
assert.ok(origin&&output&&process.env.AHP_UI_WORKSPACE_USERS_JSON,'explicit isolated origin, output and two test users required');
const users=JSON.parse(fs.readFileSync(process.env.AHP_UI_WORKSPACE_USERS_JSON));assert.equal(users.length,2);
fs.mkdirSync(output,{recursive:true});
const report={origin,run:'workspace-'+Date.now(),scope:'real browser + actual Identity + PostgreSQL + MinIO; no LLM claim',users:[],errors:[]};
const browser=await chromium.launch({headless:true}),contexts=[];
const sha=b=>createHash('sha256').update(b).digest('hex');
const click=(p,n)=>p.locator(`[data-action="${n}"]`).click();
const field=(p,n)=>p.locator(`[name="${n}"]`);
async function rpc(p,op,body){return p.evaluate(async({op,body})=>{const r=await AHP_LIVE.api.raw('/u-ahp/v1/workspace/'+op,{body});return {status:r.status,json:r.json};},{op,body});}
async function requestClick(p,action,operation,status=200){const waiting=p.waitForResponse(r=>new URL(r.url()).pathname==='/u-ahp/v1/workspace/'+operation);await click(p,action);const r=await waiting;const body=await r.json();assert.equal(r.status(),status,operation+': '+JSON.stringify(body));return body;}
async function open(p,name){await field(p,'ahp-workspace-name').fill(name);await requestClick(p,'ahp-workspace-open','GetWorkspace');await p.waitForFunction(name=>AHP_LIVE.state.L.workspace?.name===name,name);}
async function upload(p,name,bytes){await p.locator('#ahp-workspace-upload-file').setInputFiles({name,mimeType:'application/octet-stream',buffer:bytes});await click(p,'ahp-workspace-upload');await p.waitForFunction(name=>AHP_LIVE.state.L.workspaceReview?.candidate?.state==='pending'&&AHP_LIVE.state.L.workspaceReview.files.some(f=>f.path===name),name);return p.evaluate(()=>AHP_LIVE.state.L.workspaceReview);}
async function verifyPublish(p){await requestClick(p,'ahp-workspace-check','VerifyCandidate');await p.waitForFunction(()=>AHP_LIVE.state.L.workspaceReview?.checks?.some(x=>x.name==='sealed-byte-integrity'&&x.status==='passed'));const revision=await requestClick(p,'ahp-workspace-publish','Publish');await p.waitForFunction(id=>AHP_LIVE.state.L.workspace?.head_rev===id,revision.id);return revision;}
async function download(p,name,expected,label){const wait=p.waitForEvent('download');await p.locator(`[data-action="ahp-workspace-file"][data-id="${name}"]`).click();const download=await wait;const dest=path.join(output,label);await download.saveAs(dest);const bytes=fs.readFileSync(dest);assert.equal(bytes.length,expected.length);assert.equal(sha(bytes),sha(expected));return {path:dest,bytes:bytes.length,sha256:sha(bytes)};}
try{
 for(const [i,user] of users.entries()){
  const ctx=await browser.newContext({acceptDownloads:true,viewport:{width:1440,height:1100}});contexts.push(ctx);const p=await ctx.newPage();p.setDefaultTimeout(30000);p.on('pageerror',e=>report.errors.push(e.message));p.on('dialog',d=>d.accept());
  await p.goto(origin+'/#/workspace');await field(p,'ahp-user').fill(user.email||user.username);await field(p,'ahp-pass').fill(user.password);await click(p,'ahp-login');await p.waitForFunction(subject=>AHP_LIVE.state.S.subject===subject,user.subject);
  const workspace=report.run+'-'+i;await field(p,'ahp-workspace-name').fill(workspace);await requestClick(p,'ahp-workspace-create','Create');await p.locator('#ahp-workspace-upload-file').waitFor();
  const bytes=Buffer.from(('Owner '+user.subject+' '+workspace+'\n').repeat(70000));
  const draft=await upload(p,'checked.txt',bytes);assert.ok(draft.files.length>0);assert.equal(draft.checks.length,0);assert.equal(await p.locator('[data-action="ahp-workspace-publish"]').isDisabled(),true);
  const denied=await rpc(p,'Publish',{candidate_id:draft.candidate.id,expected_head:''});assert.equal(denied.status,409);
  // Re-open through the server, proving the draft and its ownership survive reload.
  await p.reload();await p.waitForFunction(()=>AHP_LIVE.state.S.online);await open(p,workspace);
  await p.locator(`[data-action="ahp-workspace-candidate"][data-id="${draft.candidate.id}"]`).click();await p.waitForFunction(id=>AHP_LIVE.state.L.workspaceReview?.candidate?.id===id,draft.candidate.id);
  const revision=await verifyPublish(p);await p.waitForFunction(()=>AHP_LIVE.state.L.workspaceFiles?.length>0);
  const saved=await download(p,'checked.txt',bytes,'owner-'+i+'.txt');
  const stream=await p.evaluate(async({workspace,revision})=>{
   const start=performance.now();const r=await fetch('/u-ahp/v1/workspace/ReadFile',{method:'POST',headers:{'content-type':'application/json',authorization:'Bearer '+AHP_LIVE.state.S.bearer},body:JSON.stringify({workspace,revision,path:'checked.txt'})});
   const reader=r.body.getReader();let chunks=0,total=0,first=null;for(;;){const part=await reader.read();if(part.done)break;if(first===null)first=performance.now()-start;chunks++;total+=part.value.length;}
   return {status:r.status,chunks,bytes:total,firstByteMs:first,totalMs:performance.now()-start};
  },{workspace,revision:revision.id});assert.equal(stream.status,200);assert.ok(stream.chunks>1);assert.equal(stream.bytes,bytes.length);
  report.users.push({subject:user.subject,workspace,candidate:draft.candidate.id,revision:revision.id,artifact:draft.files[0].artifact_id,noCheckPublishStatus:denied.status,saved,stream});
  // Keep bytes/page only in process memory, never in the JSON evidence.
  report.users[i]._page=p;report.users[i]._bytes=bytes;
 }
 for(let i=0;i<2;i++){
  const self=report.users[i],other=report.users[1-i],p=self._page;
  const get=await rpc(p,'GetWorkspace',{workspace:other.workspace});assert.ok([403,404].includes(get.status));
  const candidate=await rpc(p,'CandidateDetails',{candidate_id:other.candidate});assert.ok([403,404].includes(candidate.status));
  const file=await rpc(p,'ReadFile',{workspace:other.workspace,revision:other.revision,path:'checked.txt'});assert.ok([403,404].includes(file.status));
  self.crossDenial={workspace:get.status,candidate:candidate.status,file:file.status};
 }
 const [a,b]=report.users;
 await field(a._page,'ahp-workspace-member').fill(b.subject);await requestClick(a._page,'ahp-workspace-add-member','AddMember');
 await open(b._page,a.workspace);await b._page.waitForFunction(()=>AHP_LIVE.state.L.workspaceFiles?.length>0);report.memberDownload=await download(b._page,'checked.txt',a._bytes,'member.txt');
 await a._page.locator(`[data-action="ahp-workspace-remove-member"][data-id="${b.subject}"]`).click();await a._page.waitForFunction(sub=>!AHP_LIVE.state.L.workspaceMembers.some(m=>m.subject===sub),b.subject);
 report.revokedRead=await rpc(b._page,'ReadFile',{workspace:a.workspace,revision:a.revision,path:'checked.txt'});assert.ok([403,404].includes(report.revokedRead.status));
 const replacement=Buffer.from('second immutable revision\n');await upload(a._page,'checked.txt',replacement);await verifyPublish(a._page);await a._page.locator(`[data-action="ahp-workspace-revision"][data-id="${a.revision}"]`).click();await a._page.waitForFunction(id=>AHP_LIVE.state.L.workspaceRevision===id,a.revision);
 report.oldRevisionDownload=await download(a._page,'checked.txt',a._bytes,'old-revision.txt');
 const code=await upload(a._page,'main.go',Buffer.from('package main\n'));await requestClick(a._page,'ahp-workspace-check','VerifyCandidate',409);assert.equal(await a._page.locator('[data-action="ahp-workspace-publish"]').isDisabled(),true);report.sourceCodeCannotSelfApprove={candidate:code.candidate.id,verifyStatus:409};
 await a._page.screenshot({path:path.join(output,'workspace.png'),fullPage:true});
 // Hold an actual response from A until B is selected. An old draft must never
 // become B's publication/sharing target, even when the owner can access both.
 let releaseLate,startedLate;
 const lateStarted=new Promise(resolve=>startedLate=resolve),lateGate=new Promise(resolve=>releaseLate=resolve);
 const lateRoute=async route=>{
  if(route.request().postDataJSON().candidate_id!==code.candidate.id)return route.continue();
  const response=await route.fetch();startedLate();await lateGate;await route.fulfill({response});
 };
 await a._page.route('**/u-ahp/v1/workspace/CandidateDetails',lateRoute);
 try {
  await a._page.locator(`[data-action="ahp-workspace-candidate"][data-id="${code.candidate.id}"]`).click();await lateStarted;
  const nextProject=report.run+'-switch';await field(a._page,'ahp-workspace-name').fill(nextProject);await requestClick(a._page,'ahp-workspace-create','Create');
  await a._page.waitForFunction(name=>AHP_LIVE.state.L.workspace?.name===name,nextProject);
  releaseLate();await a._page.waitForTimeout(500);
  const selected=await a._page.evaluate(()=>({project:AHP_LIVE.state.L.workspace?.name,candidate:AHP_LIVE.state.L.workspaceReview?.candidate?.id||null}));
  assert.equal(selected.project,nextProject);assert.equal(selected.candidate,null,'late candidate from another project must be discarded');
  report.lateCandidateDiscarded=selected;
 } finally {releaseLate();await a._page.unroute('**/u-ahp/v1/workspace/CandidateDetails',lateRoute);}
 assert.deepEqual(report.errors,[]);report.status='passed';
}catch(e){report.status='failed';report.error=e.message;report.debug=[];for(const u of report.users)if(u._page)report.debug.push(await u._page.evaluate(()=>({plates:AHP_LIVE.state.L.plates,workspace:AHP_LIVE.state.L.workspace,revision:AHP_LIVE.state.L.workspaceRevision,files:AHP_LIVE.state.L.workspaceFiles})));throw e;}
finally{
 for(const u of report.users){delete u._page;delete u._bytes;}
 fs.writeFileSync(path.join(output,'browser-report.json'),JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify(report,null,2));for(const c of contexts)await c.close();await browser.close();
}
