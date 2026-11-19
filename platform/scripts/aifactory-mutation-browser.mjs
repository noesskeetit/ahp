// Real browser writes against an explicitly selected isolated AHP human origin.
// Credentials stay in an ignored input file; report contains IDs/statuses only.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
const { chromium } = await import(pathToFileURL(process.env.AHP_PLAYWRIGHT_MODULE || '/tmp/h10chain/pw/node_modules/playwright/index.mjs'));
const origin=process.env.AHP_UI_LIVE_ORIGIN, loginPath=process.env.AHP_UI_LIVE_LOGIN_JSON;
if(!origin || !loginPath) throw Error('Set AHP_UI_LIVE_ORIGIN and AHP_UI_LIVE_LOGIN_JSON for an isolated writable stand');
const user=JSON.parse(fs.readFileSync(loginPath,'utf8'));
const seed=process.env.AHP_UI_LIVE_SEED_JSON ? JSON.parse(fs.readFileSync(process.env.AHP_UI_LIVE_SEED_JSON,'utf8')) : {};
const selected=new Set((process.env.AHP_UI_LIVE_STEPS || 'memory,triggers,workspace,compute,chat,cancel,registry,approvals').split(','));
const run='browser-'+Date.now(), report={origin,run,steps:{},errors:[],http:{}};
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({acceptDownloads:true,viewport:{width:1440,height:1000}});
const page=await context.newPage(); page.setDefaultTimeout(20000);
page.on('pageerror',e=>report.errors.push(e.message));
page.on('response',r=>{const path=new URL(r.url()).pathname;if(path.startsWith('/u-ahp/') || path.startsWith('/v1/')) {const key=r.status()+' '+path;report.http[key]=(report.http[key]||0)+1;}});
const field=(name)=>page.locator(`[name="${name}"]`);
const click=(name)=>page.locator(`[data-action="${name}"]`).click();
const go=async name=>{await page.goto(origin+'/#/'+name);await page.waitForTimeout(100);};
const state=()=>page.evaluate(()=>({turn:AHP_LIVE.state.L.turn,invocation:AHP_LIVE.state.L.invocation ? {task_id:AHP_LIVE.state.L.invocation.task_id,installation_id:AHP_LIVE.state.L.invocation.installation_id,status:AHP_LIVE.state.L.invocation.status} : undefined,plates:AHP_LIVE.state.L.plates.map(p=>({code:p.code,status:p.status,message:p.message}))}));
async function step(name,fn) {
  if(!selected.has(name)) return;
  console.log(JSON.stringify({step:name,status:'running'}));
  try {report.steps[name]={status:'passed',...await fn()};}
  catch(e) {report.steps[name]={...report.steps[name],status:'failed',error:e.message,state:await state().catch(()=>({}))};}
  console.log(JSON.stringify({step:name,...report.steps[name]}));
}
async function requestClick(action,operation) {
  const response=page.waitForResponse(r=>new URL(r.url()).pathname.endsWith('/'+operation),{timeout:operation==='RunEval' ? 180000 : 20000});
  await click(action); const r=await response;
  const json=await r.json();
  assert.equal(r.status(),200,operation+' '+(json.code || '')+' '+(json.message || json.reason || 'HTTP status')); return json;
}
async function downloadedArtifact(expectedText) {
  await page.waitForFunction(()=>AHP_LIVE.state.L.artifact?.state==='resolved',null,{timeout:30000});
  const ref=await page.evaluate(()=>{const a=AHP_LIVE.state.L.artifact;return {id:a.artifact_id,version:a.version,digest:a.digest};});
  const event=page.waitForEvent('download'); await click('ahp-download');
  const download=await event, bytes=fs.readFileSync(await download.path());
  assert.equal(createHash('sha256').update(bytes).digest('hex'),ref.digest);
  const markerMatched=expectedText ? bytes.toString('utf8').includes(expectedText) : undefined;
  await download.delete(); return {...ref,bytes:bytes.length,hashMatch:true,...(expectedText ? {markerMatched} : {})};
}
async function observedCost() {
  await page.waitForFunction(()=>AHP_LIVE.state.L.cost?.status,null,{timeout:30000});
  const cost=await page.evaluate(()=>({status:AHP_LIVE.state.L.cost.status,trees:(AHP_LIVE.state.L.cost.json?.trees || []).map(t=>({tree_root:t.tree_root,unit:t.unit,tokens_total:t.tokens_total,spent:t.spent,generations:t.generations}))}));
  assert.equal(cost.status,200,'completed chat cost is available');
  assert.ok(cost.trees.length>0 && cost.trees.some(t=>t.tokens_total>0 && t.spent>0),'completed model work has recorded usage and spend');
  return cost;
}
try {
  await page.goto(origin+'/#/memory');
  await field('ahp-user').fill(user.email || user.username);
  await field('ahp-pass').fill(user.password); await click('ahp-login');
  await page.waitForFunction(()=>AHP_LIVE.state.S.online && AHP_LIVE.state.L.memoryOutcome?.rows && AHP_LIVE.state.L.chats);
  await step('memory',async()=>{
    await go('memory'); await field('ahp-mem-text').fill('Проверенная браузером запись '+run);
    await requestClick('ahp-memory-propose','Propose');
    await page.waitForFunction(()=>AHP_LIVE.state.L.memoryOutcome.rows.some(m=>m.state==='proposed'));
    const id=await page.evaluate(()=>AHP_LIVE.state.L.memoryOutcome.rows.find(m=>m.state==='proposed').id);
    await page.locator(`[data-action="ahp-memory-approve"][data-id="${id}"]`).click();
    await page.waitForFunction(id=>AHP_LIVE.state.L.memoryOutcome.rows.some(m=>m.id===id && m.state==='active'),id);
    await page.reload(); await page.waitForFunction(id=>AHP_LIVE.state.L.memoryOutcome?.rows?.some(m=>m.id===id && m.state==='active'),id);
    return {id,state:'active',reload:true};
  });
  await step('triggers',async()=>{
    await go('crons'); await field('ahp-trg-target').fill(seed.trigger_target || 'agent:root-custom');
    await field('ahp-trg-interval').fill('3600'); await field('ahp-trg-max').fill('1');
    const created=await requestClick('ahp-trigger-create','CreateSchedule');
    assert.ok(created.trigger_id);await page.waitForFunction(id=>AHP_LIVE.state.L.triggers?.some(t=>t.id===id),created.trigger_id);
    await page.locator(`[data-action="ahp-trigger-revoke"][data-id="${created.trigger_id}"]`).click();
    await page.waitForFunction(id=>AHP_LIVE.state.L.triggers?.some(t=>t.id===id && t.status==='REVOKED'),created.trigger_id);
    return {id:created.trigger_id,triggerState:'REVOKED'};
  });
  await step('workspace',async()=>{
    await go('workspace'); await field('ahp-workspace-name').fill(run);
    const created=await requestClick('ahp-workspace-create','Create');
    assert.ok(created.id);await page.waitForFunction(id=>AHP_LIVE.state.L.workspace?.id===id,created.id);
    await page.reload();await page.waitForFunction(()=>AHP_LIVE.state.S.online);
    await field('ahp-workspace-name').fill(run);await requestClick('ahp-workspace-open','GetWorkspace');
    const result={id:created.id,name:run,reopened:true};
    if(seed.workspace) {
      await field('ahp-workspace-name').fill(seed.workspace);await requestClick('ahp-workspace-open','GetWorkspace');
      await page.waitForFunction(()=>AHP_LIVE.state.L.workspaceFiles?.length>0);
      const file=await page.evaluate(()=>AHP_LIVE.state.L.workspaceFiles[0]);
      const event=page.waitForEvent('download');await page.locator('[data-action="ahp-workspace-file"]').first().click();
      const download=await event,bytes=fs.readFileSync(await download.path());
      assert.equal(createHash('sha256').update(bytes).digest('hex'),file.digest.replace(/^sha256:/,''));
      await download.delete();result.file={path:file.path,bytes:bytes.length,hashMatch:true};
    } else result.file={status:'not_run',reason:'No published workspace in seed'};
    return result;
  });
  await step('compute',async()=>{
    await go('sandbox'); const computer=await requestClick('ahp-compute-create','CreateComputer'); assert.ok(computer.id);
    const inc=await requestClick('ahp-compute-wake','Wake');assert.ok(inc.id);
    assert.equal(inc.backend,'mock','This isolated acceptance explicitly expects disclosed MicroVM mock');
    await page.waitForFunction(()=>AHP_LIVE.state.L.incarnation?.backend==='mock');
    await page.getByText('MOCK',{exact:true}).waitFor();
    await requestClick('ahp-compute-sleep','Sleep');await requestClick('ahp-compute-stop','StopIncarnation');
    return {computer:computer.id,incarnation:inc.id,backend:inc.backend,lifecycle:'create,wake,sleep,stop'};
  });
  await step('chat',async()=>{
    await go('chats');await click('ahp-chat-new'); await field('ahp-context').fill(run);
    await field('ahp-run-text').fill(seed.chat_prompt || 'Запомни код БРАУЗЕР-17 для следующего сообщения в этой беседе. Сохрани короткий текстовый файл result.txt с этим кодом, используя инструмент сохранения артефакта, затем сообщи результат.');
    await click('ahp-run');await page.waitForFunction(()=>AHP_LIVE.state.L.streamState?.startsWith('terminal:'),null,{timeout:180000});
    const first=await page.evaluate(()=>AHP_LIVE.state.L.turn);assert.equal(first.status,'done');
    const artifact=await downloadedArtifact();
    const firstCost=await observedCost();
    await field('ahp-run-text').fill(seed.followup_prompt || 'Какой код я просил запомнить в предыдущем сообщении? Ответь этим кодом, сохрани ответ в continuation.txt через инструмент артефакта.');
    await click('ahp-run');await page.waitForFunction(id=>AHP_LIVE.state.L.turn?.turn_id!==id && AHP_LIVE.state.L.streamState?.startsWith('terminal:'),first.turn_id,{timeout:180000});
    const second=await page.evaluate(()=>AHP_LIVE.state.L.turn);assert.equal(second.status,'done');assert.equal(second.thread_id,first.thread_id);assert.notEqual(second.turn_id,first.turn_id);
    const continuation=await downloadedArtifact(seed.followup_expected || 'БРАУЗЕР-17');
    const secondCost=await observedCost();
    assert.notEqual(continuation.id,artifact.id,'followup produced a new artifact');
    await page.reload();await page.waitForFunction(()=>AHP_LIVE.state.L.turnHistory?.length>=2);
    const history=await page.evaluate(()=>AHP_LIVE.state.L.turnHistory.map(t=>({id:t.id,thread_id:t.thread_id,status:t.status})));
    assert.ok(history.some(t=>t.id===first.turn_id));assert.ok(history.some(t=>t.id===second.turn_id));
    await page.waitForFunction(count=>document.querySelectorAll('.ahp-message.user').length===count,history.length);
    const reloadedCost=await observedCost();
    assert.deepEqual(reloadedCost.trees,secondCost.trees);
    const proof={first:first.turn_id,second:second.turn_id,thread:first.thread_id,history,artifact,continuation,firstCost,secondCost,reload:true};
    report.steps.chat=proof;
    assert.equal(continuation.markerMatched,true,'continuation artifact preserves previous-message fact');
    return proof;
  });
  await step('cancel',async()=>{
    await go('chats');await click('ahp-chat-new');await field('ahp-context').fill(run+'-cancel');
    await field('ahp-run-text').fill('Подготовь подробный отчёт из пяти разделов и сохрани его как артефакт.');
    await click('ahp-run'); await page.locator('[data-action="ahp-cancel"]').waitFor();
    const response=await requestClick('ahp-cancel','CancelTurn');assert.equal(response.terminal_frame_durable,true);
    await page.waitForFunction(()=>AHP_LIVE.state.L.turn?.status==='canceled',null,{timeout:30000});
    const id=await page.evaluate(()=>AHP_LIVE.state.L.turn.turn_id);
    await page.reload();await page.waitForFunction(id=>AHP_LIVE.state.L.turn?.turn_id===id && AHP_LIVE.state.L.turn?.status==='canceled',id);
    return {id,turnState:'canceled',terminal_frame_durable:true,reload:true};
  });
  await step('registry',async()=>{
    if(!seed.installation_id || !seed.suite_id || !seed.agent_digest) return {status:'not_run',reason:'No operator suite/digest/installation seed'};
    await go('stacks');await field('ahp-eval-suite').fill(seed.suite_id);await field('ahp-eval-digest').fill(seed.agent_digest);
    const evaluation=await requestClick('ahp-eval-run','RunEval');assert.equal(evaluation.verdict.outcome,'pass');
    await page.locator(`[data-action="ahp-invoke-open"][data-id="${seed.installation_id}"]`).click();
    await field('ahp-invoke-text').fill('Ответь коротко: UI '+run);await click('ahp-invoke');
    await page.waitForFunction(()=>AHP_LIVE.state.L.invocation?.observed && ['completed','failed','canceled','rejected'].includes(AHP_LIVE.state.L.invocation.status),null,{timeout:150000});
    const inv=await page.evaluate(()=>({id:AHP_LIVE.state.L.invocation.task_id,status:AHP_LIVE.state.L.invocation.status,installation_id:AHP_LIVE.state.L.invocation.installation_id,metadata:AHP_LIVE.state.L.invocation.task?.metadata}));
    assert.equal(inv.status,'completed');assert.equal(inv.installation_id,seed.installation_id);
    assert.equal(inv.metadata?.version_digest,seed.agent_digest,'runtime reports the selected immutable version');
    const output=await page.evaluate(()=>(AHP_LIVE.state.L.invocation.task?.artifacts || []).flatMap(a=>(a.parts || []).filter(p=>p.kind==='text').map(p=>p.text)).join('\n'));
    assert.ok(output.length>0,'selected runtime produced durable output');
    assert.ok((await page.locator('#main').innerText()).includes(output),'actual runtime output is visible');
    const proof={task:inv,outputBytes:Buffer.byteLength(output),outputSHA256:createHash('sha256').update(output).digest('hex'),eval:{suite:seed.suite_id,digest:seed.agent_digest,outcome:evaluation.verdict.outcome},reload:false};
    report.steps.registry=proof;
    await page.reload();await page.waitForFunction(id=>AHP_LIVE.state.L.invocation?.task_id===id && AHP_LIVE.state.L.invocation?.status==='completed',inv.id);
    return {...proof,reload:true};
  });
  await step('approvals',async()=>{
    await go('approvals');await requestClick('ahp-approvals','Approvals');
    const id=seed.approval_request_id || await page.evaluate(()=>AHP_LIVE.state.L.approvals?.[0]?.request_id);
    if(!id) return {status:'not_run',reason:'No real pending tool operation'};
    const response=page.waitForResponse(r=>new URL(r.url()).pathname.endsWith('/Approve'));
    await page.locator(`[data-action="ahp-approve"][data-id="${id}"]`).click();const r=await response;
    assert.equal(r.status(),200);assert.equal((await r.json()).approved,true);
    await page.waitForFunction(id=>!AHP_LIVE.state.L.approvals?.some(a=>a.request_id===id),id);
    return {request_id:id,approved:true,removedFromPending:true};
  });
  report.storage=await page.evaluate(()=>({local:localStorage.length,session:sessionStorage.length}));
  assert.deepEqual(report.storage,{local:0,session:0});assert.deepEqual(report.errors,[]);
  if(process.env.AHP_UI_LIVE_SCREENSHOT) await page.screenshot({path:process.env.AHP_UI_LIVE_SCREENSHOT,fullPage:true});
  await click('ahp-logout');await field('ahp-user').waitFor();
  assert.equal(await page.evaluate(()=>AHP_LIVE.state.S.bearer),'');report.logout='cleared';
} catch(e) {
  report.fatal={message:e.message,state:await state().catch(()=>({}))};throw e;
} finally {
  await context.close();await browser.close();
  if(process.env.AHP_UI_LIVE_REPORT) fs.writeFileSync(process.env.AHP_UI_LIVE_REPORT,JSON.stringify(report,null,2)+'\n');
  console.log(JSON.stringify(report,null,2));
}
if(Object.values(report.steps).some(s=>s.status==='failed')) process.exitCode=1;
