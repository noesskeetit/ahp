// Browser contract regressions. HTTP responses are test fixtures, not live E2E evidence.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { resolve, extname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
const { chromium } = await import(pathToFileURL(process.env.AHP_PLAYWRIGHT_MODULE || '/tmp/h10chain/pw/node_modules/playwright/index.mjs'));
const root = resolve('web/aifactory');
const server = createServer(async (req, res) => {
  try {
    const file = resolve(root, '.' + (req.url === '/' ? '/index.html' : req.url.split('?')[0]));
    if (!file.startsWith(root + '/')) throw Error('path');
    res.setHeader('content-type', ({'.js':'text/javascript','.css':'text/css','.html':'text/html','.svg':'image/svg+xml'})[extname(file)] || 'application/octet-stream');
    res.end(await readFile(file));
  } catch { res.writeHead(404).end(); }
});
await new Promise(r => server.listen(0, '127.0.0.1', r));
const origin = `http://127.0.0.1:${server.address().port}`;
const browser = await chromium.launch({headless:true});
const bytes = 'artifact content';
const digest = createHash('sha256').update(bytes).digest('hex');
let tokenFails = false;
const fixtures = {
  '/u-ahp/v1/auth/token': () => tokenFails ? [401,{code:'UNAUTHENTICATED'}] : [200,{access_token:'fixture-human',expires_in:600,subject:'owner',tenant:'tenant'}],
  '/u-ahp/v1/auth/logout': () => [200,{}],
  '/u-ahp/v1/registry/CatalogList': () => [200,{cards:[{installation_id:'install-one',agent_id:'agent-one',digest:'sha256:'+digest,environment:'test',visibility:'tenant',kind:'agent',published:true}]}],
  '/u-ahp/v1/memory/Search': () => [200,{hits:[{memory:{id:'memory-one',text:'Настоящая форма SearchHit',state:'active',kind:'user',subject:'owner'},score:1}]}],
  '/u-ahp/v1/triggers/List': () => [200,{triggers:[{trigger_id:'trigger-one',target:'agent:one',status:'ACTIVE',kind:'SCHEDULE',interval_ms:60000,fired_count:2,max_fires:5}]}],
  '/v1/space/ListChats': () => [200,{threads:[]}],
  '/u-ahp/v1/artifacts/Resolve': () => [200,{sha256:digest,versions:[{artifact_id:'other-artifact',version:'other-version',sha256:digest,size:bytes.length},{artifact_id:'artifact-one',version:'version-one',sha256:digest,size:bytes.length,name:'result.txt'}]}],
};
async function pageFor(hash = 'live') {
  const context = await browser.newContext({acceptDownloads:true});
  const page = await context.newPage();
  page.setDefaultTimeout(2500);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', async route => {
    const path = new URL(route.request().url()).pathname;
    if (path === '/u-ahp/v1/artifacts/download') return route.fulfill({status:200,body:bytes});
    if (fixtures[path]) { const [status,json] = fixtures[path](); return route.fulfill({status,json}); }
    return route.continue();
  });
  await page.goto(origin + '/#/' + hash);
  await page.waitForFunction(() => window.AHP_LIVE?.state.S.online);
  await page.waitForTimeout(150);
  return {page,context,errors};
}
try {
  await test('nonempty memory and triggers render without app table exceptions', async () => {
    const {page,context,errors} = await pageFor();
    try {
      await page.evaluate(async () => { await AHP_LIVE.api.searchMemory(''); await AHP_LIVE.api.listTriggers(); });
      assert.deepEqual(errors, []);
      assert.match(await page.locator('#main').innerText(), /Настоящая форма SearchHit/);
      assert.match(await page.locator('#main').innerText(), /trigger-one/);
    } finally { await context.close(); }
  });
  await test('original Memory Crons Agent Stacks sections display server results', async () => {
    const {page,context} = await pageFor('memory');
    try {
      for (const [hash,expected] of [['memory','Настоящая форма SearchHit'],['crons','trigger-one'],['stacks','agent-one']]) {
        await page.goto(origin + '/#/' + hash);
        await page.waitForTimeout(80);
        assert.match(await page.locator('#main').innerText(), new RegExp(expected));
      }
    } finally { await context.close(); }
  });
  await test('empty schedule target never becomes a fabricated task identifier', async () => {
    const {page,context}=await pageFor('crons');let writes=0;
    await page.route('**/u-ahp/v1/triggers/CreateSchedule',r=>{writes++;return r.fulfill({status:200,json:{trigger_id:'unused'}});});
    try{
      await page.locator('[name="ahp-trg-target"]').fill('');await page.locator('[data-action="ahp-trigger-create"]').click();await page.waitForTimeout(50);
      assert.equal(writes,0);assert.match(await page.locator('#main').innerText(),/trigger_target_required/);
    }finally{await context.close();}
  });
  await test('artifact selection uses exact ref and refuses mismatching download', async () => {
    const {page,context} = await pageFor('live');
    let downloads = 0;
    page.on('download', () => downloads++);
    try {
      const selected = await page.evaluate(async ({digest}) => {
        await AHP_LIVE.api.resolve(digest,{id:'artifact-one',version:'version-one',sha256:digest});
        return AHP_LIVE.state.L.artifact;
      }, {digest});
      assert.equal(selected.artifact_id, 'artifact-one');
      assert.equal(selected.version, 'version-one');
      await page.evaluate(() => { AHP_LIVE.state.L.artifact.digest = '0'.repeat(64); return AHP_LIVE.api.download(); });
      await page.waitForTimeout(80);
      assert.equal(downloads,0);
      assert.match(await page.locator('#main').innerText(), /sha_mismatch/);
      await page.evaluate(digest => { AHP_LIVE.state.L.artifact.digest = digest; return AHP_LIVE.api.download(); }, digest);
      await page.waitForTimeout(80);
      assert.equal(downloads,1);
    } finally { await context.close(); }
  });
  await test('selected installation invocation waits for actual terminal task and sends operator eval suite', async () => {
    const {page,context}=await pageFor('stacks');let invokeBody,evalBody,reads=0;
    await page.route('**/u-ahp/v1/registry/Invoke',r=>{invokeBody=r.request().postDataJSON();return r.fulfill({status:200,json:{task_id:'invoked-one'}});});
    await page.route('**/u-ahp/v1/registry/GetInvokedTask',r=>{reads++;return r.fulfill({status:200,json:{task:{id:'invoked-one',status:{state:reads>1?'completed':'working'},artifacts:[{artifactId:'checkpoint-one',parts:[{kind:'text',text:'selected-agent-result'}]}]}}});});
    await page.route('**/u-ahp/v1/evals/RunEval',r=>{evalBody=r.request().postDataJSON();return r.fulfill({status:200,json:{verdict:{id:'eval-one',task_set_id:'operator-suite',agent_digest:digest,outcome:'pass',tasks:[{task_id:'reference',passed:true}]}}});});
    try {
      await page.locator('[data-action="ahp-invoke-open"]').click();
      await page.locator('[name="ahp-invoke-text"]').fill('Selected installation task');
      await page.locator('[data-action="ahp-invoke"]').click();
      await page.waitForFunction(()=>AHP_LIVE.state.L.invocation?.status==='completed');
      assert.equal(invokeBody.installation_id,'install-one');assert.equal(invokeBody.input.text,'Selected installation task');assert.ok(reads>1);
      assert.match(await page.locator('#main').innerText(),/selected-agent-result/);
      await page.locator('[name="ahp-eval-suite"]').fill('operator-suite');await page.locator('[name="ahp-eval-digest"]').fill(digest);
      await page.locator('[data-action="ahp-eval-run"]').click();await page.waitForFunction(()=>AHP_LIVE.state.L.evalRun?.outcome==='pass');
      assert.doesNotMatch(await page.locator('#main').innerText(),/passing=undefined/);
      assert.deepEqual(evalBody,{suite_id:'operator-suite',agent_digest:digest});
    } finally {await context.close();}
  });
  await test('second message preserves thread and all user turns through reload', async () => {
    const {page,context}=await pageFor('chats');const requests=[];const turns=[];
    await page.route('**/v1/space/*',route=>{
      const op=new URL(route.request().url()).pathname.split('/').at(-1);const body=route.request().postDataJSON();
      let json={};
      if(op==='AcceptTurn'){requests.push(body);const id='turn-'+requests.length;turns.push({id,thread_id:'same-thread',status:'done',input:body.input,created_at:new Date(1000*requests.length).toISOString()});json={turn_id:id,thread_id:'same-thread',status:'accepted'};}
      if(op==='GetTurn') json={status:'done'};
      if(op==='ReadStream') json={events:[]};
      if(op==='ListTurns') json={turns};
      if(op==='ListChats') json={threads:[{thread_id:'same-thread',context_id:'same-context'}]};
      return route.fulfill({status:200,json});
    });
    await page.route('**/u-ahp/v1/model/Cost?*',r=>r.fulfill({status:200,json:{trees:[]}}));
    try {
      for(const text of ['Первое сообщение','Второе сообщение']){await page.locator('[name="ahp-run-text"]').fill(text);await page.locator('[name="ahp-context"]').fill('same-context');await page.locator('[data-action="ahp-run"]').click();await page.waitForFunction(()=>AHP_LIVE.state.L.turn?.status==='done');}
      assert.equal(requests[1].thread_id,'same-thread');
      await page.reload();await page.waitForFunction(()=>AHP_LIVE.state.L.turns?.length===2);
      const history=await page.locator('.ahp-conversation').innerText();assert.match(history,/Первое сообщение/);assert.match(history,/Второе сообщение/);
    }finally{await context.close();}
  });
  await test('mobile chat fits the viewport with durable IDs and cost rows', async () => {
    const {page,context}=await pageFor('chats');
    try{
      await page.setViewportSize({width:390,height:844});
      await page.evaluate(()=>{AHP_LIVE.state.L.turn={turn_id:'turn-one',thread_id:'20261102T103534.154515000Z_n5336rn7zjt64wn34fo6ennn7e',context_id:'browser-1793615733020',status:'done'};AHP_LIVE.state.L.cost={status:200,json:{trees:[{unit:'tokens',tokens_in:1899,tokens_out:620,tokens_total:2519,spent:2519,remaining:17481}]}};AHP_LIVE.paint();});
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth),390);
    }finally{await context.close();}
  });
  await test('table refresh re-reads the selected Workspace Sandbox and Stats data', async () => {
    for(const section of ['workspace','sandbox','stats']){
      const {page,context}=await pageFor(section);let calls=0;
      await page.route('**/u-ahp/v1/workspace/*',r=>{const op=new URL(r.request().url()).pathname.split('/').at(-1);if(op==='GetWorkspace'){calls++;return r.fulfill({status:200,json:{id:'workspace-one',name:'selected-project',head_rev:''}});}return r.fulfill({status:200,json:[]});});
      await page.route('**/u-ahp/v1/compute/GetComputer',r=>{calls++;assert.equal(r.request().postDataJSON().computer_id,'computer-one');return r.fulfill({status:200,json:{id:'computer-one',class:'X',profile:'ephemeral',status:'active'}});});
      await page.route('**/u-ahp/v1/model/Cost?*',r=>{calls++;return r.fulfill({status:200,json:{trees:[{unit:'tokens',spent:7}]}});});
      try{
        await page.evaluate(section=>{if(section==='workspace'){AHP_LIVE.state.L.workspace={id:'workspace-one',name:'selected-project'};AHP_LIVE.state.L.workspaceMembers=[{subject:'owner',role:'owner'}];}if(section==='sandbox')AHP_LIVE.state.L.computer={id:'computer-one',class:'X',profile:'ephemeral',status:'active'};if(section==='stats'){AHP_LIVE.state.L.turn={thread_id:'thread-one'};AHP_LIVE.state.L.cost={status:200,json:{trees:[{unit:'tokens',spent:1}]}};}AHP_LIVE.paint();},section);
        await page.locator('#main [data-action="refresh"]').first().click();await page.waitForTimeout(100);assert.equal(calls,1,section+' refreshed its own selected record');
      }finally{await context.close();}
    }
  });
  await test('legacy demo and setup-guide controls are absent from the connected shell', async () => {
    const {page,context}=await pageFor('memory');try{assert.equal(await page.locator('[data-action="demo-settings"],[data-action="guide"],[data-action="load-demo"],[data-action="toggle-guide"]').count(),0);}finally{await context.close();}
  });
  await test('chat repaint preserves reading position and follows new messages at bottom', async () => {
    const {page,context}=await pageFor('chats');
    try {
      await page.evaluate(()=>{AHP_LIVE.state.L.turnHistory=Array.from({length:30},(_,i)=>({id:'history-'+i,input:{content:'Message '+i},created_at:new Date(1000*i).toISOString()}));AHP_LIVE.paint();});
      const positions=await page.evaluate(()=>{
        let box=document.querySelector('.ahp-conversation');box.scrollTop=200;const before=box.scrollTop;AHP_LIVE.paint();box=document.querySelector('.ahp-conversation');const after=box.scrollTop;
        box.scrollTop=box.scrollHeight;AHP_LIVE.state.L.turnHistory.push({id:'last',input:{content:'Latest message'},created_at:new Date().toISOString()});AHP_LIVE.paint();box=document.querySelector('.ahp-conversation');
        return {before,after,bottomGap:box.scrollHeight-box.clientHeight-box.scrollTop};
      });
      assert.equal(positions.before,200);assert.equal(positions.after,positions.before);assert.ok(positions.bottomGap<=1);
    }finally{await context.close();}
  });
  await test('cancel completion cannot mutate a newly opened chat', async () => {
    const {page,context,errors}=await pageFor('chats');const getIds=[];
    await page.route('**/v1/space/CancelTurn',r=>r.fulfill({status:200,json:{terminal_frame_durable:true}}));
    await page.route('**/v1/space/GetTurn',r=>{getIds.push(r.request().postDataJSON().turn_id);return r.fulfill({status:200,json:{status:'canceled'}});});
    try {
      await page.evaluate(()=>{AHP_LIVE.state.L.turn={turn_id:'old-turn',thread_id:'old-thread',status:'running'};AHP_LIVE.paint();});
      await page.locator('[data-action="ahp-cancel"]').click();await page.locator('[data-action="ahp-chat-new"]').click();
      await page.waitForTimeout(1700);
      assert.deepEqual(errors,[]);assert.equal(await page.evaluate(()=>AHP_LIVE.state.L.turn),null);
      assert.ok(getIds.every(id=>id==='old-turn'));
    }finally{await context.close();}
  });
  await test('Workspace file rows and approvals use human routes and exact request IDs', async () => {
    const {page,context} = await pageFor('workspace');
    let approvalBody;
    await page.route('**/u-ahp/v1/workspace/*', route => {
      const op = new URL(route.request().url()).pathname.split('/').at(-1);
      return route.fulfill({status:200,json:op === 'GetWorkspace' ? {id:'ws-one',name:'project-one',head_rev:'rev-one'} : op === 'ListFiles' ? [{path:'notes.txt',artifact_id:'artifact-one',digest,mode:420}] : []});
    });
    await page.route('**/u-ahp/v1/tools/*', route => {
      const op = new URL(route.request().url()).pathname.split('/').at(-1);
      if (op === 'Approve') approvalBody = route.request().postDataJSON();
      return route.fulfill({status:200,json:op === 'Approvals' ? {approvals:[{request_id:'request-one',tool:'write_file',digest,operation_key:'operation-one',expires_at:'2099-01-01T00:00:00Z'}]} : {approved:true}});
    });
    try {
      await page.locator('[name="ahp-workspace-name"]').fill('project-one');
      await page.locator('[data-action="ahp-workspace-open"]').click();
      await page.getByText('notes.txt',{exact:true}).waitFor();
      await page.goto(origin + '/#/approvals');
      await page.locator('[data-action="ahp-approvals"]').click();
      await page.locator('[data-action="ahp-approve"]').click();
      assert.deepEqual(approvalBody,{request_id:'request-one',tool:'write_file',digest});
    } finally { await context.close(); }
  });
  await test('Sandbox uses real lifecycle API response to disclose mock backend', async () => {
    const {page,context} = await pageFor('sandbox');
    const calls=[];
    await page.route('**/u-ahp/v1/compute/*', route => {
      const op=new URL(route.request().url()).pathname.split('/').at(-1); calls.push(op);
      return route.fulfill({status:200,headers:{'AHP-Compute-Backend':'mock','AHP-Compute-Disclosure':'MOCK-COMPUTE-BACKEND: no isolation'},json:op === 'Wake' ? {id:'inc-one',computer_id:'computer-one',backend:'mock',status:'running'} : {id:'computer-one',class:'X',profile:'ephemeral',status:'active',incarnation:1}});
    });
    try {
      await page.locator('[data-action="ahp-compute-create"]').click();
      await page.locator('[data-action="ahp-compute-wake"]').click();
      await page.getByText('MOCK', {exact:true}).waitFor();
      assert.ok(calls.includes('CreateComputer') && calls.includes('Wake'));
      assert.match(await page.locator('#main').innerText(),/no isolation/);
    } finally {await context.close();}
  });
  await test('empty and refused lists never keep stale server rows', async () => {
    const {page,context} = await pageFor('memory');
    try {
      await page.route('**/u-ahp/v1/memory/Search', r=>r.fulfill({status:200,json:{hits:[]}}));
      await page.locator('[data-action="ahp-memory-search"]').first().click();
      await page.getByText('Подходящих записей пока нет.',{exact:true}).waitFor();
      await page.route('**/u-ahp/v1/memory/Search', r=>r.fulfill({status:503,json:{code:'UNAVAILABLE'}}));
      await page.locator('[data-action="ahp-memory-search"]').first().click();
      await page.getByText(/memory_search_refused/).waitFor();
      assert.doesNotMatch(await page.locator('#main').innerText(),/Настоящая форма SearchHit/);
    } finally {await context.close();}
  });
  await test('logout clears protected data, and only tool frames can supply ArtifactRef', async () => {
    const {page,context} = await pageFor('memory');
    try {
      const refs=await page.evaluate(digest=>{
        const content={id:'artifact-one',version:'version-one',sha256:digest};
        return {model:AHP_SHAPES.artifactRef([{kind:'model',payload:{content}}]),tool:AHP_SHAPES.artifactRef([{kind:'tool',payload:{step_type:'tool',content}}])};
      },digest);
      assert.equal(refs.model,null);assert.equal(refs.tool.id,'artifact-one');
      await page.locator('[data-action="ahp-logout"]').click();
      await page.locator('[name="ahp-user"]').waitFor();
      assert.equal(await page.evaluate(()=>AHP_LIVE.state.S.bearer),'');
      assert.doesNotMatch(await page.locator('#main').innerText(),/Настоящая форма SearchHit/);
      await page.route('**/u-ahp/v1/auth/login',r=>r.fulfill({status:401,json:{code:'INVALID_CREDENTIALS'}}));
      await page.locator('[name="ahp-user"]').fill('owner');await page.locator('[name="ahp-pass"]').fill('wrong-password');await page.locator('[data-action="ahp-login"]').click();
      await page.waitForFunction(()=>document.querySelector('[name="ahp-pass"]').value==='');
    } finally {await context.close();}
  });
  await test('expired session refresh terminates and clears previous user data', async () => {
    const {page,context} = await pageFor('memory');
    try {
      tokenFails = true;
      await page.evaluate(async () => { AHP_LIVE.state.S.exp = 1; await AHP_LIVE.api.searchMemory(''); });
      assert.equal(await page.evaluate(() => AHP_LIVE.state.S.online),false);
      assert.equal(await page.evaluate(() => AHP_LIVE.state.S.bearer),'');
      assert.doesNotMatch(await page.locator('#main').innerText(),/Настоящая форма SearchHit/);
    } finally { tokenFails = false; await context.close(); }
  });
} finally {
  await browser.close();
  await new Promise(r => server.close(r));
}
