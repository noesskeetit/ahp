// Two real human sessions and existing owned data; never substitutes a missing ID.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
const { chromium }=await import(pathToFileURL(process.env.AHP_PLAYWRIGHT_MODULE || '/tmp/h10chain/pw/node_modules/playwright/index.mjs'));
const origin=process.env.AHP_UI_LIVE_ORIGIN;
const owner=JSON.parse(fs.readFileSync(process.env.AHP_UI_LIVE_LOGIN_JSON,'utf8'));
const outsider=JSON.parse(fs.readFileSync(process.env.AHP_UI_OUTSIDER_LOGIN_JSON,'utf8'));
const workspace=JSON.parse(fs.readFileSync(process.env.AHP_UI_WORKSPACE_REPORT,'utf8'));
const memoryID=process.env.AHP_UI_MEMORY_ID;
assert.ok(memoryID && workspace.status==='passed');
const report={origin,workspace:workspace.workspace,memoryID,errors:[]};
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({acceptDownloads:true});
const page=await context.newPage();page.setDefaultTimeout(20000);
page.on('pageerror',e=>report.errors.push(e.message));
async function login(user) {
  await page.goto(origin+'/?session='+encodeURIComponent(user.subject)+'#/memory');
  await page.locator('[name="ahp-user"]').fill(user.email||user.username);
  await page.locator('[name="ahp-pass"]').fill(user.password);
  await page.locator('[data-action="ahp-login"]').click();
  await page.waitForFunction(subject=>AHP_LIVE.state.S.subject===subject && ['ok','empty'].includes(AHP_LIVE.state.L.memoryOutcome?.state),user.subject);
}
try {
  await login(owner);
  assert.equal(await page.evaluate(id=>AHP_LIVE.state.L.memoryOutcome.rows.some(m=>m.id===id),memoryID),true);
  await page.goto(origin+'/#/workspace');await page.locator('[name="ahp-workspace-name"]').fill(workspace.workspace);
  await page.locator('[data-action="ahp-workspace-open"]').click();
  await page.waitForFunction(()=>AHP_LIVE.state.L.workspaceFiles?.length>0);
  const event=page.waitForEvent('download');await page.locator('[data-action="ahp-workspace-file"]').first().click();
  const download=await event,bytes=fs.readFileSync(await download.path());await download.delete();
  assert.equal(createHash('sha256').update(bytes).digest('hex'),workspace.artifact.sha256);
  report.owner={memoryVisible:true,fileDownload:true,hashMatch:true};
  await page.locator('[data-action="ahp-logout"]').click();await page.locator('[name="ahp-user"]').waitFor();
  await login(outsider);
  assert.equal(await page.evaluate(id=>AHP_LIVE.state.L.memoryOutcome.rows.some(m=>m.id===id),memoryID),false);
  await page.goto(origin+'/#/workspace');await page.locator('[name="ahp-workspace-name"]').fill(workspace.workspace);
  const response=page.waitForResponse(r=>new URL(r.url()).pathname.endsWith('/workspace/GetWorkspace'));
  await page.locator('[data-action="ahp-workspace-open"]').click();const r=await response;
  assert.ok([403,404].includes(r.status()),'nonmember cannot open an existing private Workspace');
  assert.equal(await page.locator('[data-action="ahp-workspace-file"]').count(),0);
  const file=await page.evaluate(async({workspace,revision})=>{
    const r=await AHP_LIVE.api.raw('/u-ahp/v1/workspace/ReadFile',{body:{workspace,revision,path:'checked.txt'}});
    return {status:r.status,code:r.json?.code};
  },{workspace:workspace.workspace,revision:workspace.revision});
  assert.ok([403,404].includes(file.status),'nonmember cannot read a known existing revision/file');
  report.outsider={memoryHidden:true,workspaceStatus:r.status(),file};
  assert.deepEqual(report.errors,[]);
  await page.locator('[data-action="ahp-logout"]').click();report.status='passed';
}catch(e){report.status='failed';report.error=e.message;throw e;}
finally{
  await context.close();await browser.close();
  if(process.env.AHP_UI_ISOLATION_REPORT) fs.writeFileSync(process.env.AHP_UI_ISOLATION_REPORT,JSON.stringify(report,null,2)+'\n');
  console.log(JSON.stringify(report,null,2));
}
