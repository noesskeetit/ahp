import assert from 'node:assert/strict';
import fs from 'node:fs';
import {pathToFileURL} from 'node:url';
const [origin,module,evidence]=process.argv.slice(2);
const {chromium}=await import(pathToFileURL(module));
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:1440,height:1000}});
const page=await context.newPage();page.setDefaultTimeout(15000);
const errors=[],http=[];page.on('pageerror',e=>errors.push(e.message));page.on('response',r=>{if(/\/(profile|registry)\//.test(r.url()))http.push({path:new URL(r.url()).pathname,status:r.status()});});
const field=name=>page.locator(`[name="${name}"]`),click=name=>page.locator(`[data-action="${name}"]`).click();
async function action(name,op,want=200){const wait=page.waitForResponse(r=>new URL(r.url()).pathname.endsWith('/'+op));await click(name);const r=await wait;const j=await r.json();assert.equal(r.status(),want,op+' '+JSON.stringify(j));await page.waitForTimeout(70);return j;}
async function login(name,section){await page.goto(origin+'/#/'+section);if(await page.locator('[data-action="ahp-logout"]:visible').count())await click('ahp-logout');await field('ahp-user').fill(name);await field('ahp-pass').fill('fixture');await click('ahp-login');await page.waitForFunction(()=>AHP_LIVE.state.S.online);await page.waitForTimeout(150);}
try{
 await login('admin','admin');await page.waitForFunction(()=>AHP_LIVE.state.L.profileAdmin?.loaded);
 await field('pa-route-id').fill('fast');await field('pa-route-model').fill('model-v1');await action('ahp-pa-route-save','PutModelRoute');
 await field('pa-skill-id').fill('writing');await field('pa-skill-instructions').fill('Skill one');await action('ahp-pa-skill-save','PutSkill');
 await field('pa-tenant-instructions').fill('Tenant instructions');await field('pa-default-route').fill('fast');await field('pa-allowed-routes').fill('fast');await field('pa-allowed-skills').fill('writing');await field('pa-default-skills').fill('writing');await field('pa-tools').fill('save_artifact');await field('pa-tenant-max').fill('8');await field('pa-allow-instructions').check();await field('pa-allow-personal').check();await action('ahp-pa-config-save','PutConfig');
 if(evidence){fs.mkdirSync(evidence,{recursive:true});await page.screenshot({path:evidence+'/admin.png',fullPage:true});}
 await login('alice','profile');await page.waitForFunction(()=>AHP_LIVE.state.L.profileAdmin?.loaded);
 await field('pa-personal-id').fill('my-style');await field('pa-personal-instructions').fill('Personal skill instructions');await action('ahp-pa-personal-save','PutPersonalSkill');
 await field('pa-instructions').fill('Alice concise');await field('pa-max-steps').fill('4');await field('pa-route').selectOption('fast');await field('pa-personal:my-style').check();await action('ahp-pa-profile-save','PutProfile');
 await page.reload();await page.waitForFunction(()=>AHP_LIVE.state.L.profileAdmin?.profile?.revision===1);assert.equal(await field('pa-instructions').inputValue(),'Alice concise');
 const denied=await page.evaluate(async()=>{const r=await AHP_LIVE.api.raw('/u-ahp/v1/profile/PutProfile',{body:{subject:'bob',expected_revision:1,data:{}}});return r.status;});assert.equal(denied,400);
 const second=await context.newPage();await second.goto(origin+'/#/profile');await second.waitForFunction(()=>AHP_LIVE.state.L.profileAdmin?.profile?.revision===1);
 await field('pa-instructions').fill('Alice second revision');await action('ahp-pa-profile-save','PutProfile');
 const conflict=second.waitForResponse(r=>new URL(r.url()).pathname.endsWith('/PutProfile'));await second.locator('[name="pa-instructions"]').fill('stale');await second.locator('[data-action="ahp-pa-profile-save"]').click();assert.equal((await conflict).status(),409);await second.close();
 if(evidence)await page.screenshot({path:evidence+'/profile.png',fullPage:true});
 await page.goto(origin+'/#/admin');await click('ahp-pa-reload');await page.waitForFunction(()=>AHP_LIVE.state.L.profileAdmin?.admin===false);assert.match(await page.locator('#main').innerText(),/Нет прав администратора/);
 const mutation=await page.evaluate(async()=>{const r=await AHP_LIVE.api.raw('/u-ahp/v1/registry/PutModelRoute',{body:{route:'rogue',expected_revision:0,document:{model:'rogue'}}});return r.status;});assert.equal(mutation,403);
 await login('bob','profile');await page.waitForFunction(()=>AHP_LIVE.state.L.profileAdmin?.loaded);assert.equal(await field('pa-instructions').inputValue(),'');assert.equal(await field('pa-personal:my-style').count(),0);
 assert.deepEqual(errors,[]);
 const report={status:'passed',kind:'real-browser-real-PG-mTLS-settings; login and unrelated endpoints are fixtures',checks:['admin model route and skill writes','tenant policy','personal skill','profile save and reload','cross-subject field rejected','CAS conflict','ordinary admin denied','two-user isolation'],http,errors};if(evidence)fs.writeFileSync(evidence+'/report.json',JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));
}catch(e){console.error(JSON.stringify({error:e.message,profile:await page.evaluate(()=>AHP_LIVE.state.L.profileAdmin),plates:await page.evaluate(()=>AHP_LIVE.state.L.plates),http}));if(evidence)await page.screenshot({path:evidence+'/failure.png',fullPage:true});throw e;}finally{await browser.close();}
