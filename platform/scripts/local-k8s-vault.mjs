import fs from 'node:fs';import{spawnSync,spawn}from'node:child_process';import crypto from'node:crypto';import path from'node:path';import{fileURLToPath}from'node:url';
const rootDirectory=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const home=path.resolve(process.env.AHP_K8S_RUNTIME||path.join(rootDirectory,'private/deployment-complete'))+path.sep;
const config=home+'kubeconfig',ns=process.env.AHP_K8S_NAMESPACE||'ahp-poc';const args=['--kubeconfig',config,'--context','k3d-ahp-handbook-poc','-n',ns];
function k(a){const r=spawnSync('kubectl',[...args,...a],{encoding:'utf8'});if(r.status)throw Error('probe exec failed');return r.stdout}
const pf=spawn('kubectl',[...args,'port-forward','--address=127.0.0.1','svc/vault','30201:8200'],{stdio:'ignore'});
if(!['ahp-poc','ahp-poc-restore'].includes(ns))throw Error('isolated namespace required');
const root=JSON.parse(fs.readFileSync(home+'vault-'+ns+'.json')).root_token;const hash=x=>crypto.createHash('sha256').update(x).digest('hex');
const pause=ms=>new Promise(r=>setTimeout(r,ms));
async function api(method,path,body){const r=await fetch('http://127.0.0.1:30201/v1/'+path,{method,headers:{'x-vault-token':root,'content-type':'application/json'},body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(5000)});if(!r.ok)throw Error('admin API '+path+' status '+r.status+' '+JSON.stringify((await r.json()).errors));const t=await r.text();return t?JSON.parse(t):{};}
const script=`(async()=>{const base='http://127.0.0.1:8201/v1/'; const r=await fetch(base+'auth/token/lookup-self');if(r.status!==200){console.log(JSON.stringify({status:r.status}));return}const d=(await r.json()).data;const own=await fetch(base+'secret/data/ahp-poc/model/qwen');const foreign=await fetch(base+'secret/data/other-tenant/private');console.log(JSON.stringify({status:r.status,epoch:d.issue_time,ttl:d.ttl,creation_ttl:d.creation_ttl,renewable:d.renewable,own:own.status,foreign:foreign.status}))})().catch(()=>process.exit(1))`;
const probe=()=>JSON.parse(k(['exec','deployment/cell','-c','backend','--','node','-e',script]));
const report={at:new Date().toISOString(),namespace:ns,samples:[]};const safe=s=>({...s,epoch:s.epoch?hash(s.epoch):undefined});let original;
try{
 for(let i=0;i<30;i++){try{await api('GET','sys/health');break}catch{await pause(250)}}
 original=(await api('GET','auth/kubernetes/role/ahp-runtime')).data;
 const before=probe();report.before=safe(before);
 await api('POST','auth/kubernetes/role/ahp-runtime',{...original,token_ttl:10,token_max_ttl:30});
 const revoked=k(['exec','deployment/cell','-c','backend','--','node','-e',"fetch('http://127.0.0.1:8201/v1/auth/token/revoke-self',{method:'POST'}).then(r=>{console.log(r.status);if(r.status!==204)process.exit(1)})"]); report.revocationStatus=Number(revoked.trim());
 let next;for(let i=0;i<50;i++){await pause(2000);try{next=probe();report.samples.push({...safe(next),at:new Date().toISOString()});if(next.status===200&&next.epoch!==before.epoch)break}catch{}}
 if(next?.status!==200||next.epoch===before.epoch)throw Error('no re-auth after explicit revocation');
 report.revocationRecovered=true;const first=next.epoch;let lastTTL=next.ttl,renew=false,rotated=false;
 for(let i=0;i<24;i++){await pause(2000);const s=probe();report.samples.push({...safe(s),at:new Date().toISOString()});if(s.status!==200)continue;if(s.epoch===first&&s.ttl>lastTTL)renew=true;if(s.epoch!==first)rotated=true;lastTTL=s.ttl;if(s.own!==200||s.foreign!==403)throw Error('runtime policy scope violated');if(renew&&rotated)break}
 if(!renew||!rotated)throw Error('renewal/max-TTL re-auth not both observed');report.renewalObserved=renew;report.maxTTLRenewAuthentication=rotated;report.status='passed';
}catch(e){report.status='failed';report.error=e.message;process.exitCode=1;}finally{if(original)await api('POST','auth/kubernetes/role/ahp-runtime',original);pf.kill('SIGTERM');fs.writeFileSync(home+'vault-lifecycle-report.json',JSON.stringify(report,null,2)+'\n',{mode:0o600});console.log(JSON.stringify({status:report.status,error:report.error,samples:report.samples.length,revocationRecovered:report.revocationRecovered,renewalObserved:report.renewalObserved,maxTTLRenewAuthentication:report.maxTTLRenewAuthentication}));}
