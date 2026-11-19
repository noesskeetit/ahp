#!/usr/bin/env node
// Manual synchronous PostgreSQL failover drill in the disposable restore profile.
// It is not an HA controller and does not claim independent physical hosts.
import fs from'node:fs';import path from'node:path';import crypto from'node:crypto';import{spawnSync}from'node:child_process';import{fileURLToPath}from'node:url';import{images}from'../infra/local-k8s/profile.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),home=path.resolve(process.env.AHP_K8S_RUNTIME||path.join(root,'private/deployment-complete'));
const ns='ahp-poc-restore',args=['--kubeconfig',path.join(home,'kubeconfig'),'--context','k3d-ahp-handbook-poc','--request-timeout=30s','-n',ns];
const pause=ms=>new Promise(r=>setTimeout(r,ms));
function k(a,o={}){const r=spawnSync('kubectl',[...args,...a],{encoding:'utf8',maxBuffer:4<<20,...o});if(r.status!==0)throw Error('kubectl '+a[0]+' failed: '+(o.secret?'details suppressed':r.stderr?.slice(-800)));return r.stdout;}
function sql(pod,q,secret=false){return k(['exec','-i',pod,'--','psql','-U','ahp','-d','ahp','-v','ON_ERROR_STOP=1','-At'],{input:q,secret}).trim();}
function apply(o){k(['apply','-f','-'],{input:JSON.stringify(o),secret:true});}
const labels=n=>({'ahp.local/component':n,'app.kubernetes.io/part-of':'ahp-handbook'}),security={readOnlyRootFilesystem:true,allowPrivilegeEscalation:false,capabilities:{drop:['ALL']}};
async function setup(){
 if(sql('postgres-0','SELECT pg_is_in_recovery();')!=='f')throw Error('target is not primary');
 const existing=k(['get','statefulsets','-o','name']);if(existing.includes('postgres-standby'))throw Error('standby exists; refusing reinitialize');
 const password=crypto.randomBytes(24).toString('hex');
 apply({apiVersion:'v1',kind:'Secret',metadata:{name:'pg-replication',namespace:ns,labels:labels('postgres-standby')},stringData:{password}});
 sql('postgres-0',"CREATE ROLE ahp_replica WITH REPLICATION LOGIN PASSWORD '"+password+"';",true);
 k(['exec','postgres-0','--','sh','-ec','printf "\\nhost replication ahp_replica all scram-sha-256\\n" >> "$PGDATA/pg_hba.conf"']);sql('postgres-0','SELECT pg_reload_conf();');
 apply({apiVersion:'v1',kind:'PersistentVolumeClaim',metadata:{name:'postgres-standby',namespace:ns,labels:labels('postgres-standby')},spec:{accessModes:['ReadWriteOnce'],storageClassName:'local-path',resources:{requests:{storage:'1Gi'}}}});
 for(const [name,selector,policy]of[
  ['primary-from-replica','postgres',{policyTypes:['Ingress'],ingress:[{from:[{podSelector:{matchLabels:labels('postgres-standby')}}],ports:[{protocol:'TCP',port:5432}]}]}],
  ['replica-to-primary','postgres-standby',{policyTypes:['Egress'],egress:[{to:[{podSelector:{matchLabels:labels('postgres')}}],ports:[{protocol:'TCP',port:5432}]}]}],
  ['standby-from-cell','postgres-standby',{policyTypes:['Ingress'],ingress:[{from:['cell','ops'].map(n=>({podSelector:{matchLabels:labels(n)}})),ports:[{protocol:'TCP',port:5432}]}]}],
  ['cell-to-standby','cell',{policyTypes:['Egress'],egress:[{to:[{podSelector:{matchLabels:labels('postgres-standby')}}],ports:[{protocol:'TCP',port:5432}]}]}],
 ])apply({apiVersion:'networking.k8s.io/v1',kind:'NetworkPolicy',metadata:{name,namespace:ns,labels:labels(selector)},spec:{podSelector:{matchLabels:labels(selector)},...policy}});
 const env=[{name:'PGDATA',value:'/var/lib/postgresql/data/pgdata'},{name:'PGPASSWORD',valueFrom:{secretKeyRef:{name:'pg-replication',key:'password'}}}];
 const mounts=[{name:'data',mountPath:'/var/lib/postgresql/data'},{name:'tmp',mountPath:'/var/run/postgresql'}];
 const resources={requests:{cpu:'50m',memory:'192Mi'},limits:{cpu:'500m',memory:'256Mi'}};
 apply({apiVersion:'apps/v1',kind:'StatefulSet',metadata:{name:'postgres-standby',namespace:ns,labels:labels('postgres-standby')},spec:{replicas:1,serviceName:'postgres-standby',selector:{matchLabels:labels('postgres-standby')},template:{metadata:{labels:labels('postgres-standby')},spec:{serviceAccountName:'ahp-data',automountServiceAccountToken:false,terminationGracePeriodSeconds:30,securityContext:{runAsNonRoot:true,runAsUser:999,runAsGroup:999,fsGroup:999,seccompProfile:{type:'RuntimeDefault'}},initContainers:[{name:'basebackup',image:images.postgres,imagePullPolicy:'IfNotPresent',securityContext:security,resources,env,volumeMounts:mounts,command:['sh','-ec','if [ ! -e "$PGDATA/PG_VERSION" ]; then pg_basebackup --dbname="host=postgres user=ahp_replica application_name=ahp_sync_standby" -D "$PGDATA" -X stream -R --checkpoint=fast; fi; chmod 700 "$PGDATA"']}],containers:[{name:'postgres',image:images.postgres,imagePullPolicy:'IfNotPresent',securityContext:security,resources,env,volumeMounts:mounts,command:['postgres'],readinessProbe:{exec:{command:['pg_isready','-U','ahp','-d','ahp']},periodSeconds:3},startupProbe:{exec:{command:['pg_isready','-U','ahp','-d','ahp']},periodSeconds:3,failureThreshold:40}}],volumes:[{name:'data',persistentVolumeClaim:{claimName:'postgres-standby'}},{name:'tmp',emptyDir:{sizeLimit:'32Mi'}}]}}}});
 k(['rollout','status','statefulset/postgres-standby','--timeout=180s']);
 sql('postgres-0',"ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (ahp_sync_standby)';\nALTER SYSTEM SET synchronous_commit='remote_apply';\nSELECT pg_reload_conf();");
 let replication;for(let i=0;i<30;i++){replication=sql('postgres-0',"SELECT json_build_object('application_name',application_name,'sync_state',sync_state,'state',state,'sent_lsn',sent_lsn,'replay_lsn',replay_lsn) FROM pg_stat_replication WHERE application_name='ahp_sync_standby';");if(replication&&JSON.parse(replication).sync_state==='sync')break;await pause(1000)}
 if(!replication||JSON.parse(replication).sync_state!=='sync'||sql('postgres-0','SHOW synchronous_commit;')!=='remote_apply')throw Error('synchronous replica not confirmed');
 const report={at:new Date().toISOString(),namespace:ns,replication:JSON.parse(replication),synchronousCommit:'remote_apply',primaryPVC:'postgres',standbyPVC:'postgres-standby',failureDomain:'same local node/Docker host; no host or disk failure tolerance claimed',status:'passed'};
 fs.writeFileSync(path.join(home,'pg-sync-setup-report.json'),JSON.stringify(report,null,2)+'\n',{mode:0o600});console.log(JSON.stringify(report));
}
async function failover(){
 const before=JSON.parse(fs.readFileSync(path.join(home,'pg-sync-product-before.json')));if(before.status!=='passed'||!before.model?.turn)throw Error('real AHP accepted turn required before failover');
 if(sql('postgres-0','SHOW synchronous_commit;')!=='remote_apply'||sql('postgres-0',"SELECT sync_state FROM pg_stat_replication WHERE application_name='ahp_sync_standby';")!=='sync')throw Error('primary no longer synchronous');
 const started=Date.now(),pod=JSON.parse(k(['get','pod','postgres-0','-o','json']));
 // Immediate abort does not perform the clean shutdown checkpoint. Then fence
 // the StatefulSet at zero and await deletion before promoting, never two writers.
 const abort=spawnSync('kubectl',[...args,'exec','postgres-0','--','pg_ctl','-D','/var/lib/postgresql/data/pgdata','stop','-m','immediate','-w'],{encoding:'utf8'});
 // PID 1 exits on immediate shutdown, which can kill the exec process (137)
 // before pg_ctl itself returns. Fencing/deletion below is still mandatory.
 if(![0,137].includes(abort.status))throw Error('primary immediate abort failed');
 k(['scale','statefulset/postgres','--replicas=0']);k(['wait','--for=delete','pod/postgres-0','--timeout=90s']);
 const promoted=sql('postgres-standby-0','SELECT pg_promote(true,30);');if(promoted!=='t')throw Error('promotion failed');
 if(sql('postgres-standby-0','SELECT pg_is_in_recovery();')!=='f')throw Error('standby still recovering');
 // During this manual drill the promoted primary is intentionally read/write
 // without a second standby. Restore sync redundancy before normal service/SLA.
 sql('postgres-standby-0',"ALTER SYSTEM SET synchronous_standby_names='';\nSELECT pg_reload_conf();");
 k(['patch','service','postgres','--type=merge','-p',JSON.stringify({spec:{selector:{'ahp.local/component':'postgres-standby'}}})]);
 const report={at:new Date().toISOString(),namespace:ns,confirmedTurn:before.model.turn,primaryPodUID:pod.metadata.uid,primaryFailure:'pg_ctl immediate; no shutdown checkpoint',abortExecStatus:abort.status,primaryFenced:true,primaryReplicas:0,standbyPromoted:true,promotionMs:Date.now()-started,failureDomain:'same physical host; operator-triggered failover, no automatic HA',redundancyAfterPromotion:'single primary until a new synchronous standby is seeded',samples:[],status:'promotion_complete_pending_product_recheck'};
 const origin=process.env.AHP_K8S_ORIGIN||'http://127.0.0.1:30223';
 if(new URL(origin).hostname!=='127.0.0.1')throw Error('drill origin must be loopback');
 const user=JSON.parse(fs.readFileSync(path.join(home,'credentials.json'))).users[0];
 for(let attempt=0;Date.now()-started<60000;attempt++){
  let cookie='',token='';const sample={attempt,elapsedMs:Date.now()-started,requests:[]};
  async function call(route,body){const r=await fetch(origin+route,{method:'POST',headers:{'content-type':'application/json','x-ahp-session':'1',...(cookie?{cookie}:{}),...(token?{authorization:'Bearer '+token}:{})},body:JSON.stringify(body),signal:AbortSignal.timeout(5000)});sample.requests.push({route,status:r.status});const cookies=r.headers.getSetCookie();if(cookies.length)cookie=cookies.map(x=>x.split(';')[0]).join('; ');if(r.status!==200)throw Error('http '+r.status);return r.json();}
  try{
   await call('/u-ahp/v1/auth/login',{username:user.username,password:user.password});token=(await call('/u-ahp/v1/auth/token',{})).access_token;
   const turn=await call('/v1/space/GetTurn',{turn_id:before.model.turn});if((turn.status??turn.turn?.status)!=='done')throw Error('confirmed turn not done');
   const stream=await call('/v1/space/ReadStream',{thread_id:before.model.thread,after:0,limit:200});const text=stream.events.filter(x=>x.kind==='model').map(x=>JSON.stringify(x.payload)).join('\n');
   if(crypto.createHash('sha256').update(text).digest('hex')!==before.model.outputSHA256)throw Error('confirmed stream differs');
   sample.status='passed';report.samples.push(sample);report.firstSuccessfulLoginAndReadMs=Date.now()-started;report.status='passed';break;
  }catch(e){sample.status='failed';sample.error=e.message;report.samples.push(sample);await pause(500);}
 }
 if(report.status!=='passed'){report.status='failed';process.exitCode=1;}
 fs.writeFileSync(path.join(home,'pg-sync-failover-report.json'),JSON.stringify(report,null,2)+'\n',{mode:0o600});console.log(JSON.stringify(report));
}
if(process.argv[2]==='setup')await setup();else if(process.argv[2]==='failover')await failover();else throw Error('usage: local-k8s-pg-sync.mjs setup|failover (restore namespace only)');
