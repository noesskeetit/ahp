#!/usr/bin/env node
// Owned, manual PostgreSQL synchronous-pair profile. No automatic HA claim.
// Control and credentials belong in a private directory outside source control.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {spawnSync} from 'node:child_process';

const mode=process.argv[2], argv=process.argv.slice(3);
const option=name=>{const i=argv.indexOf(name);return i<0?'':argv[i+1]||'';};
const control=option('--control');
const label='ahp.platform-closure.owner', data='/var/lib/postgresql/data';
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
let c, stage='input';
const write=(file,value)=>{fs.writeFileSync(file,value,{mode:0o600});fs.chmodSync(file,0o600);};
function docker(args,{input,timeout=30000}={}) {
  const r=spawnSync('docker',['--context',c.context,...args],{input,encoding:'utf8',timeout,maxBuffer:4<<20});
  if(r.status!==0){
    write(path.join(path.dirname(control),'last-error.log'),String(r.stderr||r.error?.message||''));
    throw Error('docker operation failed');
  }
  return r.stdout.trim();
}
function inspect(kind,name){return JSON.parse(docker([kind,'inspect',name]))[0];}
function owned(kind,name){
  const r=inspect(kind,name),labels=kind==='container'?r.Config.Labels:r.Labels;
  if(labels?.[label]!==c.owner||labels?.['com.docker.compose.project']!=='ahp-handbook')throw Error('resource owner mismatch');
  if(kind==='container'&&c.volumes?.[name]&&(!r.Mounts.some(m=>m.Destination===data&&m.Name===c.volumes[name])||r.Config.Image!==c.image))throw Error('profile volume/image identity changed');
  return r;
}
function existing(kind,name){
  const r=spawnSync('docker',['--context',c.context,kind,'inspect',name],{encoding:'utf8',timeout:10000});
  if(r.status===0)return true;
  if(/No such|not found/i.test(r.stderr))return false;
  throw Error('resource existence check failed');
}
function save(){write(control,JSON.stringify(c,null,2)+'\n');}
function sql(name,query){owned('container',name);return docker(['exec','-i',name,'psql','-X','-qAt','-v','ON_ERROR_STOP=1','-U','closure','-d','postgres'],{input:query});}
function secrets(){return JSON.parse(fs.readFileSync(c.secrets,'utf8'));}
const labels=()=>['--label','com.docker.compose.project=ahp-handbook','--label',label+'='+c.owner];
const common=['postgres','-c','wal_level=replica','-c','wal_log_hints=on','-c','full_page_writes=on','-c','fsync=on','-c','shared_buffers=32MB','-c','max_connections=40','-c','max_wal_senders=4','-c','max_replication_slots=4','-c','max_wal_size=128MB','-c','min_wal_size=32MB','-c','wal_keep_size=64MB','-c','max_slot_wal_keep_size=128MB'];
// pg_rewind's crash recovery starts postgres without postmaster argv. File
// settings must match boot flags, including WAL retention and memory limits.
const persisted=common.filter((_,i)=>i>0&&i%2===0).map(value=>{const at=value.indexOf('=');return `ALTER SYSTEM SET ${value.slice(0,at)}='${value.slice(at+1)}';`;}).join('\n');
const app=name=>name===c.a?'ahp_ha_a':'ahp_ha_b';
const operatorName=suffix=>(c.prefix||c.owner+'-state-ha')+'-'+suffix;
async function until(fn,seconds=30){let error;const end=Date.now()+seconds*1000;while(Date.now()<end){try{const r=fn();if(r)return r;}catch(e){error=e;}await pause(250);}throw error||Error('readiness timeout');}
async function ready(name){await until(()=>{owned('container',name);return docker(['exec',name,'pg_isready','-h','127.0.0.1','-U','closure','-d',c.database])!=='';});}
function health(){
  const r=JSON.parse(sql(c.primary,"SELECT json_build_object('commit',current_setting('synchronous_commit'),'named',current_setting('synchronous_standby_names')<>'','in_recovery',pg_is_in_recovery(),'sync_replicas',(SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND sync_state='sync'))"));
  return {...r,ready:r.commit==='remote_apply'&&r.named&&!r.in_recovery&&r.sync_replicas===1};
}
function validate(){
  if(!/^ahp-closure-[a-f0-9]{10}$/.test(c.owner)||c.context!=='ahp-handbook-prof'||c.network!==c.owner+'-net'||!/^sha256:[a-f0-9]{64}$/.test(c.image))throw Error('invalid isolated profile');
  const infrastructure=JSON.parse(fs.readFileSync(c.infrastructure,'utf8'));
  if(infrastructure.owner!==c.owner||infrastructure.status!=='ready')throw Error('infrastructure is not ready/owned');
  owned('network',c.network);owned('container',infrastructure.runner);
}
function run(name,volume,envFile,args){
  if(existing('container',name))throw Error('container exists; refusing replacement');
  owned('volume',volume);
  docker(['run','-d','--pull=never','--name',name,'--restart=no','--network',c.network,'--memory','256m','--memory-swap','256m','--cpus','1',...labels(),'--env-file',envFile,'--mount',`type=volume,source=${volume},target=${data}`,'-e','PGDATA='+data,c.image,...args]);
  owned('container',name);
}
function oneShot(name,volume,envFile,command){
  if(existing('container',name))throw Error('operator container exists');
  owned('volume',volume);
  return docker(['run','--rm','--pull=never','--name',name,'--network',c.network,'--memory','384m','--memory-swap','384m','--cpus','1',...labels(),'--env-file',envFile,'--mount',`type=volume,source=${volume},target=${data}`,'--entrypoint','sh',c.image,'-ec',command],{timeout:60000});
}
async function setup(){
  if(fs.existsSync(control))throw Error('control exists; refusing to reinitialize data');
  const infrastructure=path.resolve(option('--infrastructure')),base=JSON.parse(fs.readFileSync(infrastructure,'utf8'));
  const home=path.dirname(control);fs.mkdirSync(home,{recursive:true,mode:0o700});fs.chmodSync(home,0o700);
  c={context:option('--context'),owner:base.owner,network:base.network,image:option('--image'),infrastructure,database:'ahp_hive_ha_'+crypto.randomBytes(5).toString('hex'),phase:'starting'};
  c.prefix=c.owner+'-state-ha-'+crypto.randomBytes(3).toString('hex');
  c.a=c.prefix+'-a';c.b=c.prefix+'-b';c.primary=c.a;c.standby=c.b;
  c.volumes={[c.a]:c.a+'-data',[c.b]:c.b+'-data'};
  c.secrets=path.join(home,'credentials.json');c.ownerEnv=path.join(home,'owner.env');c.replicaEnv=path.join(home,'replica.env');c.clientEnv=path.join(home,'client.env');
  validate();docker(['image','inspect',c.image]);
  const password=crypto.randomBytes(24).toString('hex'),replicationPassword=crypto.randomBytes(24).toString('hex');
  write(c.secrets,JSON.stringify({password,replicationPassword})+'\n');
  write(c.ownerEnv,`POSTGRES_USER=closure\nPOSTGRES_PASSWORD=${password}\nPOSTGRES_DB=${c.database}\nPGPASSWORD=${password}\n`);
  write(c.replicaEnv,`PGPASSWORD=${replicationPassword}\n`);save();
  for(const name of [c.a,c.b]){if(existing('volume',c.volumes[name]))throw Error('volume exists; refusing reuse');docker(['volume','create',...labels(),c.volumes[name]]);owned('volume',c.volumes[name]);}
  stage='primary-bootstrap';run(c.a,c.volumes[c.a],c.ownerEnv,common);await ready(c.a);
  sql(c.a,persisted+'\nSELECT pg_reload_conf();\nCHECKPOINT;');
  sql(c.a,`CREATE ROLE ahp_replica WITH REPLICATION LOGIN PASSWORD '${replicationPassword}';`);
  docker(['exec',c.a,'sh','-ec','printf "\\nhost replication ahp_replica samenet scram-sha-256\\n" >> "$PGDATA/pg_hba.conf"']);sql(c.a,'SELECT pg_reload_conf();');
  stage='basebackup';
  oneShot(operatorName('seed'),c.volumes[c.b],c.replicaEnv,`chown postgres:postgres '${data}'; chmod 700 '${data}'; exec gosu postgres pg_basebackup --dbname='host=${c.a} user=ahp_replica application_name=${app(c.b)} connect_timeout=5' -D '${data}' -X stream -R --checkpoint=fast --no-password`);
  stage='standby-bootstrap';
  run(c.b,c.volumes[c.b],c.replicaEnv,[...common,'-c','synchronous_commit=remote_apply','-c',`synchronous_standby_names=FIRST 1 (${app(c.a)})`]);await ready(c.b);
  sql(c.a,`ALTER SYSTEM SET synchronous_commit='remote_apply';\nALTER SYSTEM SET synchronous_standby_names='FIRST 1 (${app(c.b)})';\nSELECT pg_reload_conf();`);
  await until(()=>health().ready);
  const dsn=`postgres://closure:${password}@${c.a}:5432,${c.b}:5432/${c.database}?sslmode=disable&target_session_attrs=read-write&connect_timeout=3`;
  write(c.clientEnv,`AHP_PG_SYNC_DATABASE_URL=${dsn}\n`);c.phase='sync-ready';save();
  return {status:'passed',phase:c.phase,database:c.database,primary:c.primary,standby:c.standby,...health(),image:c.image,independentVolumes:true,failureDomain:'same Mac/Docker host; manual failover'};
}
async function execute(){
  if(!control||!path.isAbsolute(control))throw Error('explicit absolute --control is required');
  if(mode==='setup')return setup();
  c=JSON.parse(fs.readFileSync(control,'utf8'));validate();
  for(const name of [c.a,c.b]){owned('container',name);owned('volume',c.volumes[name]);}
  stage=mode;
  if(mode==='status')return {status:health().ready?'passed':'partial',phase:c.phase,primary:c.primary,standby:c.standby,...health()};
  if(mode==='pause-standby'){
    if(c.phase!=='sync-ready'||!health().ready)throw Error('cannot pause a non-synchronous pair');
    docker(['pause',c.standby]);c.paused=true;save();return {status:'passed',action:mode};
  }
  if(mode==='resume-standby'){
    if(owned('container',c.standby).State.Paused)docker(['unpause',c.standby]);
    await until(()=>health().ready);c.paused=false;save();return {status:'passed',action:mode};
  }
  if(mode==='crash-and-promote'){
    if(c.phase!=='sync-ready'||c.paused||!health().ready)throw Error('synchronous acknowledged cut required');
    const primary=owned('container',c.primary);
    if(primary.HostConfig.RestartPolicy.Name!=='no')throw Error('primary must not auto-restart');
    const started=Date.now();docker(['kill','--signal=KILL',c.primary]);
    const dead=owned('container',c.primary);
    if(dead.State.Running||dead.State.Restarting)throw Error('old writer is not fenced');
    if(sql(c.standby,'SELECT pg_promote(true,30);')!=='t'||sql(c.standby,'SELECT pg_is_in_recovery();')!=='f')throw Error('promotion failed');
    const old=c.primary;c.primary=c.standby;c.standby=old;c.phase='promoted-needs-replica';c.failedPrimary=primary.Id;save();
    const h=health();if(h.commit!=='remote_apply'||!h.named)throw Error('promoted writer lost synchronous requirement');
    return {status:'passed',action:mode,primaryFenced:true,failedContainer:primary.Id,promotionMs:Date.now()-started,newPrimary:c.primary,writesRequireReplica:true};
  }
  if(mode==='rejoin-old-primary'){
    if(c.phase!=='promoted-needs-replica'||owned('container',c.standby).State.Running)throw Error('old writer must be stopped');
    const started=Date.now();stage='rewind';
    if(!owned('container',c.standby).Config.Cmd.includes('wal_log_hints=on'))throw Error('old writer did not boot with WAL hints');
    oneShot(operatorName('rewind'),c.volumes[c.standby],c.ownerEnv,`rm -f '${data}/postmaster.pid'; exec gosu postgres pg_rewind --target-pgdata='${data}' --source-server='host=${c.primary} user=closure dbname=postgres application_name=${app(c.standby)} connect_timeout=5' -R`);
    const {replicationPassword}=secrets();
    // Rewind uses the control-plane owner role, but continuous WAL streaming
    // returns to the dedicated replication role. These lines stay private.
    const recovery=`\nprimary_conninfo = 'host=${c.primary} port=5432 user=ahp_replica password=${replicationPassword} application_name=${app(c.standby)} connect_timeout=5'\nsynchronous_commit = 'remote_apply'\nsynchronous_standby_names = 'FIRST 1 (${app(c.primary)})'\n`;
    const env=path.join(path.dirname(control),'recovery.env');write(env,`RECOVERY_B64=${Buffer.from(recovery).toString('base64')}\n`);
    oneShot(operatorName('recovery'),c.volumes[c.standby],env,`printf '%s' "$RECOVERY_B64" | base64 -d >> '${data}/postgresql.auto.conf'; chown postgres:postgres '${data}/postgresql.auto.conf'; chmod 600 '${data}/postgresql.auto.conf'`);
    stage='rejoin';docker(['start',c.standby]);await ready(c.standby);await until(()=>health().ready);
    if(sql(c.standby,'SELECT pg_is_in_recovery();')!=='t')throw Error('old writer rejoined as a writer');
    c.phase='sync-ready';delete c.failedPrimary;save();return {status:'passed',action:mode,rewound:true,syncRestored:true,rejoinMs:Date.now()-started,...health()};
  }
  if(mode==='stop'){
    if(owned('container',c.standby).State.Paused)docker(['unpause',c.standby]);
    for(const name of [c.primary,c.standby])if(owned('container',name).State.Running)docker(['stop','--time','5',name]);
    c.phase='stopped';save();return {status:'passed',action:mode,volumesPreserved:true};
  }
  throw Error('unsupported mode');
}
try{console.log(JSON.stringify(await execute()));}
catch(error){console.error(JSON.stringify({status:'failed',stage,error:'profile operation refused or failed; inspect private last-error.log'}));process.exitCode=1;}
