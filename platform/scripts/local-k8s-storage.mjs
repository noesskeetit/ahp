#!/usr/bin/env node
// Cold, quiesced snapshot of this isolated profile only. Exact MinIO versions
// are retained by copying the stopped store, not by re-PUT with new VersionIDs.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import{spawnSync}from'node:child_process';import{fileURLToPath}from'node:url';
import{images}from'../infra/local-k8s/profile.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const runtime=path.resolve(process.env.AHP_K8S_RUNTIME||path.join(root,'private/deployment-complete'));
const kubeconfig=path.join(runtime,'kubeconfig');
const ns=process.env.AHP_K8S_NAMESPACE||'ahp-poc';if(!['ahp-poc','ahp-poc-restore'].includes(ns))throw Error('isolated namespace required');
const args=['--kubeconfig',kubeconfig,'--context','k3d-ahp-handbook-poc','--request-timeout=30s','-n',ns];
function k(a,o={}){const r=spawnSync('kubectl',[...args,...a],{encoding:'utf8',maxBuffer:64<<20,...o});if(r.status!==0)throw Error('kubectl '+a[0]+' failed; '+r.stderr?.toString().slice(-500));return r.stdout;}
function helper(){
 const object={apiVersion:'v1',kind:'Pod',metadata:{name:'ops',namespace:ns,labels:{'ahp.local/component':'ops','app.kubernetes.io/part-of':'ahp-handbook'}},spec:{serviceAccountName:'ahp-data',automountServiceAccountToken:false,securityContext:{runAsNonRoot:true,runAsUser:1000,runAsGroup:1000,fsGroup:1000,seccompProfile:{type:'RuntimeDefault'}},containers:[{name:'ops',image:images.node,imagePullPolicy:'IfNotPresent',command:['node','-e','setInterval(()=>{},60000)'],securityContext:{readOnlyRootFilesystem:true,allowPrivilegeEscalation:false,capabilities:{drop:['ALL']}},resources:{requests:{cpu:'10m',memory:'32Mi'},limits:{cpu:'250m',memory:'96Mi'}},volumeMounts:['minio','vault','redis','collector'].map(n=>({name:n,mountPath:'/snapshot/'+n,readOnly:false}))}],volumes:['minio','vault','redis','collector'].map(n=>({name:n,persistentVolumeClaim:{claimName:n}}))}};
 k(['apply','-f','-'],{input:JSON.stringify(object)});k(['wait','--for=condition=Ready','pod/ops','--timeout=90s']);
}
function hash(file){const h=crypto.createHash('sha256'),fd=fs.openSync(file,'r'),chunk=Buffer.alloc(1<<20);try{for(;;){const n=fs.readSync(fd,chunk,0,chunk.length,null);if(!n)break;h.update(chunk.subarray(0,n));}}finally{fs.closeSync(fd)}return h.digest('hex');}
function transfer(a,file,read=false){const fd=fs.openSync(file,read?'r':'wx',0o600);try{const r=spawnSync('kubectl',[...args,...a],{encoding:'utf8',stdio:read?[fd,'ignore','pipe']:['ignore',fd,'pipe']});if(r.status!==0)throw Error('snapshot transfer failed: '+r.stderr?.slice(-500));}finally{fs.closeSync(fd)}}
const target=path.resolve(process.env.AHP_K8S_BACKUP_DIR||path.join(runtime,'backup'));
if(!target.startsWith(runtime+path.sep))throw Error('backup must remain private in this lane');
function validateSnapshot(){
 const m=JSON.parse(fs.readFileSync(path.join(target,'manifest.json')));
 for(const name of ['postgres','minio','redis','vault'])if(m.datastoreImages?.[name]!==images[name])throw Error('backup datastore version mismatch: '+name);
 for(const[n,x]of Object.entries(m.files))if(hash(path.join(target,n))!==x.sha256)throw Error('backup checksum mismatch: '+n);
 return m;
}
function vaultFingerprint(){
 const js="(async()=>{const r=await fetch('http://127.0.0.1:8201/v1/secret/data/ahp-poc/model/qwen');if(r.status!==200)process.exit(1);const d=(await r.json()).data;const hash=require('crypto').createHash('sha256').update(JSON.stringify(Object.entries(d.data).sort())).digest('hex');const f=await fetch('http://127.0.0.1:8201/v1/secret/data/other-tenant/private');console.log(JSON.stringify({contentSHA256:hash,metadata:d.metadata,foreignStatus:f.status}))})().catch(()=>process.exit(1))";
 return JSON.parse(k(['exec','deployment/cell','-c','backend','--','node','-e',js]));
}
async function backup(){
 if(fs.existsSync(path.join(target,'manifest.json')))throw Error('refusing overwrite of completed backup');fs.mkdirSync(target,{recursive:true,mode:0o700});
 fs.writeFileSync(path.join(target,'vault-fingerprint.json'),JSON.stringify(vaultFingerprint()),{mode:0o600});
 helper();k(['scale','deployment/cell','--replicas=0']);k(['wait','--for=delete','pod','-l','ahp.local/component=cell','--timeout=90s']);
 // PostgreSQL has no writers now; a transaction-consistent logical dump covers all owners.
 transfer(['exec','postgres-0','--','pg_dump','-U','ahp','-d','ahp','-Fc'],path.join(target,'postgres.dump'));
 k(['scale','statefulset/minio','statefulset/vault','statefulset/redis','--replicas=0']);
 for(const n of ['minio','vault','redis'])k(['wait','--for=delete','pod/'+n+'-0','--timeout=90s']);
 for(const n of ['minio','vault','redis'])transfer(['exec','ops','--','tar','-C','/snapshot/'+n,'-cf','-','.'],path.join(target,n+'.tar'));
 const names=['datastore','app-secrets','runner-secrets','backend-key','workload-identity'];
 const secrets=names.map(n=>{const x=JSON.parse(k(['get','secret',n,'-o','json']));return{apiVersion:'v1',kind:'Secret',metadata:{name:n},type:x.type,data:x.data}});
 fs.writeFileSync(path.join(target,'secrets.json'),JSON.stringify(secrets),{mode:0o600});
 for(const f of ['credentials.json','provider.json','vault-'+ns+'.json','build.json'])fs.copyFileSync(path.join(runtime,f),path.join(target,f));
 const files=Object.fromEntries(fs.readdirSync(target).filter(n=>fs.statSync(path.join(target,n)).isFile()).map(n=>[n,{bytes:fs.statSync(path.join(target,n)).size,sha256:hash(path.join(target,n))}]));
 const manifest={sourceNamespace:ns,createdAt:new Date().toISOString(),consistency:'all app writers drained; MinIO, Vault, Redis stopped before file snapshot; PostgreSQL pg_dump after drain',sourceBuild:JSON.parse(fs.readFileSync(path.join(runtime,'build.json'))),datastoreImages:images,files};
 fs.writeFileSync(path.join(target,'manifest.json'),JSON.stringify(manifest,null,2)+'\n',{mode:0o600});
 console.log(JSON.stringify({snapshot:'complete',directory:target,files:Object.keys(files),restart:'run local-k8s.mjs deploy to unseal and resume source'}));
}
async function restore(){
 if(ns!=='ahp-poc-restore')throw Error('restore requires explicit separate ahp-poc-restore namespace');
 const m=validateSnapshot();
 // Caller first applies profile to create new PVCs. Existing data may not be overwritten.
 helper();k(['scale','deployment/cell','--replicas=0']);k(['wait','--for=delete','pod','-l','ahp.local/component=cell','--timeout=90s']);
 k(['scale','statefulset/minio','statefulset/vault','statefulset/redis','--replicas=0']);for(const n of ['minio','vault','redis'])k(['wait','--for=delete','pod/'+n+'-0','--timeout=90s']);
 for(const n of ['minio','vault','redis']){
  const listing=k(['exec','ops','--','find','/snapshot/'+n,'-mindepth','1','-maxdepth','1']);if(listing.trim())throw Error(n+' restore target is not empty; no overwrite performed');
  transfer(['exec','-i','ops','--','tar','--strip-components=1','--no-same-owner','--no-same-permissions','--no-overwrite-dir','-C','/snapshot/'+n,'-xf','-'],path.join(target,n+'.tar'),true);
 }
 const populated=k(['exec','postgres-0','--','psql','-U','ahp','-d','ahp','-Atc',"SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'ahp_%'"]);
 if(Number(populated.trim())!==0)throw Error('restore database already contains AHP schemas; no overwrite performed');
 transfer(['exec','-i','postgres-0','--','pg_restore','-U','ahp','-d','ahp','--no-owner','--exit-on-error'],path.join(target,'postgres.dump'),true);
 const secrets=JSON.parse(fs.readFileSync(path.join(target,'secrets.json')));for(const s of secrets){s.metadata.namespace=ns;k(['apply','-f','-'],{input:JSON.stringify(s)});}
 fs.copyFileSync(path.join(target,'vault-'+m.sourceNamespace+'.json'),path.join(runtime,'vault-'+ns+'.json'));
 k(['scale','statefulset/minio','statefulset/vault','statefulset/redis','--replicas=1']);
 console.log(JSON.stringify({restored:true,namespace:ns,next:'local-k8s.mjs unseal, then scale cell to one; run product and authorization checks'}));
}
function importRecoveryMaterial(){
 const m=validateSnapshot();
 for(const name of ['credentials.json','provider.json']){
  const destination=path.join(runtime,name);if(fs.existsSync(destination))throw Error('refusing overwrite of local recovery material: '+name);
 }
 for(const name of ['credentials.json','provider.json']){fs.copyFileSync(path.join(target,name),path.join(runtime,name));fs.chmodSync(path.join(runtime,name),0o600);}
 fs.writeFileSync(path.join(runtime,'users.json'),JSON.stringify(JSON.parse(fs.readFileSync(path.join(runtime,'credentials.json'))).users),{mode:0o600});
 const identity=JSON.parse(fs.readFileSync(path.join(target,'secrets.json'))).find(s=>s.metadata.name==='workload-identity');
 const certs=path.join(runtime,'certs');fs.mkdirSync(certs,{recursive:true,mode:0o700});
 for(const name of ['ca.crt','server.crt','server.key','client.crt','client.key'])fs.writeFileSync(path.join(certs,name),Buffer.from(identity.data[name],'base64'),{flag:'wx',mode:0o600});
 console.log('Recovery identity and credentials imported privately; build the exact matching source before restore-target.');
}
if(process.argv[2]==='backup')await backup();
else if(process.argv[2]==='validate'){validateSnapshot();console.log('Backup checksums and datastore versions match; no cluster mutation.');}
else if(process.argv[2]==='import')importRecoveryMaterial();
else if(process.argv[2]==='restore')await restore();
else if(process.argv[2]==='helper')helper();
else if(process.argv[2]==='verify-vault'){
 const before=JSON.parse(fs.readFileSync(path.join(target,'vault-fingerprint.json'))),after=vaultFingerprint();
 if(JSON.stringify(before)!==JSON.stringify(after)||after.foreignStatus!==403)throw Error('Vault secret bytes, metadata or authorization differ after restore');
 const report={namespace:ns,status:'passed',sameSecretBytes:true,sameVersionMetadata:true,foreignStatus:after.foreignStatus,at:new Date().toISOString()};
 fs.writeFileSync(path.join(runtime,'restore-vault-report.json'),JSON.stringify(report,null,2)+'\n',{mode:0o600});console.log(JSON.stringify(report));
}else throw Error('usage: local-k8s-storage.mjs helper|backup|restore|verify-vault|import|validate');
