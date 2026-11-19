#!/usr/bin/env node
// Explicitly isolated local prototype. Never reads or writes default kubeconfig.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import dns from 'node:dns/promises';
import {spawnSync,spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {profile,images} from '../infra/local-k8s/profile.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const home=path.resolve(process.env.AHP_K8S_RUNTIME||path.join(root,'private/deployment-complete'));
if(!home.startsWith(root+path.sep+'private'+path.sep))throw Error('runtime must be inside this checkout private/');
fs.mkdirSync(home,{recursive:true,mode:0o700});
const namespace=process.env.AHP_K8S_NAMESPACE||'ahp-poc';
if(!['ahp-poc','ahp-poc-restore'].includes(namespace))throw Error('invalid isolated namespace');
const context='k3d-ahp-handbook-poc',config=path.join(home,'kubeconfig');
const write=(name,value)=>{fs.writeFileSync(path.join(home,name),typeof value==='string'?value:JSON.stringify(value,null,2)+'\n',{mode:0o600});};
const read=name=>JSON.parse(fs.readFileSync(path.join(home,name),'utf8'));
function run(bin,args,options={}){const r=spawnSync(bin,args,{cwd:root,encoding:'utf8',maxBuffer:16*1024*1024,...options});if(r.status!==0)throw Error(`${bin} ${args[0]} failed (${r.status}); ${options.secret?'details suppressed':r.stderr?.slice(-1500)}`);return r.stdout;}
function k(args,options={}){return run('kubectl',['--kubeconfig',config,'--context',context,'--request-timeout=20s','-n',namespace,...args],options);}
function apply(object){return k(['apply','-f','-'],{input:JSON.stringify(object),secret:true});}
const secret=(name,data)=>apply({apiVersion:'v1',kind:'Secret',metadata:{name,namespace,labels:{'app.kubernetes.io/part-of':'ahp-handbook'}},type:'Opaque',stringData:data});
const cm=(name,data)=>apply({apiVersion:'v1',kind:'ConfigMap',metadata:{name,namespace},data});
const random=()=>crypto.randomBytes(24).toString('hex');

async function prepare(){
 if(fs.existsSync(path.join(home,'credentials.json')))throw Error('credentials already exist; refuse overwrite');
 const input=JSON.parse(fs.readFileSync(process.env.AHP_K8S_MODEL_SECRET_FILE,'utf8'));
 for(const f of ['api_key','base_url','model'])if(!input.model?.[f])throw Error('explicit model secret required');
 const cr={postgres:random(),redis:random(),minio_user:'ahp-local',minio_password:random(),seed:crypto.randomBytes(32).toString('hex'),example_tool:random()};
 const privateKey=crypto.createPrivateKey({key:Buffer.concat([Buffer.from('302e020100300506032b657004220420','hex'),Buffer.from(cr.seed,'hex')]),format:'der',type:'pkcs8'});
 cr.public=crypto.createPublicKey(privateKey).export({format:'der',type:'spki'}).subarray(-32).toString('base64');
 cr.users=['alice','bob'].map(subject=>({id:'k8s-'+subject,tenant:'k8spoc',subject,display_name:'K8s '+subject,username:subject+'@ahp.local',email:subject+'@ahp.local',password:random(),roles:['admin','developer'],permissions:['space:access','state','model','execution','identity:cancel','identity:delegate','identity:exchange','space','registry','triggers','tools','tools:approve','tools:connect','artifacts','memory','memory:review','memory:label','memory:delete','memory:delete-subject','label','delete','delete-subject','compute','workspace','state:admin','state:delete-subject','state:tenant-delete']}));
 write('credentials.json',cr);write('users.json',cr.users);write('provider.json',input);
 const certs=path.join(home,'certs');fs.mkdirSync(certs,{mode:0o700});
 const openssl=args=>run('openssl',args,{cwd:certs,secret:true});
 openssl(['genpkey','-algorithm','RSA','-pkeyopt','rsa_keygen_bits:2048','-out','ca.key']);
 openssl(['req','-x509','-new','-key','ca.key','-sha256','-days','30','-subj','/CN=AHP isolated local CA','-out','ca.crt']);
 for(const type of ['server','client']){
  openssl(['genpkey','-algorithm','RSA','-pkeyopt','rsa_keygen_bits:2048','-out',type+'.key']);
  openssl(['req','-new','-key',type+'.key','-subj','/CN=AHP '+type,'-out',type+'.csr']);
  fs.writeFileSync(path.join(certs,type+'.ext'),type==='server'?'subjectAltName=DNS:localhost,IP:127.0.0.1\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n':'subjectAltName=URI:spiffe://ahp/tenant/k8spoc/deployment/djrun\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n');
  openssl(['x509','-req','-in',type+'.csr','-CA','ca.crt','-CAkey','ca.key','-CAcreateserial','-days','30','-sha256','-extfile',type+'.ext','-out',type+'.crt']);
 }
 for(const name of fs.readdirSync(certs))fs.chmodSync(path.join(certs,name),0o600);
 console.log('Prepared fresh isolated credentials and workload CA (private files only).');
}

async function configuration(){
 const cr=read('credentials.json'),p=read('provider.json');
 const cidrs=[];for(const raw of [p.model.base_url,p.embedding?.base_url].filter(Boolean)){for(const ip of await dns.resolve4(new URL(raw).hostname))cidrs.push(ip+'/32');}
 const node=JSON.parse(k(['get','nodes','-o','json'])).items[0];
 const nodeIP=node.status.addresses.find(x=>x.type==='InternalIP').address;
 const apiService=JSON.parse(run('kubectl',['--kubeconfig',config,'--context',context,'--request-timeout=10s','-n','default','get','svc','kubernetes','-o','json']));
 const backend={AHP_ROLE:'all',AHP_LISTEN:'127.0.0.1:8443',AHP_ENABLE_BROWSER:'1',AHP_ENABLE_CONTROL_EXCHANGE:'1',AHP_ENABLE_COMPACT_STATE:'1',AHP_ENABLE_DATA:'1',AHP_ENABLE_EXECUTION:'1',AHP_ENABLE_MEMORY:'1',AHP_ENABLE_COMPUTE:'1',AHP_ENABLE_MODEL:'1',AHP_ENABLE_MODEL_COST:'1',AHP_ENABLE_REGISTRY:'1',AHP_ENABLE_TOOLS:'1',AHP_ENABLE_TRIGGERS:'1',AHP_ENABLE_WORKSPACE:'1',AHP_COMPUTE_MOCK_BACKEND:'1',AHP_COMPUTE_JAIL_BASE:'/tmp/compute',AHP_DB_NAME:'ahp',AHP_S3_ENDPOINT:'http://minio:9000',AHP_S3_BUCKET:'ahp-artifacts',AHP_S3_WORKSPACE_BUCKET:'ahp-workspaces',AHP_S3_REGION:'us-east-1',AHP_EXEC_ARTIFACT_BUCKET:'ahp-artifacts',AS_ARTIFACT_BUCKET:'ahp-artifacts',AHP_TLS_CA:'/identity/ca.crt',AHP_TLS_CERT:'/identity/server.crt',AHP_TLS_KEY:'/identity/server.key',AHP_RUNTIME_CLIENT_CA:'/identity/ca.crt',AHP_RUNTIME_CLIENT_CERT:'/identity/client.crt',AHP_RUNTIME_CLIENT_KEY:'/identity/client.key',AHP_ARTIFACTS_TLS_CA:'/identity/ca.crt',AHP_ARTIFACTS_TLS_CERT:'/identity/client.crt',AHP_ARTIFACTS_TLS_KEY:'/identity/client.key',AHP_ARTIFACTS_URL:'https://127.0.0.1:8443',AHP_STATE_ADMIN_DEPLOYMENTS:'djrun',AHP_IDENTITY_EXCHANGER_DEPLOYMENTS:'djrun',AHP_IDENTITY_DELEGATOR_DEPLOYMENTS:'djrun',AHP_SINGLE_ORIGIN_UPSTREAM:'http://127.0.0.1:8081',AHP_REDIS_ADDR:'redis:6379',AHP_VAULT_ADDR:'http://127.0.0.1:8201',AHP_VAULT_DEV_TOKEN:'vault-proxy-auto-auth',AHP_LLM_VAULT_PATH:'secret/data/ahp-poc/model/qwen',AHP_MODEL_NAME:p.model.model,AHP_LLM_MODEL:p.model.model,AHP_MODEL_PROVIDER:'modelrun',AHP_MODEL_PROVIDERS_FILE:'/config/providers.json',AHP_EXEC_MAX_TOKENS:'4096',AHP_EXEC_STEP_RESERVE:'4096',AHP_TOOLS_CONFIG:JSON.stringify({connections:{example_mcp:'https://example.invalid/mcp'},tools:{example_echo:{name:'echo',connection:'example_mcp'}}}),AHP_TOOLS_VAULT_PATHS:JSON.stringify({example_mcp:'secret/data/ahp-poc/tools/example'}),AHP_TOOLS_OAUTH_VAULT_PREFIX:'secret/data/ahp-poc/oauth',AHP_TRIGGERS_VAULT_PATHS:JSON.stringify({example:'secret/data/ahp-poc/triggers/{tenant}/{deployment}/{subject}/example'}),AHP_TRIGGERS_A2A_URL:'https://127.0.0.1:8443/a2a/public',AHP_TRIGGERS_CLIENT_CA:'/identity/ca.crt',AHP_TRIGGERS_CLIENT_CERT:'/identity/client.crt',AHP_TRIGGERS_CLIENT_KEY:'/identity/client.key',OTEL_EXPORTER_OTLP_ENDPOINT:'collector:4317',OTEL_EXPORTER_OTLP_INSECURE:'true'};
 if(p.embedding){Object.assign(backend,{AHP_MEMORY_SEARCH_MODE:'hybrid',AHP_MEMORY_EMBEDDING_BASE_URL:p.embedding.base_url,AHP_MEMORY_EMBEDDING_DIM:'1024',AHP_MEMORY_EMBEDDING_KEY_FILE:'/backend-key/embedding.key',AHP_MEMORY_EMBEDDING_MODEL:'BAAI/bge-m3',AHP_MEMORY_EMBEDDING_POLICY_FILE:'/config/embedding-policy.json',AHP_MEMORY_EMBEDDING_PROVIDER:'local-embedding',AHP_MEMORY_EMBEDDING_REVISION:'fm-bge-m3-config-20261102',AHP_MEMORY_EMBEDDING_TIMEOUT:'20s'});}else backend.AHP_MEMORY_SEARCH_MODE='lexical';
 const runner={AS_LISTEN:'127.0.0.1:8081',AS_WORKER_ID:'k8s-local',AS_AUTH_ISSUER:'ahp',AS_AUTH_AUDIENCE:'ahp-browser',AS_AUTH_KEYS:'dev-1='+cr.public,AS_TASK_AUDIENCE:'ahp',AS_ENGINE_DURABLE:'1',AS_RUN_GRANT_CAPS:cr.users[0].permissions.filter(x=>!x.startsWith('state:')&&x!=='tools:approve').join(','),AS_MODEL_NAME:p.model.model,AS_ARTIFACT_BUCKET:'ahp-artifacts',AHP_S3_ENDPOINT:'http://minio:9000',AHP_S3_REGION:'us-east-1',AS_WORKLOAD_CERT:'/identity/client.crt',AS_WORKLOAD_KEY:'/identity/client.key',AS_WORKLOAD_CA:'/identity/ca.crt',OTEL_EXPORTER_OTLP_ENDPOINT:'collector:4317',OTEL_EXPORTER_OTLP_INSECURE:'true'};
 for(const name of ['STATE','MODEL','IDENTITY','ARTIFACTS','COMPUTE','MEMORY','MODEL_COST','REGISTRY','EVALS','TRIGGERS','WORKSPACE','TOOLS'])runner['AS_'+name+'_URL']='https://127.0.0.1:8443';
 runner.AS_IDENTITY_EXCHANGE_URL='https://127.0.0.1:8443/v1/identity/control/exchange';
 const network=JSON.parse(run('docker',['inspect','k3d-ahp-handbook-poc-serverlb']))[0].NetworkSettings.Networks; const hostIngress=Object.values(network).flatMap(n=>[n.IPAddress+'/32',n.Gateway+'/32']);
 return {backend,runner,inferenceCIDRs:[...new Set(cidrs)],kubeAPICIDRs:[nodeIP+'/32',apiService.spec.clusterIP+'/32'],collectorIngressCIDRs:[nodeIP+'/32',...hostIngress]};
}

function renderSecrets(){
 const c=read('credentials.json'),p=read('provider.json');
 secret('datastore',{postgres:c.postgres,redis:c.redis,minio_user:c.minio_user,minio_password:c.minio_password});
 const dsn='postgres://ahp:'+c.postgres+'@postgres:5432/ahp?sslmode=disable';
 secret('app-secrets',{AHP_DATABASE_URL:dsn,AS_DATA_DATABASE_URL:dsn,AHP_DEV_JWT_SEED:c.seed,AHP_REDIS_PASSWORD:c.redis,AHP_MINIO_ROOT_USER:c.minio_user,AHP_MINIO_ROOT_PASSWORD:c.minio_password});
 secret('runner-secrets',{AS_DATA_DATABASE_URL:dsn,AHP_MINIO_ROOT_USER:c.minio_user,AHP_MINIO_ROOT_PASSWORD:c.minio_password});
 secret('backend-key',{'embedding.key':p.embedding?.api_key||''});
 secret('workload-identity',Object.fromEntries(['ca.crt','server.crt','server.key','client.crt','client.key'].map(n=>[n,fs.readFileSync(path.join(home,'certs',n),'utf8')])));
 cm('app-config',{'providers.json':JSON.stringify({version:1,providers:[{name:'modelrun',class:'standard',audience:{kind:'external',allow_external:false},secret_ref:'ahp-poc/model/qwen',models:[p.model.model],quota:{rpm:30,tpm:120000,window_seconds:60}}],routes:[{name:'k8spoc-reference',when:{tenants:['k8spoc']},then:{provider:'modelrun'}}]}),'embedding-policy.json':JSON.stringify({version:1,providers:[{name:'local-embedding',class:'standard',audience:{kind:'private'},subject_from_scope:true,secret_ref:'AHP_MEMORY_EMBEDDING_KEY_FILE',models:['BAAI/bge-m3']}],routes:[{name:'k8spoc-embedding',when:{tenants:['k8spoc']},then:{provider:'local-embedding'}}]})});
}

async function bootstrapVault(){
 k(['wait','--for=jsonpath={.status.phase}=Running','pod/vault-0','--timeout=180s']);
 const proc=spawn('kubectl',['--kubeconfig',config,'--context',context,'-n',namespace,'port-forward','--address=127.0.0.1','svc/vault','30201:8200'],{stdio:'ignore'});
 const request=async(method,p,body,token)=>{const r=await fetch('http://127.0.0.1:30201/v1/'+p,{method,headers:{'content-type':'application/json',...(token?{'x-vault-token':token}:{})},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(10000)});const content=await r.text();if(r.status>=400)throw Error('Vault '+p+' returned '+r.status);return content?JSON.parse(content):{};};
 try{
  for(let i=0;;i++){try{await request('GET','sys/init');break;}catch(e){if(i===40)throw e;await new Promise(r=>setTimeout(r,250));}}
  const keyFile='vault-'+namespace+'.json';let init;
  const status=await request('GET','sys/init'); const seedSecrets=!status.initialized;
  if(!status.initialized){init=await request('PUT','sys/init',{secret_shares:1,secret_threshold:1});write(keyFile,init);}else init=read(keyFile);
  await request('PUT','sys/unseal',{key:init.keys_base64[0]});
  const token=init.root_token;
  const mounts=await request('GET','sys/mounts',undefined,token);if(!mounts.data?.['secret/']&&!mounts['secret/'])await request('POST','sys/mounts/secret',{type:'kv',options:{version:'2'}},token);
  const auth=await request('GET','sys/auth',undefined,token);if(!auth.data?.['kubernetes/']&&!auth['kubernetes/'])await request('POST','sys/auth/kubernetes',{type:'kubernetes'},token);
  const ca=k(['exec','vault-0','--','cat','/var/run/secrets/kubernetes.io/serviceaccount/ca.crt']);
  await request('POST','auth/kubernetes/config',{kubernetes_host:'https://kubernetes.default.svc:443',kubernetes_ca_cert:ca,disable_local_ca_jwt:false},token);
  const policy='path "secret/data/ahp-poc/model/*" { capabilities=["read"] }\npath "secret/data/ahp-poc/tools/*" { capabilities=["read"] }\npath "secret/data/ahp-poc/triggers/*" { capabilities=["read"] }\npath "secret/data/ahp-poc/oauth/*" { capabilities=["read","create","update"] }\npath "secret/metadata/ahp-poc/oauth/*" { capabilities=["delete"] }';
  await request('PUT','sys/policies/acl/ahp-runtime',{policy},token);
  await request('POST','auth/kubernetes/role/ahp-runtime',{bound_service_account_names:['ahp-cell'],bound_service_account_namespaces:[namespace],audience:'vault',token_policies:['ahp-runtime'],token_ttl:'120s',token_max_ttl:'300s'},token);
  const input=read('provider.json'),c=read('credentials.json');
  if(seedSecrets)await request('POST','secret/data/ahp-poc/model/qwen',{data:input.model},token);
  if(seedSecrets)await request('POST','secret/data/ahp-poc/tools/example',{data:{token:c.example_tool}},token);
  if(seedSecrets)for(const user of c.users)await request('POST','secret/data/ahp-poc/triggers/k8spoc/djrun/'+user.subject+'/example',{data:{secret:c.example_tool}},token);
  console.log('Vault initialized/unsealed; Kubernetes auth and scoped runtime policy configured.');
 }finally{proc.kill('SIGTERM');}
}

async function deploy(){
 const image=process.env.AHP_K8S_IMAGE||read('build.json').image;
 const c=await configuration();write('configuration-'+namespace+'.json',c);
 const manifest=profile(namespace,image,c);write('manifest-'+namespace+'.json',manifest);
 apply(manifest);renderSecrets();
 for(const name of ['postgres','minio','redis'])k(['rollout','status','statefulset/'+name,'--timeout=180s']);
 k(['wait','--for=jsonpath={.status.phase}=Running','pod/vault-0','--timeout=180s']);
 await bootstrapVault();k(['rollout','status','statefulset/vault','--timeout=90s']);
 k(['scale','deployment/cell','--replicas=1']);k(['rollout','status','deployment/cell','--timeout=240s']);
 const users=read('credentials.json').users;
 // Process environment, not shell interpolation or a Pod command argument.
 const js="const{spawnSync}=require('child_process');let b='';process.stdin.on('data',x=>b+=x);process.stdin.on('end',()=>{const r=spawnSync('/usr/local/bin/ahp',['--role','provision-browser-users'],{env:{...process.env,AHP_PROVISION_USERS_JSON:b},stdio:['ignore','pipe','pipe']});if(r.status)process.stderr.write('provision failed');process.exit(r.status||0)});";
 k(['exec','-i','deployment/cell','-c','backend','--','node','-e',js],{input:JSON.stringify(users),secret:true});
 console.log('Ready: http://127.0.0.1:30222/ (private credentials file; no external exposure).');
}

async function build(){
 const source=path.resolve(process.env.AHP_K8S_SOURCE_ROOT||(fs.existsSync(path.join(root,'cmd/ahp'))?root:path.join(root,'../platform-runtime')));
 const frontend=path.resolve(process.env.AHP_K8S_FRONTEND_ROOT||(fs.existsSync(path.join(root,'web/aifactory'))?path.join(root,'web/aifactory'):path.join(root,'../frontend/web/aifactory')));
 if(!fs.existsSync(path.join(source,'cmd/ahp'))||!fs.existsSync(path.join(frontend,'index.html')))throw Error('source or frontend layout missing; set AHP_K8S_SOURCE_ROOT and AHP_K8S_FRONTEND_ROOT');
 const workspace=path.join(root,'../go.work');const buildEnv={...process.env,CGO_ENABLED:'0',GOOS:'linux',GOARCH:process.env.AHP_K8S_GOARCH||(process.arch==='x64'?'amd64':'arm64')};
 const roots=new Set([root,source,path.resolve(frontend,'../..')]);
 if(source!==root&&fs.existsSync(workspace)){
  buildEnv.GOWORK=workspace;
  const work=JSON.parse(run('go',['work','edit','-json',workspace]));
  for(const item of work.Use||[])roots.add(path.resolve(path.dirname(workspace),item.DiskPath));
 }
 const sources=[];
 for(const directory of roots){
  const top=run('git',['rev-parse','--show-toplevel'],{cwd:directory}).trim();if(sources.some(s=>s.directory===top))continue;
  if(run('git',['status','--porcelain'],{cwd:top}).trim())throw Error('refusing runtime build from dirty checkout: '+path.basename(top));
  sources.push({directory:top,revision:run('git',['rev-parse','HEAD'],{cwd:top}).trim()});
 }
 const provenance=sources.map(s=>({repository:path.basename(s.directory),revision:s.revision})).sort((a,b)=>a.repository.localeCompare(b.repository));
 const revision=sources.length===1?sources[0].revision:'workspace-'+crypto.createHash('sha256').update(JSON.stringify(provenance)).digest('hex');
 const out=path.join(home,'build');fs.mkdirSync(out,{recursive:true});
 const commitTime=run('git',['show','-s','--format=%cI','HEAD']).trim();
 const buildInfoPackages=run('go',['list','-tags','ahp_exec,ahp_data','-deps','-f','{{.ImportPath}}','./cmd/ahp'],{cwd:source,env:buildEnv,secret:true}).trim().split('\n').filter(p=>p.startsWith('github.com/ahp-platform/')&&p.endsWith('/buildinfo'));
 if(buildInfoPackages.length!==1)throw Error('cannot resolve unique runtime buildinfo package');
 const buildInfoPackage=buildInfoPackages[0];
 const flags=`-s -w -X ${buildInfoPackage}.Revision=${revision} -X ${buildInfoPackage}.CommitTime=${commitTime} -X ${buildInfoPackage}.Modified=false`;
 for(const name of ['ahp','as-runner','artifact-parser'])run('go',['build','-p','2','-buildvcs=false','-tags','ahp_exec,ahp_data','-ldflags',flags,'-o',path.join(out,name),'./cmd/'+name],{cwd:source,env:buildEnv,secret:true});
 fs.rmSync(path.join(out,'frontend'),{recursive:true,force:true});
 fs.cpSync(frontend,path.join(out,'frontend'),{recursive:true});
 fs.copyFileSync(path.join(root,'infra/local-k8s/human-origin.mjs'),path.join(out,'human-origin.mjs'));
 const image='ahp-handbook-cell:'+revision.replace('workspace-','').slice(0,12);
 run('docker',['build','-f',path.join(root,'infra/local-k8s/runtime.Dockerfile'),'-t',image,out],{secret:true});
 const manifest={revision,sources:provenance,buildInfoPackage,image,imageID:run('docker',['image','inspect',image,'--format','{{.Id}}']).trim(),binaries:Object.fromEntries(['ahp','as-runner'].map(n=>[n,crypto.createHash('sha256').update(fs.readFileSync(path.join(out,n))).digest('hex')]))};
 const imported=run(path.join(home,'bin/k3d'),['image','import','-c','ahp-handbook-poc',image],{secret:true});write('image-import.log',imported);
 const available=JSON.parse(run('docker',['exec','k3d-ahp-handbook-poc-server-0','crictl','images','-o','json']));
 const importedImage=available.images.find(i=>i.repoTags?.includes('docker.io/library/'+image));
 // Docker containerd reports an OCI index digest as Id; CRI reports the image
 // configuration digest. Match the tag in CRI and the index in containerd.
 const nodeImages=run('docker',['exec','k3d-ahp-handbook-poc-server-0','ctr','-n','k8s.io','images','ls']);
 const sameDescriptor=nodeImages.split('\n').some(line=>{const fields=line.trim().split(/\s+/);return fields[0]==='docker.io/library/'+image&&fields[2]===manifest.imageID});
 if(!importedImage||!sameDescriptor)throw Error('runtime image descriptor missing from node after import');
 manifest.nodeConfigDigest=importedImage.id;
 write('build.json',manifest);console.log(JSON.stringify(manifest));
}

async function restoreTarget(){
 if(namespace!=='ahp-poc-restore')throw Error('restore target must be ahp-poc-restore');
 const c=await configuration(); const image=read('build.json').image; const manifest=profile(namespace,image,c);
 for(const item of manifest.items)if(item.kind==='StatefulSet'&&item.metadata.name!=='postgres')item.spec.replicas=0;
 apply(manifest);renderSecrets();k(['rollout','status','statefulset/postgres','--timeout=180s']);
 console.log('Empty isolated restore target prepared; cold stores have never started.');
}

const command=process.argv[2];
if(command==='prepare')await prepare();
else if(command==='build')await build();
else if(command==='deploy')await deploy();
else if(command==='unseal')await bootstrapVault();
else if(command==='restore-target')await restoreTarget();
else if(command==='render')console.log(JSON.stringify(profile(namespace,'ahp:test',{backend:{},runner:{},inferenceCIDRs:['203.0.113.1/32'],kubeAPICIDRs:['10.43.0.1/32']}),null,2));
else throw Error('usage: local-k8s.mjs prepare|build|deploy|unseal|render|restore-target');
