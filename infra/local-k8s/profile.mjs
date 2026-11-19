import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));
export const images={
 node:'node:22-bookworm-slim@sha256:c3de60bf2f9dd0ac6370e6117950ff62d6e339527e7472301c9c78a017978392',
 postgres:'pgvector/pgvector:0.8.1-pg17@sha256:3e8b3adfd27b5707128f60956f62a793c3c9326ea8cfaf0eab7adccb5d700b21',
 minio:'ahp-handbook-minio:9e49d5e7',
 vault:'hashicorp/vault:1.21@sha256:4e33b126a59c0c333b76fb4e894722462659a6bec7c48c9ee8cea56fccfd2569',
 redis:'redis:7-alpine@sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499',
 collector:'otel/opentelemetry-collector-contrib:0.164.0@sha256:340c9ca6a36617084c6661bdc4f0fe92b7cda5a805d61673ec8db375fddc522b',
};
const security={allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:['ALL']}};
const podSecurity=(uid=1000)=>({runAsNonRoot:true,runAsUser:uid,runAsGroup:uid,fsGroup:uid,seccompProfile:{type:'RuntimeDefault'}});
const resources=(memory='128Mi',cpu='250m')=>({requests:{memory,cpu:'50m'},limits:{memory,cpu}});
const labels=name=>({'app.kubernetes.io/name':name,'app.kubernetes.io/part-of':'ahp-handbook','ahp.local/component':name});
const envSecret=(name,key,secret='datastore')=>({name,valueFrom:{secretKeyRef:{name:secret,key}}});
const env=(name,value)=>({name,value});
const mount=(name,mountPath,readOnly=true)=>({name,mountPath,readOnly});
const probe=(port,path)=>({httpGet:{port,path},periodSeconds:5,timeoutSeconds:2,failureThreshold:6});

export function profile(namespace, image, configuration){
 if(!/^ahp-poc(?:-restore)?$/.test(namespace))throw Error('only isolated prototype namespaces supported');
 const objects=[];
 const obj=(kind,name,spec,apiVersion='v1')=>{const r={apiVersion,kind,metadata:{name,namespace,labels:labels(name)},...spec};objects.push(r);return r;};
 objects.push({apiVersion:'v1',kind:'Namespace',metadata:{name:namespace,labels:{'app.kubernetes.io/part-of':'ahp-handbook','pod-security.kubernetes.io/enforce':'restricted','pod-security.kubernetes.io/enforce-version':'v1.34'}}});
 for(const name of ['cell','data','vault','collector'])obj('ServiceAccount','ahp-'+name,{automountServiceAccountToken:false});
 objects.push({apiVersion:'rbac.authorization.k8s.io/v1',kind:'ClusterRoleBinding',metadata:{name:namespace+'-vault-reviewer',labels:labels('vault')},roleRef:{apiGroup:'rbac.authorization.k8s.io',kind:'ClusterRole',name:'system:auth-delegator'},subjects:[{kind:'ServiceAccount',name:'ahp-vault',namespace}]});
 const cm=(name,data)=>obj('ConfigMap',name,{data});
 const service=(name,port,target=port,type='ClusterIP',nodePort)=>obj('Service',name,{spec:{type,selector:{'ahp.local/component':name},ports:[{name:'api',port,targetPort:target,...(nodePort?{nodePort}:{})}]}});
 const pvc=name=>obj('PersistentVolumeClaim',name,{spec:{accessModes:['ReadWriteOnce'],storageClassName:'local-path',resources:{requests:{storage:'1Gi'}}}});
 const dataPod=(name,container,uid=1000)=>{
  pvc(name);service(name,{postgres:5432,redis:6379,minio:9000,vault:8200}[name]);
  obj('StatefulSet',name,{spec:{replicas:1,serviceName:name,selector:{matchLabels:{'ahp.local/component':name}},template:{metadata:{labels:labels(name)},spec:{serviceAccountName:name==='vault'?'ahp-vault':'ahp-data',automountServiceAccountToken:name==='vault',securityContext:podSecurity(uid),terminationGracePeriodSeconds:30,containers:[{name,image:images[name],imagePullPolicy:'IfNotPresent',securityContext:security,...container}],volumes:[{name:'data',persistentVolumeClaim:{claimName:name}},{name:'tmp',emptyDir:{sizeLimit:'64Mi'}},...(container._volumes||[])]}}}},'apps/v1');
  delete objects.at(-1).spec.template.spec.containers[0]._volumes;
  obj('PodDisruptionBudget',name,{spec:{minAvailable:1,selector:{matchLabels:{'ahp.local/component':name}}}},'policy/v1');
 };
 cm('postgres-init',{'init.sql':'CREATE EXTENSION IF NOT EXISTS vector;\n'});
 dataPod('postgres',{env:[env('POSTGRES_DB','ahp'),env('POSTGRES_USER','ahp'),envSecret('POSTGRES_PASSWORD','postgres'),env('PGDATA','/var/lib/postgresql/data/pgdata')],resources:resources('256Mi','500m'),volumeMounts:[mount('data','/var/lib/postgresql/data',false),mount('tmp','/var/run/postgresql',false),mount('init','/docker-entrypoint-initdb.d')],_volumes:[{name:'init',configMap:{name:'postgres-init'}}],readinessProbe:{exec:{command:['pg_isready','-U','ahp','-d','ahp']},periodSeconds:5},livenessProbe:{exec:{command:['pg_isready','-U','ahp','-d','ahp']},periodSeconds:15},startupProbe:{exec:{command:['pg_isready','-U','ahp','-d','ahp']},periodSeconds:3,failureThreshold:40}},999);
 dataPod('minio',{args:['server','/data','--console-address',':9001'],env:[envSecret('MINIO_ROOT_USER','minio_user'),envSecret('MINIO_ROOT_PASSWORD','minio_password')],resources:resources('256Mi','500m'),volumeMounts:[mount('data','/data',false),mount('tmp','/tmp',false)],readinessProbe:probe(9000,'/minio/health/ready'),livenessProbe:probe(9000,'/minio/health/live'),startupProbe:{...probe(9000,'/minio/health/live'),failureThreshold:40}});
 dataPod('redis',{command:['/bin/sh','-ec'],args:['exec redis-server --dir /data --appendonly yes --maxmemory 64mb --maxmemory-policy noeviction --requirepass "$REDIS_PASSWORD"'],env:[envSecret('REDIS_PASSWORD','redis')],resources:resources('96Mi'),volumeMounts:[mount('data','/data',false)],readinessProbe:{exec:{command:['sh','-ec','REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli ping | grep -q PONG']},periodSeconds:5},livenessProbe:{exec:{command:['sh','-ec','REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli ping | grep -q PONG']},periodSeconds:15}});
 cm('vault-config',{'vault.hcl':'disable_mlock = true\nui = false\napi_addr = "http://vault:8200"\nlistener "tcp" { address = "0.0.0.0:8200" tls_disable = 1 }\nstorage "file" { path = "/vault/data" }\n'});
 dataPod('vault',{command:['vault'],args:['server','-config=/config/vault.hcl'],env:[env('VAULT_ADDR','http://127.0.0.1:8200')],resources:resources('192Mi','500m'),volumeMounts:[mount('data','/vault/data',false),mount('config','/config'),mount('tmp','/tmp',false)],_volumes:[{name:'config',configMap:{name:'vault-config'}}],readinessProbe:probe(8200,'/v1/sys/health?standbyok=true'),livenessProbe:probe(8200,'/v1/sys/health?uninitcode=200&sealedcode=200&standbyok=true'),startupProbe:{...probe(8200,'/v1/sys/health?uninitcode=200&sealedcode=200'),failureThreshold:40}});
 cm('collector-config',{'config.yaml':fs.readFileSync(path.join(dir,'collector.yaml'),'utf8')});
 pvc('collector');
 service('collector',4317,4317,'NodePort',namespace==='ahp-poc'?30317:undefined);
 obj('Deployment','collector',{spec:{replicas:1,selector:{matchLabels:{'ahp.local/component':'collector'}},template:{metadata:{labels:labels('collector')},spec:{serviceAccountName:'ahp-collector',automountServiceAccountToken:false,securityContext:podSecurity(),containers:[{name:'collector',image:images.collector,imagePullPolicy:'IfNotPresent',args:['--config=/config/config.yaml'],securityContext:security,resources:resources('160Mi'),volumeMounts:[mount('config','/config'),mount('data','/evidence',false)],readinessProbe:probe(13133,'/'),livenessProbe:probe(13133,'/')}],volumes:[{name:'config',configMap:{name:'collector-config'}},{name:'data',persistentVolumeClaim:{claimName:'collector'}}]}}}},'apps/v1');
 cm('vault-proxy',{'proxy.hcl':'pid_file = "/tmp/vault-proxy.pid"\nvault { address = "http://vault:8200" }\nauto_auth { method "kubernetes" { mount_path = "auth/kubernetes" config = { role = "ahp-runtime", token_path = "/var/run/vault-auth/token" } } }\nlistener "tcp" { address = "127.0.0.1:8201" tls_disable = true }\napi_proxy { use_auto_auth_token = "force" }\n'});
 const identity={name:'identity',secret:{secretName:'workload-identity',defaultMode:288}};
 const clientIdentity={name:'client-identity',secret:{secretName:'workload-identity',defaultMode:288,items:['ca.crt','client.crt','client.key'].map(key=>({key,path:key}))}};
 const tlsMount=[mount('identity','/identity')];
 const appEnv=Object.entries(configuration.backend).map(([name,value])=>env(name,value));
 const runEnv=Object.entries(configuration.runner).map(([name,value])=>env(name,value));
 const envFrom=[{secretRef:{name:'app-secrets'}}];
 const live={exec:{command:['node','-e',"const h=require('https'),f=require('fs');h.get('https://127.0.0.1:8443/readyz',{cert:f.readFileSync('/identity/client.crt'),key:f.readFileSync('/identity/client.key'),ca:f.readFileSync('/identity/ca.crt')},r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"]},timeoutSeconds:3,periodSeconds:8,failureThreshold:5};
 obj('Deployment','cell',{spec:{replicas:0,revisionHistoryLimit:3,strategy:{type:'Recreate'},selector:{matchLabels:{'ahp.local/component':'cell'}},template:{metadata:{labels:labels('cell')},spec:{serviceAccountName:'ahp-cell',automountServiceAccountToken:false,securityContext:podSecurity(),terminationGracePeriodSeconds:45,initContainers:[{name:'prepare-key',image,imagePullPolicy:'IfNotPresent',command:['/bin/sh','-ec','umask 077; cp /key-source/embedding.key /backend-key/embedding.key; chmod 600 /backend-key/embedding.key'],securityContext:security,resources:resources('32Mi','100m'),volumeMounts:[mount('key-source','/key-source'),mount('backend-key','/backend-key',false)]}],containers:[
  {name:'backend',image,imagePullPolicy:'IfNotPresent',env:appEnv,envFrom,securityContext:security,resources:resources('384Mi','750m'),volumeMounts:[...tlsMount,mount('config','/config'),mount('backend-key','/backend-key'),mount('tmp','/tmp',false)],readinessProbe:live,livenessProbe:{...live,exec:{command:live.exec.command.map(s=>s.replace('/readyz','/healthz'))}},startupProbe:{...live,periodSeconds:3,failureThreshold:50}},
  {name:'runner',image,imagePullPolicy:'IfNotPresent',command:['/usr/local/bin/as-runner'],env:runEnv,envFrom:[{secretRef:{name:'runner-secrets'}}],securityContext:security,resources:resources('192Mi','500m'),volumeMounts:[...tlsMount,mount('tmp','/tmp',false)],readinessProbe:{exec:{command:['node','-e',"require('http').get('http://127.0.0.1:8081/readyz',r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"]},periodSeconds:8,timeoutSeconds:3},startupProbe:{exec:{command:['node','-e',"require('http').get('http://127.0.0.1:8081/readyz',r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"]},periodSeconds:3,timeoutSeconds:3,failureThreshold:50}},
  {name:'human',image,imagePullPolicy:'IfNotPresent',command:['node','/app/human-origin.mjs'],securityContext:security,resources:resources('96Mi'),volumeMounts:tlsMount,readinessProbe:probe(8080,'/healthz'),livenessProbe:probe(8080,'/healthz')},
  {name:'vault-proxy',image:images.vault,imagePullPolicy:'IfNotPresent',command:['vault'],args:['proxy','-config=/config/proxy.hcl'],securityContext:security,resources:resources('96Mi'),volumeMounts:[mount('proxy-config','/config'),mount('vault-auth','/var/run/vault-auth'),mount('proxy-tmp','/tmp',false)]},
 ],volumes:[identity,{name:'config',configMap:{name:'app-config'}},{name:'key-source',secret:{secretName:'backend-key',defaultMode:288}},{name:'backend-key',emptyDir:{medium:'Memory',sizeLimit:'1Mi'}},{name:'tmp',emptyDir:{sizeLimit:'512Mi'}},{name:'proxy-tmp',emptyDir:{sizeLimit:'16Mi'}},{name:'proxy-config',configMap:{name:'vault-proxy'}},{name:'vault-auth',projected:{defaultMode:288,sources:[{serviceAccountToken:{path:'token',expirationSeconds:3600,audience:'vault'}}]}}]}}}},'apps/v1');
 const cell=objects.find(x=>x.kind==='Deployment'&&x.metadata.name==='cell').spec.template.spec;
 cell.volumes.push(clientIdentity);
 for(const name of ['runner','human']){const c=cell.containers.find(x=>x.name===name);c.volumeMounts=c.volumeMounts.map(m=>m.name==='identity'?{...m,name:'client-identity'}:m);}
 const proxy=cell.containers.find(x=>x.name==='vault-proxy'); cell.containers=cell.containers.filter(x=>x!==proxy);
 proxy.restartPolicy='Always';proxy.startupProbe={exec:{command:['sh','-ec','wget -q -O /dev/null http://127.0.0.1:8201/v1/auth/token/lookup-self']},periodSeconds:2,timeoutSeconds:2,failureThreshold:60};cell.initContainers.push(proxy);
 service('cell',8080,8080,'NodePort',namespace==='ahp-poc'?30222:undefined);
 obj('PodDisruptionBudget','cell',{spec:{minAvailable:1,selector:{matchLabels:{'ahp.local/component':'cell'}}}},'policy/v1');
 obj('NetworkPolicy','default-deny',{spec:{podSelector:{},policyTypes:['Ingress','Egress']}},'networking.k8s.io/v1');
 obj('NetworkPolicy','dns',{spec:{podSelector:{},policyTypes:['Egress'],egress:[{to:[{namespaceSelector:{matchLabels:{'kubernetes.io/metadata.name':'kube-system'}},podSelector:{matchLabels:{'k8s-app':'kube-dns'}}}],ports:[{protocol:'UDP',port:53},{protocol:'TCP',port:53}]}]}},'networking.k8s.io/v1');
 const allow=(name,component,from,ports)=>obj('NetworkPolicy',name,{spec:{podSelector:{matchLabels:{'ahp.local/component':component}},policyTypes:['Ingress'],ingress:[{from:from.map(c=>({podSelector:{matchLabels:{'ahp.local/component':c}}})),ports:ports.map(port=>({protocol:'TCP',port}))}]}},'networking.k8s.io/v1');
 for(const name of ['postgres','redis','minio','vault'])allow(name+'-from-cell',name,['cell','ops'],[{postgres:5432,redis:6379,minio:9000,vault:8200}[name]]);
 allow('collector-from-cell','collector',['cell','ops'],[4317,4318]);
 if(configuration.collectorIngressCIDRs?.length)obj('NetworkPolicy','collector-local-edge',{spec:{podSelector:{matchLabels:{'ahp.local/component':'collector'}},policyTypes:['Ingress'],ingress:[{from:configuration.collectorIngressCIDRs.map(cidr=>({ipBlock:{cidr}})),ports:[{protocol:'TCP',port:4317}]}]}},'networking.k8s.io/v1');
 obj('NetworkPolicy','cell-egress',{spec:{podSelector:{matchLabels:{'ahp.local/component':'cell'}},policyTypes:['Egress'],egress:[{to:['postgres','redis','minio','vault','collector'].map(c=>({podSelector:{matchLabels:{'ahp.local/component':c}}}))},{to:configuration.inferenceCIDRs.map(cidr=>({ipBlock:{cidr}})),ports:[{protocol:'TCP',port:443}]}]}},'networking.k8s.io/v1');
 // Only the local NodePort edge is open; the node itself is published on 127.0.0.1.
 obj('NetworkPolicy','human-ingress',{spec:{podSelector:{matchLabels:{'ahp.local/component':'cell'}},policyTypes:['Ingress'],ingress:[{ports:[{protocol:'TCP',port:8080}]}]}},'networking.k8s.io/v1');
 obj('NetworkPolicy','vault-kubeapi',{spec:{podSelector:{matchLabels:{'ahp.local/component':'vault'}},policyTypes:['Egress'],egress:[{to:configuration.kubeAPICIDRs.map(cidr=>({ipBlock:{cidr}})),ports:[{protocol:'TCP',port:443},{protocol:'TCP',port:6443}]}]}},'networking.k8s.io/v1');
 obj('NetworkPolicy','ops-egress',{spec:{podSelector:{matchLabels:{'ahp.local/component':'ops'}},policyTypes:['Egress'],egress:[{to:[{podSelector:{}}]}]}},'networking.k8s.io/v1');
 return {apiVersion:'v1',kind:'List',items:objects};
}
