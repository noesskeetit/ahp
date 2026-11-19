#!/usr/bin/env node
// Creates only this project's explicitly named local cluster. Default kubeconfig
// is never consulted or modified. Dependency pulls/imports are bounded/sequential.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import{spawnSync}from'node:child_process';import{fileURLToPath}from'node:url';import{images}from'../infra/local-k8s/profile.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const home=path.resolve(process.env.AHP_K8S_RUNTIME||path.join(root,'private/deployment-complete'));
if(!home.startsWith(root+path.sep+'private'+path.sep))throw Error('runtime must be private to this checkout');
fs.mkdirSync(path.join(home,'bin'),{recursive:true,mode:0o700});
const executable=path.join(home,'bin/k3d'),name='ahp-handbook-poc';
function run(bin,args,options={}){const r=spawnSync(bin,args,{cwd:root,encoding:'utf8',maxBuffer:16<<20,...options});if(r.status!==0)throw Error(bin+' '+args[0]+' failed: '+r.stderr?.slice(-1500));return r.stdout;}
if(process.argv[2]==='create'){
 const arch={arm64:'arm64',x64:'amd64'}[process.arch],os={darwin:'darwin',linux:'linux'}[process.platform];if(!arch||!os)throw Error('unsupported host');
 const asset='k3d-'+os+'-'+arch,url='https://github.com/k3d-io/k3d/releases/download/v5.9.0/';
 const sums=await fetch(url+'checksums.txt');if(!sums.ok)throw Error('k3d checksums download failed');const checksums=await sums.text();
 const expected=checksums.split('\n').map(x=>x.trim().split(/\s+/)).find(x=>x[1]?.split('/').at(-1)===asset)?.[0];if(!expected)throw Error('binary absent from checksums');
 const response=await fetch(url+asset);if(!response.ok)throw Error('k3d binary download failed');const bytes=Buffer.from(await response.arrayBuffer());
 if(crypto.createHash('sha256').update(bytes).digest('hex')!==expected)throw Error('k3d release checksum mismatch');fs.writeFileSync(executable,bytes,{mode:0o700});fs.chmodSync(executable,0o700);
 const current=run('docker',['ps','-a','--format','{{.Names}}']).trim().split('\n');
 if(current.some(n=>n.startsWith('k3d-'+name+'-')))throw Error('cluster resources already exist; refusing create/replacement');
 run(executable,['cluster','create',name,'--servers','1','--agents','0','--image','rancher/k3s:v1.34.12-k3s1','--servers-memory','3g','--api-port','127.0.0.1:30243','--port','127.0.0.1:30222:30222@server:0','--port','127.0.0.1:30317:30317@server:0','--kubeconfig-update-default=false','--kubeconfig-switch-context=false','--k3s-arg','--disable=traefik,servicelb,metrics-server@server:0','--runtime-label','com.docker.compose.project=ahp-handbook@all','--k3s-node-label','ahp.local/owner=ahp-handbook@server:0','--timeout','180s']);
 run('docker',['update','--cpus','2','k3d-'+name+'-server-0']);run('docker',['update','--cpus','0.25','--memory','128m','--memory-swap','128m','k3d-'+name+'-serverlb']);
 fs.writeFileSync(path.join(home,'kubeconfig'),run(executable,['kubeconfig','get',name]),{mode:0o600});
 console.log('Created isolated cluster; kubeconfig stays in private runtime directory.');
}else if(process.argv[2]==='images'){
 const node='k3d-'+name+'-server-0';const owner=run('docker',['inspect',node,'--format','{{index .Config.Labels "com.docker.compose.project"}}']).trim();if(owner!=='ahp-handbook')throw Error('wrong cluster ownership');
 for(const image of Object.values(images)){
  if(image.includes('@sha256:')){
   // Pull through the node CRI: Docker's cached multi-arch OCI index can lack
   // non-host manifests, making k3d save/import report errors yet exit zero.
   run('docker',['exec',node,'crictl','pull',image]);
   const info=JSON.parse(run('docker',['exec',node,'crictl','inspecti',image]));
   const expected=image.split('@')[1];
   if(!info.status.repoDigests?.some(d=>d.endsWith('@'+expected)))throw Error('node CRI digest mismatch: '+image);
  }else{
   const exists=spawnSync('docker',['image','inspect',image],{stdio:'ignore'}).status===0;
   if(!exists)throw Error('first build pinned upstream MinIO using scripts/build-minio.mjs');
   const digest=run('docker',['image','inspect',image,'--format','{{.Id}}']).trim();
   run(executable,['image','import','-c',name,image]);
   const entries=run('docker',['exec',node,'ctr','-n','k8s.io','images','ls']);
   if(!entries.split('\n').some(line=>{const f=line.trim().split(/\s+/);return f[0]==='docker.io/library/'+image&&f[2]===digest}))throw Error('source-built image descriptor absent: '+image);
   run('docker',['exec',node,'crictl','inspecti',image]);
  }
  console.log('Verified '+image);
 }
}else throw Error('usage: local-k8s-cluster.mjs create|images');
