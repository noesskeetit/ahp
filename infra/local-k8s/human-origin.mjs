// Local prototype edge. Only this container's port is exposed; workload mTLS
// stays server-side and the backend/runner bind the shared Pod's loopback.
import http from 'node:http';
import https from 'node:https';
import {readFileSync, createReadStream, statSync} from 'node:fs';
import {resolve, extname, sep} from 'node:path';
const root=resolve('/app/frontend');
const agent=new https.Agent({cert:readFileSync('/identity/client.crt'),key:readFileSync('/identity/client.key'),ca:readFileSync('/identity/ca.crt'),keepAlive:true,rejectUnauthorized:true});
const mime={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.svg':'image/svg+xml','.png':'image/png','.ico':'image/x-icon','.json':'application/json','.woff2':'font/woff2'};
const server=http.createServer((req,res)=>{
 const u=new URL(req.url,'http://localhost');
 if(u.pathname==='/healthz'){res.writeHead(200).end('ok');return}
 if(['/u-ahp/v1/','/v1/space/','/v1/workspace/','/v1/compute/','/v1/trigger-webhooks/'].some(p=>u.pathname.startsWith(p))){
  const headers={...req.headers,host:'localhost:8443'};for(const k of ['connection','proxy-authorization','proxy-connection'])delete headers[k];
  const call=https.request({hostname:'127.0.0.1',port:8443,agent,method:req.method,path:u.pathname+u.search,headers},answer=>{res.writeHead(answer.statusCode,answer.headers);answer.pipe(res);});
  call.on('error',()=>{if(!res.headersSent)res.writeHead(502,{'content-type':'application/json'});res.end('{"error":"upstream_unavailable"}')});
  req.pipe(call);req.on('aborted',()=>call.destroy());res.on('close',()=>call.destroy());return;
 }
 if(!['GET','HEAD'].includes(req.method)){res.writeHead(405).end();return}
 let file;try{file=resolve(root,'.'+decodeURIComponent(u.pathname));}catch{res.writeHead(400).end();return}
 if(file!==root&&!file.startsWith(root+sep)){res.writeHead(403).end();return}
 if(file===root)file=resolve(root,'index.html');
 try{if(!statSync(file).isFile())throw Error();}catch{file=resolve(root,'index.html');}
 const type=mime[extname(file)];if(!type){res.writeHead(404).end();return}
 res.writeHead(200,{'content-type':type,'x-content-type-options':'nosniff','cache-control':'no-store'});
 if(req.method==='HEAD')res.end();else createReadStream(file).pipe(res);
});
server.listen(8080,'0.0.0.0');
for(const signal of ['SIGINT','SIGTERM'])process.on(signal,()=>server.close(()=>process.exit(0)));
