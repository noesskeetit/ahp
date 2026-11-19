import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {webcrypto,createHash} from 'node:crypto';
import vm from 'node:vm';
const source=readFileSync(new URL('./aifactory/artifact-upload.js',import.meta.url),'utf8');
const sandbox={window:{},crypto:webcrypto,URL,Uint8Array,fetch};
vm.runInNewContext(source,sandbox);
const upload=sandbox.window.AHP_DIRECT_UPLOAD.upload;

test('browser upload sends bytes only to PUT and seals verified digest',async()=>{
 const file=new File(['actual browser bytes'],'document.txt',{type:'text/plain'});
 const sha=createHash('sha256').update(await file.bytes()).digest('hex');
 const operations=[];
 const call=async(op,body)=>{
  operations.push(op);assert.equal(body instanceof Blob,false);
  if(op==='BeginUpload') {assert.equal(body.sha256,sha);return {status:200,json:{id:'id',presigned_url:'https://object.test/file',sha256:sha,size:file.size,headers:{'Content-Length':String(file.size)}}};}
  return {status:200,json:{id:'id',version:'immutable',sha256:sha,size:file.size}};
 };
 let puts=0;
 const response=await upload(file,call,async(url,options)=>{
  puts++;assert.equal(url,'https://object.test/file');assert.equal(options.method,'PUT');assert.equal(options.body,file);
  assert.equal(options.credentials,'omit');assert.equal(options.redirect,'error');assert.equal(options.referrerPolicy,'no-referrer');
  assert.deepEqual(Object.keys(options.headers),[]);return {ok:true};
 });
 assert.deepEqual(operations,['BeginUpload','Seal']);assert.equal(puts,1);assert.equal(response.status,200);
});
test('PUT failure cannot seal',async()=>{
 const file=new File(['x'],'x.txt');let seals=0;
 const sha=createHash('sha256').update('x').digest('hex');
 await assert.rejects(upload(file,async(op)=>{if(op==='Seal')seals++;return {status:200,json:{id:'id',presigned_url:'https://object.test/x',sha256:sha,size:1}};},async()=>({ok:false,status:403})),/403/);
 assert.equal(seals,0);
});
