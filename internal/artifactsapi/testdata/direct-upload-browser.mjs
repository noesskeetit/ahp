import {pathToFileURL} from 'node:url';
const {chromium}=await import(pathToFileURL(process.argv[3]));
const browser=await chromium.launch({headless:true});
try {
 const page=await browser.newPage();let puts=0;
 page.on('request',request=>{
  if(request.method()==='PUT'){
   puts++;
   if(request.headers().authorization || request.headers().cookie)throw Error('browser leaked authority to object storage');
  }
 });
 await page.goto(process.argv[2]);
 const result=await page.evaluate(async()=>{
  const file=new File(['browser direct upload to real MinIO\nПривет'], 'browser.bin',{type:'application/octet-stream'});
  const result=await window.AHP_DIRECT_UPLOAD.upload(file,async(operation,body)=>{
   const response=await fetch('/u-ahp/v1/artifacts/upload?operation='+operation,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
   return {status:response.status,json:await response.json()};
  });
  if(result.status!==200)throw Error('metadata operation failed '+result.status);
  return {id:result.json.id,sha256:result.json.sha256,bytes:file.size};
 });
 process.stdout.write(JSON.stringify({...result,puts}));
} finally {await browser.close();}
