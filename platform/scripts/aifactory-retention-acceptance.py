"""Destructive only for explicitly created synthetic artifacts in an isolated stand.

Uses real Identity login/exchange, mTLS, PostgreSQL/MinIO and the assembled GC
liveness authority. Never points to a shared stand implicitly. Signed upload URLs
and task credentials remain in memory and are omitted from reports.
"""
import datetime, hashlib, http.cookiejar, json, os, pathlib, ssl, time, urllib.error, urllib.request, urllib.parse
root=pathlib.Path(os.environ['AHP_UI_STAND_DIR'])
origin=os.environ['AHP_UI_BACKEND_ORIGIN']
users=json.loads(pathlib.Path(os.environ['AHP_UI_WORKSPACE_USERS_JSON']).read_text())
report={'origin':origin,'run':'retention-'+str(time.time_ns()),'scope':'isolated assembled API + real Identity/PostgreSQL/MinIO','operations':[]}
tls=ssl.create_default_context(cafile=str(root/'certs/ca.crt'));tls.load_cert_chain(root/'certs/source.crt',root/'certs/source.key')
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs): return None

def request(client,path,body,token=None,expect=200,binary=False):
 headers={'Content-Type':'application/json','X-AHP-Session':'1'}
 if token:headers['Authorization']='Bearer '+token
 req=urllib.request.Request(origin+path,json.dumps(body).encode(),headers)
 try:response=client.open(req,timeout=30)
 except urllib.error.HTTPError as e:response=e
 data=response.read();status=response.status
 report['operations'].append({'path':path,'status':status})
 if expect is None:return status,data
 if status!=expect:
  try:reason=json.loads(data).get('code','')
  except:reason='non-JSON response'
  raise RuntimeError(path+' status '+str(status)+' '+reason)
 return data if binary else json.loads(data)

def session(user):
 client=urllib.request.build_opener(NoRedirect(),urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=tls),urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
 request(client,'/u-ahp/v1/auth/login',{'username':user['email'],'password':user['password']})
 human=request(client,'/u-ahp/v1/auth/token',{})['access_token']
 task=report['run']+'-'+user['subject']+'-'+str(time.time_ns())
 issued=request(client,'/v1/identity/control/exchange',{'human_token':human,'task':task,'context':task,'capabilities':user['permissions']})
 return client,issued['token'],issued['task'],human,user

def call(s,op,body,**kw):return request(s[0],'/v1/artifacts/'+op,body,s[1],**kw)
def upload(s,key,body):
 digest=hashlib.sha256(body).hexdigest()
 writer=session(s[4])
 up=call(writer,'BeginUpload',{'key':key,'name':'retention.txt','mime':'text/plain','sha256':digest,'size':len(body),'labels':{}})
 obj=urllib.request.build_opener(NoRedirect(),urllib.request.ProxyHandler({}))
 try:
  with obj.open(urllib.request.Request(up['presigned_url'],data=body,method='PUT'),timeout=30) as r:
   if r.status//100!=2:raise RuntimeError()
 except:raise RuntimeError('object upload failed; URL omitted') from None
 ref=call(writer,'Seal',{'id':up['id']});assert ref['sha256']==digest
 # Live Identity authority is itself a retention reference. End only this
 # synthetic writer's own root before assessing otherwise collectable bytes.
 request(writer[0],'/v1/identity/roots/'+urllib.parse.quote(writer[2],safe='')+'/cancel',{},writer[1],expect=204,binary=True)
 return ref
try:
 alice,bob=map(session,users)
 body=b'retention holds and exact-version restore '+report['run'].encode()
 own=upload(alice,report['run']+'-own',body);otherbody=b'foreign owner bytes '+report['run'].encode();other=upload(bob,report['run']+'-foreign',otherbody)
 report.update(artifact=own['id'],foreign=other['id'],sha256=own['sha256'],version=own['version'])
 status,_=call(alice,'Get',{'id':other['id']},expect=None);assert status in (403,404);report['crossOwnerReadStatus']=status
 call(alice,'Delete',{'id':own['id'],'key':report['run']+'-delete'})
 future=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=1)).isoformat();past=(datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(minutes=1)).isoformat()
 # A real assembled deployment intentionally denies request-driven GC. The
 # scheduler owns it. A newly expired canary proves a real scheduler pass ran
 # in each phase; protected bytes are then checked through the service API.
 call(alice,'GC',{},expect=403);report['requestDrivenGCDenied']=True
 def wait_purged(id):
  deadline=time.monotonic()+15
  while time.monotonic()<deadline:
   status,_=call(alice,'GetLifecycle',{'id':id},expect=None)
   if status==404:return
   assert status==200
   time.sleep(0.2)
  raise RuntimeError('scheduler did not collect owned canary within 15 seconds')
 def scheduler_phase(label):
  canary=upload(alice,report['run']+'-'+label,('owned canary '+label+' '+report['run']).encode())
  call(alice,'Delete',{'id':canary['id'],'key':label+'-delete'})
  call(alice,'SetRetention',{'id':canary['id'],'retain_until':past})
  wait_purged(canary['id'])
  life=call(alice,'GetLifecycle',{'id':own['id']})
  assert life['deleted'] is True and life['legal_hold'] is (label=='held')
  return {'canary':canary['id'],'canaryCollected':True,'protectedArtifactLifecycle':life}
 call(alice,'SetRetention',{'id':own['id'],'retain_until':future});report['retention']=scheduler_phase('retention')
 call(alice,'SetLegalHold',{'id':own['id'],'on':True});call(alice,'SetRetention',{'id':own['id'],'retain_until':past});report['held']=scheduler_phase('held')
 call(alice,'Purge',{'id':own['id']},expect=409)
 pin={'kind':'context','ref_id':report['run']+'-pin','artifact_id':own['id']};call(alice,'PinRef',pin);call(alice,'SetLegalHold',{'id':own['id'],'on':False});report['pinned']=scheduler_phase('pinned')
 restored=call(alice,'Restore',{'id':own['id']});assert restored['version']==own['version'] and restored['sha256']==own['sha256']
 restored_bytes=call(alice,'Download',{'id':own['id']},binary=True);assert restored_bytes==body
 call(alice,'UnpinRef',pin);call(alice,'Delete',{'id':own['id'],'key':report['run']+'-delete-2'});call(alice,'SetRetention',{'id':own['id'],'retain_until':past});wait_purged(own['id']);report['purged']=True
 call(alice,'Get',{'id':own['id']},expect=404)
 foreign=call(bob,'Download',{'id':other['id']},binary=True);assert foreign==otherbody
 report.update(restoredBytesMatch=True,foreignBytesPreserved=True,status='passed')
except Exception as e:
 report.update(status='failed',error=str(e));raise
finally:
 pathlib.Path(os.environ['AHP_UI_RETENTION_REPORT']).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
