"""Common SDK wire behavior; real PG/native acceptance is a separate suite."""
import asyncio
import json
import unittest
import httpx
from ahp_state.client import StateAPIClient,StateValidationError,StateAPIError

FENCE={"run_id":"r","worker":"w","epoch":7}
class CommonClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_claim_payload_filter_preserves_workload_auth_and_defaults(self):
        seen=[]
        def handle(req):
            self.assertNotIn('Authorization',req.headers)
            seen.append(json.loads(req.content))
            return httpx.Response(200,json={'lease':None,'task_token':''})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle))as transport:
            c=StateAPIClient('https://state.invalid',token_source=lambda:self.fail('claim consulted task token'),http_client=transport)
            await c.claim_run('w',30000)
            await c.claim_run_for('w','r',30000,payload_type='turn')
            self.assertEqual(seen,[{'worker':'w','ttl_ms':30000},{'worker':'w','ttl_ms':30000,'run_id':'r','payload_type':'turn'}])
            for bad in ['x'*129,'\ud800']:
                with self.assertRaises(StateValidationError):await c.claim_run('w',payload_type=bad)
            self.assertEqual(len(seen),2)
    async def test_delta_fence_and_fresh_shared_values_survive_transport(self):
        requests=[]
        def handle(req):
            body=json.loads(req.content);requests.append((req.url.path,body))
            self.assertEqual(req.headers['Authorization'],'Bearer current')
            if req.url.path.endswith('CommitRecords'):
                self.assertNotIn('document',body);self.assertEqual(body['fence'],FENCE)
                self.assertEqual(body['delta']['root'],'s')
                return httpx.Response(200,json={'cut_id':'c1','revision':1,'kv':[{'version':3}]})
            self.assertEqual(body['known_revision'],1)
            return httpx.Response(200,json={'common':None,'common_unchanged':True,'receipt':{'cut_id':'c1','revision':1},'kv':[{'version':4}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle))as transport:
            client=StateAPIClient('https://state.invalid',token='current',http_client=transport)
            r=await client.commit_records(FENCE,'native','event1',0,delta={'format':'ahp-common-semantic-records/research-2','profile':'test/1','root':'s','put':{}})
            self.assertEqual(r['revision'],1)
            b=await client.read_records_bundle('t','native',[{'namespace':['app'],'key':'state','expected_owner':'deployment'}],1)
            self.assertIsNone(b['common']);self.assertEqual(b['kv'][0]['version'],4)
            self.assertEqual(len(requests),2)
            await client.close();self.assertFalse(transport.is_closed)
    async def test_ambiguous_publication_is_rejected_before_http(self):
        requests=[]
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:requests.append(r)or httpx.Response(200,json={})))as transport:
            c=StateAPIClient('https://state.invalid',token='t',http_client=transport)
            for kwargs in [{},{'document':{},'delta':{}}]:
                with self.assertRaises(StateValidationError):await c.commit_records(FENCE,'n','k',0,**kwargs)
            self.assertEqual(requests,[])
    async def test_common_unknown_outcome_is_not_retried(self):
        calls=0
        def handle(req):
            nonlocal calls
            calls+=1;raise httpx.ReadError('lost response',request=req)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle))as transport:
            c=StateAPIClient('https://state.invalid',token='t',http_client=transport)
            with self.assertRaises(StateAPIError)as e:await c.commit_records(FENCE,'n','same-key',0,document={})
            self.assertEqual(e.exception.code,'TRANSPORT');self.assertEqual(calls,1)
    async def test_token_source_refreshes_per_state_and_artifact_request(self):
        tokens=iter(['one','two']);seen=[]
        async def source():return next(tokens)
        def handle(req):seen.append(req.headers.get('Authorization'));return httpx.Response(200,json={})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle))as transport:
            c=StateAPIClient('https://state.invalid',token_source=source,http_client=transport)
            await c.read_records('t','n')
            await c._payloads.request('Get',{'id':'object'})
            self.assertEqual(seen,['Bearer one','Bearer two'])
    async def test_empty_source_fails_before_network(self):
        seen=[]
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:seen.append(r)or httpx.Response(200,json={})))as transport:
            c=StateAPIClient('https://state.invalid',token_source=lambda:'',http_client=transport)
            with self.assertRaises(StateAPIError)as e:await c.read_records('t','n')
            self.assertEqual(e.exception.code,'FORBIDDEN');self.assertEqual(seen,[])
if __name__=='__main__':unittest.main()
