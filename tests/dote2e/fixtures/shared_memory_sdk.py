"""Actual public Python client, stdin-only credentials, no local state fallback."""
import asyncio,json,sys
import httpx
from ahp_clients import APIError,SharedMemoryClient,AsyncSharedMemoryClient,mtls_context

request=json.load(sys.stdin)
tls=mtls_context(request['ca'],request['cert'],request['key'])
with httpx.Client(verify=tls,trust_env=False,timeout=30)as http:
    client=SharedMemoryClient(request['origin'],lambda:request['token'],http_client=http)
    if request['action']=='member':
        record=client.read(request['id']);assert record['id']==request['id']and record['state']=='active'
        result=client.search({'kind':'workspace','query':request['query'],'limit':50})
        assert result['mode']=='hybrid'and result['hits']and result['hits'][0]['memory']['id']==request['id']
        print(json.dumps({'status':'PASS','record':record['id'],'mode':result['mode']}))
    elif request['action']=='revoked':
        try:client.read(request['id'])
        except APIError as error:assert error.status in(403,404)
        else:raise AssertionError('revoked member read the fact')
        try:
            result=client.search({'kind':'workspace','query':request['query'],'limit':50})
            assert not any(h['memory']['id']==request['id']for h in result['hits'])
        except APIError as error:assert error.status in(403,404)
        try:client.propose(request['op_key'],{'kind':'workspace','workspace':request['workspace'],'text':'post-revocation write must fail','source_kind':'user_statement','evidence':[{'source_kind':'user_statement','detail':'synthetic revoked caller'}]})
        except APIError as error:assert error.status in(403,404)
        else:raise AssertionError('revoked member proposed a fact')
        print(json.dumps({'status':'PASS','read_denied':True,'retrieval_denied':True,'write_denied':True,'same_task_token':True}))
    elif request['action']=='operations':
        op=client.get_operation(request['op_key'])
        assert op['status']=='SUCCEEDED'and op['result_id']==request['id']and'text'not in op
        closed=request['closed_key']
        assert client.fence(closed)['status']=='FENCED'
        assert client.get_operation(closed)['status']=='FENCED'
        async def run():
            async with httpx.AsyncClient(verify=tls,trust_env=False,timeout=30)as async_http:
                c=AsyncSharedMemoryClient(request['origin'],lambda:request['token'],http_client=async_http)
                assert(await c.get_operation(request['op_key']))['result_id']==request['id']
                assert(await c.fence(closed))['status']=='FENCED'
        asyncio.run(run())
        print(json.dumps({'status':'PASS','sync_and_async':True,'same_go_record':True,'fenced_key':True}))
    else:raise AssertionError('unknown driver action')
