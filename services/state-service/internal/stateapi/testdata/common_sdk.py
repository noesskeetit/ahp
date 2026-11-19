"""Actual Python SDK -> AHP HTTPS/mTLS/PG, invoked by the Go integration test."""
import asyncio,json,ssl,sys
from pathlib import Path
import httpx
from ahp_state.client import StateAPIClient

async def main():
    seed=json.loads(Path(sys.argv[1]).read_text())
    context=ssl.create_default_context(cafile=seed['ca'])
    context.load_cert_chain(seed['cert'],seed['key'])
    async with httpx.AsyncClient(verify=context,trust_env=False,timeout=15)as transport:
        client=StateAPIClient(seed['url'],token=seed['token'],http_client=transport)
        if len(sys.argv)>2 and sys.argv[2]=='cold':
            from ahp_state.adk import AHPSessionService
            service=AHPSessionService(client,thread_id=seed['thread'],fence=seed['fence'],app_name='app',user_id='user',session_id='session',namespace='adk-native',user_namespace=['user'],app_namespace=['app'])
            session=await service.get_session(app_name='app',user_id='user',session_id='session')
            assert len(session.events)>=6 and session.state['app:updated']=='z'*1100000
            assert session.state['user:large']=='я'*550000
            print(json.dumps({'status':'passed','cold_process':True}));return
        old=await client.read_records(seed['thread'],'native',1)
        assert len(old['document']['records'])==1
        current=await client.read_records(seed['thread'],'native')
        assert current['receipt']['revision']==2
        assert current['document']['records']['s']['relations']['events']==['e1']
        bundle=await client.read_records_bundle(seed['thread'],'native',[{'namespace':['app'],'key':'state','expected_owner':'deployment'}],2)
        assert bundle['common']is None and bundle['common_unchanged']is True
        assert bundle['kv'][0]['version']==2
        delta={'format':'ahp-common-semantic-records/research-2','profile':'sdk-test/1','root':'s','put':{'e2':{'kind':'event','attributes':{'text':'python\u0000event'},'relations':{}}},'relations':[{'source':'s','role':'events','mode':'append','expected_length':1,'targets':['e2']}]}
        request=dict(fence=seed['fence'],namespace='native',key='python-event',expected_revision=2,delta=delta)
        receipt=await client.commit_records(**request)
        assert receipt['revision']==3
        replay=await client.commit_records(**request)
        assert replay==receipt
        final=await client.read_records(seed['thread'],'native')
        assert final['document']['records']['s']['relations']['events']==['e1','e2']
        assert final['document']['records']['e2']['attributes']['text']=='python\u0000event'
        import importlib.util
        assert importlib.util.find_spec('ahp_state.shared') is not None,'LARGE_SHARED_SDK_NOT_IMPLEMENTED'
        from ahp_state.shared import SharedValues
        assert (await SharedValues(client,['app'],'deployment','from-go').read())[0]=={'large':'g'*1200000,'null':None}
        shared=SharedValues(client,['app'],'deployment','shared-state')
        data={'alpha':'я'*600000,'beta':{'value':'b'*600000},'empty':None}
        prepared=await shared.prepare(seed['fence'],data)
        assert prepared['expected_version']==0 and len(json.dumps(prepared['value']))<20000
        doc={'format':'ahp-common-semantic-records/research-2','profile':'shared-test/1','root':'s','records':{'s':{'kind':'session','attributes':{},'relations':{}}}}
        await client.commit_records(seed['fence'],'large-shared','publish',0,document=doc,kv=[prepared])
        cold=SharedValues(client,['app'],'deployment','shared-state')
        assert (await cold.read())[0]==data
        # Publication failure cannot expose the newly staged data or a session event.
        bad=await cold.prepare(seed['fence'],{'alpha':'changed'})
        try:
            await client.commit_records(seed['fence'],'large-shared','failed-publication',0,document=doc,kv=[bad])
        except Exception as exc:
            assert getattr(exc,'code',None)=='CONFLICT'
        else:raise AssertionError('stale common publication succeeded')
        assert (await cold.read())[0]==data
        # Replacing an immutable KV chunk cannot silently change native data.
        manifest=(await cold.read())[1]['value']
        h=manifest['values'][0]['chunks'][0]
        key={'namespace':['app'],'key':'blob-'+h,'expected_owner':'deployment'}
        row=(await client.get_kv_batch([key]))['items'][0]
        await client.put_kv_batch(seed['fence'],'corrupt-chunk',[{**key,'expected_version':row['version'],'value':{'format':'ahp-shared-chunk/1','bytes_base64':'d3Jvbmc='}}])
        try:await cold.read()
        except Exception as exc:assert getattr(exc,'code',None)=='INVALID_ARGUMENT'
        else:raise AssertionError('tampered shared bytes accepted')
        await client.put_kv_batch(seed['fence'],'restore-chunk',[{**key,'expected_version':row['version']+1,'value':row['value']}])
        assert (await cold.read())[0]==data
        assert (await client.read_records(seed['thread'],'large-shared'))['receipt']['revision']==1
        assert importlib.util.find_spec('ahp_state.adk') is not None,'NATIVE_ADK_ADAPTER_NOT_IMPLEMENTED'
        from ahp_state.adk import AHPSessionService
        from google.adk.events import Event,EventActions
        service=AHPSessionService(client,thread_id=seed['thread'],fence=seed['fence'],app_name='app',user_id='user',session_id='session',namespace='adk-native',user_namespace=['user'],app_namespace=['app'])
        session=await service.create_session(app_name='app',user_id='user',session_id='session',state={'private':17,'app:large':'x'*1100000,'user:large':'я'*550000})
        event=Event(author='user',invocation_id='turn1',actions=EventActions(state_delta={'app:other':42,'user:other':43,'private':18,'temp:ephemeral':99}))
        await service.append_event(session,event)
        assert session.state['private']==18 and session.state['app:other']==42
        stale=session.model_copy(deep=True);stale._storage_update_marker='1'
        await service.append_event(stale,event)
        assert len(stale.events)==1
        await service.append_event(session,event)
        assert len(session.events)==1
        changed=event.model_copy(deep=True);changed.actions.state_delta['private']=999
        try:await service.append_event(session,changed)
        except Exception as exc:assert getattr(exc,'code',None)=='KEY_REUSE_MISMATCH'
        else:raise AssertionError('same native event ID accepted changed payload')
        restored=AHPSessionService(client,thread_id=seed['thread'],fence=seed['fence'],app_name='app',user_id='user',session_id='session',namespace='adk-native',user_namespace=['user'],app_namespace=['app'])
        recovered=await restored.get_session(app_name='app',user_id='user',session_id='session')
        assert recovered.id==session.id and len(recovered.events)==1
        assert recovered.state['app:large']=='x'*1100000 and recovered.state['user:large']=='я'*550000
        assert 'temp:ephemeral' not in recovered.state
        # Filtered view must never truncate stored history on the following append.
        from google.adk.sessions.base_session_service import GetSessionConfig
        filtered=await restored.get_session(app_name='app',user_id='user',session_id='session',config=GetSessionConfig(num_recent_events=0))
        assert filtered.events==[]
        await restored.append_event(filtered,Event(author='user',invocation_id='turn2',actions=EventActions(state_delta={'private':19,'app:updated':'z'*1100000})))
        assert len((await restored.get_session(app_name='app',user_id='user',session_id='session')).events)==2
        from google.adk.agents import LlmAgent
        from google.adk.models.base_llm import BaseLlm
        from google.adk.models.llm_response import LlmResponse
        from google.adk.runners import Runner
        from google.adk.tools import ToolContext
        from google.genai import types
        calls=[]
        class FixtureModel(BaseLlm):
            async def generate_content_async(self,llm_request,stream=False):
                responses=[p.function_response for c in llm_request.contents for p in (c.parts or []) if p.function_response]
                if responses:
                    yield LlmResponse(content=types.Content(role='model',parts=[types.Part.from_text(text='SAVED')]))
                else:
                    yield LlmResponse(content=types.Content(role='model',parts=[types.Part(function_call=types.FunctionCall(id='sdk-call',name='save_preference',args={}))]))
        async def save_preference(tool_context:ToolContext)->dict:
            assert tool_context.state['app:large']=='x'*1100000
            tool_context.state['user:preference']='blue'
            calls.append('tool')
            return {'saved':True}
        runner=Runner(agent=LlmAgent(name='root',model=FixtureModel(model='fixture'),tools=[save_preference]),app_name='app',session_service=restored)
        outputs=[e async for e in runner.run_async(user_id='user',session_id='session',new_message=types.Content(role='user',parts=[types.Part.from_text(text='SAVE')]))]
        assert calls==['tool']
        assert [p.text for e in outputs for p in (e.content.parts if e.content else []) or [] if p.text]==['SAVED']
        assert (await restored.get_user_state(app_name='app',user_id='user'))['preference']=='blue'
        process=await asyncio.create_subprocess_exec(sys.executable,'-B',str(Path(__file__)),sys.argv[1],'cold',stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
        output,_=await asyncio.wait_for(process.communicate(),30)
        assert process.returncode==0 and json.loads(output)['cold_process'] is True
        await client.delete_thread(seed['thread'])
        assert (await cold.read())[0]==data
        assert (await SharedValues(client,['app'],'deployment','adk-app-state',binding=['app']).read())[0]['large']=='x'*1100000
        assert (await SharedValues(client,['user'],'subject','adk-user-state',binding=['app','user']).read())[0]['large']=='я'*550000
        await client.close()
        assert not transport.is_closed
    print(json.dumps({'status':'passed','revision':3,'replay_exact':True,'native_prefix_preserved':True}))

if __name__=='__main__':
    try: asyncio.run(main())
    except Exception as exc:
        import traceback
        print(json.dumps({'status':'failed','exception':type(exc).__name__,'code':getattr(exc,'code',None),'locations':[f'{Path(f.filename).name}:{f.lineno}' for f in traceback.extract_tb(exc.__traceback__)]}))
        raise SystemExit(1)
