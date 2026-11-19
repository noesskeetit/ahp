"""Invoked by Go's real-owner fixture; no local database or blob side channel."""
import asyncio
import json
import ssl
import sys

import httpx
from langgraph.graph import StateGraph, START, END
from typing_extensions import TypedDict
from ahp_state import AHPCheckpointSaver, StateAPIClient

class GraphState(TypedDict):
    value: str
    count: int

async def main(path):
    with open(path, encoding='utf-8') as stream:
        config = json.load(stream)
    tls = ssl.create_default_context(cafile=config['ca_file'])
    tls.load_cert_chain(config['cert_file'], config['key_file'])
    async with httpx.AsyncClient(verify=tls, timeout=60, trust_env=False) as transport:
        async with StateAPIClient(config['base_url'], token=config['task_token'], http_client=transport) as client:
            saver = AHPCheckpointSaver(client, config['thread_id'], config['fence'], namespace='large-langgraph')
            graph = StateGraph(GraphState)
            graph.add_node('step', lambda state: {'count': state['count'] + 1})
            graph.add_edge(START, 'step')
            graph.add_edge('step', END)
            run_config = {'configurable': {'thread_id': config['thread_id']}}
            value = 'большие данные\x00' * 150000
            result = await graph.compile(checkpointer=saver).ainvoke({'value': value, 'count': 0}, run_config, durability='sync')
            fresh = AHPCheckpointSaver(client, config['thread_id'], config['fence'], namespace='large-langgraph')
            restored = await graph.compile(checkpointer=fresh).ainvoke(None, run_config, durability='sync')
            assert restored == result
            checkpoint = await fresh.aget_tuple(run_config)
            assert checkpoint.metadata['step'] >= 1
            await fresh.aput_writes(checkpoint.config, [('value', value)], 'pending-task')
            pending = (await fresh.aget_tuple(run_config)).pending_writes
            assert ('pending-task', 'value', value) in pending
            print('LangGraph real PG/MinIO/mTLS: large resume + serializer metadata + pending writes PASS')

asyncio.run(main(sys.argv[1]))
