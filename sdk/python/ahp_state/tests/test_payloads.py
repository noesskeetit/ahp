"""SDK payload behaviour over HTTP doubles; real owner coverage is separate."""
import base64
import hashlib
import json

import httpx
import pytest
import respx
from langgraph.graph import StateGraph, START, END
from typing_extensions import TypedDict

from ahp_state import AHPCheckpointSaver, StateAPIClient


class LargeState(TypedDict):
    value: str
    count: int


@pytest.mark.asyncio
async def test_large_langgraph_resume_metadata_pending_and_revocation():
    objects, refs, checkpoints, parts, writes = {}, {}, {}, {}, {}
    denied = False
    corrupt = False

    def api(request):
        nonlocal denied
        body = json.loads(request.content)
        op = request.url.path.rsplit('/', 1)[-1]
        if '/artifacts/' in request.url.path:
            if denied:
                return httpx.Response(403, json={'code': 'FORBIDDEN', 'message': 'revoked'})
            if op == 'BeginUpload':
                ident = hashlib.sha256(body['key'].encode()).hexdigest()
                refs[ident] = dict(id=ident, version='v1', sha256=body['sha256'], size=body['size'], tenant='tenant', deployment='dep', subject='alice', context_id='ctx', task_id='run', labels=None)
                return httpx.Response(200, json=dict(id=ident, presigned_url=f'https://blob.test/{ident}', sha256=body['sha256'], size=body['size']))
            if op in ('Seal', 'Get'):
                return httpx.Response(200, json=refs[body['id']])
            if op == 'Download':
                data = objects[body['id']]
                if corrupt == 'digest':
                    data = bytes([data[0] ^ 1]) + data[1:]
                elif corrupt == 'size':
                    data = b'corrupt'
                return httpx.Response(200, content=data)
        if op == 'PutCheckpoint':
            cp = json.loads(json.dumps(body['checkpoint']))
            for p in cp.get('parts', []):
                if len(base64.b64decode(p['data'])) > 1 << 20:
                    return httpx.Response(400, json={'code': 'INVALID_ARGUMENT', 'message': 'part data exceeds 1MiB'})
                parts[(cp['namespace'], p['channel'], p['version'])] = dict(data=p.pop('data'), encoding=p.pop('encoding', ''))
            checkpoints[cp['id']] = cp
            return httpx.Response(200, json=cp)
        if op == 'GetCheckpoint':
            if not checkpoints:
                return httpx.Response(404, json={'code': 'NOT_FOUND', 'message': 'missing'})
            cp = checkpoints.get(body.get('id')) or list(checkpoints.values())[-1]
            return httpx.Response(200, json=cp)
        if op == 'GetPart':
            return httpx.Response(200, json=parts[(body['namespace'], body['channel'], body['version'])])
        if op == 'PutWrites':
            writes.setdefault(body['checkpoint_id'], []).extend(body['writes'])
            return httpx.Response(200, json={'ok': True})
        if op == 'GetWrites':
            return httpx.Response(200, json={'writes': writes.get(body['checkpoint_id'], [])})
        if op == 'ListCheckpoints':
            return httpx.Response(200, json={'checkpoints': list(reversed(checkpoints.values()))})
        raise AssertionError(op)

    def put(request):
        assert 'authorization' not in request.headers
        objects[request.url.path[1:]] = request.content
        return httpx.Response(200)

    with respx.mock(assert_all_called=False) as router:
        router.post(url__startswith='https://owner.test/').mock(side_effect=api)
        router.put(url__startswith='https://blob.test/').mock(side_effect=put)
        async with StateAPIClient('https://owner.test', token='task') as client:
            fence = {'run_id': 'run', 'worker': 'w', 'epoch': 1}
            saver = AHPCheckpointSaver(client, 'thread', fence)
            graph = StateGraph(LargeState)
            graph.add_node('step', lambda s: {'count': s['count'] + 1})
            graph.add_edge(START, 'step')
            graph.add_edge('step', END)
            config = {'configurable': {'thread_id': 'thread'}}
            large = 'длинный payload ' * 100000
            result = await graph.compile(checkpointer=saver).ainvoke({'value': large, 'count': 1}, config, durability='sync')
            assert result['value'] == large
            assert objects
            # Fresh saver over the same wire: serializer/body/channel versions survive.
            fresh = AHPCheckpointSaver(client, 'thread', fence)
            restored = await graph.compile(checkpointer=fresh).ainvoke(None, config, durability='sync')
            assert restored == result
            tup = await fresh.aget_tuple(config)
            assert tup.metadata['step'] >= 1
            await fresh.aput_writes(tup.config, [('value', large)], 'pending')
            assert (await fresh.aget_tuple(config)).pending_writes[-1] == ('pending', 'value', large)
            for field in ('version', 'sha256', 'size', 'tenant', 'subject', 'context_id', 'task_id', 'labels'):
                old = {ident: ref[field] for ident, ref in refs.items()}
                for ref in refs.values():
                    ref[field] = 1 if field == 'size' else 'mismatch'
                with pytest.raises(Exception, match='descriptor'):
                    await fresh.aget_tuple(config)
                for ident, ref in refs.items():
                    ref[field] = old[ident]
            for mode in ('digest', 'size'):
                corrupt = mode
                with pytest.raises(Exception, match='integrity|size|digest'):
                    await fresh.aget_tuple(config)
            corrupt = False
            denied = True
            with pytest.raises(Exception, match='FORBIDDEN'):
                await fresh.aget_tuple(config)

@pytest.mark.asyncio
async def test_legacy_small_marker_collision_and_metadata():
    from ahp_state.payload import Payloads, MAGIC, METADATA_KEY, INLINE_BYTES
    async with StateAPIClient('https://owner.test', token='task') as client:
        adapter = client._payloads
        for raw in (b'', b'old', MAGIC + b'Ruser-data', b'x' * INLINE_BYTES):
            assert await adapter.hydrate(await adapter.spill('run', raw)) == raw
        for md in ({METADATA_KEY: 'ordinary user text'}, {METADATA_KEY: 'eA=='}, {METADATA_KEY: {'x': 1}}):
            assert (await adapter.hydrate_checkpoint({'metadata': md}))['metadata'] == md
            cp = await adapter.checkpoint('run', {'metadata': md})
            assert (await adapter.hydrate_checkpoint(cp))['metadata'] == md

@pytest.mark.asyncio
async def test_checkpoint_reference_overhead_fails_before_http():
    from ahp_state.client import StateValidationError
    async with StateAPIClient('https://owner.test', token='task') as client:
        cp = {'id': 'cp', 'version': 1, 'parts': [{'channel': 'c' * 512, 'version': 'v' * 512} for _ in range(3000)]}
        with pytest.raises(StateValidationError, match='after payload spill'):
            await client.put_checkpoint({'run_id': 'run', 'worker': 'w', 'epoch': 1}, cp, 'key')


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 403])
async def test_presigned_put_response_is_not_buffered(status):
    from ahp_state.client import StateValidationError
    raw = b"x" * ((256 << 10) + 1)
    digest = hashlib.sha256(raw).hexdigest()
    consumed = 0
    closed = False

    class UploadBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            nonlocal consumed
            for _ in range(64):
                consumed += 65536
                yield b"X" * 65536

        async def aclose(self):
            nonlocal closed
            closed = True

    def api(request):
        op = request.url.path.rsplit("/", 1)[-1]
        if op == "BeginUpload":
            return httpx.Response(200, json=dict(id="ref", presigned_url="https://blob.test/response", sha256=digest, size=len(raw)))
        assert op == "Seal"
        return httpx.Response(200, json=dict(id="ref", version="v1", sha256=digest, size=len(raw), context_id="ctx", task_id="task"))

    with respx.mock(assert_all_called=False) as router:
        router.post(url__startswith="https://owner.test/").mock(side_effect=api)
        router.put("https://blob.test/response").mock(side_effect=lambda _: httpx.Response(status, stream=UploadBody()))
        async with StateAPIClient("https://owner.test", token="task") as client:
            if status == 200:
                await client._payloads.spill("run", raw)
            else:
                with pytest.raises(StateValidationError, match="upload refused"):
                    await client._payloads.spill("run", raw)
    assert closed
    assert consumed == 0


@pytest.mark.asyncio
async def test_preexisting_opaque_payload_is_never_sniffed():
    from ahp_state.payload import MAGIC, METADATA_KEY
    async with StateAPIClient("https://owner.test", token="task") as client:
        for raw in (MAGIC + b"Ilegacy", MAGIC + b"Rnot-a-reference"):
            assert await client._payloads.hydrate(raw) == raw
        md = {METADATA_KEY: "AEFIUC1QQVlMT0FELXYxCkl7ImtlZXAiOiJsZWdhY3kifQ=="}
        assert (await client._payloads.hydrate_checkpoint({"metadata": md}))["metadata"] == md


@pytest.mark.asyncio
async def test_descriptor_ignores_current_effective_labels_on_retry():
    raw = b"x" * ((256 << 10) + 1)
    digest = hashlib.sha256(raw).hexdigest()
    effective = {"subjects": ["alice"]}
    declaration = {}
    def api(request):
        op = request.url.path.rsplit("/", 1)[-1]
        if op == "BeginUpload":
            declaration.update(json.loads(request.content))
            return httpx.Response(200, json=dict(id="ref", presigned_url="https://blob.test/retry", sha256=declaration["sha256"], size=declaration["size"]))
        assert op == "Seal"
        return httpx.Response(200, json=dict(id="ref", version="v1", sha256=declaration["sha256"], size=declaration["size"], context_id="ctx", task_id="task", labels={"subjects": ["alice"]}, effective_labels=effective))
    with respx.mock(assert_all_called=False) as router:
        router.post(url__startswith="https://owner.test/").mock(side_effect=api)
        router.put("https://blob.test/retry").respond(200)
        async with StateAPIClient("https://owner.test", token="task") as client:
            cp = dict(metadata={"large": raw.decode()}, parts=[dict(channel="x",version="v1",data=base64.b64encode(raw).decode())])
            writes = [dict(task="t",index=0,channel="x",data=base64.b64encode(raw).decode())]
            before = await client._payloads.checkpoint("run", cp)
            pending = await client._payloads.writes("run", writes)
            effective["workspaces"] = ["review-workspace"]
            assert await client._payloads.checkpoint("run", cp) == before
            assert await client._payloads.writes("run", writes) == pending
            assert "effective_labels" not in json.dumps(before)
            with pytest.raises(Exception, match="unsupported.*encoding"):
                await client._payloads.hydrate(b"{}", "future-v2")
