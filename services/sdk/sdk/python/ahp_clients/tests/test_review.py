import asyncio
import unittest
import httpx
from ahp_clients import ModelClient, AsyncModelClient, APIError

class ReviewTests(unittest.TestCase):
    def test_stream_mutation_uncertainty(self):
        for status, body, ctype, unknown in [(503,b'{"code":"UNAVAILABLE"}',"application/json",True),(503,b'proxy',"text/html",True),(200,b'bad',"text/html",True),(200,b'bad\n',"application/x-ndjson",True),(403,b'{"code":"FORBIDDEN"}',"application/json",False)]:
            def handle(request): return httpx.Response(status,content=body,headers={"content-type":ctype})
            with self.subTest(status=status,ctype=ctype):
                with httpx.Client(transport=httpx.MockTransport(handle)) as http:
                    c=ModelClient("https://owner",lambda:"token",http_client=http)
                    with self.assertRaises(APIError) as e:list(c.stream("stable",[{"role":"user","content":"x"}]))
                    self.assertEqual(e.exception.outcome_unknown,unknown)
                async def run():
                    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                        c=AsyncModelClient("https://owner",lambda:"token",http_client=http)
                        with self.assertRaises(APIError) as e:
                            [f async for f in c.stream("stable",[{"role":"user","content":"x"}])]
                        self.assertEqual(e.exception.outcome_unknown,unknown)
                asyncio.run(run())

    def test_malformed_nested_stream_shapes(self):
        import json
        from ahp_clients import A2AClient, AsyncA2AClient
        for family in ("model-delta", "model-generation", "a2a-error", "a2a-status"):
            def handle(request):
                if family.startswith("model"):
                    bad = {"type":"delta", "delta":"bad"} if family.endswith("delta") else {"type":"terminal", "generation":[]}
                    return httpx.Response(200,headers={"content-type":"application/x-ndjson"},content=(json.dumps({"type":"accepted","generation_id":"g"})+"\n"+json.dumps(bad)+"\n").encode())
                record={"jsonrpc":"2.0","id":json.loads(request.content)["id"]}
                record.update({"error":"bad"} if family.endswith("error") else {"result":{"task":{"status":[]}}})
                return httpx.Response(200,headers={"content-type":"text/event-stream"},content=("data: "+json.dumps(record)+"\n\n").encode())
            with self.subTest(family=family):
                with httpx.Client(transport=httpx.MockTransport(handle)) as http:
                    c=(ModelClient if family.startswith("model") else A2AClient)("https://owner",lambda:"token",http_client=http)
                    with self.assertRaises(APIError) as e:
                        list(c.stream("stable",[{"role":"user","content":"x"}]) if family.startswith("model") else c.resubscribe("task"))
                    self.assertTrue(e.exception.outcome_unknown)
                async def run():
                    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                        c=(AsyncModelClient if family.startswith("model") else AsyncA2AClient)("https://owner",lambda:"token",http_client=http)
                        source=c.stream("stable",[{"role":"user","content":"x"}]) if family.startswith("model") else c.resubscribe("task")
                        with self.assertRaises(APIError) as e:[f async for f in source]
                        self.assertTrue(e.exception.outcome_unknown)
                asyncio.run(run())
