"""Transport failure tests complement the real socket/PG composition contract."""
import asyncio
import json
import unittest

import httpx
from ahp_clients import ModelClient, AsyncModelClient, ToolsClient, A2AClient, APIError


class TransportTests(unittest.TestCase):
    def test_current_token_reason_redirect_and_unknown(self):
        tokens = iter(["one", "two", "three", "four"])
        calls = []
        def handle(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(200, json={"id": "g"})
            if len(calls) == 2:
                return httpx.Response(400, json={"code": "INVALID_ARGUMENT", "reason": "KEY_REUSE_MISMATCH"})
            if len(calls) == 3:
                return httpx.Response(307, headers={"Location": "https://foreign/steal"}, json={"code": "REDIRECT"})
            raise httpx.ReadError("credential must not appear", request=request)
        with httpx.Client(transport=httpx.MockTransport(handle), follow_redirects=True) as http:
            c = ModelClient("https://owner", lambda: next(tokens), http_client=http)
            self.assertEqual(c.get("g")["id"], "g")
            with self.assertRaises(APIError) as e:
                c.submit("stable", [{"role":"user","content":"x"}])
            self.assertEqual(e.exception.reason, "KEY_REUSE_MISMATCH")
            with self.assertRaises(APIError):
                c.get("g")
            self.assertEqual(len(calls), 3)
            with self.assertRaises(APIError) as e:
                c.submit("stable", [{"role":"user","content":"x"}])
            self.assertTrue(e.exception.outcome_unknown)
            self.assertNotIn("credential", str(e.exception))
        self.assertEqual([r.headers["Authorization"] for r in calls], ["Bearer " + x for x in ["one", "two", "three", "four"]])

    def test_model_cursor_and_early_eof_are_not_success(self):
        for frames in [[{"type":"accepted","generation_id":"g"}],
                       [{"type":"accepted","generation_id":"g"},{"type":"delta","delta":{"generation_id":"g","sequence":2,"text":"gap"}}]]:
            with self.subTest(frames=frames):
                def handle(request):
                    return httpx.Response(200, headers={"content-type":"application/x-ndjson"},
                                          content=b"\n".join(json.dumps(f).encode() for f in frames))
                with httpx.Client(transport=httpx.MockTransport(handle)) as http:
                    c = ModelClient("https://owner", lambda:"token",http_client=http)
                    with self.assertRaises(APIError):
                        list(c.stream("stable", [{"role":"user","content":"x"}]))

    def test_a2a_partial_stream_is_unknown(self):
        def handle(request):
            request_id=json.loads(request.content)["id"]
            return httpx.Response(200,headers={"content-type":"text/event-stream"}, content=("data: "+json.dumps({"jsonrpc":"2.0","id":request_id,"result":{"task":{"id":"task","status":{"state":"TASK_STATE_WORKING"}}}})+"\n\n").encode())
        with httpx.Client(transport=httpx.MockTransport(handle)) as http:
            c=A2AClient("https://owner",lambda:"token",http_client=http)
            with self.assertRaises(APIError) as e:
                list(c.resubscribe("task"))
            self.assertTrue(e.exception.outcome_unknown)

    def test_bounded_response_and_preflight(self):
        calls=[]
        def handle(request):
            calls.append(request)
            return httpx.Response(200,content=b"x"*((16<<20)+1))
        with httpx.Client(transport=httpx.MockTransport(handle)) as http:
            for origin in ("http://owner", "https://user:pass@owner", "https://owner/x", "https://owner?key=x"):
                with self.assertRaises(ValueError):
                    ModelClient(origin,lambda:"token",http_client=http)
            c=ToolsClient("https://owner",lambda:"token",http_client=http)
            with self.assertRaises(ValueError):c.call("", "echo", {})
            self.assertFalse(calls)
            with self.assertRaises(APIError):c.get("key")
            self.assertEqual(len(calls),1)

    def test_async_cancellation_propagates(self):
        async def run():
            started=asyncio.Event()
            async def handle(request):
                started.set()
                await asyncio.Event().wait()
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                c=AsyncModelClient("https://owner",lambda:"token",http_client=http)
                task=asyncio.create_task(c.submit("stable",[{"role":"user","content":"x"}]))
                await started.wait()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):await task
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
