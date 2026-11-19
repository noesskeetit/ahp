"""Shared Memory is an explicit API profile, with independent native clients."""
import asyncio
import json
import unittest

import httpx
from ahp_clients import APIError, SharedMemoryClient, AsyncSharedMemoryClient


class SharedMemoryTests(unittest.TestCase):
    def test_operation_receipts_sync_and_async(self):
        paths = []
        def handle(request):
            paths.append(request.url.path)
            self.assertEqual(json.loads(request.content), {"op_key": "closed"})
            return httpx.Response(200, json={"op_key": "closed", "status": "FENCED", "result_count": 0})
        with httpx.Client(transport=httpx.MockTransport(handle)) as http:
            c = SharedMemoryClient("https://owner", lambda: "task", http_client=http)
            self.assertEqual(c.fence("closed")["status"], "FENCED")
            self.assertEqual(c.get_operation("closed")["op_key"], "closed")
            with self.assertRaises(ValueError):
                c.fence("")
        async def run():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                c = AsyncSharedMemoryClient("https://owner", lambda: "task", http_client=http)
                self.assertEqual((await c.fence("closed"))["status"], "FENCED")
                self.assertEqual((await c.get_operation("closed"))["op_key"], "closed")
        asyncio.run(run())
        self.assertEqual(paths, ["/v1/memory/shared/Fence", "/v1/memory/shared/Get"]*2)

    def test_replay_and_current_token_use_shared_owner_only(self):
        calls = []
        tokens = iter(["one", "two"])
        def handle(request):
            calls.append(request)
            body = json.loads(request.content)
            self.assertEqual(request.url.path, "/v1/memory/shared/Propose")
            self.assertEqual(body["op_key"], "stable")
            self.assertEqual(body["workspace"], "workspace-id")
            self.assertFalse(set(body) & {"tenant", "deployment", "subject", "scope"})
            return httpx.Response(200, json={"memory": {"id": "m", "state": "proposed"}, "replay": len(calls) == 2})
        with httpx.Client(transport=httpx.MockTransport(handle)) as http:
            c = SharedMemoryClient("https://owner", lambda: next(tokens), http_client=http)
            request = {"kind": "workspace", "workspace": "workspace-id", "text": "fact"}
            self.assertFalse(c.propose("stable", request)["replay"])
            self.assertTrue(c.propose("stable", request)["replay"])
            self.assertNotIn("op_key", request)
        self.assertEqual([r.headers["authorization"] for r in calls], ["Bearer one", "Bearer two"])

    def test_lost_reply_single_attempt_and_preflight(self):
        calls = []
        def handle(request):
            calls.append(request)
            raise httpx.ReadError("sensitive transport detail", request=request)
        with httpx.Client(transport=httpx.MockTransport(handle)) as http:
            c = SharedMemoryClient("https://owner", lambda: "task", http_client=http)
            with self.assertRaises(APIError) as error:
                c.propose("stable", {"kind": "user", "text": "fact"})
            self.assertTrue(error.exception.outcome_unknown)
            self.assertEqual(len(calls), 1)
            with self.assertRaises(ValueError):
                c.propose("", {"kind": "user", "text": "fact"})
            with self.assertRaises(ValueError):
                c.propose("stable", {"kind": "user", "text": "fact", "op_key": "different"})
            self.assertEqual(len(calls), 1)

    def test_async_review_read_and_search(self):
        async def run():
            paths = []
            def handle(request):
                paths.append(request.url.path)
                return httpx.Response(200, json={"hits": [], "mode": "hybrid"} if paths[-1].endswith("Search") else {"id": "m", "state": "active"})
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                c = AsyncSharedMemoryClient("https://owner", lambda: "task", http_client=http)
                self.assertEqual((await c.review("approve", "m", "approve"))["state"], "active")
                self.assertEqual((await c.read("m"))["id"], "m")
                self.assertEqual((await c.search({"kind": "workspace", "query": "query"}))["mode"], "hybrid")
            self.assertEqual(paths, ["/v1/memory/shared/Review", "/v1/memory/shared/Read", "/v1/memory/shared/Search"])
        asyncio.run(run())
