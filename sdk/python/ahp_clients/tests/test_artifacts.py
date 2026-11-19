"""Adversarial wire tests; positive owner/storage/parser coverage is artifacts_live.py."""
import asyncio
import hashlib
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
from ahp_clients import ArtifactsClient, AsyncArtifactsClient, APIError

DATA=b"abcdef"
REF=dict(id="id",version="v",tenant="t",deployment="d",subject="s",context_id="c",task_id="task",size=len(DATA),sha256=hashlib.sha256(DATA).hexdigest(),labels={})

class ArtifactTests(unittest.TestCase):
    def test_full_and_range_integrity(self):
        cases=[(DATA,200,{},True),(DATA[:-1],200,{},False),(b"corrupt",200,{},False),(DATA+b"extra",200,{},False)]
        for data,status,values,valid in cases:
            with self.subTest(data=data):
                def handle(r):return httpx.Response(200,json=REF) if r.url.path.endswith("Get") else httpx.Response(status,content=data,headers=values)
                with httpx.Client(transport=httpx.MockTransport(handle)) as http:
                    c=ArtifactsClient("https://owner",lambda:"token",http_client=http)
                    if valid:self.assertEqual(c.download("id"),DATA)
                    else:
                        with self.assertRaises(APIError):c.download("id")
        for values,data,valid in [({"Content-Range":"bytes 2-3/6","ETag":'"'+REF["sha256"]+'"'},b"cd",True),({"Content-Range":"bytes 2-3/6","ETag":"wrong"},b"cd",False),({"Content-Range":"bytes 0-1/6","ETag":REF["sha256"]},b"cd",False),({"Content-Range":"bytes 2-3/6","ETag":REF["sha256"]},b"c",False)]:
            def handle(r):return httpx.Response(200,json=REF) if r.url.path.endswith("Get") else httpx.Response(206,content=data,headers=values)
            with httpx.Client(transport=httpx.MockTransport(handle)) as http:
                c=ArtifactsClient("https://owner",lambda:"token",http_client=http)
                if valid:self.assertEqual(c.read_range("id",2,2),b"cd")
                else:
                    with self.assertRaises(APIError):c.read_range("id",2,2)

    def test_bounds_redirect_mutation_and_bad_seal(self):
        for status,body,unknown in [(200,b"x"*((1<<20)+1),True),(503,b"unavailable",True),(400,b'{"code":"INVALID_ARGUMENT","reason":"KEY_REUSE_MISMATCH"}',False),(200,b'{}',True),(307,b'{}',False)]:
            with httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(status,content=body,headers={"Location":"https://trap"})),follow_redirects=True) as http:
                c=ArtifactsClient("https://owner",lambda:"token",http_client=http)
                with self.assertRaises(APIError) as e:c.seal("id")
                self.assertEqual(e.exception.outcome_unknown,unknown)
                if status==400:self.assertEqual(e.exception.reason,"KEY_REUSE_MISMATCH")
        for bad in [dict(REF,id="wrong"),dict(REF,version=""),dict(REF,size=-1),dict(REF,sha256="bad")]:
            with httpx.Client(transport=httpx.MockTransport(lambda r:httpx.Response(200,json=bad))) as http:
                with self.assertRaises(APIError):ArtifactsClient("https://owner",lambda:"token",http_client=http).seal("id")

    def test_presigned_put_is_credential_free_and_does_not_redirect(self):
        seen=[]
        class Handler(BaseHTTPRequestHandler):
            def do_PUT(self):
                seen.append((self.path,dict(self.headers),self.rfile.read(int(self.headers["Content-Length"]))))
                self.send_response(307 if self.path=="/redirect" else 200)
                if self.path=="/redirect":self.send_header("Location","/trap")
                self.end_headers()
            def log_message(self,*args):pass
        srv=ThreadingHTTPServer(("127.0.0.1",0),Handler);thread=threading.Thread(target=srv.serve_forever,daemon=True);thread.start()
        try:
            with httpx.Client(headers={"Authorization":"Bearer owner-secret","Cookie":"owner-cookie"},transport=httpx.MockTransport(lambda r:httpx.Response(200,json=REF))) as http:
                c=ArtifactsClient("https://owner",lambda:"task-secret",http_client=http)
                up=dict(id="id",size=len(DATA),sha256=REF["sha256"],presigned_url=f"http://127.0.0.1:{srv.server_port}/ok")
                c.upload(up,DATA)
                for header in ("Authorization","Cookie","Proxy-Authorization"):
                    self.assertNotIn(header,seen[-1][1])
                    with self.assertRaises(ValueError):c.upload(dict(up,headers={header:"secret"}),DATA)
                with self.assertRaises(ValueError):c.upload(up,b"wrong")
                with self.assertRaises(APIError):c.upload(dict(up,presigned_url=f"http://127.0.0.1:{srv.server_port}/redirect"),DATA)
                self.assertEqual([x[0] for x in seen],["/ok","/redirect"])
                self.assertEqual(seen[0][2],DATA)
        finally:srv.shutdown();srv.server_close();thread.join()

    def test_async_integrity_rotation_and_cancel(self):
        async def run():
            tokens=iter(["first","second"]);seen=[]
            async def token():return next(tokens)
            async def handle(r):
                seen.append(r.headers["Authorization"])
                return httpx.Response(200,json=REF) if r.url.path.endswith("Get") else httpx.Response(200,content=DATA)
            async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
                c=AsyncArtifactsClient("https://owner",token,http_client=http)
                self.assertEqual(await c.download("id"),DATA)
            self.assertEqual(seen,["Bearer first","Bearer second"])
            started=asyncio.Event()
            async def block(r):started.set();await asyncio.Event().wait()
            async with httpx.AsyncClient(transport=httpx.MockTransport(block)) as http:
                c=AsyncArtifactsClient("https://owner",lambda:"token",http_client=http)
                task=asyncio.create_task(c.seal("id"));await started.wait();task.cancel()
                with self.assertRaises(asyncio.CancelledError):await task
        asyncio.run(run())
