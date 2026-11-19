"""Tools result contract on the public Python client: categories, no retry, digest.

The owner behind these calls is a recording DOUBLE that answers with the codes of
the TOOLS-RESULT contract (2026-11-09); the real owner leg over PostgreSQL,
Identity and mTLS runs from Go (cmd/ahp/sdk_public_clients_test.go ->
sdk/python/ahp_clients/tests/live_contract.py).
"""
import asyncio
import hashlib
import json

import httpx
import pytest

from ahp_clients import APIError, AsyncToolsClient, ToolsClient, ResultShapeError, fetch_result_bytes, fetch_result_bytes_async

INLINE = {
    "key": "k1", "target": "echo", "status": "SUCCEEDED", "digest": "sha256:aa",
    "result_storage": "inline",
    "provenance": {"schema": "tools-result-v1", "grant_task": "task-1", "grant_context": "ctx-1", "source": "conn-1"},
    "payload": {"content": [{"type": "text", "text": "written"}]},
}


class Recorder:
    def __init__(self, status=200, body=None):
        self.status, self.body, self.calls = status, body if body is not None else INLINE, []

    def handler(self, request):
        self.calls.append({"path": request.url.path, "body": json.loads(request.content),
                           "auth": request.headers.get("authorization")})
        payload = self.body if isinstance(self.body, (dict, list)) else {"message": self.body}
        return httpx.Response(self.status, json=payload)


def client(rec, **kwargs):
    return ToolsClient("https://owner", lambda: "task-token",
                       http_client=httpx.Client(transport=httpx.MockTransport(rec.handler), **kwargs))


def test_successful_result_is_returned_verbatim_over_one_request():
    rec = Recorder()
    result = client(rec).tool_result("k1")
    assert result["result_storage"] == "inline"
    assert result["payload"]["content"][0]["text"] == "written"
    assert result["provenance"]["schema"] == "tools-result-v1"
    assert result["provenance"]["grant_context"] == "ctx-1"
    assert rec.calls == [{"path": "/v1/tools/GetResult", "body": {"key": "k1"}, "auth": "Bearer task-token"}]


@pytest.mark.parametrize("status,code", [(404, "NOT_FOUND"), (403, "FORBIDDEN"), (409, "CONFLICT")])
def test_owner_categories_pass_through_without_retry(status, code):
    # 404: absent OR foreign (existence is not disclosed). 403: no verified context,
    # foreign/missing provenance, another context of the same subject, revoked
    # grant, non-task bearer, reconciled row. 409: pending/uncertain/failed/stale,
    # which means poll Get and never re-send the effect.
    rec = Recorder(status=status, body={"code": code, "message": "refused"})
    with pytest.raises(APIError) as raised:
        client(rec).tool_result("k1")
    assert raised.value.code == code
    assert raised.value.status == status
    # A result read must never be reported as an unknown effect outcome: the
    # effect already settled, and "unknown" would invite a re-send.
    assert raised.value.outcome_unknown is False
    assert len(rec.calls) == 1


@pytest.mark.parametrize("body", [
    {"key": "k", "status": "SUCCEEDED", "result_storage": "inline", "digest": "sha256:aa"},
    {"key": "k", "status": "SUCCEEDED", "result_storage": "inline", "digest": "sha256:aa", "payload": {"a": 1},
     "artifact_ref": {"artifact_id": "a1", "digest": "sha256:aa", "size": 1}},
    {"key": "k", "status": "SUCCEEDED", "result_storage": "artifact", "digest": "sha256:aa"},
    {"key": "k", "status": "SUCCEEDED", "result_storage": "artifact", "digest": "sha256:aa", "payload": {"a": 1},
     "artifact_ref": {"artifact_id": "a1", "digest": "sha256:aa", "size": 1}},
    {"key": "k", "status": "SUCCEEDED", "result_storage": "telepathy", "digest": "sha256:aa"},
])
def test_a_200_that_contradicts_its_own_shape_is_refused(body):
    rec = Recorder(body=body)
    with pytest.raises(ResultShapeError):
        client(rec).tool_result("k")


def test_empty_key_never_reaches_the_owner():
    rec = Recorder()
    with pytest.raises(ValueError):
        client(rec).tool_result("   ")
    assert rec.calls == []


class FakeArtifacts:
    def __init__(self, body, error=None):
        self.body, self.error = body, error

    def download(self, artifact_id):
        assert artifact_id == "a1"
        if self.error:
            raise self.error
        return self.body


def artifact_result(digest, size, record_digest=None):
    return {"key": "k", "status": "SUCCEEDED", "digest": record_digest or digest, "result_storage": "artifact",
            "artifact_ref": {"artifact_id": "a1", "digest": digest, "size": size}}


def test_fetch_result_bytes_verifies_the_declared_digest():
    body = b"written result bytes"
    good = "sha256:" + hashlib.sha256(body).hexdigest()
    assert fetch_result_bytes(FakeArtifacts(body), artifact_result(good, len(body))) == body
    other = "sha256:" + hashlib.sha256(b"different").hexdigest()
    with pytest.raises(ResultShapeError):
        fetch_result_bytes(FakeArtifacts(body), artifact_result(other, len(body)))
    with pytest.raises(ResultShapeError):
        fetch_result_bytes(FakeArtifacts(body), artifact_result(good, len(body) + 1))
    with pytest.raises(ResultShapeError):
        fetch_result_bytes(FakeArtifacts(body), artifact_result("not-hex", len(body)))
    # Owner disagreement between the two digests it published is not resolved here.
    with pytest.raises(ResultShapeError):
        fetch_result_bytes(FakeArtifacts(body), artifact_result(good, len(body), record_digest=other))
    with pytest.raises(RuntimeError):
        fetch_result_bytes(FakeArtifacts(body, error=RuntimeError("owner down")), artifact_result(good, len(body)))
    with pytest.raises(ValueError):
        fetch_result_bytes(FakeArtifacts(body), INLINE)


def test_async_surface_matches_the_sync_one():
    body = b"written result bytes"
    good = "sha256:" + hashlib.sha256(body).hexdigest()

    async def run():
        rec = Recorder()
        async with httpx.AsyncClient(transport=httpx.MockTransport(rec.handler)) as http:
            result = await AsyncToolsClient("https://owner", lambda: "task-token", http_client=http).tool_result("k1")
        assert result["payload"]["content"][0]["text"] == "written"
        assert rec.calls[0]["path"] == "/v1/tools/GetResult"

        refused = Recorder(status=409, body={"code": "CONFLICT", "message": "not SUCCEEDED"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(refused.handler)) as http:
            with pytest.raises(APIError) as raised:
                await AsyncToolsClient("https://owner", lambda: "task-token", http_client=http).tool_result("k1")
        assert raised.value.code == "CONFLICT" and raised.value.outcome_unknown is False

        async def fetch_artifacts(artifacts, result):
            return await fetch_result_bytes_async(artifacts, result)

        class AsyncArtifacts:
            async def download(self, artifact_id):
                assert artifact_id == "a1"
                return body

        assert await fetch_artifacts(AsyncArtifacts(), artifact_result(good, len(body))) == body
        with pytest.raises(ResultShapeError):
            await fetch_artifacts(AsyncArtifacts(), artifact_result("sha256:" + "0" * 64, len(body)))

    asyncio.run(run())
