"""Tests for sync adapter methods and delete_thread.

rev4: sync methods raised NotImplementedError (FAIL@rev4 for real sync tests).
rev5: sync methods are functional (implement BaseCheckpointSaver per PDF §3/§4.2).
"""

from __future__ import annotations

import json
import threading
import pytest
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import Checkpoint
from ahp_state import AHPCheckpointSaver, StateAPIClient

_store: dict[str, Any] = {}


class MockHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        path = self.path.replace("/v1/state/", "")

        if path == "PutCheckpoint":
            cp = body.get("checkpoint", {})
            _store[f"cp:{cp.get('id','')}"] = cp
            self._json({"checkpoint": cp})
        elif path == "GetCheckpoint":
            cid = body.get("id", "")
            if cid:
                cp = _store.get(f"cp:{cid}")
            else:
                latest = [v for k, v in _store.items() if k.startswith("cp:")]
                cp = latest[-1] if latest else None
            self._json({"checkpoint": cp} if cp else {"checkpoint": None})
        elif path == "ListCheckpoints":
            cps = [v for k, v in _store.items() if k.startswith("cp:")]
            self._json({"checkpoints": list(reversed(cps))})
        elif path == "PutWrites":
            _store[f"writes:{body.get('checkpoint_id','')}"] = body.get("writes", [])
            self._json({"ok": True})
        elif path == "GetWrites":
            self._json({"writes": _store.get(f"writes:{body.get('checkpoint_id','')}", [])})
        elif path == "DeleteThread":
            tid = body.get("thread_id", "")
            keys = [k for k in _store if tid in k]
            for k in keys:
                del _store[k]
            self._json({"ok": True})
        else:
            self._json({"ok": True})

    def _json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="module")
def server():
    srv = HTTPServer(("127.0.0.1", 0), MockHandler)
    port = srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{port}"
    srv.shutdown()


@pytest.fixture(autouse=True)
def clear():
    _store.clear()
    yield
    _store.clear()


@pytest.fixture
def saver(server):
    client = StateAPIClient(server, token="tok")
    fence = {"run_id": "r1", "worker": "w1", "epoch": 1}
    return AHPCheckpointSaver(client, thread_id="thr-1", fence=fence)


class TestSyncMethodsWork:
    """Sync methods must be functional (not NotImplementedError). rev4 FAILS here."""

    def test_get_tuple_sync(self, saver):
        """Sync get_tuple returns None for empty (works without async)."""
        config: RunnableConfig = {"configurable": {"thread_id": "thr-1"}}
        result = saver.get_tuple(config)
        assert result is None

    def test_put_sync(self, saver):
        """Sync put stores a checkpoint and returns config."""
        config: RunnableConfig = {"configurable": {"thread_id": "thr-1"}}
        cp: Checkpoint = {
            "v": 1, "id": "cp-sync-1", "ts": "2026-01-01",
            "channel_values": {"data": "hello"},
            "channel_versions": {"data": 1},
            "versions_seen": {},
            "pending_sends": [],
        }
        result_config = saver.put(config, cp, {}, {"data": 1})
        assert result_config["configurable"]["checkpoint_id"] == "cp-sync-1"
        # Verify stored
        assert "cp:cp-sync-1" in _store

    def test_put_writes_sync(self, saver):
        """Sync put_writes records pending writes."""
        config: RunnableConfig = {"configurable": {"thread_id": "thr-1", "checkpoint_id": "cp-w"}}
        saver.put_writes(config, [("messages", "sync_val")], task_id="node-sync")
        assert "writes:cp-w" in _store
        assert _store["writes:cp-w"][0]["channel"] == "messages"

    def test_list_sync(self, saver):
        """Sync list returns iterator of checkpoint tuples."""
        config: RunnableConfig = {"configurable": {"thread_id": "thr-1", "checkpoint_id": ""}}
        cp1: Checkpoint = {
            "v": 1, "id": "cp-list-1", "ts": "2026-01-01",
            "channel_values": {}, "channel_versions": {}, "versions_seen": {}, "pending_sends": [],
        }
        cp2: Checkpoint = {
            "v": 1, "id": "cp-list-2", "ts": "2026-01-02",
            "channel_values": {}, "channel_versions": {}, "versions_seen": {}, "pending_sends": [],
        }
        saver.put(config, cp1, {}, {})
        saver.put(config, cp2, {}, {})
        results = list(saver.list(None))
        assert len(results) == 2
        assert results[0].config["configurable"]["checkpoint_id"] == "cp-list-2"

    def test_delete_thread_sync(self, server):
        """Sync delete_thread removes the thread."""
        _store["thread:del-thread"] = {"id": "del-thread"}
        client = StateAPIClient(server, token="tok")
        fence = {"run_id": "r1", "worker": "w1", "epoch": 1}
        saver = AHPCheckpointSaver(client, thread_id="del-thread", fence=fence)
        saver.delete_thread("del-thread")
        assert "thread:del-thread" not in _store

    def test_get_tuple_sync_returns_data(self, saver):
        """Sync get_tuple returns stored checkpoint data."""
        config: RunnableConfig = {"configurable": {"thread_id": "thr-1", "checkpoint_id": ""}}
        cp: Checkpoint = {
            "v": 1, "id": "cp-read-1", "ts": "2026-01-01",
            "channel_values": {"x": 42},
            "channel_versions": {"x": 1},
            "versions_seen": {},
            "pending_sends": [],
        }
        # A channel value only becomes durable when its version is declared in
        # new_versions - that is LangGraph's own contract (its InMemorySaver
        # loses an undeclared value exactly the same way).
        saver.put(config, cp, {}, {"x": 1})
        # Read back with sync get_tuple
        read_config: RunnableConfig = {"configurable": {"thread_id": "thr-1", "checkpoint_id": "cp-read-1"}}
        tup = saver.get_tuple(read_config)
        assert tup is not None
        assert tup.checkpoint["channel_values"]["x"] == 42


class TestDeleteThread:
    """delete_thread sends DeleteThread call and succeeds."""

    @pytest.mark.asyncio
    async def test_delete_thread_calls_api(self, server):
        _store["thread:thr-del"] = {"id": "thr-del"}
        client = StateAPIClient(server, token="tok")
        await client.delete_thread("thr-del")
        assert "thread:thr-del" not in _store

    @pytest.mark.asyncio
    async def test_delete_nonexistent_thread_no_error(self, server):
        client = StateAPIClient(server, token="tok")
        await client.delete_thread("nonexistent")
