"""Conformance tests: verify AHPCheckpointSaver implements the LangGraph interface correctly.

Tests against the actual langgraph-checkpoint==2.1.2 BaseCheckpointSaver contract.
Uses a mock async State API client to verify protocol compliance.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock

from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
from langchain_core.runnables import RunnableConfig

from ahp_state import AHPCheckpointSaver, StateAPIClient, StateAPIError


FENCE = {"run_id": "run-1", "worker": "w-1", "epoch": 7}
THREAD_ID = "th-001"
NAMESPACE = "graph"


def _header(cp_id: str = "cp-1", namespace: str = NAMESPACE, parent: str = "") -> dict:
    """One checkpoint header exactly as ``GetCheckpoint`` returns it.

    The server keeps the SDK envelope in ``metadata`` and returns part
    *references* (never blob bytes), so a fixture that wants a readable header
    must build the envelope the saver itself writes.
    """
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
    from ahp_state import wire

    serde = JsonPlusSerializer()
    body = {
        "v": 1, "id": cp_id, "ts": "2026-01-01",
        "channel_versions": {"messages": "00000000000000000000000000000001.0.1"},
        "versions_seen": {}, "pending_sends": [],
    }
    return {
        "id": cp_id,
        "thread_id": THREAD_ID,
        "namespace": namespace,
        "parent": parent,
        "version": 1,
        "metadata": wire.pack_envelope(serde, body, {"source": "input"}, parent or None),
        "parts": [{"channel": wire.encode_id("messages"),
                   "version": body["channel_versions"]["messages"]}],
        "created_at": "2026-01-01",
    }


def _part_data():
    """The blob of the ``messages`` channel of :func:`_header`."""
    from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
    from ahp_state import wire

    return wire.dump_value(JsonPlusSerializer(), ["hello"])



@pytest.fixture
def mock_client():
    client = AsyncMock(spec=StateAPIClient)
    return client


@pytest.fixture
def saver(mock_client):
    return AHPCheckpointSaver(mock_client, thread_id=THREAD_ID, fence=FENCE)


class TestInheritsBaseCheckpointSaver:
    def test_is_subclass(self, saver):
        assert isinstance(saver, BaseCheckpointSaver)

    def test_has_aget_tuple(self, saver):
        assert hasattr(saver, "aget_tuple")
        assert asyncio.iscoroutinefunction(saver.aget_tuple)

    def test_has_aput(self, saver):
        assert hasattr(saver, "aput")
        assert asyncio.iscoroutinefunction(saver.aput)

    def test_has_aput_writes(self, saver):
        assert hasattr(saver, "aput_writes")
        assert asyncio.iscoroutinefunction(saver.aput_writes)

    def test_has_alist(self, saver):
        assert hasattr(saver, "alist")


class TestAput:
    @pytest.mark.asyncio
    async def test_aput_stores_checkpoint(self, saver, mock_client):
        config: RunnableConfig = {"configurable": {"thread_id": THREAD_ID, "checkpoint_id": ""}}
        checkpoint = {"v": 1, "id": "cp-1", "ts": "2026-01-01", "channel_values": {}, "channel_versions": {}, "versions_seen": {}, "pending_sends": []}
        metadata = {"source": "put"}
        new_versions = {"messages": "v1"}

        result_config = await saver.aput(config, checkpoint, metadata, new_versions)

        mock_client.put_checkpoint.assert_called_once()
        call_args = mock_client.put_checkpoint.call_args
        fence_arg = call_args[0][0]
        cp_arg = call_args[0][1]
        assert fence_arg == FENCE
        assert cp_arg["id"] == "cp-1"
        assert cp_arg["namespace"] == NAMESPACE
        assert result_config["configurable"]["checkpoint_id"] == "cp-1"


class TestAgetTuple:
    @pytest.mark.asyncio
    async def test_aget_tuple_returns_none_when_empty(self, saver, mock_client):
        # The real client raises StateAPIError("NOT_FOUND") for a missing
        # thread/checkpoint; the fake must match that contract.
        mock_client.get_checkpoint.side_effect = StateAPIError("NOT_FOUND", "not found")
        config: RunnableConfig = {"configurable": {"thread_id": THREAD_ID}}
        result = await saver.aget_tuple(config)
        assert result is None

    @pytest.mark.asyncio
    async def test_aget_tuple_returns_checkpoint(self, saver, mock_client):
        mock_client.get_checkpoint.return_value = _header(cp_id="cp-1")
        mock_client.get_part.return_value = {"data": _part_data()}
        mock_client.get_writes.return_value = []

        config: RunnableConfig = {"configurable": {"thread_id": THREAD_ID}}
        result = await saver.aget_tuple(config)

        assert result is not None
        assert isinstance(result, CheckpointTuple)
        assert result.config["configurable"]["checkpoint_id"] == "cp-1"


class TestAputWrites:
    @pytest.mark.asyncio
    async def test_aput_writes_records_pending(self, saver, mock_client):
        config: RunnableConfig = {"configurable": {"thread_id": THREAD_ID, "checkpoint_id": "cp-1"}}
        writes = [("messages", "hello"), ("output", {"x": 1})]
        await saver.aput_writes(config, writes, task_id="task-a")

        mock_client.put_writes.assert_called_once()
        args = mock_client.put_writes.call_args[0]
        assert args[0] == FENCE
        assert args[1] == NAMESPACE
        assert args[2] == "cp-1"
        assert len(args[3]) == 2
        assert args[3][0]["channel"] == "messages"
        assert args[3][0]["task"] == "task-a"

    @pytest.mark.asyncio
    async def test_aput_writes_accepts_task_path(self, saver, mock_client):
        """task_path kwarg from LangGraph 0.6.11 must be accepted."""
        config: RunnableConfig = {"configurable": {"thread_id": THREAD_ID, "checkpoint_id": "cp-1"}}
        await saver.aput_writes(config, [("ch", "val")], task_id="t-1", task_path="sub/path")


class TestAlist:
    @pytest.mark.asyncio
    async def test_alist_yields_tuples(self, saver, mock_client):
        mock_client.list_checkpoints.return_value = [_header(cp_id="cp-2"), _header(cp_id="cp-1")]
        mock_client.get_part.return_value = {"data": _part_data()}
        mock_client.get_writes.return_value = []
        results = []
        async for tuple_ in saver.alist(None):
            results.append(tuple_)
        assert len(results) == 2
        assert results[0].config["configurable"]["checkpoint_id"] == "cp-2"
