"""Isolated per-call transport for synchronous LangGraph adapter methods."""
from __future__ import annotations

import asyncio
import copy


def invoke_sync(adapter, operation):
    """Preserve the caller's ContextVars without sharing a loop-bound pool.

    The shallow adapter view owns only its fresh client; the original adapter
    and async transport are never swapped. asyncio.run copies caller context
    into its task and closes the operation's pool on errors/cancellation too.
    Use async methods in an event-loop thread; a blocking sync method cannot
    receive that event loop's task cancellation while blocking the same loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError("use async AHP methods on an event-loop thread; run sync graph.invoke in a worker thread")

    async def run():
        view = copy.copy(adapter)
        async with adapter._client.fork_for_sync() as client:
            view._client = client
            return await operation(view)

    return asyncio.run(run())
