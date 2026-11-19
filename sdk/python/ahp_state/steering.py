"""Steering (PDF rev 3.2 §2.3): a user instruction that arrives *during* a turn.

The container reads the thread inbox and folds steering into the graph's state.
*Where* it does that is the whole problem, and the answer comes from the State
contract rather than from LangGraph's convenience API:

**A superstep boundary, never inside a running superstep.** ``aupdate_state``
computes fresh channel versions from the *durable* checkpoint. Called while the
executor is still running the same turn, it produces a sibling branch that
claims those versions a second time, and the State API refuses the second writer
with ``KEY_REUSE_MISMATCH`` (that is the durability check doing its job). So the
container stops pulling the stream at a boundary - with ``durability="sync"`` the
superstep it just reported is already durable, and nothing of the next one has
been written - folds the pending messages as a checkpoint of its own, acks them
under the same fence, and resumes the executor from the durable checkpoint with
``astream(None, ...)``. Only one writer is ever live.

**Never on top of a pending interrupt.** An interrupt is a pending write attached
to the newest checkpoint, and LangGraph derives the task id that owns it from
that checkpoint's id. A side update writes a newer checkpoint, the
``__interrupt__`` write stops belonging to the recomputed task, and the run looks
interrupt-free while the question is still open. Folding therefore happens only
while the turn is executing, or after it ended cleanly - never while paused.

**Exactly-once ack, at-least-once fold.** Each fold is journaled under
``["ahp","steering"]`` with the fenced ``PutKV`` (replay-safe) before the message
is acked, so a restart that lost the ack response does not fold it a second
time. The remaining window is the fold itself: a worker that dies after the
checkpoint commit but before the journal write folds that message once more on
resume. That is deliberate - losing a user instruction silently would be worse
than delivering it twice - and it is stated rather than hidden.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Optional

from langgraph.errors import InvalidUpdateError

from ahp_state.client import StateAPIClient, StateAPIError

#: ``stream_mode="updates"`` reports a pause under this key instead of a node name.
PAUSE_EVENT = "__interrupt__"
#: ... and the graph input under this key; neither is a node that can own an update.
INPUT_EVENT = "__start__"
STEERING_KIND = "steering"

#: Durable per-message "already folded" journal (fenced ``PutKV``, replay-safe).
FOLD_NAMESPACE = ["ahp", "steering"]

#: Keys the coordinator uses for routing, never state.
_CONTROL_KEYS = frozenset({"state", "as_node", "kind", "key", "id", "seq", "text"})


def is_checkpoint_pause(event: Any) -> bool:
    """True for the ``updates`` event that reports a durable interrupt."""
    return isinstance(event, dict) and PAUSE_EVENT in event


def completed_node(event: Any, *, seen: str = "") -> str:
    """The last node whose output is durable, given one ``updates`` event.

    ``aupdate_state`` needs ``as_node`` - the node whose writes this update stands
    in for - and it must be a node that has *already run*. ``__start__`` (the
    graph input) and ``__interrupt__`` (a pause) are not such nodes, so they are
    skipped and the previously seen node is kept.
    """
    if not isinstance(event, dict):
        return seen
    for name in event:
        if name and not name.startswith("__"):
            seen = name
    return seen


def fold_values(message: dict) -> tuple[dict, Optional[str]]:
    """``(state update, as_node)`` for one inbox row.

    ``{"state": {...}}`` is applied verbatim - the coordinator knows the schema.
    A bare mapping is the state itself. Plain text goes to ``messages``, the one
    channel convention every LangGraph graph has; a graph without that channel
    drops it, which the fold event reports.
    """
    body = message.get("payload")
    as_node = body.get("as_node") if isinstance(body, dict) else None
    if isinstance(body, dict):
        if isinstance(body.get("state"), dict):
            return body["state"], as_node
        values = {k: v for k, v in body.items() if k not in _CONTROL_KEYS}
        return values, as_node
    if body is None or body == "":
        return {}, as_node
    return {"messages": [body if isinstance(body, str) else str(body)]}, as_node


@dataclasses.dataclass
class FoldReport:
    """What one boundary fold did - the container reports it, never guesses it."""

    folded: list[str] = dataclasses.field(default_factory=list)
    replayed: list[str] = dataclasses.field(default_factory=list)
    undelivered: list[str] = dataclasses.field(default_factory=list)

    @property
    def changed(self) -> bool:
        return bool(self.folded)


class SteeringFold:
    """One run's view of the thread inbox, folded at safe points only."""

    def __init__(self, client: StateAPIClient, fence: dict, *, thread_id: str,
                 run_id: str, limit: int = 50):
        self._client = client
        self._fence = fence
        self._thread_id = thread_id
        self._run_id = run_id
        self._limit = limit
        self._consumed: set[str] = set()
        self.undelivered: list[str] = []

    # ------------------------------------------------------------------
    # reading
    # ------------------------------------------------------------------

    async def pending(self) -> list[dict]:
        """Steering rows this thread still owes: owed means not yet acked.

        The plane's own query answers with unacknowledged events only
        (``AND e.acked = false`` in ``Store.ReadInbox``), so an acked row is simply
        absent and the flag below is a second look rather than the decision. What
        actually matters here is the envelope: ``ReadInbox`` answers
        ``{"events": [...]}`` (``internal/stateapi/handlers.go``), and reading any
        other name silently owes the thread every steer its user typed.
        """
        resp = await self._client.read_inbox(self._thread_id, after=0, limit=500)
        return [m for m in resp.get("events", [])
                if m.get("kind") == STEERING_KIND and not m.get("acked")
                and m.get("id") not in self._consumed]

    async def take(self) -> list[dict]:
        """Hand the pending rows to a node instead of folding them ourselves.

        A node that consumes steering owns the state write, so the container must
        not fold the same row again (that would double it). The rows are acked at
        the next boundary, once that node's superstep is durable - so a crash
        before the ack redelivers them instead of losing them.
        """
        rows = await self.pending()
        self._consumed.update(m["id"] for m in rows)
        return rows

    async def ack_consumed(self) -> list[str]:
        """Ack rows a node already consumed; safe once their superstep is durable."""
        ids = sorted(self._consumed)
        if not ids:
            return []
        self._consumed.clear()
        await self._client.ack_inbox(self._fence, ids)
        return ids

    # ------------------------------------------------------------------
    # folding
    # ------------------------------------------------------------------

    async def fold(self, graph: Any, config: dict, *, as_node: str) -> FoldReport:
        """Apply every pending row durably, then ack it. Returns what changed.

        ``as_node`` must be a node that already completed; without one there is no
        safe point, so the caller keeps waiting for the next boundary.
        """
        report = FoldReport()
        if not as_node:
            return report
        for message in (await self.pending())[: self._limit]:
            message_id = message.get("id", "")
            if not message_id:
                continue
            values, node_override = fold_values(message)
            if values:
                if await self._is_folded(message_id):
                    report.replayed.append(message_id)
                else:
                    try:
                        await graph.aupdate_state(config, values,
                                                  as_node=node_override or as_node)
                    except InvalidUpdateError:
                        # A coordinator-specified node this graph does not have:
                        # leave the row un-acked and report it, rather than
                        # dropping a user instruction on the floor.
                        self.undelivered.append(message_id)
                        report.undelivered.append(message_id)
                        continue
                    await self._journal(message)
                    report.folded.append(message_id)
            await self._client.ack_inbox(self._fence, [message_id])
        return report

    # ------------------------------------------------------------------
    # the durable "already folded" journal
    # ------------------------------------------------------------------

    def _key(self, message_id: str) -> str:
        return f"{self._run_id}:{message_id}"

    async def _is_folded(self, message_id: str) -> bool:
        try:
            await self._client.get_kv(FOLD_NAMESPACE, self._key(message_id))
            return True
        except StateAPIError as exc:
            if exc.code == "NOT_FOUND":
                return False
            raise

    async def _journal(self, message: dict) -> None:
        key = self._key(message["id"])
        try:
            await self._client.put_kv(self._fence, FOLD_NAMESPACE, key, f"steer:{key}", 0,
                                      {"run_id": self._run_id,
                                       "seq": message.get("seq"),
                                       "state": "applied"})
        except StateAPIError as exc:
            if exc.code in ("CONFLICT", "KEY_REUSE_MISMATCH"):
                return          # journaled by an earlier life of this run
            raise
