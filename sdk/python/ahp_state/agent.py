"""The user's agent container on a custom LangGraph graph (PDF rev 3.2 §2.3, §4.2).

This is the platform contract for a **custom** agent: the user owns the graph -
their own ``StateGraph``, their own nodes, their own model/tool orchestration -
and this module owns the durable relationship between that graph and the AHP
platform. It is deliberately *not* a wrapper around the platform's internal Go
runner: nothing here asks a built-in engine to execute a turn. LangGraph's own
Pregel executor runs the user's graph inside this process, and every durable
effect of one super-step goes to the State API before the next step starts.

What one run looks like
-----------------------
1. ``ClaimRun`` hands out a run with a lease ``(run_id, worker, epoch)`` and a
   task token. The token - not a request body - is what scopes every call.
2. The graph is compiled by the user's builder with :class:`AHPCheckpointSaver`
   and :class:`AHPStore` bound to that lease, and executed with
   ``durability="sync"`` (PDF §4.2 "durability=sync"): a super-step is durable
   before the next one starts, so a pod that dies mid-turn loses nothing.
3. Side effects go through :class:`Effects`, which journals every call with
   :class:`IntentRecorder` under ``(run_id, node, tool_call_id)``. After a
   restart a finished effect replays its stored result; an ambiguous one raises
   :class:`SideEffectUnknown` and the run suspends for reconciliation. Nothing is
   ever executed twice by the restart itself.
4. ``interrupt()`` inside a node → the checkpoint holds the interrupt as a
   pending write, and the run is ``Suspend``-ed on ``wait_for="hitl_response"`` -
   the vocabulary the platform actually answers. The question is put in the
   thread inbox as a ``hitl_request`` event (the same event Space's own gate
   writes), and the answer comes back as a ``hitl_response`` event, which is what
   ``POST /v1/space/RespondHITL`` enqueues. State wakes THIS run - same id, new
   epoch - and the woken run reads the answer from the inbox and resumes with
   ``Command(resume=...)`` from that exact checkpoint. Parking on a ``wait_for``
   of any other name parks the run forever, because no answer path emits it.
5. Steering: at a super-step boundary the container parks the executor, drains
   the thread inbox (``ReadInbox``) and folds each message in as a durable state
   update (``aupdate_state``), journals the fold with ``PutKV`` and then
   ``AckInbox``-es under the same fence - so the turn is never executed with two
   concurrent writers, and never on top of a pending interrupt. A node may take
   ownership of steering itself with ``ctx.take_steering()``. See
   :mod:`ahp_state.steering`.
6. ``Heartbeat`` runs while the turn is executing; a lost lease
   (``STALE_EPOCH``) stops the graph immediately and abandons further writes
   rather than writing as a zombie.

Run statuses are explicit: ``done``, ``interrupted``, ``awaiting_reconciliation``,
``lease_lost``, ``noop``. A turn never "succeeds" by swallowing an error.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import uuid
from typing import Any, Awaitable, Callable, Optional

import httpx
from langgraph.errors import EmptyInputError
from langgraph.types import Command

from ahp_state.checkpoint_saver import AHPCheckpointSaver
from ahp_state.client import StateAPIClient, StateAPIError
from ahp_state.intents import IntentRecorder, SideEffectUnknown
from ahp_state.store import AHPStore
from ahp_state.steering import FoldReport, SteeringFold, completed_node, is_checkpoint_pause
from ahp_state.transport import mtls_client

log = logging.getLogger("ahp_state.agent")

#: The codes that mean "this worker no longer owns the run".
#:
#: ``STALE_EPOCH`` is the only one a live State was measured to answer with: a
#: released, expired, taken-over or wrong-worker lease all answer
#: ``409 STALE_EPOCH: lease epoch is stale or the lease has expired``
#: (``internal/state/store.go:1140``). ``LEASE_LOST`` is kept only as a
#: tolerated alias - the seam contract
#: (``docs/space-runner-seam-contract-2026-11-01.md`` §5) lists exactly six
#: codes and it is not among them. ``PERMISSION_DENIED`` is deliberately NOT
#: here: it does not exist on this wire either, and reading an authorization
#: fault as "someone else took my lease" would turn a tenant misconfiguration
#: into a silent takeover story. ``FORBIDDEN`` stays a hard error for the same
#: reason.
LEASE_LOST_CODES = ("STALE_EPOCH", "LEASE_LOST")

#: The causal frame name of the platform. Space answers "why did this turn fail"
#: from the thread-stream frame whose kind is ``"error"`` carrying ``{"error":
#: "<text>"}`` (``StepError`` in internal/space/types.go, read by ``stepErrorCause``
#: and by the cold restorer ``Coordinator.transcriptCause``). Any other kind is
#: delivered verbatim but is NOT a cause: a reader that trusted an arbitrary kind
#: would let any worker name another worker's failure. Agreed with vm2-state-runner
#: (answers to the five questions of 20261101T1351Z §4, post 20261101T135717Z v2),
#: question 4: "контракт имени за Space".
CAUSE_KIND = "error"

#: What this framework calls the same event, kept inside the payload as a detail.
CAUSE_DETAIL = "agent_failed"

#: Bounds of the stream scan that chooses a durable step key, deliberately equal to
#: the bounds of ``space.Coordinator.stepSeqHighWater`` (internal/space/
#: coordinator.go): one bounded pass, and no answer rather than a wrong one.
_STREAM_PAGE = 500
_STREAM_SCAN_CAP = 200_000

#: ``async def tool(name: str, args: dict) -> Any`` - in production the Tool &
#: Credential Gateway (block 6). The agent container never holds credentials.
EffectTransport = Callable[[str, dict], Awaitable[Any]]

#: ``def build(ctx: RunContext) -> CompiledStateGraph`` - the user's graph,
#: compiled with ``ctx.saver`` / ``ctx.store``.
GraphBuilder = Callable[["RunContext"], Any]


class LeaseLost(RuntimeError):
    """This worker no longer owns the run; it must stop writing immediately."""


def _langgraph_task_id() -> str | None:
    """Task instance identity from the pinned LangGraph 0.6.11 runtime config."""
    from langgraph.config import get_config
    try:
        config = get_config()
    except RuntimeError:
        return None  # explicit/manual use outside a LangGraph runnable
    configurable = config.get("configurable", {})
    task_id = configurable.get("__pregel_task_id")
    if task_id is None:
        if "__pregel_scratchpad" in configurable:
            raise ValueError("LangGraph task config has no stable task_id")
        return None
    if not isinstance(task_id, str) or not task_id:
        raise ValueError("LangGraph task_id must be a nonempty string")
    return task_id


class Effects:
    """Intent identity: (run, LangGraph task_id, persisted tool_call_id/index).

    Model output must be checkpointed before a mutating node executes. A model
    tool_call_id is stable only when read from that durable output. Without one,
    bind a fresh view at each node entry for positional effect IDs.

    Outside a LangGraph task, pass task_id explicitly. Legacy low-level callers
    without it retain their node scope; this mode does not provide graph-loop
    identity automatically and must not be advertised as the LangGraph adapter.
    """

    def __init__(self, client: StateAPIClient, fence: dict, transport: Optional[EffectTransport],
                 *, node: str = "agent", task_id: str | None = None):
        if task_id is not None and (not isinstance(task_id, str) or not task_id):
            raise ValueError("task_id must be a nonempty string")
        self._client = client
        self._fence = fence
        self._transport = transport
        self.node = node  # logical name for callers; not a graph instance ID
        self._legacy_nodes = (node,)
        self._task_id = task_id
        self._bound_task_id: str | None = None
        self._seq = 0
        self.last_call_id: Optional[str] = None
        self.last_intent_node: Optional[str] = None

    def bind(self, node: str, *, task_id: str | None = None) -> "Effects":
        """Fresh positional view, created inside EACH node invocation/retry."""
        view = Effects(self._client, self._fence, self._transport, node=node,
                       task_id=task_id if task_id is not None else self._task_id)
        view._bound_task_id = _langgraph_task_id()
        view._legacy_nodes = tuple(dict.fromkeys((node, *self._legacy_nodes)))
        return view

    def _intent_node(self) -> str:
        current = _langgraph_task_id()
        if current:
            if self._task_id is not None and self._task_id != current:
                raise ValueError("explicit task_id does not match current LangGraph task")
            if self._bound_task_id is not None and self._bound_task_id != current:
                raise ValueError("Effects view reused across LangGraph tasks; bind inside each node")
            return current
        return self._task_id or self.node

    def next_call_id(self) -> str:
        """Position in this node invocation, never a process-wide sequence."""
        current = _langgraph_task_id()
        if current and self._bound_task_id != current:
            raise ValueError("positional graph effects require bind() inside the node or an explicit persisted call_id")
        call_id = f"effect-{self._seq}"
        self._seq += 1
        return call_id

    async def call(self, tool: str, args: dict | None = None,
                   *, call_id: Optional[str] = None) -> Any:
        """Execute/replay one durable effect without hiding UNKNOWN outcomes.

        Use call_id from checkpointed model output, or a fresh bind() view's
        index. Re-minting a call_id inside the mutation node creates a different
        operation and is a developer contract violation. Same task/call with
        different arguments fails; a new task may reuse call_0 legitimately.
        """
        if self._transport is None:
            raise RuntimeError("this agent has no effect transport configured")
        if call_id is not None and (not isinstance(call_id, str) or not call_id):
            raise ValueError("call_id must be a nonempty persisted identifier")
        node = self._intent_node()
        recorder = IntentRecorder(self._client, self._fence, node=node)
        payload = args or {}
        self.last_call_id = call_id if call_id else self.next_call_id()
        self.last_intent_node = node
        if _langgraph_task_id() is not None:
            # A changed image must not create a fresh task-scoped operation for
            # a legacy run that may already have touched the outside world.
            # Mapping a logical name to one task/iteration is not provable here.
            for legacy_node in self._legacy_nodes:
                if legacy_node == node:
                    continue
                legacy = await IntentRecorder(self._client, self._fence, node=legacy_node).get(self.last_call_id)
                if legacy is not None:
                    raise StateAPIError("INTENT_IDENTITY_MIGRATION_REQUIRED",
                                        "legacy logical-node intent exists; keep the pinned image or explicitly reconcile the run")
        return await recorder.guarded(
            self.last_call_id,
            lambda: self._transport(tool, payload),
            kind="tool", target=tool, args=payload)

    async def status(self, call_id: str) -> Optional[dict]:
        """Read an intent in this task scope; outside the graph supply task_id."""
        return await IntentRecorder(self._client, self._fence, node=self._intent_node()).get(call_id)


@dataclasses.dataclass
class RunContext:
    """Everything one run may touch, already scoped to its lease."""

    client: StateAPIClient
    fence: dict
    thread_id: str
    run_id: str
    saver: AHPCheckpointSaver
    store: AHPStore
    effects: Effects
    steering_fold: SteeringFold
    input: Any = None
    resume: Any = None
    payload: dict = dataclasses.field(default_factory=dict)

    async def steering(self) -> list[dict]:
        """Pending steering rows, without taking responsibility for them.

        The container folds these into the state at the next superstep boundary,
        so a node that only peeks must not apply them itself as well.
        """
        return await self.steering_fold.pending()

    async def take_steering(self) -> list[dict]:
        """Consume the pending rows here: this node owns the state write.

        The container then leaves them alone and acks them once this node's
        superstep is durable - applied once, acked once, never lost.
        """
        return await self.steering_fold.take()

    async def memory(self, node: str, call_id: str):
        """A durable-identity scope for KV writes made by one tool call."""
        return self.store.with_identity(node=node, call=call_id)


@dataclasses.dataclass
class RunOutcome:
    status: str = "running"           # done|interrupted|awaiting_reconciliation|lease_lost|noop
    run_id: str = ""
    thread_id: str = ""
    result: Any = None
    interrupts: list[dict] = dataclasses.field(default_factory=list)
    wait_for: str = ""
    error: str = ""
    folded_steering: list[str] = dataclasses.field(default_factory=list)
    undelivered_steering: list[str] = dataclasses.field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status == "done"


def _validate_graph_contract(graph, ctx: RunContext) -> None:
    """Only this claimed run's saver/store may write on behalf of the graph."""
    if getattr(graph, "checkpointer", None) is not ctx.saver:
        raise ValueError("graph must use this run's ctx.saver as its checkpointer")
    store = getattr(graph, "store", None)
    if store is not None and store is not ctx.store:
        raise ValueError("graph must use this run's ctx.store when a store is configured")


class AgentService:
    """Hosts a user-supplied LangGraph graph as an AHP agent container."""

    def __init__(
        self,
        *,
        graph: GraphBuilder,
        base_url: str = "",
        worker: str = "",
        token: str | None = None,
        client: StateAPIClient | None = None,
        http_client: httpx.AsyncClient | None = None,
        effects: EffectTransport | None = None,
        effect_node: str = "agent",
        namespace: str = "graph",
        lease_ttl_ms: int = 120_000,
        heartbeat_ms: int = 30_000,
        steering_limit: int = 50,
    ):
        if graph is None:
            raise ValueError("AgentService requires the user's graph builder")
        if client is None and not base_url:
            raise ValueError("AgentService needs either a client or base_url")
        self._build_graph = graph
        self._worker = worker or f"ahp-agent-{os.getpid()}"
        self._token = token
        # An injected client knows its own origin and its own pool. Both are
        # needed to build the per-run task client later; without them every
        # fenced call of a container built from a handshake file went out under
        # the client it was constructed with - the stand's bootstrap credential
        # the client it was constructed with - the stand's bootstrap credential.
        # Measured against a live stand: that credential is scope-wide, so a run
        # that should have been confined to its own task token was not.
        self._base_url = (base_url or getattr(client, "base_url", "") or "").rstrip("/")
        self._injected = client
        self._http = http_client or getattr(client, "http_client", None)
        # One connection pool for the life of the service: the per-run task client
        # shares it, so a container that serves a thousand turns opens one pool,
        # not a thousand. A caller-supplied pool is left alone.
        self._owns_http = http_client is None and client is None and bool(self._base_url)
        if self._owns_http:
            self._http = httpx.AsyncClient(timeout=30.0)
        self._client = client or StateAPIClient(self._base_url, token=token,
                                                http_client=self._http)
        self._effects = effects
        self._effect_node = effect_node
        self._namespace = namespace
        self._lease_ttl_ms = lease_ttl_ms
        self._heartbeat_ms = heartbeat_ms
        self._steering_limit = steering_limit

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Release only what this service created; an injected client stays the
        caller's to close."""
        if self._injected is None and isinstance(self._client, StateAPIClient):
            await self._client.close()
        if self._owns_http and self._http is not None:
            await self._http.aclose()

    async def serve(self, *, max_turns: int | None = None, idle_wait_s: float = 1.0,
                    stop: asyncio.Event | None = None) -> list[RunOutcome]:
        """Claim and execute runs until stopped (the container's main loop)."""
        outcomes: list[RunOutcome] = []
        turns = 0
        while stop is None or not stop.is_set():
            outcome = await self.run_once()
            if outcome.status != "noop":
                outcomes.append(outcome)
                turns += 1
                if max_turns is not None and turns >= max_turns:
                    break
            else:
                await asyncio.sleep(idle_wait_s)
        return outcomes

    async def run_once(self, *, run_id: str | None = None) -> RunOutcome:
        """Claim one run (or take ``run_id``) and execute it to a terminal status."""
        try:
            claim = await self._claim(run_id)
        except StateAPIError as exc:
            if exc.code == "NOT_FOUND":
                return RunOutcome(status="noop", error=exc.code)
            raise
        if claim is None:
            return RunOutcome(status="noop")

        fence, thread_id, rid, payload, token = claim
        outcome = RunOutcome(run_id=rid, thread_id=thread_id)
        return await self._guarded_turn(fence, thread_id, payload, outcome, token)

    async def run_leased(self, fence: dict, token: str | None = None, *,
                         thread_id: str = "",
                         payload: dict | None = None) -> RunOutcome:
        """Execute a run this worker **already holds** - a takeover, not a claim.

        A restarted pod gets the lease from its supervisor: the run left ``queued``
        long ago, so ``ClaimRun`` has nothing to hand out and the only way to make
        progress is to drive the lease we were given. The turn is *continued* from
        the durable checkpoint (``payload`` defaults to a continuation) - feeding
        the original input in again is precisely the bug this path exists to avoid.
        Effects keep the identity ``(run_id, node, call)``, and the run id is
        unchanged here, which is what lets a finished effect replay instead of
        firing twice.
        """
        run_id = fence.get("run_id", "")
        if not run_id:
            raise ValueError("an adopted lease needs its run_id in the fence")
        thread = thread_id or fence.get("thread_id", "")
        if not thread:
            raise ValueError("an adopted lease needs the thread it owns")
        outcome = RunOutcome(run_id=run_id, thread_id=thread)
        body = payload if payload is not None else {"continue": True}
        return await self._guarded_turn(dict(fence), thread, body, outcome, token)

    async def _guarded_turn(self, fence: dict, thread_id: str, payload: dict,
                            outcome: RunOutcome, token: str | None) -> RunOutcome:
        """One turn, abandoned the moment the lease is gone."""
        heartbeats = asyncio.Event()
        driver = asyncio.create_task(
            self._drive(fence, thread_id, payload, outcome, token))
        keeper = asyncio.create_task(self._heartbeat(fence, heartbeats))
        try:
            done, pending = await asyncio.wait({driver, keeper},
                                               return_when=asyncio.FIRST_COMPLETED)
            if keeper in done and driver in pending:
                try:
                    keeper.result()                 # unexpected keeper failure surfaces
                except LeaseLost:
                    pass
                driver.cancel()
                try:
                    await driver
                except asyncio.CancelledError:
                    pass
                outcome.status = "lease_lost"
                outcome.error = "lease lost during the turn"
                return outcome
            heartbeats.set()
            await keeper
            return driver.result()
        finally:
            heartbeats.set()
            for task in (driver, keeper):
                if not task.done():
                    task.cancel()

    # ------------------------------------------------------------------
    # one turn
    # ------------------------------------------------------------------

    async def _drive(self, fence: dict, thread_id: str, payload: dict,
                     outcome: RunOutcome, token: str | None = None) -> RunOutcome:
        client = self._task_client(token)
        saver = AHPCheckpointSaver(client, thread_id, fence, namespace=self._namespace)
        store = AHPStore(client, fence)
        effects = Effects(client, fence, self._effects, node=self._effect_node)
        fold = SteeringFold(client, fence, thread_id=thread_id,
                            run_id=fence["run_id"], limit=self._steering_limit)
        ctx = RunContext(client=client, fence=fence, thread_id=thread_id, run_id=fence["run_id"],
                         saver=saver, store=store, effects=effects, steering_fold=fold,
                         input=payload.get("input"), resume=payload.get("resume"),
                         payload=payload)

        graph = self._build_graph(ctx)
        if graph is None:
            raise ValueError("the user's graph builder returned no graph")
        _validate_graph_contract(graph, ctx)
        # NOTE: ``checkpoint_ns`` is deliberately NOT set. For LangGraph it names
        # the *current subgraph*, so pinning it here would make the executor look
        # for a subgraph called "graph". The root namespace stays empty, and
        # AHPCheckpointSaver maps the empty namespace onto this lease's AHP
        # checkpoint namespace.
        config = {"configurable": {"thread_id": thread_id}}

        started = await self._journal_turn(client, fence, payload)
        if started and payload.get("resume") is None:
            # A run that already started and is claimed again is either a restart
            # or an answered interrupt: State wakes the SAME run when an inbox
            # event's kind equals the ``wait_for`` it was parked on. In the second
            # case the answer is that event and not a new payload, so take it from
            # the inbox before deciding what to hand LangGraph.
            payload = await self._hitl_answer(client, fence, thread_id, payload)
        graph_input = self._graph_input(payload, started)

        try:
            folds: list[FoldReport] = []
            paused, last_node = await self._run_graph(graph, config, ctx, graph_input, folds)
        except EmptyInputError:
            if graph_input is not None:
                raise
            # The worker died after journaling the turn but before writing any
            # checkpoint: nothing durable exists to continue, and no effect could
            # have run, so feeding the original input is the correct restart.
            retry_input = payload.get("input")
            if retry_input is None:
                # Nothing was ever checkpointed *and* this lease carries no input
                # to apply: there is nothing to continue and nothing to start.
                # Say so where the platform can see it instead of reporting a turn
                # that never happened.
                outcome.status = "failed"
                outcome.error = "no durable checkpoint and no input to apply"
                await self._cause_frame(client, fence, thread_id, outcome.error)
                await self._release(client, fence, "failed", close_reason="error",
                                    failure_cause=outcome.error)
                return outcome
            folds = []
            paused, last_node = await self._run_graph(graph, config, ctx,
                                                      retry_input, folds)
        except SideEffectUnknown as exc:
            outcome.status = "awaiting_reconciliation"
            outcome.error = str(exc)
            outcome.wait_for = f"reconcile:{exc.intent.get('call', '')}"
            await self._stream_event(client, fence, "agent_awaiting_reconciliation",
                                     {"run_id": fence["run_id"],
                                      "node": exc.intent.get("node"),
                                      "call": exc.intent.get("call"),
                                      "digest": exc.intent.get("digest")},
                                     key=f"agent:{fence['run_id']}:reconcile:{exc.intent['id']}")
            # the stream event goes first: Suspend releases the lease, and a
            # fenced AppendStream after it is a call that cannot succeed
            await self._suspend(client, fence, outcome.wait_for)
            return outcome
        except LeaseLost as exc:
            outcome.status = "lease_lost"
            outcome.error = exc.code
            return outcome
        except StateAPIError as exc:
            if exc.code in LEASE_LOST_CODES:
                outcome.status = "lease_lost"
                outcome.error = exc.code
                return outcome
            outcome.status = "failed"
            outcome.error = f"{exc.code}: {exc.message}"
            # journaled before the release: after it this lease cannot write, and
            # a turn that ends failed has to leave its reason somewhere the
            # platform can answer with - not only in this process's log.
            await self._cause_frame(client, fence, thread_id, outcome.error)
            await self._release(client, fence, "failed", close_reason="error",
                                failure_cause=outcome.error)
            return outcome

        state = await graph.aget_state(config)
        interrupts = _interrupts_of(state)
        if interrupts:
            # A pending interrupt is a pending write on the newest checkpoint and
            # LangGraph derives the owning task id from that checkpoint's id, so
            # folding steering here would make the open question invisible. The
            # rows stay owed and are folded when the turn resumes.
            outcome.status = "interrupted"
            outcome.interrupts = interrupts
            outcome.wait_for = self.HITL_WAIT_FOR
            await self._stream_event(client, fence, "agent_input_required",
                                     {"interrupts": interrupts,
                                      "steering_owed": [m["id"]
                                                        for m in await ctx.steering()]})
            await self._hitl_gate(client, fence, thread_id, interrupts)
            # Parked on the name the answer path emits, so RespondHITL can wake
            # THIS run; the question and the "asked" journal are already durable.
            await self._suspend(client, fence, outcome.wait_for)
            # After the park, never before it: the repair is only meaningful once
            # the park is durable, because that is the moment an answer can no
            # longer wake anything by itself (see _hitl_rearm).
            await self._hitl_rearm(client, fence, thread_id)
            return outcome

        if not paused:
            # The turn ended cleanly and no other writer is live: whatever
            # steering is still owed is folded now rather than left unread.
            end = await ctx.steering_fold.fold(graph, config, as_node=last_node)
            if end.changed:
                folds.append(end)
                state = await graph.aget_state(config)
        for report in folds:
            outcome.folded_steering.extend(report.folded)
            outcome.undelivered_steering.extend(report.undelivered)
        outcome.undelivered_steering.extend(ctx.steering_fold.undelivered)

        outcome.status = "done"
        outcome.result = state.values if state else None
        await self._stream_event(client, fence, "agent_completed",
                                 {"state": "synced",
                                  "steering_folded": outcome.folded_steering})
        await self._release(client, fence, "done")
        return outcome

    # ------------------------------------------------------------------
    # executing the user's graph, with steering folded at safe points
    # ------------------------------------------------------------------

    async def _run_graph(self, graph, config: dict, ctx: RunContext,
                         graph_input: Any,
                         folds: list[FoldReport]) -> tuple[bool, str]:
        """Run the turn to a pause, a clean end, or an error.

        Returns ``(paused, last_completed_node)``. Steering is folded only where
        the State contract allows a second writer: after a superstep became
        durable and before the next one started, with the executor parked. See
        :mod:`ahp_state.steering`.
        """
        stream_input = graph_input
        last_node = ""
        fold_blocked = False
        first_pass = True
        while True:
            owed_now = False
            agen = graph.astream(stream_input, config, stream_mode="updates",
                                 durability="sync")
            try:
                async for event in agen:
                    if is_checkpoint_pause(event):
                        return True, last_node
                    last_node = completed_node(event, seen=last_node)
                    # rows a node took with ctx.take_steering() are durable now
                    await ctx.steering_fold.ack_consumed()
                    if not fold_blocked and last_node and await ctx.steering():
                        owed_now = True
                        break
            except EmptyInputError:
                if first_pass:
                    raise
                raise StateAPIError(
                    "CONFLICT", "the durable checkpoint vanished mid-turn") from None
            finally:
                await agen.aclose()
            first_pass = False
            if not owed_now:
                return False, last_node

            report = await ctx.steering_fold.fold(graph, config, as_node=last_node)
            folds.append(report)
            await self._stream_event(ctx.client, ctx.fence, "agent_steering_applied",
                                     {"folded": report.folded,
                                      "replayed": report.replayed,
                                      "undelivered": report.undelivered})
            if not report.changed:
                # Nothing could be folded (a schema/node mismatch the coordinator
                # must fix): park folding for the rest of the turn instead of
                # spinning on rows that will never apply.
                fold_blocked = True
            # continue from the checkpoint that now carries the steering
            stream_input = None

    # ------------------------------------------------------------------
    # the durable "this turn has started" journal
    # ------------------------------------------------------------------

    #: KV namespace of the turn journal. One item per run, written under the
    #: lease with a run-scoped operation key, so it is durable, replay-safe and
    #: invisible to any other scope.
    TURN_NAMESPACE = ["ahp", "turns"]

    #: The inbox event kind ``RespondHITL`` writes (``Coordinator.RespondHITL`` ->
    #: ``Enqueue(kind="hitl_response")``), and therefore the only ``wait_for`` a
    #: HITL answer can wake: State returns a waiting head to ``pending`` when the
    #: event's kind equals its ``wait_for`` (or its ``wait_for`` is ``*``). This is
    #: a platform name, not a private one, and the container does not get to
    #: rename it - a run parked on any other name is parked forever.
    HITL_WAIT_FOR = "hitl_response"

    #: The kind that carries the question into the thread inbox, as Space's own
    #: ``SubmitHITL`` does, so a user's gate is visible to the same readers.
    HITL_GATE_KIND = "hitl_request"

    #: The prefix Space gives a gate's inbox key (``hitlRequestKeyPrefix`` in
    #: ``internal/space/hitl_gate.go``). Space matches a gate by that KEY alone -
    #: never by the kind or the payload - so a question written under any other
    #: prefix is invisible to ``GET GetHITL`` and unanswerable by ``RespondHITL``.
    HITL_GATE_KEY_PREFIX = "hitl:"

    #: The prefix ``Coordinator.RespondHITL`` gives the answer event
    #: (``hitlResponseKeyPrefix``). The answer is read by this key and not by the
    #: kind: every gate in the thread answers under the same kind, so kind-matching
    #: would hand a woken run whichever answer landed last - possibly another
    #: gate's, or another turn's.
    HITL_ANSWER_KEY_PREFIX = "hitl_response:"

    #: The suffix Space gives a REPAIR wake (``hitlWakeKeySuffix`` in
    #: ``internal/space/hitl_gate.go``, closed by 577e2a9). State wakes a parked run
    #: only in the moment an inbox event is INSERTED, so a decision that arrived
    #: between this container's gate event and its own ``Suspend`` woke nothing, and
    #: the park that follows waits for a signal that has already passed: the run
    #: stays parked forever and neither side gets an error. Space repairs that window
    #: for the gates it submits itself, and a container's gate never goes through
    #: ``SubmitHITL`` - so this service rings the same doorbell, under the platform
    #: author's own key and kind, after its own park. A doorbell is not a verdict:
    #: the decision keeps its own key and stays the only event a reader matches.
    HITL_WAKE_KEY_SUFFIX = ":wake"

    #: Two journals per run, so "this run asked" and "this run took the answer"
    #: can never overwrite each other. The first is written when the run parks,
    #: the second when it reads the answer out of the inbox.
    HITL_ASKED_NAMESPACE = ["ahp", "hitl", "asked"]
    HITL_ANSWER_NAMESPACE = ["ahp", "hitl", "answer"]

    async def _hitl_gate(self, client: StateAPIClient, fence: dict, thread_id: str,
                         interrupts: list[dict]) -> list[str]:
        """Put the open question where the answer path can find it, and journal
        under the lease that this run asked.

        Space does the same for its own gate: ``Coordinator.SubmitHITL`` enqueues
        kind ``hitl_request`` into the thread inbox and ``RespondHITL`` answers it
        with kind ``hitl_response``. Writing the question here means a user's
        interrupt is answerable over that same documented route and survives the
        executor that asked it, instead of living only in this process's log.
        ``Enqueue`` is keyed by ``(thread_id, key)``, so a re-delivered run re-asks
        the same question rather than duplicating it.

        The journal is what makes the later read safe: a run that never asked takes
        nothing out of the inbox, so an answer addressed to nothing cannot be
        mistaken for the input of an unrelated restart.
        """
        run_id = fence["run_id"]
        gates: list[str] = []
        for item in interrupts:
            interrupt_id = str(item.get("id") or "")
            if not interrupt_id:
                continue
            # Space identifies a gate by the inbox event KEY, never by the payload
            # or the kind (hitl_gate.go: hitlRequestKeyPrefix + hitlID). Keying this
            # way is what makes a user's gate readable through GET GetHITL and
            # answerable by RespondHITL with the hitl_id below, instead of being an
            # event only this container understands.
            hitl_id = f"{run_id}:{interrupt_id}"
            gates.append({"hitl_id": hitl_id, "interrupt_id": interrupt_id})
            await client.enqueue(thread_id, f"{self.HITL_GATE_KEY_PREFIX}{hitl_id}",
                                 self.HITL_GATE_KIND,
                                 {"id": hitl_id, "turn_id": run_id,
                                  "kind": item.get("kind") or "input",
                                  "prompt": item.get("value"),
                                  "status": "pending",
                                  "run_id": run_id, "interrupt_id": interrupt_id,
                                  "wait_for": self.HITL_WAIT_FOR})
        try:
            await client.put_kv(fence, self.HITL_ASKED_NAMESPACE, run_id,
                                f"hitl-asked:{run_id}", 0,
                                {"run_id": run_id, "asked": [g["hitl_id"] for g in gates],
                                 "wait_for": self.HITL_WAIT_FOR})
        except StateAPIError as exc:
            if exc.code not in ("CONFLICT", "KEY_REUSE_MISMATCH"):
                raise       # already journaled by the previous life of this run
        return [g["interrupt_id"] for g in gates]

    async def _hitl_rearm(self, client: StateAPIClient, fence: dict, thread_id: str) -> list[str]:
        """Ring the platform's own doorbell for an answer that beat the park.

        The window is ordinary, not exotic: the gate event is published first - it
        is what makes the gate's id known at all - and the run is parked second. An
        approver reading the thread inbox can answer inside that gap, and State's
        wake fires only at insert time, against a run that is still ``running``.
        Nothing is then wrong except that the turn never continues.

        So, once the park is durable, this container reads its own thread and rings
        the doorbell the platform author defined: kind ``hitl_response`` - the name
        State wakes on - under ``hitl_response:<hitl_id>:wake``, and names in its
        payload which event it re-signals. Three limits are the author's, not
        inventions of mine:

        * only on POSITIVE evidence. An answer this read cannot see is not treated
          as "unanswered": a decision acknowledged inside the window stays invisible
          here, and that is the acked-inclusive inbox read Space is not entitled to
          substitute. Such a run remains parked, and that stays a stated residual;
        * only for a run actually parked on this name. Anything else would be a wake
          this thread never asked for;
        * a failed doorbell never fails the turn. The gate and the park are durable
          and correct; saying the gate was never asked would be the worse lie.

        Returns the gates it re-armed, so a test - or an operator - can see the
        repair happen instead of inferring it from a turn that merely continued.
        """
        run_id = fence["run_id"]
        try:
            asked = await client.get_kv(self.HITL_ASKED_NAMESPACE, run_id)
        except StateAPIError as exc:
            if exc.code != "NOT_FOUND":
                raise
            return []         # this run never asked: no gate of ours can be stranded
        journaled = (asked or {}).get("value")
        asked_ids = journaled.get("asked", []) if isinstance(journaled, dict) else []
        if not asked_ids:
            return []

        answers: dict[str, dict] = {}
        rung: set[str] = set()
        after = 0
        for _ in range(100):
            page = await client.read_inbox(thread_id, after=after, limit=100)
            events = page.get("events") or []
            for msg in events:
                key = str(msg.get("key") or "")
                if not key.startswith(self.HITL_ANSWER_KEY_PREFIX):
                    continue
                gate = key[len(self.HITL_ANSWER_KEY_PREFIX):]
                if gate.endswith(self.HITL_WAKE_KEY_SUFFIX):
                    rung.add(gate[:-len(self.HITL_WAKE_KEY_SUFFIX)])
                elif not msg.get("acked"):
                    answers[gate] = msg
            if not events:
                break
            after = max([event.get("sequence") or 0 for event in events] + [after])
        if not answers:
            return []

        run = await client.get_run(run_id)
        if not isinstance(run, dict) or run.get("status") != "waiting" \
                or run.get("wait_for") != self.HITL_WAIT_FOR:
            return []

        rearmed: list[str] = []
        for hitl_id in asked_ids:
            found = answers.get(hitl_id)
            if found is None or hitl_id in rung:
                continue      # answered, but already re-armed: one doorbell per gate
            verdict = found.get("payload") if isinstance(found.get("payload"), dict) else {}
            try:
                await client.enqueue(
                    thread_id,
                    f"{self.HITL_ANSWER_KEY_PREFIX}{hitl_id}{self.HITL_WAKE_KEY_SUFFIX}",
                    self.HITL_WAIT_FOR,
                    {"hitl_id": hitl_id,
                     "approved": verdict.get("approved"),
                     "response": verdict.get("response"),
                     "resignal_of": f"{self.HITL_ANSWER_KEY_PREFIX}{hitl_id}"})
            except StateAPIError as exc:
                log.warning("could not re-arm the HITL wake of gate %s for run %s: %s: %s",
                            hitl_id, run_id, exc.code, exc.message)
                continue
            rearmed.append(hitl_id)
        if rearmed:
            log.warning("an answer beat the park and woke nothing; re-armed run %s on %s",
                        run_id, ",".join(rearmed))
        return rearmed

    async def _hitl_answer(self, client: StateAPIClient, fence: dict, thread_id: str,
                           payload: dict) -> dict:
        """Read this run's own answer out of the thread inbox, exactly once.

        The wake rule fires on the event's kind, so the woken run still has to read
        the answer itself. It is journalled under the lease BEFORE the event is
        acked: a pod that dies in between re-reads the same answer instead of
        losing it, and a re-delivered run applies that answer rather than whatever
        else happens to be in the box.

        The answer is matched by its KEY (``hitl_response:<hitl_id>``, the writer's
        own identity for the decision) against the gates this run journaled as
        asked - never by the kind. Every gate in a thread answers under kind
        ``hitl_response``, so kind-matching would resume this run on another gate's
        or another turn's answer.
        """
        run_id = fence["run_id"]
        try:
            asked = await client.get_kv(self.HITL_ASKED_NAMESPACE, run_id)
        except StateAPIError as exc:
            if exc.code != "NOT_FOUND":
                raise
            return payload        # this run never asked: nothing in the box is for it
        journaled = (asked or {}).get("value")
        asked_ids = journaled.get("asked", []) if isinstance(journaled, dict) else []
        keys = {f"{self.HITL_ANSWER_KEY_PREFIX}{hitl_id}" for hitl_id in asked_ids}
        #: the repair's own doorbells for these gates: traffic, not content, and
        #: this container is the only reader that can sweep them
        doorbells = {key + self.HITL_WAKE_KEY_SUFFIX: None for key in keys}
        doorbell_ids: list[str] = []
        if not keys:
            return payload
        try:
            held = await client.get_kv(self.HITL_ANSWER_NAMESPACE, run_id)
        except StateAPIError as exc:
            if exc.code != "NOT_FOUND":
                raise
            held = None
        value = (held or {}).get("value") if isinstance(held, dict) else None
        if isinstance(value, dict) and "answer" in value:
            return {**payload, "resume": value["answer"]}
        found: dict | None = None
        after = 0
        for _ in range(100):
            page = await client.read_inbox(thread_id, after=after, limit=100)
            # the envelope and the cursor are the plane's names: ``ReadInbox``
            # answers ``{"events": [...]}`` whose items carry ``sequence``, and it
            # answers with UNACKED events only (internal/state/events.go). There is
            # no ``next_after`` to page by, so the cursor advances on the events
            # themselves and an empty page is the end of the log.
            events = page.get("events") or []
            for msg in events:
                if msg.get("key") in keys and not msg.get("acked"):
                    found = msg
                elif msg.get("key") in doorbells and msg.get("id"):
                    doorbell_ids.append(msg["id"])
            if not events:
                break
            after = max([event.get("sequence") or 0 for event in events] + [after])
        if found is None:
            return payload
        answer = found.get("payload")
        if isinstance(answer, dict) and "response" in answer:
            answer = answer["response"]      # the RespondHITL body: hitl_id/approved/response
        try:
            await client.put_kv(fence, self.HITL_ANSWER_NAMESPACE, run_id,
                                f"hitl-answer:{run_id}", 0,
                                {"run_id": run_id, "answer": answer,
                                 "event": found.get("id"), "seq": found.get("seq")})
        except StateAPIError as exc:
            if exc.code not in ("CONFLICT", "KEY_REUSE_MISMATCH"):
                raise
        await client.ack_inbox(fence, [found["id"]])
        # Sweeping the doorbell is best effort and comes last: it is this run's own
        # traffic, but nothing here may put the verdict's ack at risk to tidy up.
        if doorbell_ids:
            try:
                await client.ack_inbox(fence, doorbell_ids)
            except StateAPIError as exc:
                log.warning("could not sweep %d HITL wake doorbell(s) of run %s: %s: %s",
                            len(doorbell_ids), run_id, exc.code, exc.message)
        return {**payload, "resume": answer}

    async def _journal_turn(self, client: StateAPIClient, fence: dict,
                            payload: dict) -> bool:
        """Record that this run's turn has started; report whether it already had.

        ``True`` means "already started" - the restart path, where the container
        must continue from the durable checkpoint instead of re-applying the
        input. Without this journal a re-claimed run would feed its original
        input a second time and run the whole turn again.
        """
        run_id = fence["run_id"]
        try:
            await client.get_kv(self.TURN_NAMESPACE, run_id)
            return True
        except StateAPIError as exc:
            if exc.code != "NOT_FOUND":
                raise
        try:
            await client.put_kv(fence, self.TURN_NAMESPACE, run_id,
                                f"turn:{run_id}", 0,
                                {"run_id": run_id,
                                 "has_input": payload.get("input") is not None,
                                 "resumes": payload.get("resume") is not None})
        except StateAPIError as exc:
            if exc.code in ("CONFLICT", "KEY_REUSE_MISMATCH"):
                return True     # journaled by the previous life of this run
            raise
        return False

    @staticmethod
    def _graph_input(payload: dict, already_started: bool) -> Any:
        """What LangGraph is invoked with for this run.

        ``{"resume": v}``   -> continue an interrupted checkpoint with the answer
        ``{"input": x}``    -> a new turn
        restart of a run that already started -> ``None`` (continue from the
        checkpoint; LangGraph replays only tasks without durable writes)
        """
        if payload.get("resume") is not None:
            return Command(resume=payload["resume"])
        if already_started or payload.get("continue"):
            return None
        if payload.get("input") is not None:
            return payload["input"]
        return None

    # ------------------------------------------------------------------
    # lease
    # ------------------------------------------------------------------

    async def _claim(self, run_id: str | None) -> tuple[dict, str, str, dict, str] | None:
        resp = await self._client.claim_run(self._worker, ttl_ms=self._lease_ttl_ms, run_id=run_id)
        lease = resp.get("lease")
        if not lease:
            return None
        token = resp.get("task_token")
        run = lease.get("run", {})
        if run_id is not None and run.get("id") != run_id:
            raise StateAPIError("CONFLICT", f"claimed {run.get('id')!r}, wanted {run_id!r}")
        fence = {"run_id": run.get("id", ""), "worker": lease.get("worker", self._worker),
                 "epoch": lease.get("epoch", 1)}
        thread_id = (lease.get("thread") or {}).get("id", "")
        return fence, thread_id, fence["run_id"], (run.get("payload") or {}), token

    async def _heartbeat(self, fence: dict, stop: asyncio.Event) -> None:
        """Keep the lease alive while the turn runs (PDF §9.2 'узел дольше аренды')."""
        interval = max(self._heartbeat_ms, 100) / 1000.0
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self._client.heartbeat(fence, ttl_ms=self._lease_ttl_ms)
            except StateAPIError as exc:
                if exc.code in LEASE_LOST_CODES:
                    raise LeaseLost(exc.code) from exc
                raise

    def _task_client(self, token: str | None) -> StateAPIClient:
        """A client bound to the task token ``ClaimRun`` handed to *this* lease.

        Scope comes from the token, never from a request body, so the
        bootstrap/admin client must not be used for fenced writes. When the
        service was built with an injected client and no base URL (embedding,
        tests), that client is reused.
        """
        if self._injected is not None and not self._base_url:
            return self._injected
        if self._injected is not None:
            return self._injected.with_token(token or self._token)
        return StateAPIClient(self._base_url, token=token or self._token,
                              http_client=self._http,
                              http_client_factory=(lambda: httpx.AsyncClient(timeout=30.0)) if self._owns_http else None)

    async def _suspend(self, client: StateAPIClient, fence: dict, wait_for: str) -> None:
        await client.suspend(fence, wait_for=wait_for)

    async def _release(self, client: StateAPIClient, fence: dict, status: str,
                       close_reason: str | None = None,
                       failure_cause: str | None = None) -> None:
        """End the lease. A terminal failed/canceled owes State its reason
        (DEF-RELEASE-REASON-1), so the failure paths pass the dictionary word and this
        runtime's own sentence here - the same words the cause frame written just before
        them carries, so the run row and the transcript cannot disagree about the end.
        `done` passes neither, which keeps its wire form byte-identical to the historical
        call: the checkpoint is that run's verdict.
        """
        await client.release(fence, status=status, close_reason=close_reason,
                             failure_cause=failure_cause)

    async def _stream_event(self, client: StateAPIClient, fence: dict, kind: str,
                            payload: dict, key: str | None = None) -> None:
        try:
            await client.append_stream(fence,
                                       key=key or f"agent:{fence['run_id']}:{kind}",
                                       kind=kind, payload=payload)
        except StateAPIError as exc:      # the run's outcome must not hinge on telemetry
            log.warning("AppendStream(%s) failed: %s", kind, exc.code)

    # ------------------------------------------------------------------
    # the turn's failure, written where the platform can answer with it
    # ------------------------------------------------------------------

    async def _cause_frame(self, client: StateAPIClient, fence: dict,
                           thread_id: str, reason: str) -> None:
        """Journal why this turn failed, under a key the platform can attribute.

        The causal frame name belongs to the platform, not to the framework: Space
        reads a turn's reason from the frame whose kind is ``"error"`` carrying an
        ``{"error": "<text>"}`` payload (``StepError`` in internal/space/types.go,
        extracted by ``stepErrorCause``). A frame of any other kind is delivered
        verbatim but never becomes the reason - otherwise any worker could name
        another worker's failure. This SDK's own name for the event is kept inside
        the payload, which is what the contract answer asks for.

        Attribution is by the writer's key: ``space.StepKey(turn, n) ==
        "<turn_id>:<n>"``. A container can satisfy that without any new contract,
        because a Space turn id IS the State run id (``internal/space/
        coordinator.go``: ``turn := &Turn{ID: run.ID, ...}``) and the run id is in
        this lease's fence. So a failed turn leaves its reason where ``GetTurn``
        reads it after this process is gone. The index comes from the stream's own
        high-water mark, so a re-delivered run never reuses a key with new bytes.
        """
        seq = await self._step_seq_high_water(client, thread_id, fence["run_id"])
        key = (f"{fence['run_id']}:{seq}" if seq is not None
               # The high-water is unprovable (unreadable stream, or a thread longer
               # than the scan bound), so no attributable key can be chosen safely.
               # Guessing one risks KEY_REUSE_MISMATCH, which would drop the reason
               # entirely, so write under a key only this run owns: the frame still
               # reaches every reader of the stream, it just cannot be claimed by one
               # turn. Chosen with a warning from the helper, never silently.
               else f"agent:{fence['run_id']}:{CAUSE_DETAIL}")
        await self._stream_event(client, fence, CAUSE_KIND,
                                 {"error": reason, "agent_event": CAUSE_DETAIL},
                                 key=key)

    async def _step_seq_high_water(self, client: StateAPIClient, thread_id: str,
                                   run_id: str) -> int | None:
        """The next unused step index for this turn, or ``None`` if unprovable.

        Deliberately the same bounded, fail-closed discipline as
        ``space.Coordinator.stepSeqHighWater``: a bounded page, a scan cap, and no
        answer at all rather than a wrong one. A wrong index is a durable key
        collision on the one frame an operator needs.
        """
        prefix = f"{run_id}:"
        highest = -1
        after = 0
        scanned = 0
        try:
            while True:
                page = (await client.read_stream(thread_id, after=after,
                                                 limit=_STREAM_PAGE)).get("events") or []
                for event in page:
                    scanned += 1
                    key = event.get("key") or ""
                    if key.startswith(prefix) and key[len(prefix):].isdigit():
                        highest = max(highest, int(key[len(prefix):]))
                    # ``state.StreamEvent`` names its cursor ``sequence``, and that is
                    # the only cursor there is: the handler answers a bare ``events``
                    # array with no next-cursor field of its own.
                    value = event.get("sequence")
                    if isinstance(value, int) and value > after:
                        after = value
                if len(page) < _STREAM_PAGE:
                    return highest + 1
                if scanned >= _STREAM_SCAN_CAP:
                    return None
        except StateAPIError as exc:
            log.warning("ReadStream for a step key failed: %s", exc.code)
            return None


def _interrupts_of(state) -> list[dict]:
    out: list[dict] = []
    for task in getattr(state, "tasks", ()) or ():
        for hit in getattr(task, "interrupts", ()) or ():
            out.append({"id": getattr(hit, "id", ""), "value": getattr(hit, "value", None)})
    return out


def new_worker_id() -> str:
    return f"py-agent-{uuid.uuid4().hex[:12]}"
