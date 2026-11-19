"""Intent recording for SDK clients, keyed by the durable (run_id, node, call).

For tool-calling agents ``call`` is the LangChain/A2A ``tool_call_id`` (per PDF
rev 3.2 section 4.2: every side effect is journaled before it fires and is made
idempotent through the operation key). The State server deduplicates on
``(run_id, node, call)``.

Authoritative status machine (``internal/state/intents.go``)
-----------------------------------------------------------
The wire contract defines exactly these statuses (NOT "pending"/"completed"/
"side_effect_unknown"):

    recorded   -> dispatched -> succeeded | failed | unknown
    recorded   -> fenced
    dispatched -> succeeded | failed | unknown | fenced
    unknown    -> fenced            (and, only via ReconcileIntent -> succeeded|failed)

Two facts drive the wrapper logic:

1. ``RecordIntent`` is idempotent on ``(run_id, node, call)``: replaying the
   identical fingerprint performs **no insert** and returns the stored intent
   with its **current** status (``internal/state/intents.go``: the existing-row
   branch answers ``Status: storedStatus``), while a different fingerprint under
   the same key fails with ``KEY_REUSE_MISMATCH``. So the answer to "may I run
   this effect?" is never found by calling ``RecordIntent`` - that call is
   read-only for an intent that already exists, and a client that recorded and
   then executed would fire a finished effect a second time. ``guarded`` asks
   ``GetIntent`` first and acts on the status it reports.
2. ``UpdateIntent`` is monotonic; an ``unknown`` intent can only be resolved by
   the privileged ``ReconcileIntent`` (exact original fingerprint required), which
   is the reconciliation path for a crash that happened after the side effect
   fired but before the checkpoint/ack was durably written.

Crash window handled by ``guarded``:
    record -> dispatch -> [side effect fires] -> (crash before completion)
    on re-entry: GetIntent shows "dispatched"/"unknown"; guarded surfaces a
    SideEffectUnknown error rather than silently re-running the effect or
    silently assuming it completed, so a reconciler can resolve it with external
    evidence via ``reconcile``.
"""

from __future__ import annotations

import asyncio
import collections
from typing import Any, Awaitable, Callable, Optional

from ahp_state.client import StateAPIClient, StateAPIError

# Intent lifecycle statuses (mirrors internal/state/intents.go wire contract).
RECORDED = "recorded"
DISPATCHED = "dispatched"
SUCCEEDED = "succeeded"
FAILED = "failed"
UNKNOWN = "unknown"
FENCED = "fenced"

TERMINAL = (SUCCEEDED, FAILED)


class _CallMutex:
    """Process-local async mutex, safe to construct outside a running loop.

    Serializes same-``(run, node, call)`` ``guarded`` dispatches within this
    worker. There is one lease per run, so same-call concurrency is in-process;
    this mutex closes the recorded->dispatched gap that the State server cannot
    (its transition table allows ``from == to``), which is the root of P4 (a
    double external side effect under concurrent same-call invocations).
    """

    def __init__(self) -> None:
        self._locked = False
        self._waiters = collections.deque()

    async def acquire(self) -> None:
        if not self._locked and not self._waiters:
            self._locked = True
            return
        fut = asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        await fut  # resolved only when this waiter is handed the lock

    def release(self) -> None:
        if self._waiters:
            nxt = self._waiters.popleft()
            if not nxt.done():
                nxt.set_result(None)
            # _locked stays True: the woken waiter now holds it.
        else:
            self._locked = False

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()
        return False


_call_mutexes: dict = {}


def _call_mutex(run_id: str, node: str, call_id: str) -> "_CallMutex":
    key = (run_id, node, call_id)
    m = _call_mutexes.get(key)
    if m is None:
        m = _CallMutex()
        _call_mutexes[key] = m
    return m


class SideEffectUnknown(RuntimeError):
    """Raised when a journaled side effect's outcome cannot be trusted.

    The intent is in ``dispatched`` or ``unknown`` after a restart: the effect
    may or may not have landed. Re-running it risks a double side effect;
    assuming it succeeded risks losing it. The caller must reconcile it with
    external evidence via ``IntentRecorder.reconcile``.
    """

    def __init__(self, intent: dict):
        self.intent = intent
        super().__init__(
            f"side effect for call {intent.get('call')!r} is {intent.get('status')!r}; "
            "reconcile via ReconcileIntent with external evidence"
        )


class IntentRecorder:
    """Records and reconciles operation intents via the State API.

    Each intent is keyed by ``(run_id, node, call)``. For tool-calling agents
    ``call`` is the tool_call_id.

    Parameters
    ----------
    client : StateAPIClient
    fence : dict
        {"run_id": str, "worker": str, "epoch": int}
    node : str
        Logical node/component name (default: "agent").
    """

    def __init__(self, client: StateAPIClient, fence: dict[str, Any], *, node: str = "agent"):
        self._client = client
        self._fence = fence
        self._node = node

    # --- low-level journal ops ---

    async def record(
        self, call_id: str, kind: str = "tool", target: str = "", args: Optional[dict] = None
    ) -> dict:
        """Journal an intent (RecordIntent). Idempotent on (run_id, node, call).

        NOTE: on replay RecordIntent returns ``status="recorded"`` regardless of
        the intent's true status; use ``get``/``guarded`` to read the current
        status. A different fingerprint under the same key raises
        StateAPIError(KEY_REUSE / KEY_REUSE_MISMATCH).
        """
        result = await self._client.call("RecordIntent", {
            "fence": {"run_id": self._fence["run_id"],
                      "worker": self._fence["worker"], "epoch": self._fence["epoch"]},
            "node": self._node,
            "call": call_id,
            "kind": kind,
            "target": target,
            "args": args or {},
        })
        return result.get("intent", result)

    async def update(self, call_id: str, status: str, result: Any = None) -> dict:
        """Advance an intent (UpdateIntent). Transitions are monotonic.

        status must be one of recorded/dispatched/succeeded/failed/unknown/fenced.
        Cannot complete an ``unknown`` intent (raises; use ``reconcile``).
        """
        body: dict[str, Any] = {
            "fence": {"run_id": self._fence["run_id"],
                      "worker": self._fence["worker"], "epoch": self._fence["epoch"]},
            "node": self._node,
            "call": call_id,
            "status": status,
        }
        if result is not None:
            body["result"] = result
        resp = await self._client.call("UpdateIntent", body)
        return resp.get("intent", resp)

    async def dispatch(self, call_id: str) -> dict:
        """Transition recorded -> dispatched (effect is about to fire)."""
        return await self.update(call_id, DISPATCHED)

    async def succeed(self, call_id: str, result: Any = None) -> dict:
        """Transition dispatched -> succeeded (effect confirmed done)."""
        return await self.update(call_id, SUCCEEDED, result)

    async def fail(self, call_id: str, result: Any = None) -> dict:
        """Transition dispatched -> failed (effect confirmed did not happen)."""
        return await self.update(call_id, FAILED, result)

    async def mark_unknown(self, call_id: str) -> dict:
        """Transition dispatched -> unknown (effect fired but outcome unconfirmed)."""
        return await self.update(call_id, UNKNOWN)

    async def get(self, call_id: str) -> Optional[dict]:
        """Read the CURRENT intent status via GetIntent (NOT_FOUND -> None)."""
        try:
            resp = await self._client.get_intent(
                run_id=self._fence["run_id"], node=self._node, call=call_id
            )
            return resp.get("intent", resp)
        except StateAPIError as e:
            if e.code == "NOT_FOUND":
                return None
            raise

    async def reconcile(
        self, call_id: str, status: str, digest: Optional[str] = None, result: Any = None
    ) -> dict:
        """Privileged ReconcileIntent: unknown -> succeeded|failed.

        The exact original fingerprint (digest) is required; if not supplied it
        is read from the current intent via GetIntent.
        """
        if status not in TERMINAL:
            raise ValueError("reconcile target must be succeeded or failed")
        if digest is None:
            current = await self.get(call_id)
            if current is None:
                raise StateAPIError("NOT_FOUND", "intent not found for reconcile")
            digest = current.get("digest", "")
        resp = await self._client.reconcile_intent(
            fence=self._fence, node=self._node, call=call_id,
            status=status, digest=digest, result=result,
        )
        return resp.get("intent", resp)

    # --- high-level crash-safe wrapper ---

    async def _verify_fingerprint(self, call_id: str, kind: str, target: str,
                                  args: Optional[dict]) -> None:
        """Re-validate the incoming (kind, target, args) against the journaled
        intent using the server's authoritative digest check.

        ``RecordIntent`` on an EXISTING intent is read-only (no state change) and
        returns OK for an identical fingerprint but KEY_REUSE for a different
        one. A mismatch raises ``INTENT_ARG_MISMATCH`` so a reused call identity
        with a CHANGED logical request is an explicit error (P5) instead of a
        silent stale result.
        """
        try:
            await self.record(call_id, kind=kind, target=target, args=args)
        except StateAPIError as e:
            if e.code in ("KEY_REUSE", "KEY_REUSE_MISMATCH"):
                raise StateAPIError(
                    "INTENT_ARG_MISMATCH",
                    f"call {call_id!r} was journaled with a different "
                    f"(kind, target, args); refusing to return a stale result",
                ) from e
            raise

    async def guarded(
        self, call_id: str, effect: Callable[[], Awaitable[Any]],
        *, kind: str = "tool", target: str = "", args: Optional[dict] = None,
    ) -> Any:
        """Execute ``effect`` at most once across restarts AND concurrent peers.

        A process-local mutex serializes same-``(run, node, call)`` dispatches so
        two concurrent invocations under one ``tool_call_id`` cannot both cross
        the recorded->dispatched gap and fire the effect twice (P4 — the State
        server allows same-status transitions and so cannot claim on its own).
        The intent status then handles the restart / crash window:

          - absent / recorded : journal, dispatch, run effect, mark succeeded.
          - succeeded         : verify fingerprint (P5), return the stored result.
          - failed            : verify fingerprint, raise (a prior failure).
          - dispatched/unknown: ambiguous -> mark unknown, raise SideEffectUnknown
                                (never a silent re-run, never "assume done").
          - fenced            : raise (this worker lost the lease).
        """
        mutex = _call_mutex(self._fence["run_id"], self._node, call_id)
        async with mutex:
            current = await self.get(call_id)
            status = current.get("status") if current else None

            if status == FENCED:
                raise StateAPIError("STALE_EPOCH", f"intent {call_id!r} is fenced")

            if status in (SUCCEEDED, FAILED):
                # P5: a changed (kind, target, args) under a terminal intent is
                # an explicit error, not a silent stale result.
                await self._verify_fingerprint(call_id, kind, target, args)
                if status == SUCCEEDED:
                    return current.get("result")  # SKIP: effect already done
                raise StateAPIError("CONFLICT", f"intent {call_id!r} previously failed")

            if status in (DISPATCHED, UNKNOWN):
                if status == DISPATCHED:
                    await self.mark_unknown(call_id)
                    current = await self.get(call_id)
                raise SideEffectUnknown(current or {"call": call_id, "status": UNKNOWN})

            # Absent or recorded: the first dispatcher fires the effect once.
            if status is None:
                await self.record(call_id, kind=kind, target=target, args=args)
                # Read-after-record: a concurrent peer may have dispatched in the
                # gap (defense-in-depth; the mutex is the primary serializer).
                current = await self.get(call_id)
                if current is not None and current.get("status") == DISPATCHED:
                    raise SideEffectUnknown(current)
            await self.dispatch(call_id)
            try:
                outcome = await effect()
            except Exception:
                # Fail-closed (Root 20261030T2103Z): the effect MAY have fired
                # before the exception (it ran but the ack/checkpoint write was
                # lost). Record UNKNOWN (needs reconciliation), not FAILED.
                await self.mark_unknown(call_id)
                raise
            await self.succeed(call_id, outcome)
            return outcome
