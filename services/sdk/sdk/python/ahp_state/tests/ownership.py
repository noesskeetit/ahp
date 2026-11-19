"""Whose run is this? One rule, shared by every live module.

``ClaimRun`` hands out the head of the FIFO for the whole
``(tenant, deployment, subject)`` scope and takes no filter, so a live test that
wants its own run has to deal with whoever is ahead of it. The obvious move -
release the straggler with ``failed``, the only release that moves the queue on -
is destructive the moment the scope is not this checkout's alone, which is the
case on the acceptance lane: one database, several roles, separation by
deployment id. Abandoning another role's run to clear one's own path does not
merely lose their leg; the run may already have an effect journaled, so the work
is done and its record says it failed.

So the rule lives here, once, and defaults to the non-destructive answer.
"""
from __future__ import annotations

import os
from typing import Any, Optional

#: Thread contexts this checkout creates. A run on any other thread is somebody
#: else's work. Keep this in step with the context prefixes the live modules use.
OWN_NAMESPACES = ("sdk-live-", "ctx-sdk-", "acc-", "ctx-acc-")

#: What to do instead of abandoning. Kept in one place because every caller owes
#: the reader the same thing: whose run is in the way, and how to declare
#: exclusivity when the stand really is private.
SHARED_QUEUE_HINT = (
    "the stand's queue is shared, so that run was left exactly as it was. Declare "
    "the stand exclusive (hand-out \"exclusive\": true, or AHP_STAND_EXCLUSIVE=1) "
    "only when it genuinely belongs to this checkout."
)


def stand_is_exclusive(handshake: Optional[dict] = None) -> bool:
    """Whether this stand belongs to this checkout alone. Declared, never guessed.

    Guessed would mean a harness deciding, from an empty-looking queue, that no
    else could be using the stand - and releasing their runs on that guess.
    """
    if handshake and isinstance(handshake.get("exclusive"), bool):
        return handshake["exclusive"]
    return os.environ.get("AHP_STAND_EXCLUSIVE", "").strip().lower() in ("1", "true", "yes")


def _identity(run: dict, context: str) -> str:
    return (f"run={run.get('id')} thread={run.get('thread_id')} "
            f"context={context or '<unreadable>'}")


async def run_is_ours(client: Any, run: dict, handshake: Optional[dict] = None) -> tuple:
    """``(ours, identity)`` for a run handed to us that we did not ask for.

    Ownership is read, not assumed: the run's thread is fetched and its
    ``context_id`` compared with the namespaces this checkout creates. An
    unreadable thread counts as foreign - a run whose owner cannot be established
    is never released on another suite's behalf.
    """
    context = ""
    try:
        context = (await client.get_thread(run.get("thread_id") or "")).get("context_id", "")
    except Exception:
        context = ""
    return (bool(context) and context.startswith(OWN_NAMESPACES)) or stand_is_exclusive(
        handshake), _identity(run, context)
