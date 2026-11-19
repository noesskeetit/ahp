"""The queue-clearing rule, tested where it can be tested: before a live stand.

Every live module that waits for its own run has to deal with whoever sits ahead
of it in the FIFO, and the only release that moves a queue on is a terminal one.
So this file pins the one decision that must never be made casually - whose run
may be released to ``failed`` - against a stand nobody is running, because the
alternative is discovering the answer by cancelling another role's leg.
"""
from __future__ import annotations

import pytest

from ahp_state.client import StateAPIError
from ahp_state.tests.ownership import (OWN_NAMESPACES, SHARED_QUEUE_HINT,
                                       run_is_ours, stand_is_exclusive)


class _Client:
    """A plane that answers GetThread for the threads it knows."""

    def __init__(self, contexts: dict[str, str]) -> None:
        self.contexts = contexts
        self.reads: list[str] = []

    async def get_thread(self, thread_id: str) -> dict:
        self.reads.append(thread_id)
        if thread_id not in self.contexts:
            raise StateAPIError("NOT_FOUND", "no such thread")
        return {"id": thread_id, "context_id": self.contexts[thread_id]}


def _run(thread_id: str = "thr-1") -> dict:
    return {"id": "run-9", "thread_id": thread_id}


@pytest.fixture(autouse=True)
def _no_exclusivity_declaration(monkeypatch):
    monkeypatch.delenv("AHP_STAND_EXCLUSIVE", raising=False)


def test_exclusivity_is_declared_and_defaults_to_false(monkeypatch):
    assert stand_is_exclusive() is False
    monkeypatch.setenv("AHP_STAND_EXCLUSIVE", "1")
    assert stand_is_exclusive() is True
    # the hand-out wins over the ambient variable, so a lane can say "shared"
    monkeypatch.setenv("AHP_STAND_EXCLUSIVE", "1")
    assert stand_is_exclusive({"exclusive": False}) is False


async def test_a_run_on_a_foreign_context_is_never_released():
    client = _Client({"thr-1": "a2a-second-deploy-7f3a", "thr-2": "acc-1c9d2b"})

    ours, identity = await run_is_ours(client, _run("thr-1"))
    assert ours is False, SHARED_QUEUE_HINT
    assert "a2a-second-deploy-7f3a" in identity, "the refusal has to name what is in the way"

    ours, _ = await run_is_ours(client, _run("thr-2"))
    assert ours is True


async def test_an_unreadable_thread_counts_as_foreign():
    """A run whose owner cannot be established is not ours to cancel."""
    ours, identity = await run_is_ours(_Client({}), _run("thr-gone"))
    assert ours is False
    assert "<unreadable>" in identity


async def test_a_declared_exclusive_stand_may_clear_its_own_queue(monkeypatch):
    monkeypatch.setenv("AHP_STAND_EXCLUSIVE", "true")
    ours, _ = await run_is_ours(_Client({"thr-1": "legacy-context-from-an-older-run"}), _run())
    assert ours is True


def test_the_namespaces_match_the_contexts_the_live_modules_create():
    """A guard that lists the wrong prefixes silently re-enables the destructive path."""
    created = {
        "ahp_state/tests/test_live_state_properties.py": "sdk-live-1a2b3c",
        "tests/test_real_stand.py": "ctx-sdk-1a2b3c",
        "tests/test_acceptance_lane.py": "acc-1a2b3c4d5e",
    }
    for module, context in created.items():
        assert context.startswith(OWN_NAMESPACES), f"{module} creates {context!r}"
