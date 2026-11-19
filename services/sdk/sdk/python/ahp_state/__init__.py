"""Framework-neutral AHP State SDK with an optional LangGraph adapter.

Provides HTTP State and checkpoint recovery clients independently of LangGraph,
plus State API-backed implementations of LangGraph's ``BaseCheckpointSaver``
and ``BaseStore``, plus the container runtime (:mod:`ahp_state.agent`) that runs a
user's own graph against the platform: lease-fenced durable checkpoints, pending
writes, intent-journaled side effects, interrupt/resume and steering.

The names below are resolved lazily (PEP 562) on purpose. ``ahp_state.check`` is
the CI gate for a candidate image and is pure stdlib: pulling LangGraph in just
to import the package would make ``ahp-sdk check`` fail - and a build look
noisy - whenever the dependency is absent or emits its own warnings.
"""

from __future__ import annotations

import importlib
from typing import Any

_EXPORTS = {
    # State API client and its error taxonomy
    "StateAPIClient": "ahp_state.client",
    "StateAPIError": "ahp_state.client",
    "StateConflictError": "ahp_state.client",
    "StateNotFoundError": "ahp_state.client",
    "StateForbiddenError": "ahp_state.client",
    "StateValidationError": "ahp_state.client",
    "StateServerError": "ahp_state.client",
    "StateTimeoutError": "ahp_state.client",
    "StateAdminClient": "ahp_state.recovery",
    "TransferClient": "ahp_state.recovery",
    "rehome_export": "ahp_state.recovery",
    # durability
    "AHPCheckpointSaver": "ahp_state.checkpoint_saver",
    "AHPStore": "ahp_state.store",
    "AHPJournalClient": "ahp_state.journal_client",
    "SyncAHPJournalClient": "ahp_state.journal_client",
    "AHPJournalStore": "ahp_state.journal_store",
    "AHPJournalCheckpointSaver": "ahp_state.journal_checkpointer",
    "AHPJournalSessionService": "ahp_state.journal_adk",
    "AHPHermesPersistenceStore": "ahp_state.adapters.hermes",
    "StandaloneToolGuard": "ahp_state.journal_tools",
    "SessionFencedError": "ahp_state.journal_tools",
    "wire": "ahp_state.wire",
    # effects and reconciliation
    "IntentRecorder": "ahp_state.intents",
    "SideEffectUnknown": "ahp_state.intents",
    # the container runtime
    "AgentService": "ahp_state.agent",
    "RunContext": "ahp_state.agent",
    "RunOutcome": "ahp_state.agent",
    "Effects": "ahp_state.agent",
    "LeaseLost": "ahp_state.agent",
    "mtls_client": "ahp_state.transport",
    "new_worker_id": "ahp_state.agent",
    # steering
    "SteeringFold": "ahp_state.steering",
}

#: names that are submodules rather than attributes of a submodule. They cannot
#: live in ``_EXPORTS``: that table means "getattr(module, name)", and
#: ``ahp_state.wire`` has no member called ``wire`` - resolving it there raises
#: AttributeError while ``__all__`` still advertises the name.
_SUBMODULES = {"wire"}

__all__ = sorted(set(_EXPORTS) | _SUBMODULES)


def __getattr__(name: str) -> Any:
    if name in _SUBMODULES:
        module = importlib.import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module 'ahp_state' has no attribute {name!r}")
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value            # one import per name, then it is cached
    return value


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(globals()))
