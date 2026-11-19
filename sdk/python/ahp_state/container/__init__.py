"""The process the platform starts for a custom agent container (PDF §2.3, §4.2).

The image owns the graph; this entry point owns the platform half of the contract:
handshake, lease, heartbeats, graceful stop. See :mod:`ahp_state.container.__main__`.
"""

from __future__ import annotations

import importlib
from typing import Any

# Resolved lazily (PEP 562). ``python -m ahp_state.container`` executes ``__main__``
# itself; importing it from here first re-executes the module and makes every real
# run start with a RuntimeWarning on stderr.
_EXPORTS = {"main": "ahp_state.container.__main__", "run": "ahp_state.container.__main__"}

__all__ = ["main", "run"]


def __getattr__(name: str) -> Any:
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module 'ahp_state.container' has no attribute {name!r}")
    value = getattr(importlib.import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(globals()))
