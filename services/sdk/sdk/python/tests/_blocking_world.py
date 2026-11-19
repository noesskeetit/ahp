"""A world that lands the effect and then refuses to come back.

The durability legs need to kill a worker in one specific millisecond: after the
external effect has landed in the world and before its result reached the journal.
That window is a few microseconds wide on a machine this fast, so a leg cannot
find it by timing - it has to be stood in deliberately. The transport below IS
the window: it charges (file + fsync, so the touch survives ``SIGKILL``) and then
hangs, exactly like a pod whose side effect returned and whose network did not.
"""
from __future__ import annotations

import json
import os
import pathlib
import time
import uuid


async def charge(tool: str, args: dict) -> dict:
    """Charge for real, then never return. The leg does the killing."""
    world = pathlib.Path(os.environ["AHP_EFFECT_WORLD_DIR"])
    world.mkdir(parents=True, exist_ok=True)
    name = f"{uuid.uuid4().hex}.json"
    with (world / name).open("wb") as handle:
        handle.write(json.dumps({"tool": tool, "args": args}, sort_keys=True).encode())
        handle.flush()
        os.fsync(handle.fileno())
    directory = os.open(str(world), os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    time.sleep(3600)          # a blocked worker, not a cancelled one
    return {"receipt": name}  # unreachable by design
