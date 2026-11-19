"""One life of a run, executed in a real process that the leg then SIGKILLs.

The leg (`test_sigkill_no_second_effect.py`) needs a life that ends the way a
killed container ends: no `close()`, no release, no `finally` - the process
stops existing while an effect is in flight. Nothing here decides a verdict; it
claims the run the leg queued, journals one effect through the SDK's own
`Effects` guard, touches the world once, and then hangs where the ack would be.

Run as a script. Args: <run_id> <worker> <world-file>. Stdout: `CLAIMED <epoch>`
then `FIRED`, which is what the leg waits for before it kills this process.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from _live_stand import STAND, claim_own_run       # noqa: E402
from ahp_state.agent import Effects                # noqa: E402

EFFECT_TTL_MS = 6000        # short on purpose: the leg must re-claim after lease expiry
CLAIM_DEADLINE_S = 45.0


async def _claim(client, want_run_id: str, worker: str) -> dict:
    """This run, not whoever the FIFO hands out first.

    ``ClaimRun`` takes no filter, and a stand that has run other legs has runs
    queued ahead of ours, so a plain claim loop starves - measured, 45 s of
    other people's leases. ``claim_own_run`` is the suite's one answer to that:
    it abandons stragglers this checkout queued, and refuses to touch a run whose
    context it did not create on a stand nobody declared exclusive.
    """
    return await claim_own_run(client, want_run_id, worker, deadline_s=CLAIM_DEADLINE_S,
                               ttl_ms=EFFECT_TTL_MS)


async def _run(run_id: str, worker: str, world: str) -> None:
    client = STAND.client()
    lease = await _claim(client, run_id, worker)
    fence = {"run_id": lease["run"]["id"], "worker": lease["worker"], "epoch": lease["epoch"]}
    print(f"CLAIMED {lease['epoch']}", flush=True)

    async def transport(tool: str, args: dict) -> dict:
        # The world is a file, and it is fsynced: after the kill the leg reads the
        # world itself, not this process's memory, so a second line cannot hide.
        with open(world, "a") as fh:
            fh.write(json.dumps({"tool": tool, "args": args, "epoch": fence["epoch"]}) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        print("FIRED", flush=True)
        time.sleep(600)          # die here: this is the moment the ack never reaches
        return {"charged": args.get("amount")}

    await Effects(client, fence, transport).call("billing.charge", {"amount": 100})


if __name__ == "__main__":
    asyncio.run(_run(sys.argv[1], sys.argv[2], sys.argv[3]))
