"""Generate the double-vs-live divergence table by measurement, not by memory.

    PYTHONPATH=. python -m ahp_state.tests.divergence_report \
        --handshake /path/handshake.json > divergence.md

Both transports get the *same* JSON bodies (``contract_probe.PROBES``); the
table is the diff of what each one actually answered. Without ``--handshake``
only the double runs, which is how the baseline frozen in
``test_double_matches_live_baseline`` was produced.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import sys

from ahp_state.tests.contract_probe import Harness, divergence_table, render, run_matrix


def double_harness() -> Harness:
    from ahp_state.tests.contract_probe import ProbeError
    from ahp_state.tests.fake_state import FakeStateServer, StateError

    server = FakeStateServer()
    server.__enter__()

    def call(op: str, body: dict, token: str | None) -> dict:
        try:
            return server.post(op, body, token=token)
        except StateError as exc:
            # the same rule the SDK applies to the wire (client._raise_typed):
            # a flattened domain code travels in ``reason``
            raise ProbeError(exc.reason or exc.code, exc.message,
                             http_class=exc.code) from exc

    # the server has to outlive the harness for the whole report
    double_harness.server = server  # type: ignore[attr-defined]
    return Harness("double", call)


def live_harness(handshake: str) -> Harness:
    """Drive the live stand through the product's own ``mtls_client``.

    One event loop, two persistent clients - the real hand-out token and a
    forged one - so no probe invents a transport the product does not use.
    """
    from ahp_state.agent import mtls_client
    from ahp_state.tests.contract_probe import ProbeError

    hand = json.loads(pathlib.Path(handshake).read_text(encoding="utf-8"))
    queue: asyncio.Queue = asyncio.Queue()

    async def worker() -> None:
        real = mtls_client(hand)
        forged = mtls_client({**hand, "task_token": "forged-token"})
        try:
            while True:
                op, body, use_forged, done = await queue.get()
                if op is None:
                    return
                client = forged if use_forged else real
                try:
                    done.set_result(("ok", await client.call(op, body)))
                except Exception as exc:  # noqa: BLE001 - normalized below
                    code = getattr(exc, "code", type(exc).__name__)
                    klass = getattr(exc, "http_class", code)
                    done.set_result(("err", (code, str(getattr(exc, "message", exc)),
                                             klass)))
        finally:
            await real.close()
            await forged.close()

    loop = asyncio.new_event_loop()
    task = loop.create_task(worker())

    def call(op: str, body: dict, token: str | None) -> dict:
        done: asyncio.Future = loop.create_future()
        loop.run_until_complete(queue.put((op, body, token == "forged-token", done)))
        kind, payload = loop.run_until_complete(done)
        if kind == "err":
            raise ProbeError(payload[0], payload[1], http_class=payload[2])
        return payload

    def close() -> None:
        loop.run_until_complete(queue.put((None, None, None, None)))
        loop.run_until_complete(task)
        loop.close()

    live_harness.close = close  # type: ignore[attr-defined]
    return Harness("live", call)


def drain(h: Harness) -> int:
    """Release everything queued before the matrix, so "nothing queued" is true.

    An earlier debugging run leaves runs behind, and ``ClaimRun`` is FIFO for the
    whole scope: an undrained stand would make the empty-claim probe lie.
    """
    drained = 0
    while drained < 200:
        claim = h.call("ClaimRun", {"worker": f"drain-{drained}", "ttl_ms": 60000}, None)
        lease = claim.get("lease")
        if not lease:
            return drained
        h.call("Release", {"run_id": lease["run"]["id"], "worker": lease["worker"],
                           "epoch": lease["epoch"], "status": "failed"}, None)
        drained += 1
    return drained


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--handshake", default="", help="live stand handshake.json")
    ap.add_argument("--json-out", default="", help="also dump both raw matrices")
    args = ap.parse_args(argv)

    dh = double_harness()
    print(f"[double] drained {drain(dh)} queued run(s) before probing", file=sys.stderr)
    double = run_matrix(dh)
    print("== ДВОЙНИК ==", file=sys.stderr)
    print(json.dumps(double, indent=1, sort_keys=True), file=sys.stderr)

    if not args.handshake:
        print("live State stand required (--handshake / AHP_STATE_HANDSHAKE)",
              file=sys.stderr)
        double_harness.server.__exit__()  # type: ignore[attr-defined]
        return 2

    lh = live_harness(args.handshake)
    print(f"[live] drained {drain(lh)} queued run(s) before probing", file=sys.stderr)
    live = run_matrix(lh)
    print("== ЖИВОЙ STATE ==", file=sys.stderr)
    print(json.dumps(live, indent=1, sort_keys=True), file=sys.stderr)
    live_harness.close()  # type: ignore[attr-defined]
    double_harness.server.__exit__()  # type: ignore[attr-defined]

    if args.json_out:
        pathlib.Path(args.json_out).write_text(
            json.dumps({"double": double, "live": live}, indent=1, sort_keys=True),
            encoding="utf-8")

    rows = divergence_table(double, live)
    print(render(double, live))
    print()
    print(f"**Расхождений: {len(rows)} из "
          f"{sum(len(v) for v in double.values())} наблюдений.**")
    for section, label, mine, theirs in rows:
        print(f"- `{section}` / {label}: двойник `{mine}` vs живой `{theirs}`")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
