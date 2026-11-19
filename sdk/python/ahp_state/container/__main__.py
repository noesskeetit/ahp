"""``python -m ahp_state.container`` - the entry point of a custom agent container.

Everything the platform promises a custom agent is in :mod:`ahp_state.agent`; this
module is only the thin process wrapper around it: read the hand-out from the
environment, build the mTLS client, import the user's graph builder, and serve
runs until the pod is asked to stop. It contains no durability logic at all - if
this file ever needs any, the layering is wrong.

Environment
-----------
``AHP_AGENT_MODULE``   import path of the user's module (required)
``AHP_AGENT_ENTRY``    attribute holding ``build(ctx) -> CompiledStateGraph``
                       (default: ``build``)
``AHP_STATE_HANDSHAKE`` path to the handshake JSON the platform mounted:
                       ``base_url, ca_file, cert_file, key_file, task_token`` -
                       paths and a token, never logged
``AHP_STATE_URL`` / ``AHP_STATE_TOKEN``
                       the same hand-out without a file (tests, sidecars)
``AHP_WORKER_ID``      worker name for ``ClaimRun`` (default: ahp-agent-<uuid>)
``AHP_LEASE_TTL_MS`` / ``AHP_HEARTBEAT_MS``
                       lease policy; the heartbeat keeps the lease while a
                       superstep runs long, and losing it stops the turn
``AHP_IDLE_WAIT_S``    how long to sit between empty ``ClaimRun`` calls
                       (seconds; fractions are allowed - ``0.2`` is a sane poll
                       interval, and this one number is parsed as a float)
``AHP_MAX_TURNS``      serve this many turns and exit (0 = serve forever)
``AHP_EFFECT_TRANSPORT``
                       ``module:callable`` touching the outside world; without it
                       any ``ctx.effects.call`` in this image refuses to run. The
                       agent module's own ``effect_transport`` is used when set.
``AHP_EFFECT_NODE``    the node name effects are journaled under (default ``agent``)

The process exits 0 after a stop signal, 1 if the agent module cannot be loaded -
a container that cannot even build its graph must not sit in a restart loop
claiming runs it will never finish.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
from importlib.machinery import PathFinder, SourceFileLoader
import logging
import os
import signal
import sys
from typing import Any, Optional

from ahp_state.agent import AgentService, GraphBuilder, mtls_client, new_worker_id

log = logging.getLogger("ahp_state.container")


def _check_module_source(module: str) -> None:
    """Locate ordinary source without executing parent package code.

    This is a candidate-code lint, not an isolation boundary. Images must be
    immutable between this check and import; platform network policy is separate.
    """
    from ahp_state.check import CheckError, check_paths

    if not module or any(not part.isidentifier() for part in module.split(".")):
        raise RuntimeError("cannot import agent module: expected a dotted Python module")
    search = None
    roots = []
    for i, part in enumerate(module.split(".")):
        name = ".".join(module.split(".")[:i + 1])
        spec = PathFinder.find_spec(name, search)
        if spec is None:
            raise RuntimeError(f"cannot import agent module {module!r}: source not found")
        locations = list(spec.submodule_search_locations or ())
        if not isinstance(spec.loader, SourceFileLoader) and not (
                spec.loader is None and locations):
            raise RuntimeError(f"cannot inspect source of agent module {name!r}")
        if i == 0:
            roots = locations or [spec.origin]
        search = locations
        if i < len(module.split(".")) - 1 and not search:
            raise RuntimeError(f"cannot import agent module {module!r}: parent is not a package")
    try:
        findings, checked = check_paths(roots)
    except CheckError as exc:
        raise RuntimeError(f"agent source check failed: {exc}") from exc
    if not checked:
        raise RuntimeError("agent source check found no inspectable Python files")
    errors = [f for f in findings if f.severity == "error"]
    if errors:
        details = "; ".join(f"{f.locator}: {f.rule}" for f in errors)
        raise RuntimeError(f"agent source violates AHP contract: {details}")


def load_builder(module: str, entry: str = "build") -> GraphBuilder:
    """Import the user's graph builder. Fails loudly, once, at startup."""
    if not module:
        raise ValueError("AHP_AGENT_MODULE is required: this image has no agent")
    _check_module_source(module)
    try:
        loaded = importlib.import_module(module)
    except ImportError as exc:
        raise RuntimeError(f"cannot import agent module {module!r}: {exc}") from exc
    builder = getattr(loaded, entry, None)
    if builder is None or not callable(builder):
        raise RuntimeError(f"agent module {module!r} has no callable {entry!r}")
    return builder


def load_effect_transport(spec: str = "", agent_module: str = ""):
    """Resolve the effect transport this image will touch the world through.

    Two sources, most explicit first:

    ``AHP_EFFECT_TRANSPORT="pkg.mod:callable"``
        any ``async def callable(tool, args) -> result``. A harness or a sidecar
        can point a shipped image at its own world without rebuilding it.
    ``<agent module>.effect_transport``
        what the image ships with, so a normal pod needs no extra environment.

    Returning ``None`` is legal and means the agent has no effects: ``Effects.call``
    then raises a named error the first time a node asks. What is NOT legal is
    discovering that mid-turn after the run has already charged somebody, which is
    why an explicitly named but unusable transport is a startup failure here.
    """
    spec = spec or os.environ.get("AHP_EFFECT_TRANSPORT", "")
    if spec:
        module_name, sep, attr = spec.partition(":")
        if not sep:
            module_name, sep, attr = spec.rpartition(".")
        if not sep or not module_name or not attr:
            raise RuntimeError(
                f"AHP_EFFECT_TRANSPORT must name 'module:callable', got {spec!r}")
        _check_module_source(module_name)
        try:
            loaded = importlib.import_module(module_name)
        except ImportError as exc:
            raise RuntimeError(
                f"cannot import effect transport module {module_name!r}: {exc}") from exc
        transport = getattr(loaded, attr, None)
        if transport is None or not callable(transport):
            raise RuntimeError(
                f"effect transport {spec!r} resolves to no callable")
        return transport
    if agent_module:
        loaded = sys.modules.get(agent_module)
        if loaded is not None:
            shipped = getattr(loaded, "effect_transport", None)
            if callable(shipped):
                return shipped
    return None


def client_from_environment():
    """Build the State API client from the handshake file or from flat variables."""
    handshake_path = os.environ.get("AHP_STATE_HANDSHAKE", "")
    if handshake_path:
        import json

        with open(handshake_path, "r", encoding="utf-8") as handle:
            handshake = json.load(handle)
        if not handshake.get("base_url"):
            raise ValueError(f"{handshake_path}: handshake has no base_url")
        return mtls_client(handshake)
    url = os.environ.get("AHP_STATE_URL", "")
    if not url:
        raise ValueError("set AHP_STATE_HANDSHAKE or AHP_STATE_URL")
    from ahp_state.client import StateAPIClient

    return StateAPIClient(url, token=os.environ.get("AHP_STATE_TOKEN") or None)


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    try:
        return int(raw) if raw else default
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None


def _seconds(name: str, default: float) -> float:
    """A duration in seconds, where a fraction is a legitimate request.

    Lease and heartbeat are milliseconds and stay integers; an idle *wait* is a
    poll interval, and an operator who asks to sit 0.2 s between empty claims is
    asking for something real. Refusing it here does not fail closed: the
    container dies at startup, the run the platform already queued stays
    ``pending``, and nothing on the State side ever says who stopped claiming.
    """
    raw = os.environ.get(name, "")
    try:
        return float(raw) if raw else default
    except ValueError:
        raise ValueError(f"{name} must be a number of seconds, got {raw!r}") from None


async def run(builder: GraphBuilder, *, max_turns: Optional[int] = None,
              stop: Optional[asyncio.Event] = None) -> int:
    """Serve runs until ``stop`` is set. Returns the process exit code."""
    transport = load_effect_transport(agent_module=os.environ.get("AHP_AGENT_MODULE", ""))
    if transport is None:
        log.warning("no effect transport configured: ctx.effects.call() will refuse "
                    "every effect this agent tries to make")
    else:
        log.info("effect transport: %s.%s", getattr(transport, "__module__", "?"),
                 getattr(transport, "__qualname__", "?"))
    # Validate options before allocating the owned HTTP pool.
    lease_ttl_ms = _int("AHP_LEASE_TTL_MS", 120_000)
    heartbeat_ms = _int("AHP_HEARTBEAT_MS", 30_000)
    idle_wait_s = _seconds("AHP_IDLE_WAIT_S", 1.0) or 1.0
    client = client_from_environment()
    service = AgentService(
        graph=builder,
        client=client,
        base_url=os.environ.get("AHP_STATE_URL", ""),
        worker=os.environ.get("AHP_WORKER_ID") or new_worker_id(),
        lease_ttl_ms=lease_ttl_ms,
        heartbeat_ms=heartbeat_ms,
        effects=transport,
        effect_node=os.environ.get("AHP_EFFECT_NODE", "agent"),
    )
    stop = stop or asyncio.Event()
    try:
        outcomes = await service.serve(max_turns=max_turns,
                                       idle_wait_s=idle_wait_s,
                                       stop=stop)
    finally:
        # the service closes what it made; this client is the container's own
        await service.close()
        await client.close()
    for outcome in outcomes[-1:]:
        if outcome.ok or not outcome.error:
            log.info("last turn: run=%s status=%s", outcome.run_id, outcome.status)
        else:
            # this log is the only thing an operator of a user image gets: a turn
            # that ends failed has to carry the reason State gave, or the
            # container goes mute exactly when someone needs to know what
            # happened. An error is a code/message pair, never the hand-out.
            log.error("last turn: run=%s status=%s error=%s",
                      outcome.run_id, outcome.status, outcome.error)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ahp-agent-container",
                                     description="Run a custom LangGraph agent on AHP")
    parser.add_argument("--module", default=os.environ.get("AHP_AGENT_MODULE", ""),
                        help="agent module (default: $AHP_AGENT_MODULE)")
    parser.add_argument("--entry", default=os.environ.get("AHP_AGENT_ENTRY", "build"),
                        help="builder attribute inside the module (default: build)")
    parser.add_argument("--max-turns", type=int,
                        default=int(os.environ.get("AHP_MAX_TURNS", "0")) or None,
                        help="exit after this many turns (0 = serve forever)")
    parser.add_argument("--log-level", default=os.environ.get("AHP_LOG_LEVEL", "INFO"))
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        builder = load_builder(args.module, args.entry)
    except (RuntimeError, ValueError) as exc:
        print(f"ahp-agent-container: {exc}", file=sys.stderr)
        return 1

    stop = asyncio.Event()

    async def serve() -> int:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:      # platforms without signal handlers
                pass
        return await run(builder, max_turns=args.max_turns, stop=stop)

    try:
        return asyncio.run(serve())
    except KeyboardInterrupt:                 # pragma: no cover - signal race
        return 0


if __name__ == "__main__":
    sys.exit(main())
