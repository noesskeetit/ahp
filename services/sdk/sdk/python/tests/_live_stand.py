"""One source of truth for a live State stand: the hand-out the platform mounts.

The real-stand modules in this directory were written against one particular
setup - an SSH tunnel to VM1 at ``https://127.0.0.1:18092`` with certificates in
``/tmp/state-stand-certs`` under fixed names - and they read those three inputs
from three separate variables, each defaulting to that setup. A lane that owns
its own stand and its own hand-out therefore could not run a single one of them:
the hand-out pointed somewhere real, the defaults pointed at a tunnel nobody had
open, and every test skipped with "Stand not configured" while a stand answered
two ports away. That is where ten skips in a live run came from.

The hand-out (``AHP_STATE_HANDSHAKE``) already carries ``base_url``, ``ca_file``,
``cert_file``, ``key_file`` and ``task_token``, so it is the source of truth
here. The tunnel variables (``AHP_STATE_URL`` / ``AHP_STATE_STAND_URL``,
``AHP_STATE_CERTS``, ``AHP_STATE_{CA_CERT,CLIENT_CERT,CLIENT_KEY,TOKEN}``) are
still honoured as overrides, so the workflow they were written for keeps working
- including its original, laxer hostname checking, which is the behaviour that
setup was validated with. A hand-out configuration is verified strictly, exactly
like the container entry point verifies it.

Nothing here turns a missing stand into a pass: an unconfigured stand still
skips, and the reason names the exact input that is absent.
"""

from __future__ import annotations

import asyncio
import json
import os
import ssl
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ahp_state.agent import mtls_client  # noqa: E402

DEFAULT_HANDSHAKE = "/tmp/state-handshake-local.json"
LEGACY_URL = "https://127.0.0.1:18092"
LEGACY_CERT_DIR = "/tmp/state-stand-certs"
LEGACY_CERT_NAMES = ("ca.pem", "client-cert.pem", "client-key.pem")

#: The ownership rule lives in one module (``ahp_state/tests/ownership.py``) so a
#: live suite and a drain loop cannot disagree about whose runs may be released.
from ahp_state.tests.ownership import (        # noqa: E402
    OWN_NAMESPACES, SHARED_QUEUE_HINT, run_is_ours, stand_is_exclusive)

def _env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return ""


def _from_handshake(path: str) -> dict:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    return {
        "base_url": data["base_url"],
        "ca_file": data["ca_file"],
        "cert_file": data.get("cert_file", ""),
        "key_file": data.get("key_file", ""),
        "task_token": data.get("task_token", ""),
        "source": f"hand-out {path}",
        "strict_hostname": True,
    }


def _from_tunnel() -> dict:
    url = _env("AHP_STATE_URL", "AHP_STATE_STAND_URL") or LEGACY_URL
    cert_dir = _env("AHP_STATE_CERTS")
    if cert_dir:
        ca, cert, key = (os.path.join(cert_dir, name) for name in LEGACY_CERT_NAMES)
    else:
        ca = _env("AHP_STATE_CA_CERT")
        cert = _env("AHP_STATE_CLIENT_CERT")
        key = _env("AHP_STATE_CLIENT_KEY")
    return {
        "base_url": url,
        "ca_file": ca,
        "cert_file": cert,
        "key_file": key,
        "task_token": _env("AHP_STATE_TOKEN"),
        "source": "tunnel variables (AHP_STATE_URL/AHP_STATE_CERTS)",
        "strict_hostname": False,      # the behaviour that setup was validated with
    }


def _missing(config: dict) -> list:
    absent = []
    if not config.get("base_url"):
        absent.append("base_url")
    for field in ("ca_file", "cert_file", "key_file"):
        value = config.get(field) or ""
        if not value:
            absent.append(field)
        elif not os.path.exists(value):
            absent.append(f"{field} ({value} does not exist)")
    return absent


def resolve() -> tuple:
    """Return ``(config, missing)``; ``config`` is None when nothing is usable."""
    hand_out = _env("AHP_STATE_HANDSHAKE") or DEFAULT_HANDSHAKE
    tunnel_named = bool(_env("AHP_STATE_URL", "AHP_STATE_STAND_URL", "AHP_STATE_CERTS",
                             "AHP_STATE_CA_CERT", "AHP_STATE_TOKEN"))
    candidates = []
    if _env("AHP_STATE_HANDSHAKE") or os.path.exists(DEFAULT_HANDSHAKE):
        candidates.append(hand_out)
    if tunnel_named:
        candidates.append(None)                     # None => build from tunnel vars
    for candidate in candidates:
        try:
            config = _from_handshake(candidate) if candidate else _from_tunnel()
        except (OSError, ValueError, KeyError):
            continue                                # unreadable hand-out: try the next source
        absent = _missing(config)
        if not absent:
            if config["task_token"]:
                return config, []
            return None, [f"no task token in {config['source']}"]
    if not candidates:
        return None, [f"no live stand: AHP_STATE_HANDSHAKE is unset and {DEFAULT_HANDSHAKE} "
                      f"is absent (tunnel variables are also unset)"]
    return None, [f"stand configured via {candidate or 'tunnel variables'} is incomplete"]


class LiveStand:
    """The stand this checkout talks to, or why there is not one."""

    def __init__(self) -> None:
        self.config, absent = resolve()
        self.reason = "; ".join(absent) if absent else "live stand is configured"

    @property
    def configured(self) -> bool:
        return self.config is not None

    @property
    def url(self) -> str:
        return self.config["base_url"] if self.config else ""

    @property
    def token(self) -> str:
        return self.config["task_token"] if self.config else ""

    def skip_mark(self):
        import pytest

        return pytest.mark.skipif(not self.configured, reason=self.reason)

    def skip_if_absent(self):
        import pytest

        if not self.configured:
            pytest.skip(self.reason)

    def context(self) -> ssl.SSLContext:
        if not self.configured:
            raise RuntimeError(self.reason)
        ctx = ssl.create_default_context(cafile=self.config["ca_file"])
        ctx.load_cert_chain(self.config["cert_file"], self.config["key_file"])
        if not self.config["strict_hostname"]:
            ctx.check_hostname = False
        return ctx

    def client(self, token: str | None = None):
        """A client on the hand-out's credentials; ``token=None`` means mTLS only."""
        if not self.configured:
            raise RuntimeError(self.reason)
        handshake = dict(self.config)
        if token is not None:
            handshake["task_token"] = token
        return mtls_client(handshake)


STAND = LiveStand()


async def _owns_run(client, run: dict) -> tuple:
    """Does this queued run belong to this checkout? Report why, too."""
    return await run_is_ours(client, run, STAND.config if STAND.configured else None)


async def claim_own_run(client, want_run_id: str, worker: str,
                        deadline_s: float = 60.0, ttl_ms: int = 120000) -> dict:
    """Claim ``want_run_id`` out of the scope's FIFO, clearing whoever is ahead.

    ``ClaimRun`` is FIFO for the whole (tenant, deployment, subject) scope and
    accepts no filter, so a straggler queued before ours - by an interrupted
    session, or by a test that queued work it never claimed - is handed out
    instead of the run we are waiting for.

    Stragglers are abandoned with ``status="failed"``, which is terminal, so the
    queue actually moves on. Re-queuing them with ``pending`` - the other legal
    choice - puts one straight back at the head and the claim never advances;
    both were measured against the live stand, along with the legal set itself
    (``done | failed | canceled | pending``).

    Abandoning is only correct for work this checkout queued, so it is now gated:
    a straggler on a thread whose context this harness never creates is left
    exactly as it is and the leg fails with its identity, on any stand that has
    not been declared exclusive (``stand_is_exclusive``). Failing loudly and
    building nothing is the honest outcome on a shared queue; releasing another
    role's run to ``failed`` would have looked like a pass here and cost them a
    leg whose effect may already be durable.
    """
    abandoned = 0
    deadline = time.monotonic() + deadline_s
    while True:
        lease = (await client.claim_run(f"{worker}-{abandoned}",
                                       ttl_ms=ttl_ms)).get("lease")
        if lease and lease["run"]["id"] == want_run_id:
            return lease
        if lease:
            ours, identity = await _owns_run(client, lease["run"])
            if not ours:
                raise AssertionError(
                    f"run {want_run_id} is blocked behind work this checkout did not "
                    "queue "
                    f"({identity}); the stand's queue is shared, and abandoning that "
                    "run would cancel another role's leg. Declare the stand exclusive "
                    "(hand-out \"exclusive\": true, or AHP_STAND_EXCLUSIVE=1) only when "
                    "it really belongs to this checkout."
                )
            abandoned += 1
            await client.release({"run_id": lease["run"]["id"],
                                  "worker": lease["worker"],
                                  "epoch": lease["epoch"]}, status="failed")
        if time.monotonic() > deadline:
            raise AssertionError(
                f"run {want_run_id} was never handed out within {deadline_s:.0f}s "
                f"({abandoned} stragglers abandoned ahead of it). Either the queue "
                f"is shared with another checkout, or the run was never queued."
            )
        await asyncio.sleep(0.2)
