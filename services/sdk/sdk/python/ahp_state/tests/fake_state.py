"""Contract double for the AHP State HTTP API (block 8 candidate).

This is NOT a permissive mock: it mirrors the observable semantics of
``internal/state`` + ``internal/stateapi`` that block 1 depends on, so a green
test here is evidence about the real platform and not about a friendly stub.

Mirrored behaviour (source of truth, read at :rev 3.2 base ``debf8b1``):

* transport    - ``POST /v1/state/{Operation}``, strict body (unknown field is
                 INVALID, like ``transport.ReadJSON`` + DisallowUnknownFields),
                 Bearer task token, JSON error envelope ``{"code","message"}``.
* identifiers  - ``validIdentifier`` charset/length, ``validPartVersion``.
* leases       - ``ClaimRun`` hands out a run + task token, every fenced op is
                 checked against (run_id, worker, epoch); a stale epoch fails
                 CONFLICT/STALE_EPOCH, a released lease fails LEASE_LOST.
* checkpoints  - ``PutCheckpoint`` is idempotent on (key, body-digest); the same
                 key with another body is KEY_REUSE_MISMATCH, a foreign body on
                 an existing public id is CONFLICT. Header + part refs + blobs
                 commit atomically and are visible in the response
                 (synchronous durability).
* parts        - reads never return blob bytes (only channel/version);
                 ``GetPart`` returns ``{"data": <base64>}`` for one
                 (thread, namespace, channel, version).
* pending write- - ``(checkpoint, task, index)`` is unique: an identical replay
                 is a no-op, a differing write is KEY_REUSE_MISMATCH; negative
                 indexes (LangGraph ``WRITES_IDX_MAP`` specials) are legal.
* KV           - ``expected_version`` is a real CAS; ``operation_key`` stores a
                 receipt with a request digest, replay returns the receipt and a
                 reused key with another digest is KEY_REUSE_MISMATCH.
* intents      - ``recorded -> dispatched -> succeeded|failed|unknown``,
                 ``RecordIntent`` replay reports ``recorded`` (never the current
                 status), only ``ReconcileIntent`` may resolve ``unknown``.
* inbox/stream - append-only with monotonic sequence numbers, ``AckInbox`` is
                 fenced.

Durability / restart fidelity knobs (used by the restart tests):

* ``server.drop_response_after_commit(op)`` - the write commits durably, then
  the response is lost (client sees a transport error). This is the exact window
  a restart must survive without repeating an external effect.
* ``server.dump()`` / ``FakeStateServer(state=...)`` - the store is a plain JSON
  document, so a test can hand it to a *real* second process and continue there.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import socket
import uuid
import string
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

# --------------------------------------------------------------------------
# wire constants (mirrored from internal/state / pkg/contracts)
# --------------------------------------------------------------------------

MAX_KEY_BYTES = 512
MAX_NAMESPACE_BYTES = 512
MAX_PART_BYTES = 1 << 20
MAX_BATCH_BYTES = 8 << 20

IDENT_CHARS = set(string.ascii_letters + string.digits + "._-:@/")

ERROR_STATUS = {
    "INVALID_ARGUMENT": 400,
    "KEY_REUSE_MISMATCH": 400,
    "UNAUTHENTICATED": 401,
    "PERMISSION_DENIED": 403,
    "NOT_FOUND": 404,
    "CONFLICT": 409,
    "STALE_EPOCH": 409,
    "LEASE_LOST": 409,
    "PRECONDITION_FAILED": 412,
    "INTERNAL": 500,
    "UNAVAILABLE": 503,
}


class StateError(Exception):
    """A wire error, shaped exactly like ``pkg/transport/json.go`` WriteError.

    ``reason`` is not decoration: live State flattens a domain code onto a
    coarser HTTP class - KEY_REUSE_MISMATCH answers HTTP 400 as
    INVALID_ARGUMENT - and puts the domain code in a sibling ``reason`` field.
    A double that answers ``code: KEY_REUSE_MISMATCH`` directly makes every
    ``exc.code == "KEY_REUSE_MISMATCH"`` guard in the SDK pass here and die
    against the platform, which flattens it. The SDK recovers the domain code
    from ``reason`` (``client._raise_typed``), so tests stay readable.
    """

    def __init__(self, code: str, message: str = "", reason: str = ""):
        self.code = code
        self.message = message
        self.reason = reason
        super().__init__(f"{code}: {message}")


def _invalid(msg: str) -> StateError:
    return StateError("INVALID_ARGUMENT", f"INVALID_ARGUMENT: {msg}")


def _key_reuse(msg: str) -> StateError:
    """HTTP 400 / code INVALID_ARGUMENT / reason KEY_REUSE_MISMATCH, as shipped."""
    return StateError("INVALID_ARGUMENT", f"KEY_REUSE_MISMATCH: {msg}",
                      reason="KEY_REUSE_MISMATCH")


def _forbidden(msg: str) -> StateError:
    return StateError("FORBIDDEN", f"auth: {msg}")


# The stand's own scope, the way the live stand derives it from the mTLS
# certificate and the task token - never from a request body.
SCOPE = {"tenant": "t-stand", "deployment": "d-stand", "subject": "u-stand"}

RELEASE_STATUSES = {"done", "failed", "canceled", "pending"}


def _valid_identifier(value: str, what: str) -> None:
    if value == "":
        raise _invalid(f"{what} must not be empty")
    if len(value.encode()) > MAX_KEY_BYTES:
        raise _invalid(f"{what} exceeds 512 bytes")
    for ch in value:
        if ch not in IDENT_CHARS:
            raise _invalid(f"{what} contains an unsafe identifier character")


def _digest(*parts: str) -> str:
    return hashlib.sha256("\x00".join(parts).encode()).hexdigest()


def _now_ts() -> str:
    """Wall clock shaped like ``internal/state/kv.go`` puts it on the wire.

    ``UpdatedAt`` is a Go ``time.Time`` tagged ``json:"updated_at"``, NOT NULL in
    ``kv_schema.go``, marshalled RFC3339. A stand that omits it lets the SDK
    drift into relying on - or crashing without - a field the real server always
    sends. See :meth:`AHPStore._ts`.
    """
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _issued_token(seq: int) -> str:
    """A task token with the SHAPE live State demands: exactly three segments.

    The platform verifies an Ed25519 signature (``internal/stateapi`` auth); a
    double cannot, so it enforces the one check it can - structure - and the
    stand's own mTLS certificate stands in for "no token presented". A forged
    bearer token is rejected exactly as live rejects it, which is the property
    the SDK relies on.
    """
    return f"run{seq:06d}.{seq:06d}.sig{seq:06d}"


def _check_token(token: str) -> None:
    if token and len(token.split(".")) != 3:
        raise _forbidden("invalid token: token must have exactly three segments")


def _expires_at(ttl_ms: int) -> str:
    deadline = datetime.now(timezone.utc) + timedelta(milliseconds=int(ttl_ms or 0))
    return deadline.isoformat(timespec="microseconds")


def _wire_lease(lease: dict) -> dict:
    """A lease as live State marshals it: the run, the thread, and the scope."""
    lease.setdefault("scope", dict(SCOPE))
    lease.setdefault("expires_at", _expires_at(120000))
    return lease


def _jsonb_contains(value: object, filter: object) -> bool:
    """PostgreSQL ``value @> filter`` containment, the operator ``SearchKV`` uses.

    Objects match key-by-key recursively; arrays match when every filter element
    is contained in *some* value element; scalars must be equal. Anything looser
    here makes a filtered store search pass on the stand and miss on the platform.
    """
    if isinstance(filter, dict):
        return isinstance(value, dict) and all(
            key in value and _jsonb_contains(value[key], wanted)
            for key, wanted in filter.items())
    if isinstance(filter, list):
        return isinstance(value, list) and all(
            any(_jsonb_contains(have, wanted) for have in value) for wanted in filter)
    if isinstance(filter, bool) is not isinstance(value, bool):
        return False       # JSON true/false never equals 1/0; Python says otherwise
    return value == filter


# --------------------------------------------------------------------------
# the store: a plain JSON document so it can cross a real process boundary
# --------------------------------------------------------------------------


class StateDoc:
    """The durable state of one deployment/tenant scope."""

    def __init__(self, state: dict | None = None):
        self.d = state if state is not None else {
            "seq": 0,
            "threads": {},        # thread_id -> {id, context_id, key, runs, deleted}
            "runs": {},           # run_id -> {id, thread_id, status, payload, epoch, ...}
            "leases": {},         # run_id -> {worker, epoch, status, ttl_ms}
            "tokens": {},         # task_token -> {run_id, thread_id, scope}
            "checkpoints": {},    # (thread, ns, public_id) -> header
            "cp_by_key": {},      # (thread, ns, key) -> public_id
            "blobs": {},          # (thread, ns, channel, version) -> b64
            "writes": {},         # (thread, ns, cp, task, index) -> {channel, data}
            "kv": {},             # (ns tuple, key) -> {version, value, deleted}
            "kv_receipts": {},    # (run, op_key) -> {digest, result}
            "intents": {},        # (run, node, call) -> intent
            "inbox": [],          # [{id, seq, thread_id, kind, payload, acked}]
            "stream": [],         # [{seq, thread_id, kind, payload}]
        }

    def next_seq(self) -> int:
        self.d["seq"] += 1
        return self.d["seq"]

    def save(self) -> str:
        return json.dumps(self.d, sort_keys=True)

    @staticmethod
    def load(raw: str) -> "StateDoc":
        return StateDoc(json.loads(raw))


# --------------------------------------------------------------------------
# the operation handler
# --------------------------------------------------------------------------


class FakeState:
    """Implements the State operations over one :class:`StateDoc`."""

    def __init__(self, doc: StateDoc | None = None):
        self.doc = doc or StateDoc()
        self.lock = threading.Lock()
        self.calls: list[str] = []
        self._drop_after_commit: set[str] = set()

    # -- test knobs ------------------------------------------------------

    def drop_response_after_commit(self, operation: str) -> None:
        """Commit durably, then lose the response (client sees a transport error)."""
        self._drop_after_commit.add(operation)

    # -- helpers ---------------------------------------------------------

    @staticmethod
    def _require(body: dict, allowed: set[str], required: set[str]) -> None:
        unknown = set(body) - allowed
        if unknown:
            raise _invalid("unknown request field " + sorted(unknown)[0])
        missing = required - set(body)
        if missing:
            raise _invalid(sorted(missing)[0] + " must not be empty")

    def _thread(self, thread_id: str) -> dict:
        t = self.doc.d["threads"].get(thread_id)
        if not t or t.get("deleted"):
            raise StateError("NOT_FOUND", "thread")
        return t

    @staticmethod
    def _wire_thread(t: dict) -> dict:
        """The thread as live State marshals it - ``key`` stays server-side."""
        return {"id": t["id"], "context_id": t["context_id"], "epoch": t["epoch"],
                "created_at": t["created_at"], "scope": dict(SCOPE)}

    @staticmethod
    def _wire_run(r: dict) -> dict:
        """The run as live State marshals it, idempotency ``key`` excluded.

        ``wait_for`` is ``omitempty`` on the live type (internal/state/types.go:73),
        so a run that is not parked answers WITHOUT the field rather than with an
        empty one. It matters: GetRun is the only way an executor can ask whether
        its own park is still there, which is exactly what a wrong shape hides.
        """
        wire = {"id": r["id"], "thread_id": r["thread_id"], "task_id": r["task_id"],
                "sequence": r["sequence"], "status": r["status"],
                "payload": r["payload"]}
        if r.get("wait_for"):
            wire["wait_for"] = r["wait_for"]
        return wire

    #: one message, every way a lease stops being yours - live State does not
    #: distinguish "released", "expired", "taken over" or "wrong worker". It has
    #: no PERMISSION_DENIED and no LEASE_LOST here at all: see
    #: ``internal/state/errorcodes_test.go`` (heartbeat after Release => STALE_EPOCH).
    _STALE = "lease epoch is stale or the lease has expired"

    def _lease(self, fence: dict) -> dict:
        run_id = fence.get("run_id", "")
        lease = self.doc.d["leases"].get(run_id)
        if not lease:
            raise StateError("NOT_FOUND", "lease")
        if lease["status"] != "active":
            raise StateError("STALE_EPOCH", f"STALE_EPOCH: {self._STALE}")
        if lease["epoch"] != fence.get("epoch"):
            raise StateError("STALE_EPOCH", f"STALE_EPOCH: {self._STALE}")
        if lease["worker"] != fence.get("worker"):
            raise StateError("STALE_EPOCH", f"STALE_EPOCH: {self._STALE}")
        return lease

    # -- operations ------------------------------------------------------

    def create_thread(self, body: dict, auth: dict) -> dict:
        self._require(body, {"context_id", "key"}, {"context_id", "key"})
        _valid_identifier(body["key"], "idempotency key")
        with self.lock:
            for t in self.doc.d["threads"].values():
                if t["context_id"] == body["context_id"]:
                    return self._wire_thread(t)
            for t in self.doc.d["threads"].values():
                if t["key"] == body["key"]:
                    # live: "idempotency key was already used with a different
                    # thread body" - the SAME key for a different context is a
                    # permanent reject, not a silent reuse of the old thread.
                    if t["context_id"] != body["context_id"]:
                        raise _key_reuse(
                            "idempotency key was already used with a different "
                            "thread body")
                    return self._wire_thread(t)
            t = {"id": f"thr-{self.doc.next_seq():06d}",
                 "context_id": body["context_id"], "key": body["key"],
                 "epoch": 0, "created_at": _now_ts(), "deleted": False}
            self.doc.d["threads"][t["id"]] = t
            return self._wire_thread(t)

    def get_thread(self, body: dict, auth: dict) -> dict:
        self._require(body, {"thread_id", "context_id"}, set())
        if body.get("thread_id"):
            return self._wire_thread(self._thread(body["thread_id"]))
        for t in self.doc.d["threads"].values():
            if t["context_id"] == body.get("context_id"):
                return self._wire_thread(t)
        raise StateError("NOT_FOUND", "thread")

    def get_thread_by_context(self, body: dict, auth: dict) -> dict:
        return self.get_thread({"context_id": body.get("context_id", "")}, auth)

    def list_threads(self, body: dict, auth: dict) -> dict:
        self._require(body, {"limit", "cursor"}, set())
        threads = [self._wire_thread(t)
                   for t in self.doc.d["threads"].values() if not t.get("deleted")]
        return {"threads": threads[: body.get("limit") or 100], "next_cursor": ""}

    def create_run(self, body: dict, auth: dict) -> dict:
        self._require(body, {"thread_id", "key", "payload", "task_id"},
                      {"thread_id", "key"})
        _valid_identifier(body["key"], "idempotency key")
        payload = body.get("payload")
        with self.lock:
            thread = self._thread(body["thread_id"])
            for run in self.doc.d["runs"].values():
                if run["thread_id"] == thread["id"] and run["key"] == body["key"]:
                    if json.dumps(run["payload"], sort_keys=True) != \
                            json.dumps(payload, sort_keys=True):
                        raise _key_reuse(
                            "idempotency key was already used with a different "
                            "run body")
                    return self._wire_run(run)
            rid = f"run-{self.doc.next_seq():06d}"
            run = {"id": rid, "thread_id": thread["id"], "key": body["key"],
                   "task_id": body.get("task_id") or "task-bootstrap",
                   "sequence": len(self.doc.d["runs"]) + 1,
                   "status": "pending", "payload": payload}
            self.doc.d["runs"][rid] = run
            return self._wire_run(run)

    def claim_run(self, body: dict, auth: dict) -> dict:
        self._require(body, {"worker", "ttl_ms", "run_id"}, {"worker"})
        run_id = body.get("run_id")
        with self.lock:
            run = next((r for r in self.doc.d["runs"].values()
                        if r["status"] == "pending"
                        and (not run_id or r["id"] == run_id)), None)
            if run is None:
                return {"lease": None, "task_token": ""}
            run["status"] = "running"
            token = _issued_token(self.doc.next_seq())
            self.doc.d["leases"][run["id"]] = {
                "run_id": run["id"], "worker": body["worker"], "epoch": 1,
                "status": "active", "ttl_ms": body.get("ttl_ms", 60000),
            }
            self.doc.d["tokens"][token] = {"run_id": run["id"],
                                           "thread_id": run["thread_id"]}
            return {"lease": _wire_lease({
                        "run": self._wire_run(run),
                        "thread": self._wire_thread(self._thread(run["thread_id"])),
                        "worker": body["worker"], "epoch": 1,
                        "expires_at": _expires_at(body.get("ttl_ms", 60000))}),
                    "task_token": token}

    def _reclaim(self, run_id: str, worker: str) -> dict:
        """Test helper: drop the current holder and re-claim (new epoch)."""
        with self.lock:
            lease = self.doc.d["leases"][run_id]
            lease["worker"] = worker
            lease["epoch"] += 1
            token = _issued_token(self.doc.next_seq())
            self.doc.d["tokens"][token] = {"run_id": run_id,
                                           "thread_id": self.doc.d["runs"][run_id]["thread_id"]}
            return {"lease": _wire_lease(dict(lease)), "task_token": token}

    def heartbeat(self, body: dict, auth: dict) -> dict:
        self._require(body, {"run_id", "worker", "epoch", "ttl_ms"},
                      {"run_id", "worker", "epoch"})
        with self.lock:
            lease = self._lease(body)
            if body.get("ttl_ms"):
                lease["ttl_ms"] = body["ttl_ms"]
            return {"ok": True, "ttl_ms": lease["ttl_ms"]}

    def get_run(self, body: dict, auth: dict) -> dict:
        """``GET GetRun``: the run row, unfenced, as ``state.Run`` marshals it.

        A parked executor is the one reader that has to ask this question without a
        lease - ``Suspend`` gave the lease up - so an unfenced read of ``status`` and
        ``wait_for`` is what tells it whether its park is still there.
        """
        self._require(body, {"run_id"}, {"run_id"})
        with self.lock:
            run = self.doc.d["runs"].get(body["run_id"])
            if run is None:
                raise StateError("NOT_FOUND", "run")
            return self._wire_run(run)

    def release(self, body: dict, auth: dict) -> dict:
        self._require(body, {"run_id", "worker", "epoch", "status",
                             "close_reason", "failure_cause"},
                      {"run_id", "worker", "epoch"})
        status = body.get("status", "done")
        if status not in RELEASE_STATUSES:
            raise _invalid("release status must be done, failed, canceled or pending")
        # Mirror internal/state's release() (DEF-RELEASE-REASON-1), so this double is
        # never more permissive than the door it models: a stated reason belongs to an
        # ended run, and a failed or canceled run cannot end without one. done is
        # deliberately not covered -- its verdict is the checkpoint, and honest done
        # callers end it unstated. Both keys are optional on the wire; empty = absent.
        close_reason = body.get("close_reason", "")
        failure_cause = body.get("failure_cause", "")
        terminal = status in {"done", "failed", "canceled"}
        if (close_reason or failure_cause) and not terminal:
            raise _invalid("close_reason may only be recorded on a terminal transition")
        if status in {"failed", "canceled"} and not (close_reason or failure_cause):
            raise _invalid(
                "release may only end a run with a stated reason (close_reason and/or failure_cause)")
        with self.lock:
            lease = self._lease(body)
            lease["status"] = "released"
            run = self.doc.d["runs"][body["run_id"]]
            run["status"] = status
            # journal the stated reason into the run's existing payload document, the way
            # the real door does, so a leg can assert the terminal row is not silent.
            if close_reason or failure_cause:
                payload = dict(run.get("payload") or {})
                if close_reason:
                    payload["close_reason"] = close_reason
                if failure_cause:
                    payload["failure_cause"] = failure_cause
                run["payload"] = payload
            return {"ok": True}

    def suspend(self, body: dict, auth: dict) -> dict:
        self._require(body, {"run_id", "worker", "epoch", "wait_for", "deadline"},
                      {"run_id", "worker", "epoch"})
        wait_for = body.get("wait_for", "")
        with self.lock:
            lease = self._lease(body)
            lease["status"] = "suspended"
            run = self.doc.d["runs"][body["run_id"]]
            # The live row is `waiting` and carries the `wait_for` that the wake
            # rule matches on (internal/state/events.go, runColumns in
            # internal/state/types.go). A double that calls this state anything
            # else cannot show a client which vocabulary its answer has to speak,
            # and that is exactly how a mismatched wait_for stayed invisible here.
            run["status"] = "waiting"
            run["wait_for"] = wait_for
            return {"ok": True, "wait_for": wait_for}

    # -- checkpoints -----------------------------------------------------

    @staticmethod
    def _cp_key(thread: str, ns: str, public_id: str) -> str:
        return json.dumps([thread, ns, public_id])

    def _checkpoint_fp(self, thread: str, ns: str, cp: dict) -> str:
        parts = [{"channel": p["channel"], "version": p["version"],
                  "data": p.get("data", "")} for p in cp.get("parts", [])]
        return _digest("cp", thread, ns, cp["id"], cp.get("parent", ""),
                       str(cp.get("version", 1)),
                       json.dumps(cp.get("metadata"), sort_keys=True),
                       json.dumps(sorted(parts, key=lambda p: p["channel"]), sort_keys=True))

    def put_checkpoint(self, body: dict, auth: dict) -> dict:
        self._require(body, {"fence", "checkpoint", "key"}, {"fence", "checkpoint", "key"})
        cp = body["checkpoint"]
        key = body["key"]
        _valid_identifier(key, "idempotency key")
        _valid_identifier(cp.get("id", ""), "checkpoint id")
        if cp.get("version", 1) < 1:
            raise _invalid("checkpoint version must be positive")
        if cp.get("parent") == cp.get("id"):
            raise _invalid("checkpoint parent must differ from the checkpoint id")
        ns = cp.get("namespace", "")
        if len(ns.encode()) > MAX_NAMESPACE_BYTES:
            raise _invalid("checkpoint namespace exceeds 512 bytes")
        seen: set[str] = set()
        total = 0
        for p in cp.get("parts", []):
            _valid_identifier(p.get("channel", ""), "channel")
            if not p.get("version"):
                raise _invalid("part version must not be empty")
            if p["channel"] in seen:
                raise _invalid(f"channel {p['channel']} appears more than once")
            seen.add(p["channel"])
            raw = base64.b64decode(p.get("data", "") or "")
            if len(raw) > MAX_PART_BYTES:
                raise _invalid("part data exceeds 1MiB")
            total += len(raw)
        if total > MAX_BATCH_BYTES:
            raise _invalid("checkpoint batch exceeds 8MiB")

        with self.lock:
            lease = self._lease(body["fence"])
            thread_id = self.doc.d["runs"][lease["run_id"]]["thread_id"]
            fp = self._checkpoint_fp(thread_id, ns, cp)

            by_key = self.doc.d["cp_by_key"]
            k_key = json.dumps([thread_id, ns, key])
            if k_key in by_key:
                stored = self.doc.d["checkpoints"][self._cp_key(thread_id, ns, by_key[k_key])]
                if stored["digest"] != fp:
                    raise _key_reuse(
                                                                          "checkpoint key was already used with a different body")
                return {"checkpoint": _public_cp(stored)}

            c_key = self._cp_key(thread_id, ns, cp["id"])
            if c_key in self.doc.d["checkpoints"]:
                stored = self.doc.d["checkpoints"][c_key]
                if stored["digest"] == fp:
                    return {"checkpoint": _public_cp(stored)}
                if stored["key"] == key:
                    raise _key_reuse(
                                                                          "checkpoint key was already used with a different body")
                raise StateError("CONFLICT",
                                 "checkpoint id already exists in this thread namespace")

            if cp.get("parent"):
                if self._cp_key(thread_id, ns, cp["parent"]) not in self.doc.d["checkpoints"]:
                    raise StateError("NOT_FOUND", "checkpoint")

            header = {
                "id": cp["id"], "thread_id": thread_id, "namespace": ns,
                "parent": cp.get("parent", ""), "version": cp.get("version", 1),
                "metadata": cp.get("metadata"), "digest": fp, "key": key,
                "created_at": f"{self.doc.next_seq():012d}",
                "parts": sorted([{"channel": p["channel"], "version": p["version"]}
                                 for p in cp.get("parts", [])],
                                key=lambda p: p["channel"]),
            }
            for p in cp.get("parts", []):
                blob_key = json.dumps([thread_id, ns, p["channel"], p["version"]])
                existing = self.doc.d["blobs"].get(blob_key)
                if existing is not None and existing != p.get("data", ""):
                    raise _key_reuse(
                                                                          "checkpoint version already exists with different data")
                self.doc.d["blobs"][blob_key] = p.get("data", "")
            self.doc.d["checkpoints"][c_key] = header
            by_key[k_key] = cp["id"]
            return {"checkpoint": _public_cp(header)}

    def get_checkpoint(self, body: dict, auth: dict) -> dict:
        self._require(body, {"thread_id", "namespace", "id"}, {"thread_id", "namespace"})
        with self.lock:
            self._thread(body["thread_id"])
            ns = body["namespace"]
            if body.get("id"):
                header = self.doc.d["checkpoints"].get(
                    self._cp_key(body["thread_id"], ns, body["id"]))
                if header is None:
                    raise StateError("NOT_FOUND", "checkpoint")
                return _public_cp(header)
            found = sorted(
                (h for h in self.doc.d["checkpoints"].values()
                 if h["thread_id"] == body["thread_id"] and h["namespace"] == ns),
                key=lambda h: h["created_at"])
            if not found:
                raise StateError("NOT_FOUND", "checkpoint")
            return _public_cp(found[-1])

    def list_checkpoints(self, body: dict, auth: dict) -> dict:
        self._require(body, {"thread_id", "namespace", "limit"}, {"thread_id", "namespace"})
        with self.lock:
            self._thread(body["thread_id"])
            found = sorted(
                (h for h in self.doc.d["checkpoints"].values()
                 if h["thread_id"] == body["thread_id"] and h["namespace"] == body["namespace"]),
                key=lambda h: h["created_at"], reverse=True)
            limit = body.get("limit") or 1000
            return {"checkpoints": [_public_cp(h) for h in found[:limit]]}

    def get_part(self, body: dict, auth: dict) -> dict:
        self._require(body, {"thread_id", "namespace", "channel", "version"},
                      {"thread_id", "namespace", "channel", "version"})
        with self.lock:
            self._thread(body["thread_id"])
            blob = self.doc.d["blobs"].get(json.dumps(
                [body["thread_id"], body["namespace"], body["channel"], body["version"]]))
            if blob is None:
                raise StateError("NOT_FOUND", "checkpoint blob")
            return {"data": blob}

    def put_writes(self, body: dict, auth: dict) -> dict:
        self._require(body, {"fence", "namespace", "checkpoint_id", "writes"},
                      {"fence", "namespace", "checkpoint_id", "writes"})
        for w in body["writes"]:
            _valid_identifier(w.get("task", ""), "task")
            _valid_identifier(w.get("channel", ""), "channel")
            if not isinstance(w.get("index"), int) or abs(w["index"]) > 2 ** 31 - 1:
                raise _invalid("write index must fit a signed int32")
            if len(base64.b64decode(w.get("data", "") or "")) > MAX_PART_BYTES:
                raise _invalid("pending write exceeds 1MiB")
        seen: set[tuple] = set()
        for w in body["writes"]:
            k = (w["task"], w["index"])
            if k in seen:
                raise _invalid("duplicate pending write for the same task and index")
            seen.add(k)

        with self.lock:
            lease = self._lease(body["fence"])
            thread_id = self.doc.d["runs"][lease["run_id"]]["thread_id"]
            for w in body["writes"]:
                k = json.dumps([thread_id, body["namespace"], body["checkpoint_id"],
                                w["task"], w["index"]])
                stored = self.doc.d["writes"].get(k)
                if stored is None:
                    self.doc.d["writes"][k] = {"task": w["task"], "index": w["index"],
                                               "channel": w["channel"], "data": w.get("data", "")}
                elif stored["data"] != w.get("data", "") or stored["channel"] != w["channel"]:
                    raise _key_reuse(
                                                                          "pending write exists with a different payload")
            return {"ok": True}

    def get_writes(self, body: dict, auth: dict) -> dict:
        self._require(body, {"thread_id", "namespace", "checkpoint_id"},
                      {"thread_id", "namespace", "checkpoint_id"})
        with self.lock:
            self._thread(body["thread_id"])
            want = [body["thread_id"], body["namespace"], body["checkpoint_id"]]
            rows = [w for k, w in self.doc.d["writes"].items() if json.loads(k)[:3] == want]
            rows.sort(key=lambda w: (w["task"], w["index"]))
            return {"writes": [{"task": w["task"], "index": w["index"],
                                "channel": w["channel"], "data": w["data"]} for w in rows]}

    def delete_thread(self, body: dict, auth: dict) -> dict:
        self._require(body, {"thread_id"}, {"thread_id"})
        with self.lock:
            thread = self._thread(body["thread_id"])
            thread["deleted"] = True
            tid = thread["id"]
            for coll, idx in (("checkpoints", 0), ("writes", 0)):
                for k in [k for k in self.doc.d[coll] if json.loads(k)[idx] == tid]:
                    del self.doc.d[coll][k]
            for k in [k for k in self.doc.d["blobs"] if json.loads(k)[0] == tid]:
                del self.doc.d["blobs"][k]
            return {"ok": True}

    # -- KV --------------------------------------------------------------

    @staticmethod
    def _kv_key(ns: list[str], key: str) -> str:
        return json.dumps([list(ns), key])

    def _op_receipt(self, fence: dict, op_key: str, request_digest: str,
                    apply_fn) -> dict:
        """Replay receipts are keyed on (scope, operation_key) - NOT on the run.

        ``internal/state/kv.go`` looks the receipt up by
        ``tenant, deployment, subject, operation_key``, so one operation key is
        spent for the whole subject and stays spent after a restart or a lease
        takeover. Keying it on the run too (as this double used to) lets a test
        reuse a key across runs and never see the collision the platform raises.
        """
        rk = op_key
        receipts = self.doc.d["kv_receipts"]
        if rk in receipts:
            stored = receipts[rk]
            if stored["digest"] != request_digest:
                raise _key_reuse("operation key was already used with a different "
                                 "kv request")
            return stored["result"]
        result = apply_fn()
        receipts[rk] = {"digest": request_digest, "result": result}
        return result

    def put_kv(self, body: dict, auth: dict) -> dict:
        self._require(body, {"fence", "namespace", "key", "operation_key",
                             "expected_version", "value"},
                      {"fence", "namespace", "key", "value"})
        with self.lock:
            self._lease(body["fence"])
            k = self._kv_key(body["namespace"], body["key"])
            request_digest = _digest("put", k, str(body.get("expected_version", 0)),
                                     json.dumps(body["value"], sort_keys=True))

            def apply():
                item = self.doc.d["kv"].get(k)
                live = item if item and not item["deleted"] else None
                baseline = (item["version"] if item else 0)
                expected = body.get("expected_version", 0)
                if expected == 0:
                    if live is not None:
                        raise StateError("CONFLICT", "kv item already exists")
                elif baseline != expected:
                    raise StateError("CONFLICT", "version mismatch")
                version = baseline + 1
                self.doc.d["kv"][k] = {"version": version, "value": body["value"],
                                       "deleted": False, "updated_at": _now_ts()}
                return self._wire_kv(body["namespace"], body["key"],
                                     self.doc.d["kv"][k], body["fence"])

            if body.get("operation_key"):
                return self._op_receipt(body["fence"], body["operation_key"],
                                        request_digest, apply)
            return apply()

    def delete_kv(self, body: dict, auth: dict) -> dict:
        self._require(body, {"fence", "namespace", "key", "operation_key",
                             "expected_version"}, {"fence", "namespace", "key"})
        with self.lock:
            self._lease(body["fence"])
            k = self._kv_key(body["namespace"], body["key"])
            request_digest = _digest("delete", k, str(body.get("expected_version", 0)))

            def apply():
                item = self.doc.d["kv"].get(k)
                if not item or item["deleted"]:
                    raise StateError("NOT_FOUND", "kv item")
                if item["version"] != body.get("expected_version"):
                    raise StateError("CONFLICT", "version mismatch")
                item["deleted"] = True
                return {"ok": True}

            if body.get("operation_key"):
                return self._op_receipt(body["fence"], body["operation_key"],
                                        request_digest, apply)
            return apply()

    def _wire_kv(self, namespace: list[str], key: str, item: dict,
                 fence: dict | None = None) -> dict:
        """``{key, namespace, thread_id, updated_at, value, version}`` - live's shape.

        A read has no lease, so ``thread_id`` is only known on the fenced path;
        live State answers it on both, and a consumer that keys off it must not
        be able to pass here and fail there.
        """
        run_id = (fence or {}).get("run_id", "")
        thread_id = self.doc.d["runs"].get(run_id, {}).get("thread_id", "")
        return {"key": key, "namespace": list(namespace), "thread_id": thread_id,
                "value": item["value"], "version": item["version"],
                "updated_at": item.get("updated_at") or _now_ts()}

    def get_kv(self, body: dict, auth: dict) -> dict:
        self._require(body, {"namespace", "key"}, {"namespace", "key"})
        with self.lock:
            item = self.doc.d["kv"].get(self._kv_key(body["namespace"], body["key"]))
            if not item or item["deleted"]:
                raise StateError("NOT_FOUND", "NOT_FOUND: kv item not found")
            return self._wire_kv(body["namespace"], body["key"], item)

    def search_kv(self, body: dict, auth: dict) -> dict:
        self._require(body, {"namespace_prefix", "key_prefix", "filter", "limit"},
                      {"namespace_prefix"})
        prefix = list(body["namespace_prefix"])
        filter_by = body.get("filter") or {}
        key_prefix = body.get("key_prefix") or ""
        limit = body.get("limit") or 100
        limit = min(max(int(limit), 1), 1000)     # kv.go clamps to 1..1000
        out = []
        with self.lock:
            for k, item in self.doc.d["kv"].items():
                ns, key = json.loads(k)
                if item["deleted"]:
                    continue
                if ns[:len(prefix)] != prefix:
                    continue
                if key_prefix and not key.startswith(key_prefix):
                    continue
                if filter_by and not _jsonb_contains(item["value"], filter_by):
                    continue
                out.append({"namespace": ns, "key": key, "value": item["value"],
                            "version": item["version"],
                            "updated_at": item.get("updated_at") or _now_ts()})
        # kv.go: ORDER BY namespace, key - a stable structural order, not
        # insertion order, or a limit picks different rows on every restart.
        out.sort(key=lambda row: (row["namespace"], row["key"]))
        return {"items": out[:limit]}

    # -- intents ---------------------------------------------------------
    # Mirrors internal/state/intents.go: the fingerprint is
    # digest(kind,target,args); a replay returns the intent with its CURRENT
    # status; transitions are monotonic with from == to allowed; a completion
    # replays only byte-identically; only ReconcileIntent may resolve unknown,
    # and only to succeeded|failed with the exact fingerprint.

    @staticmethod
    def _intent_key(run: str, node: str, call: str) -> str:
        return json.dumps([run, node, call])

    _INTENT_NEXT = {
        "recorded": {"dispatched", "fenced"},
        "dispatched": {"succeeded", "failed", "unknown", "fenced"},
        "unknown": {"fenced"},
        "succeeded": set(),
        "failed": set(),
        "fenced": set(),
    }

    def record_intent(self, body: dict, auth: dict) -> dict:
        self._require(body, {"fence", "node", "call", "kind", "target", "args"},
                      {"fence", "node", "call", "kind"})
        # live State decodes into typed fields and says so: a body that puts an
        # object where ``target`` is a string is refused, not stored. A double
        # that stores it lets a caller ship an intent it can never write.
        for field in ("node", "call", "kind", "target"):
            if field in body and not isinstance(body[field], str):
                raise _invalid(f'transport: field "{field}" is not of the '
                               f"expected type")
        digest = _digest("intent", body.get("kind", ""), body.get("target", ""),
                         json.dumps(body.get("args"), sort_keys=True))
        with self.lock:
            self._lease(body["fence"])
            k = self._intent_key(body["fence"]["run_id"], body["node"], body["call"])
            stored = self.doc.d["intents"].get(k)
            if stored is not None:
                if stored["digest"] != digest:
                    raise _key_reuse("intent key reused with a different fingerprint")
                return dict(stored)            # the CURRENT status, not "recorded"
            self.doc.d["intents"][k] = {"id": f"int-{self.doc.next_seq():06d}",
                                        "run_id": body["fence"]["run_id"],
                                        "node": body["node"], "call": body["call"],
                                        "kind": body["kind"],
                                        "target": body.get("target", ""),
                                        "digest": digest, "status": "recorded",
                                        "result": None}
            return dict(self.doc.d["intents"][k])

    def get_intent(self, body: dict, auth: dict) -> dict:
        self._require(body, {"run_id", "node", "call"}, {"run_id", "node", "call"})
        with self.lock:
            intent = self.doc.d["intents"].get(
                self._intent_key(body["run_id"], body["node"], body["call"]))
            if intent is None:
                raise StateError("NOT_FOUND", "intent")
            return dict(intent)

    def update_intent(self, body: dict, auth: dict) -> dict:
        self._require(body, {"fence", "node", "call", "status", "result"},
                      {"fence", "node", "call", "status"})
        if body["status"] not in {"recorded", "dispatched", "succeeded", "failed",
                                  "unknown", "fenced"}:
            raise _invalid("unknown intent status")
        with self.lock:
            self._lease(body["fence"])
            k = self._intent_key(body["fence"]["run_id"], body["node"], body["call"])
            intent = self.doc.d["intents"].get(k)
            if intent is None:
                raise StateError("NOT_FOUND", "intent")
            frm, to = intent["status"], body["status"]
            if frm != to and to not in self._INTENT_NEXT[frm]:
                if frm == "unknown":
                    raise StateError("PRECONDITION_FAILED", "intent needs reconciliation")
                raise StateError("CONFLICT",
                                 f"illegal intent transition {frm} -> {to}")
            if to in ("succeeded", "failed") and frm == to:
                if json.dumps(intent["result"], sort_keys=True) != \
                        json.dumps(body.get("result"), sort_keys=True):
                    raise StateError("CONFLICT",
                                     "intent already completed with a different result")
                return dict(intent)
            intent["status"] = to
            if body.get("result") is not None:
                intent["result"] = body["result"]
            return dict(intent)

    def reconcile_intent(self, body: dict, auth: dict) -> dict:
        self._require(body, {"fence", "node", "call", "status", "digest", "result"},
                      {"fence", "node", "call", "status", "digest"})
        if body["status"] not in ("succeeded", "failed"):
            raise _invalid("reconciliation target must be succeeded or failed")
        with self.lock:
            self._lease(body["fence"])
            k = self._intent_key(body["fence"]["run_id"], body["node"], body["call"])
            intent = self.doc.d["intents"].get(k)
            if intent is None:
                raise StateError("NOT_FOUND", "intent")
            if intent["status"] != "unknown":
                raise StateError("CONFLICT", "only unknown intents can be reconciled")
            if body["digest"] != intent["digest"]:
                raise _key_reuse("reconciliation fingerprint mismatch")
            intent["status"] = body["status"]
            if body.get("result") is not None:
                intent["result"] = body["result"]
            return dict(intent)

    # -- inbox / stream --------------------------------------------------

    def enqueue(self, body: dict, auth: dict) -> dict:
        self._require(body, {"thread_id", "key", "kind", "payload"},
                      {"thread_id", "key", "kind"})
        with self.lock:
            self._thread(body["thread_id"])
            for m in self.doc.d["inbox"]:
                if m["thread_id"] == body["thread_id"] and m["key"] == body["key"]:
                    # A replayed event still gets the wake rule, because the live
                    # transaction applies it on every Enqueue: a duplicate answer
                    # must never be the reason a run stays parked.
                    self._wake_waiting_head_locked(body["thread_id"], body["kind"])
                    return {"id": m["id"], "seq": m["seq"], "replayed": True}
            seq = self.doc.next_seq()
            msg = {"id": f"msg-{seq:06d}", "seq": seq, "thread_id": body["thread_id"],
                   "key": body["key"], "kind": body["kind"],
                   "payload": body.get("payload"), "acked": False}
            self.doc.d["inbox"].append(msg)
            self._wake_waiting_head_locked(body["thread_id"], body["kind"])
            return {"id": msg["id"], "seq": seq}

    def _wake_waiting_head_locked(self, thread_id: str, kind: str) -> str:
        """`internal/state/events.go`: an inbox event whose kind equals the head
        run's `wait_for` - or a head waiting on `*` - goes back to `pending` in the
        same transaction, same run id and same FIFO position. Only the HEAD of the
        thread is a candidate, so an event can never overtake older work.

        Returns the woken run id for this double's own assertions; the wire answer
        to the caller stays what the server answers.
        """
        terminal = ("done", "failed", "canceled")
        head = None
        for run in self.doc.d["runs"].values():
            if run["thread_id"] != thread_id or run["status"] in terminal:
                continue
            if head is None or run["sequence"] < head["sequence"]:
                head = run
        if head is None or head["status"] != "waiting":
            return ""
        if head.get("wait_for", "") not in (kind, "*"):
            return ""
        head["status"] = "pending"
        head["wait_for"] = ""
        return head["id"]

    def read_inbox(self, body: dict, auth: dict) -> dict:
        self._require(body, {"thread_id", "after", "limit"}, {"thread_id"})
        after = body.get("after", 0)
        with self.lock:
            msgs = [m for m in self.doc.d["inbox"]
                    if m["thread_id"] == body["thread_id"] and m["seq"] > after
                    # the real query carries `AND e.acked = false`
                    # (internal/state/events.go ReadInbox): an acknowledged event is
                    # simply not in the answer. A double that redelivers it hides
                    # every consumer that counts on "unread means unacked".
                    and not m["acked"]]
        msgs.sort(key=lambda m: m["seq"])
        return {"events": [self._wire_inbox(m) for m in msgs[: body.get("limit") or 100]]}

    @staticmethod
    def _wire_inbox(event: dict) -> dict:
        """One event as ``state.InboxEvent`` puts it on the wire.

        The names are the server's: the envelope is ``events`` (``handlers.go``), the
        cursor is ``sequence``, and there is no ``next_after`` to page by - a
        consumer that reads either of the private names works against this double
        and fails against a live State, which is the whole reason this exists.
        """
        return {"id": event["id"], "thread_id": event["thread_id"], "key": event["key"],
                "kind": event["kind"], "sequence": event["seq"],
                "payload": event["payload"], "acked": event["acked"]}

    def ack_inbox(self, body: dict, auth: dict) -> dict:
        self._require(body, {"fence", "ids"}, {"fence", "ids"})
        with self.lock:
            self._lease(body["fence"])
            n = 0
            for m in self.doc.d["inbox"]:
                if m["id"] in body["ids"] and not m["acked"]:
                    m["acked"] = True
                    n += 1
            return {"acked": n}

    def append_stream(self, body: dict, auth: dict) -> dict:
        self._require(body, {"fence", "key", "kind", "payload"}, {"fence", "kind"})
        with self.lock:
            lease = self._lease(body["fence"])
            seq = self.doc.next_seq()
            self.doc.d["stream"].append({"seq": seq,
                                         "thread_id": self.doc.d["runs"][lease["run_id"]]["thread_id"],
                                         # the real StreamEvent carries the writer's key
                                         # (internal/state/events.go), and it is the key -
                                         # not the kind - that attributes a frame to a
                                         # turn, so a double without it cannot see an
                                         # unattributable frame at all
                                         "key": body["key"],
                                         "kind": body["kind"], "payload": body.get("payload")})
            return {"seq": seq}

    def read_stream(self, body: dict, auth: dict) -> dict:
        self._require(body, {"thread_id", "after", "limit"}, {"thread_id"})
        after = body.get("after", 0)
        events = [e for e in self.doc.d["stream"]
                  if e["thread_id"] == body["thread_id"] and e["seq"] > after]
        events.sort(key=lambda e: e["seq"])
        # ``state.StreamEvent`` names its cursor ``sequence`` and the handler answers
        # with the bare ``events`` array - no next cursor field at all.
        return {"events": [{"id": e.get("id", ""), "thread_id": e["thread_id"],
                            "key": e["key"], "kind": e["kind"], "sequence": e["seq"],
                            "payload": e["payload"]}
                           for e in events[: body.get("limit") or 100]]}


def _public_cp(header: dict) -> dict:
    """The checkpoint view the server returns: part *references*, never bytes."""
    return {
        "id": header["id"], "thread_id": header["thread_id"],
        "namespace": header["namespace"], "parent": header.get("parent", ""),
        "version": header["version"], "metadata": header.get("metadata"),
        "parts": [dict(p) for p in header.get("parts", [])],
        "created_at": header["created_at"],
    }


# --------------------------------------------------------------------------
# HTTP transport
# --------------------------------------------------------------------------


class LostResponse(Exception):
    """Raised on the client side when the server committed but the response died."""


OPS = {
    "CreateThread": FakeState.create_thread, "GetThread": FakeState.get_thread,
    "GetThreadByContext": FakeState.get_thread_by_context,
    "ListThreads": FakeState.list_threads, "CreateRun": FakeState.create_run,
    "ClaimRun": FakeState.claim_run, "Heartbeat": FakeState.heartbeat,
    "GetRun": FakeState.get_run,
    "Release": FakeState.release, "Suspend": FakeState.suspend,
    "PutCheckpoint": FakeState.put_checkpoint, "GetCheckpoint": FakeState.get_checkpoint,
    "ListCheckpoints": FakeState.list_checkpoints, "GetPart": FakeState.get_part,
    "PutWrites": FakeState.put_writes, "GetWrites": FakeState.get_writes,
    "DeleteThread": FakeState.delete_thread, "PutKV": FakeState.put_kv,
    "DeleteKV": FakeState.delete_kv, "GetKV": FakeState.get_kv,
    "SearchKV": FakeState.search_kv, "RecordIntent": FakeState.record_intent,
    "GetIntent": FakeState.get_intent, "UpdateIntent": FakeState.update_intent,
    "ReconcileIntent": FakeState.reconcile_intent, "Enqueue": FakeState.enqueue,
    "ReadInbox": FakeState.read_inbox, "AckInbox": FakeState.ack_inbox,
    "AppendStream": FakeState.append_stream, "ReadStream": FakeState.read_stream,
}


class _Handler(BaseHTTPRequestHandler):
    state: FakeState = None  # injected

    def do_POST(self):  # noqa: N802
        op = self.path.rsplit("/", 1)[-1]
        handler = OPS.get(op)
        if handler is None:
            self._send(404, {"code": "NOT_FOUND", "message": "unknown operation"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(body, dict):
                raise _invalid("request body must be a JSON object")
        except (ValueError, json.JSONDecodeError):
            self._send(400, {"code": "INVALID_ARGUMENT", "message": "malformed JSON body"})
            return
        self.state.calls.append(op)
        auth = self._auth()
        try:
            _check_token(auth["token"])
            out = handler(self.state, body, auth)
        except StateError as exc:
            payload = {"code": exc.code, "message": exc.message}
            if exc.reason:
                payload["reason"] = exc.reason
            self._send(ERROR_STATUS.get(exc.code, 500), payload)
            return
        except Exception as exc:  # pragma: no cover - double must never 500 silently
            self._send(500, {"code": "INTERNAL", "message": f"{type(exc).__name__}: {exc}"})
            return
        if op in self.state._drop_after_commit:
            # The write is durable; the response never arrives. Aborting without
            # writing a status line makes the client see a closed connection,
            # which is exactly the commit-then-loose-ack window a restart must
            # survive.
            self.close_connection = True
            try:
                self.request.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            return
        self._send(200, out)

    def _auth(self) -> dict:
        header = self.headers.get("Authorization") or ""
        token = header[7:] if header.startswith("Bearer ") else ""
        return {"token": token}

    def _send(self, status: int, payload: dict) -> None:
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, *args):  # silence
        pass


class FakeStateServer:
    """Serves :class:`FakeState` over HTTP on a loopback port."""

    def __init__(self, state: FakeState | None = None):
        self.state = state or FakeState()
        self._runs: dict[str, int] = {}
        handler = type("Bound", (_Handler,), {"state": self.state})
        self.httpd = HTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self) -> "FakeStateServer":
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()

    @property
    def url(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    # -- test convenience ------------------------------------------------

    # -- wire-level convenience for tests --------------------------------

    def post(self, operation: str, body: dict, token: str | None = None) -> dict:
        """One synchronous State call (usable from inside a running loop)."""
        import http.client

        host, port = self.httpd.server_address[:2]
        conn = http.client.HTTPConnection(host, port, timeout=30)
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            conn.request("POST", f"/v1/state/{operation}", body=json.dumps(body),
                         headers=headers)
            resp = conn.getresponse()
            payload = json.loads(resp.read() or b"{}")
            if resp.status >= 400:
                raise StateError(payload.get("code", "UNKNOWN"),
                                 payload.get("message", ""),
                                 payload.get("reason", ""))
            return payload
        finally:
            conn.close()

    def ensure_thread(self, context_id: str) -> str:
        """Create (or reuse) one thread and return its id."""
        return self.post("CreateThread", {"context_id": context_id,
                                          "key": f"thread:{context_id}"})["id"]

    def queue_turn(self, thread_id: str, *, input=None, resume=None, continue_run: bool = False,
                   key: str = "") -> str:
        """Enqueue a run for ``thread_id`` the way the run coordinator does.

        ``input`` starts a turn, ``resume`` answers an interrupt, ``continue_run``
        re-queues a run whose worker died mid-turn. The run stays queued: the
        container claims it itself, exactly like a real worker.
        """
        payload: dict = {}
        if input is not None:
            payload["input"] = input
        if resume is not None:
            payload["resume"] = resume
        if continue_run:
            payload["continue"] = True
        run_key = key or f"turn:{thread_id}:{uuid.uuid4().hex[:8]}"
        return self.post("CreateRun", {"thread_id": thread_id, "key": run_key,
                                       "payload": payload})["id"]

    def bootstrap(self, context_id: str, worker: str = "w1") -> dict:
        """Create thread + run over the wire and claim the run.

        Returns ``{thread_id, token, run_id, fence}`` - the hand-out a worker
        gets from ``ClaimRun``.
        """
        thread_id = self.ensure_thread(context_id)
        self._runs.setdefault(context_id, 0)
        self._runs[context_id] += 1
        self.post("CreateRun", {"thread_id": thread_id,
                                "key": f"run:{context_id}:{self._runs[context_id]}"
                                       f":{uuid.uuid4().hex[:6]}",
                                "payload": {"context_id": context_id}})
        claim = self.post("ClaimRun", {"worker": worker, "ttl_ms": 120000})
        if not claim.get("lease"):
            raise StateError("INTERNAL", "no run to claim")
        lease = claim["lease"]
        return {"thread_id": thread_id, "token": claim["task_token"],
                "run_id": lease["run"]["id"],
                "fence": {"run_id": lease["run"]["id"], "worker": lease["worker"],
                          "epoch": lease["epoch"]}}


    def release_wait(self, run_id: str, worker: str = "w2") -> dict:
        """Model ``internal/state/events.go`` the way the server does it: enqueue an
        inbox event whose kind IS the parked run's own ``wait_for``, and State puts
        that same run back to pending - same id, same FIFO position - so the next
        pod claims it under a new epoch.

        This drives the rule instead of poking the row, so a client that parks on a
        ``wait_for`` nobody ever answers fails here exactly as it fails on a live
        stand. What stays faithful besides it: the resumed run keeps its id, and
        intent identity is ``(run_id, node, call)``, so an effect that already
        finished still replays instead of firing twice.
        """
        with self.state.lock:
            run = self.state.doc.d["runs"][run_id]
            if run["status"] != "waiting":
                raise StateError("CONFLICT", "run is not waiting")
            thread_id = run["thread_id"]
            wait_for = run.get("wait_for", "")
        self.post("Enqueue", {"thread_id": thread_id, "key": f"wake:{run_id}",
                              "kind": wait_for, "payload": {}})
        with self.state.lock:
            if self.state.doc.d["runs"][run_id]["status"] != "pending":
                raise StateError("CONFLICT",
                                 f"nothing woke the run: wait_for={wait_for!r} "
                                 "matches no inbox event kind")
            self.state.doc.d["leases"][run_id]["status"] = "active"
        hand = self.re_claim(run_id, worker=worker)
        hand["thread_id"] = thread_id
        return hand

    def re_claim(self, run_id: str, worker: str = "w2") -> dict:
        """Simulate a pod takeover / restart: the old epoch becomes stale.

        Returns the new ``fence`` and a fresh task token - the same hand-out a
        restarted worker gets from ``ClaimRun``.
        """
        claimed = self.state._reclaim(run_id, worker)
        lease = claimed["lease"]
        return {"fence": {"run_id": lease["run_id"], "worker": lease["worker"],
                          "epoch": lease["epoch"]},
                "token": claimed["task_token"]}
