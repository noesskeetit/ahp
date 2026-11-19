"""Lease lifecycle helper shared by the LangGraph adapters.

AHP State fences every durable write with a live lease: to write a checkpoint
or a KV entry the caller must hold a lease (a ``Fence`` plus the task token
minted at claim time) on the thread that owns the data. A :class:`Session`
turns the LangGraph notion of a thread id into exactly that machinery:

* the first time a LangGraph ``thread_id`` is used it creates an AHP thread and
  one run, then claims that run to obtain a fence and a task token;
* a background daemon renews every held lease so writes stay fenced for the
  whole life of the session;
* reads (checkpoints, parts, pending writes, KV) use the bootstrap task token,
  which lives in the same subject scope, so reads never need a lease.

Create-run and claim-run are serialized under a single process-wide lock. AHP
dispenses pending runs in FIFO order per deployment, so creating a run and
claiming it immediately, with no other pending run interleaving, guarantees the
claim returns the run that was just created. That is what makes the mapping
``langgraph thread_id -> fence`` deterministic under concurrency.

Nothing here mutates global mutable state other than the per-session tables it
owns; the class is safe to share across threads.
"""

import hashlib
import threading
import time
import uuid

from .errors import StateError

# Heartbeats are issued at one third of the lease TTL, with sane bounds so a
# very short or very long TTL still keeps the lease comfortably alive.
_DEFAULT_TTL_MS = 60_000


def _encode_namespace_token(langgraph_thread_id):
    """Derive a deterministic, identifier-safe AHP context id."""
    digest = hashlib.sha1(langgraph_thread_id.encode("utf-8")).hexdigest()
    return "ctx-" + digest[:16]


class _Lease:
    __slots__ = ("thread_id", "fence", "token", "expires_at", "stop")

    def __init__(self, thread_id, fence, token, expires_at):
        self.thread_id = thread_id
        self.fence = fence
        self.token = token
        self.expires_at = expires_at
        self.stop = threading.Event()


class Session:
    """Owns leases for the LangGraph threads used by one process.

    ``client`` is a configured :class:`~ahp_state.client.Client`. ``worker``
    labels claims (defaults to a unique value) and ``ttl_ms`` bounds the lease;
    the heartbeat daemon renews at one third of that interval.
    """

    def __init__(self, client, worker=None, ttl_ms=_DEFAULT_TTL_MS):
        self.client = client
        self.bootstrap_token = client.task_token
        if not self.bootstrap_token:
            raise ValueError("client must carry a bootstrap task token")
        self.worker = worker or ("worker-" + uuid.uuid4().hex)
        self.ttl_ms = int(ttl_ms)
        self._lock = threading.Lock()
        self._leases = {}  # langgraph thread id -> _Lease
        self._closed = False
        self._heartbeat = threading.Thread(
            target=self._heartbeat_loop, name="ahp-state-heartbeat", daemon=True
        )
        self._heartbeat.start()

    # -- public ------------------------------------------------------------

    def ensure(self, langgraph_thread_id):
        """Return ``(ahp_thread_id, fence, lease_token)`` for a thread.

        Creates and claims the backing run on first use; subsequent calls reuse
        the held lease.
        """
        if self._closed:
            raise StateError("INVALID_ARGUMENT", "session is closed")
        with self._lock:
            lease = self._leases.get(langgraph_thread_id)
            if lease is not None:
                return lease.thread_id, dict(lease.fence), lease.token
            ahp_thread = self._create_thread(langgraph_thread_id)
            self.client.create_run(
                ahp_thread["id"], "run-" + uuid.uuid4().hex, {},
                token=self.bootstrap_token,
            )
            claim, token = self._claim(ahp_thread["id"])
            # ClaimRun returns the authoritative granted lease; trust its run
            # id, thread id and epoch rather than the values we assumed when
            # creating the run.
            lease = _Lease(
                claim["thread_id"],
                {"run_id": claim["run_id"], "worker": self.worker,
                 "epoch": claim["epoch"]},
                token,
                time.monotonic() + self.ttl_ms / 1000.0,
            )
            self._leases[langgraph_thread_id] = lease
            return lease.thread_id, dict(lease.fence), lease.token

    def close(self):
        """Stop renewing leases and release them as terminal.

        Idempotent. Every held lease is released with a terminal ``done`` status
        so the backing run leaves the FIFO head and the next run becomes
        claimable. Release is a network call, so it is issued after the snapshot
        is taken and the internal table is cleared, never while the lock is held
        (the heartbeat loop reads that table). Release is best-effort: a lost or
        already-terminal lease is tolerated rather than raised, so closing a
        session never masks the caller's real outcome.
        """
        if self._closed:
            return
        self._closed = True
        with self._lock:
            pending = list(self._leases.values())
            for lease in pending:
                lease.stop.set()
            self._leases.clear()
        for lease in pending:
            try:
                self.client.release(lease.fence, "done", token=lease.token)
            except StateError:
                # Lease may already be lost, taken over, or terminal; that is
                # fine for a durable shutdown. Go-side stale-epoch takeover still
                # recovers crashed workers, so we never fail the close path.
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    # -- internals ---------------------------------------------------------

    def _create_thread(self, langgraph_thread_id):
        context_id = _encode_namespace_token(langgraph_thread_id)
        # context id is the dedup key: re-creating the same logical thread is
        # idempotent (same key replays the existing thread).
        thread = self.client.create_thread(
            context_id, "thr-" + hashlib.sha1(
                langgraph_thread_id.encode("utf-8")).hexdigest()[:16],
            token=self.bootstrap_token,
        )
        return thread

    def _claim(self, ahp_thread_id):
        """Claim the run just created and return ({run_id, epoch, thread_id}, token).

        Must be called while ``self._lock`` is held so the claim cannot pick up
        another caller's run.
        """
        deadline = time.monotonic() + self.ttl_ms / 1000.0
        while True:
            lease, token = self.client.claim_run(self.worker, self.ttl_ms)
            if lease is not None:
                run = lease.get("run", {})
                thread = lease.get("thread", {})
                if run.get("id"):
                    return {
                        "run_id": run["id"],
                        "epoch": lease.get("epoch", run.get("epoch", 1)),
                        "thread_id": thread.get("id", ahp_thread_id),
                    }, token
            if time.monotonic() >= deadline:
                raise StateError("NOT_FOUND", "no run available to claim")
            time.sleep(0.01)

    def _heartbeat_loop(self):
        interval = max(1.0, min(20.0, self.ttl_ms / 3000.0))
        while not self._closed:
            time.sleep(interval)
            if self._closed:
                break
            now = time.monotonic()
            snapshot = []
            with self._lock:
                for lg_id, lease in list(self._leases.items()):
                    snapshot.append((lg_id, lease))
            for lg_id, lease in snapshot:
                if lease.stop.is_set() or self._closed:
                    continue
                try:
                    self.client.heartbeat(
                        lease.fence, self.ttl_ms, token=lease.token
                    )
                    lease.expires_at = now + self.ttl_ms / 1000.0
                except StateError:
                    # A lost lease is surfaced to the next write, not masked
                    # here; stopping renewal avoids log noise.
                    lease.stop.set()
