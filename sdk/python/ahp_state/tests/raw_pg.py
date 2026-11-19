"""A read-only PostgreSQL wire reader for the live-stand properties.

Why this file exists. The contract for the live-stand proof says the durability
property must be read **in the server document, not through store/saver**. This
host has a PostgreSQL server (own cluster, own port) but no client: neither
``psql`` nor a driver (``psycopg``/``pg8000``) is installed, and the SDK is not
allowed to import a database driver at all - ``ahp-sdk check`` refuses that in a
user container (``no-direct-db``). Without something like this the strongest
claim available was "the State API's own ``GetKV`` returned it", which is one
layer above where the contract asked to look.

What it is: the minimum of protocol 3.0 to run a ``SELECT`` - startup packet,
``AuthenticationOk``, simple query, ``RowDescription``/``DataRow``,
``Terminate``. No driver, no extension, no dependency.

What it deliberately is not:

* **write-capable** - :meth:`RawPostgres.query` refuses anything that does not
  begin with ``SELECT``/``SHOW``/``VALUES``. A test that could mutate the
  server's rows would prove nothing about what the server wrote by itself;
* **a general client** - no extended protocol, no TLS, no SCRAM, no COPY, no
  binary format, no COPY, no reuse across threads. ``pg_hba`` on the test
  cluster is ``trust`` on 127.0.0.1, so anything other than
  ``AuthenticationOk`` is an environment surprise and raises rather than
  silently falling back.

Text format is what a simple query returns, so a ``jsonb`` column arrives as the
exact bytes the server stores for it - which is the point: the test compares the
document, not a rendering of it.
"""

from __future__ import annotations

import os
import socket
import struct

PROTOCOL_V3 = 196608
_READ_ONLY_PREFIXES = ("select", "show", "values", "with")


class RawPostgresError(RuntimeError):
    """A protocol-level or environment-level surprise. Never a silent retry."""


class RawPostgres:
    """One blocking connection that can only read."""

    def __init__(self, *, host: str = "127.0.0.1", port: int, database: str,
                 user: str | None = None, app_name: str = "ahp-sdk-live-test",
                 timeout: float = 10.0):
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user or os.environ.get("USER") or os.environ.get("LOGNAME") or "postgres"
        self.app_name = app_name
        self.timeout = timeout
        self._sock: socket.socket | None = None
        self._buf = b""

    # --- plumbing --------------------------------------------------------

    def connect(self) -> "RawPostgres":
        self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        params = {"user": self.user, "database": self.database,
                  "application_name": self.app_name}
        body = struct.pack("!i", PROTOCOL_V3)
        for key, value in params.items():
            body += key.encode() + b"\0" + value.encode() + b"\0"
        body += b"\0"
        self._send(b"", body)
        while True:
            kind, payload = self._message()
            if kind == "R":
                code = struct.unpack("!i", payload[:4])[0]
                if code != 0:
                    raise RawPostgresError(
                        f"authentication method {code} is not supported here: this "
                        f"reader exists for a test cluster whose pg_hba is 'trust' "
                        f"on 127.0.0.1 (a real driver would be the right tool instead)")
            elif kind == "Z":
                return self
            elif kind == "E":
                raise RawPostgresError(_error_fields(payload))
            # 'S' ParameterStatus, 'K' BackendKeyData, 'N' NoticeResponse, ...

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._send(b"X", b"")
            except OSError:
                pass
            try:
                self._sock.close()
            finally:
                self._sock = None

    def __enter__(self) -> "RawPostgres":
        return self.connect()

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _send(self, kind: bytes, payload: bytes) -> None:
        assert self._sock is not None, "not connected"
        self._sock.sendall(kind + struct.pack("!i", len(payload) + 4) + payload)

    def _read(self, n: int) -> bytes:
        assert self._sock is not None, "not connected"
        while len(self._buf) < n:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise RawPostgresError("the server closed the connection mid-message")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _message(self) -> tuple[str, bytes]:
        head = self._read(5)
        kind = head[:1].decode()
        (length,) = struct.unpack("!i", head[1:5])
        return kind, self._read(length - 4)

    # --- the one thing it can do ----------------------------------------

    def query(self, sql: str) -> list[dict[str, str | None]]:
        """Run a read-only statement; rows come back as column -> text-or-None."""
        statement = sql.strip()
        if not statement.lower().startswith(_READ_ONLY_PREFIXES):
            raise RawPostgresError(
                f"this reader refuses to send anything but a read; got: "
                f"{statement.splitlines()[0][:60]}")
        if "\0" in statement:
            raise RawPostgresError("NUL is not legal in a SQL string")
        self._send(b"Q", statement.encode() + b"\0")
        rows: list[dict[str, str | None]] = []
        columns: list[str] = []
        while True:
            kind, payload = self._message()
            if kind == "T":
                (count,) = struct.unpack("!h", payload[:2])
                rest = payload[2:]
                columns, rest = _read_fields(rest, count)
            elif kind == "D":
                (count,) = struct.unpack("!h", payload[:2])
                rest = payload[2:]
                values: list[str | None] = []
                for _ in range(count):
                    (size,) = struct.unpack("!i", rest[:4])
                    rest = rest[4:]
                    if size == -1:
                        values.append(None)
                    else:
                        values.append(rest[:size].decode("utf-8"))
                        rest = rest[size:]
                rows.append(dict(zip(columns, values)))
            elif kind in ("C", "I", "N", "S", "n", "t"):
                continue
            elif kind == "E":
                raise RawPostgresError(_error_fields(payload))
            elif kind == "Z":
                return rows
            # 'A' (NotificationResponse) and friends: ignore, keep waiting


def _read_fields(payload: bytes, count: int) -> tuple[list[str], bytes]:
    names: list[str] = []
    for _ in range(count):
        name, _, rest = payload.partition(b"\0")
        names.append(name.decode())
        payload = rest[18:]          # table oid(4) attnum(2) type oid(4) len(2) typmod(4) fmt(2)
    return names, payload


def _error_fields(payload: bytes) -> str:
    parts = []
    for chunk in payload.split(b"\0"):
        if chunk:
            parts.append(chunk.decode("utf-8", "replace"))
    return "state server error: " + " ".join(parts)


def quote(value: str) -> str:
    """A SQL string literal. Keys here are SDK-generated, but doubling quotes
    costs nothing and keeps an odd key from becoming a syntax lesson."""
    return "'" + value.replace("'", "''") + "'"


def kv_row(*, host: str, port: int, database: str, key: str,
           namespace: str | None = None) -> dict[str, str | None] | None:
    """The raw ``ahp_state.kv`` row for ``key``, exactly as the server holds it."""
    sql = ("SELECT tenant, deployment, subject, namespace::text AS namespace, key, "
           "value::text AS value, version, deleted, thread_id, "
           "extract(epoch from updated_at) AS updated_at "
           f"FROM ahp_state.kv WHERE key = {quote(key)}")
    if namespace is not None:
        sql += f" AND namespace = {quote(namespace)}::text[]"
    with RawPostgres(host=host, port=port, database=database) as pg:
        rows = pg.query(sql + " ORDER BY updated_at")
    if not rows:
        return None
    assert len(rows) == 1, f"the primary key returned {len(rows)} rows for {key!r}"
    return rows[0]


def op_receipt(*, host: str, port: int, database: str, operation_key: str) -> dict | None:
    """The raw ``ahp_state.kv_ops`` receipt behind an idempotent write."""
    with RawPostgres(host=host, port=port, database=database) as pg:
        rows = pg.query(
            "SELECT tenant, deployment, subject, operation_key, request_digest, "
            "result::text AS result, extract(epoch from created_at) AS created_at "
            f"FROM ahp_state.kv_ops WHERE operation_key = {quote(operation_key)}")
    if not rows:
        return None
    assert len(rows) == 1, f"{operation_key!r} is not unique in kv_ops"
    return rows[0]


def server_version(*, host: str, port: int, database: str) -> str:
    with RawPostgres(host=host, port=port, database=database) as pg:
        rows = pg.query("SHOW server_version")
    return rows[0]["server_version"] if rows else ""


def discover_database(*, host: str, port: int) -> str:
    """The application database of this cluster, found rather than assumed.

    The cluster belongs to one lane's stand, so it holds exactly one
    non-template database. Anything else is a shared cluster and this file must
    not be guessing which schema to read.
    """
    with RawPostgres(host=host, port=port, database="template1") as pg:
        rows = pg.query("SELECT datname FROM pg_database "
                        "WHERE datistemplate = false ORDER BY datname")
    names = [r["datname"] for r in rows if r["datname"] not in (None, "postgres")]
    if len(names) != 1:
        raise RawPostgresError(
            "expected this cluster to hold exactly one application database, "
            f"found {names}: refusing to guess which one the stand writes to")
    return names[0]
