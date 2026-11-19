"""``ahp-sdk check`` - the CI gate for a custom agent container (PDF rev 3.2 §4.2, §9.2).

A custom agent runs with the platform's credentials inside its own pod, so the
platform cannot only *run* it - it has to be able to say, before a pod ever
starts, whether the code still honours the contract:

* **durable state goes through AHP** - a foreign checkpointer or store
  (``MemorySaver``, ``SqliteSaver``, ``PostgresSaver``, ``InMemoryStore``) keeps
  the thread's state in the pod, where a restart loses it and the State API can
  neither see nor fence it;
* **no side channels** - no direct database driver, no disk writes, no egress, no
  subprocess: durable effects belong to the State API and the Model/Tool gateway;
* **the task token stays inside the SDK** - it must not reach a model call, a
  subprocess/sandbox, the environment, or a log line (§6.2, §9.2 "узел вызывает
  модель и инструмент" - the node asks the *gateway*, it never carries a
  credential to the model);
* **nothing is serialized with pickle into durable state** - a checkpoint that
  only this image can read is a restart the platform cannot perform;
* **``durability`` stays ``"sync"``** - a superstep must be durable before the
  next one starts, otherwise a killed pod rolls back work that already happened.

It is a static check over the project's own ``*.py`` files: no imports of the
target code, no execution, no network, and file *contents* are never echoed into
the report (a finding cites path, line and rule only).

::

    python -m ahp_state.check ./my_agent [--json] [--fail-on-warn] [--ignore-rule R]

Exit codes: ``0`` clean, ``1`` violations, ``2`` usage or unreadable input.
"""

from __future__ import annotations

import ast
import dataclasses
import fnmatch
import os
from typing import Iterable, Iterator, Optional, Sequence

CHECKER_VERSION = 1

# ---------------------------------------------------------------------------
# rules
# ---------------------------------------------------------------------------

FOREIGN_CHECKPOINTER = "foreign-checkpointer"
FOREIGN_STORE = "foreign-store"
MISSING_CHECKPOINTER = "missing-checkpointer"
MISSING_STORE = "missing-store"
DURABILITY_NOT_SYNC = "durability-not-sync"
DIRECT_DB = "no-direct-db"
DISK_WRITE = "no-disk-write"
EGRESS = "no-egress"
SUBPROCESS = "no-subprocess"
DIRECT_MODEL_CALL = "no-direct-model-call"
TOKEN_TO_MODEL = "no-token-to-model"
TOKEN_TO_SANDBOX = "no-token-to-sandbox"
TOKEN_TO_LOG = "no-token-to-log"
TOKEN_TO_ENV = "no-token-to-env"
UNSAFE_SERIALIZATION = "no-pickle-in-state"
RAW_STATE_OPERATION = "no-raw-state-call"

#: The one function an image may touch the outside world from: the transport the
#: standard entry resolves by name (``ahp_state.container.load_effect_transport``
#: looks for ``<agent module>.effect_transport``). The rule below forbids an
#: agent keeping durable *state* on local disk, which survives no reschedule -
#: and an effect transport's entire job is to touch a world that is not state.
#: The SDK journals that touch under ``(run, node, call)``, so exempting this one
#: function is not exempting an escape; every other rule still applies inside it.
EFFECT_TRANSPORT_FUNC = "effect_transport"

RULES: dict[str, str] = {
    FOREIGN_CHECKPOINTER: "a non-AHP LangGraph checkpointer keeps durable state in the pod",
    FOREIGN_STORE: "a non-AHP LangGraph store keeps durable memory in the pod",
    MISSING_CHECKPOINTER: "compile() without checkpointer= has no durable state at all",
    MISSING_STORE: "compile() without store= leaves agent memory unpersisted",
    DURABILITY_NOT_SYNC: "durability other than 'sync' can lose a durable superstep",
    DIRECT_DB: "a direct database driver bypasses the State API and its fencing",
    DISK_WRITE: "writing to disk is not durable and survives no reschedule",
    EGRESS: "direct outbound network bypasses the Model/Tool gateway",
    SUBPROCESS: "spawning processes escapes the sandbox boundary",
    DIRECT_MODEL_CALL: "a model SDK called directly bypasses the Model gateway (§9.2)",
    TOKEN_TO_MODEL: "the task token reached a model call",
    TOKEN_TO_SANDBOX: "the task token reached a subprocess/sandbox",
    TOKEN_TO_LOG: "the task token reached a log/print sink",
    TOKEN_TO_ENV: "the task token reached the environment a child process inherits",
    UNSAFE_SERIALIZATION: "pickle-style serialization cannot be read across images",
    RAW_STATE_OPERATION: "a raw State operation bypasses the SDK's replay handling",
}

#: checkpointers/stores that belong to someone else's durability story.
FOREIGN_SAVERS = {
    "MemorySaver", "InMemorySaver", "SqliteSaver", "AsyncSqliteSaver",
    "PostgresSaver", "AsyncPostgresSaver", "ShallowPostgresSaver",
    "MongoDBSaver", "AsyncMongoDBSaver", "MySqlSaver", "ShallowSqliteSaver",
}
FOREIGN_STORES = {"InMemoryStore", "PostgresStore", "AsyncPostgresStore"}
AHP_SAVERS = {"AHPCheckpointSaver"}
AHP_STORES = {"AHPStore"}

#: roots whose *import* is already the violation.
DB_ROOTS = {
    "psycopg": "PostgreSQL driver", "psycopg2": "PostgreSQL driver",
    "sqlalchemy": "SQL toolkit", "asyncpg": "PostgreSQL driver",
    "pymysql": "MySQL driver", "aiomysql": "MySQL driver", "MySQLdb": "MySQL driver",
    "sqlite3": "local database", "redis": "Redis client", "pymongo": "MongoDB driver",
    "motor": "MongoDB driver", "clickhouse_driver": "ClickHouse driver",
    "elasticsearch": "Elasticsearch client", "opensearch": "OpenSearch client",
    "minio": "object storage client", "boto3": "object storage client",
    "aioboto3": "object storage client",
}
EGRESS_ROOTS = {
    "requests": "HTTP client", "aiohttp": "HTTP client", "urllib3": "HTTP client",
    "httpx": "HTTP client (the SDK owns the one State API client)",
    "socket": "raw sockets", "socketserver": "raw sockets", "ssl": "raw TLS sockets",
    "smtplib": "SMTP egress", "smbclient": "file share egress", "grpc": "raw RPC",
    "websockets": "raw websocket egress",
}
#: ``urllib.parse`` is harmless; ``urllib.request`` is an egress client.
EGRESS_MODULES = ("urllib.request", "http.client", "ftplib", "telnetlib")
MODEL_ROOTS = {
    "openai": "OpenAI SDK", "anthropic": "Anthropic SDK", "google.generativeai": "Gemini SDK",
    "google.ai": "Gemini SDK", "ollama": "local model server", "litellm": "model router",
    "cohere": "Cohere SDK", "mistralai": "Mistral SDK", "transformers": "in-process model",
    "huggingface_hub": "model hub client", "vertexai": "Vertex AI SDK", "bedrock": "Bedrock SDK",
    "tencent": "Hunyuan API", "zhipuai": "GLM API", "deepseek": "DeepSeek API",
}
SERIALIZATION_ROOTS = {
    "pickle": "pickle", "cPickle": "pickle", "dill": "dill", "marshal": "marshal",
    "shelve": "shelve (pickle-backed)",
}
SUBPROCESS_ROOTS = {"subprocess", "pty", "asyncio.subprocess"}
URLLIB_EGRESS_ATTRS = {"urlopen"}

#: functions that write outside the durable store
DISK_CALLS = {
    "os.remove": "remove", "os.unlink": "unlink", "os.rename": "rename",
    "os.replace": "replace", "os.mkdir": "mkdir", "os.makedirs": "makedirs",
    "os.rmdir": "rmdir", "os.chmod": "chmod", "os.chown": "chown", "os.utime": "utime",
    "os.link": "link", "os.symlink": "symlink", "os.startfile": "startfile",
    "os.write": "write", "os.truncate": "truncate", "os.fsync": "fsync",
    "shutil.copy": "copy", "shutil.copy2": "copy2", "shutil.copytree": "copytree",
    "shutil.move": "move", "shutil.rmtree": "rmtree", "shutil.make_archive": "make_archive",
    "tempfile.mkdtemp": "mkdtemp", "tempfile.NamedTemporaryFile": "NamedTemporaryFile",
    "tempfile.mkstemp": "mkstemp", "tempfile.TemporaryDirectory": "TemporaryDirectory",
}
DISK_METHODS = {"write_text", "write_bytes", "mkdir", "touch", "unlink", "rename",
                "replace", "rmdir", "chmod", "open"}

#: sinks a task token must never reach
LOG_FUNCS = {"print", "pprint", "pp", "input"}
LOG_METHODS = {"info", "debug", "warning", "warn", "error", "exception", "critical",
               "log", "notice", "fatal", "print", "printf", "println", "write",
               "writelines", "fatal", "trace", "success", "print_exc", "print_stack"}
MODEL_METHODS = {"invoke", "ainvoke", "stream", "astream", "batch", "abatch",
                 "generate", "generate_content", "chat", "complete", "embed",
                 "embeddings", "messages", "responses"}
SUBPROCESS_FUNCS = {
    "subprocess.run": "run", "subprocess.Popen": "Popen", "subprocess.call": "call",
    "subprocess.check_output": "check_output", "subprocess.check_call": "check_call",
    "os.system": "system", "os.popen": "popen", "os.execv": "execv",
    "os.execve": "execve", "os.execl": "execl", "os.spawnl": "spawnl",
    "os.fork": "fork", "pty.spawn": "spawn",
}
STATE_OPERATIONS = {
    "PutCheckpoint", "PutWrites", "PutKV", "DeleteKV", "ClaimRun", "Release",
    "Suspend", "Heartbeat", "RecordIntent", "UpdateIntent", "ReconcileIntent",
    "Enqueue", "AckInbox", "AppendStream", "CreateRun", "CreateThread",
}

DEFAULT_EXCLUDES = (".git", "__pycache__", ".venv", "venv", "node_modules",
                    ".mypy_cache", ".pytest_cache", ".tox", "build", "dist",
                    "*.egg-info", ".idea", ".tox")


@dataclasses.dataclass
class Finding:
    """One contract violation. Carries no source text - reports must not leak it."""

    rule: str
    path: str
    line: int
    message: str
    severity: str = "error"          # error | warn

    @property
    def locator(self) -> str:
        return f"{self.path}:{self.line}"

    def as_dict(self) -> dict:
        return {"rule": self.rule, "path": self.path, "line": self.line,
                "message": self.message, "severity": self.severity,
                "checker_version": CHECKER_VERSION}


class CheckError(Exception):
    """The check itself could not run (bad path, syntax error, unreadable file)."""


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _dotted(node: ast.AST) -> str:
    """``a.b.c`` for attribute chains and names; ``""`` for anything else."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return ""


def _root(dotted: str) -> str:
    return dotted.split(".")[0] if dotted else ""


def _bound_name(value: ast.expr) -> str:
    """The class/object a ``checkpointer=``/``store=`` argument names.

    ``AHPCheckpointSaver(...)``, ``self.saver`` and ``ctx.store`` all look
    different in the tree; only the first two carry a name we can judge.
    """
    if isinstance(value, ast.Call):
        return _dotted(value.func).split(".")[-1]
    dotted = _dotted(value)
    return dotted.split(".")[-1] if dotted else ""


def _mode_writes(node: ast.Call) -> bool:
    """True when an ``open(...)``-style call asks for a writable mode."""
    mode: Optional[str] = None
    if len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
        mode = str(node.args[1].value)
    for kw in node.keywords:
        if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
            mode = str(kw.value.value)
    return bool(mode and any(ch in mode for ch in "wax+"))


def _is_env_read(node: ast.AST) -> bool:
    """Is this ``os.environ`` / ``environ`` / ``os.environ.get`` itself?"""
    dotted = _dotted(node)
    return dotted in {"os.environ", "environ", "os.getenv", "getenv"} or \
        dotted.endswith(".environ") or dotted.endswith(".environ.get") or \
        dotted.endswith(".getenv")


def _is_token_key(value: object) -> bool:
    """Does an environment variable name denote a credential?"""
    return isinstance(value, str) and "TOKEN" in value.upper()


#: Modules that own a perfectly ordinary ``.compile()`` which is not a LangGraph
#: graph build. Without this the gate reads ``re.compile(r"^x$")`` as "a graph
#: was compiled with no checkpointer" and blocks a container that never touches
#: durable state at all - a false positive that teaches authors to reach for
#: ``--ignore-rule`` on their first day, which is how a gate loses its value.
STDLIB_COMPILE_MODULES = frozenset({
    "re", "regex", "py_compile", "compileall", "sre_compile", "sre_parse",
})

#: A ``compile()`` call is only a graph build in a file that actually uses
#: LangGraph. The rule is per-file, so this costs nothing: a container that
#: receives an already-compiled graph has no ``compile()`` call to check.
GRAPH_LIBRARY_ROOTS = frozenset({"langgraph"})


def _is_token_source(node: ast.AST) -> bool:
    """Does this expression *read* the task token out of a hand-out?

    Two hand-outs count. The SDK's own (``handshake.task_token``,
    ``resp["task_token"]``, ``get_task_token()``) is the sanctioned one. Reading
    a ``*TOKEN*`` variable straight out of the environment is the unsanctioned
    one: a user agent is handed its credential by the SDK, and an agent that
    goes digging in ``os.environ`` is holding a secret the platform never meant
    it to log, embed, or hand to a child process. Flagging only the sanctioned
    shape left that door open.
    """
    if isinstance(node, ast.Attribute):
        return node.attr in {"task_token", "taskToken", "TaskToken"}
    if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
        if _is_token_key(node.slice.value) and _is_env_read(node.value):
            return True
        return node.slice.value in {"task_token", "taskToken", "TaskToken"}
    if isinstance(node, ast.Call):
        name = _dotted(node.func).split(".")[-1]
        if name in {"get", "getenv"} and node.args and \
                isinstance(node.args[0], ast.Constant) and _is_env_read(node.func):
            if _is_token_key(node.args[0].value):
                return True
        return name in {"TaskToken", "GetToken", "get_task_token", "task_token"}
    return False


class _ProjectVisitor(ast.NodeVisitor):
    """One pass over one file: imports, calls, and a small token taint set."""

    def __init__(self, path: str):
        self.path = path
        self.findings: list[Finding] = []
        self.token_names: set[str] = set()
        self._stack: list[ast.AST] = []
        #: names of the functions currently being visited, outermost first
        self._funcs: list[str] = []
        #: local name -> module, for top-level imports (``import re as r`` -> r: re)
        self._module_alias_of: dict[str, str] = {}
        #: ``from re import compile`` - a bare compile() that is not a graph build
        self._bare_compile_is_stdlib = False
        #: does this file use the graph library at all (see GRAPH_LIBRARY_ROOTS)
        self.uses_graph_library = False

    # -- reporting -------------------------------------------------------

    def _add(self, rule: str, node: ast.AST, message: str,
             severity: str = "error") -> None:
        if rule == DISK_WRITE and EFFECT_TRANSPORT_FUNC in self._funcs:
            return          # the declared world boundary, not the agent's state
        self.findings.append(Finding(rule=rule, path=self.path,
                                     line=getattr(node, "lineno", 0),
                                     message=message, severity=severity))

    # -- scope ------------------------------------------------------------

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._funcs.append(node.name)
        try:
            self.generic_visit(node)
        finally:
            self._funcs.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    # -- imports ---------------------------------------------------------

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._check_import(alias.name, node)
            local = (alias.asname or alias.name).split(".")[0]
            self._module_alias_of[local] = alias.name
            if _root(alias.name) in GRAPH_LIBRARY_ROOTS:
                self.uses_graph_library = True
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        self._check_import(module, node)
        if _root(module) in GRAPH_LIBRARY_ROOTS:
            self.uses_graph_library = True
        for alias in node.names:
            qualified = f"{module}.{alias.name}" if module else alias.name
            if alias.name == "compile" and _root(module) in STDLIB_COMPILE_MODULES:
                self._bare_compile_is_stdlib = True
            if alias.name in FOREIGN_SAVERS:
                self._add(FOREIGN_CHECKPOINTER, node,
                          f"imports {alias.name}: {RULES[FOREIGN_CHECKPOINTER]}")
            elif alias.name in FOREIGN_STORES:
                self._add(FOREIGN_STORE, node,
                          f"imports {alias.name}: {RULES[FOREIGN_STORE]}")
            elif _root(module) in SERIALIZATION_ROOTS or qualified in SUBPROCESS_ROOTS:
                self._check_import(qualified, node)
        self.generic_visit(node)

    def _check_import(self, module: str, node: ast.AST) -> None:
        root = _root(module)
        if root in DB_ROOTS:
            self._add(DIRECT_DB, node, f"{DB_ROOTS[root]} ({module}): {RULES[DIRECT_DB]}")
        if root in EGRESS_ROOTS:
            self._add(EGRESS, node, f"{EGRESS_ROOTS[root]} ({module}): {RULES[EGRESS]}")
        elif module.startswith(EGRESS_MODULES):
            self._add(EGRESS, node, f"HTTP client ({module}): {RULES[EGRESS]}")
        if root in MODEL_ROOTS:
            self._add(DIRECT_MODEL_CALL, node,
                      f"{MODEL_ROOTS[root]} ({module}): {RULES[DIRECT_MODEL_CALL]}")
        if root in SERIALIZATION_ROOTS:
            self._add(UNSAFE_SERIALIZATION, node,
                      f"{SERIALIZATION_ROOTS[root]} ({module}): {RULES[UNSAFE_SERIALIZATION]}")
        if module in SUBPROCESS_ROOTS or root in SUBPROCESS_ROOTS:
            self._add(SUBPROCESS, node, f"imports {module}: {RULES[SUBPROCESS]}")

    # -- assignment tracking for the token taint -------------------------

    def visit_Assign(self, node: ast.Assign) -> None:
        if _is_token_source(node.value):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.token_names.add(target.id)
        self.generic_visit(node)

    def visit_With(self, node: ast.With) -> None:
        # the context items carry no line of their own, so report the ``with``
        for item in node.items:
            call = item.context_expr
            if isinstance(call, ast.Call) and _dotted(call.func) in {
                    "open", "io.open", "codecs.open"} and _mode_writes(call):
                self._add(DISK_WRITE, node,
                          f"open(..., mode) writes to disk: {RULES[DISK_WRITE]}")
        self.generic_visit(node)

    visit_AsyncWith = visit_With

    # -- calls -----------------------------------------------------------

    def visit_Call(self, node: ast.Call) -> None:
        func = _dotted(node.func)
        last = func.split(".")[-1] if func else ""

        if func in SUBPROCESS_FUNCS:
            self._add(SUBPROCESS, node, f"{func}(): {RULES[SUBPROCESS]}")
        elif func in DISK_CALLS:
            self._add(DISK_WRITE, node, f"{func}(): {RULES[DISK_WRITE]}")
        elif func in {"open", "io.open", "codecs.open"} and _mode_writes(node):
            self._add(DISK_WRITE, node, "open(..., mode) writes to disk: "
                                        f"{RULES[DISK_WRITE]}")
        elif isinstance(node.func, ast.Attribute) and node.func.attr in DISK_METHODS \
                and not func.startswith(("os.", "shutil.", "tempfile.")):
            if node.func.attr in {"write_text", "write_bytes", "touch"} or \
                    (node.func.attr == "open" and _mode_writes(node)):
                self._add(DISK_WRITE, node, f"{func}(): {RULES[DISK_WRITE]}")
        elif func in URLLIB_EGRESS_ATTRS or (isinstance(node.func, ast.Attribute)
                                             and node.func.attr in URLLIB_EGRESS_ATTRS):
            self._add(EGRESS, node, f"{func}(): {RULES[EGRESS]}")

        if func == "yaml.load" and not any(kw.arg == "Loader" for kw in node.keywords):
            self._add(UNSAFE_SERIALIZATION, node,
                      "yaml.load without Loader=SafeLoader: " + RULES[UNSAFE_SERIALIZATION])
        if last in {"dumps", "loads", "dump", "load"} and _root(func) in SERIALIZATION_ROOTS:
            self._add(UNSAFE_SERIALIZATION, node, f"{func}(): {RULES[UNSAFE_SERIALIZATION]}")

        if isinstance(node.func, ast.Attribute) and node.func.attr == "call":
            if node.args and isinstance(node.args[0], ast.Constant) \
                    and node.args[0].value in STATE_OPERATIONS:
                self._add(RAW_STATE_OPERATION, node,
                          f"raw State operation {node.args[0].value!r}: "
                          f"{RULES[RAW_STATE_OPERATION]}")

        self._check_compile(node)
        self._check_durability(node)
        self._check_token_sinks(node, func, last)
        self.generic_visit(node)

    # -- compile() / durability ------------------------------------------

    def _is_stdlib_compile(self, node: ast.Call) -> bool:
        """Is this ``compile()`` the ordinary kind (regex, bytecode)?"""
        func = node.func
        if isinstance(func, ast.Attribute):
            if func.attr != "compile":
                return False
            receiver = _dotted(func.value).split(".")[0]
            if receiver in STDLIB_COMPILE_MODULES:
                return True
            module = self._module_alias_of.get(receiver, "")
            return _root(module) in STDLIB_COMPILE_MODULES if module else False
        if isinstance(func, ast.Name) and func.id == "compile":
            return self._bare_compile_is_stdlib
        return False

    def _check_compile(self, node: ast.Call) -> None:
        if self._is_stdlib_compile(node):
            return
        if not (isinstance(node.func, ast.Attribute) and node.func.attr == "compile"):
            return
        kwargs = {kw.arg for kw in node.keywords if kw.arg}
        # a missing checkpointer/store is only a defect where a graph is being
        # built; a binding that names a foreign component is a defect anywhere
        if "checkpointer" not in kwargs:
            if self.uses_graph_library:
                self._add(MISSING_CHECKPOINTER, node,
                          "graph.compile() has no checkpointer=: "
                          + RULES[MISSING_CHECKPOINTER])
        else:
            value = next(kw.value for kw in node.keywords if kw.arg == "checkpointer")
            self._check_bound(value, FOREIGN_CHECKPOINTER, AHP_SAVERS, "checkpointer")
        if "store" not in kwargs:
            if self.uses_graph_library:
                self._add(MISSING_STORE, node,
                          "graph.compile() has no store=: " + RULES[MISSING_STORE])
        else:
            value = next(kw.value for kw in node.keywords if kw.arg == "store")
            self._check_bound(value, FOREIGN_STORE, AHP_STORES, "store")

    def _check_bound(self, value: ast.expr, rule: str, allowed: set[str],
                     label: str) -> None:
        if isinstance(value, ast.Constant) and value.value is None:
            self._add(rule, value, f"{label}=None opts out of durable state: {RULES[rule]}",
                      severity="warn")
            return
        name = _bound_name(value)
        if name in FOREIGN_SAVERS or name in FOREIGN_STORES:
            self._add(rule, value, f"{label}={name}: {RULES[rule]}")
        elif name in allowed:
            return
        elif name[:1].isupper() and name not in allowed:
            # a class named by the project that is not AHP's own component
            self._add(rule, value, f"{label}={name} is not an AHP component: "
                                   f"{RULES[rule]}")
        # anything else (ctx.saver, self._saver, build_saver()) is an object the
        # platform handed the container; static analysis cannot judge it, and a
        # false positive here would block a correct agent

    def _check_durability(self, node: ast.Call) -> None:
        for kw in node.keywords:
            if kw.arg == "durability" and isinstance(kw.value, ast.Constant) \
                    and kw.value.value != "sync":
                self._add(DURABILITY_NOT_SYNC, kw.value,
                          f"durability={kw.value.value!r}: {RULES[DURABILITY_NOT_SYNC]}")

    # -- token sinks -----------------------------------------------------

    def _carries_token(self, node: ast.AST, depth: int = 0) -> bool:
        """Does this expression contain a value read from the task hand-out?"""
        if depth > 6:
            return False
        if _is_token_source(node):
            return True
        if isinstance(node, ast.Name):
            return node.id in self.token_names
        if isinstance(node, ast.Dict):
            return any(self._carries_token(v, depth + 1) for v in node.values)
        if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            return any(self._carries_token(e, depth + 1) for e in node.elts)
        if isinstance(node, ast.Call):
            args: list[ast.AST] = list(node.args) + [kw.value for kw in node.keywords]
            return any(self._carries_token(a, depth + 1) for a in args)
        if isinstance(node, ast.BinOp):
            return self._carries_token(node.left, depth + 1) or \
                self._carries_token(node.right, depth + 1)
        if isinstance(node, ast.JoinedStr):
            return any(self._carries_token(v, depth + 1) for v in node.values)
        if isinstance(node, ast.FormattedValue):
            return self._carries_token(node.value, depth + 1)
        if isinstance(node, ast.Starred):
            return self._carries_token(node.value, depth + 1)
        if isinstance(node, ast.keyword):
            return self._carries_token(node.value, depth + 1)
        return False

    def _check_token_sinks(self, node: ast.Call, func: str, last: str) -> None:
        args: list[ast.AST] = list(node.args) + [kw.value for kw in node.keywords]
        if not any(self._carries_token(a) for a in args):
            return
        if func in SUBPROCESS_FUNCS or _root(func) in SUBPROCESS_ROOTS:
            self._add(TOKEN_TO_SANDBOX, node, f"{func}(): {RULES[TOKEN_TO_SANDBOX]}")
        elif func in LOG_FUNCS or last in LOG_METHODS:
            self._add(TOKEN_TO_LOG, node, f"{func}(): {RULES[TOKEN_TO_LOG]}")
        elif func.startswith("os.environ") or last in {"putenv", "setenv"} or \
                (func.endswith("environ.__setitem__")):
            self._add(TOKEN_TO_ENV, node, f"{func}: {RULES[TOKEN_TO_ENV]}")
        elif isinstance(node.func, ast.Attribute) and last in MODEL_METHODS:
            self._add(TOKEN_TO_MODEL, node, f"{func}(): {RULES[TOKEN_TO_MODEL]}")

class _EnvAssignVisitor(ast.NodeVisitor):
    """``os.environ["X"] = token`` - a child process would inherit the credential."""

    def __init__(self, path: str, token_names: set[str]):
        self.path = path
        self.token_names = token_names
        self.findings: list[Finding] = []

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            if isinstance(target, ast.Subscript) and _dotted(target.value).startswith("os.environ"):
                visitor = _ProjectVisitor(self.path)
                visitor.token_names = self.token_names
                if visitor._carries_token(node.value):
                    self.findings.append(Finding(
                        rule=TOKEN_TO_ENV, path=self.path, line=node.lineno,
                        message="task token stored in os.environ: " + RULES[TOKEN_TO_ENV]))
        self.generic_visit(node)


# ---------------------------------------------------------------------------
# entry points
# ---------------------------------------------------------------------------

def check_source(source: str, path: str = "<source>") -> list[Finding]:
    """Check one module's source. Raises :class:`CheckError` on a syntax error."""
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as exc:
        raise CheckError(f"{path}:{exc.lineno}: syntax error: {exc.msg}") from exc
    visitor = _ProjectVisitor(path)
    visitor.visit(tree)
    env = _EnvAssignVisitor(path, visitor.token_names)
    env.visit(tree)
    seen: set[tuple[str, int, str]] = set()
    unique: list[Finding] = []
    for finding in visitor.findings + env.findings:
        # one construct can be reached by more than one rule path (an ``open``
        # that is also a ``with`` expression); the report says it once
        marker = (finding.rule, finding.line, finding.message)
        if marker in seen:
            continue
        seen.add(marker)
        unique.append(finding)
    return sorted(unique, key=lambda f: (f.line, f.rule))


def iter_python_files(roots: Sequence[str],
                      excludes: Iterable[str] = DEFAULT_EXCLUDES) -> Iterator[str]:
    """Every ``*.py`` under *roots*, skipping vendored/build/dependency trees."""
    patterns = tuple(excludes)

    def scan_error(error: OSError) -> None:
        raise CheckError(f"{error.filename}: cannot scan directory") from error
    for root in roots:
        if os.path.isfile(root):
            yield root
            continue
        for dirpath, dirnames, filenames in os.walk(root, onerror=scan_error):
            dirnames[:] = [d for d in dirnames
                           if not any(fnmatch.fnmatch(d, p) for p in patterns)]
            for name in sorted(filenames):
                if name.endswith(".py") and not any(
                        fnmatch.fnmatch(name, p) for p in patterns):
                    yield os.path.join(dirpath, name)


def check_paths(roots: Sequence[str], *, excludes: Iterable[str] = DEFAULT_EXCLUDES,
                ignore: Iterable[str] = ()) -> tuple[list[Finding], list[str]]:
    """Check a project. Returns ``(findings, files_checked)``."""
    ignored = set(ignore)
    for path in roots:
        if not os.path.exists(path):
            raise CheckError(f"{path}: no such file or directory")
    findings: list[Finding] = []
    checked: list[str] = []
    for path in iter_python_files(roots, excludes):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                source = handle.read()
        except (OSError, UnicodeError) as exc:
            raise CheckError(f"{path}: cannot read Python source") from exc
        checked.append(path)
        findings.extend(f for f in check_source(source, path) if f.rule not in ignored)
    findings.sort(key=lambda f: (f.path, f.line, f.rule))
    return findings, checked


__all__ = ["CHECKER_VERSION", "RULES", "Finding", "CheckError", "check_source",
           "check_paths", "iter_python_files", "DEFAULT_EXCLUDES"]
