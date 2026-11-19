"""``ahp-sdk check`` (PDF rev 3.2 §4.2 "чужие чекпоинтеры, egress, диск"; §9.2).

The gate has to be trustworthy in both directions, so these tests pin:

* every rule fires on the code it is about, and a contract-correct agent is clean;
* the exit codes CI keys on (0 clean, 1 violation, 2 the check could not run);
* the report cites path, line and rule - never source text, which may hold secrets;
* the shipped example agent passes, so the documented happy path is really happy.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from ahp_state.check import (
    DEFAULT_EXCLUDES,
    RULES,
    CheckError,
    check_paths,
    check_source,
)
from ahp_state.check.__main__ import EXIT_OK, EXIT_USAGE, EXIT_VIOLATIONS, main

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


def rules_for(source: str) -> set[str]:
    return {f.rule for f in check_source(source, "agent.py")}


def write_project(root: Path, files: dict[str, str]) -> Path:
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    return root


CLEAN = '''"""A contract-correct agent."""
from langgraph.graph import END, START, StateGraph


def build(ctx):
    graph = StateGraph(dict)
    graph.add_node("noop", lambda s: {})
    graph.add_edge(START, "noop")
    graph.add_edge("noop", END)
    return graph.compile(checkpointer=ctx.saver, store=ctx.store)


async def node(ctx):
    async for _ in graph.astream({}, durability="sync"):
        pass
    return await ctx.effects.call("x", {}, call_id="c1")
'''


def test_the_documented_shape_of_an_agent_is_clean(tmp_path):
    project = write_project(tmp_path / "agent", {"agent.py": CLEAN})
    findings, checked = check_paths([str(project)])
    assert checked, "the project was not scanned"
    assert findings == [], [f.as_dict() for f in findings]
    assert main([str(project)]) == EXIT_OK


def test_example_agent_ships_clean():
    assert EXAMPLES.is_dir(), f"missing example project at {EXAMPLES}"
    findings, checked = check_paths([str(EXAMPLES)])
    assert len(checked) >= 1
    assert findings == [], [f.as_dict() for f in findings]


@pytest.mark.parametrize("source,rule", [
    ("from langgraph.checkpoint.memory import MemorySaver\n", "foreign-checkpointer"),
    ("g.compile(checkpointer=MemorySaver(), store=ctx.store)\n", "foreign-checkpointer"),
    ("g.compile(checkpointer=ctx.saver, store=InMemoryStore())\n", "foreign-store"),
    # the "missing" rules are about building a graph, so the fixture is a graph
    # file: without a graph library in scope, compile() is just some method call
    ("from langgraph.graph import StateGraph\n"
     "g.compile(checkpointer=ctx.saver)\n", "missing-store"),
    ("from langgraph.graph import StateGraph\n"
     "g.compile(store=ctx.store)\n", "missing-checkpointer"),
    ("g.compile(checkpointer=ctx.saver, store=None)\n", "foreign-store"),
    ("client.invoke(x, durability='async')\n", "durability-not-sync"),
    ("import psycopg\n", "no-direct-db"),
    ("import sqlite3\n", "no-direct-db"),
    ("import requests\n", "no-egress"),
    ("from urllib.request import urlopen\nurlopen('https://x')\n", "no-egress"),
    ("import subprocess\nsubprocess.run(['ls'])\n", "no-subprocess"),
    ("import openai\n", "no-direct-model-call"),
    ("open('/tmp/x', 'w')\n", "no-disk-write"),
    ("with open('/tmp/x', 'a') as fh:\n    pass\n", "no-disk-write"),
    ("pathlib.Path('x').write_text('y')\n", "no-disk-write"),
    ("import shutil\nshutil.rmtree('/tmp/x')\n", "no-disk-write"),
    ("import pickle\npickle.dumps(x)\n", "no-pickle-in-state"),
    ("yaml.load(text)\n", "no-pickle-in-state"),
    ("client.call('PutCheckpoint', body)\n", "no-raw-state-call"),
    ('tok = resp["task_token"]\nprint(tok)\n', "no-token-to-log"),
    ('tok = resp["task_token"]\nlog.info("%s", tok)\n', "no-token-to-log"),
    ('tok = handshake.task_token\nsubprocess.run(["curl", tok])\n', "no-token-to-sandbox"),
    ('tok = get_task_token()\nawait llm.ainvoke({"k": tok})\n', "no-token-to-model"),
    ('tok = resp["task_token"]\nos.environ["T"] = tok\n', "no-token-to-env"),
    # going digging in the environment is a hand-out too, and an unsanctioned one
    ('import os\ntok = os.environ["AHP_TASK_TOKEN"]\nprint(tok)\n', "no-token-to-log"),
    ('import os\ntok = os.environ.get("AHP_TASK_TOKEN")\nawait llm.ainvoke({"k": tok})\n',
     "no-token-to-model"),
    ('import os\ntok = os.getenv("AHP_TASK_TOKEN")\nsubprocess.run(["curl", tok])\n',
     "no-token-to-sandbox"),
    ('import os\ntok = os.environ["AHP_TASK_TOKEN"]\nos.environ["F"] = tok\n',
     "no-token-to-env"),
])
def test_each_rule_fires_on_the_code_it_is_about(source, rule):
    assert rule in rules_for(source), f"{rule} did not fire for:\n{source}"


def test_rules_all_have_a_description():
    assert RULES and all(RULES[r] for r in RULES)


def test_a_token_that_stays_in_the_sdk_is_not_flagged():
    source = '''tok = resp["task_token"]
client = make_client(base_url, token=tok)
await client.put_kv(fence, ns, key, op, 0, value)
'''
    # handing the token to the State client is the *only* legal sink
    assert rules_for(source) == set()


def test_clean_code_which_happens_to_be_named_token_is_not_flagged():
    assert "no-token-to-log" not in rules_for('token = tokenize(text)\nprint(token)\n')


def test_an_env_credential_with_no_sink_is_not_flagged():
    """Reading a credential is not itself a violation - only letting it leak is.

    Without this boundary the rule would scream at every image that merely
    touches ``os.environ``, and a noisy gate is a gate that gets switched off.
    """
    assert rules_for('import os\ntok = os.environ["AHP_TASK_TOKEN"]\nreturn {"ok": True}\n') == set()
    assert "no-token-to-log" not in rules_for('import os\nprint(os.environ["USER"])\n')


def test_a_syntax_error_is_the_check_failing_not_a_pass(tmp_path):
    project = write_project(tmp_path / "broken", {"a.py": "def x(:\n"})
    with pytest.raises(CheckError):
        check_paths([str(project)])


def test_missing_path_is_a_usage_error(tmp_path):
    with pytest.raises(CheckError):
        check_paths([str(tmp_path / "nope")])


def test_cli_exit_codes_and_json_report(tmp_path, capsys):
    project = write_project(tmp_path / "proj", {
        "bad.py": "import sqlite3\n",
        "sub/__init__.py": "",
        "sub/ok.py": CLEAN,
    })
    assert main([str(project)]) == EXIT_VIOLATIONS
    text = capsys.readouterr().out
    assert "no-direct-db" in text and "bad.py:1" in text
    assert "sub/ok.py" not in text, "a clean file should not appear in the report"

    capsys.readouterr()
    assert main(["--json", str(project)]) == EXIT_VIOLATIONS
    report = json.loads(capsys.readouterr().out)
    assert report["summary"]["errors"] == 1 and report["findings"][0]["rule"] == "no-direct-db"
    assert report["files_checked"] == 3   # bad.py, sub/__init__.py, sub/ok.py
    assert set(report["findings"][0]) == {"rule", "path", "line", "message",
                                          "severity", "checker_version"}

    capsys.readouterr()
    assert main(["--ignore-rule", "no-direct-db", str(project)]) == EXIT_OK
    capsys.readouterr()
    assert main(["--exclude", "bad.py", str(project)]) == EXIT_OK


def test_cli_reports_usage_problems_as_two(tmp_path, capsys):
    assert main([str(tmp_path / "absent")]) == EXIT_USAGE
    assert "cannot run" in capsys.readouterr().err


def test_cli_can_list_rules_and_version(capsys):
    assert main(["--list-rules"]) == EXIT_OK
    listed = capsys.readouterr().out
    assert "foreign-checkpointer" in listed and "no-direct-db" in listed
    assert main(["--version"]) == EXIT_OK
    assert "ahp-sdk check" in capsys.readouterr().out


def test_the_report_never_echoes_source_text(tmp_path, capsys):
    secret = "Authorization: Bearer super-secret-value"
    project = write_project(tmp_path / "leak", {"a.py": f'X = "{secret}"\nimport sqlite3\n'})
    main([str(project)])
    out = capsys.readouterr().out + capsys.readouterr().err
    assert "super-secret-value" not in out
    findings, _ = check_paths([str(project)])
    assert all(secret not in f.message for f in findings)


def test_default_excludes_skip_dependency_trees(tmp_path):
    write_project(tmp_path, {"agent.py": CLEAN,
                             ".venv/lib/site-packages/b.py": "import sqlite3\n",
                             "__pycache__/c.py": "import sqlite3\n"})
    _, checked = check_paths([str(tmp_path)])
    names = [os.path.basename(p) for p in checked]
    assert names == ["agent.py"], checked


# ---------------------------------------------------------------------------
# the compile() false positive
#
# The gate reads any ``X.compile()`` as a graph build, which meant a container
# doing ordinary regex work was refused for "compiling a graph with no
# checkpointer" - four findings in the SDK's own wire.py. A gate that fires on
# correct code trains authors to pass --ignore-rule, so this is pinned.
# ---------------------------------------------------------------------------

def test_a_regex_compile_is_not_a_graph_build():
    assert rules_for("import re\nP = re.compile(r'^x$')\n") == set()
    assert rules_for("import re as rx\nP = rx.compile('x')\n") == set()
    assert rules_for("from re import compile\nP = compile('x')\n") == set()


def test_a_graph_build_without_durable_components_is_still_caught():
    source = ("from langgraph.graph import StateGraph\n"
              "def build(ctx):\n"
              "    return StateGraph(dict).compile()\n")
    assert {"missing-checkpointer", "missing-store"} <= rules_for(source)


def test_a_regex_in_a_langgraph_file_does_not_inherit_the_rule():
    source = ("import re\n"
              "from langgraph.graph import StateGraph\n"
              "PATTERN = re.compile('x')\n"
              "def build(ctx):\n"
              "    return StateGraph(dict).compile(checkpointer=ctx.saver, store=ctx.store)\n")
    assert rules_for(source) == set()


def test_the_sdk_package_itself_passes_the_container_rules():
    """The gate's scope is a user container, but the package must not fail it for
    the reason above: only the rules that describe a harness may fire here."""
    package = Path(__file__).resolve().parents[1]
    findings, checked = check_paths([str(package)],
                                    excludes=DEFAULT_EXCLUDES + ("tests",))
    by_rule = {f.rule for f in findings}
    assert "missing-checkpointer" not in by_rule and "missing-store" not in by_rule, (
        f"a compile() that is not a graph build is reported again: "
        f"{[f.path for f in findings if f.rule in ('missing-checkpointer', 'missing-store')]}")
    assert checked, "the package was not scanned at all"


def test_the_declared_effect_transport_may_touch_the_world():
    """The gate cannot forbid the one thing an effect transport exists to do.

    ``no-disk-write`` protects against an agent keeping durable *state* on local
    disk. A transport writing a receipt is a journaled effect under
    ``(run, node, call)``, not state - so the function the standard entry resolves
    by name is exempt from that single rule.
    """
    source = (
        "import os, pathlib\n"
        "async def effect_transport(tool, args):\n"
        "    world = pathlib.Path('world'); world.mkdir(parents=True, exist_ok=True)\n"
        "    with (world / 'r.json').open('wb') as handle:\n"
        "        handle.write(b'{}'); handle.flush(); os.fsync(handle.fileno())\n"
        "    return {'tool': tool}\n")
    assert "no-disk-write" not in rules_for(source)


def test_a_node_still_may_not_write_state_to_local_disk():
    """The exemption is one function wide - a node writing receipts is still wrong."""
    source = (
        "async def charge(state):\n"
        "    with open('/var/lib/agent/receipt.json', 'w') as handle:\n"
        "        handle.write('{}')\n"
        "    return {}\n")
    assert "no-disk-write" in rules_for(source)


def test_the_exemption_exempts_one_rule_and_not_the_rest():
    """Naming a function ``effect_transport`` must not make it a law-free zone."""
    source = (
        "import subprocess\n"
        "async def effect_transport(tool, args):\n"
        "    subprocess.run(['curl', '-d', 'x', 'https://billing.internal'])\n"
        "    return {}\n")
    assert "no-subprocess" in rules_for(source)
