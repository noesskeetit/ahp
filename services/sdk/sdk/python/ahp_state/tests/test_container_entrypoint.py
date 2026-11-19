"""The container entry point (PDF rev 3.2 §2.3): env hand-out, serve, stop.

These are the last inches of the contract: a pod gets nothing but environment
variables and a mounted handshake, runs whatever the image ships, and must stop
being a writer when the platform asks. The graph here is an ordinary temp module -
exactly what ``AHP_AGENT_MODULE`` names in production.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import pathlib
import subprocess
import sys

import pytest

from ahp_state.agent import AgentService
import ahp_state
from ahp_state.container.__main__ import (_int, _seconds, client_from_environment,
                                          load_builder, load_effect_transport, main, run)
from ahp_state.tests.fake_state import FakeState, FakeStateServer, StateDoc

AGENT_SOURCE = '''"""The agent module a pod would ship."""
from langgraph.graph import END, START, StateGraph


class S(dict):
    pass


def build(ctx):
    graph = StateGraph(dict)
    graph.add_node("work", lambda state: {"done": True, "worker": ctx.fence["worker"]})
    graph.add_edge(START, "work")
    graph.add_edge("work", END)
    return graph.compile(checkpointer=ctx.saver, store=ctx.store)


def broken_build(ctx):
    return None
'''


@pytest.fixture
def server():
    with FakeStateServer(FakeState(StateDoc())) as srv:
        yield srv


@pytest.fixture
def agent_module(tmp_path, monkeypatch):
    module_dir = tmp_path / "shipagent"
    module_dir.mkdir()
    (module_dir / "__init__.py").write_text("", encoding="utf-8")
    (module_dir / "agent.py").write_text(AGENT_SOURCE, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in [n for n in sys.modules if n.startswith("shipagent")]:
        monkeypatch.delitem(sys.modules, name, raising=False)
    return "shipagent.agent"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("AHP_STATE_URL", "AHP_STATE_TOKEN", "AHP_STATE_HANDSHAKE",
                 "AHP_AGENT_MODULE", "AHP_AGENT_ENTRY", "AHP_WORKER_ID",
                 "AHP_LEASE_TTL_MS", "AHP_HEARTBEAT_MS", "AHP_IDLE_WAIT_S",
                 "AHP_MAX_TURNS"):
        monkeypatch.delenv(name, raising=False)


def test_load_builder_needs_a_real_callable(agent_module):
    assert callable(load_builder(agent_module, "build"))
    with pytest.raises(ValueError):
        load_builder("", "build")
    with pytest.raises(RuntimeError):
        load_builder("no_such_agent_module", "build")
    with pytest.raises(RuntimeError):
        load_builder(agent_module, "not_in_the_module")


def test_client_needs_a_hand_out(monkeypatch, tmp_path):
    with pytest.raises(ValueError):
        client_from_environment()

    monkeypatch.setenv("AHP_STATE_URL", "https://state.invalid")
    assert client_from_environment().base_url == "https://state.invalid"

    handshake = tmp_path / "handshake.json"
    handshake.write_text('{"ca_file": "/tmp/ca.pem"}', encoding="utf-8")
    monkeypatch.setenv("AHP_STATE_HANDSHAKE", str(handshake))
    with pytest.raises(ValueError):
        client_from_environment()


def test_lease_policy_numbers_are_strict(monkeypatch):
    assert _int("AHP_LEASE_TTL_MS", 5) == 5
    monkeypatch.setenv("AHP_LEASE_TTL_MS", "90000")
    assert _int("AHP_LEASE_TTL_MS", 5) == 90000
    monkeypatch.setenv("AHP_LEASE_TTL_MS", "twelve seconds")
    with pytest.raises(ValueError):
        _int("AHP_LEASE_TTL_MS", 5)


async def test_the_container_runs_a_queued_turn_end_to_end(server, agent_module, monkeypatch):
    """Environment in, one turn served, run released - no other wiring."""
    thread = server.ensure_thread("container-basic")
    run_id = server.queue_turn(thread, input={"counter": 0})
    monkeypatch.setenv("AHP_STATE_URL", server.url)
    monkeypatch.setenv("AHP_WORKER_ID", "pod-e2e")

    code = await run(load_builder(agent_module, "build"), max_turns=1)

    assert code == 0
    assert server.state.doc.d["runs"][run_id]["status"] == "done"
    headers = server.state.doc.d["checkpoints"]
    assert any(key.startswith(f'["{thread}", "graph",') for key in headers), \
        "the turn produced no durable checkpoint under this lease"


async def test_a_failed_turn_says_why(server, agent_module, monkeypatch, caplog):
    """A turn that ends failed must be legible in the one log an operator has.

    Nothing was ever checkpointed and this run carries no input, so the SDK
    releases the run as failed - the operator has to see the reason, not just
    the word "failed".
    """
    thread = server.ensure_thread("container-failure")
    run_id = server.queue_turn(thread)
    monkeypatch.setenv("AHP_STATE_URL", server.url)
    monkeypatch.setenv("AHP_WORKER_ID", "pod-sad")

    with caplog.at_level(logging.INFO, logger="ahp_state.container"):
        code = await run(load_builder(agent_module, "build"), max_turns=1)

    assert code == 0
    assert server.state.doc.d["runs"][run_id]["status"] == "failed"
    assert "no durable checkpoint and no input to apply" in caplog.text
    assert "status=failed" in caplog.text
    # The reason must survive this process, under the name the platform reads.
    # Space answers "why did this turn fail" from the frame of kind "error" and
    # attributes it to a turn by its KEY - "<turn_id>:<step>", where a Space turn
    # id is the State run id. A frame named after the framework instead
    # (``agent_failed``) is delivered verbatim and never becomes the reason, which
    # is the answer vm2-state-runner gave to question 4 of 20261101T1351Z §4.
    causes = [event for event in server.state.doc.d["stream"]
              if event["kind"] == "error"]
    assert [event["payload"]["error"] for event in causes] == \
        ["no durable checkpoint and no input to apply"]
    assert [event["key"] for event in causes] == [f"{run_id}:0"], \
        "the cause frame is not attributable to this run's turn"
    # the framework's own name is not lost, it is just no longer the causal one
    assert causes[0]["payload"]["agent_event"] == "agent_failed"
    assert not [event for event in server.state.doc.d["stream"]
                if event["kind"] == "agent_failed"]


async def test_the_container_stops_when_asked(agent_module, monkeypatch):
    """A stop signal must end the loop, not strand it between claims."""
    with FakeStateServer(FakeState(StateDoc())) as srv:
        monkeypatch.setenv("AHP_STATE_URL", srv.url)
        monkeypatch.setenv("AHP_IDLE_WAIT_S", "1")
        stop = asyncio.Event()

        async def nudge():
            await asyncio.sleep(0.05)
            stop.set()

        task = asyncio.create_task(nudge())
        code = await run(load_builder(agent_module, "build"), stop=stop)
        await task
        assert code == 0


def test_main_refuses_to_start_without_a_loadable_agent(tmp_path, capsys):
    assert main(["--module", "definitely_not_installed", "--max-turns", "1"]) == 1
    assert "cannot import agent module" in capsys.readouterr().err


def test_package_import_does_not_pre_execute_dunder_main():
    """``import ahp_state.container`` must not pull in ``__main__``.

    ``python -m ahp_state.container`` executes ``__main__`` itself. If the package
    ``__init__`` imports it first, the module runs twice and CPython prints a
    RuntimeWarning on every single pod start.
    """
    probe = (
        "import sys, ahp_state.container as c; "
        "assert 'ahp_state.container.__main__' not in sys.modules; "
        "assert callable(c.main) and callable(c.run)"
    )
    subprocess.run([sys.executable, "-c", probe], check=True, capture_output=True)


def test_starting_the_module_warns_nothing():
    """The shipped commands start clean - a pod's stderr is an operator surface."""
    container = subprocess.run(
        [sys.executable, "-W", "error::RuntimeWarning", "-m", "ahp_state.container",
         "--module", "definitely_not_installed", "--max-turns", "1"],
        capture_output=True,
        text=True,
    )
    assert "RuntimeWarning" not in container.stderr
    assert "cannot import agent module" in container.stderr
    assert container.returncode == 1


def test_the_check_command_starts_clean_without_langgraph(tmp_path):
    """``ahp-sdk check`` is the CI gate: it must run with stdlib only, no warnings.

    The gate is supposed to vet an image *before* its heavy dependencies are
    installed, so importing it may not pull LangGraph in - and a clean candidate
    must exit 0 without a deprecation notice drowning the findings.
    """
    (tmp_path / "agent.py").write_text(
        "def build(ctx):\n"
        "    return graph.compile(\n"
        "        checkpointer=ctx.saver, store=ctx.store, durability='sync')\n",
        encoding="utf-8",
    )
    gate = tmp_path / "gate.py"
    gate.write_text(
        "import sys\n\n"
        "FORBIDDEN = {'langgraph', 'langchain', 'pydantic', 'httpx'}\n\n\n"
        "class Block:\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name.split('.')[0] in FORBIDDEN:\n"
        "            raise ImportError('blocked for the CI gate: ' + name)\n\n\n"
        "sys.meta_path.insert(0, Block())\n"
        "import ahp_state.check  # the library half: stdlib only\n"
        "from ahp_state.check.__main__ import main\n"
        "raise SystemExit(main([sys.argv[1]]))\n",
        encoding="utf-8",
    )
    proc = subprocess.run(
        [sys.executable, "-W", "error::RuntimeWarning", str(gate), str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert "RuntimeWarning" not in proc.stderr, proc.stderr
    assert "0 finding(s)" in proc.stdout, proc.stdout
    assert proc.returncode == 0, proc.stderr


async def test_a_fractional_idle_wait_is_honoured_instead_of_killing_the_entry(
        agent_module, monkeypatch):
    """``AHP_IDLE_WAIT_S=0.2`` must be a poll interval, not a startup crash.

    This is a defect found by running the standard entry against a live State,
    not by reading it: the entry parsed this seconds-valued knob with ``int()``,
    so a container given a fraction died before its first ``ClaimRun``. The
    damage was not visible where it was looked for - the run the platform had
    already queued simply stayed ``pending``, and no State-side read names the
    worker that stopped claiming it.
    """
    seen = {}
    real_serve = AgentService.serve

    async def spy(self, *args, **kwargs):
        seen["idle_wait_s"] = kwargs.get("idle_wait_s")
        return await real_serve(self, *args, **kwargs)

    with FakeStateServer(FakeState(StateDoc())) as srv:
        monkeypatch.setenv("AHP_STATE_URL", srv.url)
        monkeypatch.setenv("AHP_IDLE_WAIT_S", "0.2")
        monkeypatch.setattr(AgentService, "serve", spy)
        stop = asyncio.Event()

        async def nudge():
            await asyncio.sleep(0.05)
            stop.set()

        task = asyncio.create_task(nudge())
        code = await run(load_builder(agent_module, "build"), stop=stop)
        await task

    assert code == 0
    assert seen["idle_wait_s"] == pytest.approx(0.2), "the fraction never reached serve()"


@pytest.mark.parametrize("name,parse", [("AHP_IDLE_WAIT_S", _seconds),
                                        ("AHP_LEASE_TTL_MS", _int)])
def test_an_unusable_duration_names_itself_and_its_value(name, parse, monkeypatch):
    """A refused knob still has to say which one, and what it was given."""
    monkeypatch.setenv(name, "soon")
    with pytest.raises(ValueError, match=name):
        parse(name, 1)


def test_the_millisecond_knobs_stay_integers(monkeypatch):
    """Fractional seconds are a poll interval; fractional milliseconds are a bug.

    Only the seconds knob became a float. A lease or heartbeat given ``1.5`` is a
    mistake someone should hear about, not something to silently round.
    """
    monkeypatch.setenv("AHP_LEASE_TTL_MS", "1.5")
    with pytest.raises(ValueError, match="must be an integer"):
        _int("AHP_LEASE_TTL_MS", 120_000)
    assert _seconds("AHP_IDLE_WAIT_S", 1.0) == 1.0


def test_the_container_resolves_an_effect_transport_from_the_environment(tmp_path, monkeypatch):
    """A harness may point a shipped image at its own world, without a rebuild."""
    (tmp_path / "legworld.py").write_text(
        "async def touch(tool, args):\n    return {'touched': tool, 'args': args}\n",
        encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("AHP_EFFECT_TRANSPORT", "legworld:touch")

    transport = load_effect_transport()

    assert callable(transport)
    assert asyncio.run(transport("billing.charge", {"amount": 10})) == \
        {"touched": "billing.charge", "args": {"amount": 10}}


@pytest.mark.parametrize("spec", [
    "no_separator_at_all",            # names nothing
    "no_such_world_module:touch",     # cannot be imported
    "legworld:nothing_here",          # imports, but has no such callable
])
def test_a_named_but_unusable_transport_fails_at_startup_not_mid_turn(
        spec, tmp_path, monkeypatch):
    """Naming a transport you do not have is a startup error, never a surprise.

    The alternative is the sequence this whole file exists to prevent: the run
    parks on its gate, a human answers it, and only then does the agent discover
    it cannot charge - after the platform has already spent the approval.
    """
    (tmp_path / "legworld.py").write_text("x = 1\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("AHP_EFFECT_TRANSPORT", spec)
    with pytest.raises(RuntimeError):
        load_effect_transport()


def test_the_example_image_ships_the_world_the_container_looks_for(monkeypatch):
    """No environment beyond the hand-out has to be invented to run the example.

    ``python -m ahp_state.container`` finds ``effect_transport`` on the agent
    module; if that ever stops being true, the shipped Dockerfile's CMD runs an
    image that refuses its own only effect, and every durability leg goes red
    for a reason that has nothing to do with durability.
    """
    examples = pathlib.Path(ahp_state.__file__).resolve().parents[1] / "examples" / "billing_agent"
    monkeypatch.syspath_prepend(str(examples))
    monkeypatch.delenv("AHP_EFFECT_TRANSPORT", raising=False)
    monkeypatch.delitem(sys.modules, "agent", raising=False)

    importlib.import_module("agent")
    transport = load_effect_transport(agent_module="agent")

    assert callable(transport), "the example's effect_transport was not found"
