import builtins
import sys
from types import SimpleNamespace

import pytest

from ahp_state.container.__main__ import load_builder


def test_parent_package_is_not_executed_before_static_refusal(tmp_path, monkeypatch):
    package = tmp_path / "refused_image"
    package.mkdir()
    (package / "__init__.py").write_text("import builtins\nbuiltins.AHP_PARENT_RAN = True\n")
    (package / "agent.py").write_text("import httpx\ndef build(ctx): return None\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delattr(builtins, "AHP_PARENT_RAN", raising=False)
    try:
        with pytest.raises(RuntimeError, match="no-egress"):
            load_builder("refused_image.agent")
        assert not hasattr(builtins, "AHP_PARENT_RAN"), "parent package executed before check"
        assert "refused_image.agent" not in sys.modules
    finally:
        for name in ("refused_image", "refused_image.agent"):
            sys.modules.pop(name, None)


@pytest.mark.parametrize("source,rule", [
    ("import sqlite3\ndef build(ctx): return None\n", "no-direct-db"),
    ("open('durable.db','w')\ndef build(ctx): return None\n", "no-disk-write"),
    ("from langgraph.checkpoint.memory import MemorySaver\ndef build(ctx): return None\n", "foreign-checkpointer"),
])
def test_startup_checks_candidate_before_import(tmp_path, monkeypatch, source, rule):
    (tmp_path / "candidate.py").write_text(source)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("AHP_MAX_REPLICAS", "2")
    try:
        with pytest.raises(RuntimeError, match=rule): load_builder("candidate")
        assert "candidate" not in sys.modules
    finally:
        sys.modules.pop("candidate", None)


def test_runtime_rejects_foreign_or_wrong_lease_checkpoint_objects():
    from ahp_state.agent import _validate_graph_contract
    saver, store = object(), object()
    ctx = SimpleNamespace(saver=saver, store=store)
    _validate_graph_contract(SimpleNamespace(checkpointer=saver, store=store), ctx)
    for candidate in (SimpleNamespace(checkpointer=None, store=store),
                      SimpleNamespace(checkpointer=object(), store=store),
                      SimpleNamespace(checkpointer=saver, store=object())):
        with pytest.raises(ValueError): _validate_graph_contract(candidate, ctx)


@pytest.mark.parametrize("replicas", ["1", "2"])
def test_compile_without_platform_saver_refused_for_every_replica_count(tmp_path, monkeypatch, replicas):
    (tmp_path / "ungated.py").write_text("from langgraph.graph import StateGraph\ndef build(ctx): return StateGraph(dict).compile()\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("AHP_MAX_REPLICAS", replicas)
    with pytest.raises(RuntimeError, match="missing-checkpointer"):
        load_builder("ungated")
    assert "ungated" not in sys.modules


def test_invalid_source_and_non_source_loader_refused(tmp_path, monkeypatch):
    (tmp_path / "broken_source.py").write_text("def build( broken syntax")
    monkeypatch.syspath_prepend(str(tmp_path))
    with pytest.raises(RuntimeError, match="source check failed"):
        load_builder("broken_source")
    with pytest.raises(RuntimeError, match="cannot (inspect source|import agent module)"):
        load_builder("_ssl")
