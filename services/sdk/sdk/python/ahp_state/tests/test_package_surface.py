"""One authoritative implementation per public name, and docs that point at it.

The package shipped for a while with two classes called ``AHPCheckpointSaver``:
the accepted one in ``checkpoint_saver.py`` and a superseded copy in
``checkpointer.py`` whose imports had long since rotted (``client.dec_bytes``
does not exist), so importing it raised ``ImportError``. Nothing referenced it,
yet ``requirements.txt`` still named it as *the* saver - the exact shape of trap
that costs a reviewer a day: ``sdk check`` rejects a foreign checkpointer, while
the SDK itself offered a second, silently broken AHP one.
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

import pytest

PKG = Path(__file__).resolve().parent.parent
SDK = PKG.parent


def _definitions(class_name: str) -> list[str]:
    found = []
    for source in sorted(PKG.rglob("*.py")):
        if "__pycache__" in source.parts:
            continue
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == class_name:
                found.append(str(source.relative_to(SDK)))
    return found


@pytest.mark.parametrize("class_name", ["AHPCheckpointSaver", "AHPStore", "StateAPIClient"])
def test_exactly_one_definition_of_each_public_name(class_name):
    paths = _definitions(class_name)
    assert len(paths) == 1, f"{class_name} is defined more than once: {paths}"


def test_the_superseded_checkpointer_module_is_gone():
    """A dead second saver must not come back under the same or a near name."""
    assert not (PKG / "checkpointer.py").exists()
    assert _definitions("AHPCheckpointSaver") == ["ahp_state/checkpoint_saver.py"]


def test_the_documented_adapter_paths_exist():
    """requirements.txt advertises which module owns which adapter - believe it.

    The pointer is documentation a reviewer follows first; a stale one sends them
    to dead code, so it is checked against the tree rather than trusted.
    """
    text = (SDK / "requirements.txt").read_text(encoding="utf-8")
    pointers = re.findall(r"ahp_state/([A-Za-z0-9_]+)\.py\s*->\s*(\w+)", text)
    assert pointers, "expected requirements.txt to name the adapter modules"
    for module, class_name in pointers:
        assert (PKG / f"{module}.py").exists(), f"{module}.py is advertised but absent"
        tree = ast.parse((PKG / f"{module}.py").read_text(encoding="utf-8"))
        defined = [n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
        assert class_name in defined, f"{class_name} not in {module}.py"


def test_every_advertised_name_actually_resolves():
    """``__all__`` is a promise; a name that raises AttributeError breaks it.

    Checked in a fresh interpreter because the lazy resolver caches into the
    package globals, so a second lookup inside one process can hide a broken
    entry. ``wire`` earned this: it is a submodule, but it sat in the
    attribute-export table, where resolving it could only raise.
    """
    probe = (
        "import ahp_state; "
        "bad = [n for n in ahp_state.__all__ if getattr(ahp_state, n, None) is None]; "
        "assert not bad, 'unresolvable __all__ entries: %s' % bad"
    )
    subprocess.run([sys.executable, "-c", probe], check=True, capture_output=True)


def test_public_names_are_reachable_from_the_package_root():
    """``from ahp_state import X`` is the documented way a user agent wires up."""
    from ahp_state import AHPCheckpointSaver, AHPStore, IntentRecorder, SteeringFold
    from ahp_state import wire as wire_module

    import ahp_state.checkpoint_saver as saver_module
    import ahp_state.store as store_module

    assert AHPCheckpointSaver is saver_module.AHPCheckpointSaver
    assert AHPStore is store_module.AHPStore
    assert wire_module.decode_id(wire_module.encode_id("branch:to:x|ü")) == "branch:to:x|ü"
    assert callable(SteeringFold) and callable(IntentRecorder)


def test_the_test_harness_is_neither_distributed_nor_shipped():
    """`ahp_state/tests` is a harness, and part of it is a side channel.

    It listens on sockets, spawns processes, and `raw_pg.py` opens the stand's
    PostgreSQL over the wire to read a server row. Those are exactly the things
    the container rules (`EGRESS_ROOTS`, `DB_ROOTS` in `ahp_state.check`) refuse
    in a user agent, so the answer is not to tolerate them in the image but to
    keep the harness out of it: excluded from the wheel, excluded from the build
    context. If this test goes red, an agent image is again carrying a database
    path it was never allowed to have.
    """
    import tomllib
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    with open(root / "pyproject.toml", "rb") as handle:
        pyproject = tomllib.load(handle)
    excludes = pyproject["tool"]["setuptools"]["packages"]["find"]["exclude"]
    assert any("tests" in pattern for pattern in excludes), excludes

    lines = (root / ".dockerignore").read_text().splitlines()
    kept = [line.strip() for line in lines if line.strip() and not line.startswith("#")]
    assert "ahp_state/tests/" in kept, kept


def test_the_documented_image_build_command_resolves_against_its_context():
    """The one line a reviewer copies to build an image has to be runnable.

    `COPY` resolves inside the build *context*, not the directory holding the
    Dockerfile, and `.dockerignore` is read at the context root and nowhere else.
    The documented line shipped with `.` (the repository) as its context, where
    `COPY pyproject.toml` cannot resolve - there is no pyproject at the repo root
    - so the build instruction was a build error, and the ignore file that keeps
    the harness out of the image would not have been read either.
    """
    repo = SDK.parents[1]
    dockerfile = (SDK / "Dockerfile").read_text(encoding="utf-8")
    copy_sources = [src for src in re.findall(r"^COPY\s+(\S+)\s+\S+", dockerfile, re.M)
                    if "$" not in src]
    assert copy_sources, "expected literal COPY sources in the Dockerfile"

    text = (SDK / "README.md").read_text(encoding="utf-8")
    documented = re.findall(r"^docker build [^\n]*", text, re.M)
    assert documented, "README must document how to build an agent image"
    for line in documented:
        assert not line.rstrip().endswith("\\"), f"line must stay one-line: {line}"
        tokens = line.split()
        context_arg = tokens[-1]
        context = (repo / context_arg).resolve()
        assert context.is_dir(), f"{line}: context {context_arg!r} is not a directory"
        for src in copy_sources:
            assert (context / src).exists(), f"{line}: COPY {src} cannot resolve in {context_arg!r}"
        assert (context / ".dockerignore").is_file(), \
            f"{line}: .dockerignore is not at the context root, so it does not apply"
        agent_dir = re.search(r"--build-arg AGENT_DIR=(\S+)", line)
        assert agent_dir, f"{line}: no AGENT_DIR build-arg"
        assert (context / agent_dir.group(1)).is_dir(), \
            f"{line}: AGENT_DIR {agent_dir.group(1)!r} is not inside the context"
