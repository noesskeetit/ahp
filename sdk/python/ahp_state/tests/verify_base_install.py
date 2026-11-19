"""Build/install the framework-neutral wheel in a clean venv, then inspect it.

This isolated packaging verification may resolve httpx build/runtime dependencies
from pip's configured index/cache. It never changes the caller's environment.
"""
import pathlib
import argparse
import shutil
import subprocess
import sys
import tempfile
import venv

parser = argparse.ArgumentParser()
parser.add_argument("--langgraph", action="store_true")
options = parser.parse_args()

package = pathlib.Path(__file__).resolve().parents[2]
with tempfile.TemporaryDirectory(prefix="ahp-sdk-base-install-") as directory:
    root = pathlib.Path(directory)
    environment = root / "venv"
    venv.EnvBuilder(with_pip=True).create(environment)
    python = environment / "bin" / "python"
    wheel_dir = root / "wheels"
    source = root / "package"
    shutil.copytree(package, source, ignore=shutil.ignore_patterns("__pycache__", "build", "*.egg-info", ".pytest_cache"))
    def run(args):
        result = subprocess.run(args, text=True, capture_output=True, timeout=120)
        if result.returncode:
            # Dependency index URLs may contain credentials: retain no pip log.
            raise RuntimeError("isolated SDK install/check failed at %s (exit %d)" % (args[2:4], result.returncode))
    run([str(python), "-m", "pip", "wheel", "--no-deps", "-w", str(wheel_dir), str(source)])
    wheel, = wheel_dir.glob("*.whl")
    run([str(python), "-m", "pip", "install", "--disable-pip-version-check", str(wheel) + ("[langgraph]" if options.langgraph else "")])
    common = """
import importlib.util, importlib.metadata, asyncio
from ahp_state import StateAPIClient, StateAdminClient, TransferClient, mtls_client
from ahp_state.check import check_source
assert check_source("def build(ctx): return None") == []
assert importlib.util.find_spec("ahp_state.tests") is None
client = StateAPIClient("https://state.invalid")
asyncio.run(client.close())
"""
    # Integration candidates may add these independently. If present in source,
    # they must be importable from the installed wheel, with their tests excluded.
    added = [name for name in ("ahp_memory", "ahp_clients") if (package / name).is_dir()]
    common += f"\nfor name in {added!r}:\n importlib.import_module(name)\n assert importlib.util.find_spec(name + '.tests') is None\n"
    probe = common + ("""
from ahp_state import AHPCheckpointSaver, AHPStore, AgentService
from ahp_state.container.__main__ import load_builder
assert importlib.metadata.version("langgraph") == "0.6.11"
assert importlib.metadata.version("langgraph-checkpoint") == "2.1.2"
""" if options.langgraph else """
assert importlib.util.find_spec("langgraph") is None
assert not any(x.startswith("langgraph") for x in __import__("sys").modules)
""")
    run([str(python), "-c", probe])
    if options.langgraph:
        run([str(python), "-m", "ahp_state.container", "--help"])

print("clean SDK wheel installed: " + ("LangGraph extra + container entrypoint" if options.langgraph else "no LangGraph, State/Recovery/mTLS/checker") + " PASS")
