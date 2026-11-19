"""Run with docker run -i -w /tmp IMAGE python -I - < this file.

Asserts installed-wheel imports independently of /app's copied source tree.
No service credentials or running AHP stand are needed for this packaging gate.
"""
import hashlib
import importlib
import importlib.metadata
import json
from pathlib import Path
import sys
import zipfile

assert Path.cwd() == Path("/tmp")
assert sys.flags.isolated, "use Python -I to avoid cwd/PYTHONPATH source masking"
packages = ("ahp_state", "ahp_memory", "ahp_clients")
distribution = importlib.metadata.distribution("ahp-state")
installed = {}
for name in packages:
    module = importlib.import_module(name)
    location = Path(module.__file__).resolve()
    assert "/site-packages/" in str(location), (name, str(location))
    assert not location.is_relative_to(Path("/app")), (name, str(location))
    installed[name] = str(location)
    assert not (location.parent / "tests").exists(), name
    assert not (Path("/app") / name / "tests").exists(), name
assert not (Path("/app") / "tests").exists()
assert not any("tests" in Path(str(entry)).parts for entry in distribution.files)

# Installed runtime and newly accepted public exports must import, not just their
# empty namespace directories. No graph, client or service is instantiated.
importlib.import_module("ahp_state.agent")
importlib.import_module("ahp_state.container.__main__")
from ahp_memory import MemoryClient, AsyncMemoryClient
from ahp_clients import (ModelClient, AsyncModelClient, ToolsClient, AsyncToolsClient,
                         ComputeClient, AsyncComputeClient, A2AClient, AsyncA2AClient,
                         ArtifactsClient, AsyncArtifactsClient)

wheels = list(Path("/opt/ahp-sdk-wheels").glob("*.whl"))
assert len(wheels) == 1, "expected one retained SDK wheel"
wheel = wheels[0]
with zipfile.ZipFile(wheel) as archive:
    names = archive.namelist()
    assert not any("tests" in Path(name).parts for name in names)
    for package in packages:
        assert package + "/__init__.py" in names, package
        prefix = package + "/"
        for name in names:
            if name.startswith(prefix) and name.endswith(".py"):
                path = Path(distribution.locate_file(name))
                assert path.read_bytes() == archive.read(name), name
print(json.dumps({"distribution": distribution.metadata["Name"],
                  "version": distribution.version, "imports": installed,
                  "wheel": wheel.name, "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
                  "wheel_files": len(names), "sdk_test_files": 0, "runtime_imports": "PASS"}, sort_keys=True))
