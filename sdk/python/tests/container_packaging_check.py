"""Build and inspect the actual SDK image; intentionally reject an unsafe agent.

Run from repository root via the private lane runner. Never pushes or deploys.
All Docker resources carry the isolated ahp-handbook project label.
"""
import io
import json
from pathlib import Path
import subprocess
import tempfile
import tarfile

ROOT = Path(__file__).resolve().parents[3]
LABEL = "com.docker.compose.project=ahp-handbook"


def checked(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)


def main():
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
    if dirty.strip():
        raise RuntimeError("package acceptance requires a clean committed source tree")
    image = "ahp-handbook-sdk-billing:" + revision[:12]
    base_args = ["docker", "build", "--label", LABEL, "--label", "org.opencontainers.image.revision=" + revision]
    checked(base_args + ["--build-arg", "AGENT_DIR=examples/billing_agent",
                        "--build-arg", "AGENT_MODULE=examples.billing_agent.agent",
                        "-f", "sdk/python/Dockerfile", "-t", image, "sdk/python"])
    inspected = json.loads(subprocess.check_output(["docker", "image", "inspect", image], text=True))[0]
    assert inspected["Config"]["Labels"]["com.docker.compose.project"] == "ahp-handbook"
    assert inspected["Config"]["Labels"]["org.opencontainers.image.revision"] == revision
    run = ["docker", "run", "--rm", "--network=none", "--label", LABEL]
    probe = (ROOT / "sdk/python/tests/container_image_probe.py").read_bytes()
    result = checked(run + ["-i", "-w", "/tmp", image, "python", "-I", "-"], input=probe, stdout=subprocess.PIPE)
    print(result.stdout.decode(), flush=True)
    command = inspected["Config"]["Cmd"]
    assert command == ["python", "-m", "ahp_state.container"], command
    checked(run + [image] + command + ["--help"])

    # Exact tracked build context, not a second checkout or a modified real agent.
    archive = subprocess.check_output(["git", "archive", "--format=tar", "HEAD:sdk/python"], cwd=ROOT)
    with tempfile.TemporaryDirectory(prefix="ahp-sdk-negative-") as temp:
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            for entry in tar.getmembers():
                path = Path(entry.name)
                if path.is_absolute() or ".." in path.parts or entry.issym() or entry.islnk():
                    raise RuntimeError("unexpected path in tracked SDK source archive")
            tar.extractall(temp)
        agent = Path(temp) / "unsafe_agent"
        agent.mkdir()
        (agent / "agent.py").write_text("import psycopg\n\ndef build(ctx):\n    return None\n")
        negative = subprocess.run(base_args + ["--build-arg", "AGENT_DIR=unsafe_agent",
                                               "--build-arg", "AGENT_MODULE=unsafe_agent.agent",
                                               "-f", str(Path(temp) / "Dockerfile"), temp],
                                  cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = negative.stdout.decode(errors="replace")
        print(output, flush=True)
        if negative.returncode == 0 or "no-direct-db" not in output:
            raise RuntimeError("unsafe sample was not refused by the build-time SDK gate")
        print(json.dumps({"negative_build": "EXPECTED_REFUSAL", "exit_code": negative.returncode,
                          "rule": "no-direct-db"}), flush=True)
    print(json.dumps({"revision": revision, "image": image, "image_id": inspected["Id"],
                      "installed_packages": "PASS", "entrypoint_help": "PASS",
                      "unsafe_build": "REFUSED"}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
