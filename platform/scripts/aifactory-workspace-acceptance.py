"""Real isolated Workspace API acceptance; check evidence is downloaded file bytes.

Requires AHP_UI_STAND_DIR with certs/{ca.crt,source.crt,source.key} and
acceptance-task.token obtained through the verified human control exchange.
No credentials or presigned URLs are printed or written to the report.
"""
import hashlib
import json
import os
from pathlib import Path
import ssl
import time
import urllib.error
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


root = Path(os.environ["AHP_UI_STAND_DIR"])
origin = os.environ.get("AHP_UI_BACKEND_ORIGIN", "https://127.0.0.1:29820")
tls = ssl.create_default_context(cafile=str(root / "certs/ca.crt"))
tls.load_cert_chain(root / "certs/source.crt", root / "certs/source.key")
client = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(context=tls))
token = (root / "acceptance-task.token").read_text().strip()
proof = {"workspace": "browser-files-" + str(time.time_ns()), "steps": []}


def call(plane, op, body, expected=200, binary=False):
    request = urllib.request.Request(
        origin + "/v1/" + plane + "/" + op,
        data=json.dumps(body).encode(),
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    try:
        response = client.open(request, timeout=30)
    except urllib.error.HTTPError as error:
        response = error
    data = response.read()
    status = response.status
    proof["steps"].append({"operation": plane + "/" + op, "status": status})
    if status != expected:
        error = json.loads(data)
        raise RuntimeError(op + " HTTP " + str(status) + " " + error.get("code", "") + " " + error.get("message", ""))
    return data if binary else json.loads(data)


try:
    workspace = call("workspace", "Create", {"workspace": proof["workspace"]})
    proof["workspace_id"] = workspace["id"]
    content = b"AHP isolated Workspace acceptance: sealed bytes, checked candidate, published revision.\n"
    digest = hashlib.sha256(content).hexdigest()
    upload = call("artifacts", "BeginUpload", {"key": proof["workspace"], "name": "checked.txt", "mime": "text/plain", "sha256": digest, "size": len(content), "labels": {}})
    # The signed URL is only used for this PUT, never included in exception output.
    try:
        response = urllib.request.urlopen(urllib.request.Request(upload["presigned_url"], data=content, method="PUT"), timeout=30)
        if response.status not in (200, 204):
            raise RuntimeError("upload refused")
    except Exception:
        raise RuntimeError("presigned object upload failed; URL omitted") from None
    ref = call("artifacts", "Seal", {"id": upload["id"]})
    proof["artifact"] = {k: ref[k] for k in ("id", "version", "sha256")}
    assert ref["sha256"] == digest and ref["size"] == len(content)
    candidate = call("workspace", "CreateCandidate", {"workspace": proof["workspace"]})
    files = [{"path": "checked.txt", "artifact_id": ref["id"], "mode": 420, "digest": digest}]
    candidate = call("workspace", "UpdateCandidateFiles", {"candidate_id": candidate["id"], "files": files})
    proof.update(candidate=candidate["id"], candidate_digest=candidate["digest"])
    refused = call("workspace", "Publish", {"candidate_id": candidate["id"], "expected_head": ""}, expected=409)
    assert refused["code"] == "CONFLICT"
    # Execute an actual check, then record its result for this exact candidate.
    sealed_bytes = call("artifacts", "Download", {"id": ref["id"]}, binary=True)
    assert sealed_bytes == content and hashlib.sha256(sealed_bytes).hexdigest() == digest
    checked = call("workspace", "GetCandidate", {"candidate_id": candidate["id"]})
    assert checked["digest"] == candidate["digest"]
    # This file set has ASCII strings and an integer mode, so sorted compact JSON
    # is exactly the platform's canonical encoding (not a general JSON codec).
    expected_candidate = "sha256:" + hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert checked["digest"] == expected_candidate
    call("workspace", "SetCheck", {"candidate_id": candidate["id"], "name": "sealed-byte-integrity", "status": "passed", "detail": "Downloaded sealed artifact and verified SHA-256=" + digest + "; exact candidate paths and digest re-read before publication."})
    # Uploaded artifacts start private to their owner. Explicit owner publication
    # grants this workspace access; checks cannot widen artifact visibility.
    call("artifacts", "Publish", {"id": ref["id"], "workspace": proof["workspace"]})
    revision = call("workspace", "Publish", {"candidate_id": candidate["id"], "expected_head": ""})
    published = call("workspace", "ReadFile", {"workspace": proof["workspace"], "revision": revision["id"], "path": "checked.txt"}, binary=True)
    assert published == content and hashlib.sha256(published).hexdigest() == digest
    proof.update(status="passed", workspace_id=workspace["id"], candidate=candidate["id"], candidate_digest=candidate["digest"], revision=revision["id"], artifact={k: ref[k] for k in ("id", "version", "sha256")}, bytes=len(content), hashMatch=True, noCheckPublishStatus=409)
except Exception as error:
    proof.update(status="failed", error=str(error))
    raise
finally:
    if os.environ.get("AHP_UI_WORKSPACE_REPORT"):
        Path(os.environ["AHP_UI_WORKSPACE_REPORT"]).write_text(json.dumps(proof, indent=2) + "\n")
    print(json.dumps(proof, indent=2))
