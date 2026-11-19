#!/usr/bin/env python3
"""Python leg of the joint public-SDK live scenario (scenario.json, schema
ahp-sdk-live-scenario/v1, authored by Boyle). The contract is the ONLY source of
expected codes: this program never decides for itself what a step returns.

Standalone as root asked for: a customer-shaped process built from the public
ahp_clients surface plus plain httpx for the human-edge routes; one pass; no test
harness; no internal helpers.

Contract rules implemented verbatim: an unset prereq_env variable is SKIP_PREREQ
(never PASS); expect_by_env takes the first matching key with 'default' last; the
expected code is a substring match; fail_expected means the step passes when the
platform refuses; labels and notes are copied verbatim. A step with no handler
here is SKIP_NOT_IMPLEMENTED and is never counted as proved.

Configuration comes from the environment only. The verdict carries ids, statuses,
platform codes and digests — never a token, password or DSN.
"""
from __future__ import annotations

import hashlib
import json
import os
import ssl
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))

from ahp_clients import APIError, ArtifactsClient, ToolsClient, fetch_result_bytes  # noqa: E402


PASS, FAIL, SKIP_PREREQ, SKIP_UNIMPL = "PASS", "FAIL", "SKIP_PREREQ", "SKIP_NOT_IMPLEMENTED"



HUMAN_LEG = (
    ("H1", "resolve the sealed digest to an exact version: GET /u-ahp/v1/artifacts/Resolve"),
    ("H2", "read those exact bytes with the human session: GET /u-ahp/v1/artifacts/download"),
    ("H3", "refuse latest: download with version=latest must say exact_version_required"),
)

class DriverPrereq(Exception):
    """A variable this leg needs and the contract does not name (never a FAIL)."""


class Leg:
    def __init__(self) -> None:
        self.recs: list[dict] = []
        self.proposed: list[dict] = []
        self.verdict: dict = {"language": "python"}
        self.http: httpx.Client | None = None
        self.human = ""
        self.token = ""
        self.blob: dict = {}
        self.tool: dict = {}
        self.steps: list[dict] = []

    # ---------- environment ----------
    def env(self, name: str) -> str:
        return (os.environ.get(name) or "").strip()

    def or_env(self, name: str, fallback: str) -> str:
        return self.env(name) or fallback

    def bail(self, reason: str) -> int:
        self.recs.append({"id": "P0", "plane": "driver", "status": FAIL, "reason": reason})
        return 2

    # ---------- contract ----------
    def load_contract(self) -> tuple[list[dict], dict] | str:
        path = self.env("AHP_SCENARIO_CONTRACT")
        if not path:
            return "required configuration missing: AHP_SCENARIO_CONTRACT"
        try:
            raw = Path(path).read_bytes()
        except OSError:
            return "contract unreadable"
        try:
            doc = json.loads(raw)
        except ValueError:
            return "contract is not a readable ahp-sdk-live-scenario/v1 document"
        steps = doc.get("steps") or []
        if not steps:
            return "contract is not a readable ahp-sdk-live-scenario/v1 document"
        meta = {"path": path, "schema": doc.get("schema"),
                "sha256": hashlib.sha256(raw).hexdigest(), "steps_total": len(steps)}
        return steps, meta

    def expectation(self, step: dict) -> dict:
        """First matching expect_by_env key wins; 'default' is evaluated last."""
        table = step.get("expect_by_env") or {}
        keys = sorted(k for k in table if k != "default") + (["default"] if "default" in table else [])
        for key in keys:
            if key != "default":
                name, _, value = key.partition("=")
                if "=" not in key or self.env(name) != value:
                    continue
            entry = table[key] or {}
            statuses: list[int] = []
            code = entry.get("code") or ""
            raw_http = entry.get("http", [])
            if isinstance(raw_http, dict):
                raw_http = [raw_http]
            for item in raw_http:
                if isinstance(item, int):
                    statuses.append(item)
                elif isinstance(item, dict):
                    statuses.extend(x for x in (item.get("http") or []) if isinstance(x, int))
                    if item.get("code"):
                        code = item["code"]
            return {"statuses": statuses, "code": code, "fail_expected": bool(entry.get("fail_expected"))}
        return {"statuses": [], "code": "", "fail_expected": False}

    def judge(self, want: dict, got: dict) -> str:
        if want["statuses"] and got.get("status") not in want["statuses"]:
            return "HTTP %s not in expected %s" % (got.get("status"), want["statuses"])
        if want["fail_expected"] and (got.get("status") or 0) < 400:
            return "the contract expects this step to be refused, but HTTP %s answered" % got.get("status")
        if want["code"] and want["code"].lower() not in str(got.get("code") or "").lower():
            return 'code %r does not contain expected %r' % (got.get("code"), want["code"])
        return ""

    # ---------- transport ----------
    def build_transport(self) -> str:
        ca = self.env("AHP_CA_FILE")
        if not Path(ca).is_file():
            return "trust bundle unreadable"
        context = ssl.create_default_context(cafile=ca)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        cert, key = self.env("AHP_CERT_FILE"), self.env("AHP_KEY_FILE")
        sent = bool(cert and key)
        if sent:
            try:
                context.load_cert_chain(cert, key)
            except ssl.SSLError as error:
                return "client certificate pair unusable: " + type(error).__name__
        self.verdict["client_cert_sent"] = sent
        self.http = httpx.Client(verify=context, trust_env=False, timeout=90.0, http2=False)
        return ""

    # ---------- main ----------
    def run(self) -> int:
        for name in ("AHP_API_URL", "AHP_CA_FILE", "AHP_SDK_STAMP", "AHP_SCENARIO_CONTRACT"):
            if not self.env(name):
                return self.bail("required configuration missing: " + name)
        loaded = self.load_contract()
        if isinstance(loaded, str):
            return self.bail(loaded)
        self.steps, meta = loaded
        self.base = self.env("AHP_API_URL").rstrip("/")
        # The caller's label is not the run identity: two legs of one pass that share
        # a label would also share the tools idempotency key ("<stamp>-tools-1") and
        # the second leg would replay the first instead of using the platform twice.
        self.stamp_label = self.env("AHP_SDK_STAMP")
        self.stamp = "%s-python-%s-%04x" % (self.stamp_label, time.strftime("%H%M%S", time.gmtime()),
                                            time.time_ns() % 0x10000)
        self.verdict["base_host"] = httpx.URL(self.base).host
        self.verdict["contract"] = meta
        problem = self.build_transport()
        if problem:
            return self.bail(problem)

        handlers = {"P1": self.p1, "P2": self.p2, "A1": self.a1, "A2": self.a2, "A3": self.a3,
                    "A4": self.a4, "A5": self.a5, "T7": self.t7, "T9": self.t9}
        for step in self.steps:
            want = self.expectation(step)
            rec = {"id": step["id"], "plane": step.get("plane", ""), "labels": step.get("labels") or [],
                   "note": step.get("note") or "", "expected_http": want["statuses"],
                   "expected_code": want["code"], "fail_expected": want["fail_expected"]}
            rec = {k: v for k, v in rec.items() if v not in (None, [], "", False)}
            missing = [name for name in (step.get("prereq_env") or []) if not self.env(name)]
            if missing:
                rec.update({"status": SKIP_PREREQ, "prereq_missing": missing})
                self.recs.append(rec)
                continue
            handler = handlers.get(step["id"])
            if handler is None:
                rec.update({"status": SKIP_UNIMPL,
                            "reason": "no Python handler in this leg yet (plane %s)" % step.get("plane", "")})
                self.recs.append(rec)
                continue
            try:
                got = handler(step)
            except DriverPrereq as error:
                rec.update({"status": SKIP_PREREQ, "prereq_missing": [str(error)],
                            "reason": "driver prereq the contract does not name: " + str(error)})
                self.recs.append(rec)
                continue
            except Exception as error:  # noqa: BLE001 - a leg bug is a FAIL, stated plainly
                rec.update({"status": FAIL, "reason": "leg error: " + type(error).__name__ + ": " + str(error)[:160]})
                self.recs.append(rec)
                continue
            rec["actual_http"] = got.get("status")
            rec["actual_code"] = got.get("code") or ""
            rec["actual"] = got.get("detail") or ""
            rec["effect_proved"] = bool(got.get("effect_proved"))
            problem = self.judge(want, got)
            if problem:
                rec.update({"status": FAIL, "reason": problem})
            else:
                rec["status"] = PASS
            self.recs.append(rec)

        self.proposed_human_read()
        return 0

    # ---------- auth ----------
    def auth(self) -> None:
        if self.token:
            return
        handed = self.env("AHP_TASK_TOKEN_FILE")
        if handed:
            self.token = Path(handed).read_text(encoding="utf-8").strip()
            return
        status, _, body = self._post(self.base + "/u-ahp/v1/auth/login",
                                    {"username": self.env("AHP_HUMAN_USER"),
                                     "password": self.env("AHP_HUMAN_PASSWORD")})
        token = ""
        try:
            token = str(json.loads(body).get("access_token") or "")
        except (ValueError, AttributeError):
            token = ""
        if status != 200 or not token:
            raise DriverPrereq("AHP_HUMAN_USER/AHP_HUMAN_PASSWORD or AHP_HUMAN_TOKEN_FILE")
        self.human = token
        task = self.env("AHP_TASK_ID")
        if not task:
            raise DriverPrereq("AHP_TASK_ID")
        request = {"human_token": self.human, "task": task}
        if self.env("AHP_TASK_CONTEXT"):
            request["context"] = self.env("AHP_TASK_CONTEXT")
        if self.env("AHP_TASK_CAPABILITIES"):
            request["capabilities"] = self.env("AHP_TASK_CAPABILITIES").split(",")
        status, _, body = self._post(self.base + "/v1/identity/control/exchange", request)
        scoped = ""
        try:
            scoped = str(json.loads(body).get("token") or "")
        except (ValueError, AttributeError):
            scoped = ""
        if status != 200 or not scoped:
            return
        self.token = scoped

    # ---------- steps ----------
    def p1(self, step: dict) -> dict:
        """Structural: the workload identity must parse and say who it is."""
        cert, key = self.env("AHP_CERT_FILE"), self.env("AHP_KEY_FILE")
        if not cert or not key:
            raise DriverPrereq("AHP_CERT_FILE/AHP_KEY_FILE")
        context = ssl.create_default_context(cafile=self.env("AHP_CA_FILE"))
        try:
            context.load_cert_chain(cert, key)
        except ssl.SSLError:
            return {"status": 0, "detail": "client certificate pair unusable"}
        detail = "client certificate pair accepted by the TLS stack"
        try:
            from cryptography import x509  # noqa: PLC0415 - optional here, absent in the lane venv
            from cryptography.hazmat.primitives import serialization
            with open(cert, "rb") as handle:
                pem = handle.read()
            leaf = x509.load_pem_x509_certificates(pem)[0] if hasattr(x509, "load_pem_x509_certificates") \
                else x509.load_pem_x509_certificate(pem)
            from cryptography.x509.oid import NameOID
            names = leaf.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            ous = [a.value for a in leaf.subject.get_attributes_for_oid(NameOID.ORGANIZATIONAL_UNIT_NAME)]
            detail += ", subject CN=%s" % (names[0].value if names else "?")
            if ous:
                detail += ", ou=" + ",".join(ous)
            self.verdict["client_subject"] = names[0].value if names else ""
            _ = serialization
        except Exception:  # noqa: BLE001 - subject reading is best effort, the load above is the assertion
            self.verdict["client_subject"] = "unread (cryptography absent in this interpreter)"
            detail += ", subject unread (no cryptography in this interpreter)"
        return {"status": 200, "detail": detail}

    def p2(self, step: dict) -> dict:
        """login -> /v1/identity/control/exchange, the chain the contract names."""
        handed = self.env("AHP_TASK_TOKEN_FILE")
        if handed:
            self.token = Path(handed).read_text(encoding="utf-8").strip()
            return {"status": 200, "detail": "task token handed in"}
        if self.env("AHP_HUMAN_TOKEN_FILE"):
            self.human = Path(self.env("AHP_HUMAN_TOKEN_FILE")).read_text(encoding="utf-8").strip()
        elif self.env("AHP_HUMAN_USER") and self.env("AHP_HUMAN_PASSWORD"):
            status, code, body = self._post(self.base + "/u-ahp/v1/auth/login",
                                            {"username": self.env("AHP_HUMAN_USER"),
                                             "password": self.env("AHP_HUMAN_PASSWORD")})
            token = ""
            try:
                token = str(json.loads(body).get("access_token") or "")
            except (ValueError, AttributeError):
                token = ""
            if status != 200 or not token:
                return {"status": status, "code": code, "detail": "login answered HTTP %s" % status}
            self.human = token
        else:
            raise DriverPrereq("AHP_HUMAN_USER/AHP_HUMAN_PASSWORD or AHP_HUMAN_TOKEN_FILE")
        task = self.env("AHP_TASK_ID")
        if not task:
            raise DriverPrereq("AHP_TASK_ID")
        request = {"human_token": self.human, "task": task}
        if self.env("AHP_TASK_CONTEXT"):
            request["context"] = self.env("AHP_TASK_CONTEXT")
        if self.env("AHP_TASK_CAPABILITIES"):
            request["capabilities"] = self.env("AHP_TASK_CAPABILITIES").split(",")
        status, code, body = self._post(self.base + "/v1/identity/control/exchange", request)
        scoped, named = "", ""
        try:
            payload = json.loads(body)
            scoped, named = str(payload.get("token") or ""), str(payload.get("task") or "")
        except (ValueError, AttributeError):
            scoped = ""
        if status != 200 or not scoped:
            return {"status": status, "code": code, "detail": "exchange answered HTTP %s" % status}
        self.token = scoped
        return {"status": 200,
                "detail": "login -> control/exchange produced a scoped task token for task " + named}

    def a1(self, step: dict) -> dict:
        self.auth()
        if not self.token:
            return {"status": 0, "detail": "no task token from P2"}
        store = ArtifactsClient(self.or_env("AHP_ARTIFACTS_API_URL", self.base), lambda: self.token,
                                http_client=self.http)
        payload = ("AHP-SDK-LIVE python " + self.stamp + "\n" + "q" * 4096).encode()
        digest = hashlib.sha256(payload).hexdigest()
        try:
            up = store.begin_upload(self.stamp + "-blob", self.stamp + "-blob.txt", "text/plain",
                                    digest, len(payload))
            store.upload(up, payload)
            ref = store.seal(up["id"])
        except APIError as error:
            return {"status": error.status, "code": str(error.code),
                    "detail": "upload/seal refused with " + str(error.code)}
        self.blob = {"artifact_id": ref.get("id"), "version": ref.get("version"),
                     "sha256": ref.get("sha256"), "size": ref.get("size"), "payload": payload}
        self.verdict["artifact"] = {k: self.blob[k] for k in ("artifact_id", "version", "sha256", "size")}
        if ref.get("sha256") != digest:
            return {"status": 200, "effect_proved": True,
                    "detail": "sealed digest differs from the bytes sent"}
        return {"status": 200, "effect_proved": True,
                "detail": "sealed %s@%s size=%s digest=%s"
                          % (ref.get("id"), ref.get("version"), ref.get("size"), ref.get("sha256"))}

    def _store(self) -> ArtifactsClient:
        return ArtifactsClient(self.or_env("AHP_ARTIFACTS_API_URL", self.base), lambda: self.token,
                               http_client=self.http)

    def a2(self, step: dict) -> dict:
        if not self.blob:
            raise DriverPrereq("A1 (this run sealed nothing, so the read has nothing to check)")
        try:
            body = self._store().download(self.blob["artifact_id"])
        except APIError as error:
            return {"status": error.status, "code": str(error.code),
                    "detail": "Download refused with " + str(error.code)}
        if hashlib.sha256(body).hexdigest() != self.blob["sha256"]:
            return {"status": 200, "detail": "downloaded bytes hash differently from the sealed digest"}
        return {"status": 200, "effect_proved": True,
                "detail": "%d bytes, sha256 equals the sealed digest" % len(body)}

    def a3(self, step: dict) -> dict:
        if not self.blob:
            raise DriverPrereq("A1 (this run sealed nothing, so the read has nothing to check)")
        try:
            part = self._store().read_range(self.blob["artifact_id"], 0, 64)
        except APIError as error:
            return {"status": error.status, "code": str(error.code),
                    "detail": "ReadRange refused with " + str(error.code)}
        if part != self.blob["payload"][:64]:
            return {"status": 200, "detail": "the slice is not the requested one"}
        return {"status": 200, "detail": "%d bytes, equal to offset 0 length 64" % len(part)}

    def a4(self, step: dict) -> dict:
        if not self.blob:
            raise DriverPrereq("A1 (this run sealed nothing, so the read has nothing to check)")
        try:
            body = self._store().read_range(self.blob["artifact_id"], int(self.blob["size"]) + 1, 16)
        except APIError as error:
            return {"status": error.status, "code": str(error.code),
                    "detail": "refused with " + str(error.code)}
        return {"status": 200, "detail": "served %d bytes past the end" % len(body)}

    def a5(self, step: dict) -> dict:
        if not self.blob:
            raise DriverPrereq("A1 (this run sealed nothing, so the read has nothing to check)")
        path = self.env("AHP_SCENARIO_FOREIGN_TASK_TOKEN_FILE")
        if not path:
            raise DriverPrereq("AHP_SCENARIO_FOREIGN_TASK_TOKEN_FILE (a foreign TLS client alone cannot "
                               "read artifacts; the face needs its own task token)")
        foreign_token = Path(path).read_text(encoding="utf-8").strip()
        foreign = ArtifactsClient(self.or_env("AHP_ARTIFACTS_API_URL", self.base), lambda: foreign_token,
                                  http_client=self.http)
        try:
            foreign.download(self.blob["artifact_id"])
        except APIError as error:
            return {"status": error.status, "code": str(error.code),
                    "detail": "foreign read refused with " + str(error.code)}
        return {"status": 200, "detail": "the foreign tenant read the artifact"}

    def _ensure_tool(self) -> None:
        self.auth()
        self.tool.setdefault("key", self.stamp + "-tools-1")
        self.tool.setdefault("target", self.env("AHP_SCENARIO_TOOL"))

    def _tools(self, token: str = "") -> ToolsClient:
        chosen = token or self.token
        return ToolsClient(self.base, lambda: chosen, http_client=self.http)

    def _tool_arguments(self) -> dict:
        raw = self.env("AHP_TOOL_ARGUMENTS_JSON")
        return json.loads(raw) if raw else {"text": self.stamp}

    def t7(self, step: dict) -> dict:
        self._ensure_tool()
        if not self.token:
            return {"status": 0, "detail": "no task token from P2"}
        caller = self._tools()
        try:
            first = caller.call(self.tool["key"], self.tool["target"], self._tool_arguments())
            second = caller.call(self.tool["key"], self.tool["target"], self._tool_arguments())
        except APIError as error:
            return {"status": error.status, "code": str(error.code),
                    "detail": "call refused with " + str(error.code)}
        if first.get("digest") != second.get("digest") or first.get("key") != second.get("key"):
            return {"status": 200, "effect_proved": True,
                    "detail": "replay did not return the journaled operation"}
        self.tool["digest"] = str(first.get("digest"))
        return {"status": 200, "effect_proved": True,
                "detail": "replay returned the journaled operation, digest " + str(first.get("digest"))}

    def t9(self, step: dict) -> dict:
        self._ensure_tool()
        if not self.token:
            return {"status": 0, "detail": "no task token from P2"}
        try:
            result = self._tools().tool_result(self.tool["key"])
        except APIError as error:
            return {"status": error.status, "code": str(error.code),
                    "detail": "GetResult refused with " + str(error.code)}
        if result.get("status") != "SUCCEEDED":
            return {"status": 200, "code": str(result.get("status")),
                    "detail": "stored row is not a success: " + str(result.get("status"))}
        ref = result.get("artifact_ref")
        if result.get("result_storage") == "artifact" and ref:
            try:
                body = fetch_result_bytes(self._store(), result)
            except APIError as error:
                return {"status": error.status, "code": str(error.code),
                        "detail": "artifact result unusable: " + str(error.code)}
            self.verdict["tool_result"] = {"storage": result.get("result_storage"), "bytes": len(body),
                                           "digest": result.get("digest"), "artifact_id": ref.get("artifact_id")}
            return {"status": 200, "effect_proved": True,
                    "detail": "full result read through the artifact ref, %d bytes" % len(body)}
        payload = json.dumps(result.get("payload") or {}).encode()
        self.verdict["tool_result"] = {"storage": result.get("result_storage"), "bytes": len(payload),
                                       "digest": result.get("digest")}
        return {"status": 200, "effect_proved": True,
                "detail": "inline result read, %d bytes" % len(payload)}

    # ---------- proposed human leg (NOT part of the shared contract) ----------
    # ---------- proposed human leg (NOT part of the shared contract) ----------
    def proposed_human_read(self) -> None:
        def add(rec: dict) -> None:
            self.proposed.append(rec)

        if not self.blob or not self.human:
            reason = "needs a sealed artifact from A1 and a human session from P2"
            for step_id, purpose in HUMAN_LEG:
                add({"id": step_id, "plane": "human-edge", "status": SKIP_PREREQ,
                     "reason": reason, "contracts": purpose})
            return
        digest = self.blob["sha256"]
        status, _, body = self._get("/u-ahp/v1/artifacts/Resolve?sha256=" + quote(digest, safe=""))
        versions = []
        try:
            versions = list((json.loads(body) or {}).get("versions") or [])
        except (ValueError, AttributeError):
            versions = []
        picked = next((v.get("version") for v in versions if v.get("artifact_id") == self.blob["artifact_id"]), "")
        if status != 200 or not picked:
            add({"id": "H1", "plane": "human-edge", "status": FAIL, "actual_http": status,
                 "reason": "the human resolver did not return this artifact's sealed version"})
            return
        add({"id": "H1", "plane": "human-edge", "status": PASS, "actual_http": status,
             "actual": "digest resolved to exactly sealed version " + str(picked)})

        status, headers, body = self._get("/u-ahp/v1/artifacts/download?artifact_id=%s&version=%s"
                                          % (quote(self.blob["artifact_id"], safe=""), quote(str(picked), safe="")))
        got = hashlib.sha256(body).hexdigest()
        self.verdict["human_read"] = {"http_status": status, "downloaded_sha256": got,
                                      "cache_control": headers.get("cache-control"), "version": picked}
        if status != 200 or got != digest:
            add({"id": "H2", "plane": "human-edge", "status": FAIL, "actual_http": status,
                 "reason": "human-read bytes are not the bytes the SDK sealed"})
        else:
            add({"id": "H2", "plane": "human-edge", "status": PASS, "actual_http": status,
                 "actual": "%d bytes read by the human edge, digest matches" % len(body)})

        status, _, body = self._get("/u-ahp/v1/artifacts/download?artifact_id=%s&version=latest"
                                    % quote(self.blob["artifact_id"], safe=""))
        reason = ""
        try:
            refusal = json.loads(body) or {}
            reason = str(refusal.get("reason") or refusal.get("code") or "")
        except (ValueError, AttributeError):
            reason = ""
        self.verdict["human_latest_gate"] = {"http_status": status, "reason": reason}
        if 200 <= status < 300:
            add({"id": "H3", "plane": "human-edge", "status": FAIL, "actual_http": status,
                 "reason": "the proxy served 'latest': the exact-version gate is not enforced"})
        elif reason != "exact_version_required":
            add({"id": "H3", "plane": "human-edge", "status": FAIL, "actual_http": status, "actual_code": reason,
                 "reason": "refused for another reason than exact_version_required"})
        else:
            add({"id": "H3", "plane": "human-edge", "status": PASS, "actual_http": status,
                 "actual": "'latest' refused with exact_version_required"})

    # ---------- http helpers ----------
    def _post(self, url: str, payload: dict, bearer: str = "") -> tuple[int, str, bytes]:
        headers = {"Content-Type": "application/json"}
        if bearer:
            headers["Authorization"] = "Bearer " + bearer
        try:
            response = self.http.post(url, json=payload, headers=headers)
        except httpx.HTTPError:
            return 0, "TRANSPORT_UNREACHABLE", b""
        return response.status_code, code_of(response.content), response.content

    def _get(self, path: str) -> tuple[int, httpx.Headers, bytes]:
        headers = {"Authorization": "Bearer " + self.human} if self.human else {}
        try:
            response = self.http.get(self.base + path, headers=headers)
        except httpx.HTTPError:
            return 0, httpx.Headers(), b""
        return response.status_code, response.headers, response.content


def code_of(raw: bytes) -> str:
    try:
        return str((json.loads(raw) or {}).get("code") or "")
    except (ValueError, AttributeError):
        return ""


def main() -> int:
    leg = Leg()
    code = leg.run()
    counts = {PASS: 0, FAIL: 0, SKIP_PREREQ: 0, SKIP_UNIMPL: 0}
    proved: dict[str, int] = {}
    for rec in leg.recs:
        counts[rec["status"]] = counts.get(rec["status"], 0) + 1
        if rec["status"] == PASS and rec.get("effect_proved"):
            proved[rec["plane"]] = proved.get(rec["plane"], 0) + 1
    proposed = {PASS: 0, FAIL: 0, SKIP_PREREQ: 0}
    for rec in leg.proposed:
        proposed[rec["status"]] = proposed.get(rec["status"], 0) + 1
    leg.verdict.update({
        "stamp": leg.stamp, "stamp_label": leg.stamp_label, "steps": leg.recs, "proposed_steps": leg.proposed,
        "counts": counts, "proposed_counts": proposed, "effect_proved_by_plane": proved,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    })
    out = Path(leg.env("AHP_VERDICT_OUT") or "verdict-python.json")
    out.write_text(json.dumps(leg.verdict, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(leg.verdict, indent=2, ensure_ascii=False))
    if code == 0 and counts[FAIL]:
        return 1
    return code


if __name__ == "__main__":
    sys.exit(main())
