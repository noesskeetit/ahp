"""Public, task-authenticated Artifacts API; no framework or storage credentials.

HTTP pools are caller-owned. Each owner call obtains current authority. Calls
are single-attempt: on UNKNOWN reconcile the same upload ID or operation key.
Presigned PUT uses a fresh credential-free pool with redirects disabled.
"""
from __future__ import annotations

import hashlib
import re
import ssl
from urllib.parse import urlsplit

import httpx

from ._transport import APIError, AsyncTransport, Transport, decode, headers, required

MAX_ARTIFACT_BYTES = 64 << 20
MAX_METADATA_BYTES = 1 << 20
_PREFIX = "/v1/artifacts/"


def _invalid(message: str, *, unknown=False):
    return APIError(0, "INVALID_RESPONSE", message, outcome_unknown=unknown)


def _size(value):
    return type(value) is int and 0 < value <= MAX_ARTIFACT_BYTES


def _digest(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _ref(ref: dict, artifact_id: str | None = None) -> dict:
    if (not isinstance(ref, dict) or not all(isinstance(ref.get(k), str) and ref[k]
            for k in ("id", "version", "tenant", "deployment", "subject"))
            or not _size(ref.get("size")) or not _digest(ref.get("sha256"))
            or (artifact_id is not None and ref["id"] != artifact_id)):
        raise _invalid("invalid sealed artifact reference")
    return ref


def _upload_request(key, name, mime, sha256, size, labels, journal_stage=False):
    if not _size(size) or not _digest(sha256):
        raise ValueError("artifact declaration requires 1 byte..64 MiB and SHA-256")
    body = dict(key=required(key), name=required(name, "name"), mime=required(mime, "MIME"),
                sha256=sha256, size=size, labels=labels or {})
    if journal_stage:
        body["journal_stage"] = True
    return body


def _upload(up, data):
    if not isinstance(up, dict):
        raise ValueError("upload descriptor must be an object")
    if (not isinstance(data, bytes) or not _size(len(data)) or up.get("size") != len(data)
            or up.get("sha256") != hashlib.sha256(data).hexdigest()):
        raise ValueError("upload bytes differ from declaration")
    target = urlsplit(up.get("presigned_url", ""))
    if (target.scheme not in ("https", "http") or not target.hostname
            or target.username is not None or target.password is not None or target.fragment):
        raise ValueError("invalid presigned upload URL")
    values = up.get("headers") or {}
    if not isinstance(values, dict):
        raise ValueError("invalid upload headers")
    for key, value in values.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError("upload headers must contain strings")
        if key.lower() in ("authorization", "proxy-authorization", "cookie", "host"):
            raise ValueError("unsafe upload header")
        if key.lower() == "content-length" and str(value) != str(len(data)):
            raise ValueError("upload Content-Length differs from declaration")
    return values


def _bytes(ref, status, response_headers, data, offset=None, length=None):
    if offset is None:
        if status != 200 or len(data) != ref["size"] or hashlib.sha256(data).hexdigest() != ref["sha256"]:
            raise _invalid("artifact digest or size mismatch")
    else:
        expected = f"bytes {offset}-{offset + length - 1}/{ref['size']}"
        if (status != 206 or len(data) != length or response_headers.get("content-range") != expected
                or response_headers.get("etag", "").strip('"') != ref["sha256"]):
            raise _invalid("artifact range version or size mismatch")
    return data


def _range(ref, offset, length):
    if type(offset) is not int or type(length) is not int or offset < 0 or not _size(length) or offset >= ref["size"]:
        raise ValueError("range outside artifact")
    return min(length, ref["size"] - offset)


def _decode_response(raw, status, mutation):
    try:
        return decode(raw, status)
    except APIError as error:
        if len(error.code) > 64 or len(error.reason) > 128:
            raise APIError(status, "INVALID_RESPONSE", "invalid error identifier", outcome_unknown=mutation and (200 <= status < 300 or status >= 500)) from None
        if mutation and (200 <= status < 300 or status >= 500 or error.reason == "transfer_outcome_unknown"):
            error.outcome_unknown = True
        raise


class ArtifactsClient:
    """Owner API client. Supply a verified mTLS pool and a current task-token callback.

    upload_ca_file only adds trust roots to a new TLS context; owner credentials,
    cookies, proxy settings and client certificates are never copied to PUT.
    """

    def __init__(self, base_url, token, *, http_client: httpx.Client, upload_ca_file: str | None = None):
        self._t = Transport(base_url, token, http_client=http_client)
        self._upload_tls = ssl.create_default_context(cafile=upload_ca_file)

    def _exchange(self, op, payload, *, mutation=False, limit=MAX_METADATA_BYTES, range_header=None):
        request_headers = headers(self._t.token(), "application/json")
        if range_header:
            request_headers["Range"] = range_header
        try:
            with self._t.http.stream("POST", self._t.base + _PREFIX + op, json=payload,
                                       headers=request_headers, follow_redirects=False) as response:
                bound = limit if 200 <= response.status_code < 300 else MAX_METADATA_BYTES
                body = bytearray()
                for chunk in response.iter_bytes():
                    if len(chunk) > bound - len(body):
                        raise _invalid("artifact response exceeds declared limit", unknown=mutation)
                    body.extend(chunk)
                if not 200 <= response.status_code < 300:
                    _decode_response(bytes(body), response.status_code, mutation)
                return response.status_code, response.headers, bytes(body)
        except httpx.HTTPError:
            raise APIError(0, "UNKNOWN" if mutation else "UNAVAILABLE", "artifact transport interrupted; reconcile same ID/key",
                           outcome_unknown=mutation) from None

    def _call(self, op, payload, *, mutation=False):
        status, _, raw = self._exchange(op, payload, mutation=mutation)
        return _decode_response(raw, status, mutation)

    def begin_upload(self, key: str, name: str, mime: str, sha256: str, size: int, *,
                     labels: dict | None = None, journal_stage: bool = False) -> dict:
        """Declare immutable bytes under a persisted operation key; replay the same key."""
        request = _upload_request(key, name, mime, sha256, size, labels, journal_stage)
        up = self._call("BeginUpload", request, mutation=True)
        if not up.get("id") or up.get("sha256") != sha256 or up.get("size") != size:
            raise _invalid("owner upload declaration mismatch", unknown=True)
        return up

    def upload(self, upload: dict, data: bytes) -> None:
        """PUT exactly declared bytes without bearer, cookies or workload certificate."""
        values = _upload(upload, data)
        try:
            with httpx.Client(verify=self._upload_tls, timeout=60, follow_redirects=False, trust_env=False) as blob:
                with blob.stream("PUT", upload["presigned_url"], content=data, headers=values) as response:
                    if not 200 <= response.status_code < 300:
                        refused = APIError(response.status_code,
                                           "UNKNOWN" if response.status_code >= 500 else "INVALID_ARGUMENT",
                                           "presigned upload refused" + ("; reconcile same upload" if response.status_code >= 500 else ""),
                                           outcome_unknown=response.status_code >= 500)
                        raise refused
        except httpx.HTTPError:
            raise APIError(0, "UNKNOWN", "presigned upload unconfirmed; reconcile same upload", outcome_unknown=True) from None

    def seal(self, artifact_id: str) -> dict:
        """Freeze the owner's verified object version; repeat the same ID to reconcile."""
        ref = self._call("Seal", {"id": required(artifact_id, "artifact ID")}, mutation=True)
        try:
            return _ref(ref, artifact_id)
        except APIError as error:
            error.outcome_unknown = True
            raise

    def get(self, artifact_id: str) -> dict:
        """Read immutable metadata with current authority, never a cached decision."""
        return _ref(self._call("Get", {"id": required(artifact_id, "artifact ID")}), artifact_id)

    def put(self, key: str, name: str, mime: str, data: bytes, *,
            labels: dict | None = None, journal_stage: bool = False) -> dict:
        """Begin, PUT and Seal; a lost answer reconciles the SAME upload, never a re-key.

        A missing answer is not proof of a missing write. Seal verifies at the
        owner the bytes that actually landed (SHA-256, size, pinned version),
        so a confirmed Seal IS the upload confirmation and re-sending would be
        exactly the duplicate the contract forbids. Only when the owner cannot
        confirm does the SAME upload get one more presentation - bounded and
        observable; if that answer is lost too the caller keeps reconciling by
        the same ID/key, and BeginUpload of that key returns this same ID.
        """
        if not isinstance(data, bytes) or not _size(len(data)):
            raise ValueError("artifact data must be 1 byte..64 MiB")
        digest = hashlib.sha256(data).hexdigest()
        options = {"journal_stage": True} if journal_stage else {}
        up = self.begin_upload(key, name, mime, digest, len(data), labels=labels, **options)
        try:
            self.upload(up, data)
        except APIError as error:
            if not error.outcome_unknown:
                raise
            ref, seal_error = None, None
            try:
                ref = self.seal(up["id"])
            except APIError as failure:
                seal_error = failure
            if ref is not None:
                if ref["sha256"] != digest or ref["size"] != len(data):
                    raise _invalid("sealed artifact differs from upload", unknown=True)
                return ref
            if seal_error.code == "NOT_FOUND":
                raise
            self.upload(up, data)
            ref = self.seal(up["id"])
            if ref["sha256"] != digest or ref["size"] != len(data):
                raise _invalid("sealed artifact differs from upload", unknown=True)
            return ref
        ref = self.seal(up["id"])
        if ref["sha256"] != digest or ref["size"] != len(data):
            raise _invalid("sealed artifact differs from upload", unknown=True)
        return ref

    def download(self, artifact_id: str) -> bytes:
        """Read complete bytes through owner API and verify exact size and SHA-256."""
        ref = self.get(artifact_id)
        status, values, data = self._exchange("Download", {"id": artifact_id}, limit=ref["size"])
        return _bytes(ref, status, values, data)

    def read_range(self, artifact_id: str, offset: int, length: int) -> bytes:
        """Read a bounded exact-version range; length is clamped only at artifact EOF."""
        ref = self.get(artifact_id)
        length = _range(ref, offset, length)
        status, values, data = self._exchange("Download", {"id": artifact_id}, limit=length,
                                                range_header=f"bytes={offset}-{offset + length - 1}")
        return _bytes(ref, status, values, data, offset, length)

    def readiness(self, artifact_id: str) -> dict:
        """Read content/extraction/search readiness; sealed never implies extracted."""
        return self._call("Readiness", {"id": required(artifact_id, "artifact ID")})

    def process(self, artifact_id: str) -> dict:
        """Request owner sandbox extraction; requires current derive authority."""
        return self._call("Process", {"id": required(artifact_id, "artifact ID")}, mutation=True)

    def read_text_chunk(self, artifact_id: str, version: str, offset: int, limit: int, context_budget: int) -> dict:
        """Read owner-extracted UTF-8 with explicit version and context budget."""
        return self._call("ReadTextChunk", dict(id=required(artifact_id, "artifact ID"),
                            version=required(version, "version"), offset=offset, limit=limit, context_budget=context_budget))

    def search_text(self, artifact_id: str, version: str, query: str) -> dict:
        """Search one exact version's owner-maintained index under current authority."""
        return self._call("SearchText", dict(id=required(artifact_id, "artifact ID"),
                            version=required(version, "version"), query=query))

    def resolve(self, sha256: str) -> dict:
        """Resolve a digest inside current scope; grants no new read authority."""
        return self._call("Resolve", {"sha256": sha256})

    def transfer_grant(self, ref: dict, target_deployment: str, target_context: str, target_task: str) -> dict:
        """Delegate one exact source to a current target task of the same subject."""
        _ref(ref)
        grant = self._call("TransferGrant", dict(id=ref["id"], version=ref["version"],
                            target_deployment=required(target_deployment, "target deployment"),
                            target_context=required(target_context, "target context"),
                            target_task=required(target_task, "target task")), mutation=True)
        if grant.get("artifact_id") != ref["id"] or grant.get("version") != ref["version"] or grant.get("sha256") != ref["sha256"]:
            raise _invalid("transfer grant differs from source reference", unknown=True)
        return grant

    def copy_from_transfer_grant(self, grant: str, key: str) -> dict:
        """Owner-copy with preserved provenance/taint. UNKNOWN must keep the SAME key."""
        ref = self._call("CopyFromTransferGrant", {"grant": required(grant, "grant"), "key": required(key)}, mutation=True)
        try:
            return _ref(ref)
        except APIError as error:
            error.outcome_unknown = True
            raise


class AsyncArtifactsClient:
    """Owner API client. Supply a verified mTLS pool and a current task-token callback.

    upload_ca_file only adds trust roots to a new TLS context; owner credentials,
    cookies, proxy settings and client certificates are never copied to PUT.
    """

    def __init__(self, base_url, token, *, http_client: httpx.AsyncClient, upload_ca_file: str | None = None):
        self._t = AsyncTransport(base_url, token, http_client=http_client)
        self._upload_tls = ssl.create_default_context(cafile=upload_ca_file)

    async def _exchange(self, op, payload, *, mutation=False, limit=MAX_METADATA_BYTES, range_header=None):
        request_headers = await self._t._headers("application/json")
        if range_header:
            request_headers["Range"] = range_header
        try:
            async with self._t.http.stream("POST", self._t.base + _PREFIX + op, json=payload,
                                       headers=request_headers, follow_redirects=False) as response:
                bound = limit if 200 <= response.status_code < 300 else MAX_METADATA_BYTES
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(chunk) > bound - len(body):
                        raise _invalid("artifact response exceeds declared limit", unknown=mutation)
                    body.extend(chunk)
                if not 200 <= response.status_code < 300:
                    _decode_response(bytes(body), response.status_code, mutation)
                return response.status_code, response.headers, bytes(body)
        except httpx.HTTPError:
            raise APIError(0, "UNKNOWN" if mutation else "UNAVAILABLE", "artifact transport interrupted; reconcile same ID/key",
                           outcome_unknown=mutation) from None

    async def _call(self, op, payload, *, mutation=False):
        status, _, raw = await self._exchange(op, payload, mutation=mutation)
        return _decode_response(raw, status, mutation)

    async def begin_upload(self, key: str, name: str, mime: str, sha256: str, size: int, *,
                           labels: dict | None = None, journal_stage: bool = False) -> dict:
        """Declare immutable bytes under a persisted operation key; replay the same key."""
        request = _upload_request(key, name, mime, sha256, size, labels, journal_stage)
        up = await self._call("BeginUpload", request, mutation=True)
        if not up.get("id") or up.get("sha256") != sha256 or up.get("size") != size:
            raise _invalid("owner upload declaration mismatch", unknown=True)
        return up

    async def upload(self, upload: dict, data: bytes) -> None:
        """PUT exactly declared bytes without bearer, cookies or workload certificate."""
        values = _upload(upload, data)
        try:
            async with httpx.AsyncClient(verify=self._upload_tls, timeout=60, follow_redirects=False, trust_env=False) as blob:
                async with blob.stream("PUT", upload["presigned_url"], content=data, headers=values) as response:
                    if not 200 <= response.status_code < 300:
                        refused = APIError(response.status_code,
                                           "UNKNOWN" if response.status_code >= 500 else "INVALID_ARGUMENT",
                                           "presigned upload refused" + ("; reconcile same upload" if response.status_code >= 500 else ""),
                                           outcome_unknown=response.status_code >= 500)
                        raise refused
        except httpx.HTTPError:
            raise APIError(0, "UNKNOWN", "presigned upload unconfirmed; reconcile same upload", outcome_unknown=True) from None

    async def seal(self, artifact_id: str) -> dict:
        """Freeze the owner's verified object version; repeat the same ID to reconcile."""
        ref = await self._call("Seal", {"id": required(artifact_id, "artifact ID")}, mutation=True)
        try:
            return _ref(ref, artifact_id)
        except APIError as error:
            error.outcome_unknown = True
            raise

    async def get(self, artifact_id: str) -> dict:
        """Read immutable metadata with current authority, never a cached decision."""
        return _ref(await self._call("Get", {"id": required(artifact_id, "artifact ID")}), artifact_id)

    async def put(self, key: str, name: str, mime: str, data: bytes, *,
                  labels: dict | None = None, journal_stage: bool = False) -> dict:
        """Begin, PUT and Seal; a lost answer reconciles the SAME upload, never a re-key.

        Mirror of the synchronous put: a confirmed Seal is the confirmation of
        landed bytes (no blind re-send); only an unconfirmed one earns the
        SAME upload exactly one more presentation.
        """
        if not isinstance(data, bytes) or not _size(len(data)):
            raise ValueError("artifact data must be 1 byte..64 MiB")
        digest = hashlib.sha256(data).hexdigest()
        options = {"journal_stage": True} if journal_stage else {}
        up = await self.begin_upload(key, name, mime, digest, len(data), labels=labels, **options)
        try:
            await self.upload(up, data)
        except APIError as error:
            if not error.outcome_unknown:
                raise
            ref, seal_error = None, None
            try:
                ref = await self.seal(up["id"])
            except APIError as failure:
                seal_error = failure
            if ref is not None:
                if ref["sha256"] != digest or ref["size"] != len(data):
                    raise _invalid("sealed artifact differs from upload", unknown=True)
                return ref
            if seal_error.code == "NOT_FOUND":
                raise
            await self.upload(up, data)
            ref = await self.seal(up["id"])
            if ref["sha256"] != digest or ref["size"] != len(data):
                raise _invalid("sealed artifact differs from upload", unknown=True)
            return ref
        ref = await self.seal(up["id"])
        if ref["sha256"] != digest or ref["size"] != len(data):
            raise _invalid("sealed artifact differs from upload", unknown=True)
        return ref

    async def download(self, artifact_id: str) -> bytes:
        """Read complete bytes through owner API and verify exact size and SHA-256."""
        ref = await self.get(artifact_id)
        status, values, data = await self._exchange("Download", {"id": artifact_id}, limit=ref["size"])
        return _bytes(ref, status, values, data)

    async def read_range(self, artifact_id: str, offset: int, length: int) -> bytes:
        """Read a bounded exact-version range; length is clamped only at artifact EOF."""
        ref = await self.get(artifact_id)
        length = _range(ref, offset, length)
        status, values, data = await self._exchange("Download", {"id": artifact_id}, limit=length,
                                                range_header=f"bytes={offset}-{offset + length - 1}")
        return _bytes(ref, status, values, data, offset, length)

    async def readiness(self, artifact_id: str) -> dict:
        """Read content/extraction/search readiness; sealed never implies extracted."""
        return await self._call("Readiness", {"id": required(artifact_id, "artifact ID")})

    async def process(self, artifact_id: str) -> dict:
        """Request owner sandbox extraction; requires current derive authority."""
        return await self._call("Process", {"id": required(artifact_id, "artifact ID")}, mutation=True)

    async def read_text_chunk(self, artifact_id: str, version: str, offset: int, limit: int, context_budget: int) -> dict:
        """Read owner-extracted UTF-8 with explicit version and context budget."""
        return await self._call("ReadTextChunk", dict(id=required(artifact_id, "artifact ID"),
                            version=required(version, "version"), offset=offset, limit=limit, context_budget=context_budget))

    async def search_text(self, artifact_id: str, version: str, query: str) -> dict:
        """Search one exact version's owner-maintained index under current authority."""
        return await self._call("SearchText", dict(id=required(artifact_id, "artifact ID"),
                            version=required(version, "version"), query=query))

    async def resolve(self, sha256: str) -> dict:
        """Resolve a digest inside current scope; grants no new read authority."""
        return await self._call("Resolve", {"sha256": sha256})

    async def transfer_grant(self, ref: dict, target_deployment: str, target_context: str, target_task: str) -> dict:
        """Delegate one exact source to a current target task of the same subject."""
        _ref(ref)
        grant = await self._call("TransferGrant", dict(id=ref["id"], version=ref["version"],
                            target_deployment=required(target_deployment, "target deployment"),
                            target_context=required(target_context, "target context"),
                            target_task=required(target_task, "target task")), mutation=True)
        if grant.get("artifact_id") != ref["id"] or grant.get("version") != ref["version"] or grant.get("sha256") != ref["sha256"]:
            raise _invalid("transfer grant differs from source reference", unknown=True)
        return grant

    async def copy_from_transfer_grant(self, grant: str, key: str) -> dict:
        """Owner-copy with preserved provenance/taint. UNKNOWN must keep the SAME key."""
        ref = await self._call("CopyFromTransferGrant", {"grant": required(grant, "grant"), "key": required(key)}, mutation=True)
        try:
            return _ref(ref)
        except APIError as error:
            error.outcome_unknown = True
            raise
