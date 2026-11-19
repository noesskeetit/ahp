"""Administrative export and provenance-preserving checkpoint restoration.

Transfer uses two independently authorised task clients, plus an explicit human
admin client for export. No byte download/upload may substitute for owner Copy:
it would drop source-reader labels and fail to taint the recipient's journal.
"""
from __future__ import annotations

import copy
import hashlib
import json
from urllib.parse import urlsplit

from ahp_state.client import StateAPIClient, StateValidationError, _raise_typed
from ahp_state.payload import ENCODING, _IDENTITY, _decode, _encode, _json


def _operation_key(key: str, ref: dict) -> str:
    # This wire identity is shared with Go contracts.Digest([]string{...}).
    # encoding/json additionally escapes HTML and JS line separators; using
    # ordinary ensure_ascii=False JSON would redispatch after a language switch.
    canonical = _json(["rehome-v1", key, ref["id"], ref["version"], ref["sha256"]]).decode("utf-8")
    for char, escaped in (("<", r"\u003c"), (">", r"\u003e"), ("&", r"\u0026"),
                          ("\u2028", r"\u2028"), ("\u2029", r"\u2029")):
        canonical = canonical.replace(char, escaped)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class TransferClient:
    """Framework-neutral Artifacts transfer API over a task's mTLS client."""

    def __init__(self, client: StateAPIClient):
        self.client = client

    async def transfer_grant(self, ref: dict, *, deployment: str,
                             context_id: str, task_id: str) -> dict:
        return await self.client._payloads.request("TransferGrant", {
            "id": ref["id"], "version": ref["version"],
            "target_deployment": deployment, "target_context": context_id,
            "target_task": task_id,
        })

    async def copy_from_transfer_grant(self, grant: str, key: str) -> dict:
        """Same key reconciles; transfer_outcome_unknown is never auto-retried."""
        return await self.client._payloads.request("CopyFromTransferGrant", {
            "grant": grant, "key": key,
        })


class StateAdminClient:
    """Distinct human/mTLS control-plane authority; task credentials are refused."""

    def __init__(self, client: StateAPIClient):
        self.client = client

    async def export_thread(self, deployment: str, subject: str, thread_id: str) -> dict:
        origin = urlsplit(self.client.base_url)
        if origin.scheme != "https" or not origin.netloc or origin.path or origin.query or origin.fragment or origin.username:
            raise StateValidationError("INVALID_ARGUMENT", "State admin requires a bare HTTPS origin")
        async with self.client.http_client.stream("POST",
            f"{self.client.base_url}/v1/state-admin/ExportThread",
            json={"target": {"deployment": deployment, "subject": subject},
                  "input": {"thread_id": thread_id}},
            headers={"Authorization": f"Bearer {self.client._token or ''}"},
            follow_redirects=False,
        ) as response:
            data = bytearray()
            async for chunk in response.aiter_bytes():
                if len(data) + len(chunk) > 16 << 20:
                    raise StateValidationError("INVALID_ARGUMENT", "admin export response exceeds 16MiB")
                data.extend(chunk)
            try:
                document = json.loads(data)
            except ValueError:
                raise StateValidationError("INVALID_ARGUMENT", "invalid admin export response") from None
            if response.status_code != 200:
                raise _raise_typed(response.status_code, document)
            return document


async def rehome_export(source: StateAPIClient, target: StateAPIClient, export: dict,
                        *, deployment: str, context_id: str, task_id: str, key: str) -> dict:
    """Prepare a snapshot for target.import_thread; same key reconciles retries.

    Each explicit descriptor transfers at most 64MiB (owner may lower it).
    Legacy unmarked payloads and original serializer tags stay opaque.
    """
    if not all((deployment, context_id, task_id, key)):
        raise StateValidationError("INVALID_ARGUMENT", "full target and stable recovery key required")
    result = copy.deepcopy(export)

    async def rewrite(value, encoding, *, metadata=False):
        if not encoding:
            return value
        if encoding != ENCODING:
            raise StateValidationError("INVALID_ARGUMENT", "unsupported checkpoint encoding")
        ref = value if metadata else json.loads(_decode(value))
        ticket = await TransferClient(source).transfer_grant(ref,
            deployment=deployment, context_id=context_id, task_id=task_id)
        if (ticket.get("artifact_id") != ref.get("id") or ticket.get("version") != ref.get("version")
                or ticket.get("sha256") != ref.get("sha256")):
            raise StateValidationError("INVALID_ARGUMENT", "source transfer grant does not match checkpoint digest")
        operation = _operation_key(key, ref)
        copied = await TransferClient(target).copy_from_transfer_grant(ticket["grant"], operation)
        if (any(copied.get(k) != ref.get(k) for k in ("tenant", "subject", "sha256", "size"))
                or copied.get("deployment") != deployment
                or copied.get("context_id") != context_id or copied.get("task_id") != task_id):
            raise StateValidationError("INVALID_ARGUMENT", "owner transfer binding mismatch")
        descriptor = {k: copied.get(k) for k in _IDENTITY}
        return descriptor if metadata else _encode(_json(descriptor))

    for checkpoint in result.get("checkpoints") or []:
        checkpoint["metadata"] = await rewrite(checkpoint.get("metadata"),
                                                checkpoint.get("metadata_encoding"), metadata=True)
        for part in checkpoint.get("parts") or []:
            part["data"] = await rewrite(part.get("data"), part.get("encoding"))
    for group in result.get("writes") or []:
        for write in group.get("writes") or []:
            write["data"] = await rewrite(write.get("data"), write.get("encoding"))
    return result
