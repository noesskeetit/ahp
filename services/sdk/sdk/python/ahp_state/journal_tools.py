"""Conservative external tool admission for the compact State API."""

from __future__ import annotations

import json

from ahp_state.client import StateAPIError, StateConflictError, StateServerError


class SessionFencedError(StateConflictError):
    """A newer worker owns the session. Stop the current execution."""


def admission_may_have_committed(error):
    return isinstance(error, (json.JSONDecodeError, StateServerError)) or (
        isinstance(error, StateAPIError) and error.code in {"TIMEOUT", "TRANSPORT", "UNKNOWN"}
    )


class StandaloneToolGuard:
    """Allow one dispatch after a durable tool intent and fence lost leases."""

    def __init__(self, client, session_path, lease_epoch, *, turn_seq=None):
        self.client = client
        self.session_path = session_path
        self.lease_epoch = lease_epoch
        self.turn_seq = turn_seq
        self.fenced = False

    async def begin(self, call_id, name, args_digest):
        if self.fenced:
            raise SessionFencedError("STALE_EPOCH", "tool execution lease was lost")
        path = self.session_path + "/tools:begin-standalone"
        body = {"lease_epoch": self.lease_epoch, "call_id": call_id,
                "tool_name": name, "args_digest": list(args_digest)}
        if self.turn_seq is not None:
            body["turn_seq"] = self.turn_seq
        try:
            try:
                return await self.client.request_state("POST", path, body=body)
            except (StateAPIError, json.JSONDecodeError) as error:
                if not admission_may_have_committed(error):
                    raise
                return await self.client.request_state("POST", path, body={**body, "ack_lost": True})
        except StateAPIError as error:
            if error.code == "STALE_EPOCH":
                self.fenced = True
                raise SessionFencedError(error.code, error.message) from error
            raise
