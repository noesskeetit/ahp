"""Errors raised by the AHP State SDK.

The State API reports failures as a strict JSON document ``{"code": ..., "message": ...}``
where ``code`` is one of the wire error codes below. The SDK turns every
non-success response into a :class:`StateError` carrying that code, so callers
can branch on behavior (for example a missing checkpoint) without parsing text.
"""

NOT_FOUND = "NOT_FOUND"
CONFLICT = "CONFLICT"
STALE_EPOCH = "STALE_EPOCH"
FORBIDDEN = "FORBIDDEN"
INVALID = "INVALID_ARGUMENT"
KEY_REUSE = "KEY_REUSE_MISMATCH"
LEASE_LOST = "LEASE_LOST"   # tolerated alias only: a live State never answers
                            # with it - every dead lease answers STALE_EPOCH


class StateError(Exception):
    """A State API error mapped to a stable wire ``code``.

    ``code`` is one of the module-level constants. ``status`` records the HTTP
    status for diagnostics only; callers should branch on ``code``.
    """

    def __init__(self, code, message="", status=0):
        super().__init__("%s: %s" % (code, message) if message else code)
        self.code = code
        self.message = message or ""
        self.status = status

    def is_not_found(self):
        return self.code == NOT_FOUND

    def is_conflict(self):
        return self.code == CONFLICT

    def is_stale_epoch(self):
        return self.code in (STALE_EPOCH, LEASE_LOST)

    def is_key_reuse(self):
        return self.code == KEY_REUSE


class NotFoundError(StateError):
    def __init__(self, message="", status=404):
        super().__init__(NOT_FOUND, message, status)


class ConflictError(StateError):
    def __init__(self, message="", status=409):
        super().__init__(CONFLICT, message, status)
