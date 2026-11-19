"""Host-bound Hermes PersistenceStore for the compact AHP State API.

Install the native Hermes runtime in the host when using these repositories.
The SDK itself can be imported before the Hermes runtime is installed.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import MISSING, asdict, fields
from datetime import datetime
from heapq import nsmallest
from itertools import islice
import json
from time import time

from ahp_state.client import StateAPIError

__all__ = ["AHPHermesPersistenceStore"]

_SESSION_LIST = "/v1/state/sessions"


class AHPHermesPersistenceStore:
    """Expose synchronous repositories and `await store.aio.<repository>.<verb>`.

    The host supplies a synchronous authenticated State API request callable
    and owns the lease lifecycle. An epoch is required for writes to the bound
    session. Other sessions in the authenticated scope may be read.
    """

    def __init__(self, request, session_id, *, lease_epoch=None, holder=None, scope=None,
                 effect_tools=(), artifacts=None, tool_lease_proof=None, gateway_mode=False):
        from persistence import AwaitableStore, PersistenceCapabilities

        from ._hermes_messages import JournalMessages

        self.scope = scope
        self.read_only = lease_epoch is None
        self._closed = False
        self._transport = request
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ahp-hermes")
        self.messages = JournalMessages(
            self._request, session_id, lease_epoch=lease_epoch, holder=holder, effect_tools=effect_tools,
            artifacts=artifacts, tool_lease_proof=tool_lease_proof, gateway_mode=gateway_mode,
        )
        self.sessions = _Sessions(self)
        self.lineage = _Lineage(self)
        self.search = _Search(self)
        self.aio = AwaitableStore(self)
        self.capabilities = PersistenceCapabilities(
            full_text_search=True, substring_search=False, transactional_claims=False,
            cross_process=True, remote=True, reopen_durability=True, authority_fences=False,
            durable_admissions=False, receipted_writes=True, physical_admin=False,
        )

    def _request(self, method, route, **kwargs):
        if self._closed:
            raise RuntimeError("Hermes persistence store is closed")
        return self._transport(method, route, **kwargs)

    def _messages(self, session_id):
        from ._hermes_messages import JournalMessages

        if self.messages.session_id == session_id:
            return self.messages
        return JournalMessages(self._request, session_id, artifacts=self.messages.artifacts)

    def close(self):
        """Evict the session projection and close the async executor."""
        self._closed = True
        self.executor.shutdown(cancel_futures=True)
        self.messages.discard_projection()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


def _time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() if value else None


class _Sessions:
    def __init__(self, store):
        self._store = store

    def _writer(self, session_id):
        messages = self._store.messages
        messages._bound(session_id)
        if self._store.read_only:
            raise ValueError("Hermes session writes require the AHP host's write lease")
        return messages

    def catalog(self, *, search_text="", limit=100, cursor=""):
        """Read a cursor-paged session catalog from the State API."""
        return self._store._request("GET", _SESSION_LIST, params={
            "view": "catalog", "harness": "hermes", "search_query": search_text,
            "limit": limit, "cursor": cursor,
        })

    def _cards(self, search_text=""):
        cursor = ""
        while True:
            page = self.catalog(search_text=search_text, cursor=cursor)
            yield from page["sessions"]
            next_cursor = page.get("next_cursor")
            if not next_cursor:
                return
            if next_cursor == cursor:
                raise ValueError("Hermes catalog cursor did not advance")
            cursor = next_cursor

    def create(self, spec):
        from ._hermes_messages import session_path

        writer = self._writer(spec.session_id)
        session = writer.request("GET", session_path(spec.session_id), params={"view": "resume"})
        version, existing = writer._read_session_value("metadata")
        config_version, config = writer._read_session_value("model_config")

        initial = asdict(spec)
        initial.pop("session_id")
        initial.pop("model_config")
        kind = initial.pop("lineage_kind")
        changes = {key: value for key, value in initial.items() if value is not None}
        if existing.get("source") == "unknown":
            existing.pop("source")
        metadata = {**changes, **existing}
        metadata["parent_session_id"] = session.get("parent_session_id") or metadata.get("parent_session_id")
        metadata.setdefault("started_at", time())
        updated_config = config.copy()
        if kind.value != "none":
            marker = {"delegate": "_delegate_from", "branch": "_branched_from", "reset": "_reset_from"}[kind.value]
            updated_config.setdefault(marker, metadata["parent_session_id"])
        if config_version == 0 or set(config) <= {"_reset_from"}:
            updated_config = {**(spec.model_config or {}), **updated_config}
        mutations = []
        catalog = None
        if metadata != existing:
            mutations.append(writer._session_value_change("metadata", version, metadata))
            catalog = self._catalog_update(metadata)
        if updated_config != config:
            mutations.append(writer._session_value_change("model_config", config_version, updated_config))
        if mutations:
            writer._session_mutation(changes=mutations, catalog=catalog)
        return spec.session_id

    @staticmethod
    def _catalog_update(metadata):
        summary = {}
        for key in ("source", "model", "created_source", "cwd", "git_repo_root", "hidden", "archived", "ended_at"):
            if key in metadata:
                candidate = {**summary, key: metadata[key]}
                # Leave room for PostgreSQL JSONB whitespace in the 4 KiB server limit.
                if len(_json_text(candidate)) <= 3968:
                    summary = candidate
        return {
            **{key: metadata[key] for key in ("title", "parent_session_id") if metadata.get(key) is not None},
            "summary_metadata": summary,
        }

    def get(self, session_id):
        from ._hermes_messages import session_path

        reader = self._store._messages(session_id)
        try:
            state = reader.request("GET", session_path(session_id), params={"view": "resume"})
            if state["harness_type"] != "hermes":
                return None
            _, metadata = reader._read_session_value("metadata")
            _, config = reader._read_session_value("model_config")
        except StateAPIError as error:
            if error.code != "NOT_FOUND":
                raise
            return None
        return self._record(state, metadata, config)

    @staticmethod
    def _record(card, metadata=None, config=None):
        from persistence.conversation.sessions import SessionLineageKind, SessionRecord, TitleSource

        config = config or {}
        metadata = metadata if metadata is not None else card.get("summary_metadata") or {}
        model_config_json = None if not config else _json_text(config)
        title_source = metadata.get("title_source")
        lineage_kind, lineage_origin = SessionLineageKind.NONE, None
        for marker, kind in (
            ("_delegate_from", SessionLineageKind.DELEGATE),
            ("_branched_from", SessionLineageKind.BRANCH),
            ("_reset_from", SessionLineageKind.RESET),
        ):
            if config.get(marker):
                lineage_kind, lineage_origin = kind, config[marker]
                break
        values = {
            **metadata, "session_id": card["session_id"], "source": metadata.get("source", "unknown"),
            "session_key": metadata.get("session_key") or card.get("native_key"),
            "parent_session_id": card.get("parent_session_id") or metadata.get("parent_session_id"),
            "title": metadata.get("title", card.get("title")),
            "title_source": TitleSource(title_source) if title_source else None,
            "started_at": _time(card.get("created_at")) or metadata.get("started_at", 0.0),
            "last_activity_at": _time(card.get("updated_at")),
            "message_count": card.get("head_seq", 0),
            "model_config": config, "model_config_json": model_config_json,
            "lineage_kind": lineage_kind, "lineage_origin_id": lineage_origin,
            "reset_from": config.get("_reset_from"), "branched_from": config.get("_branched_from"),
            "origin_json": _json_text(metadata["origin"]) if metadata.get("origin") else None,
            "transcript_revision": card.get("head_seq", 0),
        }
        defaults = {"bool": False, "int": 0}
        return SessionRecord(**{
            field.name: values.get(field.name, field.default if field.default is not MISSING
                                   else defaults.get(field.type))
            for field in fields(SessionRecord)
        })

    def _update(self, session_id, update):
        writer = self._writer(session_id)
        version, metadata = writer._read_session_value("metadata")
        previous = metadata.copy()
        update(metadata)
        if previous == metadata:
            return False
        writer._session_mutation(
            changes=[writer._session_value_change("metadata", version, metadata)],
            catalog=self._catalog_update(metadata),
        )
        return True

    def set_title(self, session_id, title):
        return self._update(session_id, lambda metadata: metadata.update(
            title=(title or "").strip(), title_source="user" if title else None,
        ))

    def get_title(self, session_id):
        record = self.get(session_id)
        return record.title if record else None

    def patch_model_config(self, session_id, patch):
        if patch:
            writer = self._writer(session_id)
            writer._session_mutation(changes=[writer._model_config_change(patch)])

    def get_model_config_value(self, session_id, key, default=None):
        try:
            _, config = self._store._messages(session_id)._read_session_value("model_config")
        except StateAPIError as error:
            if error.code != "NOT_FOUND":
                raise
            return default
        return config.get(key, default)

    def end(self, session_id, reason):
        def write_end(metadata):
            if metadata.get("ended_at") is None:
                metadata.update(ended_at=time(), end_reason=reason)

        self._update(session_id, write_end)

    def reopen(self, session_id):
        def clear_end(metadata):
            metadata.pop("ended_at", None)
            metadata.pop("end_reason", None)

        self._update(session_id, clear_end)

    def _filtered_records(self, *, search_text="", sources=None, exclude_sources=()):
        for card in self._cards(search_text):
            record = self._record(card)
            if (sources is None or record.source in sources) and record.source not in exclude_sources:
                yield record

    def list_sessions(self, query, window=None, *, compact_rows=False):
        from persistence.conversation.sessions import SessionListEntry, SessionWindow

        window = window if window is not None else SessionWindow(limit=20)
        rows = self._filtered_records(search_text=query.search_text or query.id_query or "",
                                      sources=query.sources, exclude_sources=query.exclude_sources)
        rows = (row for row in rows
                if row.message_count >= query.min_message_count
                and (query.include_children or not row.parent_session_id)
                and (query.include_hidden or not row.hidden)
                and (query.include_subagents or row.source != "subagent")
                and (query.include_archived or query.archived_only or not row.archived)
                and (not query.archived_only or row.archived)
                and (query.cwd_prefix is None or (row.cwd or "").startswith(query.cwd_prefix))
                and (query.workspace_key is None or _in_workspace(row, query.workspace_key))
                and (query.session_key is None or row.session_key == query.session_key))
        if query.order.value == "started_at":
            rows = sorted(rows, key=lambda row: (row.started_at, row.session_id), reverse=True)
        return tuple(SessionListEntry(row, "", False, row.last_activity_at, compact=compact_rows)
                     for row in _window(rows, window))

    def search_by_id(self, text, *, limit=20, include_archived=True, sources=None, exclude_sources=()):
        from persistence.conversation.sessions import SessionListEntry

        if not text.strip() or limit <= 0:
            return ()
        needle = text.strip().casefold()
        rows = self._filtered_records(search_text=text.strip(), sources=sources, exclude_sources=exclude_sources)
        rows = (row for row in rows if needle in row.session_id.casefold() and (include_archived or not row.archived))

        def rank(row):
            value = row.session_id.casefold()
            return (value != needle, not value.startswith(needle), -(row.last_activity_at or 0))

        return tuple(SessionListEntry(row, "", False, row.last_activity_at)
                     for row in nsmallest(limit, rows, key=rank))

    def search_sessions(self, *, sources=None, workspace_key=None, local_created_sources=None,
                        skip_launch_stubs=False, window=None, query=""):
        from persistence.conversation.sessions import SessionRecencyEntry, SessionWindow

        window = window if window is not None else SessionWindow(limit=20)
        rows = self._filtered_records(search_text=query, sources=sources)
        rows = (row for row in rows
                if (workspace_key is None or _in_workspace(row, workspace_key))
                and (not skip_launch_stubs or row.message_count > 0)
                and (local_created_sources is None or row.source != "local"
                     or row.created_source in local_created_sources))
        return tuple(SessionRecencyEntry(row, row.last_activity_at) for row in _window(rows, window))


def _in_workspace(row, workspace):
    workspace = workspace.rstrip("/") or "/"
    return (row.git_repo_root == workspace if row.git_repo_root else
            row.cwd == workspace or (row.cwd or "").startswith(workspace.rstrip("/") + "/"))


def _window(rows, window):
    end = None if window.limit is None else window.offset + window.limit
    return islice(rows, window.offset, end)


def _json_text(value):
    from ._hermes_messages import encode

    return encode(value).decode("ascii")


class _Lineage:
    def __init__(self, store):
        self._store = store

    def lineage(self, session_id):
        from ._hermes_messages import session_lineage

        return session_lineage(self._store._request, session_id)

    def conversation_root(self, session_id):
        ids = self.lineage(session_id)
        return ids[0] if ids else session_id

    def resume_lineage_ids(self, session_id):
        return self.lineage(session_id) or (session_id,)


class _Search:
    def __init__(self, store):
        self._store = store

    def search_messages(self, query, *, source_filter=None, exclude_sources=None, role_filter=None, limit=20,
                        offset=0, sort=None, include_inactive=False, fields=None, after_ts=None, before_ts=None):
        """Run State API full-text search across scoped Hermes sessions."""
        from persistence.conversation.search import SEARCH_RESULT_FIELDS, SearchHit

        if isinstance(fields, str):
            raise TypeError("fields must be a collection of field names")
        selected = set(SEARCH_RESULT_FIELDS if fields is None else fields)
        if unknown := selected.difference(SEARCH_RESULT_FIELDS):
            raise ValueError(f"Unknown Hermes search fields: {sorted(unknown)}")
        text = query.strip()
        if not text or "\0" in text or limit == 0:
            return ()
        text = text.encode()[:512].decode(errors="ignore")
        offset = max(offset, 0)
        sort_order = getattr(sort, "value", "relevance")
        matches = self._matches(text, source_filter, exclude_sources or (), role_filter, include_inactive,
                                after_ts, before_ts)

        def rank(match):
            card, row, data = match
            timestamp = data.get("timestamp") or 0
            return (timestamp if sort_order == "oldest" else -timestamp, card["session_id"], row["seq"])

        ranked = sorted(matches, key=rank) if limit < 0 else nsmallest(offset + limit, matches, key=rank)
        results = []
        for card, row, data in ranked[offset:]:
            session_id = card["session_id"]
            summary = card.get("summary_metadata") or {}
            content = row.get("content_text") or data.get("content") or ""
            if not isinstance(content, str):
                content = _json_text(content)
            values = {
                "id": row["seq"], "session_id": session_id, "role": row["role"],
                "snippet": _snippet(content, text), "timestamp": data.get("timestamp"),
                "tool_name": data.get("tool_name"), "source": summary.get("source", "unknown"),
                "model": summary.get("model"), "session_started": _time(card.get("created_at")),
            }
            if "context" in selected:
                values["context"] = self._context(session_id, row["seq"])
            hit = {key: value for key, value in values.items() if key in selected}
            if "id" in hit:
                hit["message_id"] = hit.pop("id")
            results.append(SearchHit(**hit))
        return tuple(results)

    def _matches(self, text, sources, exclude_sources, roles, inactive, after_ts, before_ts):
        from ._hermes_messages import EVENT_TYPE

        for card in self._store.sessions._cards():
            summary = card.get("summary_metadata") or {}
            source = summary.get("source", "unknown")
            started_at = _time(card.get("created_at"))
            if ((sources is not None and source not in sources) or source in exclude_sources
                    or (after_ts is not None and (started_at is None or started_at < after_ts))
                    or (before_ts is not None and (started_at is None or started_at >= before_ts))):
                continue
            messages = self._store._messages(card["session_id"])
            offset = 0
            while True:
                page = messages.request("GET", messages.route, params={"q": text, "after_seq": offset, "limit": 100})
                for row in page["events"]:
                    if row["type"] != EVENT_TYPE or (roles and row["role"] not in roles):
                        continue
                    if not (inactive or row.get("is_active", True) or row.get("is_compacted", False)):
                        continue
                    data = json.loads(messages._load_payload(row["payload"]))
                    if data.get("display_kind") != "hidden":
                        yield card, row, data
                if len(page["events"]) < 100:
                    break
                next_offset = page["events"][-1]["seq"]
                if next_offset <= offset:
                    raise ValueError("Hermes message search cursor did not advance")
                offset = next_offset

    def _context(self, session_id, seq):
        from persistence.conversation.search import SearchContext

        from ._hermes_messages import EVENT_TYPE

        messages = self._store._messages(session_id)
        rows = messages.request("GET", messages.route, params={"after_seq": max(0, seq - 2), "limit": 3})["events"]
        return tuple(SearchContext(row["role"], (row.get("content_text") or "")[:200])
                     for row in rows if row["type"] == EVENT_TYPE)


def _snippet(content, text):
    word = text.split()[0]
    start = content.lower().find(word.lower())
    if start < 0:
        return content[:120]
    left = max(0, start - 40)
    end = start + len(word)
    right = min(len(content), left + 160)
    return ("..." if left else "") + content[left:start] + ">>>" + content[start:end] + "<<<" + content[end:right] + (
        "..." if right < len(content) else ""
    )
