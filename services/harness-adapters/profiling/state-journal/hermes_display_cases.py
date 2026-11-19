"""Pinned Hermes transcript fixture shared by the AHP and SQLite probes."""

from dataclasses import replace
import json
from types import SimpleNamespace

from agent.conversation_compression_archive import commit_compaction
from persistence import ProfileScope, RowVisibility
from persistence.conversation.messages import ConversationRequest, NewMessage, TranscriptRead, TranscriptView
from persistence.conversation.sessions import NewSession
from persistence.sqlite import SQLiteBackend


def seed_display(repo, session, *, holder=None):
    def append(messages):
        repo.append_batch(session, messages, turn_lease_holder=holder)

    # More than one AHP page and deliberately regressing timestamps.
    for start in (0, 51):
        append([NewMessage("user" if i % 2 == 0 else "assistant", f"original {i}",
                           timestamp=800. - i, message_uid=f"old-{i}")
                for i in range(start, start + 51)])
    append([
        NewMessage("user", "lookup item", timestamp=400., message_uid="lookup-user",
                   api_content="lookup item in the indexed catalogue"),
        NewMessage("assistant", None, timestamp=401., message_uid="lookup-call",
                   tool_calls=({"id": "call-lookup", "type": "function",
                                "function": {"name": "lookup", "arguments": '{"item":4}'}},),
                   tool_call_uids={"call-lookup": "tool-identity"}),
        NewMessage("tool", "four results", timestamp=402., message_uid="lookup-result", tool_name="lookup",
                   tool_call_id="call-lookup", tool_call_uid="tool-identity", display_metadata={"fixture": True}),
        NewMessage("assistant", "lookup finished", timestamp=403., message_uid="lookup-reply"),
    ])
    for generation in (1, 2):
        if generation == 2:
            append([NewMessage("user", "lookup item", timestamp=399., message_uid="independent")])
        tail_count = 3 + generation
        current = repo.read_conversation(session, ConversationRequest(include_row_ids=True))
        tail = [row.as_openai_message() for row in current[-tail_count:]]
        summary = {"role": "user", "content": f"Context summary {generation}", "timestamp": 900. + generation,
                   "message_uid": f"summary-{generation}", "_compressed_summary": True}
        commit_compaction(None, SimpleNamespace(messages=repo), session, [summary, *tail],
                          tail_count=tail_count, lock_holder=holder)
    append([NewMessage("assistant", "model scratchpad", timestamp=904., message_uid="private-model",
                       display_metadata={"model_only": True})])


def transcript_fields(rows):
    result = []
    for row in rows:
        wire = row.as_wire_row()
        wire.pop("id")
        wire.pop("session_id")
        for key in ("absorbed_message_uids", "tool_call_uids", "reasoning_details",
                    "codex_reasoning_items", "codex_message_items"):
            wire[key] = json.loads(wire[key]) if wire[key] else None
        result.append(wire)
    return result


def conversation_fields(rows):
    messages = [row.as_openai_message() for row in rows]
    for message in messages:
        message.pop("_row_id", None)
    return messages


def check_display(repo, session, directory):
    profile = directory / "reference" / "profiles" / "display"
    profile.mkdir(parents=True)
    scope = ProfileScope(profile_dir=profile, name="display")
    backend = SQLiteBackend({scope.hermes_home_key: profile / "state.db"})
    try:
        store = backend.open(scope)
        reference = store.messages
        store.sessions.create(NewSession(session, "cli"))
        seed_display(reference, session)
        display = TranscriptRead(view=TranscriptView.DISPLAY)
        requests = (display, replace(display, limit=10, offset=98), replace(display, limit=6, offset=1, latest=True),
                    replace(display, limit=0), replace(display, offset=200), TranscriptRead(),
                    TranscriptRead(view=TranscriptView.ALL))
        for query in requests:
            actual = transcript_fields(repo.read_transcript(session, query))
            expected = transcript_fields(reference.read_transcript(session, query))
            assert len(actual) == len(expected), (query, len(actual), len(expected))
            for entry, wanted in zip(actual, expected):
                assert entry == wanted, (query, entry, wanted)
        raw_display = repo.read_transcript(session, display)
        active = repo.read_transcript(session)
        all_rows = repo.read_transcript(session, TranscriptRead(view=TranscriptView.ALL))
        matching_reference = reference.read_transcript(session)
        keyset = TranscriptRead(after_id=active[2].id, limit=2)
        assert transcript_fields(repo.read_transcript(session, keyset)) == transcript_fields(
            reference.read_transcript(session, replace(keyset, after_id=matching_reference[2].id)))
        assert repo.display_count(session) == reference.display_count(session) == 109
        assert len(raw_display) == 109 and len(active) == 7 and len(all_rows) == 119
        assert sum(row.content == "lookup item" for row in raw_display) == 2
        assert sum(row.role == "tool" for row in raw_display) == 1
        assert any(row.visibility is RowVisibility.REWOUND for row in all_rows)
        assert all(row.visibility is not RowVisibility.REWOUND for row in raw_display)
        assert raw_display[0].content == "original 0"
        assert "model scratchpad" in [row.content for row in active]
        assert "model scratchpad" not in [row.content for row in raw_display]
        for include_row_ids in (False, True):
            query = ConversationRequest(view=TranscriptView.DISPLAY, include_row_ids=include_row_ids)
            actual = repo.read_conversation(session, query)
            expected = reference.read_conversation(session, query)
            assert conversation_fields(actual) == conversation_fields(expected)
            if include_row_ids:
                assert [row.row_id for row in actual] == [row.id for row in raw_display]
        return raw_display, {"display_rows": len(raw_display), "active_rows": len(active), "all_rows": len(all_rows),
                             "sqlite_queries_matched": len(requests) + 3}
    finally:
        backend.close()
