"""Exercise the pinned Hermes native compaction commit on the live compact journal."""

import asyncio
from base64 import b64decode
from dataclasses import replace
import json
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace
from uuid import uuid4

from agent.conversation_compression_archive import commit_compaction, held_archive_coverage
from persistence.conversation.messages import ConversationRequest, NewMessage, TranscriptRead, TranscriptView
from persistence.core.errors import HeldRowRewritten
from session_rules.turns import repair_message_sequence

from adapters.hermes.journal_messages import HermesReconciliationRequired, JournalMessages
from ahp_state.client import StateAPIError
from hermes_compact_probe import acquire, connection
from hermes_display_cases import check_display, seed_display

HOLDER = "compaction-probe"
EXPECTED = ["Context summary", "kept user", "kept assistant", "new user during compression"]
REWRITTEN_TEXT = "[CONTEXT COMPACTION] summary of the earlier turns\n\nkeep this whole line"
MERGED_SUMMARY = "[CONTEXT COMPACTION] shortened context"


def compact(request, session, epoch):
    repo = JournalMessages(request, session, lease_epoch=epoch, holder=HOLDER)
    source = [NewMessage(role, text, timestamp=ts, message_uid=uid) for role, text, ts, uid in (
        ("user", "old user", 1001., "old-user"),
        ("assistant", "old assistant", 1002., "old-assistant"),
        ("user", "kept user", 1003., "kept-user"),
        ("assistant", "kept assistant", 1004., "kept-assistant"),
    )]
    repo.append_batch(session, source, turn_lease_holder=HOLDER)
    rows = repo.read_conversation(session, ConversationRequest(include_row_ids=True))
    watermark = rows[-1].row_id
    repo.append_batch(session, [NewMessage("user", EXPECTED[-1], timestamp=1000.,
                                            message_uid="concurrent-user")], turn_lease_holder=HOLDER)
    before = request("GET", repo.route)["events"]
    attempts = []

    def lose_ack(method, path, **kwargs):
        result = request(method, path, **kwargs)
        if path.endswith("/events:append"):
            attempts.append(kwargs["body"]["key"])
            if len(attempts) == 1:
                raise StateAPIError("TRANSPORT", "Fixture dropped committed compaction response")
        return result

    repo = JournalMessages(lose_ack, session, lease_epoch=epoch, holder=HOLDER)
    replacement = [{"role":"user", "content":EXPECTED[0], "timestamp":2000.,
                    "message_uid":"summary", "_compressed_summary":True},
                   *(recent.as_openai_message() for recent in rows[-2:])]
    commit_compaction(None, SimpleNamespace(messages=repo), session, replacement,
                      watermark=watermark, lock_holder=HOLDER, tail_count=2,
                      covered_ids=[record.row_id for record in rows], held_for_cas=[
                          row.as_openai_message() for row in rows])
    assert len(attempts) == 2 and attempts[0] == attempts[1]
    after = request("GET", repo.route)["events"]
    assert after[:len(before)] == before
    assert sum(event["type"] == "hermes.compaction" for event in after) == 1
    assert [row.content for row in repo.read_conversation(session)] == EXPECTED
    assert [msg["_row_id"] for msg in replacement] == [7, 8, 9]
    print(json.dumps({"epoch": epoch, "published_events": len(after), "append_attempts": len(attempts),
                      "original_entries_preserved": len(before), "watermark": watermark}), flush=True)


def publish_rewritten_carry(request, session):
    repo = JournalMessages(request, session, lease_epoch=acquire(request, session), holder=HOLDER)
    repo.append(session, NewMessage("assistant", "previous reply"), turn_lease_holder=HOLDER)
    stamp = repo.append_batch(session, [NewMessage("user", "keep this whole line", message_uid="retained")],
                              turn_lease_holder=HOLDER).outcomes[0]
    repo.append(session, NewMessage("user", "keep this whole line", message_uid="independent"),
                turn_lease_holder=HOLDER)
    original = repo.read_conversation(session, ConversationRequest(include_row_ids=True))[1].as_openai_message()
    merged = {**original, "content": REWRITTEN_TEXT, "_compressed_summary": True}
    before = request("GET", repo.route)["events"]
    for changed in (
        {**merged, "_db_row_snapshot": "stale"},
        {**merged, "_db_row_snapshot": None},
        {**merged, "content": "prefix keep this whole line suffix"},
        {**merged, "role": "assistant"},
        {**merged, "tool_calls": [{"id": "unexpected"}]},
    ):
        try:
            commit_compaction(None, SimpleNamespace(messages=repo), session, [changed],
                              watermark=1, lock_holder=HOLDER, tail_count=1, held_for_cas=[original])
        except HeldRowRewritten:
            pass
        else:
            raise AssertionError("unproven rewrite was published")
        assert request("GET", repo.route)["events"] == before

    commit_compaction(None, SimpleNamespace(messages=repo), session, [merged],
                      watermark=1, lock_holder=HOLDER, tail_count=1, held_for_cas=[original])
    assert (merged["_row_id"], merged["message_uid"]) == (5, stamp.message_uid)
    after = request("GET", repo.route)["events"]
    assert after[:len(before)] == before
    assert json.loads(b64decode(after[3]["payload"]["inline"])) == {
        "base_seq": 3, "archived": [1, 2], "rewound": [3],
    }


def publish_merged_carry(request, session):
    repo = JournalMessages(request, session, lease_epoch=acquire(request, session), holder=HOLDER)
    repo.append_batch(session, [
        NewMessage("assistant", "previous reply"),
        NewMessage("user", "alpha", message_uid="alpha", api_content="alternate alpha"),
        NewMessage("user", "beta", message_uid="beta"),
    ], turn_lease_holder=HOLDER)
    original = [row.as_openai_message() for row in repo.read_conversation(
        session, ConversationRequest(include_row_ids=True))]
    held = [dict(row) for row in original]
    assert repair_message_sequence(None, held) == 1
    assert held[-1]["_absorbed_row_ids"] == [3]
    assert held[-1]["_absorbed_message_uids"] == ["beta"]
    assert held[-1]["content"] == "alpha\n\nbeta" and "api_content" not in held[-1]
    covered, unresolved = held_archive_coverage(held)
    assert covered == [1, 2, 3] and not unresolved
    repo.append(session, NewMessage("user", "foreign prompt"), turn_lease_holder=HOLDER)
    summary = {"role": "user", "content": MERGED_SUMMARY, "_compressed_summary": True}
    before = request("GET", repo.route)["events"]

    for change in ({"_absorbed_message_uids": ["invented"]}, {"_absorbed_row_ids": [4]},
                   {"content": "alpha\n\nreplacement"}):
        try:
            commit_compaction(None, SimpleNamespace(messages=repo), session, [dict(summary), {**held[-1], **change}],
                              watermark=3, lock_holder=HOLDER, tail_count=1, covered_ids=covered,
                              held_for_cas=original)
        except HeldRowRewritten:
            pass
        else:
            raise AssertionError("compaction accepted an invalid merge witness")
        assert request("GET", repo.route)["events"] == before

    commit_compaction(None, SimpleNamespace(messages=repo), session, [summary, held[-1]],
                      watermark=3, lock_holder=HOLDER, tail_count=1, covered_ids=covered,
                      held_for_cas=original)
    after = request("GET", repo.route)["events"]
    assert after[:len(before)] == before
    assert json.loads(b64decode(after[4]["payload"]["inline"])) == {
        "base_seq": 4, "archived": [1, 2], "rewound": [3, 4],
    }


def acquire_replacement(request, session, *, ttl_ms=5000):
    deadline = time.monotonic() + 8
    while True:
        try:
            return acquire(request, session, ttl_ms)
        except StateAPIError as error:
            if error.code != "CONFLICT" or time.monotonic() >= deadline:
                raise
            time.sleep(.1)


def probe_display_snapshot(request, writer, before):
    session = writer.session_id
    display = TranscriptRead(view=TranscriptView.DISPLAY)
    insert_id = None

    def read_during_append(method, path, **kwargs):
        nonlocal insert_id
        result = request(method, path, **kwargs)
        if method == "GET" and path == writer.route and insert_id is None:
            assert len(result["events"]) < result["head_seq"], "fixture must cross a page boundary"
            insert_id = writer.append(session, NewMessage("user", "visible on next read"),
                                      turn_lease_holder=HOLDER)
        return result

    reader = JournalMessages(read_during_append, session)
    assert reader.read_transcript(session, display) == before
    assert insert_id is not None
    after = reader.read_transcript(session, display)
    assert after[:-1] == before
    assert after[-1].id == insert_id and after[-1].content == "visible on next read"
    missing = session + "-missing"
    assert JournalMessages(request, missing).read_transcript(missing, display) == ()


def run(request, directory):
    session = "hermes-compaction-" + uuid4().hex
    marker = directory / "compacted.json"
    with (directory / "source.log").open("wb") as log:
        process = subprocess.Popen([sys.executable, __file__, "source", session, str(marker)],
                                   stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 20
            while not marker.exists():
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError((directory / "source.log").read_text())
                time.sleep(.05)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
    assert process.returncode == -signal.SIGKILL
    epoch = acquire_replacement(request, session)
    assert epoch == 2
    stale = JournalMessages(request, session, lease_epoch=1, holder=HOLDER)
    try:
        stale.compact(session, [NewMessage("user", "stale")], lock_holder=HOLDER)
    except StateAPIError as error:
        assert error.code == "STALE_EPOCH"
    else:
        raise AssertionError("old epoch published a compaction")
    live = JournalMessages(request, session, lease_epoch=epoch, holder=HOLDER)
    current = live.read_conversation(session, ConversationRequest(include_row_ids=True))
    assert [row.content for row in current] == EXPECTED
    assert [row.row_id for row in current] == [7, 8, 9, 10]
    assert [row.message_uid for row in current] == ["summary", "kept-user", "kept-assistant", "concurrent-user"]
    all_rows = live.read_conversation(session, ConversationRequest(view=TranscriptView.ALL))
    assert len(all_rows) == 9 and [row.content for row in all_rows[:2]] == ["old user", "old assistant"]
    assert live.append_batch(session, [NewMessage("assistant", "continued")],
                             turn_lease_holder=HOLDER).outcomes[0].id == 11
    assert [row.content for row in live.read_conversation(session)] == [*EXPECTED, "continued"]
    stale_stamp = replace(NewMessage.from_live(current[1].as_openai_message()), db_row_snapshot="wrong")
    try:
        live.compact(session, [stale_stamp], lock_holder=HOLDER)
    except HeldRowRewritten:
        pass
    else:
        raise AssertionError("compaction accepted a stale native row stamp")

    def race_append(method, path, **kwargs):
        if path.endswith("/events:append"):
            live.append_batch(session, [NewMessage("user", "concurrent write at publish")],
                              turn_lease_holder=HOLDER)
        return request(method, path, **kwargs)

    racing = JournalMessages(race_append, session, lease_epoch=epoch, holder=HOLDER)
    try:
        racing.compact(session, [NewMessage("user", "stale summary")], lock_holder=HOLDER)
    except StateAPIError as error:
        assert error.code == "CONFLICT"
    else:
        raise AssertionError("compaction lost a concurrent write")
    assert [row.content for row in live.read_conversation(session)] == [
        *EXPECTED, "continued", "concurrent write at publish",
    ]
    assert sum(row["type"] == "hermes.compaction" for row in request("GET", live.route)["events"]) == 1

    probe_pending_effect(request, session + "-effect")
    rewrite_id = session + "-rewrite"
    rewrite_epoch = acquire_replacement(request, rewrite_id)
    assert rewrite_epoch == 2
    rewrite = JournalMessages(request, rewrite_id, lease_epoch=rewrite_epoch, holder=HOLDER)
    rewritten_rows = rewrite.read_conversation(rewrite_id, ConversationRequest(include_row_ids=True))
    assert [(row.row_id, row.message_uid, row.content) for row in rewritten_rows] == [
        (5, "retained", REWRITTEN_TEXT), (6, "independent", "keep this whole line"),
    ]
    assert rewritten_rows[0].compressed_summary
    assert rewrite.append(rewrite_id, NewMessage("assistant", "resumed"), turn_lease_holder=HOLDER) == 7
    merge_id = session + "-merge"
    merge_epoch = acquire_replacement(request, merge_id)
    assert merge_epoch == 2
    merge = JournalMessages(request, merge_id, lease_epoch=merge_epoch, holder=HOLDER)
    merged_rows = merge.read_conversation(merge_id, ConversationRequest(include_row_ids=True))
    assert [(row.row_id, row.content) for row in merged_rows] == [
        (6, MERGED_SUMMARY), (7, "alpha\n\nbeta"), (8, "foreign prompt"),
    ]
    assert merged_rows[1].message_uid == "alpha"
    assert merged_rows[1].absorbed_message_uids == ("beta",)
    assert merged_rows[1].api_content is None
    display_id = session + "-display"
    display_epoch = acquire_replacement(request, display_id, ttl_ms=30000)
    assert display_epoch == 2
    display_repo = JournalMessages(request, display_id, lease_epoch=display_epoch, holder=HOLDER)
    display_before, display_result = check_display(display_repo, display_id, directory)
    probe_display_snapshot(request, display_repo, display_before)
    display_result["concurrent_append_snapshot"] = True
    source = json.loads((directory / "source.log").read_text().splitlines()[-1])
    print(json.dumps({"source":source, "epoch":epoch, "signal":"SIGKILL", "original_epoch_rejected":True,
                      "active_rows":len(current), "stored_messages":len(all_rows), "continued_at":11,
                      "stale_row_stamp_rejected":True, "concurrent_head_rejected":True,
                      "pending_effect_preserved":True, "rewritten_carry_epoch":rewrite_epoch,
                      "independent_copy_preserved":True, "rewrite_continued_at":7,
                      "merged_carry_epoch":merge_epoch, "merged_uid_witness_preserved":True,
                      "display":display_result, "display_epoch":display_epoch}))


def probe_pending_effect(request, session):
    repo = JournalMessages(request, session, lease_epoch=acquire(request, session),
                           holder=HOLDER, effect_tools=("write",))
    message = NewMessage("assistant", None, tool_calls=({
        "id":"pending-call", "type":"function", "function":{"name":"write", "arguments":'{"item":17}'},
    },))
    repo.append_batch(session, [message], turn_lease_holder=HOLDER)
    key = repo._calls()["pending-call"].key
    try:
        repo.compact(session, [NewMessage("user", "pending action summary")], lock_holder=HOLDER)
    except HermesReconciliationRequired as error:
        assert error.call_id == "pending-call"
    else:
        raise AssertionError("compaction hid an unresolved tool call")
    assert repo._read_intent(key)["status"] == "PREPARED"
    assert request("GET", repo.route)["head_seq"] == 1


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        client, request_state = connection(runner)
        try:
            if sys.argv[1] == "source":
                _, _, session_id, ready_path = sys.argv
                compact(request_state, session_id, acquire(request_state, session_id))
                publish_rewritten_carry(request_state, session_id + "-rewrite")
                publish_merged_carry(request_state, session_id + "-merge")
                display_id = session_id + "-display"
                display_repo = JournalMessages(request_state, display_id, lease_epoch=acquire(request_state, display_id),
                                               holder=HOLDER)
                seed_display(display_repo, display_id, holder=HOLDER)
                Path(ready_path).touch()
                signal.pause()
            else:
                run(request_state, Path(sys.argv[1]))
        finally:
            runner.run(client.close())
