"""The double may not lie quietly again: its answers are pinned to live State.

Two tests, one matrix (``contract_probe.PROBES``).

``test_the_double_still_matches_the_measured_live_answers`` runs the matrix
against ``fake_state.py`` and compares it with ``LIVE`` below - every value in
that table was read off a real ``cmd/state-stand`` over real HTTPS against real
PostgreSQL on 2026-11-01, and regenerated with::

    PYTHONPATH=. python -m ahp_state.tests.divergence_report --handshake ... \
        > divergence.md

It runs with no stand and no network, so the ordinary suite cannot drift the
double back into the shape that made block 1's evidence worthless.

``test_the_live_stand_still_answers_this_way`` re-runs the same matrix against a
live stand and compares against the same table, so a platform change is caught
too. It needs the hand-out and skips otherwise::

    AHP_STATE_HANDSHAKE=/path/handshake.json
"""

from __future__ import annotations

import json
import os
import pathlib

import pytest

from ahp_state.tests.contract_probe import Harness, run_matrix
from ahp_state.tests.divergence_report import double_harness, drain, live_harness

HANDSHAKE = os.environ.get("AHP_STATE_HANDSHAKE", "")

#: Measured against the live State stand, 2026-11-01. Do not edit to make a test
#: pass: re-measure with ``python -m ahp_state.tests.divergence_report`` and
#: paste what the stand actually answered.
LIVE = {
    'KV idempotency': {
        'PutKV first': 'version=1',
        'another run reuses the operation key': 'ERR KEY_REUSE_MISMATCH',
        'new op key, correct version': 'accepted',
        'new op key, stale expected_version': 'ERR CONFLICT',
        'replay same op key, changed body': 'ERR KEY_REUSE_MISMATCH',
        'replay same operation_key': 'version=1',
        'value after all writes': '{"v": 2}',
    },
    'absent reads': {
        'GetIntent absent': 'ERR NOT_FOUND',
        'GetKV absent key': 'ERR NOT_FOUND',
    },
    'credentials': {
        'fenced write with forged token': 'ERR FORBIDDEN',
        'read with forged token': 'ERR FORBIDDEN',
    },
    'empty claim': {
        'ClaimRun with nothing queued': 'lease null',
    },
    'envelope shapes': {
        'ClaimRun lease keys': "['epoch', 'expires_at', 'run', 'scope', 'thread', 'worker']",
        'ClaimRun run keys': "['id', 'payload', 'sequence', 'status', 'task_id', 'thread_id']",
        'CreateRun envelope': "FLAT+['id', 'payload', 'sequence', 'status', 'task_id', 'thread_id']",
        'CreateThread envelope': "FLAT+['context_id', 'created_at', 'epoch', 'id', 'scope']",
    },
    'intent machine': {
        'GetIntent after dispatch': 'dispatched',
        'GetIntent after success': 'succeeded',
        'RecordIntent row has id': 'True',
        'RecordIntent status': 'recorded',
        'dispatched -> succeeded': 'accepted',
        'record with a non-string target': 'ERR INVALID_ARGUMENT',
        'record with new fingerprint': 'ERR KEY_REUSE_MISMATCH',
        'recorded -> dispatched': 'accepted',
        'recorded -> succeeded': 'ERR NOT_FOUND',
        'replay reports current status': 'dispatched',
        'replay same digest': 'True',
        'replay same identity': 'True',
        'succeeded -> different result': 'ERR CONFLICT',
        'succeeded -> dispatched (rewind)': 'ERR CONFLICT',
    },
    'released lease': {
        'Release': "FLAT+['ok']",
        'Release with an unknown status': 'ERR INVALID_ARGUMENT',
        'heartbeat after release': 'ERR STALE_EPOCH',
        'write after release': 'ERR STALE_EPOCH',
    },
    'thread identity': {
        're-CreateThread same context_id': 'True',
        'same context_id new key': 'True',
    },
    'write authorization': {
        'write at stale epoch': 'ERR STALE_EPOCH',
        'write by wrong worker': 'ERR STALE_EPOCH',
        'write with unknown run': 'ERR NOT_FOUND',
    },
}


def _diff(observed: dict, expected: dict) -> list[str]:
    rows = []
    for section, answers in expected.items():
        for label, wanted in answers.items():
            got = observed.get(section, {}).get(label, "<not probed>")
            if got != wanted:
                rows.append(f"{section} / {label}: expected {wanted!r}, got {got!r}")
    for section, answers in observed.items():
        for label in answers:
            if label not in expected.get(section, {}):
                rows.append(f"{section} / {label}: new observation, not in the "
                            f"measured table")
    return rows


def test_the_double_still_matches_the_measured_live_answers() -> None:
    harness = double_harness()
    try:
        drain(harness)
        observed = run_matrix(harness)
    finally:
        double_harness.server.__exit__()          # type: ignore[attr-defined]
    drift = _diff(observed, LIVE)
    assert drift == [], "the double drifted from measured live State:\n" + "\n".join(drift)


@pytest.mark.skipif(not (HANDSHAKE and pathlib.Path(HANDSHAKE).is_file()),
                    reason="no live State stand (AHP_STATE_HANDSHAKE unset)")
def test_the_live_stand_still_answers_this_way() -> None:
    harness = live_harness(HANDSHAKE)
    try:
        drain(harness)
        observed = run_matrix(harness)
    finally:
        live_harness.close()                      # type: ignore[attr-defined]
    drift = _diff(observed, LIVE)
    assert drift == [], "the double drifted from measured live State:\n" + "\n".join(drift)
