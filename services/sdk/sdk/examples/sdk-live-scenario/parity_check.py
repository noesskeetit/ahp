#!/usr/bin/env python3
"""Compare the Go and Python verdicts of sdk-live-scenario against each other.

The shared contract (Boyle's scenario.json) is the source of expected codes; this
script never re-derives them. It answers three questions and nothing else:

1. Did both legs read the SAME contract bytes?  (contract.sha256 must match)
2. Do the two legs AGREE step by step?  A step one leg has not implemented is
   reported as asymmetric coverage, never as a mismatch -- absence of an
   implementation is not a disagreement about the platform.
3. Were they two independent runs?  Per-run identity (stamp, artifact id/version/
   digest, tool result digest) must DIFFER, otherwise one leg replayed the other.

Exit codes: 0 PARITY_PASS, 1 PARITY_FAIL, 3 PARTIAL (agreement proven, but no
plane had an effect proved by either leg -- i.e. nothing actually ran end to end).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

UNIMPLEMENTED = "SKIP_NOT_IMPLEMENTED"
# Fields that must be unique per run: two legs that sealed the same bytes under
# the same ids are one run wearing two hats.
IDENTITY_FIELDS = (
    ("stamp", ("stamp",)),
    ("artifact.artifact_id", ("artifact", "artifact_id")),
    ("artifact.version", ("artifact", "version")),
    ("artifact.sha256", ("artifact", "sha256")),
    ("tool_result.digest", ("tool_result", "digest")),
)


def dig(report: dict, path: tuple[str, ...]):
    node = report
    for piece in path:
        if not isinstance(node, dict):
            return None
        node = node.get(piece)
    return node if node not in ("", 0) else None


def load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("go_report")
    parser.add_argument("python_report")
    parser.add_argument("--out", default="")
    args = parser.parse_args()

    go, py = load(args.go_report), load(args.python_report)
    failures: list[str] = []
    notes: list[str] = []

    go_sha = (go.get("contract") or {}).get("sha256", "")
    py_sha = (py.get("contract") or {}).get("sha256", "")
    if not go_sha or go_sha != py_sha:
        failures.append(f"contract sha256 differs: go={go_sha[:12]} python={py_sha[:12]}")

    go_steps = {row["id"]: row for row in go.get("steps", [])}
    py_steps = {row["id"]: row for row in py.get("steps", [])}
    only_go = sorted(set(go_steps) - set(py_steps))
    only_py = sorted(set(py_steps) - set(go_steps))
    if only_go or only_py:
        failures.append(f"step sets differ: go-only={only_go} python-only={only_py}")

    agreed: list[str] = []
    asymmetric: list[dict] = []
    neither: list[str] = []
    for step_id in sorted(set(go_steps) & set(py_steps)):
        left, right = go_steps[step_id]["status"], py_steps[step_id]["status"]
        if left == UNIMPLEMENTED or right == UNIMPLEMENTED:
            entry = {"id": step_id, "go": left, "python": right}
            if left == right == UNIMPLEMENTED:
                neither.append(step_id)
            else:
                asymmetric.append(entry)
            continue
        if left != right:
            failures.append(f"{step_id}: go={left} python={right} "
                            f"(expected {go_steps[step_id].get('expected')})")
        else:
            agreed.append(step_id)
            if left == "FAIL":
                failures.append(f"{step_id}: both legs FAIL -- parity agrees, the platform does not")

    go_prop = {row["id"]: row["status"] for row in go.get("proposed_steps", [])}
    py_prop = {row["id"]: row["status"] for row in py.get("proposed_steps", [])}
    for step_id in sorted(set(go_prop) & set(py_prop)):
        if go_prop[step_id] != py_prop[step_id]:
            failures.append(f"proposed {step_id}: go={go_prop[step_id]} python={py_prop[step_id]}")
        else:
            agreed.append(step_id)
    if set(go_prop) != set(py_prop):
        failures.append(f"proposed step sets differ: go={sorted(go_prop)} python={sorted(py_prop)}")

    shared_identity: dict[str, dict] = {}
    for label, path in IDENTITY_FIELDS:
        left, right = dig(go, path), dig(py, path)
        if left is None or right is None:
            continue
        shared_identity[label] = {"go": str(left)[:32], "python": str(right)[:32]}
        if str(left) == str(right):
            failures.append(f"{label} is identical in both legs ({str(left)[:32]}) -- "
                            "one run cannot prove two SDKs")

    planes: dict[str, str] = {}
    for leg_name, report in (("go", go), ("python", py)):
        for plane, proved in (report.get("effect_proved_by_plane") or {}).items():
            if proved:
                planes[plane] = (planes.get(plane, "") + " " + leg_name).strip()

    verdict = "PARITY_FAIL" if failures else ("PARTIAL" if not planes else "PARITY_PASS")
    out = {
        "schema": "ahp-sdk-live-parity/v1",
        "contract_sha256": go_sha,
        "verdict": verdict,
        "counts": {"agreed": len(agreed), "asymmetric": len(asymmetric),
                   "unimplemented_both": len(neither), "failures": len(failures)},
        "agreement": {"steps_agreed": len(agreed), "asymmetric_coverage": asymmetric,
                      "identity_checked": shared_identity,
                      "effect_proved_by_plane": planes,
                      "unimplemented_by_both_legs": neither},
        "failures": failures,
        "notes": notes,
    }
    text = json.dumps(out, ensure_ascii=False, indent=1)
    if args.out:
        Path(args.out).write_text(text + "\n", encoding="utf-8")
    print(f"verdict={verdict} agreed={len(agreed)} asymmetric={len(asymmetric)} "
          f"effect_planes={sorted(planes)}")
    for line in failures:
        print("  FAIL", line)
    for item in asymmetric:
        print(f"  ASYM {item['id']} go={item['go']} python={item['python']}")
    if args.out:
        print("report:", args.out)
    return {"PARITY_PASS": 0, "PARTIAL": 3}[verdict] if verdict != "PARITY_FAIL" else 1


if __name__ == "__main__":
    sys.exit(main())
