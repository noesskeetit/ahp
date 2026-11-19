#!/usr/bin/env python3
"""Generate the acceptance-mechanism view of the skip-tag registry.

Why two files exist (COND-5):
  tools/skip-tags.json            -- the CANONICAL registry. Human vocabulary: tags,
                                     classes, status, emitted_by, notes. Owned by the
                                     owner of the skip-token form (Volta).
  tools/skip-tags.mechanism.json  -- the view scripts/skip-verdict.mjs can consume.
                                     Its --tags flag requires a JSON ARRAY of objects
                                     with at least "tag" and "class" (readTagFile),
                                     and refuses a tag that is absent from it as
                                     UNKNOWN_SKIP_TAG. The registry is an OBJECT keyed
                                     by tag, so it cannot be passed directly: passing
                                     it would make the mechanism refuse the file, and a
                                     refused dictionary is a gate that cannot read.

This file is generated, never hand-edited. Deterministic: sorted by tag, fixed field
order, trailing newline.

  regenerate: python3 tools/gen-skip-tags-mechanism.py
  verify:     python3 tools/gen-skip-tags-mechanism.py --check   (exit 3 on drift)

tests/hygiene re-derives the same expectation in Go, independently of this script, so
a bug in either implementation shows up as a red leg rather than as a silent gate.
"""
import json
import sys

HERE = __file__.rsplit("/", 1)[0]
REGISTRY = f"{HERE}/skip-tags.json"
VIEW = f"{HERE}/skip-tags.mechanism.json"


def render():
    with open(REGISTRY, encoding="utf-8") as handle:
        registry = json.load(handle)
    rows = []
    for tag in sorted(registry["tags"]):
        entry = registry["tags"][tag]
        if entry["class"] not in registry["closed_classes"]:
            raise SystemExit(f"{tag}: class {entry['class']!r} is outside closed_classes")
        rows.append({"tag": tag, "class": entry["class"], "status": entry["status"]})
    return json.dumps(rows, indent=2) + "\n"


def main():
    want = render()
    if "--check" in sys.argv[1:]:
        try:
            with open(VIEW, encoding="utf-8") as handle:
                have = handle.read()
        except FileNotFoundError:
            have = ""
        if have != want:
            sys.stderr.write("DRIFT: tools/skip-tags.mechanism.json does not match tools/skip-tags.json; "
                             "run: python3 tools/gen-skip-tags-mechanism.py\n")
            return 3
        print("skip-tags.mechanism.json is in sync with the registry")
        return 0
    with open(VIEW, "w", encoding="utf-8") as handle:
        handle.write(want)
    print(f"wrote {VIEW}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
