"""Command surface of the SDK contract check (``python -m ahp_state.check``).

CI calls it on a candidate agent project and gates the image on the exit code, so
the codes are part of the contract: ``0`` no violation, ``1`` violation found,
``2`` the check itself could not run (bad path, unreadable or unparsable file).

The report names files, lines and rules - never source text, because a candidate
project may hold anything in its strings.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Iterable, Sequence

from ahp_state.check import (
    CHECKER_VERSION,
    DEFAULT_EXCLUDES,
    RULES,
    CheckError,
    Finding,
    check_paths,
)

EXIT_OK = 0
EXIT_VIOLATIONS = 1
EXIT_USAGE = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ahp-sdk check",
        description="Check a custom agent project against the AHP container contract "
                    "(PDF rev 3.2 §4.2, §9.2).",
    )
    parser.add_argument("paths", nargs="*", metavar="PATH",
                        help="project file or directory to check (default: .)")
    parser.add_argument("--json", action="store_true", dest="as_json",
                        help="emit a machine-readable report")
    parser.add_argument("--fail-on-warn", action="store_true",
                        help="treat warnings as violations")
    parser.add_argument("--ignore-rule", action="append", default=[], metavar="RULE",
                        help="drop a rule (repeatable); CI should not need this")
    parser.add_argument("--exclude", action="append", default=[], metavar="GLOB",
                        help="extra path name to skip (repeatable)")
    parser.add_argument("--list-rules", action="store_true",
                        help="print the rule ids this build enforces")
    parser.add_argument("--version", action="store_true",
                        help="print the checker version")
    return parser


def format_text(findings: Sequence[Finding], files: int, paths: Iterable[str]) -> str:
    lines = [f"ahp-sdk check {' '.join(paths) or '.'}: "
             f"{files} file(s), {len(findings)} finding(s)"]
    for finding in findings:
        lines.append(f"  {finding.severity.upper():<5} {finding.rule:<22} "
                     f"{finding.locator}: {finding.message}")
    if findings:
        lines.append("rules hit:")
        for rule in sorted({f.rule for f in findings}):
            count = sum(1 for f in findings if f.rule == rule)
            lines.append(f"  {rule:<22} x{count}  {RULES[rule]}")
        lines.append("exit 1: the candidate must be fixed before a pod may run it")
    else:
        lines.append("no violation of the container contract")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.version:
        print(f"ahp-sdk check {CHECKER_VERSION}")
        return EXIT_OK
    if args.list_rules:
        for rule in sorted(RULES):
            print(f"{rule:<22} {RULES[rule]}")
        return EXIT_OK

    paths = args.paths or ["."]
    excludes = tuple(DEFAULT_EXCLUDES) + tuple(args.exclude)
    try:
        findings, checked = check_paths(paths, excludes=excludes,
                                        ignore=args.ignore_rule)
    except CheckError as exc:
        print(f"ahp-sdk check: cannot run: {exc}", file=sys.stderr)
        return EXIT_USAGE

    if args.as_json:
        report = {
            "checker_version": CHECKER_VERSION,
            "paths": list(paths),
            "files_checked": len(checked),
            "findings": [f.as_dict() for f in findings],
            "summary": {"errors": sum(1 for f in findings if f.severity == "error"),
                        "warnings": sum(1 for f in findings if f.severity == "warn")},
        }
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print(format_text(findings, len(checked), paths))

    blocking = [f for f in findings
                if f.severity == "error" or (args.fail_on_warn and f.severity == "warn")]
    return EXIT_VIOLATIONS if blocking else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
