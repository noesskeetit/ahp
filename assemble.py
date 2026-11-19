#!/usr/bin/env python3
"""Assemble an AHP build tree from the local service directories."""

import argparse
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tempfile


def archive_files(root):
    ignored_dirs = {".git", "__pycache__", ".pytest_cache", "node_modules"}
    for group in (root / "services", root / "platform"):
        for directory, dirs, files in os.walk(group):
            dirs[:] = sorted(name for name in dirs if name not in ignored_dirs)
            for name in sorted(files):
                local_env = name == ".env" or (
                    name.startswith(".env.") and name != ".env.example"
                )
                if name == ".DS_Store" or name.endswith(".pyc") or local_env:
                    continue
                yield (Path(directory) / name).relative_to(root).as_posix()


def source_files(root):
    if (root / ".git").exists():
        tracked = subprocess.check_output(
            ["git", "-C", str(root), "ls-files", "--cached", "-z", "--", "services/", "platform/"]
        )
        paths = (os.fsdecode(raw) for raw in tracked.split(b"\0") if raw)
    else:
        paths = archive_files(root)
    for name in paths:
        path = PurePosixPath(name)
        prefix = 2 if path.parts[0] == "services" else 1
        source = root.joinpath(*path.parts)
        if not source.is_file() or source.is_symlink():
            raise ValueError(f"Expected a regular source file: {path}")
        yield source, Path(*path.parts[prefix:])


def assemble(root, destination):
    if destination.exists():
        raise ValueError(f"Destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    owners = {}
    with tempfile.TemporaryDirectory(prefix=".ahp-assemble-", dir=destination.parent) as tmp:
        staged = Path(tmp) / "tree"
        staged.mkdir()
        for source, relative in source_files(root):
            if relative in owners:
                raise ValueError(f"{relative} is provided by both {owners[relative]} and {source}")
            owners[relative] = source
            output = staged / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, output)
        if not (staged / "go.mod").is_file():
            raise ValueError("The assembled tree is missing platform/go.mod")
        staged.rename(destination)
    return len(owners)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path, help="path to a new build directory")
    args = parser.parse_args()
    destination = args.destination.resolve()
    try:
        count = assemble(Path(__file__).resolve().parent, destination)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Cannot assemble AHP: {error}\n")
    print(f"Assembled {count} files in {destination}")


if __name__ == "__main__":
    main()
