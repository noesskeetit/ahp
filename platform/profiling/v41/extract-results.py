#!/usr/bin/env python3
"""Extract the bounded profiling results stream into a private host directory."""

import shutil
import sys
import tarfile
from pathlib import Path, PurePosixPath


def extract(archive, destination):
    limit = 256 * 2**20
    destination.mkdir(mode=0o700)
    with tarfile.open(archive, "r:gz") as results:
        for member in results:
            name = PurePosixPath(member.name)
            if name.is_absolute() or ".." in name.parts:
                raise ValueError(f"unsafe result path: {name}")
            if any(part.endswith((".pem", ".key", ".p12", ".pfx")) for part in name.parts):
                raise ValueError(f"key or certificate material in result path: {name}")
            path = destination.joinpath(*name.parts)
            if member.isdir():
                path.mkdir(mode=0o700, parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise ValueError(f"unexpected result entry type: {name}")
            limit -= member.size
            if limit < 0:
                raise ValueError("result archive exceeds the 256 MiB extraction budget")
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with results.extractfile(member) as source, path.open("xb") as target:
                shutil.copyfileobj(source, target)


if __name__ == "__main__":
    try:
        extract(Path(sys.argv[1]), Path(sys.argv[2]))
    except (OSError, ValueError, tarfile.TarError) as error:
        sys.exit(f"Cannot extract profiling results: {error}")
