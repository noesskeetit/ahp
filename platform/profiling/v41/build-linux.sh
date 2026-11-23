#!/bin/bash
set -euo pipefail
if [[ $# -ne 2 ]]; then
    echo "usage: $0 assembled-go-module new-binary-directory" >&2
    exit 64
fi
base="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
assembled="$(cd -- "$1" && pwd)"
mkdir -- "$2"
bins="$(cd -- "$2" && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/ahp-v41-build.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT

# The overlays activate instrumentation at the production programs' entry points.
python3 "$base/prepare-overlay.py" "$assembled" "$base/profile-init.go.in" "$scratch"

revision="${AHP_LAB_PRODUCT_SHA:-$(git -C "$base" rev-parse --verify HEAD 2>/dev/null || true)}"
if [[ ! "$revision" =~ ^[0-9a-f]{40}$ ]]; then
    echo 'Set AHP_LAB_PRODUCT_SHA to the assembled product commit when building outside Git' >&2
    exit 65
fi
profiling_revision="${AHP_LAB_PROFILING_SHA:-$(git -C "$base" rev-parse --verify HEAD 2>/dev/null || true)}"
profiling_revision="${profiling_revision:-$revision}"
if [[ ! "$profiling_revision" =~ ^[0-9a-f]{40}$ ]]; then
    echo 'AHP_LAB_PROFILING_SHA must be a full commit SHA' >&2
    exit 65
fi
cd "$assembled"
export GOOS=linux GOARCH=arm64 CGO_ENABLED=0 GOWORK=off
go build -mod=readonly -trimpath -ldflags "-X main.productRevision=$revision -X main.profilingRevision=$profiling_revision" -o "$bins/ahp-v41-profile" ./profiling/cmd/ahp-v41-profile
for role in run-coordinator state-api ahp-vm-runtime; do
    go build -mod=readonly -trimpath -overlay="$scratch/$role.json" -o "$bins/$role" "./cmd/$role"
done
