#!/usr/bin/env bash
# Build a product binary FROM SOURCE into bin/ and attest its VCS stamp.
#
# Why this exists (DEF-LANE-TRACKED-BINARY): `go build ./cmd/ahp/` writes ./ahp
# into the repository root, and until now that file was tracked in git, so any
# plain build dirtied the tree and every primary log taken from it carried a
# dirty-base stamp. Consumers must build into bin/ (ignored) and prove WHICH
# revision the binary carries instead of trusting a filename.
#
# Refuses to run on a dirty tree: a binary whose stamp cannot be trusted is not
# worth producing, and a dirty tree is exactly how the 8e4d8766 / modified=true
# stamps were produced earlier today.
#
# Usage: scripts/build-product.sh [cmd-name ...]        (default: ahp as-runner)
# Environment:
#   AHP_BUILD_TAGS     default "ahp_exec ahp_data"      (the canonical product tags)
#   AHP_BUILD_OUT      default "bin"
#   AHP_BUILD_GOPROXY  default "off" (module cache must be warm; set to override)
#   AHP_BUILD_FLAGS    extra `go build` flags
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel) || { echo "REFUSE: not a git work tree" >&2; exit 2; }
cd "$repo_root"

dirty=$(git status --porcelain | wc -l | tr -d ' ')
if [ "$dirty" != "0" ]; then
  echo "REFUSE: work tree is dirty ($dirty paths): a build taken from a dirty tree cannot be stamped" >&2
  git status --porcelain | head -10 >&2
  exit 2
fi

rev=$(git rev-parse HEAD)
tags=${AHP_BUILD_TAGS:-"ahp_exec ahp_data"}
out=${AHP_BUILD_OUT:-bin}
goproxy=${AHP_BUILD_GOPROXY:-off}
targets=("${@:-}")
if [ -z "${targets[0]}" ]; then targets=(ahp as-runner); fi

mkdir -p "$out"
declare -a attested=()
for name in "${targets[@]}"; do
  pkg="./cmd/$name"
  [ -d "$pkg" ] || { echo "REFUSE: no such product command: $pkg" >&2; exit 2; }
  echo "== build $pkg -> $out/$name (tags=[$tags] CGO_ENABLED=0 -trimpath GOPROXY=$goproxy)"
  # -trimpath keeps paths out of the binary; CGO_ENABLED=0 keeps it static;
  # the tags are what makes it the product at all (88 files are tag-guarded,
  # so an untagged build is a different program, not a cheaper one).
  env CGO_ENABLED=0 GOPROXY="$goproxy" GOFLAGS="-trimpath ${AHP_BUILD_FLAGS:-}" \
    go build -tags "$tags" -o "$out/$name" "$pkg"

  info=$(go version -m "$out/$name" 2>/dev/null) || { echo "REFUSE: cannot read build info of $out/$name" >&2; exit 3; }
  stamp=$(printf '%s\n' "$info" | sed -n 's/.*vcs\.revision=\([0-9a-f]*\).*/\1/p' | head -1)
  modified=$(printf '%s\n' "$info" | sed -n 's/.*vcs\.modified=\([a-z]*\).*/\1/p' | head -1)
  gotags=$(printf '%s\n' "$info" | sed -n 's/.*build\tbuildTags=\(.*\)/\1/p' | head -1)
  if [ "$stamp" != "$rev" ]; then
    echo "REFUSE: $out/$name carries vcs.revision=${stamp:-<none>}, expected $rev" >&2
    echo "        (a nested work tree stamps its parent -- build from a standalone clone of the exact revision)" >&2
    exit 3
  fi
  if [ "$modified" != "false" ]; then
    echo "REFUSE: $out/$name carries vcs.modified=${modified:-<unknown>}; refusing to publish an unattestable binary" >&2
    exit 3
  fi
  attested+=("$out/$name vcs.revision=$stamp vcs.modified=$modified buildTags=${gotags:-<unset>}")
done

printf '%s\n' "== attested:"
printf '   %s\n' "${attested[@]}"
printf '%s\n' "== sha256:"
for name in "${targets[@]}"; do shasum -a 256 "$out/$name"; done
