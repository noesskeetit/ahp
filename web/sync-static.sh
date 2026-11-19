#!/bin/sh
# Put a BUILT frontend into web/static, i.e. into what `//go:embed static` compiles into as-runner.
#
# Why this exists: web/static in the repository is a hand-written 2 678-byte page titled
# "Agents Space" (index.html + app.js + style.css). Every surface that serves it — as-runner at
# /, and scripts/vm/iso-vm-harness.sh `run-origin` via AHP_HUMAN_ORIGIN_UI — therefore shows a
# placeholder while the real SPA exists only as a separately served Vite build. No step in either
# repository copied the build into web/static, so "the stand shows a placeholder" was structural,
# not a frontend bug. This script is that step. It only copies files: no node, no compiler, no
# network — run it before `go build ./cmd/as-runner`, which is the build lane's own command.
#
#   sh web/sync-static.sh <built-dist-dir> [repo-root]
#
# Refusals, all deliberate:
#   * a shell whose window.env still contains ${VITE_*} — that is the 405-on-login incident, and an
#     unrendered shell embedded in a binary is worse than a stub because it looks authenticated;
#   * a directory that is not a build at all (needs index.html plus a module script tag);
#   * the stub itself (index.html under 3 000 bytes, or no /assets/ reference).
set -eu

DIST=${1:?usage: sync-static.sh <built-dist-dir> [repo-root]}
REPO=${2:-$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)}
TARGET="$REPO/web/static"

# The copy deletes the target directory, so refuse anything that is not this repository: the embed
# target is only ever the directory next to the file that carries `//go:embed static`.
[ -f "$REPO/web/embed.go" ] || { echo "not this repo (no web/embed.go): $REPO" >&2; exit 2; }

[ -d "$DIST" ] || { echo "not a directory: $DIST" >&2; exit 2; }
[ -f "$DIST/index.html" ] || { echo "no index.html in $DIST — that is not a build" >&2; exit 2; }

BYTES=$(wc -c <"$DIST/index.html")
[ "$BYTES" -ge 3000 ] || { echo "index.html is $BYTES bytes — that is the stub, not the built UI" >&2; exit 3; }
grep -q '/assets/' "$DIST/index.html" || { echo "index.html references no /assets/ bundle — not the built SPA" >&2; exit 3; }

GUARD=/home/alex/ahp-hive/tools/stand-html-render-check.sh
if [ -x "$GUARD" ] || [ -f "$GUARD" ]; then
  sh "$GUARD" "$DIST/index.html" || { echo "refusing an unrendered shell (guard rc=3)" >&2; exit 3; }
else
  grep -q '\${VITE_' "$DIST/index.html" && { echo "refusing: unresolved \${VITE_*} placeholders and no guard available" >&2; exit 3; }
fi

rm -rf "$TARGET"
mkdir -p "$TARGET"
# `cp -R src/. dest` so dotfiles and the tree shape survive without depending on GNU-only flags.
cp -R "$DIST/." "$TARGET/"

FILES=$(find "$TARGET" -type f | wc -l)
TREE_SHA=$(find "$TARGET" -type f -exec sha256sum {} + | sed "s|$TARGET/||" | LC_ALL=C sort | sha256sum | cut -d' ' -f1)
INDEX_SHA=$(sha256sum "$TARGET/index.html" | cut -d' ' -f1)

echo "web/static := $DIST"
echo "files=$FILES bytes=$(du -sk "$TARGET" | cut -f1)KiB"
echo "index_sha256=$INDEX_SHA"
echo "tree_sha256=$TREE_SHA"
echo "next: go build ./cmd/as-runner — the binary now embeds these bytes, so check the served shell equals index_sha256"
echo "or skip the rebuild: AHP_WEB_STATIC_DIR=$DIST runs the same bytes from disk (same guard, fails closed without index.html)"
