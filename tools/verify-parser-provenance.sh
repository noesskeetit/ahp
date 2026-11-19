#!/usr/bin/env bash
# Task B (build-hygiene lane): prove that private/pdf-closure-20261102/tools/ahp-artifact-parser
# is byte-identical to /usr/local/bin/artifact-parser extracted from the pinned image
# sha256:938bc8e8675b78bfbc93882cdaf23f0addbac9184fb7d6019d80882c4d01bb84 (evidence/parser-image-id.txt).
# Writes evidence/parser-provenance-<UTC>Z.json. Fails closed. No secrets in output.
# Usage: tools/verify-parser-provenance.sh   (requires a responsive Docker daemon)
set -euo pipefail

IMAGE_ID="sha256:938bc8e8675b78bfbc93882cdaf23f0addbac9184fb7d6019d80882c4d01bb84"
PARSER_IN_IMAGE="/usr/local/bin/artifact-parser"
EXPECTED_SHA="${AHP_PARSER_EXPECTED_SHA256:-2d61620a22a026b5236faa1bf0a48ef2e903353e310308b8302b4ea7f29e2c00}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CAND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"          # lane (candidate) root
PDF_DIR="$(cd "$CAND_DIR/.." && pwd)"             # private/pdf-closure-20261102
EVIDENCE_DIR="$PDF_DIR/evidence"
TOOLS_BIN="$PDF_DIR/tools/ahp-artifact-parser"
RUNS_DIR="$PDF_DIR/tools/go-build-cache/runs"     # bounded cache root, outside $TMPDIR
mkdir -p "$EVIDENCE_DIR" "$RUNS_DIR"

[ -x "$TOOLS_BIN" ] || { echo "missing tools binary: $TOOLS_BIN" >&2; exit 2; }
docker info >/dev/null 2>&1 || { echo "DOCKER_UNAVAILABLE: daemon not answering; provenance proof deferred" >&2; exit 3; }
docker image inspect "$IMAGE_ID" >/dev/null 2>&1 || { echo "pinned image not present locally: $IMAGE_ID" >&2; exit 4; }

RUN_TMP="$(mktemp -d "$RUNS_DIR/parser-prov.XXXXXXXX")"
CID=""
cleanup(){ local rc=$?
  [ -n "$CID" ] && docker rm "$CID" >/dev/null 2>&1 || true
  /usr/bin/env python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1], ignore_errors=True)' "$RUN_TMP"
  exit $rc; }
trap cleanup EXIT INT TERM

docker run --rm --entrypoint /bin/sh "$IMAGE_ID" -c "sha256sum $PARSER_IN_IMAGE" > "$RUN_TMP/in-image-sha.txt" 2>/dev/null \
  || { echo "image present but $PARSER_IN_IMAGE not readable in it" >&2; exit 5; }
IMAGE_SHA="$(awk '{print $1}' "$RUN_TMP/in-image-sha.txt" | head -1)"
[ "${#IMAGE_SHA}" = 64 ] || { echo "unexpected sha256sum output from image" >&2; exit 5; }

CID="$(docker create "$IMAGE_ID")"
docker cp "$CID:$PARSER_IN_IMAGE" "$RUN_TMP/artifact-parser" >/dev/null
EXTRACTED_SHA="$(shasum -a 256 "$RUN_TMP/artifact-parser" | cut -d' ' -f1)"
TOOLS_SHA="$(shasum -a 256 "$TOOLS_BIN" | cut -d' ' -f1)"

[ "$EXTRACTED_SHA" = "$IMAGE_SHA" ] || { echo "EXTRACT_MISMATCH: docker cp sha != in-image sha256sum" >&2; exit 6; }

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$EVIDENCE_DIR/parser-provenance-$TS.json"
MATCH_TOOLS=false; [ "$EXTRACTED_SHA" = "$TOOLS_SHA" ] && MATCH_TOOLS=true
MATCH_EXPECTED=false; [ "$EXTRACTED_SHA" = "$EXPECTED_SHA" ] && MATCH_EXPECTED=true

python3 - "$OUT" "$IMAGE_ID" "$PARSER_IN_IMAGE" "$IMAGE_SHA" "$EXTRACTED_SHA" "$TOOLS_SHA" "$EXPECTED_SHA" "$TOOLS_BIN" "$TS" "$MATCH_TOOLS" "$MATCH_EXPECTED" <<'PY'
import sys,json
out,image,path_in_image,image_sha,extracted,tools_sha,expected,tools_path,ts,mt,me=sys.argv[1:13]
json.dump({"kind":"parser-provenance","created_at_utc":ts+"Z","pinned_image_id":image,
 "path_in_image":path_in_image,"image_in_container_sha256":image_sha,
 "extracted_sha256":extracted,"tools_binary":{"path":tools_path,"sha256":tools_sha},
 "expected_sha256":expected,"match_extracted_vs_tools":mt=="true","match_expected":me=="true",
 "verdict":"PASS" if (mt=="true" and me=="true") else "FAIL","method":"docker create + docker cp + shasum -a 256"},
 open(out,"w"),indent=2)
print("evidence:",out)
PY

[ "$MATCH_TOOLS" = true ] && [ "$MATCH_EXPECTED" = true ] || { echo "PROVENANCE FAIL (see $OUT)" >&2; exit 7; }
echo "PROVENANCE OK: extracted==$TOOLS_SHA==$EXPECTED_SHA"
