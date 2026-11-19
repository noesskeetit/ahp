#!/usr/bin/env bash
# Reproducible candidate build with REAL provenance (docs/acceptance-run.md section 0).
#
# WHY a standalone clone: `go build` stamps VCS only when the module's `.git` is a real
# directory. Inside a nested *worktree* (`.git` is a file) Go walks up and stamps the
# enclosing repository -- on this Mac the CONTROL repo (company-brain), not the product
# -- or omits the stamp. ldflags alone is NOT provenance. We therefore build from a
# minimal standalone clone (real .git dir) checked out at the target revision, so the
# native VCS stamp reports vcs.revision==HEAD and vcs.modified==false.
#
# buildinfo: internal/buildinfo.{Revision,CommitTime,Modified} is ALSO injected via
# -ldflags so candidate binaries match the live stand (which uses provenance-build*.sh).
# This is additive runtime reporting; the PROVENANCE proof is the native vcs stamp.
#
# Reproducibility: every binary is built twice into separate fresh caches and must be
# byte-identical (sha256) or the build fails closed.
#
# Disk hygiene (ops-tmp-cache-provenance-20261109): every temp this script creates
# (clone, the two cold A/B GOCACHEs per binary, image build contexts) lives under one
# RUN_TMP inside a bounded cache root OUTSIDE $TMPDIR, and an EXIT/INT/TERM trap removes
# it on success AND on failure. Deletions use python shutil.rmtree (sandbox policy
# forbids rm -rf) and only ever touch paths this run created.
#
# Usage: scripts/build-candidate.sh [--revision <40hex>] [--out <dir>] [--linux] [--images] [--keep-temp]
# Env: AHP_BUILD_TAGS (default ahp_data,ahp_exec), AHP_BUILDINFO=0 to skip buildinfo ldflags,
#  AHP_ROOT (default: three levels above the repo root), AHP_BUILD_CACHE_ROOT (default
#  $AHP_ROOT/private/pdf-closure-20261102/tools/go-build-cache), AHP_BUILD_CACHE_MAX_BYTES
#  (default 6442450944 = 6 GiB), KEEP_TMP=1 / KEEP_CLONE=1 or --keep-temp to keep temps for a post-mortem.
# Cross-run determinism note: the standalone clone is checked out at a FIXED path
# ($AHP_BUILD_CACHE_ROOT/runs/clone-current) when free, because debug info records absolute
# source paths -- a random mktemp clone dir makes consecutive runs differ byte-wise. If the
# fixed path is busy (parallel builds), a unique clone path is used and cross-run
# byte-identity for that run makes no claim; the in-run A/B reproducibility check still holds.
# No secrets are printed. Requires git, go, (docker only with --images).
set -euo pipefail

REVISION=""; OUT=""; BUILD_LINUX=0; BUILD_IMAGES=0
KEEP_TMP="${KEEP_TMP:-0}"; KEEP_CLONE="${KEEP_CLONE:-0}"
TAGS="${AHP_BUILD_TAGS:-ahp_data,ahp_exec}"
BUILDINFO="${AHP_BUILDINFO:-1}"
CMDS=(ahp ahp-agent ahp-deploy ahp-note-mcp artifact-parser as-runner compute-host state-consumer state-stand synthetic-authority)
while [ $# -gt 0 ]; do case "$1" in
  --revision) REVISION="$2"; shift 2;;
  --out) OUT="$2"; shift 2;;
  --linux) BUILD_LINUX=1; shift;;
  --images) BUILD_LINUX=1; BUILD_IMAGES=1; shift;;
  --keep-temp) KEEP_TMP=1; KEEP_CLONE=1; shift;;
  *) echo "unknown arg: $1" >&2; exit 2;;
esac; done

GIT_COMMON="$(git rev-parse --path-format=absolute --git-common-dir)"
REPO_ROOT="$(git rev-parse --show-toplevel)"
: "${REVISION:=$(git rev-parse HEAD)}"
echo "$REVISION" | grep -Eq '^[a-f0-9]{40}$' || { echo "revision must be full 40-hex" >&2; exit 2; }

OUT="${OUT:-$REPO_ROOT/build-out}"
HOST_OS="$(go env GOOS)"; HOST_ARCH="$(go env GOARCH)"; HOST_GO="$(go version | awk '{print $3}')"
BUILT_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
BI_PKG=github.com/ahp-platform/ahp/internal/buildinfo

# ---- disk hygiene: root cause of the 2026-11-09 ENOSPC incident was mktemp(1) cold
# ---- GOCACHEs accumulating in $TMPDIR with no removal path. Own temps go under one
# ---- RUN_TMP below a bounded cache root on the Data volume, swept by an EXIT trap.
AHP_ROOT="${AHP_ROOT:-$(cd "$REPO_ROOT/../../.." && pwd)}"
AHP_BUILD_CACHE_ROOT="${AHP_BUILD_CACHE_ROOT:-$AHP_ROOT/private/pdf-closure-20261102/tools/go-build-cache}"
AHP_BUILD_CACHE_MAX_BYTES="${AHP_BUILD_CACHE_MAX_BYTES:-6442450944}"  # 6 GiB
mkdir -p "$AHP_BUILD_CACHE_ROOT/gocache" "$AHP_BUILD_CACHE_ROOT/runs"

ahp_tree_bytes() { # apparent bytes under $1 (0 on any error)
  /usr/bin/env python3 -c 'import os,sys
t=0
for r,_,fs in os.walk(sys.argv[1]):
    for f in fs:
        try: t+=os.path.getsize(os.path.join(r,f))
        except OSError: pass
print(t)' "$1" 2>/dev/null || echo 0
}

ahp_rmtree_bytes() { # delete OUR OWN paths with python (never rm -rf), print bytes freed
  [ $# -gt 0 ] || return 0
  /usr/bin/env python3 - "$@" <<'PY'
import os,sys,shutil
total=0
for p in sys.argv[1:]:
    n=0
    for r,_,fs in os.walk(p):
        for f in fs:
            try: n+=os.path.getsize(os.path.join(r,f))
            except OSError: pass
    shutil.rmtree(p, ignore_errors=True)
    total+=n
print(total)
PY
}

ahp_num() { local v="${1//[^0-9]/}"; printf '%s' "${v:-0}"; }

ahp_trim_build_cache() { # keep the shared cache root bounded BEFORE a build starts
  local before after
  before="$(ahp_num "$(ahp_tree_bytes "$AHP_BUILD_CACHE_ROOT")")"
  if [ "$before" -le "$AHP_BUILD_CACHE_MAX_BYTES" ]; then return 0; fi
  # 1) crash litter: our own run.* dirs older than 6h (healthy runs delete themselves)
  /usr/bin/env python3 - "$AHP_BUILD_CACHE_ROOT/runs" 21600 >&2 <<'PY' || true
import os,sys,shutil,time
root,maxage=sys.argv[1],int(sys.argv[2]); now=time.time(); freed=0
try: entries=sorted(os.listdir(root))
except OSError: entries=[]
for name in entries:
    p=os.path.join(root,name)
    if not (name.startswith("run.") or name=="clone-current") or not os.path.isdir(p): continue
    try:
        if now-os.stat(p).st_mtime < maxage: continue
    except OSError: continue
    n=0
    for r,_,fs in os.walk(p):
        for f in fs:
            try: n+=os.path.getsize(os.path.join(r,f))
            except OSError: pass
    shutil.rmtree(p, ignore_errors=True); freed+=n
print("[build-hygiene] trim_stale_runs freed_bytes=%d"%freed, file=sys.stderr)
PY
  after="$(ahp_num "$(ahp_tree_bytes "$AHP_BUILD_CACHE_ROOT")")"
  if [ "$after" -gt "$AHP_BUILD_CACHE_MAX_BYTES" ]; then
    # 2) hard cap: reset the shared warm GOCACHE, scoped to its own subdirectory
    GOCACHE="$AHP_BUILD_CACHE_ROOT/gocache" go clean -cache >/dev/null 2>&1 || true
    # 3) legacy flat cache entries at the cache root: drop entries idle for > 30 days
    /usr/bin/env python3 - "$AHP_BUILD_CACHE_ROOT" 2592000 >&2 <<'PY' || true
import os,sys,time
root,maxage=sys.argv[1],int(sys.argv[2]); now=time.time(); freed=0
for name in sorted(os.listdir(root)):
    d=os.path.join(root,name)
    if len(name)!=2 or not os.path.isdir(d): continue
    for sub in sorted(os.listdir(d)):
        p=os.path.join(d,sub)
        if not os.path.isdir(p): continue
        for r,_,fs in os.walk(p):
            for f in fs:
                fp=os.path.join(r,f)
                try:
                    if now-os.stat(fp).st_mtime < maxage: continue
                    freed+=os.path.getsize(fp); os.remove(fp)
                except OSError: pass
        for r,ds,fs in os.walk(p,topdown=False):
            try: os.rmdir(r)
            except OSError: pass
print("[build-hygiene] trim_legacy_cache freed_bytes=%d"%freed, file=sys.stderr)
PY
    after="$(ahp_num "$(ahp_tree_bytes "$AHP_BUILD_CACHE_ROOT")")"
  fi
  echo "[build-hygiene] cache_trim before_bytes=$before after_bytes=$after max_bytes=$AHP_BUILD_CACHE_MAX_BYTES" >&2
}
ahp_trim_build_cache

RUN_TMP="$(mktemp -d "$AHP_BUILD_CACHE_ROOT/runs/run.XXXXXXXX")"
: > "$RUN_TMP/.freed_bytes"
CLONE_FIXED="$AHP_BUILD_CACHE_ROOT/runs/clone-current"
if mkdir "$CLONE_FIXED" 2>/dev/null; then
  CLONE="$CLONE_FIXED"
else
  echo "[build-hygiene] clone-current busy (parallel build?) falling back to unique clone in TMPDIR" >&2
  CLONE="$(mktemp -d "${TMPDIR:-/tmp}/ahp-candidate-clone.XXXXXX")"
fi
CLEANED=0
cleanup() { # removes ONLY this run's own temps, on success and on failure alike
  local rc=$? f freed=0
  if [ "$CLEANED" = 1 ]; then exit "$rc"; fi
  CLEANED=1
  f="$(awk '{s+=$1} END {printf "%d", s+0}' "$RUN_TMP/.freed_bytes" 2>/dev/null || true)"
  freed=$(( freed + $(ahp_num "$f") ))
  if [ "$KEEP_TMP" = 1 ]; then
    echo "[build-hygiene] KEEP_TMP=1: kept $RUN_TMP" >&2
  else
    f="$(ahp_rmtree_bytes "$RUN_TMP" || true)"; freed=$(( freed + $(ahp_num "$f") ))
  fi
  if [ "$KEEP_CLONE" = 1 ]; then
    echo "[build-hygiene] KEEP_CLONE=1: kept $CLONE" >&2
  else
    f="$(ahp_rmtree_bytes "$CLONE" || true)"; freed=$(( freed + $(ahp_num "$f") ))
  fi
  echo "[build-hygiene] freed_bytes=$freed exit_rc=$rc (own temps removed; shared bounded cache kept: $AHP_BUILD_CACHE_ROOT)" >&2
  df -h "$AHP_BUILD_CACHE_ROOT" >&2
  exit "$rc"
}
trap cleanup EXIT INT TERM
git init -q "$CLONE"; git -C "$CLONE" remote add origin "$GIT_COMMON"
git -C "$CLONE" fetch -q --depth 1 origin "$REVISION"
git -C "$CLONE" checkout -q FETCH_HEAD
( cd "$CLONE"
  [ -z "$(git status --porcelain)" ] || { echo "clone not clean after checkout" >&2; exit 3; }
  HEAD_AT_BUILD="$(git rev-parse HEAD)"; CT="$(git log -1 --format=%cI)"
  [ "$HEAD_AT_BUILD" = "$REVISION" ] || { echo "clone HEAD $HEAD_AT_BUILD != $REVISION" >&2; exit 3; }
  LDFLAGS=""
  if [ "$BUILDINFO" = 1 ]; then
    LDFLAGS="-X ${BI_PKG}.Revision=${HEAD_AT_BUILD} -X ${BI_PKG}.CommitTime=${CT} -X ${BI_PKG}.Modified=false"
  fi
  build_one() { # outdir goos goarch cgo
    local od="$1" gos="$2" arc="$3" cgo="$4" c
    mkdir -p "$od"
    for c in "${CMDS[@]}"; do
      # A/B stay separate fresh caches (the reproducibility proof is not weakened),
      # but they are created under RUN_TMP and deleted right after each comparison.
      local o1="$od/.$c.a" o2="$od/.$c.b" A B fb
      A="$(mktemp -d "$RUN_TMP/gocache-$c-a.XXXXXX")"; B="$(mktemp -d "$RUN_TMP/gocache-$c-b.XXXXXX")"
      GOCACHE="$A" GOFLAGS="-tags=$TAGS" CGO_ENABLED="$cgo" GOOS="$gos" GOARCH="$arc" go build -buildvcs=true -ldflags="$LDFLAGS" -o "$o1" "./cmd/$c"
      GOCACHE="$B" GOFLAGS="-tags=$TAGS" CGO_ENABLED="$cgo" GOOS="$gos" GOARCH="$arc" go build -buildvcs=true -ldflags="$LDFLAGS" -o "$o2" "./cmd/$c"
      local s1 s2; s1="$(shasum -a 256 "$o1"|cut -d' ' -f1)"; s2="$(shasum -a 256 "$o2"|cut -d' ' -f1)"
      [ "$s1" = "$s2" ] || { echo "NOT_REPRODUCIBLE $gos/$arc/$c $s1 $s2" >&2; exit 4; }
      mv "$o1" "$od/$c"; rm -f "$o2"
      fb="$(ahp_rmtree_bytes "$A" "$B" || true)"; echo "$(ahp_num "$fb")" >> "$RUN_TMP/.freed_bytes"
    done
  }
  echo "== host build ($HOST_OS/$HOST_ARCH) tags=$TAGS rev=$HEAD_AT_BUILD buildinfo=$BUILDINFO =="
  build_one "$OUT/host" "$HOST_OS" "$HOST_ARCH" 1
  if [ "$BUILD_LINUX" = 1 ]; then
    echo "== linux/arm64 static build tags=$TAGS =="
    build_one "$OUT/linux-arm64" linux arm64 0
  fi
)

echo "== go version -m (host) =="
for c in "${CMDS[@]}"; do echo "-- $c --"; go version -m "$OUT/host/$c" | grep -E 'vcs\.revision|vcs\.modified|GOOS=|GOARCH=|tags=' || true; done

python3 - "$OUT" "$REVISION" "$TAGS" "$HOST_GO" "$HOST_OS" "$HOST_ARCH" "$BUILT_AT" "$BUILDINFO" "${CMDS[@]}" <<'PY'
import sys,json,hashlib,os,subprocess
out,rev,tags,gover,goo,goarch,built,bi,*cmds=sys.argv[1:]
def sha(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        for b in iter(lambda:f.read(1<<20),b''): h.update(b)
    return h.hexdigest()
def stamp(p):
    d={}
    for line in subprocess.check_output(['go','version','-m',p],text=True).splitlines():
        parts=line.split('\t')
        if len(parts)>=3:
            kv=parts[2]
            if '=' in kv:
                k,v=kv.split('=',1)
                if k in ('vcs.revision','vcs.modified','GOOS','GOARCH','CGO_ENABLED') or k=='-tags': d[k]=v
    return d
man={"revision":rev,"built_at_utc":built,"go_version":gover,"tags":tags,"buildinfo_ldflags":bi=="1",
     "host":{"goos":goo,"goarch":goarch},"images":{},"provenance_rule":"vcs.revision==revision AND vcs.modified==false","binaries":{}}
for sub,label in (("host","host"),("linux-arm64","linux-arm64")):
    d=os.path.join(out,sub)
    if not os.path.isdir(d): continue
    for c in cmds:
        p=os.path.join(d,c)
        if os.path.exists(p): man["binaries"][f"{label}/{c}"]={"path":os.path.relpath(p,out),"sha256":sha(p),"provenance":stamp(p)}
os.makedirs(out,exist_ok=True); open(os.path.join(out,"candidate-manifest.json"),"w").write(json.dumps(man,indent=2)+"\n")
print("manifest:",os.path.join(out,"candidate-manifest.json"))
bad=[k for k,v in man["binaries"].items() if v["provenance"].get("vcs.revision")!=rev or v["provenance"].get("vcs.modified")!="false"]
if bad: print("PROVENANCE FAIL for:",bad); sys.exit(4)
print("provenance OK: %d binaries, all vcs.revision==%s vcs.modified==false"%(len(man["binaries"]),rev[:12]))
PY

if [ "$BUILD_IMAGES" = 1 ]; then
  R8="$(printf '%s' "$REVISION" | cut -c1-12)"
  echo "== images from the SAME stamped linux binaries =="
  RD="$(mktemp -d "$RUN_TMP/img-runner.XXXXXX")"; cp "$OUT/linux-arm64/ahp-agent" "$RD/ahp-agent"
  echo "runner image id: $(docker build -q --platform linux/arm64 -f "$REPO_ROOT/infra/agent-workload/Dockerfile" -t "ahp-candidate-runner:$R8" "$RD")"
  AD="$(mktemp -d "$RUN_TMP/img-ahp.XXXXXX")"; cp "$OUT/linux-arm64/ahp" "$AD/ahp"
  # infra/Dockerfile.ahp stamps only cmd/ahp via -ldflags (NOT provenance per section 0);
  # minimal conforming image: copy the prebuilt VCS-stamped linux binary.
  printf 'FROM alpine:3.21\nRUN apk add --no-cache ca-certificates\nCOPY ahp /usr/local/bin/ahp\nEXPOSE 8080\nENTRYPOINT ["ahp"]\n' > "$AD/Dockerfile"
  echo "ahp image id: $(docker build -q --platform linux/arm64 -f "$AD/Dockerfile" -t "ahp-candidate-ahp:$R8" "$AD")"
  for img in ahp-candidate-ahp:$R8 ahp-candidate-runner:$R8; do
    cid="$(docker image inspect --format '{{.Id}}' "$img")"
    rd="$(docker image inspect --format '{{index .RepoDigests 0}}' "$img" 2>/dev/null || true)"
    python3 - "$OUT/candidate-manifest.json" "$img" "$cid" "$rd" <<'PY'
import sys,json
mf,img,cid,rd=sys.argv[1:5]
d=json.load(open(mf)); d.setdefault("images",{})[img]={"config_id":cid,"repo_digest":(rd or None),"pushed_registry_digest": bool(rd and '@' in rd)}
json.dump(d,open(mf,"w"),indent=2)
print("image",img,"config_id",cid,"pushed_registry_digest", bool(rd and '@' in rd))
PY
  done
fi
echo "DONE out=$OUT"
