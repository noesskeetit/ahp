#!/usr/bin/env bash
# verify-tier-target.sh — reproducible, NON-MUTATING deployment-manifest verifier.
#
# Purpose (acceptance #3 "Tier0/1/2A/2B manifests + target-runtime proof,
# source != deployment"): give the VM-plane / integration-validation a SINGLE
# reproducible command that produces a machine-readable evidence report per tier.
#
# Evidence discipline (see infra/manifests/README.md "Evidence discipline"):
#   * STATIC layer (always, safe on any host): each manifest must be well-formed
#     YAML and declare the kinds/names its runbook promises. L1/L2 STATIC only.
#     It does NOT prove a running deployment.
#   * SERVER-DRY-RUN layer (ONLY when the operator opts in with
#     AHP_TARGET_RUNTIME=1 AND passes --context <ctx>): `kubectl apply
#     --dry-run=server` validates admission against the real API server but does
#     NOT persist objects. Stronger than client dry-run, STILL not L4 rollout
#     proof (no pods run, no latency measured).
#   * L4 target-runtime (apply + rollout + measured State latency + restart/HA)
#     is NEVER claimed by this script; reported PENDING unless the operator runs
#     the apply/verify procedure in the tier README on a real cluster.
#
# It NEVER applies without --dry-run and NEVER touches a cluster unless the
# operator explicitly names a context. Safe to run on a laptop.
set -u
ROOT="${AHP_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
MANI="$ROOT/infra/manifests"
KCTX=""
TARGET="${AHP_TARGET_RUNTIME:-0}"
while [ $# -gt 0 ]; do
  case "$1" in
    --context) KCTX="${2:-}"; shift 2 ;;
    --target) TARGET=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

PY="${PYTHON:-python3}"
SUM_FILE="$(mktemp -t ahp-tier-sum.XXXXXX)"
# Build-hygiene (ENOSPC incident 2026-11-09): remove our own mktemp files on exit,
# whether the run succeeds or fails.
TMP_FILES=("$SUM_FILE")
cleanup_tmp(){ local f; for f in "${TMP_FILES[@]}"; do [ -n "$f" ] && rm -f "$f" 2>/dev/null || true; done; }
trap cleanup_tmp EXIT
overall_rc=0
emit() { printf '%s\n' "$1" >> "$SUM_FILE"; }

echo "== STATIC layer (L1/L2): parse + expected kinds/names =="
"$PY" - "$MANI" "$SUM_FILE" <<'PYEOF' || overall_rc=1
import sys, os, json
mani, sumfile = sys.argv[1], sys.argv[2]
try:
    import yaml
except Exception as e:
    print("pyyaml unavailable:", e); sys.exit(3)
REQ = {
  "tier0":  [("ConfigMap","ahp-rls-kms"),("Deployment","ahp-shared-pool"),
             ("Service","ahp-shared"),("ServiceAccount","ahp-shared")],
  "tier1":  None,
  "tier2a": [("Namespace","ahp"),
             ("Deployment","ahp-state"),("Deployment","ahp-identity"),("Deployment","ahp-model"),
             ("Service","ahp-state"),("Service","ahp-identity"),("Service","ahp-model"),
             ("ServiceAccount","ahp-state"),("ServiceAccount","ahp-identity"),("ServiceAccount","ahp-model")],
  "tier2b": [("HorizontalPodAutoscaler","ahp-state"),("HorizontalPodAutoscaler","ahp-identity"),
             ("HorizontalPodAutoscaler","ahp-model"),
             ("NetworkPolicy","default-deny-ingress"),("NetworkPolicy","ahp-allow-internal"),
             ("NetworkPolicy","ahp-allow-egress-dns"),
             ("PodDisruptionBudget","ahp-state"),("PodDisruptionBudget","ahp-identity"),
             ("PodDisruptionBudget","ahp-model"),
             ("Role","ahp-secret-reader"),
             ("RoleBinding","ahp-state-secret-reader"),("RoleBinding","ahp-identity-secret-reader"),
             ("RoleBinding","ahp-model-secret-reader")],
}
fail = 0
summary = {}
for tier in ["tier0","tier1","tier2a","tier2b"]:
    d = os.path.join(mani, tier)
    if not os.path.isdir(d):
        print("[FAIL] %s: missing dir" % tier); fail = 1; continue
    objs = []
    for fn in sorted(os.listdir(d)):
        if not (fn.endswith(".yaml") or fn.endswith(".yml")):
            continue
        path = os.path.join(d, fn)
        try:
            docs = [x for x in yaml.safe_load_all(open(path)) if x]
        except Exception as e:
            print("[FAIL] %s/%s: YAML parse error: %s" % (tier, fn, e)); fail = 1; continue
        for o in docs:
            kind = o.get("kind"); name = (o.get("metadata") or {}).get("name")
            if not kind or not name:
                print("[FAIL] %s/%s: doc missing kind/metadata.name" % (tier, fn)); fail = 1; continue
            objs.append((kind, name, fn))
    if tier == "tier1":
        for fn in sorted(os.listdir(d)):
            if not fn.endswith(".service"): continue
            txt = open(os.path.join(d, fn)).read()
            miss = [t for t in ["[Unit]","[Service]","ExecStart"] if t not in txt]
            if ("WatchdogSec" not in txt) and ("Type=notify" not in txt) and ("ExecStartPost" not in txt):
                print("[NOTE] %s/%s: no WatchdogSec/Type=notify/ExecStartPost (readiness hook unclear)" % (tier, fn))
            if miss:
                print("[FAIL] %s/%s: missing %s" % (tier, fn, miss)); fail = 1
        summary["tier1"] = {"unit_files": sum(1 for f in os.listdir(d) if f.endswith(".service"))}
        continue
    present = set((k,n) for k,n,_ in objs)
    kinds = set(k for k,_,_ in objs)
    for (rk, rn) in (REQ.get(tier) or []):
        if rn is None:
            if rk not in kinds:
                print("[FAIL] %s: required kind %s absent" % (tier, rk)); fail = 1
        else:
            if (rk, rn) not in present:
                print("[FAIL] %s: required %s/%s absent" % (tier, rk, rn)); fail = 1
    summary[tier] = {"objects": len(objs), "kinds": sorted(kinds)}
with open(sumfile,"a") as f:
    f.write(json.dumps({"static": summary}, sort_keys=True) + "\n")
print("STATIC summary:", json.dumps(summary, sort_keys=True))
sys.exit(fail)
PYEOF

echo
echo "== SERVER-DRY-RUN layer (L3 admission, NOT L4 rollout) =="
if [ "$TARGET" != "1" ]; then
  echo "[PENDING] server-dry-run not requested (set AHP_TARGET_RUNTIME=1 --context <ctx>). L4 target-runtime proof stays OPEN (external VM-plane)."
  emit '{"server_dry_run":"not-requested","l4":"pending"}'
elif ! command -v kubectl >/dev/null 2>&1; then
  echo "[FAIL] AHP_TARGET_RUNTIME=1 but kubectl not installed"; overall_rc=1
elif [ -z "$KCTX" ]; then
  echo "[FAIL] --context <ctx> required with --target (refusing to guess a cluster / touch a neighbor context)"; overall_rc=1
else
  sdr_fail=0
  ERRF="$(mktemp -t ahp-kubeerr.XXXXXX)"
  TMP_FILES+=("$ERRF")
  for f in "$MANI"/tier0/*.yaml "$MANI"/tier2a/*.yaml "$MANI"/tier2b/*.yaml; do
    [ -e "$f" ] || continue
    echo "-- server dry-run: ${f##*/}"
    if kubectl --context "$KCTX" apply --dry-run=server -f "$f" >/dev/null 2>"$ERRF"; then
      echo "   admitted OK"
    else
      echo "   ADMISSION REJECTED:"; sed 's/^/     /' "$ERRF"; sdr_fail=1
    fi
  done
  [ "$sdr_fail" = "1" ] && overall_rc=1
  emit '{"server_dry_run":"run","context_given":true,"l4":"pending-rollout"}'
fi

echo
echo "== L4 target-runtime (rollout + measured State latency + restart/HA) =="
echo "[PENDING] L4 requires 'kubectl apply' + 'rollout status' + latency/restart capture per each tier README, executed on a real cluster by the VM-plane. This verifier NEVER fakes it. source != deployment."

echo
echo "== machine-readable summary =="
cat "$SUM_FILE"

echo
echo "RESULT: static_rc=$overall_rc  (L4 target-runtime: PENDING/external unless captured on a real cluster)"
exit $overall_rc
