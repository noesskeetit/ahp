#!/usr/bin/env bash
# G0 native-Qwen provider-identity gate — DELIVERY wiring (iv criterion #3).
#
# Why: internal/core/openai_compat.go is fail-closed on a missing base URL but
# carries NO host allowlist, so "native Qwen only / no OpenAI/Astra fallback" is
# NOT a wire fact the production binary can enforce. It is enforced ONLY at this
# external layer. Therefore NO prototype-checkout bin may start until this gate
# passes. The canonical, INDEPENDENT gate implementation lives in the
# integration-validation lane:
#   acceptance/integration-validation/scripts/native-qwen-assert.sh
# This wrapper only LOCATES and EXECUTES it (fail-closed). It never reads or
# prints any env value, key, token, DSN or URL secret.
#
# Location order: $AHP_NATIVE_ASSERT override => iv-lane paths relative to this
# checkout => DENY if the gate file is absent (a missing gate is DENY).
#
# Inputs (env, never printed): AHP_LLM_BASE_URL, AHP_LLM_MODEL,
#   AHP_NATIVE_QWEN_HOSTS (VM-injected native host allowlist; unset => DENY).
# Exit: 0 = ALLOW (provably native); non-zero = DENY (caller MUST NOT start bin).
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cands=()
[ -n "${AHP_NATIVE_ASSERT:-}" ] && cands+=("$AHP_NATIVE_ASSERT")
cands+=("$ROOT/scripts/native-qwen-assert.sh")   # vendored in product repo (DEF-H10-2 fix)
cands+=("$ROOT/../../integration-validation/scripts/native-qwen-assert.sh")
cands+=("$ROOT/acceptance/integration-validation/scripts/native-qwen-assert.sh")
cands+=("/Users/aogabbasov/AI Agents/ahp/company-brain/acceptance/integration-validation/scripts/native-qwen-assert.sh")
GATE=""
for c in "${cands[@]}"; do [ -f "$c" ] && { GATE="$c"; break; }; done
if [ -z "$GATE" ]; then
  echo "G0 DENY: native-qwen-assert.sh not found (set AHP_NATIVE_ASSERT); a missing gate is DENY, no bin start." >&2
  exit 1
fi
bash "$GATE"; rc=$?
if [ "$rc" -ne 0 ]; then
  echo "G0 DENY: native-Qwen assertion failed (rc=$rc); refusing to start checkout bin (no OpenAI/Astra fallback permitted)." >&2
  exit "$rc"
fi
echo "G0 ALLOW: native-Qwen provider identity asserted (scheme+host+model only)." >&2
