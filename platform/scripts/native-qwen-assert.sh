#!/usr/bin/env bash
# Independent native-Qwen provider-identity assertion (iv lane).
# Implements director-accepted acceptance requirement (forum
# 20261029T164458Z-... §2): "native Qwen only" is a RUNTIME assertion, not a
# wire fact. internal/core/openai_compat.go validates only http(s):// prefix of
# AHP_LLM_BASE_URL, so a green OpenAI-compat round-trip proves reachability to
# ANY host, NOT native-Qwen-only / no-OpenAI/Astra-fallback.
#
# This gate is FAIL-CLOSED and SECRET-FREE. It prints ONLY scheme+host+model.
# It never reads or prints an API key/token. It makes NO network call here
# (a probe variant lives in run-crosscut-e2e.sh).
#
# Classification is kept in lock-step with the canonical always-on Go gate
# tests/space/native_qwen_test.go::classifyNativeQwen at canonical Space
# functional tip 66bbf41 (no build-tag => compiled & run on every `go test`):
#   - require non-empty base+model (fail, never skip)
#   - require http(s) scheme with a hostname            (v1.1: added scheme check)
#   - reject foreign-provider host markers             (no OpenAI/Astra fallback)
#   - require host in the native allowlist             (DENY if allowlist unset)
#   - require model in the native model allowlist
#
# Inputs (env, never printed verbatim):
#   AHP_LLM_BASE_URL  e.g. https://<id>.modelrun.inference.cloud.ru/v1
#   AHP_LLM_MODEL     e.g. model-run-tqmbu-road
#   AHP_NATIVE_QWEN_HOSTS  comma-separated native host allowlist, injected at
#                          VM-plane from trusted config. UNSET => DENY.
# Exit 0 = ALLOW (native), 1 = DENY (not provably native).
set -u

SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

# Director §2 verbatim native model allowlist (mirrors nativeQwenModels @66bbf41).
MODEL_ALLOWLIST="model-run-tqmbu-road qwen-38-27b-fp8"
# Known foreign providers that MUST be blocked even if operator misconfigures.
# Superset of Go foreignProviderHostMarkers {openai.com, openai.azure.com, astra}.
FOREIGN_DENYLIST="openai.com astra generativelanguage googleapis.com anthropic.com azure-api.net bedrock-runtime cohere.com mistral"

sanitize_url() { # $1=url -> scheme://host[:port]/path (userinfo+query stripped)
  python3 - "$1" <<'PY'
import sys
from urllib.parse import urlsplit
u=urlsplit(sys.argv[1])
host=u.hostname or ""
port=f":{u.port}" if u.port else ""
print(f"{u.scheme}://{host}{port}{u.path}")
PY
}

host_of() {
  python3 - "$1" <<'PY'
import sys
from urllib.parse import urlsplit
print((urlsplit(sys.argv[1]).hostname or "").lower())
PY
}

scheme_of() {
  python3 - "$1" <<'PY'
import sys
from urllib.parse import urlsplit
print((urlsplit(sys.argv[1]).scheme or "").lower())
PY
}

deny() { echo "NATIVE-QWEN-ASSERT: DENY ($1)"; exit 1; }

selftest() {
  local pass=0 fail=0
  run_case() { # $1 desc  $2 exp(ALLOW|DENY)  $3 base_url  $4 model  $5 native_hosts
    local desc="$1" exp="$2" bu="$3" mo="$4" nh="$5" out rc verdict
    out=$( AHP_LLM_BASE_URL="$bu" AHP_LLM_MODEL="$mo" AHP_NATIVE_QWEN_HOSTS="$nh" \
           "$SELF" 2>&1 ); rc=$?
    verdict="DENY"; [ $rc -eq 0 ] && verdict="ALLOW"
    if [ "$verdict" = "$exp" ]; then echo "  ok  : $desc (=> $verdict)"; pass=$((pass+1));
    else echo "  FAIL: $desc (want $exp got $verdict) :: $out"; fail=$((fail+1)); fi
  }
  local NAT="https://0038df35-f6e5-4958-a4ec-0b289a0b0aef.modelrun.inference.cloud.ru/v1"
  echo "=== native-qwen-assert selftest (synthetic, no secrets/network) ==="
  # --- synthetic allowlist (unchanged, env-driven) ---
  run_case "native host + native model"        ALLOW "https://qwen.internal.lan/v1" "model-run-tqmbu-road" "qwen.internal.lan"
  run_case "native host + qwen38 model"        ALLOW "https://qwen.internal.lan/v1" "qwen-38-27b-fp8"       "qwen.internal.lan"
  run_case "OPENAI host, native model"         DENY  "https://api.openai.com/v1"    "model-run-tqmbu-road" "qwen.internal.lan"
  run_case "foreign host, foreign model"       DENY  "https://api.openai.com/v1"    "gpt-4o"               "qwen.internal.lan"
  run_case "astra host"                        DENY  "https://astra.corp/v1"        "model-run-tqmbu-road" "qwen.internal.lan"
  run_case "unknown host not in allowlist"     DENY  "https://evil.example/v1"      "model-run-tqmbu-road" "qwen.internal.lan"
  run_case "native host but model not native"  DENY  "https://qwen.internal.lan/v1" "gpt-4o"               "qwen.internal.lan"
  run_case "native allowlist UNSET"            DENY  "https://qwen.internal.lan/v1" "model-run-tqmbu-road" ""
  # --- canonical cases mirrored from native_qwen_test.go @66bbf41 (real allowlist) ---
  run_case "canon native+road ok"      ALLOW "$NAT" "model-run-tqmbu-road" "modelrun.inference.cloud.ru"
  run_case "canon native+38b ok"       ALLOW "$NAT" "qwen-38-27b-fp8"      "modelrun.inference.cloud.ru"
  run_case "canon foreign openai"      DENY  "https://api.openai.com/v1"    "model-run-tqmbu-road" "modelrun.inference.cloud.ru"
  run_case "canon foreign astra"       DENY  "https://astra.internal/v1"    "model-run-tqmbu-road" "modelrun.inference.cloud.ru"
  run_case "canon native host gpt-4o"  DENY  "$NAT" "gpt-4o"               "modelrun.inference.cloud.ru"
  run_case "canon non-native host"     DENY  "https://api.example.com/v1"   "model-run-tqmbu-road" "modelrun.inference.cloud.ru"
  run_case "canon empty base"          DENY  ""                             "model-run-tqmbu-road" "modelrun.inference.cloud.ru"
  run_case "canon empty model"         DENY  "$NAT" ""                      "modelrun.inference.cloud.ru"
  run_case "canon not http (ftp)"      DENY  "ftp://0038df35.modelrun.inference.cloud.ru" "model-run-tqmbu-road" "modelrun.inference.cloud.ru"
  echo "=== selftest: $pass pass / $fail fail ==="
  [ $fail -eq 0 ]
}

[ "${1:-}" = "--selftest" ] && { selftest; exit $?; }

# --- real gate ---
[ -n "${AHP_LLM_BASE_URL:-}" ] || deny "AHP_LLM_BASE_URL unset; cannot prove native"
[ -n "${AHP_LLM_MODEL:-}" ]    || deny "AHP_LLM_MODEL unset; cannot prove native"
MODEL=$(printf '%s' "$AHP_LLM_MODEL" | tr 'A-Z' 'a-z')
SCHEME=$(scheme_of "$AHP_LLM_BASE_URL")
[ "$SCHEME" = "http" ] || [ "$SCHEME" = "https" ] || deny "scheme '$SCHEME' is not http(s); cannot prove native"
HOST=$(host_of "$AHP_LLM_BASE_URL")
[ -n "$HOST" ] || deny "no hostname parsed from AHP_LLM_BASE_URL"

# 1) model must be native
m_ok=0
for m in $MODEL_ALLOWLIST; do [ "$MODEL" = "$m" ] && m_ok=1; done
[ $m_ok -eq 1 ] || deny "AHP_LLM_MODEL '$AHP_LLM_MODEL' not in native allowlist"

# 2) host must not match a foreign provider
for f in $FOREIGN_DENYLIST; do
  case "$HOST" in *"$f"*) deny "host '$HOST' matches foreign provider '$f'" ;; esac
done

# 3) native allowlist must be provided (else cannot positively prove native)
[ -n "${AHP_NATIVE_QWEN_HOSTS:-}" ] || deny "AHP_NATIVE_QWEN_HOSTS allowlist not provided"

# 4) host must be in the native allowlist
h_ok=0
IFS=',' read -ra NH <<< "${AHP_NATIVE_QWEN_HOSTS:-}"
for n in "${NH[@]:-}"; do
  n=$(printf '%s' "$n" | tr -d ' ' | tr 'A-Z' 'a-z')
  [ -n "$n" ] && { case "$HOST" in "$n"|*".$n") h_ok=1 ;; esac; }
done
[ $h_ok -eq 1 ] || deny "host '$HOST' not in native allowlist"

echo "NATIVE-QWEN-ASSERT: ALLOW host=$(sanitize_url "$AHP_LLM_BASE_URL") model=$AHP_LLM_MODEL"
exit 0
