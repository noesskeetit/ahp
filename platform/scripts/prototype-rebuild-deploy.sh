#!/usr/bin/env bash
# Prototype rebuild + deploy (UNACCEPTED). VM-plane ONLY (Mac disk 15GiB < 20
# DATA_MIN_GIB and no egress; builds and live runs happen on the VM/gigasearch
# plane). This is the canonical rebuild/deploy entrypoint for the delivery +
# execution lanes to (re)build the re-pinned prototype checkout and run its suites.
#
# ORDER IS MANDATORY (director criterion #3): G0 native-Qwen gate runs FIRST and
# is fail-closed. NO `go build` and NO bin start runs if G0 DENIES, because the
# production binary cannot enforce native-only on its own.
#
# Isolation: writes only under isolated HOME / GOCACHE; DB name allowlist
#   ^ahp_hive_[a-z][a-z0-9_]{0,23}$  (rejects everything else, including drop).
# Secrets/env/DSN values are NEVER printed. Native Qwen only; no OpenAI/Astra
# fallback. Requirements source: ahp-agents-space-architecture (2) (1).pdf rev3.2.
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1

echo "== G0 native-Qwen gate (mandatory, before any build/start) =="
bash scripts/native-qwen-gate.sh

echo "== DB-name allowlist =="
: "${AHP_DB_NAME:?AHP_DB_NAME must be an isolated ahp_hive_* database}"
echo "$AHP_DB_NAME" | grep -Eq '^ahp_hive_[a-z][a-z0-9_]{0,23}$' \
  || { echo "DENY: AHP_DB_NAME outside allowlist" >&2; exit 5; }

export GOCACHE="${GOCACHE:-/tmp/ahp-gocache}"
export GOMODCACHE="${GOMODCACHE:-$HOME/go/pkg/mod}"
export GOPROXY="${GOPROXY:-off}"
export GOFLAGS="${GOFLAGS:--mod=mod}"

echo "== build separated-role bins (feature tags ahp_exec, ahp_data) =="
go build -o bin/ahp        ./cmd/ahp
go build -tags ahp_exec,ahp_data -o bin/ahp-tagged ./cmd/ahp
go build -o bin/as-runner  ./cmd/as-runner
echo "build OK"

echo "== suites (start isolated role -> smoke/e2e/chaos/load -> teardown) =="
exec bash scripts/ci-serve-and-test.sh
