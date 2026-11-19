#!/usr/bin/env bash
# Prints immutable-binary + live-process attestation for the isolated demo.
set -uo pipefail
HQ="/Users/aogabbasov/AI Agents/ahp/company-brain"; DUR="$HQ/private/composed-backend"
BIN="${AHP_ISO_BIN:-$DUR/bin/ahp-full-iso29700-2dd6a61b6d55}"
echo "immutable_bin=$BIN"; echo "bin_sha256=$(shasum -a 256 "$BIN" | awk '{print $1}')"
for port in 29700 29710; do
  echo "--- :$port ---"
  PID=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2>/dev/null | head -1 || true)
  if [ -n "${PID:-}" ]; then
    INODE=$(lsof -p "$PID" -Fn 2>/dev/null | awk '/^n/{p=$0} /^\[/{next} /^f0$/{f=1} END{print p}' | tr -d 'n')
    TXT=$(lsof -p "$PID" -FftFn 2>/dev/null | sed -n '/ t/n;p' | head -1)
    echo "pid=$PID  inode=$INODE"
    echo "readiness=$(curl -sk -o /dev/null -w '%{http_code}' --max-time 3 "https://127.0.0.1:$port/healthz" 2>/dev/null) plain=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://127.0.0.1:$port/healthz" 2>/dev/null)"
  else echo "pid=<not-listening>"; fi
done
