#!/usr/bin/env bash
# delivery lane: EMITS (does not load) staged LaunchAgents for the ISOLATED demo:
#   com.ahp.delivery.iso29700  = composed backend (:29700, mTLS, browser, isolated PG)
#   com.ahp.delivery.human29710 = human-origin proxy (:29710, UI + /u-ahp/v1 proxy)
# Secrets/certs/DSN are referenced by PATH from privateenv only (never inlined).
# The director/root loads them from the host shell: launchctl bootstrap gui/501 <plist>
set -euo pipefail
DUR="${AHP_DELIVERY_DUR:-/Users/aogabbasov/AI Agents/ahp/company-brain/private/composed-backend}"
BE_BIN="${AHP_ISO_BIN:-$DUR/bin/ahp-full-iso29700-2dd6a61b6d55}"
BE_PORT="${AHP_ISO_BACKEND_PORT:-29700}"; OR_PORT="${AHP_ISO_ORIGIN_PORT:-29710}"
ENV_FILE="${AHP_ISO_ENV_FILE:-$DUR/.env}"           # isolated PG DSN, browser seed, MinIO bucket-ns, mTLS paths
OUT_DIR="${AHP_ISO_STAGED:-$DUR/staged}"
mkdir -p "$OUT_DIR"
plist() { # name port role
  cat <<PL
<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
 <key>Label</key><string>$1</string>
 <key>WorkingDirectory</key><string>$DUR</string>
 <key>ProgramArguments</key><array>$3</array>
 <key>EnvironmentVariables</key><dict>
   <key>AHP_DELIVERY_ENV_FILE</key><string>$ENV_FILE</string>
   <key>AHP_ISO_COMPOSED_TLS</key><string>1</string>
   <key>AHP_ENABLE_BROWSER</key><string>1</string>
   <key>AHP_BACKEND_PORT</key><string>$2</string>
   <key>AHP_ORIGIN_PORT</key><string>$OR_PORT</string>
 </dict>
 <key>RunAtLoad</key><true/>
 <key>KeepAlive</key><true/>
 <key>StandardOutPath</key><string>$DUR/logs/$1.out.log</string>
 <key>StandardErrorPath</key><string>$DUR/logs/$1.err.log</string>
</dict></plist>
PL
}
mkdir -p "$DUR/logs"
# backend wrapper sources ENV_FILE then execs the bin with role=all + mTLS certs from env
cat > "$OUT_DIR/iso-backend.sh" <<BE
#!/usr/bin/env bash
set -a; . "\$AHP_DELIVERY_ENV_FILE"; set +a
exec "$BE_BIN" --role all --listen ":\$AHP_BACKEND_PORT" \\
  --tls-cert "\$AHP_TLS_SERVER_CERT" --tls-key "\$AHP_TLS_SERVER_KEY" --tls-ca "\$AHP_TLS_CA"
BE
chmod +x "$OUT_DIR/iso-backend.sh"
cat > "$OUT_DIR/iso-origin.sh" <<OR
#!/usr/bin/env bash
set -a; . "\$AHP_DELIVERY_ENV_FILE"; set +a
exec node "$DUR/scripts/human-origin.mjs"
OR
chmod +x "$OUT_DIR/iso-origin.sh"
plist com.ahp.delivery.iso29700 "$BE_PORT" "<string>/bin/bash</string><string>$OUT_DIR/iso-backend.sh</string>" > "$OUT_DIR/com.ahp.delivery.iso29700.plist"
plist com.ahp.delivery.human29710 "$OR_PORT" "<string>/bin/bash</string><string>$OUT_DIR/iso-origin.sh</string>" > "$OUT_DIR/com.ahp.delivery.human29710.plist"
echo "staged plists in $OUT_DIR"
echo "load (host shell, director/root):"
echo "  launchctl bootstrap gui/501 '$OUT_DIR/com.ahp.delivery.iso29700.plist'"
echo "  launchctl bootstrap gui/501 '$OUT_DIR/com.ahp.delivery.human29710.plist'"
