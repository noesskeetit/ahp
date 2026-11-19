#!/usr/bin/env bash
# helper23 local IDENTITY-composed stand (DEF-TGW-2 leg): role=all + browser
# identity + control exchange + tools plane on the CONTRACT alias set.
# Own DB ahp_hive_h23i, own ports 29860/29861, own effect log. No secrets printed.
set -u
WT="/Users/aogabbasov/AI Agents/ahp/company-brain/private/mac-migration/product-import/toolgw-wt"
BIN=/tmp/h23-toolgw
FRONT_PORT=29860
MCP_PORT=29861
EFFECT=$BIN/mine-effect-log-i.jsonl
source /tmp/ahp-test-env.sh
DSN=$(printf '%s' "$AHP_TEST_DATABASE_URL" | sed -E 's#/ahp_go\?#/ahp_hive_h23i?#')
CFG='{"connections":{"live":"http://127.0.0.1:'$MCP_PORT'/mcp"},"tools":{"record_note":{"Name":"record_note","Connection":"live","SideEffecting":false,"RequiresApproval":false},"record_note_gated":{"Name":"record_note","Connection":"live","SideEffecting":true,"RequiresApproval":false},"side_effect":{"Name":"record_note","Connection":"live","SideEffecting":true,"RequiresApproval":true},"side_unknown":{"Name":"boom","Connection":"live","SideEffecting":true,"RequiresApproval":true}}}'
VP='{"live":"secret/data/ahp-hive/toolgw/h23"}'
export GOCACHE=/tmp/ahp-gocache GOMODCACHE=/tmp/ahp-modcache GOPATH=/tmp/ahp-gopath GOPROXY=off GOFLAGS=-mod=mod
for p in $FRONT_PORT $MCP_PORT; do
  if lsof -nP -iTCP:$p -sTCP:LISTEN >/dev/null 2>&1; then echo "PORT $p BUSY -- refusing to run"; exit 3; fi
done
cleanup(){ [ -n "${FRONTPID:-}" ] && kill "$FRONTPID" 2>/dev/null; [ -n "${MCPPID:-}" ] && kill "$MCPPID" 2>/dev/null; }
trap cleanup EXIT

# operator provisioning (0600 users file; permissions include tools:approve)
IFILE=$BIN/i-users.json
umask 077
node -e '
const pw=require("crypto").randomBytes(18).toString("base64url");
require("fs").writeFileSync(process.argv[1], JSON.stringify([{id:"h23i-approver-0001",username:"h23iapprover",password:pw,subject:"u-h23i-approver",tenant:"h23ih",display_name:"h23 identity stand approver",roles:["user"],permissions:["space:access","tools","tools:approve"]}]),{mode:0o600});
' "$IFILE"
# harness-only: seed upserts by user_id/username, not by (tenant,subject);
# clear this stand's own approver rows so re-provisioning is idempotent.
docker exec ahp-handbook-go-postgres-1 psql -U ahp_go -d ahp_hive_h23i -q -c \
  "delete from ahp_identity.browser_token_bindings where session_id in (select session_id from ahp_identity.browser_sessions where tenant='h23ih');
   delete from ahp_identity.browser_sessions where tenant='h23ih';
   delete from ahp_identity.browser_grants where user_id in (select user_id from ahp_identity.browser_users where tenant='h23ih');
   delete from ahp_identity.browser_credentials where username='h23iapprover';
   delete from ahp_identity.browser_users where tenant='h23ih';" >/dev/null 2>&1 || true
export AHP_DATABASE_URL="$DSN"
AHP_ROLE=provision-browser-users AHP_PROVISION_USERS_JSON="$(cat "$IFILE")" $BIN/ahp-full >/tmp/h23-toolgw/provision-i.log 2>&1 || { echo "PROVISION FAILED"; tail -3 /tmp/h23-toolgw/provision-i.log; exit 4; }
echo "PROVISIONED ok"

export H23_MCP_BEARER="$(cat $BIN/mcp-bearer.txt)"
$BIN/h23mcp -addr 127.0.0.1:$MCP_PORT -path /mcp -effect-log $EFFECT >$BIN/mine-mcp-i.log 2>&1 &
MCPPID=$!
AHP_ROLE=all AHP_LISTEN=:$FRONT_PORT \
  AHP_TLS_CERT="$WT/infra/certs/server/server.crt" AHP_TLS_KEY="$WT/infra/certs/server/server.key" AHP_TLS_CA="$WT/infra/certs/ca/ca.crt" \
  AHP_DEV_JWT_SEED="$AHP_DEV_JWT_SEED" \
  AHP_ENABLE_BROWSER=1 AHP_ENABLE_CONTROL_EXCHANGE=1 AHP_IDENTITY_EXCHANGER_DEPLOYMENTS=space \
  AHP_ENABLE_TOOLS=1 AHP_TOOLS_SCHEMA=ahp_tools_h23i \
  AHP_TOOLS_CONFIG="$CFG" AHP_TOOLS_VAULT_PATHS="$VP" \
  AHP_VAULT_ADDR="$AHP_VAULT_ADDR" AHP_VAULT_DEV_TOKEN="$AHP_VAULT_DEV_TOKEN" \
  $BIN/ahp-full >$BIN/front-i.log 2>&1 &
FRONTPID=$!

for i in $(seq 1 60); do curl -s -o /dev/null http://127.0.0.1:$MCP_PORT/__health && break; sleep 0.3; done
for i in $(seq 1 80); do curl -sk -o /dev/null --cert $BIN/cert-home/client.crt --key $BIN/cert-home/client.key --cacert "$WT/infra/certs/ca/ca.crt" https://localhost:$FRONT_PORT/readyz && break; sleep 0.3; done
echo "STAND_UP mcp=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$MCP_PORT/__health) front=$(curl -sk -o /dev/null -w '%{http_code}' --cert $BIN/cert-home/client.crt --key $BIN/cert-home/client.key --cacert "$WT/infra/certs/ca/ca.crt" https://localhost:$FRONT_PORT/readyz)"
grep -i "tools plane mounted" $BIN/front-i.log
grep -iE "identity|exchange" $BIN/front-i.log | head -2

PROG="$WT/tests/toolgw-live/scoped-token-provider.mjs"
export AHP_TOOLGW_LIVE=1 AHP_TOOLGW_TOKEN_SOURCE=external AHP_TOOLGW_EVIDENCE=approve
export AHP_DEPLOYED_URL=https://localhost:$FRONT_PORT \
  AHP_DEPLOYED_CA="$WT/infra/certs/ca/ca.crt" AHP_DEPLOYED_CA_DIR="$WT/infra/certs/ca" \
  AHP_TOOLGW_MCP_URL=http://127.0.0.1:$MCP_PORT/__effect \
  AHP_TOOLGW_HOME_TENANT=h23ih AHP_TOOLGW_DEPLOYMENT=space \
  AHP_TOOLGW_FOREIGN_TENANT=h23iforeign AHP_TOOLGW_ALLOW_TENANT_MISMATCH=1 \
  AHP_TOOLGW_CREDS_FILE="$IFILE" \
  AHP_TOOLGW_GEN_CLIENT="$WT/infra/gen-client.sh" \
  AHP_TOOLGW_TASK_TOKEN_CMD="node '$PROG'" AHP_TOOLGW_HUMAN_TOKEN_CMD="node '$PROG'" \
  AHP_TOOLS_DB_URL="$DSN" AHP_TOOLS_SCHEMA=ahp_tools_h23i
cd "$WT"
go test -tags 'ahp_exec ahp_data' -count=2 -v -run TestToolGatewayLive ./tests/registry-triggers/ 2>&1
RC=$?
echo "GO_TEST_RC=$RC"
echo "MCP_OBSERVE:"; curl -s http://127.0.0.1:$MCP_PORT/__effect | head -c 300; echo
exit $RC
