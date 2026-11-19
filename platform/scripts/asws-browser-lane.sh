#!/bin/sh
# LANE ONLY -- not prod, not the director's :183xx window.
#
# Stands up the acceptance DEPLOYMENT SHAPE on this host with this lane's own
# ports, own database, own deployment ids, own CA and own signing seed, and runs
# the browser upload-into-project leg through the user edge:
#
#   browser --HTTP--> human edge --mTLS + exchanged task token--> composed origin
#
# It exists because the acceptance window is down and because the ruling about
# which origin owns a user-authenticated Workspace face is still open: this proves
# the shipped edge code carries the real chain across PROCESSES with the REAL
# identity exchange (internal/identityapi/browser.go), not a test stub.
#
#   sh scripts/asws-browser-lane.sh [path-to-composed-binary]
#
# The binary is not built here: lane bytes are built on VM1 under run/build.lock.
# Nothing on prod ports or another lane's data is touched.
set -eu

REPO=$(cd "$(dirname "$0")/.." && pwd)
BIN=${1:-/dev/shm/asws-lane/composed-lane.bin}
PGBIN=${PGBIN:-/home/alex/ahp-handbook/tools/pg/root/bin}
PGPORT=${AHP_AWSWS_PGPORT:-25471}
PGDATA=${AHP_AWSWS_PGDATA:-/dev/shm/ahp_ws1_pgdata}
PGSOCK=${AHP_AWSWS_PGSOCK:-/dev/shm/ahp_ws1_pg}
PGDB=${AHP_AWSWS_EDGE_DB:-ahp_ws1_edge}
S3PORT=${AHP_AWSWS_S3PORT:-16910}
S3USER=${AHP_MINIO_ROOT_USER:-ahpws1root}
S3PASS=${AHP_MINIO_ROOT_PASSWORD:-ahpws1secret}
BUCKET=${AHP_AWSWS_EDGE_BUCKET:-as-artifacts-aswsbrowserlane}

AGG_PORT=${AHP_AWSWS_AGG_PORT:-18390}
EDGE_PORT=${AHP_AWSWS_EDGE_LISTEN:-18391}
DATA_PORT=${AHP_AWSWS_DATA_PORT:-18392}
RUNNER_STUB_PORT=${AHP_AWSWS_RUNNER_PORT:-18399}
TENANT=${AHP_AWSWS_TENANT:-asws-lane-tenant}
AGG_DEP=${AHP_AWSWS_AGG_DEP:-asws-agg-lane}
EDGE_DEP=${AHP_AWSWS_EDGE_DEP:-asws-edge-lane}

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN=${AHP_AWSWS_LANE_RUN:-/dev/shm/asws-browser-lane-$STAMP}
mkdir -p "$RUN/pki"
EV=$RUN/browser-leg.log

listening(){ (ss -ltn 2>/dev/null || netstat -ltn 2>/dev/null) | grep -q ":$1"; }
# Whose socket is it? cmd/ahp logs "listening" BEFORE it binds (main.go: runServer logs, then
# ListenAndServeTLS), so a process that LOST the port to an older lane still prints the line this
# script waits for -- and the leg then measures the other lane's process while reporting a verdict
# for these bytes. Measured the hard way at 20261101T160426Z: two lane runs sat on :18390/:18391,
# the newer one logged "listening" on both, its own edge served nothing, and the run still printed
# BROWSER_LEG=PASS. Nothing here trusts that log line again.
port_pid(){ ss -ltnp 2>/dev/null | awk -v p=":$1" '$4 ~ p"$"' | sed -n 's/.*pid=\([0-9]*\).*/\1/p' | head -1; }
refuse_taken_port(){
  holder=$(port_pid "$1")
  [ -z "$holder" ] && return 0
  say "FAIL: port $1 (the $2) is already held by pid $holder, so this lane cannot own it"
  say "lane refuses to measure: the leg would run against another deployment's process and report a verdict for these bytes"
  exit 4
}
owns_port(){
  i=0
  while [ "$(port_pid "$1")" != "$2" ]; do
    i=$((i+1)); [ $i -ge 15 ] && return 1
    sleep 1
  done
  return 0
}
say(){ printf '%s\n' "$*" | tee -a "$EV"; }

[ -x "$BIN" ] || { echo "no binary at $BIN (build lane bytes on VM1 first)"; exit 2; }
[ -f "$PGDATA/PG_VERSION" ] || { echo "lane Postgres cluster missing; run scripts/asws-upload-leg.sh first"; exit 2; }
BIN_SHA=$(sha256sum "$BIN" | cut -d' ' -f1)

# Every port this run intends to serve must be free BEFORE the run touches its database: a lane that
# discovers the clash afterwards has already dropped and recreated ahp_ws1_edge under the previous run.
refuse_taken_port "$AGG_PORT" "composed origin"
refuse_taken_port "$EDGE_PORT" "user edge"
[ "${AHP_AWSWS_SPLIT_DATA:-}" = "1" ] && refuse_taken_port "$DATA_PORT" "split data plane"
true

# --- stores: this lane's own database inside its own cluster, own bucket ------
if ! listening "$S3PORT"; then
  echo "MinIO for this lane is not up; run scripts/asws-upload-leg.sh first"; exit 2
fi
DB_URL="postgres://alex@127.0.0.1:$PGPORT/$PGDB?sslmode=disable"
# No psql exists on this host (only initdb, pg_ctl, postgres), so the database is
# created through the single-user backend. That backend takes the cluster lock, so
# this lane's own cluster is cycled for the one statement -- never another lane's,
# and this cluster holds only this lane's tmpfs data.
# The database is dropped and recreated every run, for two reasons the leg learned
# the hard way: the users carry freshly generated passwords each run (a stored row
# would reject them), and the identity authority tree remembers the child task ids
# of the previous run (a second run then answers KEY_REUSE_MISMATCH). Both are
# stale-state artifacts, not product behaviour, so a clean database is the only
# honest substrate for a re-runnable leg.
# Guard: this script may only ever touch a database of THIS lane.
case "$PGDB" in
  ahp_ws1_*) : ;;
  *) echo "refusing to recreate $PGDB: the lane database name must start with ahp_ws1_"; exit 3 ;;
esac
if ! echo "$PGDATA" | grep -q "ahp_ws1"; then
  echo "refusing to run against a Postgres cluster that is not this lane's: $PGDATA"; exit 3
fi
WAS_RUNNING=no
"$PGBIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1 && WAS_RUNNING=yes
[ "$WAS_RUNNING" = yes ] && "$PGBIN/pg_ctl" -D "$PGDATA" -m fast stop >/dev/null 2>&1
printf 'DROP DATABASE IF EXISTS %s;\nCREATE DATABASE %s;\n' "$PGDB" "$PGDB" \
  | "$PGBIN/postgres" --single -D "$PGDATA" -c fsync=off postgres >"$RUN/createdb.log" 2>&1 || true
if grep -qi "error" "$RUN/createdb.log"; then
  say "FAIL: could not recreate $PGDB"; cat "$RUN/createdb.log" | tee -a "$EV"; exit 3
fi
say "database $PGDB recreated for this run (clean authority tree, clean users)"
if ! listening "$PGPORT"; then
  "$PGBIN/pg_ctl" -D "$PGDATA" -l "$RUN/pg.log" \
    -o "-p $PGPORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$PGSOCK -c fsync=off" start >/dev/null
  i=0; until listening "$PGPORT" || [ $i -ge 30 ]; do sleep 1; i=$((i+1)); done
fi

# --- lane PKI: own CA, own workload identities -------------------------------
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$RUN/pki/ca.key" -out "$RUN/pki/ca.crt" \
  -subj "/CN=asws-browser-lane-ca" -days 1 -addext "basicConstraints=critical,CA:TRUE" >/dev/null 2>&1
python3 - "$RUN" "$TENANT" "$EDGE_DEP" "$AGG_DEP" <<'PY'
import datetime, pathlib, sys
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

run, tenant, edge_dep, agg_dep = sys.argv[1:5]
ca_key = serialization.load_pem_private_key(pathlib.Path(run, "pki/ca.key").read_bytes(), None)
ca_cert = x509.load_pem_x509_certificate(pathlib.Path(run, "pki/ca.crt").read_bytes())
now = datetime.datetime.utcnow()

def leaf(name, uri=None, server=False):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    sub = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    san = [x509.IPAddress(__import__("ipaddress").IPv4Address("127.0.0.1")),
           x509.DNSName("localhost")]
    if uri:
        san = [x509.UniformResourceIdentifier(uri)] + san
    cert = (x509.CertificateBuilder().subject_name(sub).issuer_name(ca_cert.subject)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(hours=1))
            .not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName(san), critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False,
                key_encipherment=True, data_encipherment=False, key_agreement=False,
                key_cert_sign=False, crl_sign=False, encipher_only=False, decipher_only=False), critical=True)
            .add_extension(x509.ExtendedKeyUsage([x509.OID_SERVER_AUTH, x509.OID_CLIENT_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256()))
    pathlib.Path(run, "pki/%s.crt" % name).write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    pathlib.Path(run, "pki/%s.key" % name).write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                          serialization.NoEncryption()))

leaf("server")
leaf("edge-client", "spiffe://ahp/tenant/%s/deployment/%s" % (tenant, edge_dep))
leaf("agg-client", "spiffe://ahp/tenant/%s/deployment/%s" % (tenant, agg_dep))
PY
chmod 600 "$RUN"/pki/*.key

# --- seed + two humans, provisioned into the tenant the certificate carries ---
SEED=$(openssl rand -hex 32)
i=0
for who in a b; do
  PASS=$(openssl rand -hex 12)
  printf 'LANE_TENANT=%s\nLANE_SUBJECT=asws-lane-human-%s\nLANE_USERNAME=asws-lane-%s\nLANE_PASSWORD=%s\nLANE_EMAIL=asws-lane-%s@lane.internal\nLANE_USER_ID=asws-lane-%s-1\n' \
    "$TENANT" "$who" "$who" "$PASS" "$who" "$who" > "$RUN/user-$who.env"
  chmod 600 "$RUN/user-$who.env"
done
export AHP_PROVISION_USERS_JSON=$(python3 - "$RUN/user-a.env" "$RUN/user-b.env" <<'PY'
import json, sys
def f(p):
    return dict(l.split("=", 1) for l in open(p).read().split("\n") if "=" in l)
out = []
for p, name in ((sys.argv[1], "Lane A"), (sys.argv[2], "Lane B")):
    d = f(p)
    out.append({"id": d["LANE_USER_ID"], "tenant": d["LANE_TENANT"], "subject": d["LANE_SUBJECT"],
                "display_name": name, "email": d["LANE_EMAIL"], "username": d["LANE_USERNAME"],
                "password": d["LANE_PASSWORD"], "roles": ["agent"],
                # space:access is the exchange entitlement identityapi requires.
                # workspace and artifact:read are the two capabilities this edge
                # asks for, one per boundary; the acceptance deployment grants its
                # humans the same two, so this lane is not inventing a wider human.
                "permissions": ["space:access", "workspace", "artifact:read"]})
print(json.dumps(out))
PY
)

export AHP_DATABASE_URL="$DB_URL" AHP_DEV_JWT_SEED="$SEED"
export AHP_S3_ENDPOINT="http://127.0.0.1:$S3PORT" AHP_S3_REGION="us-east-1"
export AHP_MINIO_ROOT_USER="$S3USER" AHP_MINIO_ROOT_PASSWORD="$S3PASS"
export AHP_S3_BUCKET="$BUCKET"
"$BIN" --role provision-browser-users >> "$RUN/provision.log" 2>&1 \
  || { say "FAIL: provision-browser-users exited non-zero"; tail -5 "$RUN/provision.log" | sed -E 's/(password["\\]*: ?["\\]*)[^"]*/\1<masked>/g' | tee -a "$EV"; exit 7; }
unset AHP_PROVISION_USERS_JSON

# --- 1) the composed origin: data plane + identity, mTLS, single migration point
export AHP_TLS_CERT="$RUN/pki/server.crt" AHP_TLS_KEY="$RUN/pki/server.key" AHP_TLS_CA="$RUN/pki/ca.crt"
export AHP_ROLE=all AHP_LISTEN="127.0.0.1:$AGG_PORT" AHP_ENABLE_BROWSER=1 AHP_BROWSER_AUDIENCE=ahp-browser
export AHP_ENABLE_DATA=1
# The authorization this lane cannot invent for the window: the edge's own
# deployment is the only exchanger this origin accepts.
export AHP_IDENTITY_EXCHANGER_DEPLOYMENTS="$EDGE_DEP"
# Opt-in only, and empty by default: the ruling 20261101T154234Z item 2 shape, in
# which the user edge forwards /v1/workspace/ with the user's own bearer and the
# DATA plane verifies it. Left unset the origin stays task-token-only, exactly as
# the acceptance window's bytes behave today, so the lane cannot silently measure
# a mount nobody decided to turn on.
export AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS="${AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS:-}"
export AHP_WORKSPACE_USER_AUDIENCE="${AHP_WORKSPACE_USER_AUDIENCE:-}"
setsid nohup "$BIN" --role all >> "$RUN/service.log" 2>&1 &
AGG=$!; echo $AGG > "$RUN/service.pid"
i=0; until grep -qF '"msg":"listening"' "$RUN/service.log" 2>/dev/null || [ $i -ge 90 ]; do sleep 1; i=$((i+1)); done
grep -qF '"msg":"listening"' "$RUN/service.log" || { say "FAIL: origin never finished migrations"; tail -8 "$RUN/service.log" | tee -a "$EV"; exit 5; }
owns_port "$AGG_PORT" "$AGG" || { say "FAIL: pid $AGG does not hold :$AGG_PORT (holder=$(port_pid "$AGG_PORT")) -- the log line lied, the bind failed"; exit 5; }

# --- 1b) optional: the data plane as its OWN process --------------------------
# The director's closing criterion asks for the same set "on a THREE-PROCESS stand, not a
# single binary". AHP_AWSWS_SPLIT_DATA=1 runs the data plane (--role data) as a second
# server on its own port, so the browser credential crosses a real process boundary twice:
# edge -> data plane, and edge -> identity for the artifact boundary. Off by default.
DATA_ORIGIN="$AGG_PORT"
if [ "${AHP_AWSWS_SPLIT_DATA:-}" = "1" ]; then
  env AHP_ROLE=data AHP_LISTEN="127.0.0.1:$DATA_PORT" \
    AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS="${AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS:-}" \
    AHP_WORKSPACE_USER_AUDIENCE="${AHP_WORKSPACE_USER_AUDIENCE:-}" \
    setsid nohup "$BIN" --role data --listen "127.0.0.1:$DATA_PORT" >> "$RUN/data-plane.log" 2>&1 &
  DATA_PID=$!; echo $DATA_PID > "$RUN/data-plane.pid"
  i=0; until grep -qF '"msg":"listening"' "$RUN/data-plane.log" 2>/dev/null || [ $i -ge 90 ]; do sleep 1; i=$((i+1)); done
  grep -qF '"msg":"listening"' "$RUN/data-plane.log" || { say "FAIL: the split data plane never came up"; tail -8 "$RUN/data-plane.log" | tee -a "$EV"; exit 9; }
  owns_port "$DATA_PORT" "$DATA_PID" || { say "FAIL: pid $DATA_PID does not hold :$DATA_PORT (holder=$(port_pid "$DATA_PORT"))"; exit 9; }
  DATA_ORIGIN="$DATA_PORT"
fi

# --- 2) the human edge: proxy only, no DATA plane, workload cert --------------
env -u AHP_TLS_CERT -u AHP_TLS_KEY -u AHP_TLS_CA -u AHP_ENABLE_DATA \
  AHP_ROLE=all AHP_LISTEN="127.0.0.1:$EDGE_PORT" AHP_ENABLE_BROWSER=1 AHP_BROWSER_AUDIENCE=ahp-browser \
  AHP_ENABLE_EXECUTION= \
  AHP_SINGLE_ORIGIN_UPSTREAM="http://127.0.0.1:$RUNNER_STUB_PORT" \
  AHP_WORKSPACE_ORIGIN="https://127.0.0.1:$DATA_ORIGIN" \
  AHP_WORKSPACE_EXCHANGE_URL="https://127.0.0.1:$AGG_PORT/v1/identity/control/exchange" \
  AHP_WORKSPACE_CLIENT_CERT="$RUN/pki/edge-client.crt" \
  AHP_WORKSPACE_CLIENT_KEY="$RUN/pki/edge-client.key" \
  AHP_WORKSPACE_CA="$RUN/pki/ca.crt" \
  AHP_WORKSPACE_USER_PASSTHROUGH="${AHP_WORKSPACE_USER_PASSTHROUGH:-}" \
  setsid nohup "$BIN" --role all >> "$RUN/human-edge.log" 2>&1 &
EDGE=$!; echo $EDGE > "$RUN/human-edge.pid"
i=0; until [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "http://127.0.0.1:$EDGE_PORT/healthz" 2>/dev/null)" = "200" ] || [ $i -ge 60 ]; do sleep 1; i=$((i+1)); done
[ $i -lt 60 ] || { say "FAIL: human edge never answered /healthz"; tail -8 "$RUN/human-edge.log" | tee -a "$EV"; exit 8; }
# /healthz answers 200 from ANYTHING on that port, including an older lane's edge -- which is how a
# run at 20261101T160426Z printed BROWSER_LEG=PASS while its own edge served nothing.
owns_port "$EDGE_PORT" "$EDGE" || { say "FAIL: pid $EDGE does not hold :$EDGE_PORT (holder=$(port_pid "$EDGE_PORT")) -- /healthz answered from another process"; exit 8; }

say "== workspace browser lane ($STAMP) LANE-NOT-PROD =="
say "bytes-under-test $BIN sha256=$BIN_SHA"
say "origin https://127.0.0.1:$AGG_PORT pid=$AGG deployment=$AGG_DEP tenant=$TENANT"
say "user edge http://127.0.0.1:$EDGE_PORT pid=$EDGE deployment=$EDGE_DEP (the only authorized exchanger)"
if [ -n "$AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS" ]; then
  say "data plane also verifies user credentials from: $AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS (audience ${AHP_WORKSPACE_USER_AUDIENCE:-ahp-browser})"
else
  say "data plane verifies task tokens only (AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS unset)"
fi
if [ "${AHP_AWSWS_SPLIT_DATA:-}" = "1" ]; then
  say "stand shape: THREE processes -- identity+state :$AGG_PORT, data plane :$DATA_ORIGIN pid $DATA_PID, user edge :$EDGE_PORT pid $EDGE"
else
  say "stand shape: TWO processes -- composed origin :$AGG_PORT, user edge :$EDGE_PORT"
fi
if [ -n "${AHP_WORKSPACE_USER_PASSTHROUGH:-}" ]; then
  say "user edge MODE: forwards the caller's own credential on /v1/workspace/ (ruling item 2); artifact upload boundary still exchanges"
else
  say "user edge MODE: exchanges a narrowed child token on both boundaries (default, measured since 15:40Z)"
fi
say "database $PGDB (:$PGPORT) bucket $BUCKET (:$S3PORT) evidence $RUN"
say "runner upstream http://127.0.0.1:$RUNNER_STUB_PORT is deliberately NOT running: this leg exercises the Workspace and artifact-upload faces, and /v1/space/* is not this lane"
grep -i "workspace prefix and the two artifact" "$RUN/human-edge.log" | tail -1 | tee -a "$EV" || true

python3 - "$RUN" "$EDGE_PORT" "$BIN_SHA" <<'PY'
import http.cookiejar, json, pathlib, subprocess, sys, urllib.error, urllib.request, hashlib, secrets

run, edge_port, bin_sha = sys.argv[1:4]
edge = "http://127.0.0.1:" + edge_port
steps, fails = [], []

def rec(step, **kw):
    steps.append((step, kw))
    line = "STEP %s %s" % (step, " ".join("%s=%s" % kv for kv in kw.items()))
    print(line, flush=True)

def login(who):
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    d = dict(l.split("=", 1) for l in pathlib.Path(run, "user-%s.env" % who).read_text().split("\n") if "=" in l)
    body = json.dumps({"username": d["LANE_USERNAME"], "password": d["LANE_PASSWORD"]}).encode()
    req = urllib.request.Request(edge + "/u-ahp/v1/auth/login", data=body,
                                 headers={"Content-Type": "application/json", "X-AHP-Session": "1"})
    try:
        opener.open(req, timeout=20).read()
    except urllib.error.HTTPError as e:
        rec("login-%s" % who, http=e.code, verdict="FAIL")
        fails.append("login-%s" % who)
        return ""
    tok = ""
    try:
        r = opener.open(urllib.request.Request(edge + "/u-ahp/v1/auth/token", data=b"{}",
                          headers={"Content-Type": "application/json", "X-AHP-Session": "1"}), timeout=20)
        tok = json.loads(r.read()).get("access_token", "")
    except urllib.error.HTTPError as e:
        rec("token-%s" % who, http=e.code, verdict="FAIL")
        fails.append("token-%s" % who)
    return tok

def call(path, token, body):
    req = urllib.request.Request(edge + path, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "Bearer " + token})
    try:
        r = urllib.request.urlopen(req, timeout=60)
        return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")

payload = b"browser upload through the user edge into project context " + secrets.token_bytes(8) + b"\n"
want = hashlib.sha256(payload).hexdigest()

tok_a = login("a")
rec("login-owner", http=200 if tok_a else "ERR", verdict="PASS" if tok_a else "FAIL")
if not tok_a:
    fails.append("login-owner")

code, body = call("/v1/artifacts/BeginUpload", tok_a,
                  {"key": "lane-" + want[:12], "name": "hello.txt", "mime": "text/plain",
                   "sha256": want, "size": len(payload)})
rec("BeginUpload-through-user-edge", http=code, want=200, verdict="PASS" if code == 200 else "FAIL",
    body="" if code == 200 else body[:200])
if code != 200:
    fails.append("BeginUpload")
up = json.loads(body) if code == 200 else {}

put_ok = False
if up.get("presigned_url"):
    put = urllib.request.Request(up["presigned_url"], data=payload, method="PUT")
    try:
        put_ok = urllib.request.urlopen(put, timeout=60).status in (200, 204)
    except urllib.error.HTTPError as e:
        rec("presigned-PUT", http=e.code, verdict="FAIL")
rec("presigned-PUT", http="ok" if put_ok else "ERR", verdict="PASS" if put_ok else "FAIL")
if not put_ok:
    fails.append("presigned-PUT")

code, body = call("/v1/artifacts/Seal", tok_a, {"id": up.get("id", "")})
sealed = json.loads(body) if code == 200 else {}
good = code == 200 and sealed.get("sha256") == want and int(sealed.get("size", -1)) == len(payload)
rec("Seal", http=code, sha256=sealed.get("sha256", ""), size=sealed.get("size", ""),
    want_sha256=want, want_size=len(payload), verdict="PASS" if good else "FAIL")
if not good:
    fails.append("Seal")

code, body = call("/v1/workspace/Create", tok_a, {"workspace": "lane-proj-" + want[:12]})
ws = json.loads(body).get("id", "") if code == 200 else ""
rec("workspace-Create", http=code, workspace=ws, verdict="PASS" if code == 200 else "FAIL")
if code != 200:
    fails.append("workspace-Create")

code, body = call("/v1/workspace/CreateCandidate", tok_a, {"workspace": ws})
cand = json.loads(body).get("id", "") if code == 200 else ""
rec("CreateCandidate", http=code, candidate=cand, verdict="PASS" if code == 200 else "FAIL")
if code != 200:
    fails.append("CreateCandidate")

files = {"candidate_id": cand, "files": [{"path": "hello.txt", "artifact_id": up.get("id", ""),
                                          "mode": 420, "digest": want}]}
code, body = call("/v1/workspace/UpdateCandidateFiles", tok_a, files)
rec("UpdateCandidateFiles-owner", http=code, want=200, verdict="PASS" if code == 200 else "FAIL",
    body="" if code == 200 else body[:200])
if code != 200:
    fails.append("UpdateCandidateFiles-owner")

# A second human, its own login and its own exchange: the refusal must come from
# the authorization gate, and the two projects must be apart.
tok_b = login("b")
rec("login-second-human", http=200 if tok_b else "ERR", verdict="PASS" if tok_b else "FAIL")
code, body = call("/v1/workspace/UpdateCandidateFiles", tok_b, files)
rec("foreign-subject-into-owners-candidate", http=code, want="403/404",
    verdict="PASS" if code in (403, 404) else "FAIL", body="" if code in (403, 404) else body[:200])
if code not in (403, 404):
    fails.append("foreign-subject")

# The bug this deployment found: a second session for the SAME human. The exchange
# reuses one child task per (root, capability set), so a caller that comes back with
# a different context is refused KEY_REUSE_MISMATCH -- which is what hashing the
# bearer did to every user after their first login. Re-login and re-use the same
# authority: it must still be 200.
tok_a2 = login("a")
code, body = call("/v1/workspace/UpdateCandidateFiles", tok_a2, files)
rec("owner-second-session-writes", http=code, want=200, verdict="PASS" if code == 200 else "FAIL",
    body="" if code == 200 else body[:200])
if code != 200:
    fails.append("owner-second-session")

code, body = call("/v1/workspace/ListMembers", tok_b, {"workspace": ws})
rec("foreign-subject-ListMembers", http=code, want="403/404",
    verdict="PASS" if code in (403, 404) else "FAIL", body="" if code in (403, 404) else body[:200])
if code not in (403, 404):
    fails.append("foreign-ListMembers")

# The tiers: the owner adds the second human as editor; an editor may write but
# may not perform an owner-only operation.
# memberRequest is {workspace, subject}: AddMember grants the default role, which
# the store sets to editor (ahp_workspace.members.role DEFAULT 'editor').
code, body = call("/v1/workspace/AddMember", tok_a, {"workspace": ws, "subject": "asws-lane-human-b"})
rec("owner-AddMember", http=code, verdict="PASS" if code == 200 else "FAIL",
    body="" if code == 200 else body[:200])
if code != 200:
    fails.append("owner-AddMember")

code, body = call("/v1/workspace/UpdateCandidateFiles", tok_b, files)
rec("editor-write", http=code, want=200, verdict="PASS" if code == 200 else "FAIL",
    body="" if code == 200 else body[:200])
if code != 200:
    fails.append("editor-write")

code, body = call("/v1/workspace/ListMembers", tok_b, {"workspace": ws})
rec("editor-refused-owner-only-ListMembers", http=code, want=403,
    verdict="PASS" if code == 403 else "FAIL", body="" if code == 403 else body[:200])
if code != 403:
    fails.append("editor-owner-only")

code, body = call("/v1/workspace/Publish", tok_b, {"candidate_id": cand, "expected_head": ""})
rec("editor-refused-publish", http=code, want="403", verdict="PASS" if code == 403 else "FAIL",
    body="" if code == 403 else body[:200])

verdict = "PASS" if not fails else "FAIL"
print("BROWSER_LEG=%s sha256=%s size=%d steps=%d failed=%s bytes=%s origin=https://127.0.0.1:%s edge=%s lane-not-prod"
      % (verdict, want, len(payload), len(steps), "none" if not fails else ",".join(fails),
         bin_sha[:12], edge.split(":")[-1], edge_port))
sys.exit(0 if not fails else 1)
PY
LEG=$?

say "-- authority the edge actually used, from its own log (not the leg's claim) --"
# Spans reach this file in batches, not per request (measured: 0 lines readable at leg end, 19 a few
# seconds later), so the attribution read is polled rather than taken once.
i=0
while :; do
python3 - "$RUN/human-edge.log" > "$RUN/authority.txt" 2>&1 <<'PY'
import json, pathlib, sys, collections
seen = collections.Counter()
warns = 0
served = 0
for line in pathlib.Path(sys.argv[1]).read_text(errors="replace").splitlines():
    try:
        rec = json.loads(line)
    except ValueError:
        continue
    if rec.get("level") == "WARN":
        warns += 1
    attrs = rec.get("attributes") or {}
    target = attrs.get("http.target", "")
    if rec.get("kind") == "server" and (target.startswith("/v1/workspace/") or target.startswith("/u-ahp/")):
        served += 1
    if rec.get("msg", "").startswith("workspace edge: grant narrowed"):
        seen[(rec.get("boundary", "?"), rec.get("requested_task", "?"), rec.get("minted_task", "?"))] += 1
for (boundary, task, minted), n in sorted(seen.items()):
    print("AUTHORITY boundary=%s requested_task=%s minted_task=%s exchanges=%d" % (boundary, task, minted, n))
print("EDGE_WARN_LINES=%d" % warns)
print("EDGE_SERVED_REQUESTS=%d" % served)
PY
  served=$(grep -o 'EDGE_SERVED_REQUESTS=[0-9]*' "$RUN/authority.txt" | tail -1 | cut -d= -f2)
  i=$((i+1))
  { [ "${served:-0}" != "0" ] || [ $i -ge 15 ]; } && break
  sleep 1
done
cat "$RUN/authority.txt" | tee -a "$EV"
# Attribution, not assertion: the counts above come from THIS run's own edge log, so if the leg's
# requests never reached the process this script started, no verdict printed above means anything.
if [ "${served:-0}" = "0" ]; then
  say "FAIL: the edge this run started (pid $(cat "$RUN/human-edge.pid")) served 0 of the leg's requests"
  say "lane refuses to report a verdict: the leg was answered by a socket this run does not own"
  say "LEG_EXIT=7 evidence=$EV"
  exit 7
fi
say "attribution: the edge pid $(cat "$RUN/human-edge.pid") served $served of the leg's requests, read from its own span log after ${i}s of polling"
say "-- two distinct authority prefixes above means no merged root; minted_task is identity's narrowed child --"
say "pids origin=$(cat "$RUN/service.pid") edge=$(cat "$RUN/human-edge.pid") (pid files are this script's own; pin bytes by sha256 above)"
say "LEG_EXIT=$LEG evidence=$EV"
exit $LEG
