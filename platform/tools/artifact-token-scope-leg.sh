#!/bin/sh
# Task-token scope leg over the artifact plane, measured with a CLIENT CERTIFICATE.
#
# Why the certificate is not optional: /v1/artifacts/* is a workload face. A call
# made with --cacert only (server verification, no client certificate) never
# reaches the authorizer at all - it dies in the TLS handshake and curl answers
# 000. That is what made the previous measurement read as "no scope" when it
# measured nothing. Every artifact call below passes --cert/--key.
#
# Scenario (one subject, three task tokens from the stand's own Identity exchange):
#   tokenA  task A, context CTX          -> writes the artifact (BeginUpload/PUT/Seal)
#   tokenB  task B, context CTX          -> Get + Download: the case in question
#   tokenC  task C, context CTX-other    -> Get + Download: another conversation
# Claims of every token are printed (payload fields only; no signature, no token).
#
#   200 for tokenB  = the scope defect stands, with claims attached
#   403/404 tokenB  = the negative is withdrawn
#   tokenC must be refused in both worlds; a 200 there is a cross-conversation leak.
#
#   usage: sh tools/artifact-token-scope-leg.sh
#          AGG=https://127.0.0.1:18094 PKI=... HUMAN=... USER_ENV=... sh ...
set -eu
HIVE=/home/alex/ahp-hive
AGG=${AGG:-https://127.0.0.1:18080}
PKI=${PKI:-$HIVE/state/stand-pki}
HUMAN=${HUMAN:-http://127.0.0.1:18081}
USER_ENV=${USER_ENV:-$HIVE/env/stand-demo-user.env}
USER_KEY=${USER_KEY:-STAND_USERNAME}
PASS_KEY=${PASS_KEY:-STAND_PASSWORD}
# -k is never used: the CA of the target is passed explicitly by the leg itself.
[ -s "$PKI/client-space.crt" ] || { echo "TOKEN_SCOPE=FAIL (no client certificate in $PKI - the leg cannot be measured without one)"; exit 2; }
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
CTX="tokscope-$STAMP"
CTX2="tokscope-other-$STAMP"
OUT=${OUT:-$HIVE/state/journey-runs/token-scope-$STAMP}
mkdir -p "$OUT"

python3 - "$AGG" "$HUMAN" "$PKI" "$USER_ENV" "$USER_KEY" "$PASS_KEY" "$CTX" "$CTX2" "$STAMP" "$OUT" <<'PY'
import hashlib, json, os, subprocess, sys, time, base64
agg, human, pki, user_env, ukey, pkey, ctx, ctx2, stamp, out = sys.argv[1:11]
MT = ["--cacert", pki + "/ca.crt", "--cert", pki + "/client-space.crt", "--key", pki + "/client-space.key"]
TLSCA = ["--cacert", pki + "/ca.crt"] if human.startswith("https") else []
EV = out + "/evidence.jsonl"

env = dict(os.environ)
for line in open(user_env):
    if "=" in line:
        k, v = line.strip().split("=", 1)
        env[k] = v.strip().strip('"')

def rec(step, detail):
    with open(EV, "a") as f:
        f.write(json.dumps({"ts": time.strftime("%FT%TZ", time.gmtime()), "step": step,
                            "detail": detail}, ensure_ascii=False) + "\n")

def sh(args, binary=None):
    if binary:
        r = subprocess.run(args + ["-o", binary], capture_output=True)
        code = r.stdout.decode(errors="replace").strip()
        return code, ""
    r = subprocess.run(args + ["-w", "\n%{http_code}"], capture_output=True, text=True, env=env)
    body, _, code = r.stdout.rpartition("\n")
    return code, body

def jparse(text):
    try:
        return json.loads(text)
    except Exception:
        return {"_raw": (text or "")[:200]}


def post(op, body, tok, svc="artifacts", mt=True, extra=None, binary=None):
    cmd = ["curl", "-s", "-w", "\n%{http_code}", "--max-time", "30"] + (MT if mt else ["--cacert", pki + "/ca.crt"])
    cmd += ["-X", "POST", "%s/v1/%s/%s" % (agg, svc, op),
            "-H", "Authorization: Bearer " + tok, "-H", "Content-Type: application/json",
            "-d", json.dumps(body)]
    if extra:
        cmd += extra
    if binary:
        cmd = ["curl", "-s", "-o", binary, "-w", "%{http_code}", "--max-time", "60"] + MT + \
              ["-X", "POST", "%s/v1/%s/%s" % (agg, svc, op),
               "-H", "Authorization: Bearer " + tok, "-H", "Content-Type: application/json",
               "-d", json.dumps(body)]
        code, _ = sh(cmd, binary=binary)
        return code, ""
    code, text = sh(cmd)
    return code, jparse(text)

def claims(tok):
    if not tok:
        return {}
    seg = tok.split(".")
    if len(seg) < 2:
        return {"unparsed": True}
    p = seg[1]
    p += "=" * (-len(p) % 4)
    d = json.loads(base64.urlsafe_b64decode(p))
    d.pop("sig", None)
    return {k: d[k] for k in ("aud", "token_kind", "sub", "tenant", "azp", "task", "ctx",
                              "root", "grant", "depth") if k in d}

jar = out + "/cookies.txt"
subprocess.run(["curl", "-s", "-c", jar, "-o", "/dev/null", "--max-time", "15", "-X", "POST",
                human + "/u-ahp/v1/auth/login", "-H", "Content-Type: application/json",
                "-H", "X-AHP-Session: 1"] + TLSCA +
               ["-d", json.dumps({"username": env[ukey], "password": env[pkey]})],
               capture_output=True, text=True, env=env)
code, tb = sh(["curl", "-s", "-b", jar, "--max-time", "15", "-X", "POST",
               human + "/u-ahp/v1/auth/token", "-H", "X-AHP-Session: 1",
               "-H", "Content-Type: application/json", "-d", "{}"] + TLSCA)
human_tok = ""
try:
    human_tok = json.loads(tb).get("access_token", "")
except Exception:
    pass
rec("login", {"token_http": code, "human_claims": claims(human_tok)})
if not human_tok:
    print("TOKEN_SCOPE=FAIL (no human token from %s, http=%s)" % (human, code))
    sys.exit(2)

def exchange(task, context):
    c, b = sh(["curl", "-s", "--max-time", "20"] + MT + ["-X", "POST",
             agg + "/v1/identity/control/exchange", "-H", "Content-Type: application/json",
             "-d", json.dumps({"human_token": human_tok, "task": task, "context": context})])
    return c, jparse(b).get("token", "") if isinstance(jparse(b), dict) else ""

codeA, tokA = exchange("scope-A-" + stamp, ctx)
codeB, tokB = exchange("scope-B-" + stamp, ctx)
codeC, tokC = exchange("scope-C-" + stamp, ctx2)
rec("exchange", {"http": [codeA, codeB, codeC], "claims_A": claims(tokA),
                 "claims_B": claims(tokB), "claims_C": claims(tokC)})
if not (tokA and tokB and tokC):
    print("TOKEN_SCOPE=FAIL (exchange http %s/%s/%s - no task tokens)" % (codeA, codeB, codeC))
    sys.exit(3)

body_bytes = ("token-scope-leg %s subject-token-A conversation %s" % (stamp, ctx)).encode()
sha = hashlib.sha256(body_bytes).hexdigest()
c, up = post("BeginUpload", {"key": "tokscope-A-" + stamp, "name": "scope-A.txt",
                             "mime": "text/plain", "sha256": sha, "size": len(body_bytes),
                             "labels": {}}, tokA)
aid = up.get("id", "") if isinstance(up, dict) else ""
signed = up.get("presigned_url", "") if isinstance(up, dict) else ""
rec("begin_upload", {"http": c, "artifact_id": aid, "context_id": (up or {}).get("context_id", ""),
                     "task_id": (up or {}).get("task_id", "")})
if not signed:
    print("TOKEN_SCOPE=FAIL (BeginUpload http=%s body=%s)" % (c, json.dumps(up)[:160]))
    sys.exit(4)
put = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "30",
                      "-X", "PUT", "-H", "Content-Type: text/plain", "--data-binary", "@" + "-",
                      signed], input=body_bytes, capture_output=True)
rec("put", {"http": put.stdout.decode().strip()})
c, sealed = post("Seal", {"id": aid}, tokA)
rec("seal", {"http": c, "sha256": (sealed or {}).get("sha256", "") if isinstance(sealed, dict) else ""})

# the control: the writing token must read its own artifact
c_own, own = post("Get", {"id": aid}, tokA)
own_bin = out + "/own.bin"
c_own_dl, _ = post("Download", {"id": aid}, tokA, binary=own_bin)
rec("owner_read", {"get_http": c_own, "download_http": c_own_dl,
                   "bytes": os.path.getsize(own_bin) if os.path.exists(own_bin) else -1})

# THE CASE: same subject, another task, the SAME context
c_b, body_b = post("Get", {"id": aid}, tokB)
b_bin = out + "/task-B.bin"
c_b_dl, _ = post("Download", {"id": aid}, tokB, binary=b_bin)
b_bytes = os.path.getsize(b_bin) if os.path.exists(b_bin) else -1
# the verb the foreman named; the platform's metadata verb is Get, so both are measured
c_b_meta, _ = post("GetMetadata", {"id": aid}, tokB)
rec("same_context_other_task", {"get_http": c_b, "getmetadata_http": c_b_meta,
                                "download_http": c_b_dl, "bytes": b_bytes,
                                "object_served": serves_object(b_bin),
                                "get_body_head": json.dumps(body_b)[:200]})

# another conversation, same subject: must be refused in every version of the rule
c_c, body_c = post("Get", {"id": aid}, tokC)
c_bin = out + "/task-C.bin"
c_c_dl, _ = post("Download", {"id": aid}, tokC, binary=c_bin)
c_bytes = os.path.getsize(c_bin) if os.path.exists(c_bin) else -1
rec("other_context", {"get_http": c_c, "download_http": c_c_dl, "bytes": c_bytes,
                      "object_served": serves_object(c_bin),
                      "get_body_head": json.dumps(body_c)[:200]})

# the same call WITHOUT the client certificate, for the record: this is the 000
nocert = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "15",
                         "--cacert", pki + "/ca.crt", "-X", "POST", agg + "/v1/artifacts/Get",
                         "-H", "Authorization: Bearer " + tokB, "-H", "Content-Type: application/json",
                         "-d", json.dumps({"id": aid})], capture_output=True, text=True)
rec("without_client_cert", {"get_http": nocert.stdout.strip() or "000ERR"})

def serves_object(path):
    """A refusal writes the JSON error body to the same file, so byte counts prove
    nothing; the object bytes either appear there or they do not."""
    if not os.path.exists(path):
        return False
    return body_bytes in open(path, "rb").read()


verdict = "OK"
if c_b == "200" or c_b_dl == "200" or serves_object(b_bin):
    verdict = "DEFECT_SAME_CONTEXT_OTHER_TASK_READABLE(get=%s download=%s bytes=%s)" % (c_b, c_b_dl, b_bytes)
elif c_c == "200" or c_c_dl == "200" or serves_object(c_bin):
    verdict = "DEFECT_FOREIGN_CONTEXT_READABLE(get=%s download=%s bytes=%s)" % (c_c, c_c_dl, c_bytes)
elif c_own != "200":
    verdict = "FAIL_OWNER_CANNOT_READ(get=%s download=%s)" % (c_own, c_own_dl)

rec("verdict", {"verdict": verdict, "artifact_id": aid,
                "claims_A": claims(tokA), "claims_B": claims(tokB), "claims_C": claims(tokC),
                "owner": [c_own, c_own_dl], "same_ctx": [c_b, c_b_meta, c_b_dl],
                "other_ctx": [c_c, c_c_dl], "nocert": nocert.stdout.strip() or "000ERR"})
open(out + "/verdict", "w").write(verdict + "\n")
print("TOKEN_SCOPE=%s owner_get=%s owner_dl=%s B_get=%s B_getmetadata=%s B_dl=%s B_bytes=%s C_get=%s C_dl=%s C_bytes=%s nocert=%s" %
      (verdict, c_own, c_own_dl, c_b, c_b_meta, c_b_dl, b_bytes, c_c, c_c_dl, c_bytes,
       nocert.stdout.strip() or "000ERR"))
print("CLAIMS_A=%s" % json.dumps(claims(tokA), ensure_ascii=False, sort_keys=True))
print("CLAIMS_B=%s" % json.dumps(claims(tokB), ensure_ascii=False, sort_keys=True))
print("CLAIMS_C=%s" % json.dumps(claims(tokC), ensure_ascii=False, sort_keys=True))
print("EVIDENCE=%s" % out)
PY
rc=0
[ "$(cat "$OUT/verdict" 2>/dev/null)" = "OK" ] || rc=1
exit $rc
