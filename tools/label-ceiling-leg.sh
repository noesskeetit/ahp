set -eu
# Label-ceiling leg, required by the foreman (steer 20261031T1102Z) on the basis of PDF rev3.2
# стр.123/614/764: a label set is a CEILING, and a read whose ceiling is narrower than the
# artifact's labels must be refused. Task-id equality is NOT the obligation, so this leg does
# not measure task ids - it measures one subject in two chats with different source labels.
#
# Measured over real HTTP against the ISOLATED composed stand (tools/cascade-stand-up.sh):
# real browser login, real Identity exchange, real run tokens, real sealed bytes. The live
# stand (:18080-:18082) is never contacted. No token, signature or DSN is ever written.
#
#   usage: sh tools/label-ceiling-leg.sh
HIVE=/home/alex/ahp-hive
RUNDIR=${AHP_CASCADE_RUN:-$HIVE/run/delivery-cascade-stand}
PKI="$RUNDIR/pki"
AGG=https://127.0.0.1:${AHP_CASCADE_PORT_AGG:-18094}
HUMAN=https://127.0.0.1:${AHP_CASCADE_PORT_HUMAN:-18095}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT=${OUT:-$RUNDIR/evidence/label-ceiling-$STAMP}
mkdir -p "$OUT"
[ -s "$PKI/ca.crt" ] || { echo "LABEL_CEILING=FAIL (no isolated stand PKI: run tools/cascade-stand-up.sh)"; exit 2; }
set -a; . "$RUNDIR/stand-user.env"; set +a
python3 - "$AGG" "$HUMAN" "$PKI" "$STAMP" "$OUT" "$STAND_USERNAME" "$STAND_PASSWORD" <<'PY' > "$OUT/leg.out" 2>&1
import base64, json, os, subprocess, sys, hashlib, time
agg, human, pki, stamp, out, user, pw = sys.argv[1:8]
MT = ["--cacert", pki+"/ca.crt", "--cert", pki+"/client-work.crt", "--key", pki+"/client-work.key"]
TLSCA = ["--cacert", pki+"/ca.crt"]
EV = out + "/evidence.jsonl"


def rec(step, detail):
    with open(EV, "a") as f:
        f.write(json.dumps({"ts": time.strftime("%FT%TZ", time.gmtime()), "step": step,
                            "detail": detail}, ensure_ascii=False) + "\n")


def sh12(v):
    return hashlib.sha256(v.encode()).hexdigest()[:12] if v else ""


def post(base, op, body, tok, mt=True, binary_to=None, svc=None):
    path = ("/v1/" + svc + (("/" + op) if op else "")) if svc else ("/v1/state/" + op)
    cmd = ["curl", "-s", "--max-time", "60"] + MT + ["-X", "POST", base + path,
           "-H", "Content-Type: application/json"]
    if tok:
        cmd += ["-H", "Authorization: Bearer " + tok]
    cmd += ["-d", json.dumps(body)]
    if binary_to:
        r = subprocess.run(cmd + ["-o", binary_to, "-w", "%{http_code}"], capture_output=True, text=True)
        return (r.stdout or "").strip() or "000ERR"
    r = subprocess.run(cmd + ["-w", "\n%{http_code}"], capture_output=True, text=True)
    b, _, c = r.stdout.rpartition("\n")
    try:
        return c, json.loads(b)
    except Exception:
        return c, {"raw": b[:200]}


def claims_of(tok):
    """Non-secret claim members of a JWT: the verdict is answered with claims, not opinions."""
    try:
        pad = tok.split(".")[1] + "=" * (-len(tok.split(".")[1]) % 4)
        c = json.loads(base64.urlsafe_b64decode(pad))
    except Exception:
        return {}
    return {k: c.get(k, "") for k in ("kind", "sub", "tenant", "azp", "task", "ctx", "deploy")
            if k in c}


jar = out + "/cookies.txt"
subprocess.run(["curl", "-s", "-c", jar, "-o", "/dev/null", "--max-time", "20"] + TLSCA +
               ["-X", "POST", human + "/u-ahp/v1/auth/login", "-H", "Content-Type: application/json",
                "-H", "X-AHP-Session: 1", "-d", json.dumps({"username": user, "password": pw})],
               capture_output=True, text=True)
r = subprocess.run(["curl", "-s", "-b", jar, "--max-time", "20"] + TLSCA +
                   ["-X", "POST", human + "/u-ahp/v1/auth/token", "-H", "X-AHP-Session: 1",
                    "-H", "Content-Type: application/json", "-d", "{}"],
                   capture_output=True, text=True)
human_tok = ""
try:
    human_tok = json.loads(r.stdout).get("access_token", "")
except Exception:
    pass
if not human_tok:
    print("LABEL_CEILING=FAIL (browser login)")
    sys.exit(2)
rec("login", {"http": 200, "username": user, "token_sha12": sh12(human_tok)})


def chat(name):
    """One conversation of one subject: a thread, a run under it, and the run's own token.

    ClaimRun is a deployment-wide queue with no selector, so foreign pending runs are
    drained (each released with its own token) until this leg holds the run it created.
    """
    ctx = "ceiling-%s-%s" % (name, stamp)
    code, ex = post(agg, "", {"human_token": human_tok, "task": "ceiling-%s-%s" % (name, stamp),
                              "context": ctx}, "", svc="identity/control/exchange")
    tok = ex.get("token", "") if isinstance(ex, dict) else ""
    if not tok:
        print("LABEL_CEILING=FAIL (exchange chat %s http=%s)" % (name, code))
        sys.exit(3)
    code, th = post(agg, "CreateThread", {"context_id": ctx, "key": "k-" + ctx}, tok)
    thread_id = th.get("id", "") if isinstance(th, dict) else ""
    if not thread_id:
        print("LABEL_CEILING=FAIL (CreateThread chat %s http=%s)" % (name, code))
        sys.exit(4)
    code, rn = post(agg, "CreateRun", {"thread_id": thread_id, "key": "k-run-" + ctx,
                                       "payload": {}}, tok)
    run_id = rn.get("id", "") if isinstance(rn, dict) else ""
    if not run_id:
        print("LABEL_CEILING=FAIL (CreateRun chat %s http=%s)" % (name, code))
        sys.exit(5)
    run_tok, lease_run = "", ""
    for _ in range(12):
        code, cl = post(agg, "ClaimRun", {"worker": "ceiling-worker", "ttl_ms": 60000}, "")
        run_tok = cl.get("task_token", "") if isinstance(cl, dict) else ""
        lease = (cl.get("lease") or {}) if isinstance(cl, dict) else {}
        lease_run = (lease.get("run") or {}).get("id", "")
        if not lease_run:
            break
        if lease_run == run_id:
            break
        rel, _ = post(agg, "Release", {"run_id": lease_run, "worker": "ceiling-worker",
                                       "epoch": lease.get("epoch", 0), "status": "failed"}, run_tok)
        rec("claim_drain_foreign", {"chat": name, "leased_run": lease_run, "release_http": rel})
    if lease_run != run_id:
        print("LABEL_CEILING=FAIL (chat %s claimed %s, not its own run %s)" % (name, lease_run, run_id))
        sys.exit(6)
    rec("chat", {"chat": name, "context_id": ctx, "thread_id": thread_id, "run_id": run_id,
                 "run_token_sha12": sh12(run_tok), "claims": claims_of(run_tok)})
    return {"ctx": ctx, "thread": thread_id, "run": run_id, "tok": run_tok}


def upload(owner, labels, text, tag):
    """One upload per (chat, tag): the idempotency key is unique per artifact, because a
    reused key answers KEY_REUSE_MISMATCH and the control below would measure nothing."""
    body = text.encode()
    sha = hashlib.sha256(body).hexdigest()
    code, up = post(agg, "BeginUpload", {"key": "k-%s-%s" % (owner["ctx"], tag), "name": "ceiling-%s.txt" % tag,
                                         "mime": "text/plain", "sha256": sha, "size": len(body),
                                         "labels": labels}, owner["tok"], svc="artifacts")
    aid = up.get("id", "") if isinstance(up, dict) else ""
    signed = up.get("presigned_url", "") if isinstance(up, dict) else ""
    rec("begin_upload", {"chat": owner["ctx"], "tag": tag, "labels": labels, "http": code, "artifact_id": aid,
                         "body_head": "" if aid else json.dumps(up)[:200]})
    if not aid:
        return "", {}
    plain = out + "/payload-%s.txt" % abs(hash(owner["ctx"]))
    open(plain, "w").write(text)
    put = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "60",
                          "-X", "PUT", "-T", plain, signed], capture_output=True, text=True)
    code, sealed = post(agg, "Seal", {"id": aid}, owner["tok"], svc="artifacts")
    rec("seal", {"chat": owner["ctx"], "artifact_id": aid, "put_http": (put.stdout or "").strip(),
                 "seal_http": code, "body_head": json.dumps(sealed)[:200]})
    return aid, sealed


a = chat("a")
b = chat("b")

# 1) the artifact of chat A, carrying chat A as its source label (a private fact of one chat)
private_text = "приватный факт чата A %s\n" % stamp
aid, _ = upload(a, {"workspaces": ["chat-" + a["ctx"]]}, private_text, "private")
if not aid:
    print("LABEL_CEILING=FAIL (chat A upload refused)")
    sys.exit(7)
code_a_get, meta = post(agg, "Get", {"id": aid}, a["tok"], svc="artifacts")
code_a_dl = post(agg, "Download", {"id": aid}, a["tok"], svc="artifacts", binary_to=out + "/owner.bin")
rec("owner_read", {"get_http": code_a_get, "download_http": code_a_dl,
                   "labels": (meta or {}).get("labels", {}) if isinstance(meta, dict) else {},
                   "object_served": os.path.exists(out + "/owner.bin") and
                   private_text.encode() in open(out + "/owner.bin", "rb").read()})

# 2) the same subject, another chat, a narrower ceiling: what does the read path answer?
code_b_get, bbody = post(agg, "Get", {"id": aid}, b["tok"], svc="artifacts")
code_b_dl = post(agg, "Download", {"id": aid}, b["tok"], svc="artifacts", binary_to=out + "/other.bin")
other_serves = os.path.exists(out + "/other.bin") and private_text.encode() in open(out + "/other.bin", "rb").read()
rec("narrower_chat_read", {"get_http": code_b_get, "download_http": code_b_dl,
                           "bytes": os.path.getsize(out + "/other.bin") if os.path.exists(out + "/other.bin") else -1,
                           "object_served": other_serves,
                           "get_body_head": json.dumps(bbody)[:200]})

# 3) control: a label naming ANOTHER subject must stay unreadable for this subject
foreign_text = "чужая приватная строка %s\n" % stamp
tok_a_for_foreign = a["tok"]
fid, _ = upload(a, {"subjects": ["someone-else-" + stamp]}, foreign_text, "foreign")
if fid:
    code_f_get, fbody = post(agg, "Get", {"id": fid}, tok_a_for_foreign, svc="artifacts")
    rec("foreign_subject_control", {"get_http": code_f_get,
                                    "get_body_head": json.dumps(fbody)[:200]})
else:
    code_f_get = "UPLOAD_REFUSED"

# 4) an equal ceiling must still read, and an unlabeled artifact answers the same way across
#    chats as a labeled one does: that is what tells the reader WHICH gate refused - the label
#    ceiling or the context binding. Without this pair a 403 could be claimed as either.
plain_text = "общая строка чата A %s\n" % stamp
wid, _ = upload(a, {}, plain_text, "plain")
code_w = "SKIPPED"
code_unlabeled_other = "SKIPPED"
if wid:
    code_w = post(agg, "Download", {"id": wid}, a["tok"], svc="artifacts", binary_to=out + "/plain.bin")
    code_unlabeled_other = post(agg, "Get", {"id": wid}, b["tok"], svc="artifacts")
    rec("equal_ceiling_read", {"artifact_id": wid, "download_http": code_w,
                               "object_served": os.path.exists(out + "/plain.bin") and
                               plain_text.encode() in open(out + "/plain.bin", "rb").read(),
                               "other_chat_get_http": code_unlabeled_other})

GONE = ("403", "404")
verdict = "PASS"
if not (os.path.exists(out + "/owner.bin") and private_text.encode() in open(out + "/owner.bin", "rb").read()):
    verdict = "FAIL_OWNER_READ_REFUSED(get=%s download=%s)" % (code_a_get, code_a_dl)
elif code_f_get not in GONE and code_f_get != "UPLOAD_REFUSED":
    # the subject ceiling is the floor under everything else; if it leaks, nothing above holds
    verdict = "FAIL_SUBJECT_CEILING_BROKEN(get=%s)" % code_f_get
elif code_w not in ("200",):
    verdict = "FAIL_EQUAL_CEILING_REFUSED(download=%s)" % code_w
elif code_b_get not in GONE or code_b_dl not in GONE or other_serves:
    verdict = "FAIL_LABEL_CEILING_NOT_ENFORCED(get=%s download=%s object_served=%s)" % (
        code_b_get, code_b_dl, other_serves)
rec("verdict", {"verdict": verdict, "artifact_id": aid, "chat_a": a["ctx"], "chat_b": b["ctx"],
                "chat_a_claims": claims_of(a["tok"]), "chat_b_claims": claims_of(b["tok"]),
                "narrower_chat_get": code_b_get, "narrower_chat_download": code_b_dl,
                "narrower_chat_object_served": other_serves,
                "foreign_subject_control": code_f_get, "equal_ceiling_download": code_w,
                "unlabeled_other_chat_get": code_unlabeled_other})
open(out + "/verdict", "w").write(verdict + "\n")
print("LABEL_CEILING=%s owner_get=%s owner_dl=%s narrower_get=%s narrower_dl=%s object_served=%s foreign_subject=%s equal_ceiling=%s unlabeled_other_chat_get=%s"
      % (verdict, code_a_get, code_a_dl, code_b_get, code_b_dl, other_serves, code_f_get, code_w,
         code_unlabeled_other))
PY
rc=0
[ "$(cat "$OUT/verdict" 2>/dev/null)" = "PASS" ] || rc=1
echo "LABEL_CEILING_EVIDENCE=$OUT"
exit $rc
