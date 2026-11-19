set -eu
# The deletion cascade, measured end to end over HTTP against the ISOLATED composed stand
# (tools/cascade-stand-up.sh): its own ports, its own database, its own PKI. The live stand is
# never contacted. Nothing here is asserted from source - every claim is an HTTP code and a byte
# count, and no token, signature or DSN is ever written.
#
# It measures, in this order:
#   1) the artifact row carries the support the PDF names (the HTTP face shows its
#      tenant/deployment/subject AND context_id/task_id, taken from the verified run token);
#   2) State's DeleteThread makes the artifact unreadable AND undownloadable, with no agent hook;
#   3) a freshly issued task token cannot resurrect it (re-issuing credentials is not a repair).
#
#   usage: sh tools/cascade-stand-leg.sh
HIVE=/home/alex/ahp-hive
RUNDIR=${AHP_CASCADE_RUN:-$HIVE/run/delivery-cascade-stand}
PKI="$RUNDIR/pki"
AGG=https://127.0.0.1:${AHP_CASCADE_PORT_AGG:-18094}
HUMAN=https://127.0.0.1:${AHP_CASCADE_PORT_HUMAN:-18095}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
CTX="cascade-iso-$STAMP"
OUT=${OUT:-$RUNDIR/evidence/cascade-iso-$STAMP}
mkdir -p "$OUT"
[ -s "$PKI/ca.crt" ] || { echo "CASCADE_STAND_ISO=FAIL (no isolated stand PKI: run tools/cascade-stand-up.sh)"; exit 2; }
set -a; . "$RUNDIR/stand-user.env"; set +a
python3 - "$AGG" "$HUMAN" "$PKI" "$CTX" "$STAMP" "$OUT" "$STAND_USERNAME" "$STAND_PASSWORD" <<'PY' > "$OUT/leg.out" 2>&1
import json, os, subprocess, sys, hashlib, time
agg, human, pki, ctx, stamp, out, user, pw = sys.argv[1:9]
MT = ["--cacert", pki + "/ca.crt", "--cert", pki + "/client-work.crt", "--key", pki + "/client-work.key"]
TLSCA = ["--cacert", pki + "/ca.crt"]
EV = out + "/evidence.jsonl"


def rec(step, detail):
    with open(EV, "a") as f:
        f.write(json.dumps({"ts": time.strftime("%FT%TZ", time.gmtime()), "step": step,
                            "detail": detail}, ensure_ascii=False) + "\n")


def sh12(v):
    return hashlib.sha256(v.encode()).hexdigest()[:12] if v else ""


def serves_object(path):
    """True when the file holds the sealed object. A refusal writes the JSON error
    body to the same file, so counting bytes measures nothing - the object either
    appears or it does not. Used by the scope pair, the cascade and the re-issue."""
    if not os.path.exists(path):
        return False
    return body.encode() in open(path, "rb").read()


def post(base, op, body, tok, mt=True, binary_to=None, svc=None):
    # "…/exchange/" is a different route to Go's mux than "…/exchange" and answers
    # 404, which reads as "the stand has no exchange".
    path = ("/v1/" + svc + (("/" + op) if op else "")) if svc else ("/v1/state/" + op)
    url = base + path
    cmd = ["curl", "-s", "--max-time", "60"] + (MT if mt else TLSCA) + ["-X", "POST", url,
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


# 1) a real human login on the browser edge, then a real Identity exchange for a task token
jar = out + "/cookies.txt"
subprocess.run(["curl", "-s", "-c", jar, "-o", "/dev/null", "--max-time", "20"] + TLSCA +
               ["-X", "POST", human + "/u-ahp/v1/auth/login", "-H", "Content-Type: application/json",
                "-H", "X-AHP-Session: 1", "-d", json.dumps({"username": user, "password": pw})],
               capture_output=True, text=True)
r = subprocess.run(["curl", "-s", "-b", jar, "--max-time", "20"] + TLSCA +
                   ["-X", "POST", human + "/u-ahp/v1/auth/token", "-H", "X-AHP-Session: 1",
                    "-H", "Content-Type: application/json", "-d", "{}"],
                   capture_output=True, text=True)
try:
    human_tok = json.loads(r.stdout).get("access_token", "")
except Exception:
    human_tok = ""
if not human_tok:
    print("CASCADE_STAND_ISO=FAIL (browser login)")
    sys.exit(2)
rec("login", {"http": 200, "username": user, "token_sha12": sh12(human_tok)})

code, ex = post(agg, "", {"human_token": human_tok, "task": "cascade-iso-" + stamp, "context": ctx},
                "", svc="identity/control/exchange")
task_tok = ex.get("token", "") if isinstance(ex, dict) else ""
rec("exchange", {"http": code, "token_sha12": sh12(task_tok)})
if not task_tok:
    print("CASCADE_STAND_ISO=FAIL (no task token from real Identity, http=%s)" % code)
    sys.exit(3)

# 2) a thread and a run, then the run's OWN token: that is the token the live chain writes with
code, th = post(agg, "CreateThread", {"context_id": ctx, "key": "k-" + ctx}, task_tok)
thread_id = th.get("id", "") if isinstance(th, dict) else ""
rec("create_thread", {"http": code, "thread_id": thread_id, "context_id": th.get("context_id", "")})
if not thread_id:
    print("CASCADE_STAND_ISO=FAIL (CreateThread http=%s)" % code)
    sys.exit(4)
# State owns the run's task: a body task_id that differs from the presenting token is 403
# and an absent one is bound to it (internal/stateapi/routes.go). The body field is
# "payload"; an unknown key is a 400 "request body is not valid JSON", measured here.
code, rn = post(agg, "CreateRun", {"thread_id": thread_id, "key": "k-run-" + stamp,
                                   "payload": {}}, task_tok)
run_id = rn.get("id", "") if isinstance(rn, dict) else ""
run_task = rn.get("task_id", "") if isinstance(rn, dict) else ""
rec("create_run", {"http": code, "run_id": run_id, "task_id": run_task,
                   "body_head": "" if run_id else str(rn)[:200]})
if not run_id:
    print("CASCADE_STAND_ISO=FAIL (CreateRun http=%s body=%s)" % (code, str(rn)[:160]))
    sys.exit(5)
# ClaimRun is a deployment-wide queue with NO selector (internal/stateapi/claim.go: it takes
# tenant/deployment/worker only), so a run left pending by an earlier leg is leased first.
# Measured: a leg wrote its artifact under the context of a foreign run and then "proved"
# the cascade broken, because the thread it deleted never owned that artifact. The leg
# therefore drains foreign leases - releasing each with its own token - until it holds the
# run it created.
run_tok = ""
lease_run = ""
for attempt in range(12):
    code, cl = post(agg, "ClaimRun", {"worker": "cascade-iso-worker", "ttl_ms": 60000}, "")
    run_tok = cl.get("task_token", "") if isinstance(cl, dict) else ""
    lease = (cl.get("lease") or {}) if isinstance(cl, dict) else {}
    lease_run = (lease.get("run") or {}).get("id", "")
    if not lease_run:
        rec("claim_run", {"http": code, "attempt": attempt, "lease_run": "",
                          "note": "queue empty, nothing of ours is claimable"})
        break
    if lease_run == run_id:
        break
    rel, _ = post(agg, "Release", {"run_id": lease_run, "worker": "cascade-iso-worker",
                                   "epoch": lease.get("epoch", 0), "status": "failed"}, run_tok)
    rec("claim_drain_foreign", {"attempt": attempt, "leased_run": lease_run,
                                "leased_task": (lease.get("run") or {}).get("task_id", ""),
                                "release_http": rel})
rec("claim_run", {"http": code, "run_token_sha12": sh12(run_tok), "lease_run": lease_run,
                  "lease_is_ours": lease_run == run_id})
if not run_tok:
    print("CASCADE_STAND_ISO=FAIL (ClaimRun gave no run token, http=%s)" % code)
    sys.exit(6)
if lease_run != run_id:
    print("CASCADE_STAND_ISO=FAIL (claimed run %s is not the run %s this leg created)" % (lease_run, run_id))
    sys.exit(6)

body = "каскадное удаление %s: строка, которая должна исчезнуть вместе с тредом\n" % stamp
sha = hashlib.sha256(body.encode()).hexdigest()
code, up = post(agg, "BeginUpload", {"key": "k-" + stamp, "name": "cascade.txt",
                                     "mime": "text/plain", "sha256": sha, "size": len(body.encode()),
                                     "labels": {}}, run_tok, svc="artifacts")
aid = up.get("id", "") if isinstance(up, dict) else ""
signed = up.get("presigned_url", "") if isinstance(up, dict) else ""
rec("begin_upload", {"http": code, "artifact_id": aid, "presigned_present": bool(signed)})
if not aid:
    print("CASCADE_STAND_ISO=FAIL (BeginUpload http=%s)" % code)
    sys.exit(7)
plain = out + "/payload.txt"
open(plain, "w").write(body)
put = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "60",
                      "-X", "PUT", "-T", plain, signed], capture_output=True, text=True)
rec("put_object", {"http": (put.stdout or "").strip()})
code, sealed = post(agg, "Seal", {"id": aid}, run_tok, svc="artifacts")
rec("seal", {"http": code, "sha256": sealed.get("sha256", "") if isinstance(sealed, dict) else ""})

code, meta = post(agg, "Get", {"id": aid}, run_tok, svc="artifacts")
binding = meta if isinstance(meta, dict) else {}
rec("binding_recorded", {"http": code, "tenant": binding.get("tenant", ""),
                         "deployment": binding.get("deployment", ""), "subject": binding.get("subject", ""),
                         "context_id": binding.get("context_id", ""), "task_id": binding.get("task_id", "")})
before_bin = out + "/before.bin"
code_before = post(agg, "Download", {"id": aid}, run_tok, svc="artifacts", binary_to=before_bin)
rec("before_delete", {"get_http": code, "download_http": code_before,
                      "bytes": os.path.getsize(before_bin) if os.path.exists(before_bin) else -1})

# 2b) read scope, measured with REAL Identity tokens on the same artifact. The pair
# demands the context-scoped read gate (commit b86f766); a stand built without it
# answers 200 here, which is the defect rather than a leg bug, so
# AHP_CASCADE_SCOPE_PAIR=0 skips it and the verdict records that it was skipped.
SCOPE_PAIR = os.environ.get("AHP_CASCADE_SCOPE_PAIR", "1") != "0"
sib_tok = foreign_tok = ""
code_sib_get = code_sib_dl = code_for_get = code_for_dl = "SKIPPED"
foreign_bin = out + "/foreign.bin"
foreign_bytes = -1
if SCOPE_PAIR:
    code, ex_sib = post(agg, "", {"human_token": human_tok, "task": "cascade-iso-sibling-" + stamp,
                                  "context": ctx}, "", svc="identity/control/exchange")
    sib_tok = ex_sib.get("token", "") if isinstance(ex_sib, dict) else ""
    code_sib_get, _ = post(agg, "Get", {"id": aid}, sib_tok, svc="artifacts")
    # with binary_to the helper answers the status code alone
    code_sib_dl = post(agg, "Download", {"id": aid}, sib_tok, svc="artifacts", binary_to=out + "/sibling.bin")
    rec("scope_same_context", {"exchange_http": code, "get_http": code_sib_get,
                               "download_http": code_sib_dl})

    code, ex_for = post(agg, "", {"human_token": human_tok, "task": "cascade-iso-foreign-" + stamp,
                                  "context": ctx + "-foreign"}, "", svc="identity/control/exchange")
    foreign_tok = ex_for.get("token", "") if isinstance(ex_for, dict) else ""
    code_for_get, for_body = post(agg, "Get", {"id": aid}, foreign_tok, svc="artifacts")
    foreign_bin = out + "/foreign.bin"
    code_for_dl = post(agg, "Download", {"id": aid}, foreign_tok, svc="artifacts", binary_to=foreign_bin)
    foreign_bytes = os.path.getsize(foreign_bin) if os.path.exists(foreign_bin) else -1
    rec("scope_foreign_context", {"exchange_http": code, "get_http": code_for_get,
                                  "download_http": code_for_dl, "bytes": foreign_bytes,
                                  "object_served": serves_object(foreign_bin),
                                  "get_body_head": json.dumps(for_body)[:160]})

# 3) the deletion the PDF names, through the State API, with no agent involved at all
code_del, dbody = post(agg, "DeleteThread", {"thread_id": thread_id}, task_tok)
rec("delete_thread", {"http": code_del, "body_head": json.dumps(dbody)[:160]})

code_get, gbody = post(agg, "Get", {"id": aid}, run_tok, svc="artifacts")
after_bin = out + "/after.bin"
code_dl = post(agg, "Download", {"id": aid}, run_tok, svc="artifacts", binary_to=after_bin)
after_bytes = os.path.getsize(after_bin) if os.path.exists(after_bin) else -1
code_thread, tbody = post(agg, "GetThreadByContext", {"context_id": ctx}, task_tok)
rec("after_delete", {"artifact_get_http": code_get, "artifact_download_http": code_dl,
                     "artifact_bytes_still_served": after_bytes,
                     "object_still_served": serves_object(after_bin),
                     "thread_lookup_http": code_thread,
                     "get_body_head": json.dumps(gbody)[:160]})

# 4) a fresh credential for the same subject must not bring the bytes back
code, ex2 = post(agg, "", {"human_token": human_tok, "task": "cascade-iso-reissue-" + stamp,
                           "context": ctx}, "", svc="identity/control/exchange")
fresh = ex2.get("token", "") if isinstance(ex2, dict) else ""
rec("reissue", {"http": code, "token_sha12": sh12(fresh)})
code_re_get, _ = post(agg, "Get", {"id": aid}, fresh, svc="artifacts")
reissue_bin = out + "/reissue.bin"
code_re_dl = post(agg, "Download", {"id": aid}, fresh, svc="artifacts", binary_to=reissue_bin)
reissue_bytes = os.path.getsize(reissue_bin) if os.path.exists(reissue_bin) else -1
rec("after_reissue", {"artifact_get_http": code_re_get, "artifact_download_http": code_re_dl,
                      "bytes_still_served": reissue_bytes,
                      "object_served": serves_object(reissue_bin)})


GONE = ("404", "410")
verdict = "PASS"
if code_del != "200":
    verdict = "FAIL_DELETE_REFUSED(http=%s)" % code_del
elif code_thread == "200":
    verdict = "FAIL_THREAD_STILL_READABLE"
elif not (binding.get("context_id") and binding.get("task_id")):
    verdict = "FAIL_BINDING_NOT_RECORDED(context_id=%r task_id=%r)" % (
        binding.get("context_id", ""), binding.get("task_id", ""))
elif binding.get("context_id") != ctx or binding.get("task_id") != run_task:
    # The row must be bound to THIS thread's context and THIS run's task. Bound to
    # anything else, the deletion below cannot reach it and the leg would measure a
    # foreign artifact while reporting a cascade defect (measured on this stand).
    verdict = "FAIL_BINDING_NOT_THIS_RUN(context=%r want=%r task=%r want=%r)" % (
        binding.get("context_id", ""), ctx, binding.get("task_id", ""), run_task)
elif SCOPE_PAIR and (code_for_get != "403" or code_for_dl != "403"):
    verdict = "FAIL_FOREIGN_CONTEXT_READABLE(get=%s download=%s bytes=%s)" % (
        code_for_get, code_for_dl, foreign_bytes)
elif SCOPE_PAIR and serves_object(foreign_bin):
    verdict = "FAIL_FOREIGN_CONTEXT_BYTES_SERVED(bytes=%s)" % foreign_bytes
elif SCOPE_PAIR and (code_sib_get != "200" or code_sib_dl != "200"):
    verdict = "FAIL_SAME_CONTEXT_READ_REFUSED(get=%s download=%s)" % (code_sib_get, code_sib_dl)
elif code_get not in GONE or code_dl not in GONE:
    verdict = "FAIL_CASCADE_ARTIFACT_STILL_READABLE(get=%s download=%s bytes=%s)" % (
        code_get, code_dl, after_bytes)
elif serves_object(after_bin):
    verdict = "FAIL_CASCADE_BYTES_STILL_SERVED(bytes=%s)" % after_bytes
elif code_re_get not in GONE or code_re_dl not in GONE:
    verdict = "FAIL_REISSUED_TOKEN_RESURRECTED(get=%s download=%s bytes=%s)" % (
        code_re_get, code_re_dl, reissue_bytes)
elif serves_object(reissue_bin):
    verdict = "FAIL_REISSUED_BYTES_STILL_SERVED(bytes=%s)" % reissue_bytes

rec("verdict", {"verdict": verdict, "artifact_id": aid, "thread_id": thread_id,
                "foreign_context_get": code_for_get, "foreign_context_download": code_for_dl,
                "same_context_get": code_sib_get, "same_context_download": code_sib_dl,
                "scope_pair_measured": SCOPE_PAIR,
                "get_after": code_get, "download_after": code_dl, "bytes_after": after_bytes,
                "reissue_get": code_re_get, "reissue_download": code_re_dl, "reissue_bytes": reissue_bytes})
open(os.path.join(out, "verdict"), "w").write(verdict + "\n")
print("CASCADE_STAND_ISO=%s delete=%s thread_lookup=%s foreign_ctx_get=%s foreign_ctx_dl=%s same_ctx_get=%s get_after=%s download_after=%s bytes=%s reissue_get=%s reissue_download=%s"
      % (verdict, code_del, code_thread, code_for_get, code_for_dl, code_sib_get, code_get, code_dl,
         after_bytes, code_re_get, code_re_dl))
PY
rc=0
[ "$(cat "$OUT/verdict" 2>/dev/null)" = "PASS" ] || rc=1
echo "EVIDENCE=$OUT"
exit "$rc"
