set -eu
# DownloadGrant leg, required by decisions/2026-10-31-artifacts-download-grant-contract.md
# (PDF rev3.2 §6 стр.1582 names DownloadGrant as an Artifacts operation; рис. 7 стр.608 names the
# last step "скачивание через прокси: текущие права, точная версия"; подпись стр.614 makes the label
# ceiling apply to handing the file out).
#
# Every leg below is measured over real HTTP against the ISOLATED composed stand
# (tools/cascade-stand-up.sh, :18094/:18095). The live stand (:18080-:18082) is never contacted.
# No token, signature, DSN or password is ever written into the evidence: only sha256 prefixes and
# non-secret claim members.
#
#   usage: sh tools/download-grant-leg.sh
HIVE=/home/alex/ahp-hive
RUNDIR=${AHP_CASCADE_RUN:-$HIVE/run/delivery-cascade-stand}
PKI="$RUNDIR/pki"
AGG=https://127.0.0.1:${AHP_CASCADE_PORT_AGG:-18094}
HUMAN=https://127.0.0.1:${AHP_CASCADE_PORT_HUMAN:-18095}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT=${OUT:-$RUNDIR/evidence/download-grant-$STAMP}
mkdir -p "$OUT"
[ -s "$PKI/ca.crt" ] || { echo "DOWNLOAD_GRANT=FAIL (no isolated stand PKI: run tools/cascade-stand-up.sh)"; exit 2; }
set -a; . "$RUNDIR/stand-user.env"; set +a
set +e
python3 - "$AGG" "$HUMAN" "$PKI" "$STAMP" "$OUT" "$STAND_USERNAME" "$STAND_PASSWORD" <<'PY' > "$OUT/leg.out" 2>&1
import base64, hashlib, json, os, subprocess, sys, time

agg, human, pki, stamp, out, user, pw = sys.argv[1:8]
MT = ["--cacert", pki + "/ca.crt", "--cert", pki + "/client-work.crt", "--key", pki + "/client-work.key"]
TLSCA = ["--cacert", pki + "/ca.crt"]
EV = out + "/evidence.jsonl"
FAILS = []
CHECKS = []


def rec(step, detail):
    with open(EV, "a") as f:
        f.write(json.dumps({"ts": time.strftime("%FT%TZ", time.gmtime()), "step": step,
                            "detail": detail}, ensure_ascii=False) + "\n")


def sh12(v):
    return hashlib.sha256(v.encode()).hexdigest()[:12] if v else ""


def check(name, ok, detail):
    CHECKS.append(name)
    rec(name, dict(detail, verdict="PASS" if ok else "FAIL"))
    if not ok:
        FAILS.append(name)
    print("%-42s %s" % (name, "PASS" if ok else "FAIL"))


def post(base, op, body, tok, svc="artifacts", binary_to=None, range_hdr=None):
    path = ("/v1/" + svc + (("/" + op) if op else ""))
    cmd = ["curl", "-s", "--max-time", "60"] + MT + ["-X", "POST", base + path,
           "-H", "Content-Type: application/json"]
    if tok:
        cmd += ["-H", "Authorization: Bearer " + tok]
    cmd += ["-d", json.dumps(body)]
    if range_hdr:
        cmd += ["-H", "Range: " + range_hdr]
    if binary_to:
        r = subprocess.run(cmd + ["-o", binary_to, "-w", "%{http_code}|%{size_download}"],
                           capture_output=True, text=True)
        return (r.stdout or "").strip() or "000ERR|0"
    r = subprocess.run(cmd + ["-w", "\n%{http_code}"], capture_output=True, text=True)
    b, _, c = r.stdout.rpartition("\n")
    try:
        return c, json.loads(b)
    except Exception:
        return c, {"raw": b[:200]}


def claims_of(tok):
    """Non-secret claim members only: the verdict answers with claims, never with opinions."""
    try:
        part = tok.split(".")[1]
        c = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
    except Exception:
        return {}
    return {k: c.get(k, "") for k in ("kind", "sub", "tenant", "azp", "task", "ctx", "deploy") if k in c}


def reason_of(body):
    return (body or {}).get("reason", "") if isinstance(body, dict) else ""


# --- browser identity (the human edge), which is exactly what must NOT reach the artifact plane --
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
    print("DOWNLOAD_GRANT=FAIL (browser login)")
    sys.exit(2)
rec("login", {"http": 200, "username": user, "token_sha12": sh12(human_tok), "claims": claims_of(human_tok)})


def chat(name):
    """One conversation of one subject: a thread, a run under it, and that run's own task token."""
    ctx = "grant-%s-%s" % (name, stamp)
    code, ex = post(agg, "", {"human_token": human_tok, "task": "grant-%s-%s" % (name, stamp),
                              "context": ctx}, "", svc="identity/control/exchange")
    tok = ex.get("token", "") if isinstance(ex, dict) else ""
    if not tok:
        print("DOWNLOAD_GRANT=FAIL (exchange chat %s http=%s)" % (name, code))
        sys.exit(3)
    code, th = post(agg, "CreateThread", {"context_id": ctx, "key": "k-" + ctx}, tok, svc="state")
    thread = th.get("id", "") if isinstance(th, dict) else ""
    if not thread:
        print("DOWNLOAD_GRANT=FAIL (CreateThread chat %s http=%s)" % (name, code))
        sys.exit(4)
    code, rn = post(agg, "CreateRun", {"thread_id": thread, "key": "k-run-" + ctx, "payload": {}},
                    tok, svc="state")
    run = rn.get("id", "") if isinstance(rn, dict) else ""
    if not run:
        print("DOWNLOAD_GRANT=FAIL (CreateRun chat %s http=%s)" % (name, code))
        sys.exit(5)
    run_tok, leased = "", ""
    for _ in range(12):
        code, cl = post(agg, "ClaimRun", {"worker": "grant-worker", "ttl_ms": 60000}, "", svc="state")
        run_tok = cl.get("task_token", "") if isinstance(cl, dict) else ""
        lease = (cl.get("lease") or {}) if isinstance(cl, dict) else {}
        leased = (lease.get("run") or {}).get("id", "")
        if not leased or leased == run:
            break
        rel, _ = post(agg, "Release", {"run_id": leased, "worker": "grant-worker",
                                       "epoch": lease.get("epoch", 0), "status": "failed"},
                      run_tok, svc="state")
        rec("claim_drain_foreign", {"chat": name, "leased_run": leased, "release_http": rel})
    if leased != run:
        print("DOWNLOAD_GRANT=FAIL (chat %s claimed %s, not its own run %s)" % (name, leased, run))
        sys.exit(6)
    rec("chat", {"chat": name, "context_id": ctx, "thread_id": thread, "run_id": run,
                 "run_token_sha12": sh12(run_tok), "claims": claims_of(run_tok)})
    return {"ctx": ctx, "thread": thread, "run": run, "tok": run_tok}


def upload(owner, key, text, labels=None):
    body = text.encode()
    sha = hashlib.sha256(body).hexdigest()
    req = {"key": key, "name": "grant-%s.txt" % key, "mime": "text/plain",
           "sha256": sha, "size": len(body)}
    if labels:
        req["labels"] = labels
    code, up = post(agg, "BeginUpload", req, owner["tok"])
    aid = up.get("id", "") if isinstance(up, dict) else ""
    signed = up.get("presigned_url", "") if isinstance(up, dict) else ""
    if not aid:
        rec("begin_upload", {"key": key, "http": code, "body_head": json.dumps(up)[:200]})
        return "", ""
    plain = out + "/payload-%s.txt" % hashlib.sha256(key.encode()).hexdigest()[:8]
    open(plain, "w").write(text)
    put = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "60",
                          "-X", "PUT", "-T", plain, signed], capture_output=True, text=True)
    c2, sealed = post(agg, "Seal", {"id": aid}, owner["tok"])
    rec("upload_seal", {"key": key, "artifact_id": aid, "put_http": (put.stdout or "").strip(),
                        "seal_http": c2, "sealed_version": (sealed or {}).get("version", "")
                        if isinstance(sealed, dict) else "", "sha256": sha})
    return aid, sha


a = chat("a")
b = chat("b")

# --- (e) positive control: the granted read is byte-for-byte the exact sealed version ----------
text_a = "точная версия для прокси %s\n" % stamp
aid, sha_a = upload(a, "k-grant-%s" % a["ctx"], text_a)
if not aid:
    print("DOWNLOAD_GRANT=FAIL (upload refused)")
    sys.exit(7)
code, meta = post(agg, "Get", {"id": aid}, a["tok"])
version = meta.get("version", "") if isinstance(meta, dict) else ""
check("metadata_carries_exact_version", code == "200" and bool(version),
      {"get_http": code, "artifact_id": aid, "version": version})

code, grant = post(agg, "DownloadGrant", {"id": aid, "version": version}, a["tok"])
gtok = grant.get("grant", "") if isinstance(grant, dict) else ""
check("grant_issued_for_exact_version",
      code == "200" and bool(gtok) and grant.get("artifact_id") == aid
      and grant.get("version") == version and grant.get("one_time") is True,
      {"mint_http": code, "grant_sha12": sh12(gtok), "artifact_id": grant.get("artifact_id"),
       "version": grant.get("version"), "one_time": grant.get("one_time"),
       "expires_at": grant.get("expires_at"), "claims_of_issuer": claims_of(a["tok"])})

got = out + "/granted.bin"
res = post(agg, "OpenDownloadGrant", {"grant": gtok}, a["tok"], binary_to=got)
served_sha = sh12(open(got).read()) if os.path.exists(got) else ""
served_exact = os.path.exists(got) and open(got, "rb").read() == text_a.encode()
check("granted_bytes_are_exact_version", res.startswith("200") and served_exact and served_sha == sha_a[:12],
      {"open_http": res.split("|")[0], "bytes": os.path.getsize(got) if os.path.exists(got) else -1,
       "served_sha256_12": served_sha, "sealed_sha256_12": sha_a[:12]})

# --- Range through the grant: the proxy is a pipe, not a second implementation ------------------
res = post(agg, "OpenDownloadGrant", {"grant": sh12(gtok)}, a["tok"], binary_to=out + "/bogus.bin")
check("unknown_grant_is_refused", res.split("|")[0] == "403",
      {"open_http": res.split("|")[0], "note": "a digest-shaped unknown token must not stream"})

# --- (b) a one-time grant refuses its second consumption, with a machine-readable reason --------
code, body = post(agg, "OpenDownloadGrant", {"grant": gtok}, a["tok"])
check("second_use_refused_with_reason", code == "403" and reason_of(body) == "grant_used",
      {"open_http": code, "reason": reason_of(body), "code_field": (body or {}).get("code", "")})

# --- (b) an expired grant says "expired", not "used" --------------------------------------------
mint_code, short = post(agg, "DownloadGrant", {"id": aid, "version": version, "ttl_seconds": 5}, a["tok"])
stok = short.get("grant", "") if isinstance(short, dict) else ""
time.sleep(7)
code, body = post(agg, "OpenDownloadGrant", {"grant": stok}, a["tok"])
check("expired_grant_says_expired", mint_code == "200" and code == "403" and reason_of(body) == "grant_expired",
      {"mint_http": mint_code, "open_http": code, "reason": reason_of(body),
       "expires_at": short.get("expires_at") if isinstance(short, dict) else "",
       "requested_ttl_seconds": 5, "waited_seconds": 7})

# --- "latest" is not a version ------------------------------------------------------------------
for name, req in (("mint_without_version", {"id": aid}),
                  ("mint_latest_version", {"id": aid, "version": "latest"}),
                  ("mint_foreign_version", {"id": aid, "version": "00000000-0000-0000-0000-000000000000"})):
    code, body = post(agg, "DownloadGrant", req, a["tok"])
    want = "grant_version_required" if "version" not in req or req["version"] == "latest" else "grant_version_mismatch"
    check(name + "_refused", code in ("400", "403") and reason_of(body) == want,
          {"http": code, "reason": reason_of(body), "code_field": (body or {}).get("code", "")})

# --- (a) a grant may not name another recipient -------------------------------------------------
code, body = post(agg, "DownloadGrant", {"id": aid, "version": version,
                                         "recipient": "someone-else-" + stamp}, a["tok"])
check("foreign_recipient_refused", code == "403" and reason_of(body) == "grant_recipient_mismatch",
      {"http": code, "reason": reason_of(body), "claims_of_caller": claims_of(a["tok"])})

# --- (d) lifecycle: a hold refuses issue AND consumption, a tombstone answers 404 ---------------
text_l = "файл, помеченный legal hold %s\n" % stamp
lid, _ = upload(a, "k-hold-%s" % a["ctx"], text_l)
code, lmeta = post(agg, "Get", {"id": lid}, a["tok"])
lversion = lmeta.get("version", "") if isinstance(lmeta, dict) else ""
code, held = post(agg, "DownloadGrant", {"id": lid, "version": lversion}, a["tok"])
htok = held.get("grant", "") if isinstance(held, dict) else ""
c1, _ = post(agg, "SetLegalHold", {"id": lid, "on": True}, a["tok"])
code, body = post(agg, "DownloadGrant", {"id": lid, "version": lversion}, a["tok"])
check("legal_hold_refuses_new_grant", code == "403" and reason_of(body) == "artifact_legal_hold",
      {"hold_http": c1, "mint_http": code, "reason": reason_of(body)})
code, body = post(agg, "OpenDownloadGrant", {"grant": htok}, a["tok"])
check("legal_hold_refuses_issued_grant", code == "403" and reason_of(body) == "artifact_legal_hold",
      {"open_http": code, "reason": reason_of(body),
       "note": "the grant was minted before the hold: rights are current, not printed on the ticket"})
c2, _ = post(agg, "SetLegalHold", {"id": lid, "on": False}, a["tok"])
c3, _ = post(agg, "Delete", {"id": lid, "key": "k-del-%s" % a["ctx"]}, a["tok"])
code, body = post(agg, "OpenDownloadGrant", {"grant": htok}, a["tok"])
check("tombstone_refuses_issued_grant", code == "404" and reason_of(body) == "artifact_deleted",
      {"hold_off_http": c2, "delete_http": c3, "open_http": code, "reason": reason_of(body),
       "code_field": (body or {}).get("code", "")})

# --- the grant cannot out-run the conversation gate of the issuing token ------------------------
text_b = "общий файл того же субъекта %s\n" % stamp
bid, _ = upload(b, "k-chatb-%s" % b["ctx"], text_b)
code, bmeta = post(agg, "Get", {"id": bid}, b["tok"])
bversion = bmeta.get("version", "") if isinstance(bmeta, dict) else ""
code, body = post(agg, "DownloadGrant", {"id": bid, "version": bversion}, b["tok"])
code, body = post(agg, "DownloadGrant", {"id": aid, "version": version}, b["tok"])
check("other_context_cannot_issue_grant", code == "403",
      {"http": code, "code_field": (body or {}).get("code", ""),
       "message_head": str((body or {}).get("message", ""))[:120],
       "issuer_claims": claims_of(b["tok"]), "artifact_context": claims_of(a["tok"]).get("ctx", ""),
       "note": "same subject, another chat: the grant is issued under the caller's own read gate"})

# --- (c) a human JWT straight at the artifact plane, behind a valid workload certificate --------
for op in ("DownloadGrant", "OpenDownloadGrant"):
    code, body = post(agg, op, {"id": aid, "version": version, "grant": gtok}, human_tok)
    check("human_token_at_%s_refused" % op,
          code not in ("200", "206") and not code.startswith("5") and code == "403",
          {"http": code, "code_field": (body or {}).get("code", ""),
           "message_head": str((body or {}).get("message", ""))[:120],
           "claims": claims_of(human_tok)})

# --- the granted path is a proxy: Range, then the use count actually runs out -------------------
code, rng = post(agg, "DownloadGrant", {"id": aid, "version": version, "uses": 2}, a["tok"])
rtok = rng.get("grant", "") if isinstance(rng, dict) else ""
res_full = post(agg, "OpenDownloadGrant", {"grant": rtok}, a["tok"], binary_to=out + "/full.bin")
res_range = post(agg, "OpenDownloadGrant", {"grant": rtok}, a["tok"],
                 binary_to=out + "/slice.bin", range_hdr="bytes=0-3")
slice_ok = (os.path.exists(out + "/slice.bin") and
            open(out + "/slice.bin", "rb").read() == text_a.encode()[:4])
check("multi_use_grant_honours_range", res_full.startswith("200") and res_range.startswith("206") and slice_ok,
      {"full_http": res_full.split("|")[0], "range_http": res_range.split("|")[0],
       "range": "bytes=0-3", "slice_bytes": os.path.getsize(out + "/slice.bin")
       if os.path.exists(out + "/slice.bin") else -1, "uses_max": rng.get("uses_max")})
code, body = post(agg, "OpenDownloadGrant", {"grant": rtok}, a["tok"])
check("use_count_runs_out", code == "403" and reason_of(body) == "grant_used",
      {"open_http": code, "reason": reason_of(body), "uses_max": rng.get("uses_max")})

print("")
if FAILS:
    print("DOWNLOAD_GRANT=FAIL legs=%d failing_legs=%s" % (len(CHECKS), ",".join(FAILS)))
    sys.exit(1)
print("DOWNLOAD_GRANT=PASS legs=%d artifact_id=%s version=%s" % (len(CHECKS), aid, version))
PY
rc=$?
cat "$OUT/leg.out"
echo "EVIDENCE=$OUT"
exit $rc
