#!/usr/bin/env bash
# asws-user-token-leg.sh -- does the DATA plane accept a USER credential on /v1/workspace/*?
#
# Why this leg exists. The director's ruling 20261101T154234Z item 2 (corrected by the erratum
# 20261101T154241Z) chose option B for the browser upload route: the user edge forwards
# /v1/workspace/ to the DATA plane, presenting its own client certificate, and does not rewrite
# Authorization, so the user's own bearer arrives at the plane that is supposed to verify it.
# The ruling names one feasibility measurement to run before the flip and splits the work by
# file: the route in cmd/ahp/single_origin.go is the director's, the token verification on the
# data plane is mine. This leg is that measurement, kept as an instrument so the verdict is
# re-runnable by the verifier instead of living as prose in a post.
#
# What it measures, in order:
#   C1  a task token exchanged by this very certificate, presented with that certificate -> 200
#   C2  that same task token presented with a DIFFERENT deployment's certificate         -> 403
#       (the workload binding must not loosen, or a token minted for one deployment could be
#       replayed through any other edge; C2 going green is a finding, not a win)
#   U1  the owner's own browser bearer on the workspace face                             -> 200
#   U2  the owner's browser bearer performing the real write UpdateCandidateFiles        -> 200
#   U3  a foreign subject's browser bearer on the same workspace                          -> 403, not 404
#   U4  a browser bearer on the ARTIFACT face                                            -> 403 (that face stays task-only)
#   U5  a browser bearer with no client certificate at all                                -> refused at TLS
#       (a browser holds no workload certificate, which is why the edge has to present one)
#   C3  the SAME Publish with a task token on its own candidate, so the publish refusal
#       below is attributable: identical codes mean a product rule the user path inherits,
#       a difference would mean this lane regressed the credential.
#
# One gap is recorded rather than failed, because it is measured and out of the ruling's
# scope: an artifact a browser seals may not be published into a workspace ("labels do not
# permit publication to this sink"), and C3 shows the task-token path is refused identically.
# No revision therefore exists to list files from, which is why U6b/U6 read KNOWN-GAP. The
# ruling's closing criterion -- owner read 200, owner write 200, foreign subject 403 not 404
# -- is measured by U1, U2, U3 and U7 and is what the verdict reports.
#
# It talks to a live deployment only: it starts nothing, restarts nothing, writes only its own
# evidence directory. Setup (seal + workspace + candidate) goes through the user edge, the path
# already proven by asws-browser-lane.sh, so this leg measures credential acceptance and nothing
# else. Tokens and passwords stay in memory; evidence carries codes, ids, sha256, byte counts.
set -u

RUN=${AHP_AWSWS_RUN:?set AHP_AWSWS_RUN to the live lane run dir holding pki/ and user-*.env}
EDGE_PORT=${AHP_AWSWS_EDGE_LISTEN:-18391}
DATA_PORT=${AHP_AWSWS_DATA_PORT:-18390}
# The control exchange and the workspace face are different planes, and on a split stand they are
# different PROCESSES: --role data serves no identity route at all. Default = DATA_PORT, so a
# two-process run behaves exactly as before; set it when the exchange lives elsewhere.
EXCHANGE_PORT=${AHP_AWSWS_EXCHANGE_PORT:-${AHP_AWSWS_DATA_PORT:-18390}}
DATA_CERT=${AHP_AWSWS_DATA_CERT:-edge-client}
OTHER_CERT=${AHP_AWSWS_OTHER_CERT:-agg-client}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
EV=${AHP_AWSWS_EVIDENCE:-/dev/shm/asws-user-token-$STAMP}
mkdir -p "$EV"

printf '%s\n' "== data-plane user credential leg ($STAMP) LANE-NOT-PROD =="
printf '%s\n' "user edge http://127.0.0.1:$EDGE_PORT   data plane https://127.0.0.1:$DATA_PORT"
printf '%s\n' "control exchange https://127.0.0.1:$EXCHANGE_PORT$( [ "$EXCHANGE_PORT" = "$DATA_PORT" ] || printf ' (a different process: the data plane serves no identity route)' )"
printf '%s\n' "edge workload cert $DATA_CERT   foreign workload cert $OTHER_CERT   evidence $EV"

python3 - "$RUN" "$EDGE_PORT" "$DATA_PORT" "$DATA_CERT" "$OTHER_CERT" "$EV" "$EXCHANGE_PORT" > "$EV/user-token-leg.log" 2>&1 <<'PY'
import base64, hashlib, http.cookiejar, json, pathlib, secrets, ssl, sys, urllib.error, urllib.request

run, edge_port, data_port, cert, other_cert, ev, exchange_port = sys.argv[1:8]
edge = "http://127.0.0.1:" + edge_port
data = "https://127.0.0.1:" + data_port
exchange = "https://127.0.0.1:" + exchange_port
fails = []
gaps = []


def rec(step, **kw):
    print("STEP %-30s %s" % (step, " ".join("%s=%s" % kv for kv in kw.items())), flush=True)


def fail(step, want, got):
    fails.append(step)
    rec(step + "-verdict", want=want, got=got, verdict="FAIL")


def tls_ctx(client):
    ctx = ssl.create_default_context(cafile=str(pathlib.Path(run, "pki", "ca.crt")))
    ctx.check_hostname = False
    ctx.load_cert_chain(str(pathlib.Path(run, "pki", "%s.crt" % client)),
                        str(pathlib.Path(run, "pki", "%s.key" % client)))
    return ctx


def post(url, token, body, client=None, tls=False):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers)
    try:
        if tls:
            r = urllib.request.urlopen(req, timeout=60, context=tls_ctx(client))
        else:
            r = urllib.request.urlopen(req, timeout=60)
        return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:
        return "TLS-REFUSED", type(e).__name__


def login(who):
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    d = dict(l.split("=", 1) for l in pathlib.Path(run, "user-%s.env" % who).read_text().split("\n")
             if "=" in l)
    body = json.dumps({"username": d["LANE_USERNAME"], "password": d["LANE_PASSWORD"]}).encode()
    opener.open(urllib.request.Request(edge + "/u-ahp/v1/auth/login", data=body,
                headers={"Content-Type": "application/json", "X-AHP-Session": "1"}), timeout=30).read()
    r = opener.open(urllib.request.Request(edge + "/u-ahp/v1/auth/token", data=b"{}",
                    headers={"Content-Type": "application/json", "X-AHP-Session": "1"}), timeout=30)
    return json.loads(r.read())["access_token"]


def edge_call(path, token, body):
    req = urllib.request.Request(edge + path, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "Bearer " + token})
    try:
        r = urllib.request.urlopen(req, timeout=60)
        return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def expect(step, code, want, body="", gate=True):
    # A refusal is only evidence when the accepted case was accepted: while U1 is red every
    # probe is refused for the credential's audience, and calling U3 green on top of that
    # would be a false green. gate=False downgrades the step to FAIL-blocked, never to PASS.
    doc = {}
    try:
        doc = json.loads(body)
    except Exception:
        pass
    wanted = want if isinstance(want, (list, tuple)) else [want]
    ok = gate and code in wanted
    rec(step, http=code, want="/".join(map(str, wanted)),
        sha256=str(doc.get("sha256", ""))[:16], size=doc.get("size", ""),
        error=str(doc.get("message", doc.get("code", "")))[:110],
        verdict="PASS" if ok else ("FAIL" if gate else "FAIL-blocked"))
    if not ok:
        fail(step, wanted, code if gate else "blocked")
    return doc


tag = hashlib.sha256(secrets.token_bytes(8)).hexdigest()[:12]
owner = login("a")
foreign = login("b")
rec("login-two-humans", verdict="PASS", human_token_bytes=len(owner))

# --- setup through the user edge: the path already proven by asws-browser-lane.sh ---
payload = b"data-plane user credential leg " + secrets.token_bytes(8) + b"\n"
want_sha, want_size = hashlib.sha256(payload).hexdigest(), len(payload)
code, body = edge_call("/v1/artifacts/BeginUpload", owner,
                       {"key": "ut-" + tag, "name": "note.txt", "mime": "text/plain",
                        "sha256": want_sha, "size": want_size})
up = json.loads(body) if code == 200 else {}
rec("setup-BeginUpload", http=code, verdict="PASS" if code == 200 else "FAIL")
if code != 200:
    fails.append("setup-BeginUpload")
put_ok = False
if up.get("presigned_url"):
    try:
        put_ok = urllib.request.urlopen(urllib.request.Request(
            up["presigned_url"], data=payload, method="PUT"), timeout=60).status in (200, 204)
    except urllib.error.HTTPError as e:
        rec("setup-presigned-PUT-http", http=e.code)
rec("setup-presigned-PUT", http="ok" if put_ok else "ERR", bytes=want_size,
    verdict="PASS" if put_ok else "FAIL")
if not put_ok:
    fails.append("setup-presigned-PUT")
code, body = edge_call("/v1/artifacts/Seal", owner, {"id": up.get("id", "")})
sealed = json.loads(body) if code == 200 else {}
rec("setup-Seal", http=code, sha256=sealed.get("sha256", ""), size=sealed.get("size", ""),
    verdict="PASS" if code == 200 and sealed.get("sha256") == want_sha else "FAIL")
if sealed.get("sha256") != want_sha:
    fails.append("setup-Seal")
code, body = edge_call("/v1/workspace/Create", owner, {"workspace": "ut-proj-" + tag})
ws_id = json.loads(body).get("id", "") if code == 200 else ""
rec("setup-workspace-Create", http=code, workspace=ws_id, verdict="PASS" if code == 200 else "FAIL")
if code != 200:
    fails.append("setup-workspace-Create")
code, body = edge_call("/v1/workspace/CreateCandidate", owner, {"workspace": ws_id})
cand = json.loads(body).get("id", "") if code == 200 else ""
rec("setup-CreateCandidate", http=code, candidate=cand, verdict="PASS" if code == 200 else "FAIL")
if code != 200:
    fails.append("setup-CreateCandidate")
files = {"candidate_id": cand, "files": [{"path": "note.txt", "artifact_id": up.get("id", ""),
                                          "mode": 420, "digest": want_sha}]}

# --- C1/C2: the task-token controls on the data-plane face itself ---
code, body = post(exchange + "/v1/identity/control/exchange", None,
                  {"human_token": owner, "task": "utleg-" + tag, "context": "utctx-" + tag,
                   "capabilities": ["workspace"]}, client=cert, tls=True)
doc = json.loads(body) if code == 200 else {}
task_tok = doc.get("token", "")
rec("control-exchange", http=code, minted_task=doc.get("task", ""),
    task_token_bytes=len(task_tok), verdict="PASS" if code == 200 else "FAIL")
if code != 200:
    fails.append("control-exchange")
c1, b1 = post(data + "/v1/workspace/GetWorkspace", task_tok, {"workspace": ws_id},
              client=cert, tls=True)
expect("C1-task-token-own-cert", c1, 200, b1)
c2, b2 = post(data + "/v1/workspace/GetWorkspace", task_tok, {"workspace": ws_id},
              client=other_cert, tls=True)
expect("C2-task-token-foreign-cert", c2, 403, b2)

# --- U1..U5: the user credential on the same face ---
read_body = {"workspace": ws_id}
u1_doc = expect("U1-owner-bearer-read", post(data + "/v1/workspace/GetWorkspace", owner, read_body,
                                             client=cert, tls=True)[0], 200)
head = str(u1_doc.get("head_rev", ""))
u1_ok = "U1-owner-bearer-read" not in fails
code, body = post(data + "/v1/workspace/UpdateCandidateFiles", owner, files, client=cert, tls=True)
expect("U2-owner-bearer-write", code, 200, body, gate=u1_ok)
code, body = post(data + "/v1/workspace/GetWorkspace", foreign, read_body, client=cert, tls=True)
expect("U3-foreign-bearer-read", code, [403], body, gate=u1_ok)
# U6: the sealed file, read back out of a REAL revision through the user
# credential. The project context only lists files that belong to a revision, so
# the leg runs the owner's own publish path with the browser bearer -- check,
# publish, list -- and compares the digest the store returns against the digest
# Seal measured. The byte count is the one Seal measured; the listing carries the
# path and the digest, which is what the project context stores per path.
# C3: the SAME publish attempt with a task token instead of a user credential, on
# its own candidate. Whether the refusal below is about the credential or about
# the artifact's own labels decides who owns the gap: a difference would be this
# lane's regression, an identical code is a product rule the user path inherits.
code, body = edge_call("/v1/workspace/CreateCandidate", owner, {"workspace": ws_id})
cand2 = json.loads(body).get("id", "") if code == 200 else ""
rec("C3-setup-second-candidate", http=code, candidate=cand2,
    verdict="PASS" if code == 200 else "FAIL")
if code != 200:
    fails.append("C3-setup-second-candidate")
code, body = post(data + "/v1/workspace/UpdateCandidateFiles", task_tok,
                  {"candidate_id": cand2, "files": [{"path": "note.txt",
                                                     "artifact_id": up.get("id", ""),
                                                     "mode": 420, "digest": want_sha}]},
                  client=cert, tls=True)
rec("C3a-task-token-write-candidate", http=code, want=200,
    verdict="PASS" if code == 200 else "FAIL")
if code != 200:
    fails.append("C3a-task-token-write-candidate")
post(data + "/v1/workspace/SetCheck", task_tok,
     {"candidate_id": cand2, "name": "user-token-leg", "status": "passed"},
     client=cert, tls=True)
code, body = post(data + "/v1/workspace/Publish", task_tok,
                  {"candidate_id": cand2, "expected_head": head}, client=cert, tls=True)
task_publish_code = code
rec("C3b-task-token-publish", http=code, note="compared against U6b below",
    verdict="MEASURED")

code, body = post(data + "/v1/workspace/SetCheck", owner,
                  {"candidate_id": cand, "name": "user-token-leg", "status": "passed"},
                  client=cert, tls=True)
expect("U6a-owner-bearer-setcheck", code, 200, body, gate=u1_ok)
code, body = post(data + "/v1/workspace/Publish", owner,
                  {"candidate_id": cand, "expected_head": head}, client=cert, tls=True)
published = {}
try:
    published = json.loads(body)
except Exception:
    pass
rev_id = str(published.get("id", "")) if code == 200 else ""
user_publish_code = code
publish_gap = ("labels do not permit publication" in body
               and code == task_publish_code == 403)
if publish_gap:
    # Measured, credential-independent, and outside the ruling's closing criterion
    # (which asks for the owner read, the owner write and the foreign refusal): the
    # artifact a browser seals may not enter a workspace under the label rule, so
    # no revision exists to list. Recorded, never hidden, and raised to the director.
    gaps.append("U6b-publish-labels")
    rec("U6b-owner-bearer-publish", http=code, want=200, error=str(body)[:110],
        task_token_code=task_publish_code, verdict="KNOWN-GAP",
        note="identical on the task-token path, so not a user-credential regression")
else:
    expect("U6b-owner-bearer-publish", code, 200, body, gate=u1_ok)
code, body = post(data + "/v1/workspace/ListFiles", owner,
                  {"workspace": ws_id, "revision": rev_id}, client=cert, tls=True)
listed = {}
try:
    for f in json.loads(body):
        listed[f.get("path", "")] = f.get("digest", "")
except Exception:
    pass
digest_ok = listed.get("note.txt") == want_sha
if publish_gap:
    rec("U6-owner-bearer-listfiles", http=code, files="-", want_sha256=want_sha[:16],
        size=want_size, verdict="KNOWN-GAP", note="no revision exists while U6b refuses")
else:
    rec("U6-owner-bearer-listfiles", http=code, files=",".join(sorted(listed)) or "-",
        digest=listed.get("note.txt", "")[:16], want_sha256=want_sha[:16], size=want_size,
        verdict="PASS" if code == 200 and digest_ok else "FAIL", gate=u1_ok)
    if code != 200 or not digest_ok:
        fails.append("U6-owner-bearer-listfiles")
code, body = post(data + "/v1/workspace/ListFiles", foreign,
                  {"workspace": ws_id, "revision": rev_id}, client=cert, tls=True)
expect("U7-foreign-bearer-listfiles", code, [403], body, gate=u1_ok)
code, body = post(data + "/v1/artifacts/BeginUpload", owner,
                  {"key": "ut2-" + tag, "name": "x.txt", "mime": "text/plain",
                   "sha256": want_sha, "size": want_size}, client=cert, tls=True)
expect("U4-bearer-on-artifact-face", code, [403], body, gate=u1_ok)
code, body = post(data + "/v1/workspace/GetWorkspace", owner, read_body, client=None, tls=False)
expect("U5-bearer-without-client-cert", code, ["TLS-REFUSED", 403], str(body))

verdict = "USER_TOKEN_LEG=PASS" if not fails else "USER_TOKEN_LEG=FAIL"
rec("verdict", verdict=verdict, failed=",".join(fails) or "none",
    known_gaps=",".join(gaps) or "none", workspace=ws_id,
    candidate=cand, artifact_sha256=want_sha, artifact_bytes=want_size)
json.dump({"verdict": verdict, "failed": fails, "known_gaps": gaps,
           "workspace": ws_id, "candidate": cand,
           "revision": rev_id,
           "artifact": up.get("id", ""), "artifact_sha256": want_sha, "artifact_bytes": want_size,
           "data_plane": data, "user_edge": edge,
           "edge_workload_cert": cert, "foreign_workload_cert": other_cert},
          open(pathlib.Path(ev, "verdict.json"), "w"), indent=2)
PY
rc=$?
cat "$EV/user-token-leg.log"
printf 'EVIDENCE=%s rc=%s\n' "$EV" "$rc"
