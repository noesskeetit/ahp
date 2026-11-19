#!/bin/sh
# Acceptance leg for the Workspace upload-into-project-context mount, run against
# the FLIPPED VM1 acceptance deployment (not the lane, not prod).
#
# The lane leg (tests/aswsupload) proves the mechanism on an origin I own. This
# leg proves the same chain on the bytes the acceptance stand actually runs:
#   real login -> real Identity control exchange -> task token -> seal a real
#   artifact through the origin -> stage it into a real project candidate,
#   with a second subject refused.
#
# It writes NOTHING on the host it runs on: every finding goes to stdout, and the
# caller keeps the evidence file. Tokens and passwords are never printed; only
# HTTP codes, ids, sha256 and sizes.
#
#   sh scripts/asws-acceptance-leg.sh          # run on the acceptance host
set -eu
RUNDIR=${AHP_LEG_RUN_DIR:-/home/alex/ahp-hive/run/dir-acceptance-20261101T141605Z}
AGG=${AHP_LEG_AGG:-https://127.0.0.1:18380}
HUMAN=${AHP_LEG_HUMAN:-http://127.0.0.1:18381}
PY=${AHP_LEG_PYTHON:-python3}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
CTX="asws-$STAMP"
$PY - "$RUNDIR" "$AGG" "$HUMAN" "$CTX" <<'PY'
import hashlib, json, os, re, subprocess, sys, urllib.parse

run_dir, agg, human, ctx = sys.argv[1:5]
pki = os.path.join(run_dir, "pki")
MT = ["--cacert", os.path.join(pki, "ca.crt"),
      "--cert", os.path.join(pki, "client-acceptance.crt"),
      "--key", os.path.join(pki, "client-acceptance.key")]
fails = []


def rec(step, **kw):
    print(" ".join("%s=%s" % kv for kv in [("step", step)] + list(kw.items())), flush=True)


def fail(step, detail):
    fails.append(step)
    rec(step, verdict="FAIL", detail=str(detail)[:200])


def creds(which):
    env = {}
    with open(os.path.join(run_dir, which)) as fh:
        for line in fh:
            line = line.strip().removeprefix("export ")
            if "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env["STAND_USERNAME"], env["STAND_PASSWORD"]


def sh_json(cmd, binary=False):
    r = subprocess.run(cmd, capture_output=True, text=not binary)
    return r


def login(user, pwd):
    jar = "/dev/shm/asws-leg-%s.jar" % ctx
    subprocess.run(["curl", "-s", "-c", jar, "-o", "/dev/null", "--max-time", "15", "-X", "POST",
                    human + "/u-ahp/v1/auth/login", "-H", "Content-Type: application/json",
                    "-H", "X-AHP-Session: 1",
                    "-d", json.dumps({"username": user, "password": pwd})], capture_output=True, text=True)
    r = subprocess.run(["curl", "-s", "-b", jar, "--max-time", "15", "-X", "POST",
                        human + "/u-ahp/v1/auth/token", "-H", "X-AHP-Session: 1",
                        "-H", "Content-Type: application/json", "-d", "{}"],
                       capture_output=True, text=True)
    try:
        os.unlink(jar)
    except OSError:
        pass
    try:
        return json.loads(r.stdout).get("access_token", "")
    except Exception:
        return ""


def exchange(human_token, task, context=None):
    r = subprocess.run(["curl", "-s", "--max-time", "15"] + MT + ["-X", "POST",
                        agg + "/v1/identity/control/exchange", "-H", "Content-Type: application/json",
                        "-d", json.dumps({"human_token": human_token, "task": task,
                                              "context": context or ctx})],
                       capture_output=True, text=True)
    try:
        return json.loads(r.stdout).get("token", ""), r.stdout
    except Exception:
        return "", r.stdout


def refused(cond, step, code, body):
    rec(step, http=code, want="403/404", verdict="PASS" if cond else "FAIL",
        body="" if cond else body[:160])
    if not cond:
        fails.append(step)
    return cond


def envval(which, key):
    with open(os.path.join(run_dir, which)) as fh:
        for line in fh:
            line = line.strip().removeprefix("export ")
            k, _, v = line.partition("=")
            if k.strip() == key:
                return v.strip().strip('"').strip("'")
    return ""


def post(svc, op, body, tok):
    r = subprocess.run(["curl", "-s", "-w", "\n%{http_code}", "--max-time", "60"] + MT +
                       ["-X", "POST", "%s/v1/%s/%s" % (agg, svc, op),
                        "-H", "Authorization: Bearer " + tok, "-H", "Content-Type: application/json",
                        "-d", json.dumps(body)], capture_output=True, text=True)
    head, _, code = r.stdout.rpartition("\n")
    return code, head


def ok(step, want, got, body):
    good = got == want
    rec(step, http=got, want=want, verdict="PASS" if good else "FAIL",
        body=("" if good else body[:160]))
    if not good:
        fails.append(step)
    return good


def listen_port(var, default):
    ports = os.path.join(run_dir, "ports.env")
    try:
        with open(ports) as fh:
            for line in fh:
                line = line.strip().removeprefix("export ")
                k, _, v = line.partition("=")
                if k.strip() == var:
                    return v.strip().strip('"').strip("'").rsplit(":", 1)[-1]
    except OSError:
        pass
    return default


def live_listener(port):
    """Who actually holds this socket, from the kernel rather than from a pid file.

    Measured reason this matters: the deployment's own service.pid / human-edge.pid /
    runner.pid still named 3380416 / 3381150 / 3380690 at 14:56Z, hours after that
    pair was replaced by 3448541 / 3450139 / 3450112. Evidence pinned to those files
    would name bytes that are not running.
    """
    r = subprocess.run(["ss", "-ltnp"], capture_output=True, text=True)
    for line in (r.stdout or "").splitlines():
        if ":" + port + " " in line or line.strip().endswith(":" + port):
            m = re.search(r"pid=(\d+)", line)
            if m:
                return m.group(1)
    return ""


def deployed_bytes():
    """Name the bytes this evidence describes.

    Acceptance on a new candidate is a RE-RUN, not a carried-over green (deputy
    14:55:15Z: dropping -trimpath changes the bytes for the same code), so a leg
    that does not record which binary answered cannot stand as evidence for a
    window it never ran against.
    """
    for name, var, default in (("state-origin", "AHP_LISTEN_STATE", "18380"),
                               ("human-edge", "AHP_LISTEN_IDENTITY", "18381"),
                               ("runner", "AHP_LISTEN_EXECUTION", "18382")):
        port = listen_port(var, default)
        pid = live_listener(port)
        if not pid:
            rec("bytes-under-test", role=name, port=port, verdict="UNKNOWN", detail="no listener found")
            continue
        try:
            exe = os.readlink("/proc/" + pid + "/exe")
            digest = hashlib.sha256(open(exe, "rb").read()).hexdigest()
            rec("bytes-under-test", role=name, port=port, pid=pid, exe=os.path.basename(exe), sha256=digest)
        except OSError as err:
            rec("bytes-under-test", role=name, port=port, pid=pid, verdict="UNKNOWN", detail=str(err)[:80])


deployed_bytes()

user_a, pass_a = creds("user-a.env")
user_b, pass_b = creds("user-b.env")
tok_a_human = login(user_a, pass_a)
rec("login-a", http="200" if tok_a_human else "NO_TOKEN", token_sha12=hashlib.sha256(tok_a_human.encode()).hexdigest()[:12])
if not tok_a_human:
    print("ASWS_ACC_LEG=FAIL (login user-a)")
    sys.exit(2)
tok_a, raw = exchange(tok_a_human, "asws-upload-leg")
rec("exchange-a", http="200" if tok_a else "NO_TOKEN", detail="" if tok_a else raw[:160])
if not tok_a:
    print("ASWS_ACC_LEG=FAIL (control exchange)")
    sys.exit(2)

payload = ("ahp workspace acceptance upload into project context %s\n" % ctx).encode()
want_sha = hashlib.sha256(payload).hexdigest()
want_size = len(payload)

# LEG 1: seal a real artifact through the flipped origin; Seal is content-addressed.
code, body = post("artifacts", "BeginUpload", {"key": "leg-" + ctx, "name": "hello.txt",
                                               "mime": "text/plain", "sha256": want_sha, "size": want_size}, tok_a)
if ok("L1a BeginUpload", "200", code, body):
    up = json.loads(body)
    put = subprocess.run(["curl", "-s", "-w", "%{http_code}", "--max-time", "60", "-X", "PUT",
                          "--data-binary", "@-", up["presigned_url"]],
                         input=payload, capture_output=True)
    put_code = put.stdout.decode()
    rec("L1b presigned-PUT", http=put_code, bytes=want_size,
        verdict="PASS" if put_code in ("200", "204") else "FAIL", detail=put.stdout.decode()[:120])
    if put_code not in ("200", "204"):
        fails.append("L1b presigned-PUT")
    code, body = post("artifacts", "Seal", {"id": up["id"]}, tok_a)
    sealed = code == "200"
    if sealed:
        ref = json.loads(body)
        sealed = ref.get("sha256") == want_sha and ref.get("size") == want_size
        rec("L1c Seal", http=code, artifact_id=ref.get("id", ""), sha256=ref.get("sha256", ""),
            size=ref.get("size", -1), verdict="PASS" if sealed else "FAIL")
    if not sealed:
        fails.append("L1c Seal")
        print("ASWS_ACC_LEG=FAIL (artifact seal)"); sys.exit(2)
    art_id = json.loads(body)["id"]
else:
    print("ASWS_ACC_LEG=FAIL (BeginUpload)"); sys.exit(2)

# LEG 2: upload into PROJECT CONTEXT -- workspace, candidate, staged file.
code, body = post("workspace", "Create", {"workspace": "proj-" + ctx}, tok_a)
ws_id = json.loads(body).get("id", "") if code == "200" else ""
ok("L2a workspace/Create", "200", code, body)
if not ws_id:
    print("ASWS_ACC_LEG=FAIL (no workspace id)"); sys.exit(2)
code, body = post("workspace", "CreateCandidate", {"workspace": ws_id}, tok_a)
cand = json.loads(body) if code == "200" else {}
ok("L2b CreateCandidate", "200", code, body)
files = [{"path": "hello.txt", "artifact_id": art_id, "mode": 420, "digest": want_sha}]
code, body = post("workspace", "UpdateCandidateFiles",
                  {"candidate_id": cand.get("id", ""), "files": files}, tok_a)
ok("L2c UpdateCandidateFiles", "200", code, body)
rec("L2 upload-into-project", workspace=ws_id, candidate=cand.get("id", ""),
    path="hello.txt", sha256=want_sha, size=want_size,
    verdict="FAIL" if "L2c UpdateCandidateFiles" in fails else "PASS")

# LEG 3: projects are separated, not merely writes refused. user-b is not a
# member of user-a's project, so it must not read it, must not list its members,
# and must not write into it. And it goes both ways: user-a, owner of its own
# project, has no more claim on user-b's project than any stranger.
tok_b_human = login(user_b, pass_b)
tok_b, raw = exchange(tok_b_human, "asws-upload-leg-b", ctx + "b")
rec("login+b-exchange-b", http="200" if tok_b else "NO_TOKEN", detail="" if tok_b else raw[:160])
if not tok_b:
    fail("L3/L4/L5 skipped", "no task token for user-b")
else:
    for op in ("GetWorkspace", "ListMembers"):
        code, body = post("workspace", op, {"workspace": ws_id}, tok_b)
        refused(code in ("403", "404"), "L3a %s(user-b, non-member)" % op, code, body)
    code, body = post("workspace", "UpdateCandidateFiles",
                      {"candidate_id": cand.get("id", ""), "files": files}, tok_b)
    refused(code in ("403", "404"), "L3b foreign-subject UpdateCandidateFiles", code, body)

    # LEG 4: the same second subject in its own project succeeds, so a 403 above
    # is isolation and not a globally broken write path.
    code, body = post("workspace", "Create", {"workspace": "projB-" + ctx}, tok_b)
    ws_b = json.loads(body).get("id", "") if code == "200" else ""
    ok("L4a workspace/Create(user-b)", "200", code, body)
    cand_b = {}
    if ws_b:
        code, body = post("workspace", "CreateCandidate", {"workspace": ws_b}, tok_b)
        cand_b = json.loads(body) if code == "200" else {}
        ok("L4b CreateCandidate(user-b)", "200", code, body)
        code, body = post("workspace", "UpdateCandidateFiles",
                          {"candidate_id": cand_b.get("id", ""), "files": files}, tok_b)
        ok("L4c UpdateCandidateFiles(user-b)", "200", code, body)
    code, body = post("workspace", "UpdateCandidateFiles",
                      {"candidate_id": cand_b.get("id", ""), "files": files}, tok_a)
    refused(code in ("403", "404"), "L4d owner-of-A into B candidate (symmetry)", code, body)

    # LEG 5: the three tiers on the acceptance byte. The owner invites user-b
    # (editor); the editor then writes into the project it joined and is refused
    # the owner-only actions -- so "the rights of two users are separated" is
    # measured here rather than read off the policy table.
    subj_b = envval("user-b.env", "STAND_SUBJECT")
    subj_a = envval("user-a.env", "STAND_SUBJECT")
    code, body = post("workspace", "AddMember", {"workspace": ws_id, "subject": subj_b}, tok_a)
    ok("L5a AddMember(editor) by owner", "200", code, body)
    code, body = post("workspace", "UpdateCandidateFiles",
                      {"candidate_id": cand.get("id", ""), "files": files}, tok_b)
    ok("L5b editor writes into the project it joined", "200", code, body)
    for op, subj in (("AddMember", subj_a), ("ListMembers", None)):
        req = {"workspace": ws_id} if subj is None else {"workspace": ws_id, "subject": subj}
        code, body = post("workspace", op, req, tok_b)
        refused(code == "403", "L5c editor refused owner-only %s" % op, code, body)

print("ASWS_ACC_LEG=%s ctx=%s sha256=%s size=%d origin=%s failed=%s" % (
    "PASS" if not fails else "FAIL", ctx, want_sha, want_size, agg, ",".join(fails) or "none"))
sys.exit(0 if not fails else 1)
PY
