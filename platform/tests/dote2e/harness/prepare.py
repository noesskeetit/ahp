#!/usr/bin/env python3
"""Prepare an isolated personal-Dot E2E stand for the dot-e2e lane.

Creates only NEW resources (own MinIO bucket, own tenant, own PKI, own config)
inside the lane's isolated PostgreSQL database. Secret values go to 0600 env
files only and are never printed.
"""
import argparse
import base64
import datetime
import hashlib
import hmac
import json
import os
import secrets
import shlex
import socket
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit

os.umask(0o077)

parser = argparse.ArgumentParser()
parser.add_argument("--runtime", required=True, type=Path, help="lane runtime root")
parser.add_argument("--env-file", required=True, type=Path, help="lane native env JSON")
parser.add_argument("--backend-port", type=int, default=29720)
parser.add_argument("--agent-port", type=int, default=29731)
parser.add_argument("--notes-port", type=int, default=29741)
parser.add_argument("--runner-port", type=int, default=29750)
parser.add_argument("--hindsight-url", default="http://127.0.0.1:38888")
args = parser.parse_args()

runtime = args.runtime.resolve()
lane_env = json.loads(args.env_file.read_text())
for port in (args.backend_port, args.agent_port, args.notes_port, args.runner_port):
    with socket.socket() as probe:
        # SO_REUSEADDR here only forgives the TIME_WAIT of a stand this harness
        # stopped moments ago; a live listener still refuses the bind.
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            raise SystemExit("port %d already occupied; refusing duplicate stand" % port)

run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%m%d%H%M%S") + secrets.token_hex(3)
instance = runtime / ("inst-" + run_id)
tenant = "dot-e2e-" + run_id
foreign_tenant = tenant + "-foreign"
bucket = "ahp-dot-e2e-" + run_id
database = lane_env["AHP_DB_NAME"]
dsn = lane_env["AHP_TEST_DATABASE_URL"]
parts = urlsplit(dsn)
if parts.hostname != "127.0.0.1" or parts.port != 25495:
    raise SystemExit("unexpected lane database endpoint")

for name in ("certs", "config", "logs", "notes"):
    (instance / name).mkdir(parents=True, exist_ok=True)


def write(name, content):
    path = instance / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(content.encode() if isinstance(content, str) else content)
    path.chmod(0o600)
    return path


def run(cmd, **kwargs):
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


check = run(["docker", "exec", "-i", "ahp-handbook-acceptance-pg", "psql", "-X", "-qAt", "-U",
             parts.username, "--dbname", "postgres", "-v", "ON_ERROR_STOP=1"],
            input="SELECT 1 FROM pg_database WHERE datname='" + database + "';")
if check.returncode or not check.stdout.strip():
    raise SystemExit("lane database missing; run the lane preparer first")


def sign(key, message):
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def s3_request(method, path, body=b""):
    endpoint = lane_env["AHP_S3_ENDPOINT"]
    host = endpoint.split("//", 1)[1].rstrip("/")
    region = lane_env.get("AHP_S3_REGION", "us-east-1")
    moment = datetime.datetime.now(datetime.timezone.utc)
    amz_date = moment.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = moment.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    canonical = "\n".join([method, path, "", "host:" + host, "x-amz-content-sha256:" + payload_hash,
                           "x-amz-date:" + amz_date, "", "host;x-amz-content-sha256;x-amz-date", payload_hash])
    scope = "/".join([date_stamp, region, "s3", "aws4_request"])
    string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    key = sign(sign(sign(sign(("AWS4" + lane_env["AHP_MINIO_ROOT_PASSWORD"]).encode(), date_stamp), region), "s3"),
               "aws4_request")
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request(endpoint + path, data=body or None, method=method)
    request.add_header("x-amz-date", amz_date)
    request.add_header("x-amz-content-sha256", payload_hash)
    request.add_header("Authorization", "AWS4-HMAC-SHA256 Credential=" + lane_env["AHP_MINIO_ROOT_USER"] + "/" + scope +
                       ", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=" + signature)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


if s3_request("PUT", "/" + bucket) not in (200, 409):
    raise SystemExit("could not create the lane artifact bucket")
if s3_request("HEAD", "/" + bucket) != 200:
    raise SystemExit("lane artifact bucket is not readable")

openssl_config = instance / "config/ca.cnf"
write("config/ca.cnf", """
[req]
distinguished_name = dn
prompt = no
[dn]
CN = AHP dot-e2e
[v3_ca]
basicConstraints = critical, CA:TRUE, pathlen:0
keyUsage = critical, keyCertSign, cRLSign, digitalSignature
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
""")
ca_key = write("certs/ca.key", run(["openssl", "genrsa", "-3", "3072"]).stdout)
signed_ca = run(["openssl", "req", "-new", "-x509", "-key", str(ca_key), "-sha256", "-days", "14",
                 "-out", str(instance / "certs/ca.crt"), "-subj", "/CN=AHP dot-e2e " + run_id,
                 "-extensions", "v3_ca", "-config", str(openssl_config)])
if signed_ca.returncode or not (instance / "certs/ca.crt").exists():
    raise SystemExit("CA generation failed")


def leaf(name, cert_tenant, deployment):
    san = "DNS:localhost,IP:127.0.0.1,URI:spiffe://ahp/tenant/%s/deployment/%s" % (cert_tenant, deployment)
    cfg = write("config/leaf-%s.cnf" % name, """
[req]
distinguished_name = dn
prompt = no
[dn]
CN = %s
[v3_ext]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = %s
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
""" % (name, san))
    key_path = write("certs/%s.key" % name, run(["openssl", "genrsa", "-3", "2048"]).stdout)
    csr = instance / ("certs/%s.csr" % name)
    run(["openssl", "req", "-new", "-key", str(key_path), "-out", str(csr), "-config", str(cfg)])
    result = run(["openssl", "x509", "-req", "-in", str(csr), "-CA", str(instance / "certs/ca.crt"),
                  "-CAkey", str(ca_key), "-CAcreateserial", "-days", "7", "-sha256", "-out",
                  str(instance / ("certs/%s.crt" % name)), "-extfile", str(cfg), "-extensions", "v3_ext"])
    if result.returncode or not (instance / ("certs/%s.crt" % name)).exists():
        raise SystemExit("leaf certificate generation failed for " + name)


leaf("server", tenant, "dot")
leaf("operator", tenant, "dot")
# Queue isolation (root requirement, director relay 2026-11-10 01:2xZ): the A2A
# agent replica claims DEPLOYMENT-WIDE (internal/agentworkload/service.go:276 ->
# ClaimRun by the certificate's tenant+deployment). Sharing deployment "dot" with
# as-runner let it claim browser turns and fail them with the digest/profile
# guard (observed 01:25:51Z, run azvzquth..., durable cause named). The agent
# therefore gets its own deployment/cert queue "a2a"; the Dot deployment stays
# single-consumer (as-runner only). A2A legs exercise the same plane, different
# queue - this is a recorded row constraint, not a silenced feature.
leaf("agent", tenant, "a2a")
leaf("foreign", foreign_tenant, "dot")
leaf("runner", tenant, "dot")

# The dev-JWT signer is an ed25519 seed; the agent verifies tokens with the
# matching raw public key, so both are derived here and never hard-coded.
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
jwt_seed = secrets.token_bytes(32)
signing_public = Ed25519PrivateKey.from_private_bytes(jwt_seed).public_key()
write("jwt.seed", jwt_seed)
write("config/public-keys.json", json.dumps({"dev-1": base64.b64encode(
    signing_public.public_bytes_raw()).decode()}))
note_bearer = secrets.token_urlsafe(40)
tools_config = {"connections": {"lane-notes": "http://127.0.0.1:%d/mcp" % args.notes_port}, "tools": {
    # A side effect on the employee's note store is the PDF's HITL case (L942): the
    # Tool GW must stop the call and ask the employee, not the agent, to sign it.
    "record_note": {"name": "record_note", "connection": "lane-notes", "sideeffecting": True,
                    "requiresapproval": True},  # ToolSpec has no json tags: the name must match the field
    "list_notes": {"name": "list_notes", "connection": "lane-notes", "sideeffecting": False}}}
# The delegable agent's public A2A Agent Card is an operator document: the gateway
# publishes it only when named by AHP_EXEC_AGENT_CARD_FILE, and refuses a card that
# leaks the internal face or a DSN. It advertises ONLY the public face.
public_a2a_url = "https://127.0.0.1:%d" % args.backend_port + "/a2a/public"
write("config/agent-card.json", json.dumps({
    "name": "AHP lane delegated agent",
    "description": "Delegable AHP agent reached through the AHP gateway over A2A.",
    "version": "1.0.0",
    "url": public_a2a_url,
    "preferredTransport": "JSONRPC",
    "protocolVersion": "1.0",
    "supportedInterfaces": [{"url": public_a2a_url, "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}],
    "capabilities": {"streaming": False, "extensions": [{"uri": "https://ahp.cloud.ru/extensions/agent/v1",
                                                       "required": True,
                                                       "params": {"history": "owned", "steering": "queue",
                                                                  "run_timeout_ms": 300000}}]},
    "defaultInputModes": ["text/plain"], "defaultOutputModes": ["text/plain"],
    "skills": [{"id": "execute-task", "name": "Execute a durable A2A task"}]}, indent=2) + "\n")

operator_password = secrets.token_urlsafe(24)
foreign_password = secrets.token_urlsafe(24)
admin_password = secrets.token_urlsafe(24)
permissions = ["space:access", "state", "model", "execution", "identity:cancel", "identity:delegate",
               "identity:exchange", "registry", "tools", "artifacts", "memory", "compute", "workspace"]

copied = ("AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD", "AHP_S3_ENDPOINT", "AHP_S3_REGION",
          "AHP_VAULT_ADDR", "AHP_VAULT_DEV_TOKEN", "AHP_LLM_BASE_URL", "AHP_LLM_MODEL", "AHP_LLM_VAULT_PATH",
          "AHP_REDIS_ADDR")
backend = {key: lane_env[key] for key in copied if key in lane_env}
backend.update({key: "1" for key in ("AHP_ENABLE_BROWSER", "AHP_ENABLE_CONTROL_EXCHANGE", "AHP_ENABLE_DATA",
                                     "AHP_ENABLE_EXECUTION", "AHP_ENABLE_MODEL", "AHP_ENABLE_MODEL_COST",
                                     "AHP_ENABLE_TOOLS", "AHP_ENABLE_REGISTRY", "AHP_ENABLE_WORKSPACE",
                                     "AHP_ENABLE_MEMORY", "AHP_ENABLE_COMPUTE", "AHP_COMPUTE_MOCK_BACKEND")})
backend.update({
    "AHP_DATABASE_URL": dsn, "AHP_TEST_DATABASE_URL": dsn, "AS_DATA_DATABASE_URL": dsn, "DATABASE_URL": dsn,
    "AHP_DB_NAME": database, "PGDATABASE": database, "AHP_ROLE": "all",
    "AHP_EXEC_MAX_TOKENS": "20000",
    # A generation reserves prompt + its completion ceiling. The per-task root
    # must also fund narrowed child slices and subsequent steps.
    "AHP_EXEC_TASK_TOKEN_LIMIT": "1000000",
    "AHP_UI_AIFACTORY": "1",
    "AHP_LISTEN": "127.0.0.1:%d" % args.backend_port,
    "AHP_TLS_CERT": str(instance / "certs/server.crt"), "AHP_TLS_KEY": str(instance / "certs/server.key"),
    "AHP_TLS_CA": str(instance / "certs/ca.crt"),
    "AHP_DEV_JWT_SEED": jwt_seed.hex(), "AHP_IAM_PROFILE": "local",
    # The registry supply-chain PKI (candidate 5af9ffa9) is out of the personal-Dot
    # lane scope: this stand publishes lane-authored configuration only, never
    # signed third-party skill artifacts, so the plane runs explicitly disabled.
    "AHP_REGISTRY_SUPPLY_CHAIN": "disabled",
    "AHP_IDENTITY_EXCHANGER_DEPLOYMENTS": "dot", "AHP_IDENTITY_DELEGATOR_DEPLOYMENTS": "dot",
    "AHP_STATE_ADMIN_DEPLOYMENTS": "dot",
    # DEF-DOTE2E-12: since DEF-H30-x the accepted turn carries its caller-stated max_steps
    # into the run payload, so as-runner (AHP_DELEGATE_TREE_BUDGET=1) charges every model
    # step through the Model tree API. With an empty reporter allowlist the backend denies
    # every caller (step 0 FORBIDDEN -> turn failed). The only legitimate tree reporter on
    # this stand is this lane's own runner workload; the foreign tenant is deliberately NOT
    # listed, so the two-user isolation leg keeps its teeth.
    "AHP_MODEL_TREE_REPORTER_WORKLOADS": "spiffe://ahp/tenant/%s/deployment/dot" % tenant,
    "AHP_S3_BUCKET": bucket, "AHP_S3_WORKSPACE_BUCKET": bucket, "AHP_EXEC_ARTIFACT_BUCKET": bucket,
    "AS_ARTIFACT_BUCKET": bucket,
    "AHP_MEMORY_BACKEND": "hindsight", "AHP_MEMORY_HINDSIGHT_URL": args.hindsight_url,
    "AHP_MEMORY_RUNTIME_DEPLOYMENTS": "dot", "AHP_MEMORY_CONTROL_DEPLOYMENTS": "dot",
    "AHP_TOOLS_CONFIG": json.dumps(tools_config), "AHP_TOOLS_DEV_BEARER": note_bearer,
    "AHP_EXEC_AGENT_CARD_FILE": str(instance / "config" / "agent-card.json"),
    # The browser speaks to ONE origin: this HTTPS edge forwards /v1/space/* and the human
    # plane prefixes to the Space runner, which is the only process that turns a human
    # session into a scoped task token.
    "AHP_SINGLE_ORIGIN_UPSTREAM": "http://127.0.0.1:%d" % args.runner_port,
})


def envfile(name, values):
    write(name, "".join("%s=%s\n" % (k, shlex.quote(str(v))) for k, v in values.items()))


envfile("backend.env", backend)
envfile("notes.env", {"AHP_NOTE_MCP_LISTEN": "127.0.0.1:%d" % args.notes_port,
                      "AHP_NOTE_MCP_BEARER": note_bearer, "AHP_NOTE_MCP_DIR": str(instance / "notes")})
envfile("agent.env", {"AHP_LISTEN": "127.0.0.1:%d" % args.agent_port, "AHP_AGENT_TENANT": tenant,
                      "AHP_EXEC_MAX_TOKENS": "20000",
                      "AHP_EXEC_TASK_TOKEN_LIMIT": "1000000",
                      "AHP_AGENT_DEPLOYMENT": "a2a", "AHP_AGENT_ID": "dot-personal",
                      "AHP_AGENT_PUBLIC_KEYS_FILE": str(instance / "config/public-keys.json"),
                      "AHP_AGENT_GATEWAY_WORKLOADS": "spiffe://ahp/tenant/%s/deployment/dot" % tenant,
                      "AHP_STATE_URL": "https://127.0.0.1:%d" % args.backend_port,
                      "AHP_MODEL_URL": "https://127.0.0.1:%d" % args.backend_port,
                      "AHP_TLS_CERT": str(instance / "certs/agent.crt"), "AHP_TLS_KEY": str(instance / "certs/agent.key"),
                      "AHP_TLS_CA": str(instance / "certs/ca.crt")})
# as-runner is the as-turn-runner of PDF rev3.2 L2044: it owns the Dot turn lifecycle
# and reaches every owner API over its OWN workload mTLS; the browser credential is
# exchanged for a scoped task token and never reaches State.
ORIGIN = "https://127.0.0.1:%d" % args.backend_port
runner = {
    "AS_LISTEN": "127.0.0.1:%d" % args.runner_port,
    "AS_WORKER_ID": "lane-dot-runner-" + run_id,
    "AS_STATE_URL": ORIGIN, "AS_IDENTITY_URL": ORIGIN, "AS_IDENTITY_EXCHANGE_URL": ORIGIN,
    "AS_MODEL_URL": ORIGIN, "AS_MODEL_COST_URL": ORIGIN, "AS_ARTIFACTS_URL": ORIGIN,
    "AS_REGISTRY_URL": ORIGIN, "AS_MEMORY_URL": ORIGIN, "AS_COMPUTE_URL": ORIGIN,
    "AS_WORKSPACE_URL": ORIGIN, "AS_TOOLS_URL": ORIGIN,
    "AS_AUTH_ISSUER": "ahp", "AS_AUTH_AUDIENCE": "ahp-browser", "AS_TASK_AUDIENCE": "ahp",
    "AS_AUTH_KEYS": "dev-1=" + base64.b64encode(signing_public.public_bytes_raw()).decode(),
    "AS_WORKLOAD_CERT": str(instance / "certs/runner.crt"),
    "AS_WORKLOAD_KEY": str(instance / "certs/runner.key"),
    "AS_WORKLOAD_CA": str(instance / "certs/ca.crt"),
    "AS_RUN_GRANT_CAPS": "state,identity:cancel,identity:delegate,execution,model,tools,artifacts,"
                         "memory,registry,compute,workspace",
    "AS_RUN_GRANT_TTL": "15m", "AS_CLAIM_INTERVAL_MS": "500", "AS_ENGINE_DURABLE": "1",
    "AHP_UI_AIFACTORY": "1",
    "AHP_EXEC_MAX_TOKENS": "20000", "AS_MODEL_MAX_TOKENS": "20000",
    "AHP_EXEC_TASK_TOKEN_LIMIT": "1000000",
    "AHP_DELEGATE_TREE_BUDGET": "1",
    # The delegable agent is reached through the gateway's public face, whose card the
    # same backend publishes; the hop presents the run's own task token over mTLS.
    "EMIL_BASE_URL": ORIGIN, "EMIL_CLIENT_CERT": str(instance / "certs/runner.crt"),
    "EMIL_CLIENT_KEY": str(instance / "certs/runner.key"), "EMIL_CLIENT_CA": str(instance / "certs/ca.crt"),
    "CARD_CACHE_TTL_S": "0",
}
envfile("runner.env", runner)
write("login.json", json.dumps({"tenant": tenant, "subject": "dot-owner",
                                "email": "owner@" + tenant + ".local", "password": operator_password,
                                "foreign_tenant": foreign_tenant, "foreign_subject": "dot-stranger",
                                "foreign_email": "stranger@" + foreign_tenant + ".local",
                                "foreign_password": foreign_password,
                                "admin_email": "admin@" + tenant + ".local", "admin_password": admin_password}))
write("users.json", json.dumps([
    {"id": tenant + "-operator", "tenant": tenant, "subject": "dot-owner", "display_name": "Dot owner",
     "email": "owner@" + tenant + ".local", "username": "owner@" + tenant + ".local",
     # tools:approve is what lets THIS employee sign a Tool GW approval (L942: the
     # approval goes to the employee directly). The tenant admin deliberately does
     # not hold it, which is the negative the HITL scenario uses.
     "password": operator_password, "roles": ["developer", "owner"],
     "permissions": permissions + ["tools:approve"]},
    {"id": tenant + "-admin", "tenant": tenant, "subject": "dot-tenant-admin", "display_name": "Tenant admin",
     "email": "admin@" + tenant + ".local", "username": "admin@" + tenant + ".local", "password": admin_password,
     "roles": ["admin"], "permissions": permissions + ["registry:admin"]},
    {"id": foreign_tenant + "-stranger", "tenant": foreign_tenant, "subject": "dot-stranger",
     "display_name": "Foreign stranger", "email": "stranger@" + foreign_tenant + ".local",
     "username": "stranger@" + foreign_tenant + ".local", "password": foreign_password,
     "roles": ["developer"], "permissions": permissions}]))
print(json.dumps({"instance": str(instance), "run_id": run_id, "tenant": tenant, "bucket": bucket,
                  "database": database, "status": "PREPARED_NOT_STARTED"}))

write("scope.json", json.dumps({"run_id": run_id, "tenant": tenant, "foreign_tenant": foreign_tenant,
                             "deployment": "dot", "subject": "dot-owner", "database": database, "bucket": bucket,
                             "a2a_deployment": "a2a",
                             "queue_isolation_note": "A2A agent replica runs in deployment 'a2a' so the Dot 'dot' queue has exactly one consumer (as-runner); root 'not two consumers in one queue' constraint, recorded as a row limitation",
                              "origin": "https://127.0.0.1:%d" % args.backend_port,
                              "agent_origin": "https://127.0.0.1:%d" % args.agent_port,
                              "runner_origin": "http://127.0.0.1:%d" % args.runner_port,
                              "notes_origin": "http://127.0.0.1:%d" % args.notes_port,
                              "hindsight_url": args.hindsight_url, "iam_profile": "local-development",
                              "compute_profile": "mock-microvm", "status": "PREPARED_NOT_STARTED"}, indent=2) + "\n")
