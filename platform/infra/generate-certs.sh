#!/usr/bin/env bash
# infra/generate-certs.sh — dev-only mTLS certificate chain for local testing.
#
# Generates:
#   ca/ca.crt, ca/ca.key
#   server/server.crt, server/server.key  (CN=localhost, SANs: localhost, 127.0.0.1)
#   client/client.crt, client/client.key  (CN=ahp-client, O=ahp-workload)
#
# Output: infra/certs/ (git-ignored). Idempotent: safe to re-run.
# NO PRODUCTION USE: keys are ephemeral, no CRL/OCSP, 1-year validity.

set -euo pipefail

CERT_DIR="$(cd "$(dirname "$0")" && pwd)/certs"
DAYS=365
SUBJ_BASE="/C=RU/ST=Moscow/L=Moscow/O=AHP/OU=Development"
# Workload identity (SPIFFE ID) carried in the client cert URI SAN.
# The server (pkg/transport workloadScope) requires the client cert to carry an
# EXACT URI of the form spiffe://ahp/tenant/<t>/deployment/<d>. Override via env.
AHP_WORKLOAD_TENANT="${AHP_WORKLOAD_TENANT:-dev}"
AHP_WORKLOAD_DEPLOYMENT="${AHP_WORKLOAD_DEPLOYMENT:-dev}"
SPIFFE_ID="spiffe://ahp/tenant/${AHP_WORKLOAD_TENANT}/deployment/${AHP_WORKLOAD_DEPLOYMENT}"

mkdir -p "$CERT_DIR/ca" "$CERT_DIR/server" "$CERT_DIR/client"

# --- CA ---
if [ ! -f "$CERT_DIR/ca/ca.crt" ]; then
  echo "[certs] generating CA key pair..."
  openssl genpkey -algorithm RSA -out "$CERT_DIR/ca/ca.key" -pkeyopt rsa_keygen_bits:4096 2>/dev/null
  openssl req -new -x509 -days "$DAYS" -key "$CERT_DIR/ca/ca.key" \
    -out "$CERT_DIR/ca/ca.crt" \
    -subj "$SUBJ_BASE/CN=AHP-Dev-CA"
else
  echo "[certs] CA exists, skipping"
fi

# --- Server cert (for ahp service) ---
if [ ! -f "$CERT_DIR/server/server.crt" ]; then
  echo "[certs] generating server key pair (localhost, 127.0.0.1)..."
  openssl genpkey -algorithm RSA -out "$CERT_DIR/server/server.key" -pkeyopt rsa_keygen_bits:2048 2>/dev/null
  openssl req -new -key "$CERT_DIR/server/server.key" \
    -out "$CERT_DIR/server/server.csr" \
    -subj "$SUBJ_BASE/CN=localhost"
  openssl x509 -req -days "$DAYS" -in "$CERT_DIR/server/server.csr" \
    -CA "$CERT_DIR/ca/ca.crt" -CAkey "$CERT_DIR/ca/ca.key" -CAcreateserial \
    -out "$CERT_DIR/server/server.crt" \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n") 2>/dev/null
  rm -f "$CERT_DIR/server/server.csr"
else
  echo "[certs] server cert exists, skipping"
fi

# --- Client cert (for mTLS workload identity) ---
if [ ! -f "$CERT_DIR/client/client.crt" ]; then
  echo "[certs] generating client key pair (workload identity)..."
  openssl genpkey -algorithm RSA -out "$CERT_DIR/client/client.key" -pkeyopt rsa_keygen_bits:2048 2>/dev/null
  openssl req -new -key "$CERT_DIR/client/client.key" \
    -out "$CERT_DIR/client/client.csr" \
    -subj "$SUBJ_BASE/CN=ahp-client/O=ahp-workload"
  openssl x509 -req -days "$DAYS" -in "$CERT_DIR/client/client.csr" \
    -CA "$CERT_DIR/ca/ca.crt" -CAkey "$CERT_DIR/ca/ca.key" -CAcreateserial \
    -out "$CERT_DIR/client/client.crt" \
    -extfile <(printf "subjectAltName=URI:${SPIFFE_ID}\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n") 2>/dev/null
  rm -f "$CERT_DIR/client/client.csr"
else
  echo "[certs] client cert exists, skipping"
fi

echo "[certs] done. Files in $CERT_DIR"
echo "  CA:     $CERT_DIR/ca/ca.crt"
echo "  Server: $CERT_DIR/server/server.crt + server.key"
echo "  Client: $CERT_DIR/client/client.crt + client.key"
echo "  Client workload identity (SPIFFE URI SAN): $SPIFFE_ID"
