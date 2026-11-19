#!/usr/bin/env bash
# infra/gen-client.sh — issue an additional mTLS client cert from the existing
# dev CA, carrying an arbitrary SPIFFE workload identity.
#
# Usage: infra/gen-client.sh <tenant> <deployment> <outdir>
#
# Requires infra/certs/ca/{ca.crt,ca.key} (see infra/generate-certs.sh).
# Idempotent: skips when <outdir>/client.crt already exists.
# Output is under infra/certs/ (git-ignored). DEV ONLY — no CRL/OCSP.

set -euo pipefail

TENANT="${1:?tenant required}"
DEPLOYMENT="${2:?deployment required}"
OUTDIR="${3:-${AHP_CLIENT_CERT_DIR:?outdir required (arg 3 or AHP_CLIENT_CERT_DIR)}}"
# Default unchanged: with no AHP_CLIENT_CERT_DIR and an explicit arg 3 the resolved OUTDIR is byte-identical to before.

CERT_DIR="$(cd "$(dirname "$0")" && pwd)/certs"
# CA location is overridable so a harness in one checkout can mint client certs under
# ANOTHER checkout's stand CA (acceptance runs the test from the integration worktree
# against a stand raised in a different worktree). Defaults are byte-identical to
# before: with no AHP_CA_DIR/AHP_CA_FILE/AHP_CA_KEY the resolved paths equal CERT_DIR/ca.
CA_DIR="${AHP_CA_DIR:-$CERT_DIR/ca}"
CA_CRT="${AHP_CA_FILE:-$CA_DIR/ca.crt}"
CA_KEY="${AHP_CA_KEY:-$CA_DIR/ca.key}"
DAYS=365
SUBJ_BASE="/C=RU/ST=Moscow/L=Moscow/O=AHP/OU=Development"
SPIFFE_ID="spiffe://ahp/tenant/${TENANT}/deployment/${DEPLOYMENT}"

if [ ! -f "$CA_CRT" ] || [ ! -f "$CA_KEY" ]; then
  # Reproducibility guard: the commonest acceptance-run failure is an AHP_CA_DIR that
  # points at a directory from ANOTHER checkout (the control repo has no infra/certs at
  # all -- the stand CA lives in the PRODUCT worktree). Name the resolved paths so the
  # operator fixes the override instead of regenerating certificates.
  echo "[gen-client] CA missing: looked for cert=$CA_CRT key=$CA_KEY (AHP_CA_DIR=${AHP_CA_DIR:-<unset>} AHP_CA_FILE=${AHP_CA_FILE:-<unset>}); the canonical stand CA is $(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs/ca -- run infra/generate-certs.sh first if it is really absent" >&2
  exit 1
fi

mkdir -p "$OUTDIR"
if [ -f "$OUTDIR/client.crt" ]; then
  echo "[gen-client] $OUTDIR/client.crt exists, skipping"
  exit 0
fi

# mktemp -t <name> (BSD form) fails on GNU coreutils >= 8.30 with "too few X's";
# use the explicit-path form which works on both macOS and Linux.
TMPCSR="$(mktemp "${TMPDIR:-/tmp}/ahp-client.XXXXXX")"
CSR="${TMPCSR}.csr"
trap 'rm -f "$TMPCSR" "$CSR"' EXIT
KEY="$OUTDIR/client.key"
openssl genpkey -algorithm RSA -out "$KEY" -pkeyopt rsa_keygen_bits:2048 2>/dev/null
openssl req -new -key "$KEY" -out "$CSR" -subj "$SUBJ_BASE/CN=ahp-client/O=ahp-workload"
openssl x509 -req -days "$DAYS" -in "$CSR" \
  -CA "$CA_CRT" -CAkey "$CA_KEY" -CAcreateserial \
  -out "$OUTDIR/client.crt" \
  -extfile <(printf "subjectAltName=URI:${SPIFFE_ID}\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n") 2>/dev/null

echo "[gen-client] issued $OUTDIR/client.crt for $SPIFFE_ID"
