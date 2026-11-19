"""Caller-owned mTLS context for the AHP owner APIs.

The owner faces demand a REAL mutual TLS: the workload certificate is the
deployment identity, so a client that presents none is refused during the
handshake. This helper exists because the convenient-looking httpx form is
silently broken for exactly that case:

    httpx.Client(verify="/path/ca.pem", cert=("/path/client.pem", "/path/client.key"))

`httpx.create_ssl_context` returns as soon as it sees a string `verify`, so the
deprecated `cert=` argument is never applied. The connection then carries no
client certificate, and a Go server answers the handshake with
``TLSV13_ALERT_CERTIFICATE_REQUIRED`` (measured: the server logs
"tls: client didn't provide a certificate"). Nothing about the platform, the
certificate material or TLS 1.3 is at fault, and the failure is not a reason to
weaken mTLS: verification and hostname checking stay exactly as strict here.

    client = httpx.Client(verify=mtls_context(ca, cert, key), trust_env=False)
"""
from __future__ import annotations

import ssl

__all__ = ["mtls_context"]


def mtls_context(cafile: str, certfile: str, keyfile: str | None = None, *,
                 password: str | None = None,
                 min_version: ssl.TLSVersion = ssl.TLSVersion.TLSv1_2) -> ssl.SSLContext:
    """Return a verifying SSLContext that actually presents the client certificate.

    ``cafile`` is the trust bundle that signed the owner's certificate; it is also
    what the owner uses to accept this workload, so it must be the platform CA,
    not an empty pool. Pass the result as httpx's ``verify`` argument; httpx uses
    a supplied ``SSLContext`` verbatim.
    """
    for name, value in (("cafile", cafile), ("certfile", certfile)):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"mtls_context: {name} is required (mTLS is not optional on owner faces)")
    context = ssl.create_default_context(cafile=cafile)
    # create_default_context already sets CERT_REQUIRED + check_hostname; assert it
    # so a future relaxation cannot slip in through this shared helper.
    if context.verify_mode is not ssl.CERT_REQUIRED or not context.check_hostname:
        raise RuntimeError("mtls_context: refusing to build a non-verifying context")
    context.load_cert_chain(certfile, keyfile, password)
    context.minimum_version = min_version
    return context
