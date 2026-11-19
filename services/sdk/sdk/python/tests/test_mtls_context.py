"""Why the SDK ships mtls_context(): httpx drops cert= when verify is a string.

Measured on the wire, so it holds on any TLS stack: a throwaway front that
requires a client certificate answers whether it saw one and with which TLS
version it negotiated. The cross-language live leg is
TestMTLSParityGoAndPythonOnOneFront (tests/sdk/mtls_parity_test.go).

Certificate material comes from AHP_MTLS_TEST_CERT_DIR when that directory holds
ca.pem/server.pem/server_key.pem, otherwise it is minted here. The environment
hook exists so the same measurement also runs on an interpreter whose TLS stack
matters for the verdict (CPython/OpenSSL, TLS 1.3) but that lacks
``cryptography`` for throwaway certificates.
"""
import datetime
import os
import socket
import ssl
import threading
from pathlib import Path

import httpx
import pytest

from ahp_clients import mtls_context

_MATERIAL_NAMES = ("ca", "server", "server_key")


def _material_from_environment():
    """Throwaway material supplied by the harness (see module docstring)."""
    directory = os.environ.get("AHP_MTLS_TEST_CERT_DIR", "").strip()
    if not directory:
        return None
    source = Path(directory)
    paths = {name: source / (name + ".pem") for name in _MATERIAL_NAMES}
    if not all(path.is_file() for path in paths.values()):
        return None
    return {"ca": str(paths["ca"]), "server": str(paths["server"]),
            "server_key": str(paths["server_key"]),
            "client": str(paths["server"]), "client_key": str(paths["server_key"])}


def _material_minted(tmp_path):
    pytest.importorskip("cryptography", reason="throwaway certificate material")
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "mtls-test-ca")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - datetime.timedelta(hours=1))
          .not_valid_after(now + datetime.timedelta(hours=1))
          .add_extension(x509.BasicConstraints(True, None), critical=True)
          .add_extension(x509.KeyUsage(False, False, False, False, False, True, True, False, False),
                         critical=True)
          # OpenSSL >= 3 rejects a leaf whose issuer has no SKI/AKI pairing
          # ("Missing Authority Key Identifier"); LibreSSL does not, so a minter
          # without these extensions passes on macOS host Python and fails on CI.
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
          .sign(ca_key, hashes.SHA256()))
    ca_ski = ca.extensions.get_extension_for_class(x509.SubjectKeyIdentifier).value.digest
    leaf = (x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "mtls-test-workload")]))
            .issuer_name(ca_name).public_key(leaf_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(hours=1))
            .not_valid_after(now + datetime.timedelta(hours=1))
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
            .add_extension(x509.KeyUsage(True, False, True, False, False, False, False, False, False),
                           critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH,
                                                 ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier(ca_ski, None, None), critical=False)
            .sign(ca_key, hashes.SHA256()))
    paths = {}
    for name, data in (("ca", ca.public_bytes(serialization.Encoding.PEM)),
                       ("server", leaf.public_bytes(serialization.Encoding.PEM)),
                       ("server_key", leaf_key.private_bytes(serialization.Encoding.PEM,
                                                             serialization.PrivateFormat.PKCS8,
                                                             serialization.NoEncryption()))):
        path = tmp_path / (name + ".pem")
        path.write_bytes(data)
        paths[name] = str(path)
    paths["client"] = paths["server"]
    paths["client_key"] = paths["server_key"]
    return paths


def _material(tmp_path):
    return _material_from_environment() or _material_minted(tmp_path)


def _saw_client_cert(paths, client_context):
    """Handshake against a front that requires a client certificate; return what it saw."""
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(paths["server"], paths["server_key"])
    server.load_verify_locations(cafile=paths["ca"])
    server.verify_mode = ssl.CERT_REQUIRED
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(10)
    observed = {}

    def serve():
        try:
            connection, _ = listener.accept()
        except OSError as error:  # pragma: no cover - diagnostics only
            observed["accept_error"] = repr(error)
            return
        try:
            wrapped = server.wrap_socket(connection, server_side=True)
            observed["peer_cert"] = bool(wrapped.getpeercert())
            observed["tls_version"] = wrapped.version()
            wrapped.sendall(b"ok")
            wrapped.close()
        except Exception as error:  # noqa: BLE001 - the refusal IS the observation
            observed["server_error"] = type(error).__name__
        finally:
            connection.close()
            listener.close()

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    client_socket = socket.create_connection(listener.getsockname(), timeout=10)
    client_connected = False
    try:
        wrapped = client_context.wrap_socket(client_socket, server_hostname="localhost")
        wrapped.recv(16)
        wrapped.close()
        client_connected = True
    except Exception:  # noqa: BLE001 - a refused handshake is a valid outcome
        pass
    finally:
        client_socket.close()
    worker.join(10)
    return observed, client_connected


def test_mtls_context_presents_the_client_certificate_on_the_wire(tmp_path):
    paths = _material(tmp_path)
    context = mtls_context(paths["ca"], paths["client"], paths["client_key"])
    observed, connected = _saw_client_cert(paths, context)
    assert connected, "the helper context could not complete an mTLS handshake"
    assert observed.get("peer_cert") is True, observed
    assert observed.get("tls_version") in ("TLSv1.2", "TLSv1.3"), observed


def test_string_verify_with_cert_argument_arrives_without_a_certificate(tmp_path):
    """Exactly the standalone scenario's construction, measured at the handshake."""
    paths = _material(tmp_path)
    with pytest.warns(DeprecationWarning):
        context = httpx.create_ssl_context(verify=paths["ca"], cert=(paths["client"], paths["client_key"]))
    observed, connected = _saw_client_cert(paths, context)
    assert observed.get("peer_cert") is not True, (
        "httpx now applies cert= with a string verify; revisit the SDK helper note and this pin")
    assert not connected, "a certificate-less client should not complete an mTLS handshake"


@pytest.mark.parametrize("args", [("", "client.pem"), ("ca.pem", ""), ("ca.pem", None)])
def test_helper_refuses_to_build_a_certificate_less_context(args):
    with pytest.raises(ValueError):
        mtls_context(*args)
