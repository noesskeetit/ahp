"""Framework-neutral workload mTLS hand-out transport (no LangGraph dependency)."""
import ssl
import httpx
from ahp_state.client import StateAPIClient


def mtls_client(handshake: dict) -> StateAPIClient:
    """Build a client from the candidate handshake file (container entry point).

    The file holds paths and a token; neither is ever logged.
    """
    ctx = ssl.create_default_context(cafile=handshake["ca_file"])
    if handshake.get("cert_file") and handshake.get("key_file"):
        ctx.load_cert_chain(handshake["cert_file"], handshake["key_file"])
    def factory():
        return httpx.AsyncClient(verify=ctx, timeout=30.0, trust_env=False)
    http = factory()
    return StateAPIClient(handshake["base_url"], token=handshake.get("task_token"),
                          http_client=http, owns_http_client=True, http_client_factory=factory)
