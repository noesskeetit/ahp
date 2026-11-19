"""Recovery-only Tool API protocol fixture; never a production connector.

The test server requires mTLS, commits a non-idempotent receipt and can withhold
the response. It deliberately ignores the operation key: only the SDK's durable
intent prevents a second effect after SIGKILL. Use the shipped owner client and
hand-out transport so the ordinary container startup check remains enabled.
"""
import json
import os

from ahp_clients.tools import AsyncToolsClient
from ahp_state.transport import mtls_client


async def charge(tool: str, args: dict) -> dict:
    with open(os.environ["AHP_STATE_HANDSHAKE"], encoding="utf-8") as handle:
        handshake = json.load(handle)
    async with mtls_client(handshake) as client:
        tools = AsyncToolsClient(os.environ["AHP_RECOVERY_WORLD_URL"],
                                 client.authorization_token,
                                 http_client=client.http_client)
        return await tools.call("sdk-recovery-http-effect", tool, args)
