"""Framework-neutral State API example; install the base SDK without LangGraph.

A private platform hand-out contains the workload mTLS paths, current task token
and the fence of an already claimed run. Never commit or print that file.
"""
import asyncio
import json
import sys
from ahp_state import mtls_client


async def main(path: str) -> None:
    with open(path, encoding="utf-8") as stream:
        hand = json.load(stream)
    async with mtls_client(hand) as client:
        namespace = ["standalone-example"]
        item = await client.get_kv(namespace, "from-go")
        assert item["value"] == {"language": "go", "value": 42}
        assert item["version"] == 1
        # Preserve all three on retry: operation key, expected version, body.
        await client.put_kv(hand["fence"], namespace, "from-python",
                            "standalone-python-v1", 0, {"language": "python", "value": 42})
        own = await client.get_kv(namespace, "from-python")
        assert own["version"] == 1
    print("standalone Python State SDK PASS")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
