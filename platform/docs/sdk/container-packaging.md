# SDK container packaging gate

Build context is `sdk/python`, so its `.dockerignore` applies. The Dockerfile
copies `ahp_state`, `ahp_memory`, and `ahp_clients`; builds one SDK wheel; installs
that exact wheel with the optional LangGraph runtime; then checks the selected
agent's source before the image can finish. All three SDK test trees and the
root SDK tests directory are excluded from context, wheel and image. The
pyproject package inclusion/exclusion rules remain owned by the runtime block.

```sh
docker build --label com.docker.compose.project=ahp-handbook \
  -f sdk/python/Dockerfile \
  --build-arg AGENT_DIR=examples/billing_agent \
  --build-arg AGENT_MODULE=examples.billing_agent.agent \
  -t ahp-handbook-sdk-billing:review sdk/python

docker run --rm --label com.docker.compose.project=ahp-handbook \
  -i -w /tmp ahp-handbook-sdk-billing:review python -I - \
  < sdk/python/tests/container_image_probe.py

docker run --rm --label com.docker.compose.project=ahp-handbook \
  ahp-handbook-sdk-billing:review python -m ahp_state.container --help
```

The import probe runs outside `/app` in isolated Python mode. Every import must
resolve to `site-packages`; copied source cannot mask a missing wheel package.
The probe compares installed SDK Python files to the retained wheel under
`/opt/ahp-sdk-wheels` and checks neither contains SDK tests. It imports the State
runtime, Memory sync/async and all public client families including Artifacts.

A negative build with a temporary agent importing a direct database driver must
fail at the unchanged build-time contract gate (`no-direct-db`). Do not add ignore
rules or bypass the gate to make a sample build pass. A source check is a lint,
not a sandbox: network policy and runtime authorization remain separate.

Image import, package-content and `--help` evidence proves packaging and startup
argument parsing. It does not prove a task was claimed, an LLM ran, or deployment
onto the main AHP stand. The exact source hash, image ID, wheel SHA and positive/
negative build logs belong in the private acceptance report.
