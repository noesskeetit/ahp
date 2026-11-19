# Custom agent examples

`billing_agent/agent.py` is the smallest agent that exercises the whole container
contract: a custom `StateGraph`, a durable question (`interrupt`), one external
effect through the journal, platform memory through `ctx.store`, and steering.

## Ship it

```python
# my_agent/agent.py
def build(ctx):                      # the only required entry point
    ...
    return graph.compile(checkpointer=ctx.saver, store=ctx.store)
```

```dockerfile
FROM python:3.11-slim
RUN pip install "ahp-state[langgraph]"              # the SDK, from the platform index
COPY my_agent/ /app/my_agent/
ENV PYTHONUNBUFFERED=1 AHP_AGENT_MODULE=my_agent.agent AHP_AGENT_ENTRY=build
CMD ["python", "-m", "ahp_state.container"]
```

The platform mounts the handshake (mTLS material + task token) and starts the
process; the container claims runs, executes them with `durability="sync"`, and
stops writing the moment its lease is gone. Nothing else in the image talks to the
State API.

## Gate it before the registry sees it

```bash
ahp-sdk-check my_agent          # or: python -m ahp_state.check my_agent
```

It fails the build when the agent keeps durable state somewhere else, opens a
socket or a file, spawns a process, calls a model SDK directly, hands the task
token to a sink that must not see it, or asks for a durability weaker than `sync`.
`--json` for CI, `--list-rules` to see what is enforced.
