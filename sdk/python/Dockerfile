# Template for a custom agent image (PDF rev 3.2 §2.3, §4.2).
#
# The image owns the graph; the SDK owns the platform contract. Two things are
# required of any agent: a module exposing build(ctx), and a check pass.
#
#   docker build -f sdk/python/Dockerfile --build-arg AGENT_DIR=my_agent --build-arg AGENT_MODULE=my_agent.agent -t my-agent sdk/python
#
# The last argument is the build context and it has to be this directory: the
# COPY lines below are context-relative, and .dockerignore - the thing that keeps
# all SDK tests, including raw-socket/PostgreSQL harnesses, out of the
# image - is only read at the context root. A context of `.` (the repository)
# fails at `COPY pyproject.toml` and would not read the ignore file at all.
FROM python:3.11-slim

ARG AGENT_DIR=my_agent
ARG AGENT_MODULE=my_agent.agent

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    AHP_AGENT_ENTRY=build

WORKDIR /app

# 1. the SDK (in this repo: from source; in production: from the platform index)
COPY pyproject.toml ./
COPY ahp_state ./ahp_state
COPY ahp_memory ./ahp_memory
COPY ahp_clients ./ahp_clients
# Keep the exact installed wheel for provenance and package-content checks.
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /opt/ahp-sdk-wheels . \
    && for sdk_wheel in /opt/ahp-sdk-wheels/*.whl; do \
         pip install --no-cache-dir "${sdk_wheel}[langgraph]"; \
       done

# 2. the user's agent
COPY ${AGENT_DIR}/ /app/${AGENT_DIR}/
ENV AHP_AGENT_MODULE=${AGENT_MODULE}

# 3. the contract gate must pass at build time, not at first run
RUN python -m ahp_state.check /app/${AGENT_DIR}

# The platform mounts the handshake (CA, client cert/key, task token) and sets
# AHP_STATE_HANDSHAKE; leases, heartbeats and shutdown are the entry point's job.
CMD ["python", "-m", "ahp_state.container"]
