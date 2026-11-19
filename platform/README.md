# AHP Go Platform

Fresh implementation of the AHP (Agentic Host Platform) — Go backend, separated service roles, PostgreSQL/pgvector, Redis, MinIO, Vault.

## Requirements

- Go 1.26+
- Docker + Docker Compose (infra containers)
- Node 22+ (test/run scripts)

## Quick Start

```bash
# 1. Copy env template and fill in credentials
cp .env.example .env

# 2. Start infrastructure (PostgreSQL, Redis, MinIO, Vault)
npm run infra:up

# 3. Build the application binary
go build -o bin/ahp ./cmd/ahp

# 4. Run all roles (dev mode, single port)
npm run service:run

# 5. Quick HTTP smoke check (in another terminal)
npm run smoke
```

## Service Roles

The `ahp` binary runs one of: `state`, `identity`, `model`, or `all`.

| Role | Port (dev) | Purpose |
|------|-----------|---------|
| state | 8081 | Thread/Run/Checkpoint/KV State API |
| identity | 8082 | Grants, tokens, authorization |
| model | 8083 | Budget management, model gateway |
| all | 8080 | Development co-location |

### Single Role

```bash
./bin/ahp --role state --listen :8081
```

### With TLS (mTLS)

```bash
npm run certs:generate
./bin/ahp --role state --listen :8081 \
  --tls-cert infra/certs/server.pem \
  --tls-key infra/certs/server-key.pem \
  --tls-ca infra/certs/ca.pem
```

## Docker Compose

```bash
# Build app image
npm run app:build

# Start app services alongside infra
npm run app:up

# Stop app
npm run app:down
```

App ports: state 18081, identity 18082, model 18083, all-in-one 18080.

## Testing

```bash
# Unit + vet (requires running infra)
npm run check

# HTTP liveness (requires running service)
npm run smoke

# E2E conformance (requires running service)
npm run test:e2e

# Chaos: SIGKILL restart, graceful shutdown
npm run test:chaos

# Load: latency, RSS, checkpoint throughput
npm run test:load
```

## Observability

OpenTelemetry tracing is built in. Set:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
export OTEL_EXPORTER_OTLP_INSECURE=true
```

Without an endpoint, traces are written as JSON to stderr (offline/development mode).

## Deployment Manifests

- **Tier 0**: Docker Compose (this repo)
- **Tier 1**: systemd units — `infra/manifests/tier1/`
- **Tier 2A**: Kubernetes base — `infra/manifests/tier2a/`
- **Tier 2B**: HA production — `infra/manifests/tier2b/` (HPA, NetworkPolicy, PDB, RBAC)

## Architecture

See `docs/implementation.md` for the full task breakdown, `docs/coverage.md` for the requirements-to-evidence matrix, `docs/acceptance-pack.md` for the consolidated delivery acceptance pack (per-criterion evidence + explicit open gates), and `docs/progress.md` for the running log.

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `AHP_DATABASE_URL` | PostgreSQL connection string |
| `AHP_ROLE` | Service role (state/identity/model/all) |
| `AHP_LISTEN` | Listen address |
| `AHP_TLS_CERT` / `AHP_TLS_KEY` / `AHP_TLS_CA` | mTLS files |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC collector |
| `AHP_VERBOSE` | Debug logging (1=on) |
