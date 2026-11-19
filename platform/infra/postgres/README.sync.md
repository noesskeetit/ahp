# Local synchronous State storage profile

PDF rev3.2 §2.5 guarantee6 needs acknowledged writes to survive storage-node loss.
One PostgreSQL with `synchronous_commit=on` and no synchronous standby does not
prove this. `scripts/local-pg-sync.mjs` provides a bounded local two-node profile.

Run only against the explicitly owned AHP Docker infrastructure. Setup requires
its private infrastructure JSON, Docker context `ahp-handbook-prof`, an already
available PostgreSQL image **digest**, and a new absolute private control path:

```sh
node scripts/local-pg-sync.mjs setup \
  --context ahp-handbook-prof \
  --infrastructure /private/ahp/linux-infrastructure.json \
  --image sha256:PINNED_IMAGE_DIGEST \
  --control /private/ahp/pg-sync/control.json
node scripts/local-pg-sync.mjs status --control /private/ahp/pg-sync/control.json
```

The image must provide PostgreSQL17, pg_basebackup, pg_rewind and gosu. The tested
image is `sha256:d76cfda85dcabe4c220611fe59b289078b1b82ea07479cd91ab48d4f2933df17`
(17.11, arm64). No image pull, published PG port or foreign resource cleanup.
Each pair has unique names and separate labelled volumes. Actions check exact
owner/project labels, image and data-volume identity. Each PG is limited to
256MiB/1 CPU; bounded operator jobs use up to384MiB. Credentials/control are
written0600 in a0700 directory, never printed. Keep that directory outside Git.

The client DSN is in private `client.env`: both hosts, `target_session_attrs=read-write`.
Use it only in the privileged State/control-plane process; agents keep State API
mTLS/task credentials. The profile uses a dedicated replication role, `remote_apply`
and a named synchronous peer. Boot flags are also persisted: pg_rewind invokes
single-user recovery without the original postmaster arguments, so WAL hints,
retention and memory settings must agree in files.

Manual controlled failover:

```sh
node scripts/local-pg-sync.mjs crash-and-promote --control /private/ahp/pg-sync/control.json
node scripts/local-pg-sync.mjs rejoin-old-primary --control /private/ahp/pg-sync/control.json
```

The old writer has restart policy `no`; its container must be confirmed stopped
before promotion. The promoted node retains its synchronous requirement, so new
mutation ACKs wait until a peer returns. `rejoin-old-primary` rewinds the stopped
owned data volume and starts it as a standby. It never starts the old timeline
as a second writer. `stop` stops only the named pair and preserves volumes.
`pause-standby`/`resume-standby` are destructive-test phases, not operating modes.

Opt-in API acceptance: `TestStateAPIConfirmedCutSurvivesPrimaryLossRealState`
(`state_integration` tag). Set private `AHP_PG_SYNC_DATABASE_URL` and absolute
`AHP_PG_SYNC_CONTROL_DIR`; the operator must answer its bounded file requests
for pause/resume/crash/rejoin. No HTTP control server is created. The test uses
actual State API/mTLS/JWT/PG; Identity policy is explicitly the cEnv fixture.
It checks timeout and idempotent replay with replica absent, exact acknowledged
checkpoint/stream/KV after primary loss, new writes after rejoin and stale epoch.

Both PostgreSQL nodes remain on the same Mac/Docker host. This exercises process/
container failure and manual recovery; it does not prove independent disk/host
failure tolerance, automatic HA or99.9% availability. Original single-PG data
must remain retained during any offline migration into the pair.

References: [PostgreSQL synchronous replication](https://www.postgresql.org/docs/17/warm-standby.html),
[WAL/synchronous_commit](https://www.postgresql.org/docs/17/runtime-config-wal.html),
[pg_rewind prerequisites](https://www.postgresql.org/docs/17/app-pgrewind.html),
[pgx multi-host configuration](https://pkg.go.dev/github.com/jackc/pgx/v5/pgconn#ParseConfig).
