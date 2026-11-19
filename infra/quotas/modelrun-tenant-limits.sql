-- Provider quota seed: EXPLICIT, FINITE limits for provider "modelrun".
--
-- Why this file exists: the quota gate treats a tenant that resolves to no row as
-- UNLIMITED (internal/model/quota.go, Admit: "no limit row ... no window at all").
-- That default is right for a deployment that never configured quotas and wrong
-- for a live platform, where "we think the default covers everybody" is exactly
-- the state an operator cannot audit. This seed therefore publishes a limit for
-- every tenant the canonical stand accepts, and keeps a finite provider-wide
-- default underneath so an un-named caller is bounded too.
--
-- Applied by scripts/apply-provider-quotas.sh (idempotent: pure UPSERTs on the
-- (provider, tenant) primary key). Limits are configuration; re-apply after a
-- schema rollback rather than editing rows by hand.
--
-- Rollback (removes the whole quota plane, everything else in ahp_model survives):
--   DROP TABLE IF EXISTS ahp_model.quota_admits;
--   DROP TABLE IF EXISTS ahp_model.quota_windows;
--   DROP TABLE IF EXISTS ahp_model.provider_quotas;

CREATE SCHEMA IF NOT EXISTS ahp_model;
CREATE TABLE IF NOT EXISTS ahp_model.provider_quotas (
  provider       text NOT NULL,
  tenant         text NOT NULL,
  rpm            bigint NOT NULL DEFAULT 0 CHECK (rpm >= 0),
  tpm            bigint NOT NULL DEFAULT 0 CHECK (tpm >= 0),
  window_seconds int    NOT NULL DEFAULT 60 CHECK (window_seconds BETWEEN 1 AND 3600),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, tenant)
);
-- The window and admission ledgers belong to the same plane; created here so the
-- seed is reproducible against a database that has not run the gateway migration
-- yet (identical DDL to internal/model.QuotaStore.Migrate, IF NOT EXISTS).
CREATE TABLE IF NOT EXISTS ahp_model.quota_windows (
  provider text   NOT NULL,
  tenant   text   NOT NULL,
  bucket   bigint NOT NULL,
  requests bigint NOT NULL DEFAULT 0 CHECK (requests >= 0),
  tokens   bigint NOT NULL DEFAULT 0 CHECK (tokens >= 0),
  PRIMARY KEY (provider, tenant, bucket)
);
CREATE TABLE IF NOT EXISTS ahp_model.quota_admits (
  provider        text   NOT NULL,
  tenant          text   NOT NULL,
  op_key          text   NOT NULL,
  bucket          bigint NOT NULL,
  requests        bigint NOT NULL CHECK (requests > 0),
  tokens          bigint NOT NULL CHECK (tokens >= 0),
  refunded_tokens bigint NOT NULL DEFAULT 0 CHECK (refunded_tokens >= 0),
  settled         boolean NOT NULL DEFAULT false,
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (provider, tenant, op_key)
);

-- Floor for any tenant without its own row. Finite on purpose: 0 in either
-- dimension would leave that dimension unbounded.
INSERT INTO ahp_model.provider_quotas (provider, tenant, rpm, tpm, window_seconds, updated_at)
VALUES ('modelrun', '', 20, 120000, 60, now())
ON CONFLICT (provider, tenant) DO UPDATE
   SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
       window_seconds = EXCLUDED.window_seconds, updated_at = now();

-- Acceptance tenant. Every journey and J4 leg runs here: 30 model attempts per
-- minute and 200k tokens per minute is comfortably above one journey (two model
-- steps, ~1.4k tokens) and still a bound a runaway loop hits within seconds.
INSERT INTO ahp_model.provider_quotas (provider, tenant, rpm, tpm, window_seconds, updated_at)
VALUES ('modelrun', 'djourney', 30, 200000, 60, now())
ON CONFLICT (provider, tenant) DO UPDATE
   SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
       window_seconds = EXCLUDED.window_seconds, updated_at = now();

-- Verifier probe tenants (the names verifiers actually use on the canonical
-- stand). Deliberately tight: a probe exists to measure refusal, and a probe that
-- shares the acceptance budget moves the thing it is measuring.
INSERT INTO ahp_model.provider_quotas (provider, tenant, rpm, tpm, window_seconds, updated_at)
VALUES ('modelrun', 'quotaprobe', 5, 20000, 60, now())
ON CONFLICT (provider, tenant) DO UPDATE
   SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
       window_seconds = EXCLUDED.window_seconds, updated_at = now();
INSERT INTO ahp_model.provider_quotas (provider, tenant, rpm, tpm, window_seconds, updated_at)
VALUES ('modelrun', 'dh21probe', 5, 20000, 60, now())
ON CONFLICT (provider, tenant) DO UPDATE
   SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
       window_seconds = EXCLUDED.window_seconds, updated_at = now();
INSERT INTO ahp_model.provider_quotas (provider, tenant, rpm, tpm, window_seconds, updated_at)
VALUES ('modelrun', 'h20probe', 5, 20000, 60, now())
ON CONFLICT (provider, tenant) DO UPDATE
   SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
       window_seconds = EXCLUDED.window_seconds, updated_at = now();


-- Verifier tenant on the external copy, stated instead of inherited: t-foreign is
-- the tenant foreign-principal legs run under, and a bound an auditor has to
-- derive ("it falls back to the provider default") is not a published bound.
INSERT INTO ahp_model.provider_quotas (provider, tenant, rpm, tpm, window_seconds, updated_at)
VALUES ('modelrun', 't-foreign', 10, 60000, 60, now())
ON CONFLICT (provider, tenant) DO UPDATE
   SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
       window_seconds = EXCLUDED.window_seconds, updated_at = now();

-- The SECOND COPY. This is the row set that was missing: routing can send a turn
-- to "modelrun-in-tenant" (restricted class, and now any server-side label route),
-- the quota gate charges the copy that was ROUTED (internal/execbridge/gateway_model.go),
-- and a copy with no limit row is admitted UNLIMITED -- the provider-wide
-- "modelrun" rows do not cover it, because limits are keyed by (provider, tenant).
-- Declared in infra/model/providers.json as rpm=10 / tpm=60000: half the external
-- copy, because this one is reserved for label-bound restricted work and a runaway
-- there must not be able to spend the provider relationship.
INSERT INTO ahp_model.provider_quotas (provider, tenant, rpm, tpm, window_seconds, updated_at)
VALUES ('modelrun-in-tenant', '', 10, 60000, 60, now())
ON CONFLICT (provider, tenant) DO UPDATE
   SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
       window_seconds = EXCLUDED.window_seconds, updated_at = now();
INSERT INTO ahp_model.provider_quotas (provider, tenant, rpm, tpm, window_seconds, updated_at)
VALUES ('modelrun-in-tenant', 'djourney', 10, 60000, 60, now())
ON CONFLICT (provider, tenant) DO UPDATE
   SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
       window_seconds = EXCLUDED.window_seconds, updated_at = now();
INSERT INTO ahp_model.provider_quotas (provider, tenant, rpm, tpm, window_seconds, updated_at)
VALUES ('modelrun-in-tenant', 't-foreign', 5, 30000, 60, now())
ON CONFLICT (provider, tenant) DO UPDATE
   SET rpm = EXCLUDED.rpm, tpm = EXCLUDED.tpm,
       window_seconds = EXCLUDED.window_seconds, updated_at = now();

-- Self-check, for EVERY provider this seed publishes.
--
-- The seed claims "no copy of this provider is reachable without a finite bound",
-- and a claim an operator must verify by hand is a claim that will be skipped. Two
-- rules, both fatal:
--   (1) every tenant this gateway has served has a finite effective limit on EVERY
--       published provider -- served tenants are read from the gateway's own
--       ledgers, consulted only when those relations exist;
--   (2) every published provider carries the copy-default row (tenant '') plus at
--       least one tenant row.
DO $$
DECLARE
  prov       text;
  bad        text;
  rows       int;
  defaults   int;
  tenantrows int;
BEGIN
  FOREACH prov IN ARRAY ARRAY['modelrun', 'modelrun-in-tenant'] LOOP
    CREATE TEMP TABLE IF NOT EXISTS seed_observed (tenant text PRIMARY KEY) ON COMMIT DROP;
    DELETE FROM seed_observed;

    INSERT INTO seed_observed
         SELECT DISTINCT tenant FROM ahp_model.quota_windows
          WHERE provider = prov AND tenant <> ''
     ON CONFLICT DO NOTHING;

    IF to_regclass('ahp_model.generations') IS NOT NULL THEN
      EXECUTE $q$INSERT INTO seed_observed
                    SELECT DISTINCT tenant FROM ahp_model.generations WHERE tenant <> ''
                 ON CONFLICT DO NOTHING$q$;
    END IF;
    IF to_regclass('ahp_model.budgets') IS NOT NULL THEN
      EXECUTE $q$INSERT INTO seed_observed
                    SELECT DISTINCT tenant FROM ahp_model.budgets WHERE tenant <> ''
                 ON CONFLICT DO NOTHING$q$;
    END IF;

    SELECT string_agg(o.tenant, ', ' ORDER BY o.tenant) INTO bad
      FROM seed_observed o
     WHERE coalesce((SELECT q.rpm FROM ahp_model.provider_quotas q
                      WHERE q.provider = prov AND q.tenant = ANY(ARRAY[o.tenant, ''])
                      ORDER BY (q.tenant = o.tenant) DESC LIMIT 1), 0) <= 0
       OR coalesce((SELECT q.tpm FROM ahp_model.provider_quotas q
                      WHERE q.provider = prov AND q.tenant = ANY(ARRAY[o.tenant, ''])
                      ORDER BY (q.tenant = o.tenant) DESC LIMIT 1), 0) <= 0;

    IF bad IS NOT NULL THEN
      RAISE EXCEPTION 'quota seed refused: provider % leaves tenant(s) with no finite effective limit: %', prov, bad;
    END IF;

    SELECT count(*) INTO rows FROM ahp_model.provider_quotas WHERE provider = prov;
    SELECT count(*) INTO defaults FROM ahp_model.provider_quotas WHERE provider = prov AND tenant = '' AND rpm > 0 AND tpm > 0;
    SELECT count(*) INTO tenantrows FROM ahp_model.provider_quotas WHERE provider = prov AND tenant <> '' AND rpm > 0 AND tpm > 0;
    IF defaults <> 1 THEN
      RAISE EXCEPTION 'quota seed incomplete: provider % needs exactly one finite copy-default row, found %', prov, defaults;
    END IF;
    IF tenantrows < 1 THEN
      RAISE EXCEPTION 'quota seed incomplete: provider % publishes no per-tenant limit row', prov;
    END IF;
    IF rows < 2 THEN
      RAISE EXCEPTION 'quota seed incomplete: provider % has % rows, want >= 2', prov, rows;
    END IF;
  END LOOP;
END $$;
