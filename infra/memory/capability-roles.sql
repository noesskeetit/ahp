-- Memory capability seed: the PUBLISHED, CLOSED vocabulary of the Memory plane,
-- bundled into three operator roles and grantable to named browser users.
--
-- Why this file exists (DEF-MEM-6): the plane authorises "memory" at its HTTP
-- face (internal/memory/memoryapi/api.go) and "memory:review", "memory:label",
-- "memory:delete", "memory:delete-subject" inside the store
-- (internal/memory/lifecycle.go). Those strings used to live only in Go source,
-- and provisioning accepted any string, so the usual failure was silent: a human
-- got "memory", could write and read, and then met an unexplainable
-- 403 "grant does not hold capability memory:review" on the review step -- the
-- PDF's review-before-activation and subject-erasure obligations simply did not
-- happen. A list an operator cannot read is not an interface.
--
-- This seed is the operator-facing copy. cmd/ahp/memory_capability_seed_test.go
-- fails the build if the two drift, and cmd/ahp/provision.go refuses to seed a
-- user whose "memory"-looking capability is not in the set.
--
-- This is NOT an ACL relaxation: every grant below is still checked by the same
-- Identity authority as every other plane, and the set is closed -- no wildcard,
-- no "memory:*", nothing broader than the four actions that exist.
--
-- Apply reproducibly (idempotent UPSERTs on browser_grants_pk):
--   scripts/apply-memory-capabilities.sh "accept,foreign"
--
-- Bundles (cumulative, least privilege first):
--   memory_reader    : memory
--   memory_reviewer  : + memory:review, memory:label
--   memory_custodian : + memory:delete, memory:delete-subject
--
-- Rollback for one user (grants only; the plane's own data is untouched):
--   DELETE FROM ahp_identity.browser_grants
--    WHERE kind = 'permission'
--      AND value IN ('memory','memory:review','memory:label','memory:delete','memory:delete-subject')
--      AND user_id = (SELECT user_id FROM ahp_identity.browser_credentials WHERE username = :'u');

-- Target usernames: passed by the apply script as an ARRAY literal.
--   psql -v memory_users="ARRAY['accept']" -f this-file.sql
WITH target(username) AS (
  SELECT unnest(:memory_users::text[])
),
bundle(capability, min_bundle) AS (
  VALUES
    ('memory',                'memory_reader'),
    ('memory:review',         'memory_reviewer'),
    ('memory:label',          'memory_reviewer'),
    ('memory:delete',         'memory_custodian'),
    ('memory:delete-subject', 'memory_custodian')
),
resolved AS (
  -- A username is resolved through browser_credentials, the only place a login
  -- name exists; browser_users deliberately does not carry it.
  SELECT u.user_id, u.tenant, c.username, b.capability
    FROM target t
    JOIN ahp_identity.browser_credentials c ON c.username = t.username
    JOIN ahp_identity.browser_users        u ON u.user_id  = c.user_id
   CROSS JOIN bundle b
)
INSERT INTO ahp_identity.browser_grants (user_id, kind, value)
SELECT user_id, 'permission', capability FROM resolved
ON CONFLICT (user_id, kind, value) DO UPDATE SET value = EXCLUDED.value;

-- Fail loud on a mistyped username: granting nothing is the failure mode this
-- whole change exists to eliminate, so it must not be possible here either.
--
-- The list travels through a session setting because psql does NOT substitute
-- :'var'/":var" inside a dollar-quoted body -- discovered by applying this seed,
-- which is exactly why the seed is executed in CI rather than only read.
SELECT set_config('ahp_memory_seed.missing', COALESCE((
         SELECT string_agg(t.username, ', ' ORDER BY t.username)
           FROM (SELECT unnest(:memory_users::text[]) AS username) t
          WHERE NOT EXISTS (SELECT 1 FROM ahp_identity.browser_credentials c WHERE c.username = t.username)
       ), ''), false);

DO $$
DECLARE
  missing text := current_setting('ahp_memory_seed.missing', true);
BEGIN
  IF COALESCE(missing, '') <> '' THEN
    RAISE EXCEPTION 'memory capability seed: unknown browser username(s): %', missing
      USING HINT = 'provision the human first (AHP_PROVISION_USERS_JSON), then re-apply';
  END IF;
END
$$;

-- Prove the result: every targeted user must hold all five capabilities (the
-- custodian bundle is cumulative), so an operator can audit with one query.
SELECT c.username, u.tenant, g.value
  FROM ahp_identity.browser_grants g
  JOIN ahp_identity.browser_users        u ON u.user_id = g.user_id
  JOIN ahp_identity.browser_credentials c ON c.user_id = g.user_id
 WHERE g.kind = 'permission'
   AND c.username = ANY(:memory_users::text[])
   AND g.value IN ('memory','memory:review','memory:label','memory:delete','memory:delete-subject')
 ORDER BY c.username, g.value;
