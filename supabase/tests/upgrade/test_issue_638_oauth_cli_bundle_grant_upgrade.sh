#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
migration="$repo_root/supabase/migrations/20260910150000_grant_official_cli_bundle_capability.sql"

if [[ ! -f "$migration" ]]; then
  echo "Issue #638 migration is missing: $migration" >&2
  exit 1
fi

database_url="$(
  supabase status --output env \
    | sed -n 's/^DB_URL="\([^"]*\)"$/\1/p'
)"

if [[ -z "$database_url" ]]; then
  echo "unable to resolve the local Supabase DB_URL" >&2
  exit 1
fi

cd "$repo_root"
supabase db reset --version 20260908090300 --no-seed

# No matching environment client is a valid Preview/local no-op.
psql "$database_url" -v ON_ERROR_STOP=1 -f "$migration"

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
select api.svc_oauth_client_configure(
  'issue-638-single-client',
  'cli',
  true,
  array[
    'CLI-RPC-01',
    'DB-CORE-READ-01',
    'DB-CORE-WRITE-01',
    'NX-CORE-02'
  ]
);
SQL

# Hold the capability table so the migration remains open after acquiring its
# registry lock. A concurrent facade call must time out instead of introducing
# a phantom candidate inside the selection/mutation window.
psql "$database_url" -v ON_ERROR_STOP=1 -c \
  "begin; lock table private.oauth_client_capability_grants in access exclusive mode; select pg_sleep(4); commit" \
  >/dev/null &
capability_lock_pid=$!

for _ in {1..30}; do
  capability_lock_count="$(
    psql "$database_url" -v ON_ERROR_STOP=1 -Atc \
      "select count(*) from pg_locks where relation = 'private.oauth_client_capability_grants'::regclass and mode = 'AccessExclusiveLock' and granted"
  )"
  [[ "$capability_lock_count" == "1" ]] && break
  sleep 0.1
done

if [[ "$capability_lock_count" != "1" ]]; then
  echo "Issue #638 could not establish the concurrency test blocker" >&2
  wait "$capability_lock_pid" || true
  exit 1
fi

psql "$database_url" -v ON_ERROR_STOP=1 -f "$migration" >/dev/null &
migration_pid=$!

for _ in {1..30}; do
  registry_lock_count="$(
    psql "$database_url" -v ON_ERROR_STOP=1 -Atc \
      "select count(*) from pg_locks where relation = 'private.oauth_client_registry'::regclass and mode = 'ShareRowExclusiveLock' and granted"
  )"
  [[ "$registry_lock_count" == "1" ]] && break
  sleep 0.1
done

if [[ "$registry_lock_count" != "1" ]]; then
  echo "Issue #638 migration did not acquire the registry serialization lock" >&2
  wait "$capability_lock_pid" || true
  wait "$migration_pid" || true
  exit 1
fi

if concurrent_output="$(
  PGOPTIONS='-c statement_timeout=500ms' psql "$database_url" -v ON_ERROR_STOP=1 -c \
    "select api.svc_oauth_client_configure('issue-638-concurrent-client', 'cli', true, array['CLI-RPC-01', 'DB-CORE-READ-01', 'DB-CORE-WRITE-01', 'NX-CORE-02'])" \
    2>&1
)"; then
  echo "Issue #638 concurrent client configuration unexpectedly crossed the migration lock" >&2
  wait "$capability_lock_pid" || true
  wait "$migration_pid" || true
  exit 1
fi

if [[ "$concurrent_output" != *"canceling statement due to statement timeout"* ]]; then
  echo "Issue #638 concurrent client configuration failed for an unexpected reason" >&2
  wait "$capability_lock_pid" || true
  wait "$migration_pid" || true
  exit 1
fi

wait "$capability_lock_pid"
wait "$migration_pid"

# Exactly one matching client is repaired through the audited service facade.

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
do $verify_single_repair$
declare
  v_actual text[];
  v_expected constant text[] := array[
    'CLI-RPC-01',
    'DB-CORE-READ-01',
    'DB-CORE-WRITE-01',
    'EDGE-BUNDLE-01',
    'NX-CORE-02'
  ];
begin
  select array_agg(capability_id order by capability_id) filter (where allowed)
  into v_actual
  from private.oauth_client_capability_grants
  where client_id = 'issue-638-single-client';

  if v_actual is distinct from v_expected then
    raise exception 'Issue #638 single-client repair mismatch: %', v_actual;
  end if;

  if (
    select count(*)
    from private.oauth_client_registry_audit
    where client_id = 'issue-638-single-client'
      and action = 'replace'
      and before_state -> 'capabilities' = to_jsonb(array[
        'CLI-RPC-01',
        'DB-CORE-READ-01',
        'DB-CORE-WRITE-01',
        'NX-CORE-02'
      ]::text[])
      and after_state -> 'capabilities' = to_jsonb(v_expected)
  ) <> 1 then
    raise exception 'Issue #638 expected one exact replace audit event';
  end if;
end
$verify_single_repair$;
SQL

audit_count_before="$(
  psql "$database_url" -v ON_ERROR_STOP=1 -Atc \
    "select count(*) from private.oauth_client_registry_audit where client_id = 'issue-638-single-client'"
)"
psql "$database_url" -v ON_ERROR_STOP=1 -f "$migration"
audit_count_after="$(
  psql "$database_url" -v ON_ERROR_STOP=1 -Atc \
    "select count(*) from private.oauth_client_registry_audit where client_id = 'issue-638-single-client'"
)"

if [[ "$audit_count_after" != "$audit_count_before" ]]; then
  echo "Issue #638 idempotent replay appended an audit event" >&2
  exit 1
fi

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
select api.svc_oauth_client_configure(
  'issue-638-ambiguous-a',
  'cli',
  true,
  array['CLI-RPC-01', 'DB-CORE-READ-01', 'DB-CORE-WRITE-01', 'NX-CORE-02']
);
select api.svc_oauth_client_configure(
  'issue-638-ambiguous-b',
  'cli',
  true,
  array['CLI-RPC-01', 'DB-CORE-READ-01', 'DB-CORE-WRITE-01', 'NX-CORE-02']
);
SQL

if ambiguous_output="$(psql "$database_url" -v ON_ERROR_STOP=1 -f "$migration" 2>&1)"; then
  echo "Issue #638 ambiguous client state unexpectedly succeeded" >&2
  exit 1
fi

if [[ "$ambiguous_output" != *"expected at most one matching enabled CLI client"* ]]; then
  echo "Issue #638 ambiguous client state failed for an unexpected reason" >&2
  exit 1
fi

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
do $verify_ambiguous_rollback$
begin
  if exists (
    select 1
    from private.oauth_client_capability_grants
    where client_id in ('issue-638-ambiguous-a', 'issue-638-ambiguous-b')
      and capability_id = 'EDGE-BUNDLE-01'
      and allowed
  ) then
    raise exception 'Issue #638 ambiguous repair changed a client';
  end if;
end
$verify_ambiguous_rollback$;
SQL

echo "Issue #638 OAuth CLI bundle grant upgrade checks passed"
