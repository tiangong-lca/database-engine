#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
supabase_cli="${SUPABASE_CLI:-supabase}"
test_workdir="${PORTAL_PROJECTION_SUPABASE_WORKDIR:-}"

if [[ "${PORTAL_PROJECTION_RECOVERY_TARGET:-}" != "local-isolated" ]]; then
  echo "set PORTAL_PROJECTION_RECOVERY_TARGET=local-isolated" >&2
  exit 2
fi
if [[ -z "$test_workdir" || "$test_workdir" != /* ]]; then
  echo "PORTAL_PROJECTION_SUPABASE_WORKDIR must be an absolute isolated project path" >&2
  exit 2
fi
if [[ ! -x "$supabase_cli" ]] && ! command -v "$supabase_cli" >/dev/null 2>&1; then
  echo "SUPABASE_CLI does not resolve to an executable" >&2
  exit 2
fi
if [[ ! -f "$test_workdir/supabase/config.toml" ]]; then
  echo "isolated Supabase config is missing" >&2
  exit 2
fi

shopt -s nullglob
repo_migrations=("$repo_root"/supabase/migrations/*.sql)
test_migrations=("$test_workdir"/supabase/migrations/*.sql)
if [[ "${#repo_migrations[@]}" -ne 273 \
   || "${#test_migrations[@]}" -ne 273 ]]; then
  echo "complete migration tree must contain exactly 273 files" >&2
  exit 2
fi
migration_manifest_payload=""
for migration_index in "${!repo_migrations[@]}"; do
  repo_migration="${repo_migrations[$migration_index]}"
  test_migration="${test_migrations[$migration_index]}"
  repo_name="$(basename "$repo_migration")"
  test_name="$(basename "$test_migration")"
  if [[ "$repo_name" != "$test_name" ]] \
     || ! cmp -s "$repo_migration" "$test_migration"; then
    echo "isolated migration tree differs from repository HEAD: $repo_name/$test_name" >&2
    exit 2
  fi
  migration_hash="$(shasum -a 256 "$repo_migration" | awk '{print $1}')"
  migration_manifest_payload="${migration_manifest_payload}${migration_hash}  ${repo_name}\n"
done
migration_tree_sha256="$({
  printf '%b' "$migration_manifest_payload" | shasum -a 256 | awk '{print $1}'
})"
repository_head="$(git -C "$repo_root" rev-parse HEAD)"
if [[ ! "$repository_head" =~ ^[0-9a-f]{40}$ \
   || ! "$migration_tree_sha256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "repository/migration aggregate identity is malformed" >&2
  exit 2
fi
if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  echo "recovery evidence requires a clean exact repository HEAD" >&2
  exit 2
fi
if [[ -e "$test_workdir/supabase/migrations/20260826080354_portal_projection_process_hnsw.sql" \
   || -e "$test_workdir/supabase/migrations/20260826080357_portal_projection_flow_hnsw.sql" ]]; then
  echo "isolated project retains retired projection HNSW migrations" >&2
  exit 2
fi

project_id="$({
  sed -n 's/^project_id = "\([^"]*\)"$/\1/p' \
    "$test_workdir/supabase/config.toml" | head -n 1
})"
if [[ ! "$project_id" =~ ^database-engine-(531|532|539)-[a-z0-9-]+$ ]]; then
  echo "refusing non-Issue-531/532/539 Supabase project_id: $project_id" >&2
  exit 2
fi

container_name="supabase_db_${project_id}"
if ! docker inspect "$container_name" >/dev/null 2>&1; then
  echo "isolated database container is not running: $container_name" >&2
  exit 2
fi

run_psql() {
  docker exec -i "$container_name" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres "$@"
}

scalar_sql() {
  docker exec "$container_name" \
    psql -X -A -t -v ON_ERROR_STOP=1 -U postgres -d postgres \
    -c "$1"
}

terminate_application() {
  local application_name="$1"
  local backend_pid
  local terminated
  backend_pid="$(scalar_sql "
    select pid
    from pg_catalog.pg_stat_activity
    where application_name = '$application_name'
      and pid <> pg_catalog.pg_backend_pid()
    order by pid
    limit 1
  ")"
  if [[ ! "$backend_pid" =~ ^[1-9][0-9]*$ ]]; then
    return 1
  fi
  terminated="$(scalar_sql "
    select pg_catalog.pg_terminate_backend($backend_pid)
  ")"
  [[ "$terminated" == "t" ]]
}

reset_to() {
  "$supabase_cli" --workdir "$test_workdir" \
    db reset --local --no-seed --version "$1" >/dev/null 2>&1
}

apply_pending() {
  "$supabase_cli" --workdir "$test_workdir" \
    migration up --local >/dev/null
}

apply_sql_file() {
  local sql_file="$1"
  docker exec -i "$container_name" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres \
    <"$sql_file"
}

assert_log_contains() {
  local log_file="$1"
  local expected_pattern="$2"
  local description="$3"
  if ! grep -Eiq "$expected_pattern" "$log_file"; then
    echo "$description did not expose the expected database error" >&2
    echo "retained log: $log_file" >&2
    exit 1
  fi
}

wait_for_pg_sleep() {
  local application_name="$1"
  local observed="0"
  for _ in {1..50}; do
    observed="$(scalar_sql "
      select count(*)
      from pg_catalog.pg_stat_activity
      where application_name = '$application_name'
        and wait_event = 'PgSleep'
    ")"
    if [[ "$observed" == "1" ]]; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for $application_name" >&2
  return 1
}

run_stale_snapshot() {
  local application_name="$1"
  local fixture_id="$2"
  local output_file="$3"

  docker exec -i "$container_name" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres \
    >"$output_file" 2>&1 <<SQL &
set application_name = '$application_name';
grant api_internal_executor to postgres;
set role api_internal_executor;
begin;
create temporary table portal_race_snapshot on commit drop as
select *
from private.portal_catalog_search_rows_v1
where dataset_kind = 'process'
  and id = '$fixture_id'::uuid
  and version = '01.00.000';
select pg_sleep(3);
insert into private.portal_catalog_search_rows_v1
select * from portal_race_snapshot
on conflict (dataset_kind, id, version) do nothing;
commit;
reset role;
revoke api_internal_executor from postgres;
SQL
  stale_snapshot_pid=$!
}

assert_sql() {
  local condition="$1"
  local message="$2"
  run_psql <<SQL
do \$assert_portal_projection_recovery\$
begin
  if ($condition) is not true then
    raise exception '$message';
  end if;
end
\$assert_portal_projection_recovery\$;
SQL
}

cd "$repo_root"
python3 "$repo_root/scripts/check_portal_projection_manifest.py"
supabase_cli_version="$($supabase_cli --version)"
if [[ "$supabase_cli_version" != "2.109.1" ]]; then
  echo "Supabase CLI must be the reviewed 2.109.1, found $supabase_cli_version" >&2
  exit 2
fi
echo "Supabase CLI: $supabase_cli_version"
echo "Recovery target: $project_id"
echo "Repository HEAD: $repository_head"
echo "Migration tree SHA-256 (273 files): $migration_tree_sha256"

# Breakpoint 1: expand plus every bounded backfill is recorded, while old API
# wrappers remain authoritative. Exercise all five write/snapshot races before
# the final source-write fence.
reset_to 20260826080342

run_psql <<'SQL'
grant api_internal_executor to postgres;
set role api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
reset role;
revoke api_internal_executor from postgres;
SQL

assert_sql "
  (select count(*) = 1
   from private.portal_catalog_projection_contract_v1
   where contract_version = 1
     and manifest_sha256 =
       'b5e0aff9abbffcc8d2dacaf559a5d1a8c993c20b647d0c70f0e4fa18eb06d2dc'
     and pg_catalog.cardinality(function_identities) = 11)
  and exists (
    select 1
    from pg_catalog.pg_constraint
    where conrelid = 'private.portal_catalog_search_rows_v1'::regclass
      and confrelid =
        'private.portal_catalog_projection_contract_v1'::regclass
      and conname =
        'portal_catalog_search_rows_contract_version_v1_fk'
      and contype = 'f'
      and convalidated
      and confupdtype = 'r'
      and confdeltype = 'r'
  )
  and not exists (
    select 1
    from private.portal_catalog_search_rows_v1
    where projection_contract_version <> 1
  )
" "expand/backfill breakpoint lacks the exact immutable derivation contract"

run_psql <<'SQL'
alter table public.processes disable trigger user;
alter table public.flows disable trigger user;
alter table public.processes
  enable trigger portal_catalog_projection_content_sync_v1;
alter table public.flows
  enable trigger portal_catalog_projection_content_sync_v1;

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft, model_id
)
select fixture.id,
  '01.00.000',
  fixture.payload,
  fixture.payload::json,
  '53190000-0000-4000-8000-000000000010'::uuid,
  100,
  true,
  fixture.modified_at,
  null,
  null,
  null
from (
  values
    (
      '53190000-0000-4000-8000-000000000001'::uuid,
      'race valid old'::text,
      '2026-08-26 09:00:01+00'::timestamptz
    ),
    (
      '53190000-0000-4000-8000-000000000002'::uuid,
      'race delete old'::text,
      '2026-08-26 09:00:02+00'::timestamptz
    ),
    (
      '53190000-0000-4000-8000-000000000003'::uuid,
      'race invalid old'::text,
      '2026-08-26 09:00:03+00'::timestamptz
    ),
    (
      '53190000-0000-4000-8000-000000000004'::uuid,
      'race key old'::text,
      '2026-08-26 09:00:04+00'::timestamptz
    ),
    (
      '53190000-0000-4000-8000-000000000006'::uuid,
      'race embedding only'::text,
      '2026-08-26 09:00:06+00'::timestamptz
    )
) as seed(id, name, modified_at)
cross join lateral (
  select pg_catalog.jsonb_build_object(
    'processDataSet', pg_catalog.jsonb_build_object(
      'processInformation', pg_catalog.jsonb_build_object(
        'dataSetInformation', pg_catalog.jsonb_build_object(
          'name', pg_catalog.jsonb_build_object(
            'baseName', pg_catalog.jsonb_build_object(
              '@xml:lang', 'en', '#text', seed.name
            )
          )
        )
      ),
      'administrativeInformation', pg_catalog.jsonb_build_object(
        'publicationAndOwnership', pg_catalog.jsonb_build_object(
          'common:dataSetVersion', '01.00.000',
          'common:licenseType', 'Free of charge for all users and uses'
        )
      )
    )
  ) as payload
) as fixture_payload
cross join lateral (
  select seed.id, fixture_payload.payload, seed.modified_at
) as fixture;
SQL

race_log_dir="$(mktemp -d /tmp/database-engine-531-race.XXXXXX)"
facet_reconcile_holder_pid=""
successful_fence_pid=""

cleanup_recovery() {
  local exit_code=$?
  trap - EXIT INT TERM
  set +e
  terminate_application portal_projection_reconcile_lock_holder
  terminate_application portal_facet_reconcile_lock_holder
  terminate_application portal_projection_successful_fence_holder
  for holder_pid in \
    "${facet_reconcile_holder_pid:-}" \
    "${successful_fence_pid:-}"; do
    if [[ "$holder_pid" =~ ^[1-9][0-9]*$ ]]; then
      wait "$holder_pid" >/dev/null 2>&1 || true
    fi
  done
  if [[ "$exit_code" -eq 0 ]]; then
    rm -rf "$race_log_dir"
  else
    echo "retained recovery diagnostics: $race_log_dir" >&2
  fi
  exit "$exit_code"
}
trap cleanup_recovery EXIT INT TERM

run_stale_snapshot \
  portal_projection_race_valid \
  53190000-0000-4000-8000-000000000001 \
  "$race_log_dir/valid.log"
wait_for_pg_sleep portal_projection_race_valid
run_psql <<'SQL'
grant api_internal_executor to postgres;
set role api_internal_executor;
delete from private.portal_catalog_search_rows_v1
where dataset_kind = 'process'
  and id = '53190000-0000-4000-8000-000000000001'::uuid;
reset role;
revoke api_internal_executor from postgres;
update public.processes
set json = pg_catalog.jsonb_set(
      json,
      '{processDataSet,processInformation,dataSetInformation,name,baseName,#text}',
      pg_catalog.to_jsonb('race valid new'::text)
    ),
    modified_at = '2026-08-26 09:01:01+00'
where id = '53190000-0000-4000-8000-000000000001'::uuid;
SQL
wait "$stale_snapshot_pid"

run_stale_snapshot \
  portal_projection_race_delete \
  53190000-0000-4000-8000-000000000002 \
  "$race_log_dir/delete.log"
wait_for_pg_sleep portal_projection_race_delete
run_psql <<'SQL'
grant api_internal_executor to postgres;
set role api_internal_executor;
delete from private.portal_catalog_search_rows_v1
where dataset_kind = 'process'
  and id = '53190000-0000-4000-8000-000000000002'::uuid;
reset role;
revoke api_internal_executor from postgres;
delete from public.processes
where id = '53190000-0000-4000-8000-000000000002'::uuid;
SQL
wait "$stale_snapshot_pid"

run_stale_snapshot \
  portal_projection_race_invalid \
  53190000-0000-4000-8000-000000000003 \
  "$race_log_dir/invalid.log"
wait_for_pg_sleep portal_projection_race_invalid
run_psql <<'SQL'
grant api_internal_executor to postgres;
set role api_internal_executor;
delete from private.portal_catalog_search_rows_v1
where dataset_kind = 'process'
  and id = '53190000-0000-4000-8000-000000000003'::uuid;
reset role;
revoke api_internal_executor from postgres;
update public.processes
set state_code = 20,
    modified_at = '2026-08-26 09:01:03+00'
where id = '53190000-0000-4000-8000-000000000003'::uuid;
SQL
wait "$stale_snapshot_pid"

run_stale_snapshot \
  portal_projection_race_key_change \
  53190000-0000-4000-8000-000000000004 \
  "$race_log_dir/key-change.log"
wait_for_pg_sleep portal_projection_race_key_change
run_psql <<'SQL'
update public.processes
set id = '53190000-0000-4000-8000-000000000005'::uuid,
    version = '01.00.001',
    modified_at = '2026-08-26 09:01:04+00'
where id = '53190000-0000-4000-8000-000000000004'::uuid
  and version = '01.00.000';
SQL
wait "$stale_snapshot_pid"

run_psql <<'SQL'
grant api_internal_executor to postgres;
set role api_internal_executor;
delete from private.portal_catalog_search_rows_v1
where dataset_kind = 'process'
  and id = '53190000-0000-4000-8000-000000000006'::uuid;
reset role;
revoke api_internal_executor from postgres;
update public.processes
set embedding_ft = (
      '[1,' || pg_catalog.array_to_string(
        pg_catalog.array_fill('0'::text, array[1023]),
        ','
      ) || ']'
    )::extensions.vector(1024)
where id = '53190000-0000-4000-8000-000000000006'::uuid;
SQL

assert_sql "
  coalesce((
    select card #>> '{names,0,value}' = 'race valid new'
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
      and id = '53190000-0000-4000-8000-000000000001'::uuid
  ), false)
  and (select count(*) = 3
       from private.portal_catalog_search_rows_v1
       where id in (
         '53190000-0000-4000-8000-000000000002'::uuid,
         '53190000-0000-4000-8000-000000000003'::uuid,
         '53190000-0000-4000-8000-000000000004'::uuid
       ))
  and coalesce((
    select version = '01.00.001'
      and id = '53190000-0000-4000-8000-000000000005'::uuid
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
      and id = '53190000-0000-4000-8000-000000000005'::uuid
  ), false)
  and coalesce((
    select embedding_ft is not null
    from public.processes
    where id = '53190000-0000-4000-8000-000000000006'::uuid
      and version = '01.00.000'
  ), false)
  and not exists (
    select 1
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
      and id = '53190000-0000-4000-8000-000000000006'::uuid
  )
  and not exists (
    select 1
    from private.portal_catalog_search_rows_v1
    where projection_contract_version <> 1
  )
" "pre-fence race state did not preserve valid/key/source-vector winners and exact missing/stale rows"

# Hold a real writer lock beyond the five-second lock budget. The authored
# reconcile transaction must fail with no ledger row and no partial cleanup.
docker exec -i "$container_name" \
  psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres \
  >"$race_log_dir/lock-holder.log" 2>&1 <<'SQL' &
set application_name = 'portal_projection_reconcile_lock_holder';
begin;
lock table public.processes, public.flows in row exclusive mode;
select pg_sleep(60);
commit;
SQL
lock_holder_pid=$!
wait_for_pg_sleep portal_projection_reconcile_lock_holder

reconcile_failure_started=$SECONDS
reconcile_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-reconcile-failure.log" 2>&1; then
  terminate_application portal_projection_reconcile_lock_holder || true
  wait "$lock_holder_pid" || true
  echo "reconcile unexpectedly ignored the held writer lock" >&2
  exit 1
fi
docker logs --since "$reconcile_log_since" "$container_name" \
  >>"$race_log_dir/expected-reconcile-failure.log" 2>&1
reconcile_failure_seconds=$((SECONDS - reconcile_failure_started))
echo "Reconcile lock-acquisition failure: ${reconcile_failure_seconds}s"
if ((reconcile_failure_seconds < 5 || reconcile_failure_seconds > 30)); then
  echo "reconcile lock failure fell outside the 5-30s evidence window" >&2
  exit 1
fi
assert_log_contains \
  "$race_log_dir/expected-reconcile-failure.log" \
  '55P03|lock timeout|canceling statement due to lock timeout' \
  'reconcile lock-timeout failure'

if ! terminate_application portal_projection_reconcile_lock_holder; then
  echo "failed to terminate the exact reconcile lock holder" >&2
  exit 1
fi
wait "$lock_holder_pid" || true

assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260826080345'
  )
  and pg_catalog.to_regprocedure(
    'private.backfill_portal_catalog_search_range_v1(uuid,uuid)'
  ) is not null
" "failed reconcile left a ledger row or partial transaction effects"

apply_pending

assert_sql "
  coalesce((
    select card #>> '{names,0,value}' = 'race valid new'
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
      and id = '53190000-0000-4000-8000-000000000001'::uuid
  ), false)
  and not exists (
    select 1
    from private.portal_catalog_search_rows_v1
    where id in (
      '53190000-0000-4000-8000-000000000002'::uuid,
      '53190000-0000-4000-8000-000000000003'::uuid,
      '53190000-0000-4000-8000-000000000004'::uuid
    )
  )
  and coalesce((
    select version = '01.00.001'
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
      and id = '53190000-0000-4000-8000-000000000005'::uuid
  ), false)
  and coalesce((
    select card #>> '{names,0,value}' = 'race embedding only'
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
      and id = '53190000-0000-4000-8000-000000000006'::uuid
  ), false)
  and pg_catalog.to_regprocedure(
    'private.backfill_portal_catalog_search_range_v1(uuid,uuid)'
  ) is null
  and not exists (
    select 1
    from private.portal_catalog_search_rows_v1
    where projection_contract_version <> 1
  )
" "successful retry did not reconcile delete/state races exactly"

run_psql <<'SQL'
grant api_internal_executor to postgres;
set role api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
reset role;
revoke api_internal_executor from postgres;
SQL

# Breakpoint 1a: a leaf projector can drift only outside the governed migration
# path.  Reconcile must detect that live digest and roll back its whole fence
# transaction without recording history or dropping its retry helper.
reset_to 20260826080342
run_psql <<'SQL'
grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;
create or replace function private.portal_scalar_text_v1(p_value jsonb)
returns text
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select case
    when pg_catalog.jsonb_typeof(p_value) = 'string'
      then nullif(
        pg_catalog.btrim(p_value #>> '{}'),
        '__portal_projection_recovery_drift__'
      )
    else null
  end
$function$;
reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;
SQL

contract_drift_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-contract-drift-failure.log" 2>&1; then
  echo "reconcile unexpectedly accepted a drifted projector manifest" >&2
  exit 1
fi
docker logs --since "$contract_drift_log_since" "$container_name" \
  >>"$race_log_dir/expected-contract-drift-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-contract-drift-failure.log" \
  '55000|Portal projection derivation contract drifted' \
  'reconcile derivation-contract failure'
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260826080345'
  )
  and pg_catalog.to_regprocedure(
    'private.backfill_portal_catalog_search_range_v1(uuid,uuid)'
  ) is not null
" "drifted reconcile left a ledger row or partial transaction effects"

# Breakpoint 1b: simulate reconcile COMMIT succeeding immediately before the
# CLI records migration history. The same file must be safe to apply again with
# its backfill helper already removed.
reset_to 20260826080342
apply_sql_file \
  "$repo_root/supabase/migrations/20260826080345_portal_projection_reconcile.sql" \
  >"$race_log_dir/reconcile-commit-gap.log" 2>&1
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260826080345'
  )
  and pg_catalog.to_regprocedure(
    'private.backfill_portal_catalog_search_range_v1(uuid,uuid)'
  ) is null
" "manual reconcile commit-gap fixture changed history or retained its helper"
apply_pending
assert_sql "
  exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260826080345'
  )
" "reconcile commit-gap retry did not record migration history"

# Breakpoint 2: an unrecorded same-name index build (valid or INVALID) must be
# removed before the canonical concurrent build is retried.
reset_to 20260826080345
run_psql <<'SQL'
create index portal_catalog_search_process_document_v1_pgroonga
on private.portal_catalog_search_rows_v1 (id);
SQL

index_name_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-index-name-failure.log" 2>&1; then
  echo "concurrent index unexpectedly replaced an unrecorded same-name relation" >&2
  exit 1
fi
docker logs --since "$index_name_log_since" "$container_name" \
  >>"$race_log_dir/expected-index-name-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-index-name-failure.log" \
  '42P07|already exists|duplicate_table' \
  'same-name concurrent-index failure'
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260826080348'
  )
" "failed concurrent index unexpectedly advanced migration history"
run_psql <<'SQL'
drop index concurrently if exists
  private.portal_catalog_search_process_document_v1_pgroonga;
SQL
apply_pending

assert_sql "
  (select access_method.amname = 'pgroonga'
   from pg_catalog.pg_class as index_relation
   join pg_catalog.pg_am as access_method
     on access_method.oid = index_relation.relam
   where index_relation.oid =
     'private.portal_catalog_search_process_document_v1_pgroonga'::regclass)
" "same-name concurrent-index recovery did not install PGroonga"

# Breakpoint 2b: the canonical concurrent build itself can COMMIT before its
# history row is recorded. A retry must fail closed, then the same standalone
# controlled cleanup must make the unchanged ledger retryable.
reset_to 20260826080345
apply_sql_file \
  "$repo_root/supabase/migrations/20260826080348_portal_projection_process_pgroonga.sql" \
  >"$race_log_dir/index-canonical-commit-gap.log" 2>&1
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260826080348'
  )
  and (select access_method.amname = 'pgroonga'
       from pg_catalog.pg_class as index_relation
       join pg_catalog.pg_am as access_method
         on access_method.oid = index_relation.relam
       where index_relation.oid =
         'private.portal_catalog_search_process_document_v1_pgroonga'::regclass)
" "canonical concurrent-index commit-gap fixture is not exact"
canonical_index_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-canonical-index-failure.log" 2>&1; then
  echo "canonical unrecorded concurrent index unexpectedly advanced" >&2
  exit 1
fi
docker logs --since "$canonical_index_log_since" "$container_name" \
  >>"$race_log_dir/expected-canonical-index-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-canonical-index-failure.log" \
  '42P07|already exists|duplicate_table' \
  'canonical concurrent-index history-gap failure'
run_psql <<'SQL'
drop index concurrently if exists
  private.portal_catalog_search_process_document_v1_pgroonga;
SQL
apply_pending

# Breakpoint 3: a cutover guard failure rolls back every CREATE OR REPLACE.
# Restoring the guarded index and repeating migration-up must continue from the
# unchanged ledger, then a no-op repeat must preserve both new lexical indexes
# and both reused source HNSW index OIDs.
reset_to 20260826080351
wrapper_before="$(scalar_sql "
  select pg_catalog.md5(pg_catalog.pg_get_functiondef(
    'api.portal_search_processes_v1(text,jsonb,text,text,integer)'::regprocedure
  ))
")"
if [[ ! "$wrapper_before" =~ ^[0-9a-f]{32}$ ]]; then
  echo "pre-cutover wrapper digest is not a 32-character MD5" >&2
  exit 1
fi
run_psql <<'SQL'
alter index private.portal_catalog_search_process_document_v1_pgroonga
rename to portal_catalog_search_process_document_v1_pgroonga_held;
SQL

cutover_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-cutover-failure.log" 2>&1; then
  echo "cutover unexpectedly accepted a missing canonical index" >&2
  exit 1
fi
docker logs --since "$cutover_log_since" "$container_name" \
  >>"$race_log_dir/expected-cutover-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-cutover-failure.log" \
  'Portal candidate index contract drifted|P0001|contract drifted' \
  'cutover index-guard failure'
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260826080400'
  )
" "failed cutover unexpectedly advanced migration history"
wrapper_after_failure="$(scalar_sql "
  select pg_catalog.md5(pg_catalog.pg_get_functiondef(
    'api.portal_search_processes_v1(text,jsonb,text,text,integer)'::regprocedure
  ))
")"
if [[ ! "$wrapper_after_failure" =~ ^[0-9a-f]{32}$ ]]; then
  echo "post-failure wrapper digest is not a 32-character MD5" >&2
  exit 1
fi
if [[ "$wrapper_before" != "$wrapper_after_failure" ]]; then
  echo "failed cutover changed the external Process wrapper" >&2
  exit 1
fi

run_psql <<'SQL'
alter index private.portal_catalog_search_process_document_v1_pgroonga_held
rename to portal_catalog_search_process_document_v1_pgroonga;
SQL
apply_pending

# Breakpoint 4: the post-cutover sparse-Flow eligibility index is an online,
# single-statement migration with a separate transactional catalog guard.
# Wrong-name collisions and a canonical COMMIT/history gap must require the
# same explicit standalone cleanup before unchanged-ledger retry. A guard
# failure must not record its migration or disturb the already-cut-over API.
reset_to 20260826080403
run_psql <<'SQL'
create index flows_portal_embedding_eligible_v1_idx
on public.flows (id, version)
where state_code in (100, 200)
  and embedding_ft is not null;
SQL

eligibility_name_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-eligibility-name-failure.log" 2>&1; then
  echo "eligibility migration unexpectedly replaced a same-name index" >&2
  exit 1
fi
docker logs --since "$eligibility_name_log_since" "$container_name" \
  >>"$race_log_dir/expected-eligibility-name-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-eligibility-name-failure.log" \
  '42P07|already exists|duplicate_table' \
  'Flow eligibility same-name failure'
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version in ('20260827010000', '20260827010003')
  )
" "failed eligibility build unexpectedly advanced migration history"
run_psql <<'SQL'
drop index concurrently if exists
  public.flows_portal_embedding_eligible_v1_idx;
SQL
apply_pending

assert_sql "
  exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260827010000'
  )
  and exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260827010003'
  )
  and (select access_method.amname = 'btree'
       from pg_catalog.pg_class as index_relation
       join pg_catalog.pg_am as access_method
         on access_method.oid = index_relation.relam
       where index_relation.oid =
         'public.flows_portal_embedding_eligible_v1_idx'::regclass)
" "same-name eligibility recovery did not install and guard the btree"

reset_to 20260826080403
apply_sql_file \
  "$repo_root/supabase/migrations/20260827010000_portal_flow_embedding_eligibility_index.sql" \
  >"$race_log_dir/eligibility-canonical-commit-gap.log" 2>&1
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260827010000'
  )
  and (select index_catalog.indisvalid
         and index_catalog.indisready
         and index_catalog.indislive
       from pg_catalog.pg_index as index_catalog
       where index_catalog.indexrelid =
         'public.flows_portal_embedding_eligible_v1_idx'::regclass)
" "canonical eligibility commit-gap fixture is not exact"
eligibility_commit_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-eligibility-commit-gap-failure.log" 2>&1; then
  echo "canonical unrecorded eligibility index unexpectedly advanced" >&2
  exit 1
fi
docker logs --since "$eligibility_commit_log_since" "$container_name" \
  >>"$race_log_dir/expected-eligibility-commit-gap-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-eligibility-commit-gap-failure.log" \
  '42P07|already exists|duplicate_table' \
  'Flow eligibility canonical history-gap failure'
run_psql <<'SQL'
drop index concurrently if exists
  public.flows_portal_embedding_eligible_v1_idx;
SQL
apply_pending

reset_to 20260827010000
run_psql <<'SQL'
alter index public.flows_portal_embedding_eligible_v1_idx
rename to flows_portal_embedding_eligible_v1_idx_held;
SQL
eligibility_guard_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-eligibility-guard-failure.log" 2>&1; then
  echo "eligibility guard unexpectedly accepted a missing canonical index" >&2
  exit 1
fi
docker logs --since "$eligibility_guard_log_since" "$container_name" \
  >>"$race_log_dir/expected-eligibility-guard-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-eligibility-guard-failure.log" \
  'Portal Flow embedding eligibility index drifted|55000|P0001' \
  'Flow eligibility catalog-guard failure'
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260827010003'
  )
" "failed eligibility guard unexpectedly advanced migration history"
run_psql <<'SQL'
alter index public.flows_portal_embedding_eligible_v1_idx_held
rename to flows_portal_embedding_eligible_v1_idx;
SQL
apply_pending

# Breakpoint 5: the facet expand is transactional but intentionally not a
# blind-idempotent DDL file. A COMMIT/history gap must expose existing objects,
# fail the normal retry, and leave recovery to an explicit reset/operator path.
reset_to 20260827010003
facet_wrapper_before_expand_gap="$(scalar_sql "
  select pg_catalog.md5(pg_catalog.pg_get_functiondef(
    'api.portal_facets_v1(text,text,jsonb)'::regprocedure
  ))
")"
apply_sql_file \
  "$repo_root/supabase/migrations/20260827020000_portal_facet_projection_expand.sql" \
  >"$race_log_dir/facet-expand-commit-gap.log" 2>&1
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260827020000'
  )
  and pg_catalog.to_regclass(
    'private.portal_catalog_facet_rows_v1'
  ) is not null
  and pg_catalog.to_regprocedure(
    'private.assert_portal_catalog_facet_contract_v1()'
  ) is not null
" "facet expand commit-gap fixture is not exact"
facet_expand_gap_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-facet-expand-gap-failure.log" 2>&1; then
  echo "facet expand unexpectedly ignored an unrecorded committed copy" >&2
  exit 1
fi
docker logs --since "$facet_expand_gap_log_since" "$container_name" \
  >>"$race_log_dir/expected-facet-expand-gap-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-facet-expand-gap-failure.log" \
  'already exists|duplicate_(table|function|object)|42P07|42723' \
  'facet expand commit/history-gap failure'
facet_wrapper_after_expand_gap="$(scalar_sql "
  select pg_catalog.md5(pg_catalog.pg_get_functiondef(
    'api.portal_facets_v1(text,text,jsonb)'::regprocedure
  ))
")"
if [[ "$facet_wrapper_before_expand_gap" != "$facet_wrapper_after_expand_gap" ]]; then
  echo "unrecorded facet expand changed the public Facets wrapper" >&2
  exit 1
fi

# Breakpoint 6: every facet shard is retry-safe after an ambiguous COMMIT.
# Disable only the new child trigger to construct one historical parent row,
# apply the first shard without history, then let normal migration-up repeat it.
reset_to 20260827020000
run_psql <<'SQL'
alter table private.portal_catalog_search_rows_v1
  disable trigger portal_catalog_facet_sync_v1;
grant api_internal_executor to postgres;
set role api_internal_executor;
insert into private.portal_catalog_search_rows_v1 (
  dataset_kind, id, version, state_code, modified_at,
  card, document, projection_contract_version
) values (
  'process',
  '13190000-0000-4000-8000-000000000001',
  '01.00.000',
  100,
  '2026-08-27 02:00:00+00',
  '{"document":"facet recovery","accessLevel":"open","geography":{"code":"CN"},"referenceYear":2024,"processSubtype":"unit process","source":"Recovery"}'::jsonb,
  'facet recovery',
  1
);
reset role;
revoke api_internal_executor from postgres;
alter table private.portal_catalog_search_rows_v1
  enable trigger portal_catalog_facet_sync_v1;
SQL
apply_sql_file \
  "$repo_root/supabase/migrations/20260827020001_portal_facet_projection_backfill_00_3f.sql" \
  >"$race_log_dir/facet-shard-commit-gap.log" 2>&1
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260827020001'
  )
  and coalesce((
    select facet_geography = 'cn'
      and facet_reference_year = '2024'
      and facet_source = 'recovery'
      and facet_contract_version = 1
    from private.portal_catalog_facet_rows_v1
    where dataset_kind = 'process'
      and id = '13190000-0000-4000-8000-000000000001'
      and version = '01.00.000'
  ), false)
" "facet shard commit-gap did not persist exact narrow facts"
apply_pending
assert_sql "
  (select count(*) = 6
   from supabase_migrations.schema_migrations
   where version between '20260827020001' and '20260827020006')
  and (select count(*) from private.portal_catalog_facet_rows_v1) = 1
" "facet shard retry did not complete exactly once"

# Breakpoint 7: the short parent-first reconcile fence fails closed under a
# concurrent projection writer and succeeds unchanged after that writer exits.
reset_to 20260827020004
docker exec -i "$container_name" \
  psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres \
  >"$race_log_dir/facet-reconcile-lock-holder.log" 2>&1 <<'SQL' &
set application_name = 'portal_facet_reconcile_lock_holder';
begin;
lock table private.portal_catalog_search_rows_v1 in row exclusive mode;
select pg_sleep(60);
commit;
SQL
facet_reconcile_holder_pid=$!
wait_for_pg_sleep portal_facet_reconcile_lock_holder
facet_reconcile_started="$(perl -MTime::HiRes=time -e 'printf "%.6f", time')"
facet_reconcile_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-facet-reconcile-lock-failure.log" 2>&1; then
  echo "facet reconcile unexpectedly crossed a conflicting writer lock" >&2
  exit 1
fi
docker logs --since "$facet_reconcile_log_since" "$container_name" \
  >>"$race_log_dir/expected-facet-reconcile-lock-failure.log" 2>&1
facet_reconcile_elapsed_ms="$(perl -MTime::HiRes=time -e \
  'printf "%.3f", (time - $ARGV[0]) * 1000' "$facet_reconcile_started")"
if ! awk -v value="$facet_reconcile_elapsed_ms" \
  'BEGIN { exit !(value >= 5000 && value <= 30000) }'; then
  echo "facet reconcile lock timeout fell outside 5-30s: $facet_reconcile_elapsed_ms" >&2
  exit 1
fi
assert_log_contains \
  "$race_log_dir/expected-facet-reconcile-lock-failure.log" \
  'lock timeout|55P03|canceling statement due to lock timeout' \
  'facet reconcile lock failure'
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version in ('20260827020005', '20260827020006')
  )
" "failed facet reconcile unexpectedly advanced migration history"
terminate_application portal_facet_reconcile_lock_holder
wait "$facet_reconcile_holder_pid" || true
apply_pending

# Breakpoint 8: a post-DDL metadata guard failure rolls back both the newly
# created fast helper and the replaced wrapper. The pre-cutover wrapper keeps
# one deliberate extra GUC so the final metadata comparison fails only after
# CREATE FUNCTION / CREATE OR REPLACE FUNCTION have executed.
reset_to 20260827020005
run_psql <<'SQL'
grant portal_public_executor to postgres;
set role portal_public_executor;
alter function api.portal_facets_v1(text, text, jsonb)
  set work_mem = '64MB';
reset role;
revoke portal_public_executor from postgres;
SQL
facet_wrapper_before_cutover_failure="$(scalar_sql "
  select pg_catalog.md5(pg_catalog.pg_get_functiondef(
    'api.portal_facets_v1(text,text,jsonb)'::regprocedure
  ))
")"
facet_cutover_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-facet-cutover-failure.log" 2>&1; then
  echo "facet cutover unexpectedly accepted external metadata drift" >&2
  exit 1
fi
docker logs --since "$facet_cutover_log_since" "$container_name" \
  >>"$race_log_dir/expected-facet-cutover-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-facet-cutover-failure.log" \
  'Portal facet cutover contract drifted|55000' \
  'facet cutover post-DDL metadata failure'
facet_wrapper_after_cutover_failure="$(scalar_sql "
  select pg_catalog.md5(pg_catalog.pg_get_functiondef(
    'api.portal_facets_v1(text,text,jsonb)'::regprocedure
  ))
")"
if [[ "$facet_wrapper_before_cutover_failure" != "$facet_wrapper_after_cutover_failure" ]]; then
  echo "failed facet cutover changed the public wrapper" >&2
  exit 1
fi
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260827020006'
  )
  and pg_catalog.to_regprocedure(
    'private.catalog_portal_facets_empty_v1_impl(text,text)'
  ) is null
" "failed facet cutover unexpectedly recorded migration history"

reset_to 20260827020005
apply_pending

run_psql <<'SQL'
grant api_internal_executor to postgres;
set role api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();
reset role;
revoke api_internal_executor from postgres;
SQL

assert_sql "
  not exists (
    select 1
    from private.portal_catalog_search_rows_v1
    where projection_contract_version <> 1
  )
  and (
    select count(*)
    from pg_catalog.pg_proc as routine
    where routine.oid = any (array[
      'private.portal_search_v1(text,text,jsonb,text,text,integer)'::regprocedure::oid,
      'private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'::regprocedure::oid,
      'api.portal_facets_v1(text,text,jsonb)'::regprocedure::oid
    ])
      and (
        pg_catalog.length(routine.prosrc)
        - pg_catalog.length(pg_catalog.replace(
          routine.prosrc,
          'assert_portal_catalog_projection_contract_v1',
          ''
        ))
      ) / pg_catalog.length(
        'assert_portal_catalog_projection_contract_v1'
      ) = 1
  ) = 3
  and (
    select (
      pg_catalog.length(routine.prosrc)
      - pg_catalog.length(pg_catalog.replace(
          routine.prosrc,
          'assert_portal_catalog_facet_contract_v1',
          ''
        ))
    ) / pg_catalog.length(
      'assert_portal_catalog_facet_contract_v1'
    ) = 1
    from pg_catalog.pg_proc as routine
    where routine.oid = 'api.portal_facets_v1(text,text,jsonb)'::regprocedure
  )
  and (
    select count(*) from private.portal_catalog_facet_rows_v1
  ) = (
    select count(*) from private.portal_catalog_search_rows_v1
  )
" "completed cutovers lost projection versions or per-request contract guards"

index_oids_before="$(scalar_sql "
  select pg_catalog.string_agg(index_relation.oid::text, ',' order by index_relation.relname)
  from pg_catalog.pg_class as index_relation
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = index_relation.relnamespace
  where (
      namespace.nspname = 'private'
      and index_relation.relname in (
        'portal_catalog_search_process_document_v1_pgroonga',
        'portal_catalog_search_flow_document_v1_pgroonga',
        'portal_catalog_facet_rows_v1_pkey',
        'portal_catalog_facet_rows_latest_v1_idx',
        'portal_sitemap_latest_shard_v1_idx'
      )
    ) or (
      namespace.nspname = 'public'
      and index_relation.relname in (
        'processes_embedding_ft_hnsw_idx',
        'flows_embedding_ft_hnsw_idx',
        'flows_portal_embedding_eligible_v1_idx'
      )
    )
")"
index_count_before="$(scalar_sql "
  select count(*)
  from pg_catalog.pg_class as index_relation
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = index_relation.relnamespace
  where (
      namespace.nspname = 'private'
      and index_relation.relname in (
        'portal_catalog_search_process_document_v1_pgroonga',
        'portal_catalog_search_flow_document_v1_pgroonga',
        'portal_catalog_facet_rows_v1_pkey',
        'portal_catalog_facet_rows_latest_v1_idx',
        'portal_sitemap_latest_shard_v1_idx'
      )
    ) or (
      namespace.nspname = 'public'
      and index_relation.relname in (
        'processes_embedding_ft_hnsw_idx',
        'flows_embedding_ft_hnsw_idx',
        'flows_portal_embedding_eligible_v1_idx'
      )
    )
")"
if [[ "$index_count_before" != "8" \
   || ! "$index_oids_before" =~ ^[0-9]+(,[0-9]+){7}$ ]]; then
  echo "post-cutover index identity evidence is incomplete" >&2
  exit 1
fi
apply_pending
index_oids_after="$(scalar_sql "
  select pg_catalog.string_agg(index_relation.oid::text, ',' order by index_relation.relname)
  from pg_catalog.pg_class as index_relation
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = index_relation.relnamespace
  where (
      namespace.nspname = 'private'
      and index_relation.relname in (
        'portal_catalog_search_process_document_v1_pgroonga',
        'portal_catalog_search_flow_document_v1_pgroonga',
        'portal_catalog_facet_rows_v1_pkey',
        'portal_catalog_facet_rows_latest_v1_idx',
        'portal_sitemap_latest_shard_v1_idx'
      )
    ) or (
      namespace.nspname = 'public'
      and index_relation.relname in (
        'processes_embedding_ft_hnsw_idx',
        'flows_embedding_ft_hnsw_idx',
        'flows_portal_embedding_eligible_v1_idx'
      )
    )
")"
if [[ "$index_oids_before" != "$index_oids_after" ]]; then
  echo "no-op migration retry rebuilt an already-recorded index" >&2
  exit 1
fi

# Breakpoint 9: the latest-only sitemap projection is one transactional expand
# over a new empty table/index, followed by one transactional public cutover.
# A COMMIT/history gap is never repaired by deleting a subset on a hosted
# branch; the isolated recovery project proves the explicit reset path. A
# cutover prerequisite failure leaves both public RPCs absent.
reset_to 20260827134100
apply_sql_file \
  "$repo_root/supabase/migrations/20260827134101_portal_sitemap_latest_projection.sql" \
  >"$race_log_dir/sitemap-latest-expand-commit-gap.log" 2>&1
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260827134101'
  )
  and pg_catalog.to_regclass(
    'private.portal_sitemap_latest_rows_v1'
  ) is not null
  and pg_catalog.to_regprocedure(
    'private.sync_portal_sitemap_latest_row_v1()'
  ) is not null
  and pg_catalog.to_regprocedure(
    'api.portal_sitemap_manifest_v1()'
  ) is null
" "sitemap latest expand COMMIT/history-gap fixture is not exact"
sitemap_expand_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-sitemap-expand-gap-failure.log" 2>&1; then
  echo "sitemap latest expand unexpectedly ignored an unrecorded committed copy" >&2
  exit 1
fi
docker logs --since "$sitemap_expand_log_since" "$container_name" \
  >>"$race_log_dir/expected-sitemap-expand-gap-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-sitemap-expand-gap-failure.log" \
  'already exists|duplicate_(table|function|object)|42P07|42723|55000' \
  'sitemap latest expand COMMIT/history-gap failure'

reset_to 20260827134101
run_psql <<'SQL'
alter index private.portal_sitemap_latest_shard_v1_idx
rename to portal_sitemap_latest_shard_v1_idx_held;
SQL
sitemap_cutover_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-sitemap-cutover-failure.log" 2>&1; then
  echo "sitemap cutover unexpectedly accepted a missing canonical index" >&2
  exit 1
fi
docker logs --since "$sitemap_cutover_log_since" "$container_name" \
  >>"$race_log_dir/expected-sitemap-cutover-failure.log" 2>&1
assert_log_contains \
  "$race_log_dir/expected-sitemap-cutover-failure.log" \
  'Portal sitemap shard prerequisites are unsafe|55000' \
  'sitemap public cutover prerequisite failure'
assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260827134102'
  )
  and pg_catalog.to_regprocedure(
    'api.portal_sitemap_manifest_v1()'
  ) is null
  and pg_catalog.to_regprocedure(
    'api.portal_sitemap_shard_v1(text)'
  ) is null
" "failed sitemap cutover unexpectedly exposed the public contract"
run_psql <<'SQL'
alter index private.portal_sitemap_latest_shard_v1_idx_held
rename to portal_sitemap_latest_shard_v1_idx;
SQL
apply_pending
assert_sql "
  exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260827134102'
  )
  and pg_catalog.jsonb_array_length(
    api.portal_sitemap_manifest_v1() -> 'shards'
  ) = 64
" "sitemap cutover recovery did not reach the exact 64-shard contract"

# Reverse-direction lock proof: a successful source-write fence blocks a real
# content UPDATE until COMMIT. The fixed two-second sleep is coordination only;
# representative fence work is measured separately by the benchmark SQL.
run_psql <<'SQL'
alter table public.processes disable trigger user;
alter table public.processes
  enable trigger portal_catalog_projection_content_sync_v1;
insert into public.processes (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft, model_id
)
values (
  '53190000-0000-4000-8000-000000000099',
  '01.00.000',
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"successful fence writer"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::jsonb,
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"successful fence writer"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::json,
  '53190000-0000-4000-8000-000000000010',
  100,
  true,
  '2026-08-26 09:10:00+00',
  null,
  null,
  null
);
SQL

docker exec -i "$container_name" \
  psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres \
  >"$race_log_dir/successful-fence-holder.log" 2>&1 <<'SQL' &
set application_name = 'portal_projection_successful_fence_holder';
begin;
lock table public.processes, public.flows in share row exclusive mode;
select pg_sleep(2);
commit;
SQL
successful_fence_pid=$!
wait_for_pg_sleep portal_projection_successful_fence_holder
writer_started="$(perl -MTime::HiRes=time -e 'printf "%.6f", time')"
run_psql <<'SQL'
update public.processes
set json = pg_catalog.jsonb_set(
      json,
      '{processDataSet,processInformation,dataSetInformation,name,baseName,#text}',
      pg_catalog.to_jsonb('successful fence writer updated'::text)
    ),
    modified_at = '2026-08-26 09:10:01+00'
where id = '53190000-0000-4000-8000-000000000099'
  and version = '01.00.000';
SQL
writer_wait_ms="$(perl -MTime::HiRes=time -e \
  'printf "%.3f", (time - $ARGV[0]) * 1000' "$writer_started")"
wait "$successful_fence_pid"
if ! awk -v value="$writer_wait_ms" \
  'BEGIN { exit !(value >= 1000 && value <= 5000) }'; then
  echo "successful-fence writer wait fell outside 1000-5000ms: $writer_wait_ms" >&2
  exit 1
fi
assert_sql "
  coalesce((
    select card #>> '{names,0,value}' = 'successful fence writer updated'
      and modified_at = '2026-08-26 09:10:01+00'::timestamptz
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
      and id = '53190000-0000-4000-8000-000000000099'::uuid
      and version = '01.00.000'
  ), false)
  and coalesce((
    select modified_at = '2026-08-26 09:10:01+00'::timestamptz
      and facet_contract_version = 1
    from private.portal_catalog_facet_rows_v1
    where dataset_kind = 'process'
      and id = '53190000-0000-4000-8000-000000000099'::uuid
      and version = '01.00.000'
  ), false)
" "writer did not commit exact content after successful fence release"
echo "Successful fence blocked writer: ${writer_wait_ms}ms (includes 2s coordination hold)"

run_psql <<'SQL'
delete from public.processes
where id = '53190000-0000-4000-8000-000000000099'
  and version = '01.00.000';
alter table public.processes enable trigger user;
SQL

assert_sql "
  not exists (
    select 1
    from private.portal_catalog_facet_rows_v1
    where dataset_kind = 'process'
      and id = '53190000-0000-4000-8000-000000000099'::uuid
      and version = '01.00.000'
  )
" "facet child survived parent/source deletion"

echo "Portal projection race and partial-upgrade recovery validation passed"
