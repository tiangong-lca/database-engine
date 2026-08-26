#!/usr/bin/env bash
set -euo pipefail

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
repo_issue_migrations=(
  "$repo_root"/supabase/migrations/20260826060422_*.sql
  "$repo_root"/supabase/migrations/2026082608*.sql
)
test_issue_migrations=(
  "$test_workdir"/supabase/migrations/20260826060422_*.sql
  "$test_workdir"/supabase/migrations/2026082608*.sql
)
if [[ "${#repo_issue_migrations[@]}" -ne 22 \
   || "${#test_issue_migrations[@]}" -ne 22 ]]; then
  echo "Issue 531 migration manifest is incomplete" >&2
  exit 2
fi
for repo_migration in "${repo_issue_migrations[@]}"; do
  migration_name="$(basename "$repo_migration")"
  test_migration="$test_workdir/supabase/migrations/$migration_name"
  if [[ ! -f "$test_migration" ]] \
     || ! cmp -s "$repo_migration" "$test_migration"; then
    echo "isolated migration differs from repository HEAD: $migration_name" >&2
    exit 2
  fi
done
if [[ -e "$test_workdir/supabase/migrations/20260826080354_portal_projection_process_hnsw.sql" \
   || -e "$test_workdir/supabase/migrations/20260826080357_portal_projection_flow_hnsw.sql" ]]; then
  echo "isolated project retains retired projection HNSW migrations" >&2
  exit 2
fi

project_id="$({
  sed -n 's/^project_id = "\([^"]*\)"$/\1/p' \
    "$test_workdir/supabase/config.toml" | head -n 1
})"
if [[ ! "$project_id" =~ ^database-engine-531-[a-z0-9-]+$ ]]; then
  echo "refusing non-Issue-531 Supabase project_id: $project_id" >&2
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
echo "Supabase CLI: $($supabase_cli --version)"
echo "Recovery target: $project_id"

# Breakpoint 1: expand plus every bounded backfill is recorded, while old API
# wrappers remain authoritative. Exercise all five write/snapshot races before
# the final source-write fence.
reset_to 20260826080342

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
" "pre-fence race state did not preserve valid/key/source-vector winners and exact missing/stale rows"

# Hold a real writer lock beyond the five-second lock budget. The authored
# reconcile transaction must fail with no ledger row and no partial cleanup.
docker exec -i "$container_name" \
  psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres \
  >"$race_log_dir/lock-holder.log" 2>&1 <<'SQL' &
set application_name = 'portal_projection_reconcile_lock_holder';
begin;
lock table public.processes, public.flows in row exclusive mode;
select pg_sleep(8);
commit;
SQL
lock_holder_pid=$!
wait_for_pg_sleep portal_projection_reconcile_lock_holder

reconcile_failure_started=$SECONDS
reconcile_log_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
if apply_pending >"$race_log_dir/expected-reconcile-failure.log" 2>&1; then
  echo "reconcile unexpectedly ignored the held writer lock" >&2
  exit 1
fi
docker logs --since "$reconcile_log_since" "$container_name" \
  >>"$race_log_dir/expected-reconcile-failure.log" 2>&1
reconcile_failure_seconds=$((SECONDS - reconcile_failure_started))
echo "Reconcile lock-acquisition failure: ${reconcile_failure_seconds}s"
if ((reconcile_failure_seconds < 5 || reconcile_failure_seconds > 15)); then
  echo "reconcile lock failure fell outside the 5-15s evidence window" >&2
  exit 1
fi
assert_log_contains \
  "$race_log_dir/expected-reconcile-failure.log" \
  '55P03|lock timeout|canceling statement due to lock timeout' \
  'reconcile lock-timeout failure'

assert_sql "
  not exists (
    select 1 from supabase_migrations.schema_migrations
    where version = '20260826080345'
  )
  and pg_catalog.to_regprocedure(
    'private.backfill_portal_catalog_search_range_v1(uuid,uuid)'
  ) is not null
" "failed reconcile left a ledger row or partial transaction effects"

wait "$lock_holder_pid"
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
" "successful retry did not reconcile delete/state races exactly"

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

index_oids_before="$(scalar_sql "
  select pg_catalog.string_agg(index_relation.oid::text, ',' order by index_relation.relname)
  from pg_catalog.pg_class as index_relation
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = index_relation.relnamespace
  where (
      namespace.nspname = 'private'
      and index_relation.relname in (
        'portal_catalog_search_process_document_v1_pgroonga',
        'portal_catalog_search_flow_document_v1_pgroonga'
      )
    ) or (
      namespace.nspname = 'public'
      and index_relation.relname in (
        'processes_embedding_ft_hnsw_idx',
        'flows_embedding_ft_hnsw_idx'
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
        'portal_catalog_search_flow_document_v1_pgroonga'
      )
    ) or (
      namespace.nspname = 'public'
      and index_relation.relname in (
        'processes_embedding_ft_hnsw_idx',
        'flows_embedding_ft_hnsw_idx'
      )
    )
")"
if [[ "$index_count_before" != "4" \
   || ! "$index_oids_before" =~ ^[0-9]+,[0-9]+,[0-9]+,[0-9]+$ ]]; then
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
        'portal_catalog_search_flow_document_v1_pgroonga'
      )
    ) or (
      namespace.nspname = 'public'
      and index_relation.relname in (
        'processes_embedding_ft_hnsw_idx',
        'flows_embedding_ft_hnsw_idx'
      )
    )
")"
if [[ "$index_oids_before" != "$index_oids_after" ]]; then
  echo "no-op migration retry rebuilt an already-recorded index" >&2
  exit 1
fi

rm -rf "$race_log_dir"
echo "Portal projection race and partial-upgrade recovery validation passed"
