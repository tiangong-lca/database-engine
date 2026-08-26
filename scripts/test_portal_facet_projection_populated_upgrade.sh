#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
supabase_cli="${SUPABASE_CLI:-supabase}"
test_workdir="${PORTAL_FACET_UPGRADE_SUPABASE_WORKDIR:-}"

if [[ "${PORTAL_FACET_UPGRADE_TARGET:-}" != "local-isolated" ]]; then
  echo "set PORTAL_FACET_UPGRADE_TARGET=local-isolated" >&2
  exit 2
fi
if [[ -z "$test_workdir" || "$test_workdir" != /* \
   || ! -f "$test_workdir/supabase/config.toml" ]]; then
  echo "PORTAL_FACET_UPGRADE_SUPABASE_WORKDIR must be an absolute isolated project" >&2
  exit 2
fi
if [[ ! -x "$supabase_cli" ]] && ! command -v "$supabase_cli" >/dev/null 2>&1; then
  echo "SUPABASE_CLI does not resolve to an executable" >&2
  exit 2
fi

project_id="$(
  sed -n 's/^project_id = "\([^"]*\)"$/\1/p' \
    "$test_workdir/supabase/config.toml" | head -n 1
)"
if [[ ! "$project_id" =~ ^database-engine-531-[a-z0-9-]+$ ]]; then
  echo "refusing non-Issue-531 Supabase project_id: $project_id" >&2
  exit 2
fi
container_name="supabase_db_${project_id}"
if ! docker inspect "$container_name" >/dev/null 2>&1; then
  echo "isolated database container is not running: $container_name" >&2
  exit 2
fi

shopt -s nullglob
repo_migrations=("$repo_root"/supabase/migrations/*.sql)
test_migrations=("$test_workdir"/supabase/migrations/*.sql)
if [[ "${#repo_migrations[@]}" -ne 266 \
   || "${#test_migrations[@]}" -ne 266 ]]; then
  echo "complete migration tree must contain exactly 266 files" >&2
  exit 2
fi
for migration_index in "${!repo_migrations[@]}"; do
  if [[ "$(basename "${repo_migrations[$migration_index]}")" != \
        "$(basename "${test_migrations[$migration_index]}")" ]] \
     || ! cmp -s \
       "${repo_migrations[$migration_index]}" \
       "${test_migrations[$migration_index]}"; then
    echo "isolated migration tree differs from repository HEAD" >&2
    exit 2
  fi
done
if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  echo "populated-upgrade evidence requires a clean exact repository HEAD" >&2
  exit 2
fi
if [[ "$($supabase_cli --version)" != "2.109.1" ]]; then
  echo "Supabase CLI must be the reviewed 2.109.1" >&2
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

apply_sql_file() {
  docker exec -i "$container_name" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d postgres \
    <"$1"
}

reset_to() {
  "$supabase_cli" --workdir "$test_workdir" \
    db reset --local --no-seed --version "$1" >/dev/null 2>&1
}

cleanup_populated_upgrade() {
  local exit_code=$?
  trap - EXIT INT TERM
  set +e
  "$supabase_cli" --workdir "$test_workdir" \
    db reset --local --no-seed >/dev/null 2>&1
  exit "$exit_code"
}
trap cleanup_populated_upgrade EXIT INT TERM

cd "$repo_root"
python3 scripts/check_portal_projection_manifest.py
reset_to 20260827010003

run_psql <<'SQL'
set statement_timeout = '15min';

create function pg_temp.portal_facet_upgrade_uuid(
  p_kind text,
  p_ordinal integer
)
returns uuid
language sql
immutable
set search_path = ''
as $function$
  select (
    pg_catalog.substr(hash.value, 1, 8) || '-' ||
    pg_catalog.substr(hash.value, 9, 4) || '-' ||
    '4' || pg_catalog.substr(hash.value, 14, 3) || '-' ||
    '8' || pg_catalog.substr(hash.value, 18, 3) || '-' ||
    pg_catalog.substr(hash.value, 21, 12)
  )::uuid
  from (
    select pg_catalog.md5(p_kind || ':' || p_ordinal::text) as value
  ) as hash
$function$;

grant execute on function pg_temp.portal_facet_upgrade_uuid(text, integer)
to api_internal_executor;
grant api_internal_executor to postgres;
set role api_internal_executor;

insert into private.portal_catalog_search_rows_v1 (
  dataset_kind, id, version, state_code, modified_at,
  card, document, projection_contract_version
)
select
  'process',
  pg_temp.portal_facet_upgrade_uuid('process', ordinal),
  '01.00.000',
  case when ordinal % 5 = 0 then 200 else 100 end,
  '2026-08-27 03:00:00+00'::timestamptz
    + ordinal * interval '1 millisecond',
  pg_catalog.jsonb_build_object(
    'document', document.value,
    'accessLevel', case when ordinal % 5 = 0
      then 'metadata_only' else 'open' end,
    'geography', pg_catalog.jsonb_build_object(
      'code', case when ordinal % 3 = 0 then 'DE' else 'CN' end
    ),
    'referenceYear', 2000 + ordinal % 25,
    'processSubtype', 'unit process, single operation',
    'source', 'Populated Upgrade Provider'
  ),
  document.value,
  1
from pg_catalog.generate_series(1, 17299) as fixture(ordinal)
cross join lateral (
  select repeat('process public facet card ', 64) || ordinal::text as value
) as document;

insert into private.portal_catalog_search_rows_v1 (
  dataset_kind, id, version, state_code, modified_at,
  card, document, projection_contract_version
)
select
  'flow',
  pg_temp.portal_facet_upgrade_uuid('flow', ordinal),
  '01.00.000',
  case when ordinal % 5 = 0 then 200 else 100 end,
  '2026-08-27 03:00:00+00'::timestamptz
    + ordinal * interval '1 millisecond',
  pg_catalog.jsonb_build_object(
    'document', document.value,
    'accessLevel', case when ordinal % 5 = 0
      then 'metadata_only' else 'open' end,
    'geography', pg_catalog.jsonb_build_object(
      'code', case when ordinal % 3 = 0 then 'DE' else 'CN' end
    ),
    'referenceYear', 2000 + ordinal % 25,
    'source', 'Populated Upgrade Provider'
  ),
  document.value,
  1
from pg_catalog.generate_series(1, 108947) as fixture(ordinal)
cross join lateral (
  select repeat('flow public facet card ', 64) || ordinal::text as value
) as document;

reset role;
revoke api_internal_executor from postgres;
analyze private.portal_catalog_search_rows_v1;
SQL

if [[ "$(scalar_sql "select count(*) from private.portal_catalog_search_rows_v1")" \
   != "126246" ]]; then
  echo "populated parent fixture cardinality is not 126246" >&2
  exit 1
fi

apply_sql_file \
  "$repo_root/supabase/migrations/20260827020000_portal_facet_projection_expand.sql" \
  >/dev/null

for migration_version in 20260827020001 20260827020002 20260827020003 20260827020004; do
  migration_file=("$repo_root"/supabase/migrations/${migration_version}_*.sql)
  started="$(perl -MTime::HiRes=time -e 'printf "%.6f", time')"
  apply_sql_file "${migration_file[0]}" >/dev/null
  elapsed_ms="$(perl -MTime::HiRes=time -e \
    'printf "%.3f", (time - $ARGV[0]) * 1000' "$started")"
  echo "Facet backfill ${migration_version}: ${elapsed_ms}ms"
  if ! awk -v value="$elapsed_ms" \
    'BEGIN { exit !(value > 0 && value <= 60000) }'; then
    echo "facet backfill lacks 2x headroom under its 120s timeout" >&2
    exit 1
  fi
done

reconcile_started="$(perl -MTime::HiRes=time -e 'printf "%.6f", time')"
apply_sql_file \
  "$repo_root/supabase/migrations/20260827020005_portal_facet_projection_reconcile.sql" \
  >/dev/null
reconcile_ms="$(perl -MTime::HiRes=time -e \
  'printf "%.3f", (time - $ARGV[0]) * 1000' "$reconcile_started")"
echo "Facet reconcile: ${reconcile_ms}ms"
if ! awk -v value="$reconcile_ms" \
  'BEGIN { exit !(value > 0 && value <= 5000) }'; then
  echo "facet reconcile exceeded its 5s successful-fence budget" >&2
  exit 1
fi

apply_sql_file \
  "$repo_root/supabase/migrations/20260827020006_portal_facet_projection_cutover.sql" \
  >/dev/null

run_psql <<'SQL'
grant api_internal_executor to postgres;
set role api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();
do $verify_populated_facet_upgrade$
begin
  if (select count(*) from private.portal_catalog_facet_rows_v1) <> 126246
     or exists (
       select 1
       from private.portal_catalog_search_rows_v1 as projection
       cross join lateral private.portal_catalog_facet_facts_v1(
         projection.dataset_kind,
         projection.card
       ) as facts
       left join private.portal_catalog_facet_rows_v1 as facet
         on facet.dataset_kind = projection.dataset_kind
        and facet.id = projection.id
        and facet.version = projection.version
       where facet.id is null
          or facet.state_code is distinct from projection.state_code
          or facet.modified_at is distinct from projection.modified_at
          or facet.facet_access_level is distinct from facts.facet_access_level
          or facet.facet_geography is distinct from facts.facet_geography
          or facet.facet_reference_year is distinct from facts.facet_reference_year
          or facet.facet_process_subtype is distinct from facts.facet_process_subtype
          or facet.facet_source is distinct from facts.facet_source
          or facet.facet_contract_version is distinct from 1
     ) then
    raise exception 'populated facet upgrade parity failed';
  end if;
end
$verify_populated_facet_upgrade$;
reset role;
revoke api_internal_executor from postgres;

grant portal_public_executor to postgres;
set role portal_public_executor;
do $verify_populated_facet_dto_parity$
declare
  v_kind text;
  v_fingerprint text;
begin
  foreach v_kind in array array['process', 'flow', 'all'] loop
    v_fingerprint := private.portal_query_fingerprint_v1(
      v_kind, '', '{}'::jsonb, 'relevance'
    );
    if api.portal_facets_v1(v_kind, '', '{}'::jsonb)
       is distinct from private.catalog_portal_facets_v1_impl(
         v_kind, '', null::uuid, null::text, '{}'::jsonb, v_fingerprint
       ) then
      raise exception 'populated facet DTO parity failed for %', v_kind;
    end if;
  end loop;
end
$verify_populated_facet_dto_parity$;
reset role;
revoke portal_public_executor from postgres;
SQL

echo "Portal facet populated 126246-row upgrade validation passed"
