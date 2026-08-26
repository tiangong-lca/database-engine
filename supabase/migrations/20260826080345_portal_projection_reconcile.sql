-- Fail closed if a short source-write fence cannot be acquired. All expensive
-- card projection work completed in UUID-prefix batches before this point.
begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';
lock table public.processes, public.flows in share row exclusive mode;

grant api_internal_executor to postgres;
set role api_internal_executor;

with missing as materialized (
  select process.id,
    process.version::text as version,
    process.state_code,
    process.modified_at,
    process.json
  from public.processes as process
  where process.state_code in (100, 200)
    and process.modified_at is not null
    and pg_catalog.jsonb_typeof(process.json) = 'object'
    and pg_catalog.jsonb_typeof(process.json -> 'processDataSet') = 'object'
    and not exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'process'
        and projection.id = process.id
        and projection.version = process.version::text
    )
)
insert into private.portal_catalog_search_rows_v1 (
  dataset_kind, id, version, state_code, modified_at,
  card, document, projection_contract_version
)
select
  'process',
  missing.id,
  missing.version,
  missing.state_code,
  missing.modified_at,
  payload.value -> 'card',
  payload.value ->> 'document',
  1
from missing
cross join lateral (
  select private.catalog_portal_projection_payload_v1(
    'process', missing.state_code, missing.json
  ) as value
) as payload
where pg_catalog.jsonb_typeof(payload.value) = 'object'
on conflict (dataset_kind, id, version) do nothing;

with missing as materialized (
  select flow.id,
    flow.version::text as version,
    flow.state_code,
    flow.modified_at,
    flow.json
  from public.flows as flow
  where flow.state_code in (100, 200)
    and flow.modified_at is not null
    and pg_catalog.jsonb_typeof(flow.json) = 'object'
    and pg_catalog.jsonb_typeof(flow.json -> 'flowDataSet') = 'object'
    and not exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'flow'
        and projection.id = flow.id
        and projection.version = flow.version::text
    )
)
insert into private.portal_catalog_search_rows_v1 (
  dataset_kind, id, version, state_code, modified_at,
  card, document, projection_contract_version
)
select
  'flow',
  missing.id,
  missing.version,
  missing.state_code,
  missing.modified_at,
  payload.value -> 'card',
  payload.value ->> 'document',
  1
from missing
cross join lateral (
  select private.catalog_portal_projection_payload_v1(
    'flow', missing.state_code, missing.json
  ) as value
) as payload
where pg_catalog.jsonb_typeof(payload.value) = 'object'
on conflict (dataset_kind, id, version) do nothing;

delete from private.portal_catalog_search_rows_v1 as projection
where (
    projection.dataset_kind = 'process'
    and not exists (
      select 1
      from public.processes as process
      where process.id = projection.id
        and process.version::text = projection.version
        and process.state_code in (100, 200)
        and process.modified_at is not null
        and pg_catalog.jsonb_typeof(process.json) = 'object'
        and pg_catalog.jsonb_typeof(process.json -> 'processDataSet') = 'object'
    )
  ) or (
    projection.dataset_kind = 'flow'
    and not exists (
      select 1
      from public.flows as flow
      where flow.id = projection.id
        and flow.version::text = projection.version
        and flow.state_code in (100, 200)
        and flow.modified_at is not null
        and pg_catalog.jsonb_typeof(flow.json) = 'object'
        and pg_catalog.jsonb_typeof(flow.json -> 'flowDataSet') = 'object'
    )
  );

do $verify_portal_projection_reconciliation$
begin
  if (
    select count(*)
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
  ) <> (
    select count(*)
    from public.processes
    where state_code in (100, 200)
      and modified_at is not null
      and pg_catalog.jsonb_typeof(json) = 'object'
      and pg_catalog.jsonb_typeof(json -> 'processDataSet') = 'object'
  ) or (
    select count(*)
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'flow'
  ) <> (
    select count(*)
    from public.flows
    where state_code in (100, 200)
      and modified_at is not null
      and pg_catalog.jsonb_typeof(json) = 'object'
      and pg_catalog.jsonb_typeof(json -> 'flowDataSet') = 'object'
  ) then
    raise exception 'Portal projection reconciliation count mismatch';
  end if;

  if exists (
    select 1
    from private.portal_catalog_search_rows_v1 as projection
    left join public.processes as process
      on projection.dataset_kind = 'process'
     and process.id = projection.id
     and process.version::text = projection.version
    where projection.dataset_kind = 'process'
      and (
        process.id is null
        or process.state_code <> projection.state_code
        or process.modified_at <> projection.modified_at
      )
  ) or exists (
    select 1
    from private.portal_catalog_search_rows_v1 as projection
    left join public.flows as flow
      on projection.dataset_kind = 'flow'
     and flow.id = projection.id
     and flow.version::text = projection.version
    where projection.dataset_kind = 'flow'
      and (
        flow.id is null
        or flow.state_code <> projection.state_code
        or flow.modified_at <> projection.modified_at
      )
  ) then
    raise exception 'Portal projection reconciliation key/source parity mismatch';
  end if;
end
$verify_portal_projection_reconciliation$;

select private.assert_portal_catalog_projection_contract_v1();

drop function if exists
  private.backfill_portal_catalog_search_range_v1(uuid, uuid);

reset role;
revoke api_internal_executor from postgres;

commit;
