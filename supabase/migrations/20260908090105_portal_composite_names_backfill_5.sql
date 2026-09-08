begin;
set local lock_timeout='5s';
set local statement_timeout='120s';
grant api_internal_executor to postgres;
-- Lock source tuples before deriving. Concurrent updates/deletes serialize behind
-- this page, and the dual writer subsequently installs the committed newer value.
do $bound$ begin
  if (select count(*) from (select 1 from public.processes source where source.id >= '50000000-0000-0000-0000-000000000000'::uuid and source.id < '60000000-0000-0000-0000-000000000000'::uuid and source.state_code in (100,200) and source.modified_at is not null and jsonb_typeof(source.json) = 'object' and jsonb_typeof(source.json -> 'processDataSet') = 'object' limit 20001) bounded) > 20000 then
    raise exception 'Portal names shard exceeds reviewed 20000-row bound; use a reviewed finer partition';
  end if;
end $bound$;
with source_rows as materialized (
  select source.id, source.version, source.state_code, source.modified_at, source.json
  from public.processes source where source.id >= '50000000-0000-0000-0000-000000000000'::uuid and source.id < '60000000-0000-0000-0000-000000000000'::uuid and source.state_code in (100,200) and source.modified_at is not null and jsonb_typeof(source.json) = 'object' and jsonb_typeof(source.json -> 'processDataSet') = 'object'
  order by source.id, source.version for share of source
), payloads as materialized (
  select source_rows.*, private.catalog_portal_projection_payload_cn1('process', state_code, json) payload
  from source_rows
)
insert into private.portal_catalog_search_rows_v2
  (dataset_kind,id,version,state_code,modified_at,card,document,projection_contract_version)
select 'process', id,version::text,state_code,modified_at,payload -> 'card',payload ->> 'document',2
from payloads
on conflict (dataset_kind,id,version) do nothing;

-- Lock source tuples before deriving. Concurrent updates/deletes serialize behind
-- this page, and the dual writer subsequently installs the committed newer value.
do $bound$ begin
  if (select count(*) from (select 1 from public.flows source where source.id >= '50000000-0000-0000-0000-000000000000'::uuid and source.id < '60000000-0000-0000-0000-000000000000'::uuid and source.state_code in (100,200) and source.modified_at is not null and jsonb_typeof(source.json) = 'object' and jsonb_typeof(source.json -> 'flowDataSet') = 'object' limit 20001) bounded) > 20000 then
    raise exception 'Portal names shard exceeds reviewed 20000-row bound; use a reviewed finer partition';
  end if;
end $bound$;
with source_rows as materialized (
  select source.id, source.version, source.state_code, source.modified_at, source.json
  from public.flows source where source.id >= '50000000-0000-0000-0000-000000000000'::uuid and source.id < '60000000-0000-0000-0000-000000000000'::uuid and source.state_code in (100,200) and source.modified_at is not null and jsonb_typeof(source.json) = 'object' and jsonb_typeof(source.json -> 'flowDataSet') = 'object'
  order by source.id, source.version for share of source
), payloads as materialized (
  select source_rows.*, private.catalog_portal_projection_payload_cn1('flow', state_code, json) payload
  from source_rows
)
insert into private.portal_catalog_search_rows_v2
  (dataset_kind,id,version,state_code,modified_at,card,document,projection_contract_version)
select 'flow', id,version::text,state_code,modified_at,payload -> 'card',payload ->> 'document',2
from payloads
on conflict (dataset_kind,id,version) do nothing;

insert into private.portal_names_backfill_v2(shard,process_count,flow_count)
select 5, count(*) filter(where dataset_kind='process'), count(*) filter(where dataset_kind='flow')
from private.portal_catalog_search_rows_v2 where id >= '50000000-0000-0000-0000-000000000000'::uuid and id < '60000000-0000-0000-0000-000000000000'::uuid
on conflict(shard) do update set process_count=excluded.process_count,flow_count=excluded.flow_count,completed_at=pg_catalog.clock_timestamp();
revoke api_internal_executor from postgres;
commit;
