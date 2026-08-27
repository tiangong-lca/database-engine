-- Issue #539 Preview forward repair: an earlier PR head recorded a shared
-- latest-winner table. Replace it atomically with an exact-version child so
-- Portal maintenance never introduces a cross-version writer lock. Fresh
-- databases already have the exact final child and take the no-drift path.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_sitemap_version_repair_prerequisite_guard$
begin
  if pg_catalog.to_regclass(
       'private.portal_catalog_facet_rows_v1'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_sitemap_projection_v1()'
     ) is null
     or pg_catalog.to_regprocedure(
       'api.portal_sitemap_manifest_v1()'
     ) is null
     or pg_catalog.to_regprocedure(
       'api.portal_sitemap_shard_v1(text)'
     ) is null
     or (
       pg_catalog.to_regclass('private.portal_sitemap_rows_v1') is null
       and pg_catalog.to_regclass(
         'private.portal_sitemap_latest_rows_v1'
       ) is null
     ) then
    raise exception 'Portal sitemap version repair prerequisites are unsafe'
      using errcode = '55000';
  end if;
end
$portal_sitemap_version_repair_prerequisite_guard$;

-- Freeze the sole governed parent writer while the shadow child is populated,
-- the public shard reader is rebound, and the obsolete winner table is retired.
lock table private.portal_catalog_facet_rows_v1
  in share row exclusive mode;

create table if not exists private.portal_sitemap_rows_v1 (
  dataset_kind text not null
    check (dataset_kind in ('process', 'flow')),
  id uuid not null,
  version text not null
    check (version ~ '^\d{2}\.\d{2}\.\d{3}$'),
  modified_at timestamptz not null,
  shard_no smallint not null
    check (shard_no between 0 and 63),
  contract_version smallint not null
    check (contract_version = 1),
  primary key (dataset_kind, id, version),
  constraint portal_sitemap_rows_source_v1_fk
    foreign key (dataset_kind, id, version)
    references private.portal_catalog_facet_rows_v1(
      dataset_kind,
      id,
      version
    )
    on update restrict
    on delete cascade
);

alter table private.portal_sitemap_rows_v1 owner to postgres;
alter table private.portal_sitemap_rows_v1 enable row level security;
alter table private.portal_sitemap_rows_v1 force row level security;

do $install_portal_sitemap_rows_policies_v1$
begin
  if not exists (
    select 1
    from pg_catalog.pg_policy as policy
    where policy.polrelid = 'private.portal_sitemap_rows_v1'::regclass
      and policy.polname = 'portal_sitemap_rows_portal_select_v1'
  ) then
    execute $policy$
      create policy portal_sitemap_rows_portal_select_v1
      on private.portal_sitemap_rows_v1
      for select
      to portal_public_executor
      using (contract_version = 1 and shard_no between 0 and 63)
    $policy$;
  end if;

  if not exists (
    select 1
    from pg_catalog.pg_policy as policy
    where policy.polrelid = 'private.portal_sitemap_rows_v1'::regclass
      and policy.polname = 'portal_sitemap_rows_internal_all_v1'
  ) then
    execute $policy$
      create policy portal_sitemap_rows_internal_all_v1
      on private.portal_sitemap_rows_v1
      for all
      to api_internal_executor
      using (contract_version = 1 and shard_no between 0 and 63)
      with check (contract_version = 1 and shard_no between 0 and 63)
    $policy$;
  end if;
end
$install_portal_sitemap_rows_policies_v1$;

revoke all on table private.portal_sitemap_rows_v1
  from public, anon, authenticated, service_role;
grant select (
  dataset_kind,
  id,
  version,
  modified_at,
  shard_no,
  contract_version
) on table private.portal_sitemap_rows_v1
  to portal_public_executor;
grant select, insert, update, delete
on table private.portal_sitemap_rows_v1
  to api_internal_executor;

create index if not exists portal_sitemap_rows_shard_v1_idx
on private.portal_sitemap_rows_v1 (
  shard_no,
  contract_version,
  dataset_kind,
  id,
  version desc,
  modified_at desc
);

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
set role api_internal_executor;

create or replace function private.sync_portal_sitemap_row_v1()
returns trigger
language plpgsql
volatile
parallel unsafe
security definer
set search_path = ''
set row_security = 'on'
as $function$
begin
  insert into private.portal_sitemap_rows_v1 (
    dataset_kind,
    id,
    version,
    modified_at,
    shard_no,
    contract_version
  ) values (
    new.dataset_kind,
    new.id,
    new.version,
    new.modified_at,
    (
      pg_catalog.get_byte(
        pg_catalog.decode(
          pg_catalog.md5(
            new.dataset_kind || ':'::text || new.id::text
          ),
          'hex'::text
        ),
        0
      ) / 4
    )::smallint,
    1
  )
  on conflict (dataset_kind, id, version) do update
  set modified_at = excluded.modified_at,
      shard_no = excluded.shard_no,
      contract_version = excluded.contract_version
  where (
    portal_sitemap_rows_v1.modified_at,
    portal_sitemap_rows_v1.shard_no,
    portal_sitemap_rows_v1.contract_version
  ) is distinct from (
    excluded.modified_at,
    excluded.shard_no,
    excluded.contract_version
  );
  return null;
end
$function$;

revoke all on function private.sync_portal_sitemap_row_v1()
  from public, anon, authenticated, service_role, portal_public_executor;
grant execute on function private.sync_portal_sitemap_row_v1()
  to postgres;

reset role;
revoke create on schema private from api_internal_executor;

drop trigger if exists portal_sitemap_latest_sync_v1
  on private.portal_catalog_facet_rows_v1;
drop trigger if exists portal_sitemap_latest_delete_v1
  on private.portal_catalog_facet_rows_v1;

do $install_portal_sitemap_rows_trigger_v1$
begin
  if not exists (
    select 1
    from pg_catalog.pg_trigger as trigger
    where trigger.tgrelid =
      'private.portal_catalog_facet_rows_v1'::regclass
      and trigger.tgname = 'portal_sitemap_rows_sync_v1'
      and not trigger.tgisinternal
  ) then
    execute $trigger$
      create trigger portal_sitemap_rows_sync_v1
      after insert or update of
        dataset_kind,
        id,
        version,
        state_code,
        modified_at,
        facet_contract_version
      on private.portal_catalog_facet_rows_v1
      for each row
      execute function private.sync_portal_sitemap_row_v1()
    $trigger$;
  end if;
end
$install_portal_sitemap_rows_trigger_v1$;

comment on table private.portal_sitemap_rows_v1 is
  'Exact public Process/Flow version and stable 64-way sitemap bucket; contains no card, document, actor, credential, or locator.';
comment on index private.portal_sitemap_rows_shard_v1_idx is
  'Latest-version sitemap shard order over the exact-version locator-free projection.';
comment on trigger portal_sitemap_rows_sync_v1
  on private.portal_catalog_facet_rows_v1 is
  'Upserts only the affected exact sitemap version after a governed facet INSERT or UPDATE converges; DELETE follows the exact FK cascade.';

set role api_internal_executor;
revoke execute on function private.sync_portal_sitemap_row_v1()
  from postgres;

insert into private.portal_sitemap_rows_v1 (
  dataset_kind,
  id,
  version,
  modified_at,
  shard_no,
  contract_version
)
select facet.dataset_kind,
  facet.id,
  facet.version,
  facet.modified_at,
  (
    pg_catalog.get_byte(
      pg_catalog.decode(
        pg_catalog.md5(
          facet.dataset_kind || ':'::text || facet.id::text
        ),
        'hex'::text
      ),
      0
    ) / 4
  )::smallint,
  1
from private.portal_catalog_facet_rows_v1 as facet
where facet.state_code in (100, 200)
  and facet.facet_contract_version = 1
on conflict (dataset_kind, id, version) do update
set modified_at = excluded.modified_at,
    shard_no = excluded.shard_no,
    contract_version = excluded.contract_version
where (
  portal_sitemap_rows_v1.modified_at,
  portal_sitemap_rows_v1.shard_no,
  portal_sitemap_rows_v1.contract_version
) is distinct from (
  excluded.modified_at,
  excluded.shard_no,
  excluded.contract_version
);

delete from private.portal_sitemap_rows_v1 as rows
where not exists (
  select 1
  from private.portal_catalog_facet_rows_v1 as facet
  where facet.dataset_kind = rows.dataset_kind
    and facet.id = rows.id
    and facet.version = rows.version
    and facet.state_code in (100, 200)
    and facet.facet_contract_version = 1
);

reset role;
revoke api_internal_executor from postgres;

grant portal_public_executor to postgres;
grant create on schema private, api to portal_public_executor;
set role portal_public_executor;

create or replace function private.assert_portal_sitemap_projection_v1()
returns void
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
declare
  v_index regclass :=
    pg_catalog.to_regclass('private.portal_sitemap_rows_shard_v1_idx');
begin
  if v_index is null
     or pg_catalog.to_regclass(
       'private.portal_sitemap_latest_rows_v1'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.sync_portal_sitemap_latest_delete_v1()'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.sync_portal_sitemap_latest_row_v1()'
     ) is not null
     or not exists (
       select 1
       from pg_catalog.pg_class as relation
       where relation.oid = 'private.portal_sitemap_rows_v1'::regclass
         and relation.relowner = 'postgres'::regrole
         and relation.relrowsecurity
         and relation.relforcerowsecurity
     )
     or not exists (
       select 1
       from pg_catalog.pg_constraint as constraint_catalog
       where constraint_catalog.conrelid =
         'private.portal_sitemap_rows_v1'::regclass
         and constraint_catalog.contype = 'p'
         and constraint_catalog.conkey = array[
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = constraint_catalog.conrelid
               and attribute.attname = 'dataset_kind'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = constraint_catalog.conrelid
               and attribute.attname = 'id'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = constraint_catalog.conrelid
               and attribute.attname = 'version'
           )
         ]::smallint[]
     )
     or not exists (
       select 1
       from pg_catalog.pg_index as index_catalog
       where index_catalog.indexrelid = v_index
         and index_catalog.indrelid =
           'private.portal_sitemap_rows_v1'::regclass
         and index_catalog.indisvalid
         and index_catalog.indisready
         and index_catalog.indislive
         and index_catalog.indnkeyatts = 6
         and index_catalog.indnatts = 6
         and index_catalog.indpred is null
         and index_catalog.indexprs is null
         and pg_catalog.string_to_array(
           index_catalog.indoption::text,
           ' '
         )::smallint[] = array[0, 0, 0, 0, 3, 3]::smallint[]
         and pg_catalog.string_to_array(
           index_catalog.indkey::text,
           ' '
         )::smallint[] = array[
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'shard_no'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'contract_version'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'dataset_kind'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'id'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'version'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'modified_at'
           )
         ]::smallint[]
         and array(
           select operator_class.opcname
           from pg_catalog.unnest(index_catalog.indclass::oid[])
             with ordinality as class_oid(oid, ordinality)
           join pg_catalog.pg_opclass as operator_class
             on operator_class.oid = class_oid.oid
           order by class_oid.ordinality
         ) = array[
           'int2_ops',
           'int2_ops',
           'text_ops',
           'uuid_ops',
           'text_ops',
           'timestamptz_ops'
         ]::name[]
     )
     or not exists (
       select 1
       from pg_catalog.pg_constraint as constraint_catalog
       where constraint_catalog.conrelid =
         'private.portal_sitemap_rows_v1'::regclass
         and constraint_catalog.confrelid =
           'private.portal_catalog_facet_rows_v1'::regclass
         and constraint_catalog.conname =
           'portal_sitemap_rows_source_v1_fk'
         and constraint_catalog.contype = 'f'
         and constraint_catalog.convalidated
         and constraint_catalog.confupdtype = 'r'
         and constraint_catalog.confdeltype = 'c'
         and constraint_catalog.conkey = array[
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = constraint_catalog.conrelid
               and attribute.attname = 'dataset_kind'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = constraint_catalog.conrelid
               and attribute.attname = 'id'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = constraint_catalog.conrelid
               and attribute.attname = 'version'
           )
         ]::smallint[]
         and constraint_catalog.confkey = array[
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = constraint_catalog.confrelid
               and attribute.attname = 'dataset_kind'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = constraint_catalog.confrelid
               and attribute.attname = 'id'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = constraint_catalog.confrelid
               and attribute.attname = 'version'
           )
         ]::smallint[]
     )
     or not exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid =
         'private.portal_catalog_facet_rows_v1'::regclass
         and trigger.tgname = 'portal_sitemap_rows_sync_v1'
         and not trigger.tgisinternal
         and trigger.tgenabled = 'O'
         and trigger.tgtype = 21
         and pg_catalog.string_to_array(
           trigger.tgattr::text,
           ' '
         )::smallint[] = array[
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = trigger.tgrelid
               and attribute.attname = 'dataset_kind'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = trigger.tgrelid
               and attribute.attname = 'id'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = trigger.tgrelid
               and attribute.attname = 'version'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = trigger.tgrelid
               and attribute.attname = 'state_code'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = trigger.tgrelid
               and attribute.attname = 'modified_at'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = trigger.tgrelid
               and attribute.attname = 'facet_contract_version'
           )
         ]::smallint[]
         and trigger.tgfoid =
           'private.sync_portal_sitemap_row_v1()'::regprocedure
     )
     or exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid =
         'private.portal_catalog_facet_rows_v1'::regclass
         and trigger.tgname = 'portal_sitemap_latest_delete_v1'
         and not trigger.tgisinternal
     )
     or not exists (
       select 1
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.sync_portal_sitemap_row_v1()'::regprocedure
         and routine.proowner = 'api_internal_executor'::regrole
         and routine.prosecdef
         and routine.provolatile = 'v'
         and routine.proparallel = 'u'
         and coalesce(routine.proconfig, '{}'::text[]) @> array[
           'search_path=""',
           'row_security=on'
         ]::text[]
         and pg_catalog.md5(routine.prosrc) =
           '9bc7007c0e8fef48c75d997ea8ef96d8'
     ) then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  end if;
end
$function$;

create or replace function api.portal_sitemap_shard_v1(p_shard_cursor text)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '4s'
set work_mem = '8MB'
set plan_cache_mode = 'force_custom_plan'
set max_parallel_workers_per_gather = '0'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_cursor jsonb;
  v_expected_cursor jsonb;
  v_bucket integer;
  v_items jsonb;
  v_result jsonb;
begin
  if p_shard_cursor is null
     or pg_catalog.octet_length(p_shard_cursor) not between 1 and 4096
     or p_shard_cursor ~ '[[:space:]]' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  v_cursor := private.portal_cursor_decode_v1(p_shard_cursor);
  if v_cursor is null
     or pg_catalog.jsonb_typeof(v_cursor) <> 'object'
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_cursor)) <> 4
     or coalesce(v_cursor ->> 'bucket', '') !~ '^([0-9]|[1-5][0-9]|6[0-3])$' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_bucket := (v_cursor ->> 'bucket')::integer;
  v_expected_cursor := pg_catalog.jsonb_build_object(
    'v', 1,
    'scope', 'sitemap-shard',
    'bucket', v_bucket,
    'shardCount', 64
  );

  if v_cursor is distinct from v_expected_cursor
     or private.portal_cursor_encode_v1(v_expected_cursor) <>
       p_shard_cursor then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  perform private.assert_portal_catalog_projection_contract_v1();
  perform private.assert_portal_catalog_facet_contract_v1();
  perform private.assert_portal_sitemap_projection_v1();

  with latest as materialized (
    select distinct on (projection.dataset_kind, projection.id)
      projection.dataset_kind,
      projection.id,
      projection.version,
      projection.modified_at
    from private.portal_sitemap_rows_v1 as projection
    where projection.shard_no = v_bucket
      and projection.contract_version = 1
    order by projection.dataset_kind,
      projection.id,
      projection.version desc,
      projection.modified_at desc
    limit 4097
  )
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'key', pg_catalog.jsonb_build_object(
      'kind', latest.dataset_kind,
      'id', latest.id::text,
      'version', latest.version
    ),
    'modifiedAt', private.portal_timestamp_v1(latest.modified_at)
  ) order by latest.dataset_kind, latest.id), '[]'::jsonb)
  into v_items
  from latest;

  if pg_catalog.jsonb_array_length(v_items) > 4096 then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  end if;

  v_result := pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-sitemap-shard.v1',
    'shardCursor', p_shard_cursor,
    'items', v_items
  );
  if pg_catalog.octet_length(v_result::text) > 2 * 1024 * 1024 then
    raise exception using
      errcode = '54000',
      message = 'portal sitemap response exceeded its budget';
  end if;
  return v_result;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  when others then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
end
$function$;

reset role;
revoke create on schema private, api from portal_public_executor;
revoke portal_public_executor from postgres;

-- The public reader and assertion now reference only the new exact-version
-- child, so retiring the old trigger/functions/table is dependency-safe.
drop trigger if exists portal_sitemap_latest_sync_v1
  on private.portal_catalog_facet_rows_v1;
drop trigger if exists portal_sitemap_latest_delete_v1
  on private.portal_catalog_facet_rows_v1;
drop function if exists private.sync_portal_sitemap_latest_delete_v1();
drop function if exists private.sync_portal_sitemap_latest_row_v1();
drop table if exists private.portal_sitemap_latest_rows_v1;

grant portal_public_executor to postgres;
set role portal_public_executor;
select private.assert_portal_sitemap_projection_v1();

do $verify_portal_sitemap_version_public_contract$
begin
  if pg_catalog.jsonb_array_length(
       api.portal_sitemap_manifest_v1() -> 'shards'
     ) <> 64 then
    raise exception 'Portal sitemap version repair public contract drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_sitemap_version_public_contract$;

reset role;
revoke portal_public_executor from postgres;

grant api_internal_executor to postgres;
set role api_internal_executor;

do $verify_portal_sitemap_version_repair$
begin
  if pg_catalog.to_regclass(
       'private.portal_sitemap_latest_rows_v1'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.sync_portal_sitemap_latest_row_v1()'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.sync_portal_sitemap_latest_delete_v1()'
     ) is not null
     or exists (
       with expected as (
         select facet.dataset_kind,
           facet.id,
           facet.version,
           facet.modified_at,
           (
             pg_catalog.get_byte(
               pg_catalog.decode(
                 pg_catalog.md5(
                   facet.dataset_kind || ':'::text || facet.id::text
                 ),
                 'hex'::text
               ),
               0
             ) / 4
           )::smallint as shard_no,
           1::smallint as contract_version
         from private.portal_catalog_facet_rows_v1 as facet
         where facet.state_code in (100, 200)
           and facet.facet_contract_version = 1
       )
       (select * from expected
        except
        select * from private.portal_sitemap_rows_v1)
       union all
       (select * from private.portal_sitemap_rows_v1
        except
        select * from expected)
     ) then
    raise exception 'Portal sitemap version repair did not converge'
      using errcode = '55000';
  end if;
end
$verify_portal_sitemap_version_repair$;

reset role;
revoke api_internal_executor from postgres;

commit;
