-- Issue #539 Preview concurrency forward repair: the first PR head exposed the
-- latest-only projection before its same-identity writers were transactionally
-- serialized.
-- Fresh databases already receive the final advisory fence from 20260827134101;
-- an existing PR Preview receives and validates it here without editing history.
-- The latest table remains deliberately FK-free so serialized version deletes
-- never acquire a reverse dependency on another transaction's facet row lock.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $portal_sitemap_latest_concurrency_prerequisite_guard$
begin
  if pg_catalog.to_regclass(
       'private.portal_sitemap_latest_rows_v1'
     ) is null
     or pg_catalog.to_regclass(
       'private.portal_catalog_facet_rows_v1'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.sync_portal_sitemap_latest_row_v1()'
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
     or exists (
       select 1
       from pg_catalog.pg_constraint as constraint_catalog
       where constraint_catalog.conrelid =
         'private.portal_sitemap_latest_rows_v1'::regclass
         and constraint_catalog.contype = 'f'
     ) then
    raise exception 'Portal sitemap latest concurrency prerequisites are unsafe'
      using errcode = '55000';
  end if;
end
$portal_sitemap_latest_concurrency_prerequisite_guard$;

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
set role api_internal_executor;

create or replace function private.sync_portal_sitemap_latest_row_v1()
returns trigger
language plpgsql
volatile
parallel unsafe
security definer
set search_path = ''
set row_security = 'on'
as $function$
begin
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      new.dataset_kind || ':'::text || new.id::text,
      539
    )
  );

  insert into private.portal_sitemap_latest_rows_v1 (
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
  on conflict (dataset_kind, id) do update
  set version = excluded.version,
      modified_at = excluded.modified_at,
      shard_no = excluded.shard_no,
      contract_version = excluded.contract_version
  where excluded.version > portal_sitemap_latest_rows_v1.version
     or (
       excluded.version = portal_sitemap_latest_rows_v1.version
       and (
         portal_sitemap_latest_rows_v1.modified_at,
         portal_sitemap_latest_rows_v1.shard_no,
         portal_sitemap_latest_rows_v1.contract_version
       ) is distinct from (
         excluded.modified_at,
         excluded.shard_no,
         excluded.contract_version
       )
     );
  return null;
end
$function$;

create or replace function private.sync_portal_sitemap_latest_delete_v1()
returns trigger
language plpgsql
volatile
parallel unsafe
security definer
set search_path = ''
set row_security = 'on'
as $function$
declare
  v_current_version text;
  v_fallback record;
begin
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      old.dataset_kind || ':'::text || old.id::text,
      539
    )
  );

  select latest.version
  into v_current_version
  from private.portal_sitemap_latest_rows_v1 as latest
  where latest.dataset_kind = old.dataset_kind
    and latest.id = old.id
  for update;

  if not found or v_current_version <> old.version then
    return old;
  end if;

  select facet.version,
    facet.modified_at
  into v_fallback
  from private.portal_catalog_facet_rows_v1 as facet
  where facet.dataset_kind = old.dataset_kind
    and facet.id = old.id
    and facet.version <> old.version
    and facet.state_code in (100, 200)
    and facet.facet_contract_version = 1
  order by facet.version desc,
    facet.modified_at desc,
    facet.state_code desc
  limit 1;

  if found then
    update private.portal_sitemap_latest_rows_v1 as latest
    set version = v_fallback.version,
        modified_at = v_fallback.modified_at
    where latest.dataset_kind = old.dataset_kind
      and latest.id = old.id
      and latest.version = old.version;
  else
    delete from private.portal_sitemap_latest_rows_v1 as latest
    where latest.dataset_kind = old.dataset_kind
      and latest.id = old.id
      and latest.version = old.version;
  end if;
  return old;
end
$function$;

revoke all on function private.sync_portal_sitemap_latest_row_v1()
  from public, anon, authenticated, service_role, portal_public_executor;
revoke all on function private.sync_portal_sitemap_latest_delete_v1()
  from public, anon, authenticated, service_role, portal_public_executor;
grant execute on function private.sync_portal_sitemap_latest_row_v1()
  to postgres;
grant execute on function private.sync_portal_sitemap_latest_delete_v1()
  to postgres;

reset role;
revoke create on schema private from api_internal_executor;

drop trigger if exists portal_sitemap_latest_sync_v1
  on private.portal_catalog_facet_rows_v1;
drop trigger if exists portal_sitemap_latest_delete_v1
  on private.portal_catalog_facet_rows_v1;

create trigger portal_sitemap_latest_sync_v1
after insert or update of
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  facet_contract_version
on private.portal_catalog_facet_rows_v1
for each row
execute function private.sync_portal_sitemap_latest_row_v1();

create trigger portal_sitemap_latest_delete_v1
before delete
on private.portal_catalog_facet_rows_v1
for each row
execute function private.sync_portal_sitemap_latest_delete_v1();

comment on trigger portal_sitemap_latest_sync_v1
  on private.portal_catalog_facet_rows_v1 is
  'Upserts only the affected latest sitemap identity after a governed facet INSERT or UPDATE converges.';
comment on trigger portal_sitemap_latest_delete_v1
  on private.portal_catalog_facet_rows_v1 is
  'Serializes one facet identity before DELETE and rebinds its latest sitemap row to the visible predecessor.';

set role api_internal_executor;
revoke execute on function private.sync_portal_sitemap_latest_row_v1()
  from postgres;
revoke execute on function private.sync_portal_sitemap_latest_delete_v1()
  from postgres;
reset role;
revoke api_internal_executor from postgres;

grant api_internal_executor to postgres;
set role api_internal_executor;

insert into private.portal_sitemap_latest_rows_v1 (
  dataset_kind,
  id,
  version,
  modified_at,
  shard_no,
  contract_version
)
select distinct on (facet.dataset_kind, facet.id)
  facet.dataset_kind,
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
order by facet.dataset_kind,
  facet.id,
  facet.version desc,
  facet.modified_at desc,
  facet.state_code desc
on conflict (dataset_kind, id) do update
set version = excluded.version,
    modified_at = excluded.modified_at,
    shard_no = excluded.shard_no,
    contract_version = excluded.contract_version
where (
  portal_sitemap_latest_rows_v1.version,
  portal_sitemap_latest_rows_v1.modified_at,
  portal_sitemap_latest_rows_v1.shard_no,
  portal_sitemap_latest_rows_v1.contract_version
) is distinct from (
  excluded.version,
  excluded.modified_at,
  excluded.shard_no,
  excluded.contract_version
);

delete from private.portal_sitemap_latest_rows_v1 as latest
where not exists (
  select 1
  from private.portal_catalog_facet_rows_v1 as facet
  where facet.dataset_kind = latest.dataset_kind
    and facet.id = latest.id
    and facet.version = latest.version
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
    pg_catalog.to_regclass('private.portal_sitemap_latest_shard_v1_idx');
begin
  if v_index is null
     or not exists (
       select 1
       from pg_catalog.pg_index as index_catalog
       where index_catalog.indexrelid = v_index
         and index_catalog.indrelid =
           'private.portal_sitemap_latest_rows_v1'::regclass
         and index_catalog.indisvalid
         and index_catalog.indisready
         and index_catalog.indislive
         and index_catalog.indnkeyatts = 3
         and index_catalog.indnatts = 6
         and index_catalog.indpred is null
         and index_catalog.indexprs is null
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
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'contract_version'
           )
         ]::smallint[]
         and array(
           select operator_class.opcname
           from pg_catalog.unnest(index_catalog.indclass::oid[])
             with ordinality as class_oid(oid, ordinality)
           join pg_catalog.pg_opclass as operator_class
             on operator_class.oid = class_oid.oid
           order by class_oid.ordinality
         ) = array['int2_ops', 'text_ops', 'uuid_ops']::name[]
     )
     or exists (
       select 1
       from pg_catalog.pg_constraint as constraint_catalog
       where constraint_catalog.conrelid =
         'private.portal_sitemap_latest_rows_v1'::regclass
         and constraint_catalog.contype = 'f'
     )
     or not exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid =
         'private.portal_catalog_facet_rows_v1'::regclass
         and trigger.tgname = 'portal_sitemap_latest_sync_v1'
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
           'private.sync_portal_sitemap_latest_row_v1()'::regprocedure
     )
     or not exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid =
         'private.portal_catalog_facet_rows_v1'::regclass
         and trigger.tgname = 'portal_sitemap_latest_delete_v1'
         and not trigger.tgisinternal
         and trigger.tgenabled = 'O'
         and trigger.tgtype = 11
         and trigger.tgfoid =
           'private.sync_portal_sitemap_latest_delete_v1()'::regprocedure
     )
     or not exists (
       select 1
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.sync_portal_sitemap_latest_row_v1()'::regprocedure
         and routine.proowner = 'api_internal_executor'::regrole
         and routine.prosecdef
         and routine.provolatile = 'v'
         and routine.proparallel = 'u'
         and coalesce(routine.proconfig, '{}'::text[]) @> array[
           'search_path=""',
           'row_security=on'
         ]::text[]
         and pg_catalog.md5(routine.prosrc) =
           '45503a8c8455b9ae9e69bc15d150d97f'
     )
     or not exists (
       select 1
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.sync_portal_sitemap_latest_delete_v1()'::regprocedure
         and routine.proowner = 'api_internal_executor'::regrole
         and routine.prosecdef
         and routine.provolatile = 'v'
         and routine.proparallel = 'u'
         and coalesce(routine.proconfig, '{}'::text[]) @> array[
           'search_path=""',
           'row_security=on'
         ]::text[]
         and pg_catalog.md5(routine.prosrc) =
           '4278224e16a7f1932d0f3debbc245b2b'
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

  if v_cursor is distinct from pg_catalog.jsonb_build_object(
       'v', 1,
       'scope', 'sitemap-shard',
       'bucket', v_bucket,
       'shardCount', 64
     )
     or private.portal_cursor_encode_v1(v_cursor) <> p_shard_cursor then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  perform private.assert_portal_catalog_projection_contract_v1();
  perform private.assert_portal_catalog_facet_contract_v1();
  perform private.assert_portal_sitemap_projection_v1();

  with latest as materialized (
    select projection.dataset_kind,
      projection.id,
      projection.version,
      projection.modified_at
    from private.portal_sitemap_latest_rows_v1 as projection
    where projection.shard_no = v_bucket
      and projection.contract_version = 1
    order by projection.dataset_kind,
      projection.id
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

select private.assert_portal_sitemap_projection_v1();

reset role;
revoke create on schema private, api from portal_public_executor;
revoke portal_public_executor from postgres;

grant api_internal_executor to postgres;
set role api_internal_executor;

do $verify_portal_sitemap_latest_concurrency_repair$
begin
  if exists (
    select 1
    from pg_catalog.pg_constraint as constraint_catalog
    where constraint_catalog.conrelid =
      'private.portal_sitemap_latest_rows_v1'::regclass
      and constraint_catalog.contype = 'f'
  )
  or not exists (
    select 1
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.sync_portal_sitemap_latest_row_v1()'::regprocedure
      and routine.proowner = 'api_internal_executor'::regrole
      and pg_catalog.md5(routine.prosrc) =
        '45503a8c8455b9ae9e69bc15d150d97f'
      and coalesce(routine.proacl::text, '') =
        '{api_internal_executor=X/api_internal_executor}'
  )
  or not exists (
    select 1
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.sync_portal_sitemap_latest_delete_v1()'::regprocedure
      and routine.proowner = 'api_internal_executor'::regrole
      and pg_catalog.md5(routine.prosrc) =
        '4278224e16a7f1932d0f3debbc245b2b'
      and coalesce(routine.proacl::text, '') =
        '{api_internal_executor=X/api_internal_executor}'
  )
  or (
    select pg_catalog.count(*)
    from pg_catalog.pg_trigger as trigger
    where trigger.tgrelid =
      'private.portal_catalog_facet_rows_v1'::regclass
      and not trigger.tgisinternal
      and trigger.tgenabled = 'O'
      and (
        (
          trigger.tgname = 'portal_sitemap_latest_sync_v1'
          and trigger.tgtype = 21
          and trigger.tgfoid =
            'private.sync_portal_sitemap_latest_row_v1()'::regprocedure
        )
        or (
          trigger.tgname = 'portal_sitemap_latest_delete_v1'
          and trigger.tgtype = 11
          and trigger.tgfoid =
            'private.sync_portal_sitemap_latest_delete_v1()'::regprocedure
        )
      )
  ) <> 2
  or (
    select routine.prosrc !~ '45503a8c8455b9ae9e69bc15d150d97f'
      or routine.prosrc !~ '4278224e16a7f1932d0f3debbc245b2b'
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.assert_portal_sitemap_projection_v1()'::regprocedure
  )
  or (
    select routine.prosrc !~
      'v_cursor is distinct from pg_catalog.jsonb_build_object'
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'api.portal_sitemap_shard_v1(text)'::regprocedure
  )
  or exists (
    with expected as (
      select distinct on (facet.dataset_kind, facet.id)
        facet.dataset_kind,
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
      order by facet.dataset_kind,
        facet.id,
        facet.version desc,
        facet.modified_at desc,
        facet.state_code desc
    )
    (select * from expected
     except
     select * from private.portal_sitemap_latest_rows_v1)
    union all
    (select * from private.portal_sitemap_latest_rows_v1
     except
     select * from expected)
  ) then
    raise exception 'Portal sitemap latest concurrency repair did not converge'
      using errcode = '55000';
  end if;
end
$verify_portal_sitemap_latest_concurrency_repair$;

reset role;
revoke api_internal_executor from postgres;

commit;
