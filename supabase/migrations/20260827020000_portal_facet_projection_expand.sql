-- Issue #531: expand a narrow, independently versioned facet projection.
-- Existing Portal Search/Hybrid/Facets reads remain unchanged until the final
-- reconcile-and-cutover migration succeeds.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $portal_facet_projection_role_guard$
begin
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'api_internal_executor'
      and not rolcanlogin
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) or not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'portal_public_executor'
      and not rolcanlogin
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) then
    raise exception 'Portal facet projection executor prerequisite is unsafe'
      using errcode = '42501';
  end if;
end
$portal_facet_projection_role_guard$;

-- The source JSON synchronizers copy json_ordered into NEW.json and may also
-- change NEW.version, but PostgreSQL UPDATE OF triggers are selected from the
-- original SET list. Listen to json_ordered explicitly so normal dataset
-- command writes cannot leave the synchronized public-safe projection stale.
grant api_internal_executor to postgres;
set role api_internal_executor;
grant execute on function private.sync_portal_catalog_search_row_v1()
to postgres;
reset role;

drop trigger if exists portal_catalog_projection_content_sync_v1
on public.processes;
create trigger portal_catalog_projection_content_sync_v1
after insert or delete or update of
  id,
  version,
  json,
  json_ordered,
  state_code,
  modified_at
on public.processes
for each row execute function private.sync_portal_catalog_search_row_v1('content');

drop trigger if exists portal_catalog_projection_content_sync_v1
on public.flows;
create trigger portal_catalog_projection_content_sync_v1
after insert or delete or update of
  id,
  version,
  json,
  json_ordered,
  state_code,
  modified_at
on public.flows
for each row execute function private.sync_portal_catalog_search_row_v1('content');

set role api_internal_executor;
revoke execute on function private.sync_portal_catalog_search_row_v1()
from postgres;
reset role;

grant create on schema private to api_internal_executor;
set role api_internal_executor;

select private.assert_portal_catalog_projection_contract_v1();

create function private.portal_catalog_facet_facts_v1(
  p_kind text,
  p_card jsonb
)
returns table(
  facet_access_level text,
  facet_geography text,
  facet_reference_year text,
  facet_process_subtype text,
  facet_source text
)
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select
    p_card ->> 'accessLevel',
    pg_catalog.lower(pg_catalog.btrim(
      p_card #>> '{geography,code}'
    )),
    pg_catalog.btrim(p_card ->> 'referenceYear'),
    case when p_kind = 'process' then
      pg_catalog.lower(pg_catalog.btrim(
        p_card ->> 'processSubtype'
      ))
    else null::text end,
    pg_catalog.lower(pg_catalog.btrim(p_card ->> 'source'))
$function$;

comment on function private.portal_catalog_facet_facts_v1(text, jsonb) is
  'Immutable v1 facet facts derived only from an already public-safe Portal card.';

revoke all on function private.portal_catalog_facet_facts_v1(text, jsonb)
from public, anon, authenticated, service_role, portal_public_executor;

reset role;

create table private.portal_catalog_facet_contract_v1 (
  contract_version smallint primary key,
  manifest_schema text not null,
  function_identities text[] not null,
  manifest_sha256 text not null,
  created_by_migration text not null,
  constraint portal_catalog_facet_contract_version_v1_chk
    check (contract_version = 1),
  constraint portal_catalog_facet_contract_schema_v1_chk
    check (
      manifest_schema = 'portal.catalog-facet-function-manifest.v1'
    ),
  constraint portal_catalog_facet_contract_functions_v1_chk
    check (
      function_identities = array[
        'private.portal_catalog_facet_facts_v1(text,jsonb)',
        'private.sync_portal_catalog_facet_row_v1()'
      ]::text[]
    ),
  constraint portal_catalog_facet_contract_digest_v1_chk
    check (
      manifest_sha256 =
        'b238e9573ef08a9339062a2fa3092c0776318d13979ec8bf54ffc7a1ba0c7e3a'
    ),
  constraint portal_catalog_facet_contract_migration_v1_chk
    check (created_by_migration = '20260827020000')
);

alter table private.portal_catalog_facet_contract_v1 owner to postgres;
alter table private.portal_catalog_facet_contract_v1 enable row level security;
alter table private.portal_catalog_facet_contract_v1 force row level security;

create policy portal_catalog_facet_contract_internal_select_v1
on private.portal_catalog_facet_contract_v1
for select
to api_internal_executor
using (contract_version = 1);

revoke all on table private.portal_catalog_facet_contract_v1
from public, anon, authenticated, service_role, portal_public_executor;
grant select on table private.portal_catalog_facet_contract_v1
to api_internal_executor;

create table private.portal_catalog_facet_rows_v1 (
  dataset_kind text not null
    check (dataset_kind in ('process', 'flow')),
  id uuid not null,
  version text not null
    check (version ~ '^\d{2}\.\d{2}\.\d{3}$'),
  state_code integer not null
    check (state_code in (100, 200)),
  modified_at timestamptz not null,
  facet_access_level text,
  facet_geography text,
  facet_reference_year text,
  facet_process_subtype text,
  facet_source text,
  facet_contract_version smallint not null,
  primary key (dataset_kind, id, version),
  constraint portal_catalog_facet_rows_projection_v1_fk
    foreign key (dataset_kind, id, version)
    references private.portal_catalog_search_rows_v1(
      dataset_kind,
      id,
      version
    )
    on update restrict
    on delete cascade,
  constraint portal_catalog_facet_rows_contract_version_v1_chk
    check (facet_contract_version = 1),
  constraint portal_catalog_facet_rows_contract_version_v1_fk
    foreign key (facet_contract_version)
    references private.portal_catalog_facet_contract_v1(contract_version)
    on update restrict
    on delete restrict,
  constraint portal_catalog_facet_rows_process_subtype_v1_chk
    check (
      dataset_kind = 'process'
      or facet_process_subtype is null
    )
);

alter table private.portal_catalog_facet_rows_v1 owner to postgres;
alter table private.portal_catalog_facet_rows_v1 enable row level security;
alter table private.portal_catalog_facet_rows_v1 force row level security;

create policy portal_catalog_facet_rows_portal_select_v1
on private.portal_catalog_facet_rows_v1
for select
to portal_public_executor
using (state_code in (100, 200) and facet_contract_version = 1);

create policy portal_catalog_facet_rows_internal_all_v1
on private.portal_catalog_facet_rows_v1
for all
to api_internal_executor
using (state_code in (100, 200) and facet_contract_version = 1)
with check (state_code in (100, 200) and facet_contract_version = 1);

revoke all on table private.portal_catalog_facet_rows_v1
from public, anon, authenticated, service_role;
grant select (
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  facet_access_level,
  facet_geography,
  facet_reference_year,
  facet_process_subtype,
  facet_source,
  facet_contract_version
) on table private.portal_catalog_facet_rows_v1
to portal_public_executor;
grant select, insert, update, delete
on table private.portal_catalog_facet_rows_v1
to api_internal_executor;

create index portal_catalog_facet_rows_latest_v1_idx
on private.portal_catalog_facet_rows_v1 (
  dataset_kind,
  id,
  version desc,
  modified_at desc,
  state_code desc
);

set role api_internal_executor;

create function private.sync_portal_catalog_facet_row_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
set row_security = 'on'
as $function$
declare
  v_facts record;
begin
  select facts.*
  into strict v_facts
  from private.portal_catalog_facet_facts_v1(
    new.dataset_kind,
    new.card
  ) as facts;

  insert into private.portal_catalog_facet_rows_v1 (
    dataset_kind,
    id,
    version,
    state_code,
    modified_at,
    facet_access_level,
    facet_geography,
    facet_reference_year,
    facet_process_subtype,
    facet_source,
    facet_contract_version
  ) values (
    new.dataset_kind,
    new.id,
    new.version,
    new.state_code,
    new.modified_at,
    v_facts.facet_access_level,
    v_facts.facet_geography,
    v_facts.facet_reference_year,
    v_facts.facet_process_subtype,
    v_facts.facet_source,
    1
  )
  on conflict (dataset_kind, id, version) do update
  set state_code = excluded.state_code,
      modified_at = excluded.modified_at,
      facet_access_level = excluded.facet_access_level,
      facet_geography = excluded.facet_geography,
      facet_reference_year = excluded.facet_reference_year,
      facet_process_subtype = excluded.facet_process_subtype,
      facet_source = excluded.facet_source,
      facet_contract_version = excluded.facet_contract_version;

  return new;
end
$function$;

comment on function private.sync_portal_catalog_facet_row_v1() is
  'Maintains the narrow facet projection after each synchronized public-safe card write.';

revoke all on function private.sync_portal_catalog_facet_row_v1()
from public, anon, authenticated, service_role, portal_public_executor;

reset role;

create trigger portal_catalog_facet_sync_v1
after insert or update of
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  card
on private.portal_catalog_search_rows_v1
for each row execute function private.sync_portal_catalog_facet_row_v1();

set role api_internal_executor;

create function private.portal_catalog_facet_manifest_sha256_v1()
returns text
language sql
stable
parallel restricted
security definer
set search_path = ''
set row_security = 'on'
as $function$
  with expected(identity) as (
    values
      ('private.portal_catalog_facet_facts_v1(text,jsonb)'::text),
      ('private.sync_portal_catalog_facet_row_v1()')
  ), manifest_entries as (
    select expected.identity,
      pg_catalog.jsonb_build_object(
        'identity', expected.identity,
        'definition', pg_catalog.pg_get_functiondef(routine.oid),
        'owner', pg_catalog.pg_get_userbyid(routine.proowner),
        'language', language.lanname,
        'volatility', routine.provolatile,
        'parallel', routine.proparallel,
        'securityDefiner', routine.prosecdef,
        'config', coalesce(
          pg_catalog.to_jsonb(routine.proconfig),
          'null'::jsonb
        )
      )::text as entry
    from expected
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(expected.identity)
    join pg_catalog.pg_language as language
      on language.oid = routine.prolang
  )
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.string_agg(
          manifest_entries.entry,
          E'\n'
          order by manifest_entries.identity
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  from manifest_entries
$function$;

revoke all on function private.portal_catalog_facet_manifest_sha256_v1()
from public, anon, authenticated, service_role, portal_public_executor;

reset role;

insert into private.portal_catalog_facet_contract_v1 (
  contract_version,
  manifest_schema,
  function_identities,
  manifest_sha256,
  created_by_migration
)
select
  1,
  'portal.catalog-facet-function-manifest.v1',
  array[
    'private.portal_catalog_facet_facts_v1(text,jsonb)',
    'private.sync_portal_catalog_facet_row_v1()'
  ]::text[],
  'b238e9573ef08a9339062a2fa3092c0776318d13979ec8bf54ffc7a1ba0c7e3a',
  '20260827020000';

set role api_internal_executor;

create function private.assert_portal_catalog_facet_contract_v1()
returns void
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set row_security = 'on'
as $function$
declare
  v_expected_identities constant text[] := array[
    'private.portal_catalog_facet_facts_v1(text,jsonb)',
    'private.sync_portal_catalog_facet_row_v1()'
  ]::text[];
  v_expected_digest constant text :=
    'b238e9573ef08a9339062a2fa3092c0776318d13979ec8bf54ffc7a1ba0c7e3a';
  v_live_digest text;
begin
  select private.portal_catalog_facet_manifest_sha256_v1()
  into v_live_digest;

  if v_live_digest is distinct from v_expected_digest
     or (
       select count(*)
       from private.portal_catalog_facet_contract_v1 as contract
       where contract.contract_version = 1
         and contract.manifest_schema =
           'portal.catalog-facet-function-manifest.v1'
         and contract.function_identities = v_expected_identities
         and contract.manifest_sha256 = v_expected_digest
         and contract.created_by_migration = '20260827020000'
     ) <> 1
     or (
       select count(*)
       from private.portal_catalog_facet_contract_v1
     ) <> 1
     or (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_facet_contract_v1'::regclass
     ) is not false
     or (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_facet_rows_v1'::regclass
     ) is not false
     or not exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and trigger.tgname = 'portal_catalog_facet_sync_v1'
         and trigger.tgfoid =
           'private.sync_portal_catalog_facet_row_v1()'::regprocedure
         and trigger.tgenabled = 'O'
         and not trigger.tgisinternal
         and trigger.tgtype = 21
         and array(
           select attribute.attname
           from unnest(trigger.tgattr::smallint[])
             with ordinality as trigger_column(attnum, ordinality)
           join pg_catalog.pg_attribute as attribute
             on attribute.attrelid = trigger.tgrelid
            and attribute.attnum = trigger_column.attnum
           order by trigger_column.ordinality
         ) = array[
           'dataset_kind',
           'id',
           'version',
           'state_code',
           'modified_at',
           'card'
         ]::name[]
     )
     or exists (
       select 1
       from pg_catalog.pg_constraint as constraint_catalog
       where constraint_catalog.conrelid in (
           'private.portal_catalog_facet_contract_v1'::regclass,
           'private.portal_catalog_facet_rows_v1'::regclass
         )
         and not constraint_catalog.convalidated
     )
     or not exists (
       select 1
       from pg_catalog.pg_constraint as parent_fk
       where parent_fk.conrelid =
           'private.portal_catalog_facet_rows_v1'::regclass
         and parent_fk.confrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and parent_fk.conname =
           'portal_catalog_facet_rows_projection_v1_fk'
         and parent_fk.contype = 'f'
         and parent_fk.convalidated
         and parent_fk.confupdtype = 'r'
         and parent_fk.confdeltype = 'c'
         and parent_fk.conkey = array[
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = parent_fk.conrelid
               and attribute.attname = 'dataset_kind'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = parent_fk.conrelid
               and attribute.attname = 'id'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = parent_fk.conrelid
               and attribute.attname = 'version'
           )
         ]::smallint[]
     )
     or not exists (
       select 1
       from pg_catalog.pg_constraint as contract_fk
       where contract_fk.conrelid =
           'private.portal_catalog_facet_rows_v1'::regclass
         and contract_fk.confrelid =
           'private.portal_catalog_facet_contract_v1'::regclass
         and contract_fk.conname =
           'portal_catalog_facet_rows_contract_version_v1_fk'
         and contract_fk.contype = 'f'
         and contract_fk.convalidated
         and contract_fk.confupdtype = 'r'
         and contract_fk.confdeltype = 'r'
     ) then
    raise exception using
      errcode = '55000',
      message = 'Portal facet derivation contract drifted';
  end if;
end
$function$;

revoke all on function private.assert_portal_catalog_facet_contract_v1()
from public, anon, authenticated, service_role,
  portal_public_executor, api_internal_executor;
grant execute on function private.assert_portal_catalog_facet_contract_v1()
to portal_public_executor, api_internal_executor;

select private.assert_portal_catalog_facet_contract_v1();

reset role;
revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

do $verify_portal_facet_projection_expand$
declare
  v_expected_columns constant text[] := array[
    'dataset_kind:text:yes',
    'facet_access_level:text:no',
    'facet_contract_version:smallint:yes',
    'facet_geography:text:no',
    'facet_process_subtype:text:no',
    'facet_reference_year:text:no',
    'facet_source:text:no',
    'id:uuid:yes',
    'modified_at:timestamp with time zone:yes',
    'state_code:integer:yes',
    'version:text:yes'
  ]::text[];
  v_actual_columns text[];
begin
  select pg_catalog.array_agg(
    attribute.attname || ':' || pg_catalog.format_type(
      attribute.atttypid,
      attribute.atttypmod
    ) || ':' || case when attribute.attnotnull then 'yes' else 'no' end
    order by attribute.attname
  )
  into v_actual_columns
  from pg_catalog.pg_attribute as attribute
  where attribute.attrelid = 'private.portal_catalog_facet_rows_v1'::regclass
    and attribute.attnum > 0
    and not attribute.attisdropped;

  if v_actual_columns is distinct from v_expected_columns
     or (
       select count(*)
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid in (
           'public.processes'::regclass,
           'public.flows'::regclass
         )
         and trigger.tgname =
           'portal_catalog_projection_content_sync_v1'
         and trigger.tgfoid =
           'private.sync_portal_catalog_search_row_v1()'::regprocedure
         and trigger.tgenabled = 'O'
         and not trigger.tgisinternal
         and trigger.tgtype = 29
         and array(
           select attribute.attname
           from unnest(trigger.tgattr::smallint[])
             with ordinality as trigger_column(attnum, ordinality)
           join pg_catalog.pg_attribute as attribute
             on attribute.attrelid = trigger.tgrelid
            and attribute.attnum = trigger_column.attnum
           order by trigger_column.ordinality
         ) = array[
           'id',
           'version',
           'json',
           'json_ordered',
           'state_code',
           'modified_at'
         ]::name[]
     ) <> 2
     or (
       select count(*)
       from pg_catalog.pg_index as index_catalog
       join pg_catalog.pg_class as index_relation
         on index_relation.oid = index_catalog.indexrelid
       join pg_catalog.pg_namespace as namespace
         on namespace.oid = index_relation.relnamespace
       where namespace.nspname = 'private'
         and index_relation.relname in (
           'portal_catalog_facet_rows_v1_pkey',
           'portal_catalog_facet_rows_latest_v1_idx'
         )
         and index_catalog.indisvalid
         and index_catalog.indisready
         and index_catalog.indislive
     ) <> 2
     or not pg_catalog.has_table_privilege(
       'api_internal_executor',
       'private.portal_catalog_facet_rows_v1',
       'SELECT,INSERT,UPDATE,DELETE'
     )
     or pg_catalog.has_table_privilege(
       'anon',
       'private.portal_catalog_facet_rows_v1',
       'SELECT,INSERT,UPDATE,DELETE'
     )
     or pg_catalog.has_table_privilege(
       'authenticated',
       'private.portal_catalog_facet_rows_v1',
       'SELECT,INSERT,UPDATE,DELETE'
     )
     or pg_catalog.has_table_privilege(
       'service_role',
       'private.portal_catalog_facet_rows_v1',
       'SELECT,INSERT,UPDATE,DELETE'
     ) then
    raise exception 'Portal facet projection expand contract drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_facet_projection_expand$;

commit;
