-- Issue #551: expand a narrow exact-version character projection. It stores
-- only unique public-card character sets and exact one-code-point name/
-- classification values, so broad one-character Search can pre-limit without
-- copying card/document payloads or adding another text index.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_character_projection_expand_guard$
begin
  if pg_catalog.to_regclass(
       'private.portal_catalog_character_rows_v1'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.portal_catalog_character_set_v1(text)'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.portal_catalog_character_field_set_v1(jsonb,text,boolean)'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.sync_portal_catalog_character_row_v1()'
     ) is not null
     or pg_catalog.to_regclass(
       'private.portal_catalog_search_rows_v1'
     ) is null then
    raise exception 'Portal character projection expand prerequisites drifted'
      using errcode = '55000';
  end if;
end
$portal_character_projection_expand_guard$;

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

create function private.portal_catalog_character_set_v1(p_value text)
returns text
language sql
immutable
parallel safe
security definer
set search_path = ''
as $function$
  select coalesce(
    pg_catalog.string_agg(
      distinct_character.value,
      ''
      order by distinct_character.value collate pg_catalog."C"
    ),
    ''
  )
  from (
    select distinct character.value
    from pg_catalog.regexp_split_to_table(
      coalesce(p_value, ''),
      ''
    ) as character(value)
    where character.value <> ''
  ) as distinct_character
$function$;

create function private.portal_catalog_character_field_set_v1(
  p_items jsonb,
  p_key text,
  p_exact_one boolean
)
returns text
language sql
immutable
parallel safe
security definer
set search_path = ''
as $function$
  select private.portal_catalog_character_set_v1(
    coalesce(
      pg_catalog.string_agg(normalized.value, '' order by normalized.ordinality),
      ''
    )
  )
  from (
    select item.ordinality,
      pg_catalog.lower(pg_catalog.btrim(item.value ->> p_key)) as value
    from pg_catalog.jsonb_array_elements(
      case
        when pg_catalog.jsonb_typeof(p_items) = 'array' then p_items
        else '[]'::jsonb
      end
    ) with ordinality as item(value, ordinality)
    where p_key in ('value', 'code')
      and pg_catalog.jsonb_typeof(item.value) = 'object'
      and pg_catalog.jsonb_typeof(item.value -> p_key) = 'string'
      and nullif(pg_catalog.btrim(item.value ->> p_key), '') is not null
      and (
        not p_exact_one
        or pg_catalog.char_length(
          pg_catalog.lower(pg_catalog.btrim(item.value ->> p_key))
        ) = 1
      )
  ) as normalized
$function$;

revoke all on function private.portal_catalog_character_set_v1(text)
from public, anon, authenticated, service_role;
revoke all on function
  private.portal_catalog_character_field_set_v1(jsonb,text,boolean)
from public, anon, authenticated, service_role;
grant execute on function private.portal_catalog_character_set_v1(text)
to api_internal_executor;
grant execute on function
  private.portal_catalog_character_field_set_v1(jsonb,text,boolean)
to api_internal_executor;

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

create table private.portal_catalog_character_rows_v1 (
  dataset_kind text not null
    check (dataset_kind in ('process', 'flow')),
  id uuid not null,
  version text not null
    check (version ~ '^\d{2}\.\d{2}\.\d{3}$'),
  state_code integer not null
    check (state_code in (100, 200)),
  modified_at timestamptz not null,
  document_characters text not null,
  name_characters text not null,
  name_exact_characters text not null,
  classification_characters text not null,
  classification_exact_characters text not null,
  character_contract_version smallint not null default 1
    check (character_contract_version = 1),
  primary key (dataset_kind, id, version),
  constraint portal_catalog_character_parent_v1_fk
    foreign key (dataset_kind, id, version)
    references private.portal_catalog_search_rows_v1(
      dataset_kind, id, version
    )
    on update restrict
    on delete cascade
);

alter table private.portal_catalog_character_rows_v1 owner to postgres;
alter table private.portal_catalog_character_rows_v1 enable row level security;
alter table private.portal_catalog_character_rows_v1 force row level security;

create policy portal_catalog_character_rows_portal_select_v1
on private.portal_catalog_character_rows_v1
for select
to portal_public_executor
using (true);

create policy portal_catalog_character_rows_internal_all_v1
on private.portal_catalog_character_rows_v1
for all
to api_internal_executor
using (true)
with check (true);

revoke all on table private.portal_catalog_character_rows_v1
from public, anon, authenticated, service_role;
grant select (
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  document_characters,
  name_characters,
  name_exact_characters,
  classification_characters,
  classification_exact_characters
) on table private.portal_catalog_character_rows_v1
to portal_public_executor;
grant select, insert, update, delete
on table private.portal_catalog_character_rows_v1
to api_internal_executor;

create index portal_catalog_character_rows_latest_v1_idx
on private.portal_catalog_character_rows_v1 (
  dataset_kind,
  id,
  version desc,
  modified_at desc,
  state_code desc
);

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
set role api_internal_executor;

create function private.sync_portal_catalog_character_row_v1()
returns trigger
language plpgsql
security definer
set search_path = ''
set row_security = 'on'
as $function$
begin
  insert into private.portal_catalog_character_rows_v1 (
    dataset_kind,
    id,
    version,
    state_code,
    modified_at,
    document_characters,
    name_characters,
    name_exact_characters,
    classification_characters,
    classification_exact_characters,
    character_contract_version
  ) values (
    new.dataset_kind,
    new.id,
    new.version,
    new.state_code,
    new.modified_at,
    private.portal_catalog_character_set_v1(new.document),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'names', 'value', false
    ),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'names', 'value', true
    ),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'classifications', 'code', false
    ),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'classifications', 'code', true
    ),
    1
  )
  on conflict (dataset_kind, id, version) do update
  set state_code = excluded.state_code,
      modified_at = excluded.modified_at,
      document_characters = excluded.document_characters,
      name_characters = excluded.name_characters,
      name_exact_characters = excluded.name_exact_characters,
      classification_characters = excluded.classification_characters,
      classification_exact_characters =
        excluded.classification_exact_characters,
      character_contract_version = excluded.character_contract_version;
  return new;
end
$function$;

revoke all on function private.sync_portal_catalog_character_row_v1()
from public, anon, authenticated, service_role, portal_public_executor;
comment on function private.sync_portal_catalog_character_row_v1() is
  'Synchronizes one exact parent projection version into the narrow character projection.';

reset role;
revoke create on schema private from api_internal_executor;

create trigger portal_catalog_character_sync_v1
after insert or update of
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  card,
  document
on private.portal_catalog_search_rows_v1
for each row
execute function private.sync_portal_catalog_character_row_v1();

revoke api_internal_executor from postgres;

comment on table private.portal_catalog_character_rows_v1 is
  'Narrow exact-version public character sets for bounded one-code-point Search pre-limit; parent FK and INSERT/UPDATE trigger keep it synchronized.';

do $verify_portal_character_projection_expand$
begin
  if (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_character_rows_v1'::regclass
     ) is not false
     or pg_catalog.to_regclass(
       'private.portal_catalog_character_rows_latest_v1_idx'
     ) is null
     or not exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and trigger.tgname = 'portal_catalog_character_sync_v1'
         and not trigger.tgisinternal
     )
     or (
       select count(*)
       from private.portal_catalog_character_rows_v1
     ) <> 0 then
    raise exception 'Portal character projection expand drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_character_projection_expand$;

commit;
