-- Issue #551: bounded UUID-quarter backfill for the narrow character projection.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_character_backfill_guard$
begin
  if pg_catalog.to_regclass(
       'private.portal_catalog_character_rows_v1'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.portal_catalog_character_set_v1(text)'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.portal_catalog_character_field_set_v1(jsonb,text,boolean)'
     ) is null then
    raise exception 'Portal character backfill prerequisites drifted'
      using errcode = '55000';
  end if;
end
$portal_character_backfill_guard$;

grant api_internal_executor to postgres;
set role api_internal_executor;

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
)
select projection.dataset_kind,
  projection.id,
  projection.version,
  projection.state_code,
  projection.modified_at,
  private.portal_catalog_character_set_v1(projection.document),
  private.portal_catalog_character_field_set_v1(
    projection.card -> 'names', 'value', false
  ),
  private.portal_catalog_character_field_set_v1(
    projection.card -> 'names', 'value', true
  ),
  private.portal_catalog_character_field_set_v1(
    projection.card -> 'classifications', 'code', false
  ),
  private.portal_catalog_character_field_set_v1(
    projection.card -> 'classifications', 'code', true
  ),
  1
from private.portal_catalog_search_rows_v1 as projection
where projection.id >= '80000000-0000-0000-0000-000000000000'::uuid
  and projection.id < 'c0000000-0000-0000-0000-000000000000'::uuid
on conflict (dataset_kind, id, version) do nothing;

reset role;
revoke api_internal_executor from postgres;

do $verify_portal_character_backfill$
begin
  if exists (
    select 1
    from private.portal_catalog_search_rows_v1 as projection
    left join private.portal_catalog_character_rows_v1 as character_row
      on character_row.dataset_kind = projection.dataset_kind
     and character_row.id = projection.id
     and character_row.version = projection.version
    where projection.id >= '80000000-0000-0000-0000-000000000000'::uuid
  and projection.id < 'c0000000-0000-0000-0000-000000000000'::uuid
      and character_row.id is null
  ) then
    raise exception 'Portal character backfill is incomplete'
      using errcode = '55000';
  end if;
end
$verify_portal_character_backfill$;

commit;
