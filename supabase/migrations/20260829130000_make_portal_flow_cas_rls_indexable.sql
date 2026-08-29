-- Issue #551: the exact Flow CAS expression index was selected under forced
-- RLS, but the non-leakproof JSON extraction equality remained a filter behind
-- the security barrier. The scan therefore touched the whole public index to
-- return one row. Every row in this purpose-built projection is already bound
-- by a validated state-100/200 CHECK and the immutable public-card contract.
-- Keep forced RLS and make its SELECT policy row-neutral so the CAS equality
-- can become an index condition. No relation, index, Trigger, or writer changes.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $portal_flow_cas_rls_prerequisite_guard$
declare
  v_assert_sha256 text;
  v_candidate_sha256 text;
  v_summary_sha256 text;
begin
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'private.assert_portal_catalog_projection_contract_v1()'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_assert_sha256;

  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'private.catalog_portal_candidate_rows_v1(text,text,uuid,text)'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_candidate_sha256;

  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'api.portal_catalog_summary_v1()'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_summary_sha256;

  if v_assert_sha256 <>
       '35cf6024873514fa198dad8684cd41815f7cd4147f715a9becbb764a6bb37149'
     or v_candidate_sha256 <>
       'b001ad1fe7c4ae14fd577a740205af0b2e0f91a62b9912bcb133628e05e8b8cc'
     or v_summary_sha256 <>
       'c23867609136dd833aa713cf3868ad27f9b858efbca8f4f130e1840c9a359094'
     or (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_search_rows_v1'::regclass
     ) is not false
     or not exists (
       select 1
       from pg_catalog.pg_constraint as state_check
       where state_check.conrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and state_check.conname =
           'portal_catalog_search_rows_v1_state_code_check'
         and state_check.contype = 'c'
         and state_check.convalidated
         and pg_catalog.regexp_replace(
           pg_catalog.pg_get_expr(
             state_check.conbin,
             state_check.conrelid
           ),
           '[[:space:]]',
           '',
           'g'
         ) = '(state_code=ANY(ARRAY[100,200]))'
     )
     or (
       select count(*)
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_search_rows_v1'
         and policy.policyname =
           'portal_catalog_search_rows_portal_select_v1'
         and policy.permissive = 'PERMISSIVE'
         and policy.roles = array['portal_public_executor']::name[]
         and policy.cmd = 'SELECT'
         and policy.qual = '(state_code = ANY (ARRAY[100, 200]))'
         and policy.with_check is null
     ) <> 1
     or (
       select count(*)
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_search_rows_v1'
         and policy.roles @> array['portal_public_executor']::name[]
     ) <> 1
     or (
       select index_record.indisvalid is not true
         or index_record.indisready is not true
         or index_record.indisunique is true
         or index_record.indisprimary is true
         or pg_catalog.pg_get_expr(
           index_record.indpred,
           index_record.indrelid,
           true
         ) !~ 'dataset_kind = ''flow'''
         or pg_catalog.pg_get_expr(
           index_record.indpred,
           index_record.indrelid,
           true
         ) !~ 'length.*casNumber.*7.*12'
       from pg_catalog.pg_index as index_record
       where index_record.indexrelid =
         'private.portal_catalog_search_flow_cas_v1_idx'::regclass
     ) is not false then
    raise exception 'Portal Flow CAS RLS-indexability prerequisites drifted'
      using errcode = '55000';
  end if;
end
$portal_flow_cas_rls_prerequisite_guard$;

alter policy portal_catalog_search_rows_portal_select_v1
on private.portal_catalog_search_rows_v1
using (true);

comment on policy portal_catalog_search_rows_portal_select_v1
on private.portal_catalog_search_rows_v1 is
  'Row-neutral forced-RLS SELECT over a purpose-built projection whose validated table CHECK admits only public states 100/200; the neutral policy permits exact JSON-expression predicates to remain index conditions.';

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
set role api_internal_executor;

create or replace function
private.assert_portal_catalog_projection_contract_v1()
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
    'private.catalog_portal_projection_payload_v1(text,integer,jsonb)',
    'private.portal_catalog_card_v1(text,integer,jsonb)',
    'private.portal_capabilities_v1(text,integer,jsonb)',
    'private.portal_publication_root_v1(text,jsonb)',
    'private.portal_access_restrictions_open_v1(jsonb)',
    'private.portal_scalar_text_v1(jsonb)',
    'private.portal_localized_text_v1(jsonb)',
    'private.portal_json_items_v1(jsonb)',
    'private.portal_classifications_v1(jsonb)',
    'private.portal_safe_year_v1(text)',
    'private.portal_source_v1(text,jsonb)'
  ]::text[];
  v_expected_digest constant text :=
    'b5e0aff9abbffcc8d2dacaf559a5d1a8c993c20b647d0c70f0e4fa18eb06d2dc';
  v_live_digest text;
begin
  select private.portal_catalog_projection_manifest_sha256_v1()
  into v_live_digest;

  if v_live_digest is distinct from v_expected_digest
     or (
       select count(*)
       from private.portal_catalog_projection_contract_v1 as contract
       where contract.contract_version = 1
         and contract.manifest_schema =
           'portal.catalog-projection-function-manifest.v1'
         and contract.function_identities = v_expected_identities
         and contract.manifest_sha256 = v_expected_digest
         and contract.created_by_migration = '20260826060422'
     ) <> 1
     or (
       select count(*)
       from private.portal_catalog_projection_contract_v1
     ) <> 1
     or (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_projection_contract_v1'::regclass
     ) is not false
     or (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_search_rows_v1'::regclass
     ) is not false
     or not exists (
       select 1
       from pg_catalog.pg_constraint as state_check
       where state_check.conrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and state_check.conname =
           'portal_catalog_search_rows_v1_state_code_check'
         and state_check.contype = 'c'
         and state_check.convalidated
         and pg_catalog.regexp_replace(
           pg_catalog.pg_get_expr(
             state_check.conbin,
             state_check.conrelid
           ),
           '[[:space:]]',
           '',
           'g'
         ) = '(state_code=ANY(ARRAY[100,200]))'
     )
     or (
       select count(*)
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_search_rows_v1'
         and policy.policyname =
           'portal_catalog_search_rows_portal_select_v1'
         and policy.permissive = 'PERMISSIVE'
         and policy.roles = array['portal_public_executor']::name[]
         and policy.cmd = 'SELECT'
         and policy.qual = 'true'
         and policy.with_check is null
     ) <> 1
     or (
       select count(*)
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_search_rows_v1'
         and policy.roles @> array['portal_public_executor']::name[]
     ) <> 1
     or not exists (
       select 1
       from pg_catalog.pg_attribute as attribute
       where attribute.attrelid =
         'private.portal_catalog_search_rows_v1'::regclass
         and attribute.attname = 'projection_contract_version'
         and attribute.atttypid = 'pg_catalog.int2'::regtype
         and attribute.attnotnull
         and not attribute.atthasdef
         and not attribute.attisdropped
     )
     or not exists (
       select 1
       from pg_catalog.pg_constraint as contract_check
       where contract_check.conrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and contract_check.conname =
           'portal_catalog_search_rows_contract_version_v1_chk'
         and contract_check.contype = 'c'
         and contract_check.convalidated
         and pg_catalog.regexp_replace(
           pg_catalog.pg_get_expr(
             contract_check.conbin,
             contract_check.conrelid
           ),
           '[[:space:]]',
           '',
           'g'
         ) = '(projection_contract_version=1)'
     )
     or not exists (
       select 1
       from pg_catalog.pg_constraint as contract_fk
       where contract_fk.conrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and contract_fk.confrelid =
           'private.portal_catalog_projection_contract_v1'::regclass
         and contract_fk.conname =
           'portal_catalog_search_rows_contract_version_v1_fk'
         and contract_fk.contype = 'f'
         and contract_fk.convalidated
         and contract_fk.confupdtype = 'r'
         and contract_fk.confdeltype = 'r'
         and contract_fk.conkey = array[(
           select attribute.attnum
           from pg_catalog.pg_attribute as attribute
           where attribute.attrelid = contract_fk.conrelid
             and attribute.attname = 'projection_contract_version'
         )]::smallint[]
         and contract_fk.confkey = array[(
           select attribute.attnum
           from pg_catalog.pg_attribute as attribute
           where attribute.attrelid = contract_fk.confrelid
             and attribute.attname = 'contract_version'
         )]::smallint[]
     ) then
    raise exception using
      errcode = '55000',
      message = 'Portal projection derivation contract drifted';
  end if;
end
$function$;

revoke all on function
  private.assert_portal_catalog_projection_contract_v1()
from public, anon, authenticated, service_role,
  portal_public_executor, api_internal_executor;
grant execute on function
  private.assert_portal_catalog_projection_contract_v1()
to portal_public_executor, api_internal_executor;

reset role;
revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

grant portal_public_executor to postgres;
set role portal_public_executor;
select private.assert_portal_catalog_projection_contract_v1();
reset role;
revoke portal_public_executor from postgres;

do $verify_portal_flow_cas_rls_indexability$
declare
  v_summary jsonb;
  v_cas jsonb;
  v_search jsonb;
begin
  if (
       select policy.qual <> 'true'
         or policy.with_check is not null
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_search_rows_v1'
         and policy.policyname =
           'portal_catalog_search_rows_portal_select_v1'
     ) is not false
     or exists (
       select 1
       from private.portal_catalog_search_rows_v1 as projection
       where projection.state_code not in (100, 200)
     ) then
    raise exception 'Portal Flow CAS row-domain proof drifted'
      using errcode = '55000';
  end if;

  v_summary := api.portal_catalog_summary_v1();
  select example.value
  into v_cas
  from pg_catalog.jsonb_array_elements(v_summary -> 'examples') as example(value)
  where example.value ->> 'queryKind' = 'cas';

  if v_cas is not null then
    v_search := api.portal_search_flows_v1(
      v_cas ->> 'query', '{}'::jsonb, 'relevance', null, 50
    );
    if pg_catalog.jsonb_array_length(v_search -> 'items') <> 1 then
      raise exception 'Portal summary CAS is not selectively executable'
        using errcode = '55000';
    end if;
  end if;
end
$verify_portal_flow_cas_rls_indexability$;

commit;
