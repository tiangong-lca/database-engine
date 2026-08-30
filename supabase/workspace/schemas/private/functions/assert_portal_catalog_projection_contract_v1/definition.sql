CREATE OR REPLACE FUNCTION "private"."assert_portal_catalog_projection_contract_v1"() RETURNS "void"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
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
$$;

ALTER FUNCTION "private"."assert_portal_catalog_projection_contract_v1"() OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."assert_portal_catalog_projection_contract_v1"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."assert_portal_catalog_projection_contract_v1"() TO "portal_public_executor";
