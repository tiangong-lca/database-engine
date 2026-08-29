CREATE OR REPLACE FUNCTION "private"."assert_portal_catalog_character_contract_v1"() RETURNS "void"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
begin
  if (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_character_rows_v1'::regclass
     ) is not false
     or (
       select count(*)
       from pg_catalog.pg_attribute as attribute
       where attribute.attrelid =
           'private.portal_catalog_character_rows_v1'::regclass
         and attribute.attnum > 0
         and not attribute.attisdropped
         and attribute.attname in (
           'dataset_kind',
           'id',
           'version',
           'state_code',
           'modified_at',
           'document_characters',
           'name_characters',
           'name_exact_characters',
           'classification_characters',
           'classification_exact_characters',
           'character_contract_version'
         )
     ) <> 11
     or not exists (
       select 1
       from pg_catalog.pg_constraint as parent_fk
       where parent_fk.conrelid =
           'private.portal_catalog_character_rows_v1'::regclass
         and parent_fk.confrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and parent_fk.conname = 'portal_catalog_character_parent_v1_fk'
         and parent_fk.contype = 'f'
         and parent_fk.convalidated
         and parent_fk.confupdtype = 'r'
         and parent_fk.confdeltype = 'c'
     )
     or (
       select not index_record.indisvalid
         or not index_record.indisready
         or not index_record.indislive
       from pg_catalog.pg_index as index_record
       where index_record.indexrelid =
         'private.portal_catalog_character_rows_latest_v1_idx'::regclass
     ) is not false
     or not exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and trigger.tgname = 'portal_catalog_character_sync_v1'
         and not trigger.tgisinternal
         and pg_catalog.pg_get_triggerdef(trigger.oid) ~
           'AFTER INSERT OR UPDATE'
     )
     or (
       select count(*)
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_character_rows_v1'
         and policy.policyname =
           'portal_catalog_character_rows_portal_select_v1'
         and policy.roles = array['portal_public_executor']::name[]
         and policy.cmd = 'SELECT'
         and policy.qual = 'true'
         and policy.with_check is null
     ) <> 1
     or (
       select count(*)
       from pg_catalog.pg_proc as routine
       where routine.oid in (
           'private.portal_catalog_character_set_v1(text)'::regprocedure,
           'private.portal_catalog_character_field_set_v1(jsonb,text,boolean)'::regprocedure
         )
         and routine.proowner = 'portal_public_executor'::regrole
         and routine.provolatile = 'i'
         and routine.proparallel = 's'
         and routine.prosecdef
         and routine.proconfig @> array['search_path=""']::text[]
     ) <> 2
     or (
       select routine.proowner <> 'api_internal_executor'::regrole
         or not routine.prosecdef
         or not (coalesce(routine.proconfig, '{}'::text[]) @> array[
           'search_path=""',
           'row_security=on'
         ]::text[])
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.sync_portal_catalog_character_row_v1()'::regprocedure
     ) is not false then
    raise exception using
      errcode = '55000',
      message = 'Portal character projection contract drifted';
  end if;
end
$$;

ALTER FUNCTION "private"."assert_portal_catalog_character_contract_v1"() OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."assert_portal_catalog_character_contract_v1"() FROM PUBLIC;
