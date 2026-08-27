CREATE OR REPLACE FUNCTION "private"."assert_portal_sitemap_projection_v1"() RETURNS "void"
    LANGUAGE "plpgsql" STABLE PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $$
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
$$;

ALTER FUNCTION "private"."assert_portal_sitemap_projection_v1"() OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."assert_portal_sitemap_projection_v1"() FROM PUBLIC;
