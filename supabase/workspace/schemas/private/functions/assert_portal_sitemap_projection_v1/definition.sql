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
     )
     or not exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid =
         'private.portal_catalog_facet_rows_v1'::regclass
         and trigger.tgname = 'portal_sitemap_latest_sync_v1'
         and not trigger.tgisinternal
         and trigger.tgenabled = 'O'
         and trigger.tgfoid =
           'private.sync_portal_sitemap_latest_row_v1()'::regprocedure
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
           '91af513bb8fed85bd4f8a1999c30cfbc'
     ) then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  end if;
end
$$;

ALTER FUNCTION "private"."assert_portal_sitemap_projection_v1"() OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."assert_portal_sitemap_projection_v1"() FROM PUBLIC;
