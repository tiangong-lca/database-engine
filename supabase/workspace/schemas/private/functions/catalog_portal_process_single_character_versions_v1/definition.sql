CREATE OR REPLACE FUNCTION "private"."catalog_portal_process_single_character_versions_v1"("p_literal" "text") RETURNS TABLE("id" "uuid", "version" "text")
    LANGUAGE "sql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '20s'
    SET "enable_indexscan" TO 'off'
    SET "enable_indexonlyscan" TO 'off'
    SET "enable_bitmapscan" TO 'off'
    SET "max_parallel_workers_per_gather" TO '4'
    SET "min_parallel_table_scan_size" TO '0'
    SET "parallel_setup_cost" TO '0'
    SET "parallel_tuple_cost" TO '0'
    SET "row_security" TO 'on'
    AS $$
  select projection.id,
    projection.version
  from private.portal_catalog_search_rows_v1 as projection
  where projection.dataset_kind = 'process'
    and pg_catalog.strpos(projection.document, p_literal) > 0
$$;

ALTER FUNCTION "private"."catalog_portal_process_single_character_versions_v1"("p_literal" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_process_single_character_versions_v1"("p_literal" "text") FROM PUBLIC;
