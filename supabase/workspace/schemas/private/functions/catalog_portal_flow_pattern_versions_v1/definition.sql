CREATE OR REPLACE FUNCTION "private"."catalog_portal_flow_pattern_versions_v1"("p_like_pattern" "text") RETURNS TABLE("id" "uuid", "version" "text")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $_$
declare
  v_literal text;
begin
  if pg_catalog.char_length(p_like_pattern) = 3
     and pg_catalog.left(p_like_pattern, 1) = '%'
     and pg_catalog.right(p_like_pattern, 1) = '%' then
    v_literal := pg_catalog.substr(p_like_pattern, 2, 1);
    return query
    select projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
      and pg_catalog.strpos(projection.document, v_literal) > 0;
    return;
  end if;

  return query execute pg_catalog.format($sql$
    select projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
      and projection.document like %L escape E'\\'
  $sql$, p_like_pattern);
end
$_$;

ALTER FUNCTION "private"."catalog_portal_flow_pattern_versions_v1"("p_like_pattern" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_flow_pattern_versions_v1"("p_like_pattern" "text") FROM PUBLIC;
