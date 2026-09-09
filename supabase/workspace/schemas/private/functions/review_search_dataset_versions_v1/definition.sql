CREATE OR REPLACE FUNCTION "private"."review_search_dataset_versions_v1"("p_query" "text", "p_target_table" "text" DEFAULT NULL::"text") RETURNS TABLE("target_table" "text", "data_id" "uuid", "data_version" character)
    LANGUAGE "plpgsql" STABLE
    SET "search_path" TO ''
    AS $_$
declare
  v_table text;
  v_uuid uuid;
  v_terms text[];
  v_predicate text;
begin
  if nullif(pg_catalog.btrim(p_query), '') is null then return; end if;
  if pg_catalog.btrim(p_query) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_uuid := pg_catalog.btrim(p_query)::uuid;
  end if;
  v_terms := private.pgroonga_escape_query_terms(array[p_query]);
  foreach v_table in array array[
    'contacts', 'sources', 'unitgroups', 'flowproperties', 'flows',
    'processes', 'lifecyclemodels'
  ] loop
    if p_target_table is not null and v_table <> p_target_table then continue; end if;
    if v_uuid is not null then
      return query execute pg_catalog.format(
        'select %L::text, d.id, d.version from public.%I d where d.id = $1',
        v_table, v_table
      ) using v_uuid;
    else
      -- Preserve the current ordinary data-list predicates: foundation query
      -- syntax and core escaped term arrays. No AI expansion or JSON flattening.
      v_predicate := case when v_table in ('flows', 'processes', 'lifecyclemodels')
        then 'd.search_text operator(extensions.&@~|) $2'
        else 'd.search_text operator(extensions.&@~) $1' end;
      return query execute pg_catalog.format(
        'select %L::text, d.id, d.version from public.%I d where %s',
        v_table, v_table, v_predicate
      ) using p_query, v_terms;
    end if;
  end loop;
end;
$_$;

ALTER FUNCTION "private"."review_search_dataset_versions_v1"("p_query" "text", "p_target_table" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_search_dataset_versions_v1"("p_query" "text", "p_target_table" "text") FROM PUBLIC;
