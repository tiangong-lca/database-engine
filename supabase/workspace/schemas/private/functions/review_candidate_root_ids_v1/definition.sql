CREATE OR REPLACE FUNCTION "private"."review_candidate_root_ids_v1"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text") RETURNS TABLE("root_review_id" "uuid")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
begin
  if p_target_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    return;
  end if;

  return query execute pg_catalog.format(
    $query$
      select distinct (entry.value->>'id')::uuid
      from public.%I as dataset_row
      cross join lateral pg_catalog.jsonb_array_elements(
        case
          when pg_catalog.jsonb_typeof(dataset_row.reviews) = 'array'
            then dataset_row.reviews
          else '[]'::jsonb
        end
      ) as entry(value)
      where dataset_row.id = $1
        and pg_catalog.btrim(dataset_row.version::text) = $2
        and coalesce(entry.value->>'id', '')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    $query$,
    p_target_table
  ) using p_target_id, p_target_version;
end;
$_$;

ALTER FUNCTION "private"."review_candidate_root_ids_v1"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_candidate_root_ids_v1"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_candidate_root_ids_v1"("p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text") TO "api_internal_executor";
