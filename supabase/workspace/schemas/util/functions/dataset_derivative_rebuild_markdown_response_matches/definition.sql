CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_markdown_response_matches"("p_content" "text", "p_id" "uuid", "p_version" "text") RETURNS boolean
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO ''
    AS $$
  select util.dataset_derivative_rebuild_markdown_response_matches(
    p_content,
    'processes',
    p_id,
    p_version
  )
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_markdown_response_matches"("p_content" "text", "p_id" "uuid", "p_version" "text") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_markdown_response_matches"("p_content" "text", "p_table" "text", "p_id" "uuid", "p_version" "text") RETURNS boolean
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO ''
    AS $$
declare
  v_content jsonb;
begin
  if p_table is null or p_table not in ('flows', 'processes') then
    return false;
  end if;
  v_content := p_content::jsonb;
  return v_content->>'success' = 'true'
    and jsonb_typeof(v_content->'results') = 'array'
    and jsonb_array_length(v_content->'results') = 1
    and v_content #>> '{results,0,id}' = p_id::text
    and btrim(v_content #>> '{results,0,version}') = p_version
    and coalesce(v_content #>> '{results,0,table}', 'processes') = p_table
    and v_content #>> '{results,0,status}' = 'success';
exception
  when others then
    return false;
end;
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_markdown_response_matches"("p_content" "text", "p_table" "text", "p_id" "uuid", "p_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_markdown_response_matches"("p_content" "text", "p_id" "uuid", "p_version" "text") FROM PUBLIC;

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_markdown_response_matches"("p_content" "text", "p_table" "text", "p_id" "uuid", "p_version" "text") FROM PUBLIC;
