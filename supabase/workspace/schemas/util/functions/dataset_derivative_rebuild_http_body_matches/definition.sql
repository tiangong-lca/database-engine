CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_http_body_matches"("p_body" "bytea", "p_id" "uuid", "p_version" "text") RETURNS boolean
    LANGUAGE "sql" STABLE
    SET "search_path" TO ''
    AS $$
  select util.dataset_derivative_rebuild_http_body_matches(
    p_body,
    'processes',
    p_id,
    p_version
  )
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_http_body_matches"("p_body" "bytea", "p_id" "uuid", "p_version" "text") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_http_body_matches"("p_body" "bytea", "p_table" "text", "p_id" "uuid", "p_version" "text") RETURNS boolean
    LANGUAGE "plpgsql" STABLE
    SET "search_path" TO ''
    AS $$
declare
  v_body jsonb;
begin
  if p_body is null
    or p_table is null
    or p_table not in ('flows', 'processes') then
    return false;
  end if;
  v_body := pg_catalog.convert_from(p_body, 'UTF8')::jsonb;
  if jsonb_typeof(v_body) = 'object' then
    return v_body #>> '{record,id}' = p_id::text
      and btrim(v_body #>> '{record,version}') = p_version
      and coalesce(v_body->>'table', 'processes') = p_table;
  end if;

  if jsonb_typeof(v_body) = 'array' then
    return exists (
      select 1
      from jsonb_array_elements(v_body) as job(value)
      where job.value->>'id' = p_id::text
        and btrim(job.value->>'version') = p_version
        and job.value->>'schema' = 'public'
        and job.value->>'table' = p_table
        and job.value->>'embeddingColumn' = 'embedding_ft'
    );
  end if;

  return false;
exception
  when others then
    return false;
end;
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_http_body_matches"("p_body" "bytea", "p_table" "text", "p_id" "uuid", "p_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_http_body_matches"("p_body" "bytea", "p_id" "uuid", "p_version" "text") FROM PUBLIC;

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_http_body_matches"("p_body" "bytea", "p_table" "text", "p_id" "uuid", "p_version" "text") FROM PUBLIC;
