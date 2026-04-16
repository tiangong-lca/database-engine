CREATE OR REPLACE FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") RETURNS TABLE("ref_type" "text", "ref_object_id" "uuid", "ref_version" "text")
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
  with recursive walk(value) as (
    select coalesce(p_json, '{}'::jsonb)
    union all
    select child.value
    from walk
    cross join lateral (
      select object_values.value
      from jsonb_each(
        case
          when jsonb_typeof(walk.value) = 'object' then walk.value
          else '{}'::jsonb
        end
      ) as object_values(key, value)
      union all
      select array_values.value
      from jsonb_array_elements(
        case
          when jsonb_typeof(walk.value) = 'array' then walk.value
          else '[]'::jsonb
        end
      ) as array_values(value)
    ) as child
  )
  select distinct
    lower(trim(value->>'@type')) as ref_type,
    (value->>'@refObjectId')::uuid as ref_object_id,
    value->>'@version' as ref_version
  from walk
  where jsonb_typeof(value) = 'object'
    and value ? '@refObjectId'
    and value ? '@version'
    and value ? '@type'
    and (value->>'@refObjectId') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and nullif(value->>'@version', '') is not null
    and public.cmd_review_ref_type_to_table(value->>'@type') is not null
$_$;

ALTER FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") TO "service_role";
