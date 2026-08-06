CREATE OR REPLACE FUNCTION "api"."cmd_review_merge_compliance_declarations"("p_existing" "jsonb", "p_additions" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  with normalized as (
    select
      case
        when jsonb_typeof(p_existing) = 'object' then p_existing
        else '{}'::jsonb
      end as existing_obj,
      case
        when jsonb_typeof(p_additions) = 'object' then p_additions
        else '{}'::jsonb
      end as additions_obj
  ),
  merged as (
    select
      existing_obj,
      additions_obj,
      existing_obj || (additions_obj - 'compliance') as base_obj
    from normalized
  )
  select case
    when additions_obj ? 'compliance' then
      jsonb_set(
        base_obj,
        '{compliance}',
        api.cmd_review_merge_json_collection(
          existing_obj->'compliance',
          additions_obj->'compliance'
        ),
        true
      )
    else base_obj
  end
  from merged
$$;

ALTER FUNCTION "api"."cmd_review_merge_compliance_declarations"("p_existing" "jsonb", "p_additions" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_merge_compliance_declarations"("p_existing" "jsonb", "p_additions" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_merge_compliance_declarations"("p_existing" "jsonb", "p_additions" "jsonb") TO "api_internal_executor";
