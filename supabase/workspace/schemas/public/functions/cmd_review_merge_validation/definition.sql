CREATE OR REPLACE FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
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
      existing_obj || (additions_obj - 'review') as base_obj
    from normalized
  )
  select case
    when additions_obj ? 'review' then
      jsonb_set(
        base_obj,
        '{review}',
        public.cmd_review_merge_json_collection(existing_obj->'review', additions_obj->'review'),
        true
      )
    else base_obj
  end
  from merged
$$;

ALTER FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") TO "service_role";
