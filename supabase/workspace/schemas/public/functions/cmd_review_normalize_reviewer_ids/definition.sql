CREATE OR REPLACE FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
  with normalized as (
    select
      value,
      min(ordinality) as ordinality
    from jsonb_array_elements_text(
      case
        when jsonb_typeof(p_reviewer_ids) = 'array' then p_reviewer_ids
        else '[]'::jsonb
      end
    ) with ordinality as reviewer_ids(value, ordinality)
    where value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    group by value
  )
  select coalesce(
    jsonb_agg(to_jsonb(value) order by ordinality),
    '[]'::jsonb
  )
  from normalized
$_$;

ALTER FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") TO "service_role";
