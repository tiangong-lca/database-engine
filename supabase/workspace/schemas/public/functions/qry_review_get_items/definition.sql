CREATE OR REPLACE FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[] DEFAULT NULL::"uuid"[], "p_data_id" "uuid" DEFAULT NULL::"uuid", "p_data_version" "text" DEFAULT NULL::"text", "p_state_codes" integer[] DEFAULT NULL::integer[]) RETURNS TABLE("id" "uuid", "data_id" "uuid", "data_version" "text", "state_code" integer, "reviewer_id" "jsonb", "json" "jsonb", "deadline" timestamp with time zone, "created_at" timestamp with time zone, "modified_at" timestamp with time zone)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select
    r.id,
    r.data_id,
    r.data_version::text as data_version,
    r.state_code,
    coalesce(r.reviewer_id, '[]'::jsonb) as reviewer_id,
    coalesce(r.json, '{}'::jsonb) as json,
    r.deadline,
    r.created_at,
    r.modified_at
  from public.reviews as r
  where (p_review_ids is null or r.id = any (p_review_ids))
    and (
      p_data_id is null
      or r.data_id = p_data_id
      or coalesce(r.json -> 'data' ->> 'id', '') = p_data_id::text
    )
    and (
      p_data_version is null
      or r.data_version = p_data_version
      or coalesce(r.json -> 'data' ->> 'version', '') = p_data_version
    )
    and (p_state_codes is null or r.state_code = any (p_state_codes))
    and public.policy_review_can_read(r.id, auth.uid())
  order by r.modified_at desc, r.id desc
$$;

ALTER FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[], "p_data_id" "uuid", "p_data_version" "text", "p_state_codes" integer[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[], "p_data_id" "uuid", "p_data_version" "text", "p_state_codes" integer[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[], "p_data_id" "uuid", "p_data_version" "text", "p_state_codes" integer[]) TO "anon";

GRANT ALL ON FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[], "p_data_id" "uuid", "p_data_version" "text", "p_state_codes" integer[]) TO "authenticated";

GRANT ALL ON FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[], "p_data_id" "uuid", "p_data_version" "text", "p_state_codes" integer[]) TO "service_role";
