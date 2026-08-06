CREATE OR REPLACE FUNCTION "private"."review_dataset_can_read_v1"("p_actor" "uuid", "p_target_table" "text", "p_target_row" "jsonb") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select p_actor is not null and (
    coalesce((p_target_row->>'state_code')::integer, 0) >= 100
    or nullif(p_target_row->>'user_id', '')::uuid = p_actor
    or exists (
      select 1
      from private.roles as membership
      where membership.user_id = p_actor
        and membership.team_id = nullif(p_target_row->>'team_id', '')::uuid
        and membership.role in ('owner', 'admin', 'member')
    )
  )
$$;

ALTER FUNCTION "private"."review_dataset_can_read_v1"("p_actor" "uuid", "p_target_table" "text", "p_target_row" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_dataset_can_read_v1"("p_actor" "uuid", "p_target_table" "text", "p_target_row" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_dataset_can_read_v1"("p_actor" "uuid", "p_target_table" "text", "p_target_row" "jsonb") TO "api_internal_executor";
