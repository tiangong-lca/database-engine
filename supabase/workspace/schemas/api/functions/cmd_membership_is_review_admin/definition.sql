CREATE OR REPLACE FUNCTION "api"."cmd_membership_is_review_admin"("p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select exists (
    select 1
    from private.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'review-admin'
  )
$$;

ALTER FUNCTION "api"."cmd_membership_is_review_admin"("p_actor" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_membership_is_review_admin"("p_actor" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_membership_is_review_admin"("p_actor" "uuid") TO "api_internal_executor";
